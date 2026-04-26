"""PaymentService — owns idempotency logic and dispatch sequence.

Invariants enforced here:
  1. No payout dispatched without a prior persisted INTENT record.
  2. No duplicate dispatch for an order with status SUCCESS or CONFIRMED.
  3. On persistence failure post-dispatch: raise PersistenceError, never silently continue.
"""
import logging
import uuid

from .cashfree_client import CashfreeClient
from .domain import Order, PaymentDetails, PayoutRecord
from .exceptions import PendingPayoutError, PersistenceError
from .transaction_store import TransactionStore

logger = logging.getLogger(__name__)


def _new_idempotency_key(order_number: str) -> str:
    return f"BNP2P_{order_number}_{str(uuid.uuid4())[:8].upper()}"


class PaymentService:
    def __init__(self, cashfree: CashfreeClient, store: TransactionStore):
        self._cashfree = cashfree
        self._store = store

    def execute_payout(self, order: Order, payment_details: PaymentDetails) -> PayoutRecord:
        existing = self._store.get_latest(order.order_number)

        if existing:
            if existing.status in ("CONFIRMED", "SUCCESS"):
                logger.info("Order %s: status=%s, skipping dispatch", order.order_number, existing.status)
                return existing

            if existing.status in ("PENDING", "INTENT"):
                logger.info(
                    "Order %s: status=%s, querying Cashfree before proceeding",
                    order.order_number, existing.status,
                )
                live = self._cashfree.get_transfer_status(
                    idempotency_key=existing.idempotency_key,
                    order_number=order.order_number,
                    amount=existing.amount,
                    transfer_type=existing.transfer_type,
                )

                if live.status == "SUCCESS":
                    if not self._store.persist_result(live):
                        raise PersistenceError(
                            f"Order {order.order_number}: Cashfree reports SUCCESS but result "
                            f"persistence failed — cf_transfer_id={live.cf_transfer_id}"
                        )
                    return live

                if live.status == "PENDING":
                    raise PendingPayoutError(
                        f"Order {order.order_number}: payout still PENDING on Cashfree — "
                        f"idempotency_key={existing.idempotency_key}, manual review required"
                    )

                if live.status == "NOT_FOUND" and existing.status == "INTENT":
                    # Crashed between persist_intent and transfer call — safe to dispatch
                    logger.info(
                        "Order %s: INTENT found, no transfer on Cashfree — dispatching now",
                        order.order_number,
                    )
                    return self._dispatch(order, payment_details, existing.idempotency_key)

            # FAILED — safe to retry with fresh idempotency key
            if existing.status == "FAILED":
                logger.info("Order %s: previous attempt FAILED, retrying", order.order_number)

        # Fresh order or retrying after FAILED
        idempotency_key = _new_idempotency_key(order.order_number)

        if not self._store.persist_intent(
            order_number=order.order_number,
            idempotency_key=idempotency_key,
            amount=order.amount,
            transfer_type=payment_details.method,
        ):
            raise PersistenceError(
                f"Order {order.order_number}: failed to persist INTENT — aborting, no payout sent"
            )

        return self._dispatch(order, payment_details, idempotency_key)

    def _dispatch(
        self, order: Order, payment_details: PaymentDetails, idempotency_key: str
    ) -> PayoutRecord:
        balance = self._cashfree.get_balance()
        if order.amount > balance:
            raise RuntimeError(
                f"Insufficient balance: available={balance:.2f} < required={order.amount:.2f}"
            )

        result = self._cashfree.transfer(
            order_number=order.order_number,
            idempotency_key=idempotency_key,
            payment_details=payment_details,
            amount=order.amount,
        )

        if not self._store.persist_result(result):
            if result.status in ("SUCCESS", "PENDING"):
                # Money moved but we can't record it — this is the critical failure mode
                raise PersistenceError(
                    f"Order {order.order_number}: payout {result.status} but result persistence "
                    f"failed — idempotency_key={idempotency_key}, "
                    f"cf_transfer_id={result.cf_transfer_id} — MANUAL REVIEW REQUIRED"
                )
            # FAILED + persistence failure — no money moved, log and continue
            logger.error(
                "Order %s: payout FAILED and persistence also failed: %s",
                order.order_number, result.error,
            )

        return result
