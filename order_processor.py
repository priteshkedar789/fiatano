"""OrderProcessor — single workflow controller.

Only component that may trigger a payout or call mark_order_as_paid.
Owns the state machine: WAIT_PAYMENT → [payout confirmed] → WAIT_RELEASE.
"""
import logging

from .domain import Order, OrderStatus, ProcessResult
from .exceptions import PendingPayoutError, PersistenceError, PayoutResolutionError
from .payment_resolver import resolve as resolve_payment
from .payment_service import PaymentService
from .transaction_store import TransactionStore

logger = logging.getLogger(__name__)

_NON_ACTIONABLE = {
    OrderStatus.PENDING: "not yet actionable",
    OrderStatus.WAIT_RELEASE: "payment already dispatched",
    OrderStatus.COMPLETED: "order closed",
    OrderStatus.CANCELLED: "order void",
    OrderStatus.CANCELLED_BY_SYSTEM: "order void (system)",
    OrderStatus.CANCELLED_SYSTEM: "order void (system post-appeal)",
    OrderStatus.APPEALING: "requires manual intervention",
}


class OrderProcessor:
    def __init__(
        self,
        binance_client,
        chat_handler,
        payment_service: PaymentService,
        store: TransactionStore,
    ):
        self._binance = binance_client
        self._chat = chat_handler
        self._payment = payment_service
        self._store = store

    def process(self, order: Order) -> ProcessResult:
        try:
            status = OrderStatus(order.status)
        except ValueError:
            return ProcessResult(
                order_number=order.order_number, success=False,
                error=f"Unknown order status: {order.status}",
            )

        if status != OrderStatus.WAIT_PAYMENT:
            reason = _NON_ACTIONABLE.get(status, f"status={status}")
            logger.debug("Skipping order %s: %s", order.order_number, reason)
            return ProcessResult(
                order_number=order.order_number, success=False,
                skipped=True, skip_reason=reason,
            )

        logger.info("Processing order %s | %.2f %s", order.order_number, order.amount, order.fiat_unit)

        # Guard: already fully confirmed in store (handles WAIT_PAYMENT state lag on Binance side)
        existing = self._store.get_latest(order.order_number)
        if existing and existing.status == "CONFIRMED":
            logger.info("Order %s already CONFIRMED in store", order.order_number)
            return ProcessResult(
                order_number=order.order_number, success=True,
                skipped=True, skip_reason="already confirmed", payout_record=existing,
            )

        # Step 1: Fetch full order detail for payMethods
        detail_resp = self._binance.get_order_detail(order.order_number)
        if not detail_resp.get("success"):
            error = f"get_order_detail failed: {detail_resp.get('message', 'unknown')}"
            logger.error("Order %s: %s", order.order_number, error)
            return ProcessResult(order_number=order.order_number, success=False, error=error)

        detail = detail_resp.get("data", {})
        pay_methods = detail.get("payMethods", [])
        pay_type = detail.get("payType", "IMPS")
        seller_name = (
            detail.get("sellerNickName")
            or detail.get("nickName")
            or order.seller_name
            or "Unknown"
        )

        # Step 2: Resolve payment method (pure, no I/O)
        try:
            payment_details = resolve_payment(pay_methods, pay_type, order.amount)
        except PayoutResolutionError as e:
            logger.error("Order %s: %s", order.order_number, e)
            return ProcessResult(order_number=order.order_number, success=False, error=str(e))

        # Step 3: Collect PAN via chat
        if not self._chat.connect(order.order_number):
            error = "Chat connection failed"
            logger.error("Order %s: %s", order.order_number, error)
            return ProcessResult(order_number=order.order_number, success=False, error=error)

        try:
            pan_success, pan, _verification = self._chat.collect_pan(seller_name, max_attempts=3)
            if not pan_success:
                error = "PAN collection or verification failed"
                logger.error("Order %s: %s", order.order_number, error)
                self._binance.cancel_order(order.order_number)
                return ProcessResult(order_number=order.order_number, success=False, error=error)
        finally:
            self._chat.disconnect()

        # Step 4: Execute payout — idempotent; persists intent before dispatch, result after
        try:
            payout = self._payment.execute_payout(order, payment_details)
        except PendingPayoutError as e:
            logger.warning("Order %s: %s", order.order_number, e)
            return ProcessResult(order_number=order.order_number, success=False, error=str(e))
        except PersistenceError as e:
            # Money may or may not have moved — do not retry, alert immediately
            logger.critical("Order %s: PERSISTENCE FAILURE — %s", order.order_number, e)
            return ProcessResult(order_number=order.order_number, success=False, error=str(e))
        except Exception as e:
            logger.error("Order %s: payout error: %s", order.order_number, e)
            return ProcessResult(order_number=order.order_number, success=False, error=str(e))

        if payout.status not in ("SUCCESS", "PENDING"):
            error = f"Payout {payout.status}: {payout.error}"
            logger.error("Order %s: %s", order.order_number, error)
            return ProcessResult(
                order_number=order.order_number, success=False,
                error=error, payout_record=payout,
            )

        # Step 5: Mark order as paid on Binance — only after confirmed payout record exists
        mark_resp = self._binance.mark_order_as_paid(order.order_number)
        if not mark_resp.get("success"):
            # Payout succeeded but Binance transition failed.
            # Do NOT retry payout. Alert and halt for manual resolution.
            error = (
                f"Binance mark_order_as_paid failed: {mark_resp.get('message', 'unknown')} — "
                f"payout already dispatched: cf_transfer_id={payout.cf_transfer_id}, "
                f"idempotency_key={payout.idempotency_key} — MANUAL INTERVENTION REQUIRED"
            )
            logger.critical("Order %s: %s", order.order_number, error)
            return ProcessResult(
                order_number=order.order_number, success=False,
                error=error, payout_record=payout,
            )

        # Step 6: Persist Binance confirmation
        if not self._store.confirm_binance(order.order_number):
            logger.error(
                "Order %s: Binance mark-paid succeeded but CONFIRMED persistence failed",
                order.order_number,
            )

        logger.info(
            "Order %s: COMPLETE | cf_transfer_id=%s | utr=%s",
            order.order_number, payout.cf_transfer_id, payout.utr,
        )
        return ProcessResult(
            order_number=order.order_number, success=True, payout_record=payout,
        )
