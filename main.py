#!/usr/bin/env python3
"""Entry point. Run from repo root: python -m automation.main"""
import logging
import sys
import time
from pathlib import Path

# Repo root on path so binance_client, binance_chat_handler, pan_verifier import cleanly
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

from binance_client import BinanceP2PClient
from binance_chat_handler import ChatDispatcher, ChatHandler
from pan_verifier import PANVerifier

from automation.cashfree_client import CashfreeClient
from automation.config import Config
from automation.domain import Order, OrderStatus
from automation.order_processor import OrderProcessor
from automation.payment_service import PaymentService
from automation.transaction_store import TransactionStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

_CHAT_MESSAGES = {
    "welcome": "Hello {buyer_name}! To process your order, please share your PAN card number.",
    "pan_request": "Please send your PAN number (format: ABCDE1234F).",
    "pan_verifying": "Verifying PAN {pan}...",
    "pan_verified": "PAN verified successfully. Processing payment now.",
    "pan_invalid_format": "Invalid PAN format. Please use 10-character format: ABCDE1234F.",
    "pan_timeout": "No response received. Please send your PAN number.",
    "pan_max_retries": "Maximum attempts reached. Order will be cancelled.",
    "pan_name_mismatch": (
        "PAN name ({pan_name}) does not match order name ({buyer_name}). "
        "Please send the correct PAN."
    ),
    "pan_verification_failed": "PAN verification failed: {error}. Please retry.",
    "payment_success": (
        "Payment of ₹{amount} processed successfully. UTR: {utr}. "
        "Please release the crypto once you receive the funds."
    ),
    "payment_failed": "Payment could not be processed: {error}. Please contact support.",
    "payment_details_request": "Please share your payment details (UPI ID or Bank Account).",
    "payment_details_confirm": (
        "Confirm payment details:\n{details_formatted}\nAmount: ₹{amount}\n"
        "Reply YES to confirm."
    ),
    "payment_details_invalid": "Details not confirmed. Please resend your payment details.",
    "upi_payment_accepted": "UPI ID {upi_id} received. Processing payment.",
    "bank_account_request": "Please send your bank account number.",
    "bank_ifsc_request": "Account {account_number} received. Now send your IFSC code.",
    "invalid_account_number": "Could not read account number. Please send only the digits.",
    "invalid_ifsc_code": "Could not read IFSC code. Format: XXXX0XXXXXX.",
}


def _build_order_from_list_item(item: dict) -> Order:
    return Order(
        order_number=item["orderNumber"],
        status=int(item.get("orderStatus", 0)),
        amount=float(item.get("totalPrice", 0)),
        fiat_unit=item.get("fiatUnit", "INR"),
        asset=item.get("asset", "USDT"),
        trade_type=item.get("tradeType", "BUY"),
        seller_name=item.get("sellerNickName") or item.get("nickName") or "Unknown",
        order_date=str(item.get("createTime", "")),
    )


def run():
    load_dotenv()
    cfg = Config.from_env()
    cfg.data_dir.mkdir(parents=True, exist_ok=True)

    if cfg.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    binance = BinanceP2PClient(
        api_key=cfg.binance_api_key,
        secret_key=cfg.binance_secret_key,
        debug=cfg.debug,
    )
    pan_verifier = PANVerifier(
        client_id=cfg.cashfree_client_id,
        client_secret=cfg.cashfree_client_secret,
    )
    cashfree = CashfreeClient(
        client_id=cfg.cashfree_client_id,
        client_secret=cfg.cashfree_client_secret,
        sandbox=cfg.cashfree_sandbox,
    )
    store = TransactionStore(cfg.data_dir)

    dispatcher = ChatDispatcher(binance)
    if not dispatcher.connect():
        logger.critical("Cannot establish Binance WebSocket — aborting")
        sys.exit(1)

    chat_handler = ChatHandler(dispatcher, binance, pan_verifier, _CHAT_MESSAGES)
    payment_service = PaymentService(cashfree, store)
    processor = OrderProcessor(binance, chat_handler, payment_service, store)

    logger.info(
        "Automation started | poll_interval=%ds | sandbox=%s",
        cfg.poll_interval_seconds, cfg.cashfree_sandbox,
    )

    try:
        while True:
            try:
                response = binance.list_orders(order_status_list=[OrderStatus.WAIT_PAYMENT], rows=50)
                orders_raw = response.get("data", []) if response.get("success") else []

                if orders_raw:
                    logger.info("Found %d order(s) in WAIT_PAYMENT", len(orders_raw))

                for item in orders_raw:
                    order = _build_order_from_list_item(item)
                    result = processor.process(order)

                    if result.skipped:
                        logger.debug("Order %s skipped: %s", result.order_number, result.skip_reason)
                    elif result.success:
                        logger.info("Order %s: SUCCESS", result.order_number)
                    else:
                        logger.error("Order %s: FAILED — %s", result.order_number, result.error)

            except Exception as e:
                logger.error("Poll cycle error: %s", e, exc_info=True)

            time.sleep(cfg.poll_interval_seconds)

    except KeyboardInterrupt:
        logger.info("Shutting down")
    finally:
        dispatcher.disconnect()


if __name__ == "__main__":
    run()
