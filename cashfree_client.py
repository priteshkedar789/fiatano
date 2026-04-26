"""Cashfree Payouts API client. Returns typed PayoutRecord — no raw dicts escape."""
import logging
import threading
import time
from collections import deque

import requests

from .domain import PaymentDetails, PayoutRecord

logger = logging.getLogger(__name__)

_API_VERSION = "2023-08-01"
_RETRY_ATTEMPTS = 3
_RETRY_STATUSES = {429, 500, 502, 503, 504}


class CashfreeClient:
    def __init__(self, client_id: str, client_secret: str, sandbox: bool = False):
        base = "https://payout-gamma.cashfree.com" if sandbox else "https://payout-api.cashfree.com"
        self._base = base
        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-client-id": client_id,
            "x-client-secret": client_secret,
            "x-api-version": _API_VERSION,
        })
        self._rate_requests = 30
        self._rate_window = 60
        self._timestamps: deque = deque()
        self._lock = threading.Lock()

    def _check_rate_limit(self):
        while True:
            with self._lock:
                now = time.time()
                while self._timestamps and self._timestamps[0] < now - self._rate_window:
                    self._timestamps.popleft()
                if len(self._timestamps) < self._rate_requests:
                    self._timestamps.append(now)
                    return
                sleep_time = self._rate_window - (now - self._timestamps[0])
            if sleep_time > 0:
                logger.warning("Rate limit reached. Sleeping %.2fs", sleep_time)
                time.sleep(sleep_time)

    def _request(self, method: str, url: str, **kwargs) -> dict:
        last_exc: Exception = RuntimeError("no attempts made")
        for attempt in range(_RETRY_ATTEMPTS):
            self._check_rate_limit()
            try:
                response = self._session.request(method, url, **kwargs)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code not in _RETRY_STATUSES or attempt == _RETRY_ATTEMPTS - 1:
                    raise
                last_exc = e
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt == _RETRY_ATTEMPTS - 1:
                    raise
                last_exc = e
            delay = 2 ** attempt
            logger.warning("Cashfree %s %s failed, retry %d/%d in %ds: %s",
                           method, url, attempt + 1, _RETRY_ATTEMPTS, delay, last_exc)
            time.sleep(delay)
        raise last_exc

    def get_balance(self) -> float:
        result = self._request("GET", f"{self._base}/payout/v1.2/getBalance", timeout=30)
        if result.get("status") != "SUCCESS":
            raise RuntimeError(f"Balance check failed: {result}")
        return float(result.get("data", {}).get("availableBalance", 0))

    def transfer(
        self,
        order_number: str,
        idempotency_key: str,
        payment_details: PaymentDetails,
        amount: float,
    ) -> PayoutRecord:
        amount = round(amount, 2)
        mode = payment_details.method.lower()  # already resolved by PaymentResolver

        if mode == "upi":
            if not payment_details.upi_id:
                raise ValueError("PaymentDetails.upi_id required for UPI transfer")
            bene_instrument = {"vpa": payment_details.upi_id}
        else:
            if not payment_details.account_number or not payment_details.ifsc_code:
                raise ValueError("PaymentDetails.account_number and ifsc_code required for bank transfer")
            bene_instrument = {
                "bankAccount": payment_details.account_number,
                "ifsc": payment_details.ifsc_code,
            }

        payload = {
            "transferId": idempotency_key,
            "transferAmount": amount,
            "transferCurrency": "INR",
            "transferMode": mode,
            "beneficiaryDetails": {
                "beneId": idempotency_key,
                "beneName": payment_details.payee_name or "Beneficiary",
                "beneInstrument": bene_instrument,
            },
            "remarks": f"Order No: {order_number}",
        }

        try:
            result = self._request(
                "POST",
                f"{self._base}/payout/v1.2/directTransfer",
                json=payload,
                timeout=60,
            )
        except requests.exceptions.HTTPError as e:
            body = {}
            try:
                body = e.response.json()
            except Exception:
                pass
            raise RuntimeError(f"Cashfree transfer failed: {body or str(e)}") from e

        status_str = result.get("status", "")
        data = result.get("data", {})
        logger.info("Cashfree transfer status=%s order=%s", status_str, order_number)

        if status_str in ("SUCCESS", "PENDING"):
            return PayoutRecord(
                order_number=order_number,
                idempotency_key=idempotency_key,
                status=status_str,
                amount=amount,
                transfer_type=payment_details.method,
                created_at="",
                cf_transfer_id=data.get("cfTransferId"),
                utr=data.get("utr"),
            )

        error_msg = result.get("message") or result.get("subMessage") or "Unknown Cashfree error"
        return PayoutRecord(
            order_number=order_number,
            idempotency_key=idempotency_key,
            status="FAILED",
            amount=amount,
            transfer_type=payment_details.method,
            created_at="",
            error=error_msg,
        )

    def get_transfer_status(
        self, idempotency_key: str, order_number: str, amount: float, transfer_type: str
    ) -> PayoutRecord:
        try:
            result = self._request(
                "GET",
                f"{self._base}/payout/v1.2/getTransferStatus",
                params={"referenceId": idempotency_key},
                timeout=30,
            )
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return PayoutRecord(
                    order_number=order_number,
                    idempotency_key=idempotency_key,
                    status="NOT_FOUND",
                    amount=amount,
                    transfer_type=transfer_type,
                    created_at="",
                )
            raise

        transfer = result.get("data", {}).get("transfer", {})
        return PayoutRecord(
            order_number=order_number,
            idempotency_key=idempotency_key,
            status=transfer.get("status", "UNKNOWN"),
            amount=amount,
            transfer_type=transfer_type,
            created_at="",
            cf_transfer_id=transfer.get("cfTransferId"),
            utr=transfer.get("utr"),
        )
