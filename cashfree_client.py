"""Cashfree Payouts API v1 client. Returns typed PayoutRecord — no raw dicts escape."""
import logging
import threading
import time
from collections import deque

import requests

from .domain import PaymentDetails, PayoutRecord

logger = logging.getLogger(__name__)

_TOKEN_TTL = 290          # refresh 10s before the 300s expiry
_RETRY_ATTEMPTS = 3
_RETRY_STATUSES = {429, 500, 502, 503, 504}


class CashfreeClient:
    def __init__(self, client_id: str, client_secret: str, sandbox: bool = False):
        self._base = (
            "https://payout-gamma.cashfree.com" if sandbox
            else "https://payout-api.cashfree.com"
        )
        self._client_id = client_id
        self._client_secret = client_secret

        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

        self._token_expiry: float = 0.0
        self._token_lock = threading.Lock()

        self._rate_requests = 30
        self._rate_window = 60
        self._timestamps: deque = deque()
        self._rate_lock = threading.Lock()

        self._authorize()

    # ── auth ─────────────────────────────────────────────────────────────────

    def _authorize(self) -> None:
        resp = requests.post(
            f"{self._base}/payout/v1/authorize",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "X-Client-Id": self._client_id,
                "X-Client-Secret": self._client_secret,
            },
            timeout=15,
        )
        data = resp.json()
        if data.get("status") != "SUCCESS":
            raise RuntimeError(f"Cashfree auth failed: {data.get('message', data)}")
        self._session.headers["Authorization"] = f"Bearer {data['data']['token']}"
        self._token_expiry = time.time() + _TOKEN_TTL
        logger.info("Cashfree token refreshed")

    def _ensure_token(self) -> None:
        with self._token_lock:
            if time.time() >= self._token_expiry:
                self._authorize()

    # ── rate limiting ────────────────────────────────────────────────────────

    def _check_rate_limit(self) -> None:
        while True:
            with self._rate_lock:
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

    # ── http ─────────────────────────────────────────────────────────────────

    def _request(self, method: str, url: str, **kwargs) -> dict:
        self._ensure_token()
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

    # ── public ───────────────────────────────────────────────────────────────

    def get_balance(self) -> float:
        result = self._request("GET", f"{self._base}/payout/v1/getBalance", timeout=30)
        if result.get("status") != "SUCCESS":
            raise RuntimeError(f"Balance check failed: {result}")
        return float(result.get("data", {}).get("availableBalance", 0))

    def _add_beneficiary(self, bene_id: str, payment_details: PaymentDetails) -> None:
        payload: dict = {
            "beneId": bene_id,
            "name": payment_details.payee_name or "Beneficiary",
            "email": "payout@automation.internal",
            "phone": payment_details.phone or "9999999999",
            "address1": "India",
        }
        if payment_details.method.upper() == "UPI":
            if not payment_details.upi_id:
                raise ValueError("PaymentDetails.upi_id required for UPI transfer")
            payload["vpa"] = payment_details.upi_id
        else:
            if not payment_details.account_number or not payment_details.ifsc_code:
                raise ValueError("account_number and ifsc_code required for bank transfer")
            payload["bankAccount"] = payment_details.account_number
            payload["ifsc"] = payment_details.ifsc_code

        result = self._request(
            "POST", f"{self._base}/payout/v1/addBeneficiary",
            json=payload, timeout=30,
        )
        if result.get("status") != "SUCCESS":
            raise RuntimeError(f"addBeneficiary failed: {result.get('message', result)}")

    def transfer(
        self,
        order_number: str,
        idempotency_key: str,
        payment_details: PaymentDetails,
        amount: float,
    ) -> PayoutRecord:
        amount = round(amount, 2)

        # Step 1 — register beneficiary (beneId = idempotency_key for uniqueness)
        try:
            self._add_beneficiary(bene_id=idempotency_key, payment_details=payment_details)
        except requests.exceptions.HTTPError as e:
            body = {}
            try:
                body = e.response.json()
            except Exception:
                pass
            if e.response.status_code == 409:
                # Already exists from a prior attempt — safe to proceed
                logger.info("Beneficiary %s already registered, continuing", idempotency_key)
            else:
                raise RuntimeError(f"Cashfree addBeneficiary failed: {body or str(e)}") from e

        # Step 2 — request transfer
        try:
            result = self._request(
                "POST",
                f"{self._base}/payout/v1/requestTransfer",
                json={
                    "beneId": idempotency_key,
                    "amount": str(amount),
                    "transferId": idempotency_key,
                    "remarks": f"Order No: {order_number}",
                },
                timeout=60,
            )
        except requests.exceptions.HTTPError as e:
            body = {}
            try:
                body = e.response.json()
            except Exception:
                pass
            raise RuntimeError(f"Cashfree requestTransfer failed: {body or str(e)}") from e

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
                cf_transfer_id=data.get("referenceId"),
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
                f"{self._base}/payout/v1/getTransferStatus",
                params={"transferId": idempotency_key},
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
            cf_transfer_id=transfer.get("referenceId"),
            utr=transfer.get("utr"),
        )
