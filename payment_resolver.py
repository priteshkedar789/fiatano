"""Pure function: resolves PaymentDetails from raw Binance payMethods + payType.

No side effects. No I/O. No state.
"""
import logging
from typing import List, Optional

from .domain import PaymentDetails
from .exceptions import PayoutResolutionError

logger = logging.getLogger(__name__)

_TRANSFER_TYPE_MAP = {
    "IMPSPAN": "IMPS",
    "BANKINDIA": "IMPS",
    "IMPS": "IMPS",
    "NEFT": "NEFT",
    "RTGS": "RTGS",
    "UPI": "UPI",
}

_UPI_FIELD_NAMES = {"upi", "vpa", "upi id", "upi address", "virtual payment address"}

_RTGS_THRESHOLD = 500_000.0


def _valid_upi(upi_id: str) -> bool:
    parts = upi_id.split("@")
    return len(parts) == 2 and bool(parts[0]) and bool(parts[1])


def resolve(pay_methods: List[dict], pay_type: str, amount: float) -> PaymentDetails:
    if not pay_methods:
        raise PayoutResolutionError("No payMethods in order")

    fields = pay_methods[0].get("fields", [])
    upi_id: Optional[str] = None
    account_number: Optional[str] = None
    ifsc_code: Optional[str] = None
    payee_name: str = "Beneficiary"

    for field in fields:
        name = field.get("fieldName", "").lower()
        content_type = field.get("fieldContentType", "").lower()
        value = field.get("fieldValue", "").strip()
        if not value:
            continue

        if any(ind in name for ind in _UPI_FIELD_NAMES):
            if _valid_upi(value):
                upi_id = value
            else:
                logger.warning("Skipping invalid UPI ID: %s", value)

        is_account = (
            ("account" in name and "number" in name)
            or ("a/c" in name and "number" in name)
            or ("ac" in name and "no" in name)
            or content_type == "pay_account"
            or "account" in content_type
        )
        if is_account:
            account_number = value

        if "ifsc" in name:
            ifsc_code = value

        is_payee = content_type == "payee" or (
            any(x in name for x in ("name", "payee", "beneficiary"))
            and "bank" not in name
            and "ifsc" not in name
        )
        if is_payee:
            payee_name = value

    if upi_id:
        return PaymentDetails(method="UPI", payee_name=payee_name, upi_id=upi_id)

    if account_number and ifsc_code:
        pay_upper = (pay_type or "IMPS").upper()
        method = next(
            (v for k, v in _TRANSFER_TYPE_MAP.items() if k in pay_upper),
            "IMPS",
        )
        if amount > _RTGS_THRESHOLD:
            logger.info("Amount %.2f > RTGS threshold, upgrading from %s to RTGS", amount, method)
            method = "RTGS"
        return PaymentDetails(
            method=method,
            payee_name=payee_name,
            account_number=account_number,
            ifsc_code=ifsc_code,
        )

    raise PayoutResolutionError(
        f"Cannot resolve payment method | fields={len(fields)} | "
        f"upi_id={upi_id} | account_number={account_number} | ifsc_code={ifsc_code}"
    )
