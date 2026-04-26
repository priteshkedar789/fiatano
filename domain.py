from __future__ import annotations
from dataclasses import dataclass, field
from enum import IntEnum
from typing import List, Optional


class OrderStatus(IntEnum):
    PENDING = 0
    WAIT_PAYMENT = 1
    WAIT_RELEASE = 2
    APPEALING = 3
    COMPLETED = 4
    CANCELLED_BY_SYSTEM = 5
    CANCELLED = 6
    CANCELLED_SYSTEM = 7


@dataclass(frozen=True)
class PaymentDetails:
    method: str       # "UPI" | "IMPS" | "NEFT" | "RTGS"
    payee_name: str
    upi_id: Optional[str] = None
    account_number: Optional[str] = None
    ifsc_code: Optional[str] = None


@dataclass(frozen=True)
class Order:
    order_number: str
    status: int
    amount: float
    fiat_unit: str
    asset: str
    trade_type: str
    seller_name: str
    order_date: str
    pay_methods: List[dict] = field(default_factory=list)
    pay_type: str = "IMPS"


@dataclass
class PayoutRecord:
    order_number: str
    idempotency_key: str
    status: str      # INTENT | SUCCESS | PENDING | FAILED | CONFIRMED
    amount: float
    transfer_type: str
    created_at: str
    cf_transfer_id: Optional[str] = None
    utr: Optional[str] = None
    binance_confirmed: bool = False
    error: Optional[str] = None


@dataclass(frozen=True)
class ProcessResult:
    order_number: str
    success: bool
    skipped: bool = False
    skip_reason: str = ""
    error: str = ""
    payout_record: Optional[PayoutRecord] = None
