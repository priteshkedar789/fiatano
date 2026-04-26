"""Append-only CSV store for payout records.

Each status transition appends a new row. get_latest() returns the last row
for an order_number — no in-place mutation means no partial-write corruption.
"""
import csv
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

from .domain import PayoutRecord

logger = logging.getLogger(__name__)

_FIELDS = [
    "order_number", "idempotency_key", "status", "amount", "transfer_type",
    "created_at", "cf_transfer_id", "utr", "binance_confirmed", "error",
]


class TransactionStore:
    def __init__(self, data_dir: Path):
        data_dir.mkdir(parents=True, exist_ok=True)
        self._path = data_dir / "payout_records.csv"
        self._lock = threading.Lock()
        self._init()

    def _init(self):
        try:
            with open(self._path, "x", newline="") as f:
                csv.writer(f).writerow(_FIELDS)
            logger.info("Created %s", self._path)
        except FileExistsError:
            pass

    def persist_intent(
        self, order_number: str, idempotency_key: str, amount: float, transfer_type: str
    ) -> bool:
        with self._lock:
            try:
                with open(self._path, "a", newline="") as f:
                    csv.writer(f).writerow([
                        order_number, idempotency_key, "INTENT",
                        amount, transfer_type, datetime.now().isoformat(),
                        "", "", False, "",
                    ])
                logger.info("Persisted INTENT for order %s key=%s", order_number, idempotency_key)
                return True
            except Exception as e:
                logger.error("Failed to persist INTENT for order %s: %s", order_number, e)
                return False

    def persist_result(self, record: PayoutRecord) -> bool:
        with self._lock:
            try:
                with open(self._path, "a", newline="") as f:
                    csv.writer(f).writerow([
                        record.order_number, record.idempotency_key, record.status,
                        record.amount, record.transfer_type, datetime.now().isoformat(),
                        record.cf_transfer_id or "", record.utr or "",
                        record.binance_confirmed, record.error or "",
                    ])
                logger.info("Persisted %s for order %s", record.status, record.order_number)
                return True
            except Exception as e:
                logger.error("Failed to persist result for order %s: %s", record.order_number, e)
                return False

    def confirm_binance(self, order_number: str) -> bool:
        existing = self.get_latest(order_number)
        if not existing:
            logger.error("Cannot confirm Binance for unknown order %s", order_number)
            return False
        confirmed = PayoutRecord(
            order_number=existing.order_number,
            idempotency_key=existing.idempotency_key,
            status="CONFIRMED",
            amount=existing.amount,
            transfer_type=existing.transfer_type,
            created_at=existing.created_at,
            cf_transfer_id=existing.cf_transfer_id,
            utr=existing.utr,
            binance_confirmed=True,
        )
        return self.persist_result(confirmed)

    def get_latest(self, order_number: str) -> Optional[PayoutRecord]:
        with self._lock:
            try:
                latest = None
                with open(self._path, "r", newline="") as f:
                    for row in csv.DictReader(f):
                        if row["order_number"] == order_number:
                            latest = _row_to_record(row)
                return latest
            except Exception as e:
                logger.error("Failed to read store for order %s: %s", order_number, e)
                return None


def _row_to_record(row: dict) -> PayoutRecord:
    return PayoutRecord(
        order_number=row["order_number"],
        idempotency_key=row["idempotency_key"],
        status=row["status"],
        amount=float(row.get("amount") or 0),
        transfer_type=row.get("transfer_type", ""),
        created_at=row.get("created_at", ""),
        cf_transfer_id=row.get("cf_transfer_id") or None,
        utr=row.get("utr") or None,
        binance_confirmed=row.get("binance_confirmed", "").lower() == "true",
        error=row.get("error") or None,
    )
