class AutomationError(Exception):
    pass


class PersistenceError(AutomationError):
    """Payout dispatched but result could not be persisted. Manual review required."""
    pass


class PayoutResolutionError(AutomationError):
    """Payment method cannot be resolved from order details."""
    pass


class PendingPayoutError(AutomationError):
    """Existing payout is PENDING; cannot proceed safely without terminal status."""
    pass


class BinanceMarkPaidError(AutomationError):
    """Binance mark-as-paid failed after payout succeeded."""
    pass
