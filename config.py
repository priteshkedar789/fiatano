import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    binance_api_key: str
    binance_secret_key: str
    cashfree_client_id: str
    cashfree_client_secret: str
    cashfree_sandbox: bool
    poll_interval_seconds: int
    data_dir: Path
    debug: bool

    @classmethod
    def from_env(cls) -> "Config":
        missing = []

        def require(key: str) -> str:
            val = os.getenv(key, "")
            if not val:
                missing.append(key)
            return val

        cfg = cls(
            binance_api_key=require("BINANCE_API_KEY"),
            binance_secret_key=require("BINANCE_SECRET_KEY"),
            cashfree_client_id=require("CASHFREE_CLIENT_ID"),
            cashfree_client_secret=require("CASHFREE_CLIENT_SECRET"),
            cashfree_sandbox=os.getenv("CASHFREE_SANDBOX", "false").lower() == "true",
            poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "30")),
            data_dir=Path(os.getenv("DATA_DIR", "./data")),
            debug=os.getenv("DEBUG", "false").lower() == "true",
        )
        if missing:
            raise ValueError(f"Missing required env vars: {', '.join(missing)}")
        return cfg
