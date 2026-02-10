from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Tuple


# v1 event contract: minimal fields needed to validate + drive deterministic rules
REQUIRED_FIELDS = (
    "event_id",
    "user_id",
    "event_time",
    "ingest_time",
    "event_type",
    "amount",
    "currency",
    "channel",
)

ALLOWED_EVENT_TYPES = {
    "deposit_completed",
    "withdrawal_requested",
    "withdrawal_completed",
}

ALLOWED_CURRENCIES = {"GBP"}
ALLOWED_CHANNELS = {"web", "mobile", "api"}


def is_non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and value.strip() != ""


def require(event: Dict[str, Any], key: str) -> Any:
    if key not in event:
        raise ValueError(f"missing required field: {key}")
    return event[key]


def parse_iso_datetime(value: str, field_name: str) -> datetime:
    """
    Why: watcher enforces ordering in v1, so timestamps must be parseable.
    """
    try:
        # Generator emits timezone-aware ISO strings (e.g. ...+00:00).
        return datetime.fromisoformat(value)
    except Exception as e:
        raise ValueError(f"{field_name} must be ISO-8601 datetime string; got {value!r}") from e


def validate_event(event: Dict[str, Any]) -> None:
    """
    Validates a single event dict.
    Fail-fast: raises ValueError on the first failure.
    """
    if not isinstance(event, dict):
        raise ValueError(f"event must be a dict; got {type(event).__name__}")

    # Required fields
    for key in REQUIRED_FIELDS:
        require(event, key)

    # Core string fields
    for key in ("event_id", "user_id", "event_type", "currency", "channel"):
        value = event[key]
        if not is_non_empty_string(value):
            raise ValueError(f"{key} must be a non-empty string; got {value!r}")

    # Enums
    if event["event_type"] not in ALLOWED_EVENT_TYPES:
        raise ValueError(f"event_type must be one of {sorted(ALLOWED_EVENT_TYPES)}; got {event['event_type']!r}")

    if event["currency"] not in ALLOWED_CURRENCIES:
        raise ValueError(f"currency must be one of {sorted(ALLOWED_CURRENCIES)}; got {event['currency']!r}")

    if event["channel"] not in ALLOWED_CHANNELS:
        raise ValueError(f"channel must be one of {sorted(ALLOWED_CHANNELS)}; got {event['channel']!r}")

    # Amount: basic sanity only (rules decide “suspicious”)
    amount = event["amount"]
    if not isinstance(amount, (int, float)):
        raise ValueError(f"amount must be a number; got {type(amount).__name__}: {amount!r}")
    if amount <= 0:
        raise ValueError(f"amount must be > 0; got {amount!r}")
    if amount > 1_000_000:
        raise ValueError(f"amount is unreasonably large; got {amount!r}")

    # Timestamps
    parse_iso_datetime(event["event_time"], "event_time")
    parse_iso_datetime(event["ingest_time"], "ingest_time")


@dataclass(frozen=True)
class ContractSpec:
    name: str = "money_flow_event_contract"
    version: str = "v1"


def contract_id() -> Tuple[str, str]:
    """
    Identifier used in run logs and validation reports.
    """
    spec = ContractSpec()
    return spec.name, spec.version
