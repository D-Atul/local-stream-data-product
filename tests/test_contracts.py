import pytest
from src.contracts.event_contract import validate_event

VALID_EVENT = {
    "event_id": "e1",
    "user_id": "user_1",
    "event_time": "2026-01-01T00:00:00+00:00",
    "ingest_time": "2026-01-01T00:00:00+00:00",
    "event_type": "deposit_completed",
    "amount": 100,
    "currency": "GBP",
    "channel": "web",
}


def test_valid_event_passes():
    validate_event(VALID_EVENT.copy())


def test_missing_required_field():
    bad = VALID_EVENT.copy()
    del bad["user_id"]

    with pytest.raises(ValueError):
        validate_event(bad)


def test_invalid_event_type():
    bad = VALID_EVENT.copy()
    bad["event_type"] = "bad_event"

    with pytest.raises(ValueError):
        validate_event(bad)


def test_invalid_currency():
    bad = VALID_EVENT.copy()
    bad["currency"] = "USD"

    with pytest.raises(ValueError):
        validate_event(bad)


def test_invalid_amount():
    bad = VALID_EVENT.copy()
    bad["amount"] = -5

    with pytest.raises(ValueError):
        validate_event(bad)