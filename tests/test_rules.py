from src.pipelines.rules import evaluate_rules, WatcherState, RuleConfig

BASE_EVENT = {
    "event_id": "e1",
    "user_id": "user_1",
    "event_time": "2026-01-01T00:00:00+00:00",
    "ingest_time": "2026-01-01T00:00:00+00:00",
    "event_type": "deposit_completed",
    "amount": 100,
    "currency": "GBP",
    "channel": "web",
}


def test_threshold_rule_triggers():
    state = WatcherState()
    cfg = RuleConfig()

    event = BASE_EVENT.copy()
    event["amount"] = 600

    flags = evaluate_rules(event, state, cfg)

    assert any(f["rule_name"] == "threshold" for f in flags)


def test_velocity_rule_triggers():
    state = WatcherState()
    cfg = RuleConfig()

    flags = []

    for i in range(6):
        event = BASE_EVENT.copy()
        event["event_id"] = f"e{i}"
        flags += evaluate_rules(event, state, cfg)

    assert any(f["rule_name"] == "velocity" for f in flags)


def test_no_flag_for_small_amount():
    state = WatcherState()
    cfg = RuleConfig()

    flags = evaluate_rules(BASE_EVENT.copy(), state, cfg)

    assert flags == []