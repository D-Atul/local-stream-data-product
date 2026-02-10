from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import deque
from typing import Any, Dict, List


def parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts)


# -----------------------------
# Rule configuration (v1)
# -----------------------------
@dataclass(frozen=True)
class RuleConfig:
    # Threshold
    threshold_amount: float = 500.0

    # Velocity: number of deposits within window_seconds
    velocity_window_seconds: int = 10
    velocity_max_deposits: int = 5

    # Repetition: number of breaches within window_seconds
    repetition_window_seconds: int = 300
    repetition_max_breaches: int = 3


# -----------------------------
# Per-user state
# -----------------------------
@dataclass
class UserState:
    deposit_times: deque[datetime]
    breach_times: deque[datetime]
    # Why: avoid spamming repetition flags once above threshold
    repetition_already_flagged: bool = False


class WatcherState:
    def __init__(self) -> None:
        self.users: Dict[str, UserState] = {}

    def get_user(self, user_id: str) -> UserState:
        if user_id not in self.users:
            self.users[user_id] = UserState(
                deposit_times=deque(),
                breach_times=deque(),
                repetition_already_flagged=False,
            )
        return self.users[user_id]


# -----------------------------
# Helpers
# -----------------------------
def _prune_older_than(q: deque[datetime], cutoff: datetime) -> None:
    while q and q[0] < cutoff:
        q.popleft()


def _make_flag(
    *,
    user_id: str,
    rule_name: str,
    metric_value: int | float,
    threshold: int | float,
    window_seconds: int,
    event: Dict[str, Any],
) -> Dict[str, Any]:
    # Why: deterministic ID makes flags traceable and idempotent if needed later
    flag_id = f"{event['event_id']}:{rule_name}"

    return {
        "flag_id": flag_id,
        "user_id": user_id,
        "rule_name": rule_name,
        "metric_value": metric_value,
        "threshold": threshold,
        "window": window_seconds,
        "event_id": event["event_id"],
        "event_time": event["event_time"],
        "ingest_time": event["ingest_time"],
    }


# -----------------------------
# Rule evaluation
# -----------------------------
def evaluate_rules(
    event: Dict[str, Any],
    state: WatcherState,
    cfg: RuleConfig,
) -> List[Dict[str, Any]]:
    
    flags: List[Dict[str, Any]] = []

    user_id = event["user_id"]
    et = parse_iso(event["event_time"])
    u = state.get_user(user_id)

    # ---- Threshold rule
    amount = float(event["amount"])
    if amount > cfg.threshold_amount:
        flags.append(
            _make_flag(
                user_id=user_id,
                rule_name="threshold",
                metric_value=amount,
                threshold=cfg.threshold_amount,
                window_seconds=0,
                event=event,
            )
        )

    # ---- Velocity rule
    if event["event_type"] == "deposit_completed":
        window = timedelta(seconds=cfg.velocity_window_seconds)
        cutoff = et - window

        u.deposit_times.append(et)
        _prune_older_than(u.deposit_times, cutoff)

        deposit_count = len(u.deposit_times)
        if deposit_count > cfg.velocity_max_deposits:
            flags.append(
                _make_flag(
                    user_id=user_id,
                    rule_name="velocity",
                    metric_value=deposit_count,
                    threshold=cfg.velocity_max_deposits,
                    window_seconds=cfg.velocity_window_seconds,
                    event=event,
                )
            )

    # ---- Repetition rule 
    
    if flags:
        rep_window = timedelta(seconds=cfg.repetition_window_seconds)
        rep_cutoff = et - rep_window

        u.breach_times.append(et)
        _prune_older_than(u.breach_times, rep_cutoff)

        breach_count = len(u.breach_times)

        # Emit repetition flag only once per window
        if breach_count > cfg.repetition_max_breaches and not u.repetition_already_flagged:
            flags.append(
                _make_flag(
                    user_id=user_id,
                    rule_name="repetition",
                    metric_value=breach_count,
                    threshold=cfg.repetition_max_breaches,
                    window_seconds=cfg.repetition_window_seconds,
                    event=event,
                )
            )
            u.repetition_already_flagged = True

        # Reset latch once we fall back under the threshold
        if breach_count <= cfg.repetition_max_breaches:
            u.repetition_already_flagged = False

    return flags
