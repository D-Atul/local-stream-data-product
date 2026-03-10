import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone


# Money-flow event mix: mostly deposits, some withdrawals
EVENT_TYPES = [
    ("deposit_completed", 0.75),
    ("withdrawal_requested", 0.15),
    ("withdrawal_completed", 0.10),
]

CURRENCIES = ["GBP"]
CHANNELS = ["web", "mobile", "api"]


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def pick_event_type(rng: random.Random) -> str:
    roll = rng.random()
    running = 0.0
    for name, weight in EVENT_TYPES:
        running += weight
        if roll <= running:
            return name
    return EVENT_TYPES[-1][0]


def make_user_id(n: int) -> str:
    return f"user_{n:05d}"


def amount_gbp(rng: random.Random) -> float:
    # Why: real money-flow is usually small, occasionally large.
    if rng.random() < 0.85:
        base = rng.randint(1, 200)
    else:
        base = rng.randint(200, 1000)
    return round(base + rng.random(), 2)


def ensure_dir_for(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Append-only JSONL producer for local streaming tests (money-flow events)"
    )
    parser.add_argument("--out", default="data/stream.log", help="Output JSONL file (append-only)")
    parser.add_argument("--num-users", type=int, default=10_000, help="Total distinct users")
    parser.add_argument("--eps", type=int, default=100, help="Target events per second")
    parser.add_argument("--run-seconds", type=int, default=0, help="0 = run forever; else stop after N seconds")
    parser.add_argument("--seed", type=int, default=0, help="0 = random; else deterministic stream")
    parser.add_argument("--hot-users-count", type=int, default=25, help="How many 'hot' users at a time")
    parser.add_argument("--hot-user-weight", type=float, default=0.35, help="Chance an event uses a hot user")
    parser.add_argument("--hot-refresh-seconds", type=int, default=15, help="Rotate hot users every N seconds")
    parser.add_argument("--flush-every", type=int, default=1, help="Flush every N events (1 = flush each event)")
    args = parser.parse_args()

    # Why: seed controls repeatability for evidence runs.
    rng = random.Random()
    rng.seed(args.seed if args.seed else None)

    ensure_dir_for(args.out)

    # Why: append-only file acts like a persisted local stream.
    # Note: flush() pushes to OS buffers; fsync would be "disk-durable" but is too expensive for this demo.
    f = open(args.out, "a", encoding="utf-8")

    started_at = time.time()
    tick = 1.0 / max(args.eps, 1)

    next_emit_at = time.monotonic()
    hot_users: list[int] = []
    refresh_hot_at = time.monotonic()  # refresh immediately

    events_written = 0

    try:
        while True:
            # Stop condition (wall clock is fine for "run N seconds")
            if args.run_seconds > 0 and (time.time() - started_at) >= args.run_seconds:
                break

            now_mono = time.monotonic()

            # Why: hot users simulate bursts/velocity pressure on per-user state.
            if now_mono >= refresh_hot_at:
                hot_users = [rng.randint(1, args.num_users) for _ in range(args.hot_users_count)]
                refresh_hot_at = now_mono + max(args.hot_refresh_seconds, 1)

            # Pace emissions to roughly match EPS (arrival-order is the streaming model here).
            if now_mono < next_emit_at:
                time.sleep(0.001)
                continue

            event_type = pick_event_type(rng)

            if hot_users and rng.random() < args.hot_user_weight:
                user_n = rng.choice(hot_users)
            else:
                user_n = rng.randint(1, args.num_users)

            ts = utc_iso()
            event = {
                "event_id": str(uuid.uuid4()),
                "user_id": make_user_id(user_n),
                "event_time": ts,
                "ingest_time": ts,
                "event_type": event_type,
                "amount": amount_gbp(rng),
                "currency": rng.choice(CURRENCIES),
                "channel": rng.choice(CHANNELS),
            }

            f.write(json.dumps(event) + "\n")
            events_written += 1

            if events_written % max(args.flush_every, 1) == 0:
                f.flush()

            # Schedule next tick. If we fell behind badly, reset to avoid runaway catch-up.
            next_emit_at += tick
            if now_mono - next_emit_at > 1.0:
                next_emit_at = now_mono + tick

    except KeyboardInterrupt:
        pass
    finally:
        try:
            f.flush()
        finally:
            f.close()

    duration_s = max(time.time() - started_at, 0.001)
    print(
        f"Producer stopped. events={events_written} duration_s={duration_s:.2f} "
        f"approx_eps={events_written/duration_s:.2f} out={args.out}"
    )


if __name__ == "__main__":
    main()
