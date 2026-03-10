from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from src.contracts.event_contract import validate_event, contract_id
from src.jobs.rules import RuleConfig, WatcherState, evaluate_rules


# -----------------------------
# Small IO helpers
# -----------------------------
def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir_for(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def safe_write_json(path: str, payload: Dict[str, Any]) -> None:
    """
    Why: avoids half-written JSON files if the process is killed mid-write.
    """
    ensure_dir_for(path)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, path)


def parse_event_time(event: Dict[str, Any]) -> datetime:
    # Contract validates ISO shape; we parse for ordering checks.
    return datetime.fromisoformat(event["event_time"])


# -----------------------------
# State: checkpoint + stats
# -----------------------------
@dataclass
class Checkpoint:
    byte_offset: int = 0
    saved_at: Optional[str] = None


class CheckpointStore:
    def __init__(self, path: str) -> None:
        self.path = path

    def load(self) -> Checkpoint:
        if not os.path.exists(self.path):
            return Checkpoint(byte_offset=0)

        with open(self.path, "r", encoding="utf-8") as f:
            obj = json.load(f)

        return Checkpoint(
            byte_offset=int(obj.get("offset", 0)),
            saved_at=obj.get("saved_at"),
        )

    def save(self, checkpoint: Checkpoint) -> None:
        safe_write_json(
            self.path,
            {"offset": checkpoint.byte_offset, "saved_at": utc_iso()},
        )


@dataclass
class RunStats:
    events_read: int = 0
    events_valid: int = 0
    flags_emitted: int = 0

    json_parse_failures: int = 0
    contract_failures: int = 0
    duplicate_failures: int = 0
    ordering_failures: int = 0

    last_event_time_iso: Optional[str] = None


# -----------------------------
# Reports
# -----------------------------
def build_validation_report(
    *,
    run_id: str,
    status: str,
    ended_at: str,
    contract_name: str,
    contract_version: str,
    paths: Dict[str, str],
    policy: Dict[str, str],
    checkpoint_path: str,
    checkpoint_offset: int,
    stats: RunStats,
    error: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "run_id": run_id,
        "ended_at": ended_at,
        "status": status,
        "contract": {"name": contract_name, "version": contract_version},
        "paths": paths,
        "policy": policy,
        "counts": {
            "events_read": stats.events_read,
            "events_valid": stats.events_valid,
            "flags_emitted": stats.flags_emitted,
            "json_parse_failures": stats.json_parse_failures,
            "contract_failures": stats.contract_failures,
            "duplicate_failures": stats.duplicate_failures,
            "ordering_failures": stats.ordering_failures,
        },
        "checkpoint": {"path": checkpoint_path, "offset": checkpoint_offset},
        "last_event_time": stats.last_event_time_iso,
    }
    if error:
        payload["error"] = error
    return payload


def build_run_log(
    *,
    run_id: str,
    started_at: str,
    ended_at: Optional[str],
    status: str,
    stream_path: str,
    flags_path: str,
    resume_offset: int,
    final_offset: Optional[int],
    contract_name: str,
    contract_version: str,
    stats: RunStats,
    error: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    # Intentionally small: "what happened" summary.
    payload: Dict[str, Any] = {
        "run_id": run_id,
        "started_at": started_at,
        "ended_at": ended_at,
        "status": status,
        "contract": {"name": contract_name, "version": contract_version},
        "paths": {"stream": stream_path, "flags": flags_path},
        "offsets": {"resume_offset": resume_offset, "final_offset": final_offset},
        "counts": {
            "events_read": stats.events_read,
            "events_valid": stats.events_valid,
            "flags_emitted": stats.flags_emitted,
        },
    }
    if error:
        payload["error"] = error
    return payload


# -----------------------------
# Watch loop
# -----------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Local streaming watcher: contract + policy + deterministic rules + flags + checkpoint"
    )
    parser.add_argument("--stream", default="data/stream.log", help="Append-only JSONL stream file")
    parser.add_argument("--checkpoint", default="outputs/checkpoints/state.json", help="Checkpoint JSON (byte offset)")
    parser.add_argument("--run-log-dir", default="logs", help="Directory for run logs")
    parser.add_argument("--report", default="outputs/reports/validation_report.json", help="Validation report path")
    parser.add_argument("--flags", default="outputs/flags/flags.log", help="Append-only JSONL flags output")
    parser.add_argument("--poll-ms", type=int, default=50, help="Poll interval when stream is idle (ms)")
    parser.add_argument("--checkpoint-every-seconds", type=int, default=2, help="Checkpoint interval (seconds)")
    args = parser.parse_args()

    ensure_dir_for(args.stream)
    ensure_dir_for(args.checkpoint)
    ensure_dir_for(args.report)
    ensure_dir_for(args.flags)
    os.makedirs(args.run_log_dir, exist_ok=True)

    run_id = uuid.uuid4().hex
    run_log_path = os.path.join(args.run_log_dir, f"run_{run_id}.json")

    contract_name, contract_version = contract_id()

    policy = {
        "ordering": "arrival-order; event_time must be non-decreasing (fail-fast if violated)",
        "duplicates": "fail-fast on duplicate event_id within a watcher run",
    }

    # Resume state (provable)
    ckpt_store = CheckpointStore(args.checkpoint)
    ckpt = ckpt_store.load()
    initial_offset = ckpt.byte_offset

    stats = RunStats()

    # Rule engine state for this run
    rules_cfg = RuleConfig()
    rules_state = WatcherState()

    # v1 non-support boundaries
    seen_event_ids: set[str] = set()
    last_event_time: Optional[datetime] = None

    started_at = utc_iso()
    paths = {"stream": args.stream, "flags": args.flags, "checkpoint": args.checkpoint}

    # Write an initial "running" log (useful if the process dies mid-run)
    safe_write_json(
        run_log_path,
        build_run_log(
            run_id=run_id,
            started_at=started_at,
            ended_at=None,
            status="running",
            stream_path=args.stream,
            flags_path=args.flags,
            resume_offset=initial_offset,
            final_offset=None,
            contract_name=contract_name,
            contract_version=contract_version,
            stats=stats,
        ),
    )

    last_checkpoint_wall = time.time()

    with open(args.flags, "a", encoding="utf-8") as flags_f:
        # Byte offsets require binary mode.
        with open(args.stream, "rb") as stream_f:
            stream_f.seek(initial_offset)

            try:
                while True:
                    line_start = stream_f.tell()
                    raw = stream_f.readline()

                    if not raw:
                        time.sleep(max(args.poll_ms, 1) / 1000.0)
                        continue

                    # Don't consume partial producer writes.
                    if not raw.endswith(b"\n"):
                        stream_f.seek(line_start)
                        time.sleep(max(args.poll_ms, 1) / 1000.0)
                        continue

                    stats.events_read += 1

                    # Parse JSON (fail-fast)
                    try:
                        event = json.loads(raw.decode("utf-8"))
                    except Exception:
                        stats.json_parse_failures += 1
                        raise ValueError("invalid JSON line encountered; failing fast")

                    # Contract validation (fail-fast)
                    try:
                        validate_event(event)
                        stats.events_valid += 1
                    except Exception:
                        stats.contract_failures += 1
                        raise

                    # Duplicate policy (fail-fast)
                    eid = event["event_id"]
                    if eid in seen_event_ids:
                        stats.duplicate_failures += 1
                        raise ValueError(f"duplicate event_id encountered (not supported in v1): {eid}")
                    seen_event_ids.add(eid)

                    # Ordering policy (fail-fast)
                    et = parse_event_time(event)
                    if last_event_time is not None and et < last_event_time:
                        stats.ordering_failures += 1
                        raise ValueError(
                            f"out-of-order event_time encountered (not supported in v1): "
                            f"{event['event_time']} < {last_event_time.isoformat()}"
                        )
                    last_event_time = et
                    stats.last_event_time_iso = event["event_time"]

                    # Rules -> flags
                    flag_events = evaluate_rules(event, rules_state, rules_cfg)
                    for flg in flag_events:
                        flags_f.write(json.dumps(flg) + "\n")
                        stats.flags_emitted += 1
                    if flag_events:
                        flags_f.flush()

                    # "No skip" proof: offset always advances to end-of-line.
                    ckpt.byte_offset = stream_f.tell()

                    # Periodic checkpoint for restart/resume proof.
                    now_wall = time.time()
                    if now_wall - last_checkpoint_wall >= max(args.checkpoint_every_seconds, 1):
                        ckpt_store.save(ckpt)
                        last_checkpoint_wall = now_wall

            except KeyboardInterrupt:
                pass
            except Exception as e:
                # Fail-fast: checkpoint + evidence, then re-raise.
                ckpt_store.save(ckpt)
                ended_at = utc_iso()
                err = {"type": type(e).__name__, "message": str(e)}

                safe_write_json(
                    args.report,
                    build_validation_report(
                        run_id=run_id,
                        status="failed",
                        ended_at=ended_at,
                        contract_name=contract_name,
                        contract_version=contract_version,
                        paths=paths,
                        policy=policy,
                        checkpoint_path=args.checkpoint,
                        checkpoint_offset=ckpt.byte_offset,
                        stats=stats,
                        error=err,
                    ),
                )

                safe_write_json(
                    run_log_path,
                    build_run_log(
                        run_id=run_id,
                        started_at=started_at,
                        ended_at=ended_at,
                        status="failed",
                        stream_path=args.stream,
                        flags_path=args.flags,
                        resume_offset=initial_offset,
                        final_offset=ckpt.byte_offset,
                        contract_name=contract_name,
                        contract_version=contract_version,
                        stats=stats,
                        error=err,
                    ),
                )
                raise

    # Success path: checkpoint + evidence.
    ckpt_store.save(ckpt)
    ended_at = utc_iso()

    safe_write_json(
        args.report,
        build_validation_report(
            run_id=run_id,
            status="success",
            ended_at=ended_at,
            contract_name=contract_name,
            contract_version=contract_version,
            paths=paths,
            policy=policy,
            checkpoint_path=args.checkpoint,
            checkpoint_offset=ckpt.byte_offset,
            stats=stats,
        ),
    )

    safe_write_json(
        run_log_path,
        build_run_log(
            run_id=run_id,
            started_at=started_at,
            ended_at=ended_at,
            status="success",
            stream_path=args.stream,
            flags_path=args.flags,
            resume_offset=initial_offset,
            final_offset=ckpt.byte_offset,
            contract_name=contract_name,
            contract_version=contract_version,
            stats=stats,
        ),
    )

    print(
        f"Watcher stopped. run_id={run_id} events_read={stats.events_read} "
        f"flags_emitted={stats.flags_emitted} final_offset={ckpt.byte_offset}"
    )


if __name__ == "__main__":
    main()
