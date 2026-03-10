import json
import signal
import subprocess
import sys
import time
from pathlib import Path


def test_smoke_pipeline(tmp_path):
    stream = tmp_path / "stream.log"
    checkpoint = tmp_path / "state.json"
    flags = tmp_path / "flags.log"
    report = tmp_path / "report.json"
    logs_dir = tmp_path / "logs"

    logs_dir.mkdir()

    event = {
        "event_id": "e1",
        "user_id": "user_00001",
        "event_time": "2026-01-01T00:00:00+00:00",
        "ingest_time": "2026-01-01T00:00:00+00:00",
        "event_type": "deposit_completed",
        "amount": 100,
        "currency": "GBP",
        "channel": "web",
    }

    with open(stream, "w", encoding="utf-8") as f:
        f.write(json.dumps(event) + "\n")

    repo_root = Path(__file__).resolve().parents[1]

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "src.jobs.watcher",
            "--stream",
            str(stream),
            "--checkpoint",
            str(checkpoint),
            "--flags",
            str(flags),
            "--report",
            str(report),
            "--run-log-dir",
            str(logs_dir),
            "--poll-ms",
            "10",
            "--checkpoint-every-seconds",
            "1",
        ],
        cwd=repo_root,
    )

    time.sleep(0.5)
    proc.send_signal(signal.SIGINT)
    proc.wait(timeout=5)

    assert checkpoint.exists()
    assert flags.exists()
    assert report.exists()

    run_logs = list(logs_dir.glob("run_*.json"))
    assert len(run_logs) == 1