# Compliance Streaming — Local Stateful Watcher

## Overview
Events are produced and persisted to a local **append-only JSONL stream**. A watcher consumes only persisted events, validates them against a contract, maintains per-user rolling state, evaluates deterministic rules, and emits **flag events** when rules are breached. The watcher is **restart-safe** via checkpointed byte offsets and is designed to be **deterministic and auditable**.

## What this demonstrates
- Streaming-style ingestion (append-only log)
- Stateful processing (per-user rolling windows)
- Deterministic, explainable rule evaluation
- Fail-fast validation / policy boundaries
- Restart + resume using checkpoints
- Separation of execution (code) and evidence (notebook)

## What this is not
Not Kafka/Flink/Spark, not distributed, not SLA/latency focused, not an alerting/enforcement system.

## Rules implemented
- Threshold
- Velocity (deposits only)
- Repetition

## Guarantees (scope)
- No persisted event is skipped
- Invalid events fail fast before rule evaluation
- Flags are emitted deterministically
- Restart resumes from the last checkpointed offset  
Boundary: events not yet fully written to the stream are out of scope.

## Repo layout
- `src/` — generator, watcher, rules, contract
- `framework/` — frozen pre-dev framework
- `notebook.ipynb` — evidence-only review
- `logs/` — run logs (success + controlled failure)
- `data/` — small sample stream  
Generated at runtime (not committed): `outputs/checkpoints/`, `outputs/flags/`, `outputs/reports/`

## How to run
Start the event generator:
`python -m src.generator`

Start the watcher:
`python -m src.watcher`

Stop with Ctrl+C. Restarting the watcher resumes from the last checkpoint.

## Evidence
See `notebook.ipynb` for read-only inspection of run logs, validation reports, checkpoint state, and emitted flags.