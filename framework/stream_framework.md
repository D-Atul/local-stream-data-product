# Compliance Streaming Data Product Framework — Pre-Development

## Purpose
To define the **rules of the system before it is built**, ensuring:
- Deterministic behavior by design
- Contract-first ingestion
- Auditable failure handling
- Clear separation between execution and evidence

This framework intentionally does **not** describe implementation details.

---

## Status
**Pre-Development Framework (Rulebook)**  
This document defines **requirements and proof expectations only**.  
No claims in this document assert completed execution.

---

## Scope
Included:
- Local streaming data pipeline (single source, append-only events)
- Continuous watcher with checkpointed progress
- Validation, flag emission, and audit expectations
- Evidence files to be produced post-development

Explicitly excluded:
- Kafka / cloud infrastructure
- Distributed scaling
- SLA or latency guarantees
- External orchestration
- Performance optimisation
- Streaming analytics or alerting systems

---

## Success Definition & Failure Modes (NON-NEGOTIABLE)

### Success Metrics (must be provable)
- No persisted event is skipped
- Deterministic outputs given identical inputs and configuration
- Contract enforcement with zero tolerated violations
- Flag events emitted deterministically for rule breaches
- Restart resumes from last checkpointed offset

### Explicit Failure Modes
- Contract violation (schema, type, domain) → fail-fast, no flags emitted
- Ordering or duplicate policy violation → fail-fast
- I/O or write failure → fail-fast, non-zero exit
- Any unexpected exception → fail-fast, non-zero exit

---

## Design Principles
1. **No implicit guarantees** — guarantees apply only to persisted events
2. **Fail fast** — invalid events do not enter state or rule evaluation
3. **Determinism is defined, not assumed**
4. **Evidence is mandatory; narrative is insufficient**
5. **Notebook is review-only, never execution**

---

## Core Requirements and Evidence Mapping

### 1. Ingestion Contract Enforcement
**Requirement**  
All persisted events must conform to a predefined schema and domain contract before state updates or rule evaluation.

**Evidence Files**
- `src/contracts/event_contract.py`
- `outputs/reports/validation_report.json`

**Pass / Fail Criteria**
- PASS: all required fields present, types valid, domain rules satisfied  
- FAIL: any violation → watcher exits non-zero, no flags emitted

---

### 2. Deterministic Processing
**Requirement**  
Given identical persisted inputs and configuration, the watcher must emit identical flag outputs.

**Evidence Files**
- `logs/run_<run_id>.json`
- Input stream slice hash
- Flag output hash

**Pass / Fail Criteria**
- PASS: identical hashes across repeated runs  
- FAIL: any hash mismatch

---

### 3. Flag Emission Contract
**Requirement**  
When a rule is breached, the system emits a **flag event**.

**Flag Fields (required)**
- user_id
- flag_id 
- rule_name  
- metric_value  
- threshold  
- window  
- event_id  
- event_time  
- ingest_time  

**Evidence Files**
- `outputs/flags/flags.log` (append-only, JSONL)

**Pass / Fail Criteria**
- PASS: flags conform to contract and match rule conditions  
- FAIL: missing fields or inconsistent values

---

### 4. Failure Handling
**Requirement**  
Any validation or processing failure must:
- Stop execution immediately
- Prevent flag emission for the failing event and post the failure
- Preserve prior outputs

**Evidence Files**
- `logs/run_<run_id>.json`
- Non-zero process exit code

**Pass / Fail Criteria**
- PASS: failure recorded, no new flags written  
- FAIL: silent failure or partial output

---

### 5. Observability & Traceability
**Requirement**  
Each watcher run must be traceable.

**Evidence Files**
- `logs/run_<run_id>.json` containing:
  - run_id, started_at, ended_at, status
  - contract name/version
  - paths (stream, flags)
  - offsets (resume_offset, final_offset)
  - counts (events_read, events_valid, flags_emitted)
  - error (if failed)

- `outputs/reports/validation_report.json` containing:
  - policy (ordering, duplicates)
  - full failure counters (json_parse_failures, contract_failures, etc.)
  - checkpoint offset + last_event_time

**Pass / Fail Criteria**
- PASS: all fields populated  
- FAIL: missing or incomplete metadata

---

### 6. Event-Time & Ordering Policy
**Requirement**  
The watcher enforces **arrival-order processing**.

- Out-of-order `event_time` values are **not supported**
- Ordering violations cause failure

**Evidence Files**
- `outputs/reports/validation_report.json` (ordering checks)

**Pass / Fail Criteria**
- PASS: no ordering violations detected  
- FAIL: any out-of-order event detected

---

### 7. Idempotency & Duplicate Policy
**Requirement**  
Duplicate events (by `event_id`) are **not supported** in v1.

**Evidence Files**
- `outputs/reports/validation_report.json` (duplicate checks)

**Pass / Fail Criteria**
- PASS: all event_ids unique  
- FAIL: any duplicate detected

---

### 8. Replay & Restart Guarantees
**Requirement**  
The watcher is **continuous** with checkpointed progress.

Guarantees:
- No loss is guaranteed only for events already persisted to the local stream
- Restart resumes from the last checkpointed offset
- Replay uses a captured stream slice for deterministic verification
- Watcher runs until interrupted; continuous here means it polls and resumes, not an SLA

**Evidence Files**
- `logs/run_<run_id>.json`
- `outputs/checkpoints/state.json`

**Pass / Fail Criteria**
- PASS: restart resumes correctly without skipping persisted events  
- FAIL: offset regression or event loss

---

## Notebook Role
The notebook is **not** part of execution.

Notebook responsibilities:
- Load logs, flags, validation reports
- Verify determinism and rule behavior
- Review restart and replay outcomes

Notebook must not:
- Execute watcher logic
- Mutate state
- Describe hypothetical behavior

---

## Versioning Rules
- This document is **v1.0 (Pre-Development)** and frozen
- Any change requires version bump (v1.1+)
- Evidence is produced only by execution

---

## Evaluation Standard
This framework is considered **complete** when:
- Every requirement has corresponding evidence
- All guarantees are provable via outputs and logs
- No guarantees exceed what is explicitly stated
