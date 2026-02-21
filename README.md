# mimobrain_memory_system

This is the main repository for building **MimoBrain** — a multi-agent, evidence-backed personal memory system.

The long-term goal is to make memory **portable, auditable, and rebuildable**:
- Facts live in **Raw / MU (.mimo) / Assets**.
- Everything else (index/graph/views/bundles) is **derived cache** and must be rebuildable.

## Repository status
Early-stage scaffolding. Expect rapid iteration.

## Quick start (dev)

### Requirements
- Python 3.11+ recommended (works with 3.10+)

### Install
```bash
# Windows (recommended launcher):
py -m pip install -U pip
py -m pip install -r requirements.txt -r requirements-dev.txt

# macOS/Linux:
python -m pip install -U pip
python -m pip install -r requirements.txt -r requirements-dev.txt
```

### Run checks (must stay green)
```bash
# Tests
py -m pytest -q

# Lint
py -m ruff check .

# Format check
py -m ruff format --check .
```

> Note (Windows): prefer `py ...` instead of `python ...` if `python.exe` is blocked by system policy.

## Structure
- `src/` — main library code (storage/index/views/bundles/orchestrator)
- `tools/` — developer tools and small utilities
- `docs/contracts/` — stable data contracts (schemas, invariants)
- `docs/adr/` — Architecture Decision Records (design decisions)
- `tests/` — unit/integration tests and golden regressions (later)
- `logs/` — repo-safe journals (no private data)

## Logging discipline
This repo enforces a simple rule: if you change code/specs, you must update a repo-safe journal:
- `docs/LOG.md` or `logs/task_journal.jsonl`

See `tools/require_journal.py`.
