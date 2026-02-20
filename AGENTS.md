# AGENTS.md (Memory System / MU-MIMO)
## Mission
Build and maintain a MU (.mimo) based personal memory system.
North star: 10-year maintainability, reproducibility, and evidence-backed outputs.
## Invariants (do not break)
- Raw is immutable and is the single source of truth.
- All processing happens on MU (.mimo) units (Design layer never edits Raw).
- Derived artifacts (index / graph / views / bundles) are rebuildable caches. Do not treat
caches as truth.
- Every generated view or bundle MUST include Evidence: `source_mu_ids`.
## Privacy & repo safety
- Never add real private vault data to the git repo.
- Use only anonymized samples under `data/` for tests and demos.
- Do not print secrets or tokens. Do not introduce telemetry.
## Change discipline
- Prefer small PRs (<= 10 files or <= 400 LOC) unless asked.
- One PR = one purpose. Avoid drive-by refactors.
- If you change schemas, update docs + add a migration note.
## Commands (keep these working)
- Tests: python -m pytest -q
- Lint: python -m ruff check .
- Format: python -m ruff format .
## Project structure (expected)
- tools/ CLI scripts (mimo-pack / mimo-validate / mimo-extract / mimo-read)
- src/memory_system/ core library (storage/index/views/bundle/orchestrator)
- docs/ specs, schemas, ADRs (design decisions)
- tests/ unit + integration + golden regressions
## Output expectations
- Prefer deterministic outputs for non-LLM steps.
- When using an LLM, keep prompts minimal and record parameters in logs.
- Always return a short summary + what changed + how to validate.
