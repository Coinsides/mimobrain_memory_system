# LOG — Engineering Milestones

> Human-readable milestone log (repo-safe).
> Keep it short. No private data, no local paths.

## 2026-02-21
- Add logging infrastructure (repo journal + gates) and CI checks (pytest/ruff).
- P0-C: add TaskSpec/TaskResult protocol (v0.1) JSON Schemas + validator + examples + tests.
- P0-H: add Vault URI v0.1 + manifest line schemas (raw/mu/asset) + doctor/verify/repair helpers + tests.
- P0-F / P0-7A: add manifest sync analysis tool + machine-first sync report schema + tests.
- P0-F / P0-7B: add report→TaskSpec generator, authoritative local run emitter, repo-safe examples + CI gate.
- P0-F / P0-7C: add conservative append-only apply planner (plan+optional apply) + patch plan schema + tests.
- P0-F / P0-7C: add minimal task executor (TaskSpec→TaskResult) for verify/repair/apply (default dry-run) + tests.
- P0-F / P0-7C: add end-to-end pipeline runner (report→tasks→execute) that writes authoritative run artifacts + tests.
- P0-F / P0-7C: route patch plans into run_dir/patch_plans (avoid writing next to base manifest).
- P0-G / P0-8: add MU export tool (redaction + share_policy enforcement) + tests.
- P0-G / P0-8: centralize privacy defaults/share policy (tools/privacy_policy.py) + tests.
- P0-G / P0-8: add bundle export redaction (tools/export_bundle.py) + tests.
- P0-G / P0-8: add ms_export CLI wrapper (ms export --target-level ...) + tests.
- P0-G / P0-8: add bundle schema contract + validator (bundle_v0_1) and hook into export.
- P0-C / P0-9: add task journal (sqlite) + pipeline integration.
- P0-C / P0-9: persist ExecContext (vault_roots) into task journal for replay.
- P0-C / P0-9: extend journal context (run_id/run_dir) and annotate replay provenance.
- P0-C / P0-9: optional journaling for ms_export (single summary record).
- P0-C / P0-9: optional journaling for ms_doctor/verify/repair (single summary record).
- P0-D / P0-10: add Golden Set (20 questions) + schema + validator + runner/report.
- P0-D / P0-10: add report contract + SKIP mode + hard-fail path leakage gates.
- P0-K / P0-11: add structured jsonl logger + log_event schema; integrate into pipeline + ms_export.
- P0-K / P0-11: add structured logs integration for ms_doctor (summary-only).
- P1-A: add meta.sqlite schema + index_mu tool (full rebuild) + tests.
- P1-B: add search_mu (FTS + filters + reason) + tests.
- P1-D: add view cache table + minimal invalidate_by_mu_ids.
- P1-E: add minimal bundle builder (build_bundle) aligned with bundle_v0_1.
- P1-E: add evidence_depth=mu_snippets option (bundle evidence includes snippet) + bundle contract update.
- P1: connect build_bundle into golden_run (optional --db) to exercise evidence plumbing.
