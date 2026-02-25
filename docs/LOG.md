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

## 2026-02-22
- Incident: PR branches blocked by merge conflicts (docs/LOG.md, plus reported build_bundle-related files). Resolved by merging latest main into feature branches locally, removing conflict markers in docs/LOG.md, committing the merge, and pushing updated branches so GitHub could complete the merge.
- P1-G / TASK-INGEST-001: add minimal vault ingest tool (`tools/vault_ingest.py`) + CLI wrapper (`tools/ms_ingest.py`) + tests.
- P1-G / TASK-MIGRATE-001: add pointer migration tool (`tools/pointer_migrate.py`) + CLI wrapper (`tools/ms_migrate_pointers.py`) + tests (append-only via links.supersedes).
- P1-G / TASK-RESOLVE-001: add pointer resolve tool (`tools/pointer_resolve.py`) + CLI wrapper (`tools/ms_resolve_pointer.py`) + tests (sha256 verify + line_range snippet).
- P1-G / degraded propagation (v0.1): wire pointer resolve into build_bundle evidence_depth=raw_quotes (best-effort); record bundle.diagnostics.evidence_degraded when snapshot exists but pointer resolve fails.
- P1-G / repair trigger (v0.1): when degraded, emit bundle.diagnostics.repair_tasks[] entries (type=REPAIR_POINTER) for follow-up orchestration.
- P1-G / task emission (v0.1): add tool to convert bundle.diagnostics.repair_tasks -> TaskSpec files (`tools/emit_repair_tasks.py`) + wrapper (`tools/ms_emit_repair_tasks.py`) + tests.
- P1-G / run_dir closure (v0.1): add bundle repair pipeline runner (`tools/run_bundle_repair_pipeline.py`) and repair executor (`tools/repair_executor.py`) to write authoritative run_dir artifacts (bundle/tasks/task_results/run_manifest).
- P1-G / auto-fix (v0.1): repair_executor can optionally write superseding MU files with migrated pointer URIs into run_dir/fixed_mu (append-only via links.supersedes).
- P1-G / ingest fixed MU (v0.1): add MU ingest tool (`tools/vault_ingest_mu.py` + `tools/ms_ingest_mu.py`) and wire run_bundle_repair_pipeline to ingest run_dir/fixed_mu into vault + append mu_manifest.
- P1-G / index refresh (v0.1): extend run_bundle_repair_pipeline with optional --index-db to rebuild/update meta.sqlite from vault/mu after auto-fix.
- P1 config (v0.1): add shared ms_config.json loader (`tools/ms_config.py`) + schema; wire build_bundle, run_bundle_repair_pipeline, and golden_run to accept --config.
- P1 golden policy (v0.1): simplest gate — if bundle diagnostics indicates evidence_degraded=true, Golden marks the question FAIL; raw_quotes requires snippet presence.

## 2026-02-25
- CI: fix GitHub Actions workflow YAML parsing (quote step names containing colons; workflow now actually runs instead of failing instantly).
- CI: ruff lint/format hygiene pass + auto-format; tests remain green.
