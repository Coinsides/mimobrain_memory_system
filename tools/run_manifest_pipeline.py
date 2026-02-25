"""Run a full manifest sync pipeline (report -> tasks -> execute) into a local authoritative run dir.

This is the end-to-end glue for P0-7 (analysis + planning + conservative execution).

Authoritative outputs live outside the repo (data root), under:
  <DATA_ROOT>\\runs\\sync\\RUN-<timestamp>\\

This script:
- analyze_sync(kind, base, incoming) -> sync_report.<kind>.json
- tasks_from_report(report) -> tasks.<kind>.jsonl
- execute tasks -> task_results.<kind>.jsonl (and any patch plans)
- write run_manifest.json with sha256 fingerprints

It keeps execution conservative:
- VERIFY_MANIFEST uses provided --vault-root mappings
- SYNC_MANIFEST_APPLY defaults to dry-run unless --apply

Usage:
  python tools/run_manifest_pipeline.py --kind raw --base base.jsonl --incoming incoming.jsonl --vault-root default=C:/.../default

Options:
  --apply   Actually append safe new records (still append-only)
  --runs-root <dir>
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from tools.manifest_executor import ExecContext, exec_task
from tools.manifest_sync import analyze_sync
from tools.manifest_sync_tasks import tasks_from_report


# No hardcoded runs root; pass --runs-root or provide --config.


def now_run_id() -> str:
    return datetime.now(timezone.utc).strftime("RUN-%Y%m%d-%H%M%S")


def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return "sha256:" + h.hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()


def write_json(path: Path, obj: dict) -> str:
    data = (json.dumps(obj, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    path.write_bytes(data)
    return sha256_bytes(data)


def write_jsonl(path: Path, objs: list[dict]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = "".join(json.dumps(o, ensure_ascii=False) + "\n" for o in objs).encode(
        "utf-8"
    )
    path.write_bytes(data)
    return sha256_bytes(data)


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--kind", required=True, choices=["raw", "mu", "asset"])
    p.add_argument("--base", required=True)
    p.add_argument("--incoming", required=True)
    p.add_argument(
        "--vault-root",
        action="append",
        default=[],
        help="Vault root mapping like default=C:/.../vaults/default (repeatable)",
    )
    p.add_argument(
        "--config",
        default=None,
        help="Path to ms_config.json (optional; can provide runs_root_sync and vault_roots)",
    )
    p.add_argument(
        "--runs-root",
        default=None,
        help="Authoritative runs root. If omitted, tries to use config.runs_root_sync.",
    )
    p.add_argument("--apply", action="store_true")
    ns = p.parse_args(argv)

    runs_root: str | None = ns.runs_root

    vault_roots: dict[str, str] = {}
    if ns.config:
        from tools.ms_config import load_config

        cfg = load_config(ns.config)
        runs_root = runs_root or cfg.get("runs_root_sync")
        vr = cfg.get("vault_roots")
        if isinstance(vr, dict):
            vault_roots.update({str(k): str(v) for k, v in vr.items()})

    if not runs_root:
        raise SystemExit(
            "missing runs root: pass --runs-root or provide --config with runs_root_sync"
        )

    run_id = now_run_id()
    run_dir = Path(runs_root) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    for item in ns.vault_root:
        if "=" not in item:
            raise SystemExit(f"invalid --vault-root {item!r} (expected vault_id=path)")
        k, v = item.split("=", 1)
        vault_roots[k] = v

    # 1) report
    report = analyze_sync(kind=ns.kind, base_path=ns.base, incoming_path=ns.incoming)
    report_path = run_dir / f"sync_report.{ns.kind}.json"
    report_sha = write_json(report_path, report)

    # 2) tasks (force dry-run unless --apply)
    tasks = tasks_from_report(report)
    patch_plans_dir = run_dir / "patch_plans"
    patch_plans_dir.mkdir(parents=True, exist_ok=True)
    if not ns.apply:
        for t in tasks:
            if isinstance(t, dict) and t.get("type") == "SYNC_MANIFEST_APPLY":
                params = t.get("params")
                if isinstance(params, dict):
                    params["dry_run"] = True
                    params["out_dir"] = str(patch_plans_dir)
    else:
        for t in tasks:
            if isinstance(t, dict) and t.get("type") == "SYNC_MANIFEST_APPLY":
                params = t.get("params")
                if isinstance(params, dict):
                    params["out_dir"] = str(patch_plans_dir)
    tasks_path = run_dir / f"tasks.{ns.kind}.jsonl"
    tasks_sha = write_jsonl(tasks_path, tasks)

    # 3) execute
    ctx = ExecContext(vault_roots=vault_roots)
    results: list[dict] = []

    # Task journal (append-only)
    journal_db = run_dir / "task_journal.sqlite"
    journal_ctx = {
        "vault_roots": vault_roots,
        "run_id": run_id,
        "run_dir": str(run_dir),
    }
    try:
        from tools.task_journal import append_task
    except Exception:
        append_task = None  # type: ignore

    # structured logs
    try:
        from tools.logger import default_log_path, log_event

        run_log = default_log_path("pipeline")
        log_event(
            event="RUN_START",
            log_path=run_log,
            run_id=run_id,
            run_dir=str(run_dir),
            tool="run_manifest_pipeline",
        )
    except Exception:
        log_event = None  # type: ignore
        run_log = None  # type: ignore

    for t in tasks:
        if not isinstance(t, dict):
            continue
        r = exec_task(t, ctx)
        results.append(r)

        if append_task is not None:
            try:
                append_task(journal_db, t, r, context=journal_ctx)
            except Exception:
                # journal failure should not break execution
                pass

        if log_event is not None:
            try:
                log_event(
                    event="TASK_EXEC",
                    log_path=run_log,
                    run_id=run_id,
                    run_dir=str(run_dir),
                    task_id=r.get("task_id"),
                    inputs=[
                        {
                            "kind": "TASK",
                            "type": t.get("type"),
                            "idempotency_key": t.get("idempotency_key"),
                        }
                    ],
                    outputs=r.get("outputs"),
                    stats={"elapsed_ms": r.get("elapsed_ms")},
                    diagnostics={"status": r.get("status")},
                )
            except Exception:
                pass

    results_path = run_dir / f"task_results.{ns.kind}.jsonl"
    results_sha = write_jsonl(results_path, results)

    # 4) run manifest
    # best-effort git head
    git_head = None
    try:
        import subprocess

        git_head = (
            subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=str(Path(__file__).resolve().parents[1]),
            )
            .decode("utf-8")
            .strip()
        )
    except Exception:
        git_head = None

    run_manifest = {
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "tool": "manifest_pipeline",
        "kind": ns.kind,
        "tooling": {
            "repo": "mimobrain_memory_system",
            "git_head": git_head,
        },
        "inputs": {
            "base_path": ns.base,
            "incoming_path": ns.incoming,
            "base_sha256": sha256_file(Path(ns.base))
            if Path(ns.base).exists()
            else None,
            "incoming_sha256": sha256_file(Path(ns.incoming))
            if Path(ns.incoming).exists()
            else None,
            "vault_roots": vault_roots,
        },
        "outputs": {
            "report_path": str(report_path),
            "report_sha256": report_sha,
            "tasks_path": str(tasks_path),
            "tasks_sha256": tasks_sha,
            "results_path": str(results_path),
            "results_sha256": results_sha,
        },
        "notes": {
            "authoritative": True,
            "apply": bool(ns.apply),
            "repo_examples": "regenerate via tools/emit_example_manifest_sync.py",
        },
    }
    write_json(run_dir / "run_manifest.json", run_manifest)

    # log run end
    if log_event is not None:
        try:
            log_event(
                event="RUN_END",
                log_path=run_log,
                run_id=run_id,
                run_dir=str(run_dir),
                tool="run_manifest_pipeline",
                stats={"tasks": len([t for t in tasks if isinstance(t, dict)])},
            )
        except Exception:
            pass

    print(str(run_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
