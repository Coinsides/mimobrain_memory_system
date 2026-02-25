"""Run a bundle repair pipeline into an authoritative run dir (P1-G).

Pipeline:
- build_bundle(... evidence_depth=raw_quotes) -> bundle.json
- emit_repair_tasks(bundle.json) -> tasks/*.task_spec.json
- execute tasks via repair_executor -> task_results.jsonl
- write run_manifest.json

Authoritative outputs live outside the repo (data root), under:
  <DATA_ROOT>\\runs\\repair\\RUN-<timestamp>\\

v0.1 scope:
- Only REPAIR_POINTER suggestions, no MU rewrite.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from tools.repair_executor import ExecContext, exec_task

# No hardcoded runs root; pass --runs-root or provide --config.


def now_run_id() -> str:
    return datetime.now(timezone.utc).strftime("RUN-%Y%m%d-%H%M%S")


def sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return "sha256:" + h.hexdigest()


def write_json(path: Path, obj: dict) -> str:
    data = (json.dumps(obj, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return sha256_bytes(data)


def write_jsonl(path: Path, objs: list[dict]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = "".join(json.dumps(o, ensure_ascii=False) + "\n" for o in objs).encode(
        "utf-8"
    )
    path.write_bytes(data)
    return sha256_bytes(data)


def iter_task_specs(tasks_dir: Path) -> list[dict]:
    out: list[dict] = []
    for p in sorted(tasks_dir.glob("*.task_spec.json")):
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(obj, dict):
                out.append(obj)
        except Exception:
            continue
    return out


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="meta.sqlite")
    ap.add_argument("--config", default=None, help="Path to ms_config.json (optional)")
    ap.add_argument(
        "--runs-root",
        default=None,
        help="Authoritative runs root (recommended). If omitted, tries to use config.runs_root_repair.",
    )
    ap.add_argument("--query", required=True)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--template", default="time_overview_v1")
    ap.add_argument(
        "--target-level", default="private", choices=["private", "org", "public"]
    )
    ap.add_argument(
        "--vault-root",
        action="append",
        default=[],
        help="Vault root mapping like default=C:/.../vaults/default (repeatable)",
    )
    ap.add_argument("--raw-manifest", default=None, help="raw_manifest.jsonl path")
    ap.add_argument(
        "--index-db",
        default=None,
        help="Optional meta.sqlite path to re-index vault/mu after fixes",
    )
    ap.add_argument(
        "--index-reset",
        action="store_true",
        help="If set with --index-db, reset db before indexing",
    )
    ns = ap.parse_args(argv)

    vault_roots: dict[str, str] = {}
    raw_manifest = ns.raw_manifest
    runs_root: str | None = ns.runs_root

    if ns.config:
        from tools.ms_config import load_config

        cfg = load_config(ns.config)
        vr = cfg.get("vault_roots")
        if isinstance(vr, dict):
            vault_roots.update({str(k): str(v) for k, v in vr.items()})
        raw_manifest = cfg.get("raw_manifest_path") or raw_manifest
        runs_root = runs_root or cfg.get("runs_root_repair")

    if not runs_root:
        raise SystemExit(
            "missing runs root: pass --runs-root or provide --config with runs_root_repair"
        )

    run_id = now_run_id()
    run_dir = Path(runs_root) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    for item in ns.vault_root:
        if "=" not in item:
            raise SystemExit(f"invalid --vault-root {item!r} (expected vault_id=path)")
        k, v = item.split("=", 1)
        vault_roots[k] = v

    # 1) build bundle
    from tools.build_bundle import build_bundle

    bundle = build_bundle(
        db_path=Path(ns.db),
        query=ns.query,
        days=int(ns.days),
        template=str(ns.template),
        target_level=ns.target_level,
        evidence_depth="raw_quotes",
        vault_roots=vault_roots or None,
        raw_manifest_path=(Path(raw_manifest) if raw_manifest else None),
        include_diagnostics=True,
    )
    bundle_path = run_dir / "bundle.json"
    bundle_sha = write_json(bundle_path, bundle)

    # 2) emit tasks
    from tools.emit_repair_tasks import emit_repair_tasks

    tasks_dir = run_dir / "tasks"
    emit_summary = emit_repair_tasks(bundle_path, out_dir=tasks_dir)

    tasks = iter_task_specs(tasks_dir)
    tasks_sha = write_jsonl(run_dir / "tasks.jsonl", tasks)

    # 3) execute
    fixed_mu_dir = run_dir / "fixed_mu"
    ctx = ExecContext(
        vault_roots=vault_roots,
        raw_manifest_path=str(raw_manifest) if raw_manifest else None,
        out_mu_dir=str(fixed_mu_dir),
    )
    results: list[dict] = []

    # journal
    journal_db = run_dir / "task_journal.sqlite"
    journal_ctx = {
        "vault_roots": vault_roots,
        "raw_manifest": ns.raw_manifest,
        "run_id": run_id,
        "run_dir": str(run_dir),
    }
    try:
        from tools.task_journal import append_task
    except Exception:
        append_task = None  # type: ignore

    for t in tasks:
        r = exec_task(t, ctx)
        results.append(r)
        if append_task is not None:
            try:
                append_task(journal_db, t, r, context=journal_ctx)
            except Exception:
                pass

    results_sha = write_jsonl(run_dir / "task_results.jsonl", results)

    # 4) ingest fixed MU into vault + update index (optional)
    mu_manifest_path = None
    if vault_roots.get("default") and fixed_mu_dir.exists():
        try:
            from tools.vault_ingest_mu import ingest_mu_file

            for mu_file in sorted(fixed_mu_dir.rglob("*.mimo")):
                ingest_mu_file(
                    mu_file, vault_root=vault_roots["default"], vault_id="default"
                )
            mu_manifest_path = str(
                Path(vault_roots["default"]) / "manifests" / "mu_manifest.jsonl"
            )
        except Exception:
            mu_manifest_path = None

    # 5) update index (optional)
    index_out = None
    if ns.index_db and vault_roots.get("default"):
        try:
            from tools.index_mu import index_mu_dir

            mu_root = Path(vault_roots["default"]) / "mu"
            index_out = index_mu_dir(
                mu_root, Path(ns.index_db), reset=bool(ns.index_reset)
            )
        except Exception:
            index_out = None

    # 6) run manifest
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
        "tool": "bundle_repair_pipeline",
        "tooling": {"repo": "mimobrain_memory_system", "git_head": git_head},
        "inputs": {
            "db": ns.db,
            "query": ns.query,
            "days": int(ns.days),
            "template": ns.template,
            "target_level": ns.target_level,
            "vault_roots": vault_roots,
            "raw_manifest": raw_manifest,
            "config": ns.config,
        },
        "outputs": {
            "bundle_path": str(bundle_path),
            "bundle_sha256": bundle_sha,
            "tasks_dir": str(tasks_dir),
            "tasks_sha256": tasks_sha,
            "results_path": str(run_dir / "task_results.jsonl"),
            "results_sha256": results_sha,
            "fixed_mu_dir": str(fixed_mu_dir),
            "mu_manifest_path": mu_manifest_path,
            "index_db": ns.index_db,
            "index_out": index_out,
        },
        "notes": {
            "authoritative": True,
            "v0_1": True,
            "emit_tasks_wrote": emit_summary.wrote,
        },
    }
    write_json(run_dir / "run_manifest.json", run_manifest)

    print(str(run_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
