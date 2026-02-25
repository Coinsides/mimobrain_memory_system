"""Run manifest sync (report + tasks) into a local run directory (authoritative).

Authoritative outputs live outside the repo (data root), under:
  <DATA_ROOT>\runs\sync\RUN-<timestamp>\

Repo keeps only redacted examples.

This script:
- runs analyze_sync(kind, base, incoming) -> report.json
- generates tasks from report -> tasks.jsonl
- writes run_manifest.json with sha256 fingerprints for inputs/outputs

Usage:
  python tools/run_manifest_sync.py --kind raw --base <base.jsonl> --incoming <incoming.jsonl>

Optional:
  --runs-root <dir> to override default runs root.
"""

from __future__ import annotations

# ruff: noqa: E402  # This file intentionally edits sys.path for script execution.

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Allow running as a script: ensure repo root is importable.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.manifest_sync import analyze_sync
from tools.manifest_sync_tasks import tasks_from_report


# No hardcoded runs root; pass --runs-root or provide --config.
DEFAULT_RUNS_ROOT = None


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


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--kind", required=True, choices=["raw", "mu", "asset"])
    p.add_argument("--base", required=True)
    p.add_argument("--incoming", required=True)
    p.add_argument(
        "--config",
        default=None,
        help="Path to ms_config.json (optional; can provide runs_root_sync)",
    )
    p.add_argument(
        "--runs-root",
        default=None,
        help="Authoritative runs root. If omitted, tries to use config.runs_root_sync.",
    )
    ns = p.parse_args(argv)

    runs_root: str | None = ns.runs_root
    if ns.config:
        from tools.ms_config import load_config

        cfg = load_config(ns.config)
        runs_root = runs_root or cfg.get("runs_root_sync")

    if not runs_root:
        raise SystemExit(
            "missing runs root: pass --runs-root or provide --config with runs_root_sync"
        )

    run_id = now_run_id()
    run_dir = Path(runs_root) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    report = analyze_sync(kind=ns.kind, base_path=ns.base, incoming_path=ns.incoming)
    report_path = run_dir / f"sync_report.{ns.kind}.json"
    report_sha = write_json(report_path, report)

    tasks = tasks_from_report(report)
    tasks_path = run_dir / f"tasks.{ns.kind}.jsonl"
    with tasks_path.open("w", encoding="utf-8") as f:
        for t in tasks:
            f.write(json.dumps(t, ensure_ascii=False) + "\n")
    tasks_sha = sha256_file(tasks_path)

    run_manifest = {
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "tool": "manifest_sync",
        "kind": ns.kind,
        "inputs": {
            "base_path": ns.base,
            "incoming_path": ns.incoming,
            "base_sha256": sha256_file(Path(ns.base))
            if Path(ns.base).exists()
            else None,
            "incoming_sha256": sha256_file(Path(ns.incoming))
            if Path(ns.incoming).exists()
            else None,
        },
        "outputs": {
            "report_path": str(report_path),
            "report_sha256": report_sha,
            "tasks_path": str(tasks_path),
            "tasks_sha256": tasks_sha,
        },
        "notes": {
            "authoritative": True,
            "repo_examples": "regenerate via tools/emit_example_manifest_sync.py",
        },
    }
    write_json(run_dir / "run_manifest.json", run_manifest)

    print(str(run_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
