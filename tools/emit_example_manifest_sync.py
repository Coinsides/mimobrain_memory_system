"""Emit redacted example manifest sync artifacts into repo (examples/).

This is the "mirror" side: it produces small, repo-safe example inputs/outputs
so future changes to sync logic remain demonstrably consistent.

It generates:
- examples/manifests/raw_base.jsonl
- examples/manifests/raw_incoming.jsonl
- examples/sync_reports/raw_report.json
- examples/sync_tasks/raw_tasks.jsonl

Paths inside reports are normalized to placeholders.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Allow running as a script: ensure repo root is importable.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.manifest_sync import analyze_sync
from tools.manifest_sync_tasks import tasks_from_report


def write_jsonl(path: Path, objs: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for o in objs:
            f.write(json.dumps(o, ensure_ascii=False) + "\n")


def normalize_paths(report: dict) -> dict:
    r = json.loads(json.dumps(report))  # deep copy
    if "base" in r and isinstance(r["base"], dict):
        r["base"]["path"] = "<BASE_MANIFEST>"
    if "incoming" in r and isinstance(r["incoming"], dict):
        r["incoming"]["path"] = "<INCOMING_MANIFEST>"
    return r


def main(argv: list[str] | None = None) -> int:
    repo_root = Path(__file__).resolve().parents[1]

    manifests_dir = repo_root / "examples" / "manifests"
    reports_dir = repo_root / "examples" / "sync_reports"
    tasks_dir = repo_root / "examples" / "sync_tasks"

    a = "sha256:" + "a" * 64
    b = "sha256:" + "b" * 64

    base = manifests_dir / "raw_base.jsonl"
    inc = manifests_dir / "raw_incoming.jsonl"

    write_jsonl(
        base,
        [
            {
                "raw_id": a,
                "uri": "vault://default/raw/2026/02/21/a.txt",
                "sha256": a,
                "size_bytes": 1,
                "mtime": None,
                "mime": "text/plain",
                "ingested_at": "2026-02-21T00:00:00Z",
            }
        ],
    )

    write_jsonl(
        inc,
        [
            {
                "raw_id": a,
                "uri": "vault://default/raw/2026/02/22/a.txt",
                "sha256": a,
                "size_bytes": 1,
                "mtime": None,
                "mime": "text/plain",
                "ingested_at": "2026-02-22T00:00:00Z",
            },
            {
                "raw_id": b,
                "uri": "vault://default/raw/2026/02/21/a.txt",
                "sha256": b,
                "size_bytes": 1,
                "mtime": None,
                "mime": "text/plain",
                "ingested_at": "2026-02-22T00:00:00Z",
            },
        ],
    )

    report = analyze_sync(kind="raw", base_path=base, incoming_path=inc)
    report = normalize_paths(report)
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "raw_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    tasks = tasks_from_report(report)
    tasks_dir.mkdir(parents=True, exist_ok=True)
    with (tasks_dir / "raw_tasks.jsonl").open("w", encoding="utf-8") as f:
        for t in tasks:
            # avoid unstable uuids in examples
            t["task_id"] = "t_EXAMPLE"
            t["created_at"] = "2026-01-01T00:00:00Z"
            f.write(json.dumps(t, ensure_ascii=False) + "\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
