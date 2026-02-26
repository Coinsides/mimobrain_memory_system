"""Retry a failed Phase 2 job (append-only).

Policy (G-2): retry creates a *new* job_id; old job is preserved for audit.

Usage:
  python -m tools.jobs_retry --data-root "C:/memobrain/data/memory_system" --job-id JOB-FAILED-001
  python -m tools.jobs_retry --data-root "C:/memobrain/data/memory_system" --job-id JOB-FAILED-001 --new-job-id JOB-RETRY-002

Behavior:
- Reads <DATA_ROOT>/jobs/<job_id>/job.json
- Creates a new job folder <DATA_ROOT>/jobs/<new_job_id>/job.json with:
    retry_of=<old_job_id>, attempt=<n>
- Creates status.json queued
- Does not delete or mutate the old job.

Notes:
- Inbox handling is intentionally minimal: the new job points to the same inbox_path by default.
  (Inbox hygiene is handled by G-3; once done, retries may need a copy-back from _failed/_done.)
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def now_iso_z() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def default_job_id() -> str:
    return "JOB-RETRY-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


@dataclass(frozen=True)
class RetryResult:
    data_root: str
    old_job_id: str
    new_job_id: str
    old_job_dir: str
    new_job_dir: str


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--data-root", required=True)
    p.add_argument("--job-id", required=True, help="Old job id to retry")
    p.add_argument("--new-job-id", default=None)
    ns = p.parse_args(argv)

    data_root = Path(ns.data_root)
    old_job_id = str(ns.job_id)
    old_job_dir = data_root / "jobs" / old_job_id
    old_job_json = old_job_dir / "job.json"

    if not old_job_json.exists():
        raise SystemExit(f"missing job.json: {old_job_json}")

    old_job = read_json(old_job_json)

    new_job_id = str(ns.new_job_id) if ns.new_job_id else default_job_id()
    new_job_dir = data_root / "jobs" / new_job_id
    if new_job_dir.exists():
        raise SystemExit(f"new job dir already exists: {new_job_dir}")

    # attempt: count how many retries already exist is out-of-scope for MVP.
    attempt = int(old_job.get("attempt") or 1) + 1

    new_job = dict(old_job)
    new_job["job_id"] = new_job_id
    new_job["retry_of"] = old_job_id
    new_job["attempt"] = attempt
    new_job["created_at"] = now_iso_z()

    write_json(new_job_dir / "job.json", new_job)

    status = {
        "job_id": new_job_id,
        "workspace_id": new_job.get("workspace_id"),
        "status": "queued",
        "step": None,
        "created_at": now_iso_z(),
        "updated_at": now_iso_z(),
        "last_error": None,
    }
    write_json(new_job_dir / "status.json", status)

    out = RetryResult(
        data_root=str(data_root),
        old_job_id=old_job_id,
        new_job_id=new_job_id,
        old_job_dir=str(old_job_dir),
        new_job_dir=str(new_job_dir),
    )
    print(json.dumps(asdict(out), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
