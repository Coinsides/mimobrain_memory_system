"""List Phase 2 Jobs (no UI).

Reads job folders under:
  <DATA_ROOT>/jobs/<job_id>/

and prints a JSON list with the latest job/status information.

Usage:
  python -m tools.jobs_list --data-root "C:/memobrain/data/memory_system" --limit 20
  python -m tools.jobs_list --data-root "C:/memobrain/data/memory_system" --status failed
  python -m tools.jobs_list --data-root "C:/memobrain/data/memory_system" --workspace ws_design
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


@dataclass(frozen=True)
class JobRow:
    job_id: str
    workspace_id: str | None
    status: str | None
    step: str | None
    updated_at: str | None
    created_at: str | None
    last_error: str | None
    retry_of: str | None
    attempt: int | None


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--data-root", required=True)
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--status", default=None, help="Filter by status (queued|running|done|failed)")
    p.add_argument("--workspace", default=None, help="Filter by workspace_id")
    ns = p.parse_args(argv)

    data_root = Path(ns.data_root)
    jobs_root = data_root / "jobs"
    if not jobs_root.exists():
        print(json.dumps({"data_root": str(data_root), "jobs": []}, ensure_ascii=False, indent=2))
        return 0

    rows: list[JobRow] = []
    for d in sorted(jobs_root.iterdir()):
        if not d.is_dir():
            continue
        job_json = d / "job.json"
        status_json = d / "status.json"
        if not job_json.exists() and not status_json.exists():
            continue

        job: dict[str, Any] = {}
        st: dict[str, Any] = {}
        if job_json.exists():
            try:
                job = read_json(job_json)
            except Exception:
                job = {}
        if status_json.exists():
            try:
                st = read_json(status_json)
            except Exception:
                st = {}

        job_id = str((job.get("job_id") or st.get("job_id") or d.name))
        workspace_id = (st.get("workspace_id") or job.get("workspace_id"))
        status = st.get("status")
        step = st.get("step")
        updated_at = st.get("updated_at")
        created_at = st.get("created_at") or job.get("created_at")
        last_error = st.get("last_error")
        retry_of = job.get("retry_of") or st.get("retry_of")
        attempt = job.get("attempt") or st.get("attempt")

        if ns.status and status != ns.status:
            continue
        if ns.workspace and workspace_id != ns.workspace:
            continue

        rows.append(
            JobRow(
                job_id=job_id,
                workspace_id=str(workspace_id) if workspace_id is not None else None,
                status=str(status) if status is not None else None,
                step=str(step) if step is not None else None,
                updated_at=str(updated_at) if updated_at is not None else None,
                created_at=str(created_at) if created_at is not None else None,
                last_error=str(last_error) if last_error is not None else None,
                retry_of=str(retry_of) if retry_of is not None else None,
                attempt=int(attempt) if attempt is not None else None,
            )
        )

    # Sort by updated_at desc (string ISO sorts lexicographically)
    rows.sort(key=lambda r: (r.updated_at or ""), reverse=True)
    rows = rows[: int(ns.limit)]

    out = {
        "data_root": str(data_root),
        "count": len(rows),
        "jobs": [asdict(r) for r in rows],
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
