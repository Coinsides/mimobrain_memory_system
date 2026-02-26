"""Phase 2 Import command (MVP): create a Job from a file/folder.

This tool is a thin entrypoint so a user (or future UI) can do:
  import -> job.json -> jobs_worker consumes -> membership/index updated.

It copies inputs into:
  <DATA_ROOT>/inbox/<workspace_id>/_queue/<job_id>/...

Then creates:
  <DATA_ROOT>/jobs/<job_id>/job.json
  <DATA_ROOT>/jobs/<job_id>/status.json  (queued)

Usage:
  python -m tools.import_job --data-root "C:/memobrain/data/memory_system" --workspace ws_design --in "C:/path/to/file_or_dir"

Notes:
- Workspace is mandatory (membership fence requires scope).
- This command does not run the pipeline; it only enqueues a job.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


def now_iso_z() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def default_job_id() -> str:
    # Readable, unique enough for local use.
    return "JOB-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def copy_into(src: Path, dst_dir: Path) -> None:
    dst_dir.mkdir(parents=True, exist_ok=True)
    if src.is_file():
        shutil.copy2(src, dst_dir / src.name)
        return
    if src.is_dir():
        # Copy directory contents into dst_dir/<src.name>
        target = dst_dir / src.name
        if target.exists():
            raise FileExistsError(f"target already exists: {target}")
        shutil.copytree(src, target)
        return
    raise FileNotFoundError(src)


def write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


@dataclass(frozen=True)
class ImportResult:
    job_id: str
    data_root: str
    workspace_id: str
    inbox_path: str
    job_dir: str


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--data-root", required=True)
    p.add_argument("--workspace", required=True)
    p.add_argument("--in", dest="inp", required=True, help="File or directory to import")
    p.add_argument("--split", default="line_window:200")
    p.add_argument("--source-kind", default="file", choices=["file", "chat", "web", "pdf"])
    p.add_argument("--vault-id", default="default")
    p.add_argument("--job-id", default=None)
    ns = p.parse_args(argv)

    data_root = Path(ns.data_root)
    workspace_id = str(ns.workspace)
    src = Path(ns.inp)
    if not src.exists():
        raise SystemExit(f"missing --in: {src}")

    job_id = str(ns.job_id) if ns.job_id else default_job_id()

    inbox_dir = data_root / "inbox" / workspace_id / "_queue" / job_id
    jobs_dir = data_root / "jobs" / job_id

    # Copy input(s) to inbox queue
    copy_into(src, inbox_dir)

    job = {
        "job_id": job_id,
        "workspace_id": workspace_id,
        "inbox_path": str(inbox_dir),
        "split": str(ns.split),
        "source_kind": str(ns.source_kind),
        "vault_id": str(ns.vault_id),
        "created_at": now_iso_z(),
    }
    write_json(jobs_dir / "job.json", job)

    status = {
        "job_id": job_id,
        "workspace_id": workspace_id,
        "status": "queued",
        "step": None,
        "created_at": now_iso_z(),
        "updated_at": now_iso_z(),
        "last_error": None,
    }
    write_json(jobs_dir / "status.json", status)

    out = ImportResult(
        job_id=job_id,
        data_root=str(data_root),
        workspace_id=workspace_id,
        inbox_path=str(inbox_dir),
        job_dir=str(jobs_dir),
    )
    print(json.dumps(asdict(out), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
