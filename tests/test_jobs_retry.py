from __future__ import annotations

import json
from pathlib import Path


def _write_json(p: Path, obj: dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def test_jobs_retry_creates_new_job_append_only(tmp_path: Path):
    data_root = tmp_path / "data"
    old_id = "JOB-FAILED-001"
    old_dir = data_root / "jobs" / old_id

    _write_json(
        old_dir / "job.json",
        {
            "job_id": old_id,
            "workspace_id": "ws_design",
            "inbox_path": str(data_root / "inbox" / "ws_design" / "_queue" / old_id),
            "split": "line_window:200",
            "source_kind": "file",
            "vault_id": "default",
            "created_at": "2026-02-26T00:00:00Z",
        },
    )

    # Run CLI
    from tools.jobs_retry import main

    rc = main(
        [
            "--data-root",
            str(data_root),
            "--job-id",
            old_id,
            "--new-job-id",
            "JOB-RETRY-002",
        ]
    )
    assert rc == 0

    new_dir = data_root / "jobs" / "JOB-RETRY-002"
    assert (new_dir / "job.json").exists()
    assert (new_dir / "status.json").exists()

    new_job = json.loads((new_dir / "job.json").read_text(encoding="utf-8"))
    assert new_job["retry_of"] == old_id
    assert int(new_job["attempt"]) >= 2

    # Old job remains
    assert (old_dir / "job.json").exists()
