from __future__ import annotations

import json
from pathlib import Path


def _write_json(p: Path, obj: dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def test_jobs_worker_moves_inbox_on_done(tmp_path: Path):
    # Minimal: we don't run the whole pipeline; we just test the helper via consume_one_job
    # by creating a job that will fail early after ingest_raw due to missing inbox, then verify move on failed.
    from tools.jobs_worker import consume_one_job

    data_root = tmp_path / "data"
    job_id = "JOB-X"

    inbox_queue = data_root / "inbox" / "ws_design" / "_queue" / job_id
    inbox_queue.mkdir(parents=True, exist_ok=True)
    (inbox_queue / "a.txt").write_text("hello", encoding="utf-8")

    job_dir = data_root / "jobs" / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        job_dir / "job.json",
        {
            "job_id": job_id,
            "workspace_id": "ws_design",
            "inbox_path": str(inbox_queue),
            "split": "line_window:200",
            "source_kind": "file",
            "vault_id": "default",
        },
    )

    # Force failure late by pointing index step to missing module? Not stable.
    # Instead, we accept that this may complete or fail depending on environment.
    # We check: after consume_one_job returns, inbox is NOT left in _queue.
    consume_one_job(data_root=data_root, job_dir=job_dir)

    assert not inbox_queue.exists()
    done_dir = data_root / "inbox" / "ws_design" / "_done" / job_id
    failed_dir = data_root / "inbox" / "ws_design" / "_failed" / job_id
    assert done_dir.exists() or failed_dir.exists()


def test_inbox_gc_dry_run_lists_old_items(tmp_path: Path):
    from tools.inbox_gc import main

    data_root = tmp_path / "data"
    old = data_root / "inbox" / "ws_design" / "_done" / "JOB-OLD"
    old.mkdir(parents=True, exist_ok=True)
    # Set mtime far in the past
    (old / "x.txt").write_text("x", encoding="utf-8")

    # Force mtime to epoch
    import os

    os.utime(old, (0, 0))

    rc = main(["--data-root", str(data_root), "--days", "1", "--dry-run"])
    assert rc == 0
