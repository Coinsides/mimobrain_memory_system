from __future__ import annotations

import json
from pathlib import Path


def test_jobs_worker_records_raw_provenance_fields(tmp_path: Path):
    from tools.jobs_worker import consume_one_job

    data_root = tmp_path / "data"
    job_id = "JOB-PROV"

    inbox_queue = data_root / "inbox" / "ws_design" / "_queue" / job_id
    inbox_queue.mkdir(parents=True, exist_ok=True)
    (inbox_queue / "a.txt").write_text("hello", encoding="utf-8")

    job_dir = data_root / "jobs" / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "job.json").write_text(
        json.dumps(
            {
                "job_id": job_id,
                "workspace_id": "ws_design",
                "inbox_path": str(inbox_queue),
                "split": "line_window:200",
                "source_kind": "file",
                "vault_id": "default",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    consume_one_job(data_root=data_root, job_dir=job_dir)

    st_path = job_dir / "status.json"
    assert st_path.exists()
    st = json.loads(st_path.read_text(encoding="utf-8"))

    assert "raw_ingest" in st
    assert isinstance(st["raw_ingest"].get("files"), list)
    # provenance might not be set if linking/copy didn't run (should run when there are files)
    assert st.get("raw_inputs_provenance") in ("hardlink:vault/raw", "copy:vault/raw")
