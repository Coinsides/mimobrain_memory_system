from __future__ import annotations

from pathlib import Path


def test_repair_executor_suggests_uri(tmp_path: Path):
    from tools.manifest_io import append_jsonl
    from tools.repair_executor import ExecContext, exec_task

    manifest = tmp_path / "raw_manifest.jsonl"
    sha = "sha256:" + "1" * 64
    append_jsonl(
        manifest,
        {
            "raw_id": sha,
            "uri": "vault://default/raw/2026/02/x.txt",
            "sha256": sha,
            "size_bytes": 0,
            "mtime": None,
            "mime": "text/plain",
            "ingested_at": "2026-02-22T00:00:00Z",
        },
    )

    task = {
        "task_id": "t1",
        "type": "REPAIR_POINTER",
        "created_at": "2026-02-22T00:00:00Z",
        "idempotency_key": "k",
        "inputs": [{"kind": "MU_SET", "ids": ["mu_x"]}],
        "params": {"mu_id": "mu_x", "sha256": sha, "uri": "file:///C:/tmp/x.txt"},
    }

    out = exec_task(task, ExecContext(vault_roots={}, raw_manifest_path=str(manifest)))
    assert out["status"] == "OK"
    assert out["diagnostics"][0]["code"] == "SUGGEST_POINTER_URI"
