from __future__ import annotations

import json
from pathlib import Path


def test_repair_executor_autofix_writes_new_mu(tmp_path: Path):
    from tools.manifest_io import append_jsonl
    from tools.repair_executor import ExecContext, exec_task

    # raw manifest suggests a vault uri
    manifest = tmp_path / "raw_manifest.jsonl"
    sha = "sha256:" + "2" * 64
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

    # MU with legacy uri
    mu_path = tmp_path / "mu_old.mimo"
    mu_path.write_text(
        """schema_version: '1.1'
mu_id: mu_old
summary: old
pointer:
  - type: raw
    uri: file:///C:/tmp/x.txt
    sha256: %s
    locator:
      kind: line_range
      start: 1
      end: 1
links:
  supersedes: []
"""
        % sha,
        encoding="utf-8",
    )

    task = {
        "task_id": "t1",
        "type": "REPAIR_POINTER",
        "created_at": "2026-02-22T00:00:00Z",
        "idempotency_key": "k",
        "inputs": [{"kind": "MU_SET", "ids": ["mu_old"]}],
        "params": {"mu_id": "mu_old", "mu_path": str(mu_path), "sha256": sha, "uri": "file:///C:/tmp/x.txt"},
    }

    out_dir = tmp_path / "fixed"
    out = exec_task(task, ExecContext(vault_roots={}, raw_manifest_path=str(manifest), out_mu_dir=str(out_dir)))
    assert out["status"] == "OK"

    mu_outputs = [o for o in out.get("outputs", []) if o.get("kind") == "MU"]
    assert len(mu_outputs) == 1
    new_path = Path(mu_outputs[0]["uri"])
    assert new_path.exists()
    text = new_path.read_text(encoding="utf-8")
    assert "vault://default/raw/2026/02/x.txt" in text
    assert "supersedes" in text
    assert "mu_old" in text
