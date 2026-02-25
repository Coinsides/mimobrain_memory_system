import json
from pathlib import Path


def write_jsonl(p: Path, objs: list[dict]):
    p.write_text("\n".join(json.dumps(o) for o in objs) + "\n", encoding="utf-8")


def test_executor_runs_sync_manifest_apply_dry_run(tmp_path: Path):
    base = tmp_path / "base.jsonl"
    inc = tmp_path / "inc.jsonl"

    a = "sha256:" + "a" * 64
    b = "sha256:" + "b" * 64

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
                "raw_id": b,
                "uri": "vault://default/raw/2026/02/21/b.txt",
                "sha256": b,
                "size_bytes": 1,
                "mtime": None,
                "mime": "text/plain",
                "ingested_at": "2026-02-22T00:00:00Z",
            }
        ],
    )

    tasks = tmp_path / "tasks.jsonl"
    write_jsonl(
        tasks,
        [
            {
                "task_id": "t1",
                "type": "SYNC_MANIFEST_APPLY",
                "created_at": "2026-01-01T00:00:00Z",
                "parent_task_id": None,
                "idempotency_key": "k",
                "inputs": [],
                "params": {
                    "kind": "raw",
                    "base_path": str(base),
                    "incoming_path": str(inc),
                    "dry_run": True,
                },
            }
        ],
    )

    out = tmp_path / "results.jsonl"

    from tools.manifest_executor import main

    assert main(["--tasks", str(tasks), "--out", str(out)]) == 0

    # Should write a patch plan file next to base by default
    plan_path = base.with_suffix(".patch_plan.json")
    assert plan_path.exists()

    # Base should NOT have been modified in dry-run
    lines = [
        line for line in base.read_text(encoding="utf-8").splitlines() if line.strip()
    ]
    assert len(lines) == 1
