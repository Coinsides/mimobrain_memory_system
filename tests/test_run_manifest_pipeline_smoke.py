import json
from pathlib import Path


def write_jsonl(p: Path, objs: list[dict]):
    p.write_text("\n".join(json.dumps(o) for o in objs) + "\n", encoding="utf-8")


def test_pipeline_smoke(tmp_path: Path):
    # Make base/incoming manifests
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

    # run pipeline into tmp runs root
    runs_root = tmp_path / "runs"

    from tools.run_manifest_pipeline import main

    assert main(
        [
            "--kind",
            "raw",
            "--base",
            str(base),
            "--incoming",
            str(inc),
            "--runs-root",
            str(runs_root),
        ]
    ) == 0

    # should have created a RUN-* directory
    run_dirs = [p for p in runs_root.iterdir() if p.is_dir()]
    assert run_dirs

    run_dir = run_dirs[0]
    assert (run_dir / "run_manifest.json").exists()
    assert (run_dir / "sync_report.raw.json").exists()
    assert (run_dir / "tasks.raw.jsonl").exists()
    assert (run_dir / "task_results.raw.jsonl").exists()

    # patch plans should be written under run_dir/patch_plans
    patch_dir = run_dir / "patch_plans"
    assert patch_dir.exists()
    assert list(patch_dir.glob("*.patch_plan.json"))
