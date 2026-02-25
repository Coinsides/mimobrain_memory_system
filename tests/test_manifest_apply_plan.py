import json
from pathlib import Path


def write_jsonl(p: Path, objs: list[dict]):
    p.write_text("\n".join(json.dumps(o) for o in objs) + "\n", encoding="utf-8")


def test_plan_and_apply_appends_only_new_ids(tmp_path: Path):
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
            # exact dupe
            {
                "raw_id": a,
                "uri": "vault://default/raw/2026/02/21/a.txt",
                "sha256": a,
                "size_bytes": 1,
                "mtime": None,
                "mime": "text/plain",
                "ingested_at": "2026-02-21T00:00:00Z",
            },
            # new id
            {
                "raw_id": b,
                "uri": "vault://default/raw/2026/02/21/b.txt",
                "sha256": b,
                "size_bytes": 1,
                "mtime": None,
                "mime": "text/plain",
                "ingested_at": "2026-02-22T00:00:00Z",
            },
        ],
    )

    from tools.manifest_apply_plan import apply_plan, plan_patch

    plan = plan_patch(kind="raw", base_path=base, incoming_path=inc)
    assert plan["stats"]["append_new_records"] == 1

    apply_plan(plan)

    # base should now contain two records (a + b)
    lines = [
        line for line in base.read_text(encoding="utf-8").splitlines() if line.strip()
    ]
    assert len(lines) == 2
