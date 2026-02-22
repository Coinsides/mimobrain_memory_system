from __future__ import annotations

import json
from pathlib import Path


def test_emit_repair_tasks_writes_valid_taskspec(tmp_path: Path):
    from tools.emit_repair_tasks import emit_repair_tasks

    # bundle with one repair task
    bundle = {
        "bundle_id": "bndl_test",
        "template": "time_overview_v1",
        "scope": {"time_window_days": 7},
        "source_mu_ids": ["mu_x"],
        "created_at": "2026-02-22T00:00:00Z",
        "evidence": [{"mu_id": "mu_x"}],
        "diagnostics": {
            "repair_tasks": [
                {
                    "type": "REPAIR_POINTER",
                    "mu_id": "mu_x",
                    "sha256": "sha256:" + "0" * 64,
                    "uri": "file:///C:/tmp/x.txt",
                    "reason": "missing file",
                    "hint": {"need_vault_roots": True, "need_raw_manifest": True},
                }
            ]
        },
    }

    bpath = tmp_path / "bundle.json"
    bpath.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")

    out_dir = tmp_path / "tasks"
    summary = emit_repair_tasks(bpath, out_dir=out_dir)
    assert summary.wrote == 1

    files = list(out_dir.glob("*.task_spec.json"))
    assert len(files) == 1
    spec = json.loads(files[0].read_text(encoding="utf-8"))

    # validate against schema
    schema_path = Path(__file__).resolve().parents[1] / "docs" / "contracts" / "task_spec_v0_1.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    import jsonschema

    jsonschema.Draft202012Validator(schema).validate(spec)
    assert spec["type"] == "REPAIR_POINTER"
    assert spec["inputs"][0]["kind"] == "MU_SET"
    assert spec["inputs"][0]["ids"] == ["mu_x"]
    assert spec["params"]["mu_id"] == "mu_x"
