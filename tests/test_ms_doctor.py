import json
from pathlib import Path


def test_ms_doctor_manifest_journals(tmp_path: Path):
    # minimal schema + manifest
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {"x": {"type": "string"}},
        "required": ["x"],
    }
    schema_p = tmp_path / "s.json"
    schema_p.write_text(json.dumps(schema), encoding="utf-8")

    manifest_p = tmp_path / "m.jsonl"
    manifest_p.write_text('{"x":"ok"}\n', encoding="utf-8")

    journal = tmp_path / "j.sqlite"

    from tools.ms_doctor import main

    assert (
        main(
            [
                "manifest",
                "--manifest",
                str(manifest_p),
                "--schema",
                str(schema_p),
                "--journal-db",
                str(journal),
            ]
        )
        == 0
    )

    from tools.task_journal import query_tasks

    rows = query_tasks(journal, type="MS_DOCTOR_MANIFEST")
    assert rows
