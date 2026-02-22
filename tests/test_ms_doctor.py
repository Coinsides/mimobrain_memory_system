import json
from pathlib import Path


def test_ms_doctor_manifest_journals(tmp_path: Path, monkeypatch):
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

    # Redirect structured logs to a tmp file (avoid writing into repo/logs)
    from tools import logger as logger_mod

    log_path = tmp_path / "ms_doctor.jsonl"

    def _dlp(_name: str):
        return log_path

    monkeypatch.setattr(logger_mod, "default_log_path", _dlp)

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

    # ensure a structured log line was written
    assert log_path.exists()
    assert log_path.read_text(encoding="utf-8").strip()
