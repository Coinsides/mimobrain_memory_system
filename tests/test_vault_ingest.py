from __future__ import annotations

import json
from pathlib import Path


def test_vault_ingest_writes_manifest_and_copies_file(tmp_path: Path):
    from tools.vault_ingest import ingest_file
    from tools.vault_doctor import doctor_manifest

    vault_root = tmp_path / "vault"
    src = tmp_path / "hello.txt"
    src.write_text("hello world\n", encoding="utf-8")

    res = ingest_file(src, vault_root=vault_root, vault_id="default")

    assert res.dest_path.exists()
    assert res.dest_path.read_text(encoding="utf-8") == "hello world\n"

    # manifest exists and last line parses
    assert res.manifest_path.exists()
    last = res.manifest_path.read_text(encoding="utf-8").strip().splitlines()[-1]
    obj = json.loads(last)
    assert obj["raw_id"].startswith("sha256:")
    assert obj["sha256"] == obj["raw_id"]
    assert obj["uri"].startswith("vault://default/raw/")

    # validates against schema
    schema_path = (
        Path(__file__).resolve().parents[1]
        / "docs"
        / "contracts"
        / "raw_manifest_line_v0_1.schema.json"
    )
    errors = doctor_manifest(res.manifest_path, schema_path)
    assert errors == []
