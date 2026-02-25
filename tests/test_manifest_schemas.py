import json
from pathlib import Path

import pytest


@pytest.fixture()
def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_manifest_schemas_are_valid_json(repo_root: Path):
    paths = [
        repo_root / "docs" / "contracts" / "raw_manifest_line_v0_1.schema.json",
        repo_root / "docs" / "contracts" / "mu_manifest_line_v0_1.schema.json",
        repo_root / "docs" / "contracts" / "asset_manifest_line_v0_1.schema.json",
    ]
    for p in paths:
        obj = json.loads(p.read_text(encoding="utf-8"))
        assert isinstance(obj, dict)
        assert obj.get("$schema")


def test_manifest_schema_validation_if_jsonschema_available(repo_root: Path):
    jsonschema = pytest.importorskip("jsonschema")

    schema = json.loads(
        (
            repo_root / "docs" / "contracts" / "raw_manifest_line_v0_1.schema.json"
        ).read_text(encoding="utf-8")
    )
    jsonschema.Draft202012Validator(schema).validate(
        {
            "raw_id": "sha256:" + "0" * 64,
            "uri": "vault://default/raw/2026/02/21/foo.md",
            "sha256": "sha256:" + "0" * 64,
            "size_bytes": 1,
            "mtime": None,
            "mime": "text/plain",
            "ingested_at": "2026-02-21T00:00:00Z",
        }
    )
