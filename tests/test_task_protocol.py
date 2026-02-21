import json
from pathlib import Path

import pytest


@pytest.fixture()
def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_task_spec_schema_is_valid_json(repo_root: Path):
    p = repo_root / "docs" / "contracts" / "task_spec_v0_1.schema.json"
    obj = json.loads(p.read_text(encoding="utf-8"))
    assert isinstance(obj, dict)
    assert obj.get("title") == "TaskSpec v0.1"


def test_task_result_schema_is_valid_json(repo_root: Path):
    p = repo_root / "docs" / "contracts" / "task_result_v0_1.schema.json"
    obj = json.loads(p.read_text(encoding="utf-8"))
    assert isinstance(obj, dict)
    assert obj.get("title") == "TaskResult v0.1"


def test_examples_validate_with_jsonschema_if_available(repo_root: Path):
    # We keep jsonschema as a dev dependency. If it's not installed, skip.
    jsonschema = pytest.importorskip("jsonschema")

    spec_schema = json.loads(
        (repo_root / "docs" / "contracts" / "task_spec_v0_1.schema.json").read_text(
            encoding="utf-8"
        )
    )
    result_schema = json.loads(
        (repo_root / "docs" / "contracts" / "task_result_v0_1.schema.json").read_text(
            encoding="utf-8"
        )
    )

    spec = json.loads(
        (repo_root / "examples" / "task_spec_example.json").read_text(encoding="utf-8")
    )
    res = json.loads(
        (repo_root / "examples" / "task_result_example.json").read_text(
            encoding="utf-8"
        )
    )

    jsonschema.Draft202012Validator(spec_schema).validate(spec)
    jsonschema.Draft202012Validator(result_schema).validate(res)
