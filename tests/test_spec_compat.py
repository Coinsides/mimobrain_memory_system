from __future__ import annotations

import importlib
import json
from pathlib import Path


def test_can_import_mimo_spec() -> None:
    import mimo_spec  # noqa: F401


def test_can_load_mu_schema() -> None:
    # Ensure contracts are loadable from the installed/edited mimo-spec package.
    pkg = importlib.import_module("mimo_spec")
    root = Path(pkg.__file__).resolve().parent

    schema_path = root / "contracts" / "mu_v1_1.schema.json"
    assert schema_path.exists(), f"missing schema: {schema_path}"

    obj = json.loads(schema_path.read_text(encoding="utf-8"))
    assert obj.get("$schema"), "expected $schema in mu_v1_1"
    assert obj.get("title"), "expected title in mu_v1_1"


def test_spec_lock_present_and_valid_json() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    p = repo_root / "spec_lock.json"
    assert p.exists(), "spec_lock.json must exist to prevent repo drift"
    obj = json.loads(p.read_text(encoding="utf-8"))
    assert isinstance(obj.get("mimo_spec"), dict)
    assert isinstance(obj["mimo_spec"].get("ref"), str)
