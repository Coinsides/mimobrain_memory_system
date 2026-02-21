"""Validate TaskSpec / TaskResult against JSON Schema.

This is a small deterministic utility for P0-C.

Usage:
  py tools/validate_task.py --spec examples/task_spec_example.json
  py tools/validate_task.py --result examples/task_result_example.json

Exit codes:
  0 OK
  1 validation failed / error
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def _load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_schema(path: Path) -> dict:
    obj = _load_json(path)
    if not isinstance(obj, dict):
        raise TypeError("schema must be a JSON object")
    return obj


def _validate(instance: object, schema: dict) -> list[str]:
    """Return a list of human-readable errors (empty if valid)."""
    try:
        import jsonschema
    except Exception:
        return [
            "Missing dependency: jsonschema. Install dev deps: pip install -r requirements-dev.txt"
        ]

    validator = jsonschema.Draft202012Validator(schema)
    errors = []
    for e in sorted(validator.iter_errors(instance), key=lambda x: x.json_path):
        loc = e.json_path or "$"
        errors.append(f"{loc}: {e.message}")
    return errors


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    spec_schema_path = repo_root / "docs" / "contracts" / "task_spec_v0_1.schema.json"
    result_schema_path = (
        repo_root / "docs" / "contracts" / "task_result_v0_1.schema.json"
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=Path)
    parser.add_argument("--result", type=Path)
    ns = parser.parse_args()

    if (ns.spec is None) == (ns.result is None):
        print("ERROR: pass exactly one of --spec or --result")
        return 1

    if ns.spec is not None:
        schema = _load_schema(spec_schema_path)
        inst = _load_json(ns.spec)
    else:
        schema = _load_schema(result_schema_path)
        inst = _load_json(ns.result)

    errs = _validate(inst, schema)
    if errs:
        print("INVALID")
        for line in errs:
            print(f"- {line}")
        return 1

    print("VALID")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
