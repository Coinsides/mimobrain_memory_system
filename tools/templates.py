"""Schema templates (P1-C).

Templates are versioned YAML files stored under repo_root/templates/*.yaml.
They provide deterministic defaults for scope + granularity + budget.

This module is intentionally small: load, validate, return dicts.
"""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def templates_dir() -> Path:
    return repo_root() / "templates"


def template_schema_path() -> Path:
    return repo_root() / "docs" / "contracts" / "template_v0_1.schema.json"


def load_template(name: str) -> dict:
    path = templates_dir() / f"{name}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"missing template: {name} ({path})")
    obj = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise TypeError(f"template must be a mapping: {path}")
    return obj


def validate_template(obj: dict, schema: dict) -> list[str]:
    v = Draft202012Validator(schema)
    errors = sorted(v.iter_errors(obj), key=lambda e: (list(e.path), e.message))
    return [f"{list(e.path)}: {e.message}" for e in errors]


def load_and_validate_template(name: str) -> dict:
    obj = load_template(name)
    schema = json.loads(template_schema_path().read_text(encoding="utf-8"))
    errs = validate_template(obj, schema)
    if errs:
        raise ValueError(f"invalid template {name}: {errs[:5]}")
    return obj
