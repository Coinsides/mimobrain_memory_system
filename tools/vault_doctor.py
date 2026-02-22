"""Minimal doctor/verify/repair helpers (P0-H v0.1).

This is a *minimal* implementation:
- doctor: validate manifest jsonl lines against jsonschema (dev dependency)
- verify: placeholder (hash verification is a follow-up)
- repair: resolve pointer URI by sha256 lookup in raw_manifest (follow-up)

The full multi-replica merge/repair logic is planned in later tasks.
"""

from __future__ import annotations

import json
from pathlib import Path


def load_schema(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def doctor_manifest(manifest_path: Path, schema_path: Path) -> list[str]:
    """Return a list of error messages."""
    import jsonschema  # dev dep

    errors: list[str] = []
    schema = load_schema(schema_path)
    validator = jsonschema.Draft202012Validator(schema)

    if not manifest_path.exists():
        return [f"missing manifest: {manifest_path}"]

    for i, line in enumerate(manifest_path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception as e:
            errors.append(f"line {i}: invalid json: {e}")
            continue
        try:
            validator.validate(obj)
        except Exception as e:
            errors.append(f"line {i}: schema error: {e}")

    return errors
