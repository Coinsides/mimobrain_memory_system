"""Doctor/verify/repair helpers (P0-H v0.1).

v0.1 scope:
- doctor: validate manifest jsonl lines against jsonschema (dev dependency)
- verify: verify file sha256 for `vault://...` URIs given an explicit vault_roots mapping
- repair: suggest a replacement URI by sha256 lookup in manifests

Multi-replica selection is simplified in v0.1; full merge/conflict handling is P0-F/P0-7.
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


def verify_manifest(manifest_path: Path, *, vault_roots: dict[str, str]) -> list[str]:
    """Verify sha256 for each record in manifest.

    Only supports `vault://...` URIs in v0.1.
    """
    from .manifest_io import iter_jsonl
    from .vault_ops import verify_manifest_records

    return verify_manifest_records(iter_jsonl(manifest_path), vault_roots=vault_roots)


def repair_suggest_by_sha256(manifest_path: Path, *, sha256: str) -> str | None:
    """Suggest a URI for a sha256 by searching the manifest."""
    from .manifest_io import iter_jsonl
    from .vault_ops import repair_uri_by_sha256

    return repair_uri_by_sha256(sha256=sha256, manifest_records=iter_jsonl(manifest_path))
