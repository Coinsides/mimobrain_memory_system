"""Vault operations (doctor/verify/repair) v0.1.

This module provides minimal but real implementations for:
- doctor: schema validation of jsonl manifest lines
- verify: sha256 verification of files referenced by manifest URIs
- repair: resolve missing/changed URIs by sha256 lookup in manifests

Design notes:
- We deliberately keep mapping from `vault://...` URI to local filesystem path
  explicit and injectable via `vault_roots`.
- Multi-replica selection is simplified in v0.1: pick the first matching record.
"""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .vault_uri import parse_vault_uri


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()


def resolve_vault_uri_to_path(uri: str, *, vault_roots: dict[str, str]) -> Path:
    vu = parse_vault_uri(uri)
    root = vault_roots.get(vu.vault_id)
    if not root:
        raise ValueError(f"no vault root configured for vault_id={vu.vault_id!r}")
    return Path(root) / vu.kind / Path(vu.path)


@dataclass(frozen=True)
class RepairSuggestion:
    old_uri: str
    sha256: str
    suggested_uri: str


def verify_manifest_records(
    records: Iterable[dict],
    *,
    vault_roots: dict[str, str],
) -> list[str]:
    """Verify that each record's sha256 matches the file content.

    Returns list of error strings.

    Expected record keys: uri, sha256.
    """
    errors: list[str] = []
    for rec in records:
        uri = rec.get("uri")
        expected = rec.get("sha256")
        if not isinstance(uri, str) or not isinstance(expected, str):
            errors.append(f"invalid record (missing uri/sha256): {rec}")
            continue

        if uri.startswith("vault://"):
            try:
                p = resolve_vault_uri_to_path(uri, vault_roots=vault_roots)
            except Exception as e:
                errors.append(f"resolve failed for {uri}: {e}")
                continue
        else:
            # v0.1 only supports verifying vault:// URIs
            errors.append(f"unsupported uri scheme for verify: {uri}")
            continue

        if not p.exists():
            errors.append(f"missing file for uri={uri}: {p}")
            continue

        actual = sha256_file(p)
        if actual != expected:
            errors.append(f"sha256 mismatch for uri={uri}: expected={expected} actual={actual}")

    return errors


def repair_uri_by_sha256(
    *,
    sha256: str,
    manifest_records: Iterable[dict],
) -> str | None:
    """Return a suggested URI for a given sha256 by searching manifest records."""
    if not isinstance(sha256, str):
        return None
    for rec in manifest_records:
        if rec.get("sha256") == sha256:
            uri = rec.get("uri")
            if isinstance(uri, str):
                return uri
    return None


def repair_suggestions_for_missing(
    records: Iterable[dict],
    *,
    manifest_records: Iterable[dict],
    vault_roots: dict[str, str],
) -> list[RepairSuggestion]:
    """For records whose uri cannot be resolved to an existing local file, suggest a new uri by sha256 lookup."""
    suggestions: list[RepairSuggestion] = []

    # Pre-index manifest by sha256
    index: dict[str, str] = {}
    for rec in manifest_records:
        s = rec.get("sha256")
        u = rec.get("uri")
        if isinstance(s, str) and isinstance(u, str) and s not in index:
            index[s] = u

    for rec in records:
        uri = rec.get("uri")
        s = rec.get("sha256")
        if not isinstance(uri, str) or not isinstance(s, str):
            continue

        if not uri.startswith("vault://"):
            continue

        try:
            p = resolve_vault_uri_to_path(uri, vault_roots=vault_roots)
        except Exception:
            p = None

        if p is None or not p.exists():
            new_uri = index.get(s)
            if new_uri and new_uri != uri:
                suggestions.append(RepairSuggestion(old_uri=uri, sha256=s, suggested_uri=new_uri))

    return suggestions
