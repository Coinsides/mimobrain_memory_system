"""Pointer resolution (P1-G / TASK-RESOLVE-001) — evidence backtrace.

Resolves a pointer to a local file via vault_roots + (optional) raw manifest lookup,
verifies sha256, and extracts a snippet via locator.

v0.1 scope:
- Supported locator kinds: line_range
- Supported uri schemes:
  - vault://... (preferred)
  - legacy uri (file:// or absolute paths) *only* via sha256 lookup in raw_manifest

This module is designed to be reusable by build_bundle/view/orchestrator later.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tools.manifest_io import iter_jsonl
from tools.vault_ops import resolve_vault_uri_to_path, sha256_file


@dataclass(frozen=True)
class ResolveOutcome:
    ok: bool
    uri: str | None
    path: str | None
    sha256_expected: str | None
    sha256_actual: str | None
    snippet: str | None
    diagnostics: dict[str, Any]


def _index_raw_manifest_by_sha256(raw_manifest_path: Path) -> dict[str, str]:
    idx: dict[str, str] = {}
    for rec in iter_jsonl(raw_manifest_path):
        s = rec.get("sha256")
        u = rec.get("uri")
        if isinstance(s, str) and isinstance(u, str) and s not in idx:
            idx[s] = u
    return idx


def _read_line_range(p: Path, *, start: int, end: int) -> str:
    if start < 1 or end < start:
        raise ValueError(f"invalid line_range: start={start} end={end}")
    lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
    # 1-indexed inclusive
    return "\n".join(lines[start - 1 : end])


def resolve_pointer(
    pointer: dict[str, Any],
    *,
    vault_roots: dict[str, str],
    raw_manifest_path: str | Path | None = None,
) -> ResolveOutcome:
    diag: dict[str, Any] = {}

    uri = pointer.get("uri")
    sha = pointer.get("sha256")
    locator = pointer.get("locator")

    if not isinstance(uri, str) or not uri:
        return ResolveOutcome(
            ok=False,
            uri=None,
            path=None,
            sha256_expected=sha if isinstance(sha, str) else None,
            sha256_actual=None,
            snippet=None,
            diagnostics={"error": "missing uri"},
        )

    if not isinstance(sha, str) or not sha.startswith("sha256:"):
        diag["warning"] = "missing/invalid sha256; cannot verify"

    chosen_uri = uri
    if not uri.startswith("vault://"):
        # Legacy pointer: only resolve via manifest lookup by sha256.
        if raw_manifest_path is None or not isinstance(sha, str):
            return ResolveOutcome(
                ok=False,
                uri=uri,
                path=None,
                sha256_expected=sha if isinstance(sha, str) else None,
                sha256_actual=None,
                snippet=None,
                diagnostics={"error": "legacy uri without manifest lookup"},
            )
        idx = _index_raw_manifest_by_sha256(Path(raw_manifest_path))
        new_uri = idx.get(sha)
        if not new_uri:
            return ResolveOutcome(
                ok=False,
                uri=uri,
                path=None,
                sha256_expected=sha,
                sha256_actual=None,
                snippet=None,
                diagnostics={"error": "sha256 not found in raw manifest"},
            )
        chosen_uri = new_uri
        diag["resolved_via_manifest"] = True
        diag["original_uri"] = uri

    # Resolve vault uri to local path.
    try:
        p = resolve_vault_uri_to_path(chosen_uri, vault_roots=vault_roots)
    except Exception as e:
        return ResolveOutcome(
            ok=False,
            uri=chosen_uri,
            path=None,
            sha256_expected=sha if isinstance(sha, str) else None,
            sha256_actual=None,
            snippet=None,
            diagnostics={"error": f"resolve_vault_uri_to_path failed: {e}", **diag},
        )

    if not p.exists():
        return ResolveOutcome(
            ok=False,
            uri=chosen_uri,
            path=str(p),
            sha256_expected=sha if isinstance(sha, str) else None,
            sha256_actual=None,
            snippet=None,
            diagnostics={"error": "missing file", **diag},
        )

    actual = sha256_file(p)
    expected = sha if isinstance(sha, str) else None
    if expected and actual != expected:
        return ResolveOutcome(
            ok=False,
            uri=chosen_uri,
            path=str(p),
            sha256_expected=expected,
            sha256_actual=actual,
            snippet=None,
            diagnostics={"error": "sha256 mismatch", **diag},
        )

    # Extract snippet.
    snippet: str | None = None
    if locator is None:
        diag["warning"] = "missing locator; no snippet extracted"
    elif not isinstance(locator, dict):
        diag["warning"] = "invalid locator; no snippet extracted"
    else:
        kind = locator.get("kind")
        if kind == "line_range":
            try:
                snippet = _read_line_range(
                    p, start=int(locator.get("start")), end=int(locator.get("end"))
                )
            except Exception as e:
                diag["warning"] = f"snippet extraction failed: {e}"
        else:
            diag["warning"] = f"unsupported locator kind: {kind!r}"

    return ResolveOutcome(
        ok=True,
        uri=chosen_uri,
        path=str(p),
        sha256_expected=expected,
        sha256_actual=actual,
        snippet=snippet,
        diagnostics=diag,
    )


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--pointer-json", required=True, help="Pointer JSON object")
    ap.add_argument("--vault-root", required=True, help="Physical vault root")
    ap.add_argument("--vault-id", default="default")
    ap.add_argument("--raw-manifest", default=None)
    ns = ap.parse_args(argv)

    pointer = json.loads(ns.pointer_json)
    out = resolve_pointer(
        pointer,
        vault_roots={ns.vault_id: ns.vault_root},
        raw_manifest_path=ns.raw_manifest,
    )
    print(json.dumps(out.__dict__, ensure_ascii=False, indent=2))
    return 0 if out.ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
