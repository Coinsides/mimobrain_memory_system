"""Pointer migration (P1-G / TASK-MIGRATE-001) — legacy uri -> vault:// uri.

Goal:
- When MU pointers reference legacy local paths (e.g. file:// or absolute paths),
  migrate them to stable `vault://<vault_id>/raw/...` URIs using raw_manifest
  lookup by sha256.

Append-only rule:
- We do NOT modify the original MU.
- We write a new MU file (supersedes old mu_id) with updated pointer URIs.

v0.1 constraints:
- Expects MU stored as YAML (.mimo).
- Only migrates pointer items that have a `sha256` and a non-vault uri.
- Looks up uri by sha256 in raw_manifest.jsonl.

This is intentionally conservative: if we cannot find a match in the manifest,
we leave the pointer unchanged.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml

from tools.manifest_io import iter_jsonl


def _utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def _new_mu_id() -> str:
    # Not ULID, but time-sortable and unique enough for local migration.
    seed = f"{_utc_now_compact()}:{Path.cwd()}".encode("utf-8")
    rnd = hashlib.sha256(seed).hexdigest()[:10]
    return f"mu_migr_{_utc_now_compact()}_{rnd}"


def _load_mu(path: Path) -> dict[str, Any]:
    obj = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError(f"MU is not a mapping: {path}")
    return obj


def _dump_mu(obj: dict[str, Any]) -> str:
    # Keep YAML stable-ish
    return yaml.safe_dump(obj, sort_keys=False, allow_unicode=True)


def _index_manifest_by_sha256(manifest_path: Path) -> dict[str, str]:
    idx: dict[str, str] = {}
    for rec in iter_jsonl(manifest_path):
        s = rec.get("sha256")
        u = rec.get("uri")
        if isinstance(s, str) and isinstance(u, str) and s not in idx:
            idx[s] = u
    return idx


@dataclass(frozen=True)
class PointerMigration:
    old_uri: str
    new_uri: str
    sha256: str


@dataclass(frozen=True)
class MigrationResult:
    source_mu_path: Path
    source_mu_id: str
    new_mu_path: Path
    new_mu_id: str
    changed_pointers: list[PointerMigration]


def migrate_mu_pointers(
    mu_path: str | Path,
    *,
    raw_manifest_path: str | Path,
    out_dir: str | Path,
) -> MigrationResult | None:
    mu_p = Path(mu_path)
    out_dir_p = Path(out_dir)
    out_dir_p.mkdir(parents=True, exist_ok=True)

    mu = _load_mu(mu_p)
    old_mu_id = mu.get("mu_id")
    if not isinstance(old_mu_id, str) or not old_mu_id:
        raise ValueError(f"missing mu_id in {mu_p}")

    pointers = mu.get("pointer")
    if pointers is None:
        return None
    if not isinstance(pointers, list):
        raise ValueError(f"pointer must be a list in {mu_p}")

    idx = _index_manifest_by_sha256(Path(raw_manifest_path))

    changed: list[PointerMigration] = []
    new_pointers: list[dict[str, Any]] = []

    for p in pointers:
        if not isinstance(p, dict):
            new_pointers.append(p)
            continue
        uri = p.get("uri")
        sha = p.get("sha256")
        if isinstance(uri, str) and isinstance(sha, str) and uri and not uri.startswith("vault://"):
            new_uri = idx.get(sha)
            if isinstance(new_uri, str) and new_uri.startswith("vault://") and new_uri != uri:
                p2 = dict(p)
                p2["uri"] = new_uri
                new_pointers.append(p2)
                changed.append(PointerMigration(old_uri=uri, new_uri=new_uri, sha256=sha))
                continue
        new_pointers.append(p)

    if not changed:
        return None

    new_mu = dict(mu)
    new_mu_id = _new_mu_id()
    new_mu["mu_id"] = new_mu_id
    new_mu["pointer"] = new_pointers

    # Ensure links.supersedes includes old mu_id
    links = new_mu.get("links")
    if not isinstance(links, dict):
        links = {}
    supersedes = links.get("supersedes")
    if supersedes is None:
        supersedes = []
    if not isinstance(supersedes, list):
        supersedes = [supersedes]
    if old_mu_id not in supersedes:
        supersedes.append(old_mu_id)
    links["supersedes"] = supersedes
    new_mu["links"] = links

    # Write new MU next to out_dir using new id
    new_path = out_dir_p / f"{new_mu_id}.mimo"
    new_path.write_text(_dump_mu(new_mu), encoding="utf-8")

    return MigrationResult(
        source_mu_path=mu_p,
        source_mu_id=old_mu_id,
        new_mu_path=new_path,
        new_mu_id=new_mu_id,
        changed_pointers=changed,
    )


def iter_mu_files(inp: Path) -> Iterable[Path]:
    if inp.is_file():
        yield inp
        return
    for p in sorted(inp.rglob("*.mimo")):
        if p.is_file():
            yield p


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--mu", required=True, help="MU file or directory containing *.mimo")
    ap.add_argument("--raw-manifest", required=True, help="raw_manifest.jsonl path")
    ap.add_argument("--out-dir", required=True, help="Output directory for migrated MU files")
    ap.add_argument("--report", default=None, help="Optional json report output")
    ns = ap.parse_args(argv)

    mu_in = Path(ns.mu)
    if not mu_in.exists():
        raise SystemExit(f"missing --mu: {mu_in}")

    raw_manifest = Path(ns.raw_manifest)
    if not raw_manifest.exists():
        raise SystemExit(f"missing --raw-manifest: {raw_manifest}")

    results: list[dict[str, Any]] = []
    migrated = 0
    touched = 0

    for mu_path in iter_mu_files(mu_in):
        touched += 1
        res = migrate_mu_pointers(mu_path, raw_manifest_path=raw_manifest, out_dir=ns.out_dir)
        if res is None:
            continue
        migrated += 1
        results.append(
            {
                "source_mu_id": res.source_mu_id,
                "source_mu_path": str(res.source_mu_path),
                "new_mu_id": res.new_mu_id,
                "new_mu_path": str(res.new_mu_path),
                "changed_pointers": [
                    {"sha256": c.sha256, "old_uri": c.old_uri, "new_uri": c.new_uri}
                    for c in res.changed_pointers
                ],
            }
        )

    report = {"touched": touched, "migrated": migrated, "results": results}
    if ns.report:
        Path(ns.report).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps({"touched": touched, "migrated": migrated}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
