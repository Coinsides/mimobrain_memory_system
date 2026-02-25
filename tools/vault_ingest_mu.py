"""Vault ingest for MU files (P1-G) — fixed_mu -> vault/mu + mu_manifest.

This is the MU analog of tools/vault_ingest.py.

Scope (v0.1):
- Ingest .mimo YAML file into a vault root under kind=mu.
- Extract required manifest fields from MU:
  - mu_id
  - schema_version
  - idempotency.mu_key
  - content_hash
  - created_at (ingest timestamp)
  - source_raw_ids (best-effort from pointer[].sha256)
- Write/append a mu_manifest.jsonl line (append-only) that validates against
  mu_manifest_line_v0_1.schema.json.

Notes:
- This does not modify the MU content.
- It does not validate MU schema; assume MU is already v1.1+.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from tools.manifest_io import append_jsonl


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _dest_relpath_for_mu(*, mu_id: str) -> Path:
    now = datetime.now(timezone.utc)
    return Path(f"{now.year:04d}") / f"{now.month:02d}" / f"{mu_id}.mimo"


def _load_mu(path: Path) -> dict[str, Any]:
    obj = yaml.safe_load(path.read_text(encoding="utf-8", errors="replace"))
    if not isinstance(obj, dict):
        raise ValueError(f"MU is not a mapping: {path}")
    return obj


@dataclass(frozen=True)
class IngestMuResult:
    mu_id: str
    uri: str
    dest_path: Path
    manifest_path: Path


def ingest_mu_file(
    src: str | Path,
    *,
    vault_root: str | Path,
    vault_id: str = "default",
    copy_mode: str = "copy2",
    manifest_path: str | Path | None = None,
) -> IngestMuResult:
    src_p = Path(src)
    if not src_p.exists() or not src_p.is_file():
        raise FileNotFoundError(src_p)

    vault_root_p = Path(vault_root)
    if manifest_path is None:
        manifest_path_p = vault_root_p / "manifests" / "mu_manifest.jsonl"
    else:
        manifest_path_p = Path(manifest_path)

    mu = _load_mu(src_p)
    mu_id = mu.get("mu_id") or mu.get("id")
    schema_version = mu.get("schema_version")
    content_hash = mu.get("content_hash")
    idem = mu.get("idempotency") if isinstance(mu.get("idempotency"), dict) else {}
    mu_key = idem.get("mu_key")

    if not isinstance(mu_id, str) or not mu_id:
        raise ValueError("missing mu_id")
    if not isinstance(schema_version, str) or not schema_version:
        raise ValueError("missing schema_version")
    if not isinstance(content_hash, str) or not content_hash.startswith("sha256:"):
        raise ValueError("missing/invalid content_hash")
    if not isinstance(mu_key, str) or not mu_key.startswith("sha256:"):
        raise ValueError("missing/invalid idempotency.mu_key")

    # source_raw_ids best-effort: pointer sha256 values (raw files)
    source_raw_ids: list[str] = []
    pointers = mu.get("pointer")
    if isinstance(pointers, list):
        for p in pointers:
            if isinstance(p, dict):
                s = p.get("sha256")
                if isinstance(s, str) and s.startswith("sha256:"):
                    source_raw_ids.append(s)
    # stable unique
    source_raw_ids = sorted(set(source_raw_ids))

    rel = _dest_relpath_for_mu(mu_id=mu_id)
    dest_path = vault_root_p / "mu" / rel
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    if not dest_path.exists():
        if copy_mode == "copy2":
            shutil.copy2(src_p, dest_path)
        elif copy_mode == "copy":
            shutil.copy(src_p, dest_path)
        else:
            raise ValueError(f"unknown copy_mode: {copy_mode}")

    uri = f"vault://{vault_id}/mu/{rel.as_posix()}"

    rec = {
        "mu_id": mu_id,
        "schema_version": schema_version,
        "uri": uri,
        "source_raw_ids": source_raw_ids,
        "mu_key": mu_key,
        "content_hash": content_hash,
        "created_at": _utc_now_iso(),
    }
    append_jsonl(manifest_path_p, rec)

    return IngestMuResult(
        mu_id=mu_id, uri=uri, dest_path=dest_path, manifest_path=manifest_path_p
    )


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--in", dest="inp", required=True, help="Input .mimo file or directory"
    )
    ap.add_argument("--vault-root", required=True)
    ap.add_argument("--vault-id", default="default")
    ap.add_argument("--manifest", default=None)
    ap.add_argument("--copy-mode", choices=["copy2", "copy"], default="copy2")
    ns = ap.parse_args(argv)

    inp = Path(ns.inp)
    if not inp.exists():
        raise SystemExit(f"missing input: {inp}")

    def iter_files(p: Path):
        if p.is_file():
            yield p
        else:
            for q in sorted(p.rglob("*.mimo")):
                if q.is_file():
                    yield q

    count = 0
    for f in iter_files(inp):
        ingest_mu_file(
            f,
            vault_root=ns.vault_root,
            vault_id=ns.vault_id,
            copy_mode=ns.copy_mode,
            manifest_path=ns.manifest,
        )
        count += 1

    print(f"ingested_mu_files={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
