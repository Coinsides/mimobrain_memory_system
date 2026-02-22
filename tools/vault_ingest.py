"""Vault ingest (P1-G / TASK-INGEST-001) — minimal file→vault import.

Scope (v0.1):
- Ingest local files into a vault root under kind=raw.
- Compute sha256 and use it as raw_id.
- Write/append a raw_manifest.jsonl line (append-only) that validates against
  raw_manifest_line_v0_1.schema.json.

Notes:
- This tool is deliberately deterministic.
- Directory ingest is supported (recursive file walk).
"""

from __future__ import annotations

import mimetypes
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from tools.manifest_io import append_jsonl
from tools.vault_ops import sha256_file


@dataclass(frozen=True)
class IngestResult:
    raw_id: str
    uri: str
    dest_path: Path
    manifest_path: Path


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _mtime_iso(p: Path) -> str | None:
    try:
        ts = p.stat().st_mtime
    except Exception:
        return None
    return datetime.fromtimestamp(ts, timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _guess_mime(p: Path) -> str:
    mt, _ = mimetypes.guess_type(str(p))
    return mt or "application/octet-stream"


def _dest_relpath_for_raw(*, raw_hex: str, src: Path) -> Path:
    # v0.1: year/month + full sha256 hex + original suffix (if any)
    now = datetime.now(timezone.utc)
    suffix = src.suffix.lower()
    # normalize very long suffix chains (".tar.gz")? keep last suffix only for v0.1
    return Path(f"{now.year:04d}") / f"{now.month:02d}" / f"{raw_hex}{suffix}"


def ingest_file(
    src: str | Path,
    *,
    vault_root: str | Path,
    vault_id: str = "default",
    copy_mode: str = "copy2",
    manifest_path: str | Path | None = None,
) -> IngestResult:
    src_p = Path(src)
    if not src_p.exists() or not src_p.is_file():
        raise FileNotFoundError(src_p)

    vault_root_p = Path(vault_root)
    if manifest_path is None:
        manifest_path_p = vault_root_p / "manifests" / "raw_manifest.jsonl"
    else:
        manifest_path_p = Path(manifest_path)

    sha = sha256_file(src_p)
    raw_id = sha
    raw_hex = sha.split(":", 1)[1]

    rel = _dest_relpath_for_raw(raw_hex=raw_hex, src=src_p)
    dest_path = vault_root_p / "raw" / rel
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    if not dest_path.exists():
        if copy_mode == "copy2":
            shutil.copy2(src_p, dest_path)
        elif copy_mode == "copy":
            shutil.copy(src_p, dest_path)
        else:
            raise ValueError(f"unknown copy_mode: {copy_mode}")

    uri = f"vault://{vault_id}/raw/{rel.as_posix()}"

    rec = {
        "raw_id": raw_id,
        "uri": uri,
        "sha256": sha,
        "size_bytes": int(dest_path.stat().st_size),
        "mtime": _mtime_iso(dest_path),
        "mime": _guess_mime(dest_path),
        "ingested_at": _utc_now_iso(),
    }
    append_jsonl(manifest_path_p, rec)

    return IngestResult(raw_id=raw_id, uri=uri, dest_path=dest_path, manifest_path=manifest_path_p)


def iter_files(inp: Path) -> Iterable[Path]:
    if inp.is_file():
        yield inp
        return
    for p in sorted(inp.rglob("*")):
        if p.is_file():
            yield p


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Input file or directory")
    ap.add_argument("--vault-root", required=True, help="Vault root directory (physical path)")
    ap.add_argument("--vault-id", default="default", help="Vault logical id (default: default)")
    ap.add_argument("--manifest", default=None, help="raw_manifest.jsonl path (default: <vault-root>/manifests/raw_manifest.jsonl)")
    ap.add_argument("--copy-mode", choices=["copy2", "copy"], default="copy2")
    ns = ap.parse_args(argv)

    inp = Path(ns.inp)
    if not inp.exists():
        raise SystemExit(f"missing input: {inp}")

    count = 0
    for p in iter_files(inp):
        ingest_file(p, vault_root=ns.vault_root, vault_id=ns.vault_id, copy_mode=ns.copy_mode, manifest_path=ns.manifest)
        count += 1

    print(f"ingested_files={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
