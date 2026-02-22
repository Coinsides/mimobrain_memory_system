"""MU export tool (P0-8 / P0-G) v0.1.

Exports MU records (YAML .mimo) to a redacted JSONL suitable for sharing.

Goals:
- Respect MU privacy policy fields:
  - privacy.level (private|org|public)
  - privacy.redact (none|light|heavy)
  - privacy.share_policy.allow_snapshot
  - privacy.share_policy.allow_pointer
- Ensure public/org exports do not leak local absolute paths.

This is intentionally conservative:
- Unless allow_pointer=true, pointer is removed.
- Unless allow_snapshot=true, snapshot.payload is removed.
- For target_level=public: always strip any pointer uri that looks like a local file path.

Usage:
  python tools/export_mu.py --in <dir_or_file> --out exported.jsonl --target-level public
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable

import yaml


LOCAL_PATH_RE = re.compile(r"^[A-Za-z]:\\|^/|^file://", re.IGNORECASE)


def iter_mimo_files(path: Path) -> Iterable[Path]:
    if path.is_file():
        yield path
        return
    for p in path.rglob("*.mimo"):
        if p.is_file():
            yield p


def load_mu(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8", errors="ignore"))


def target_rank(level: str) -> int:
    # Higher rank = more restrictive
    return {"public": 0, "org": 1, "private": 2}.get(level, 99)


def sanitize_pointer(pointer: list, *, target_level: str) -> list:
    out: list = []
    for item in pointer:
        if not isinstance(item, dict):
            continue
        uri = item.get("uri")
        if target_level in {"org", "public"} and isinstance(uri, str):
            if LOCAL_PATH_RE.search(uri) or LOCAL_PATH_RE.search(item.get("path", "") if isinstance(item.get("path"), str) else ""):
                # drop anything that looks like a local path
                continue
        out.append(item)
    return out


def redact_mu(mu: dict, *, target_level: str) -> dict:
    mu = json.loads(json.dumps(mu))  # deep copy

    privacy = mu.get("privacy") if isinstance(mu.get("privacy"), dict) else {}
    level = privacy.get("level", "private")
    redact = privacy.get("redact", "none")
    share = privacy.get("share_policy") if isinstance(privacy.get("share_policy"), dict) else {}

    allow_snapshot = bool(share.get("allow_snapshot", False))
    allow_pointer = bool(share.get("allow_pointer", False))

    # Enforce: do not export higher-sensitivity data into lower target.
    if target_rank(level) > target_rank(target_level):
        # Still return a tombstone-ish stub so the consumer can see it existed.
        return {
            "mu_id": mu.get("mu_id") or mu.get("id"),
            "schema_version": mu.get("schema_version"),
            "export": {"skipped": True, "reason": f"privacy.level={level} > target={target_level}"},
        }

    # Pointer
    if not allow_pointer:
        mu["pointer"] = []
    else:
        if isinstance(mu.get("pointer"), list):
            mu["pointer"] = sanitize_pointer(mu["pointer"], target_level=target_level)

    # Snapshot
    snap = mu.get("snapshot")
    if isinstance(snap, dict):
        if not allow_snapshot:
            if "payload" in snap:
                snap["payload"] = {}
            snap["export_note"] = "snapshot payload removed by share_policy"
        # Always strip file:// in source_ref.uri for org/public
        src = snap.get("source_ref")
        if target_level in {"org", "public"} and isinstance(src, dict):
            u = src.get("uri")
            if isinstance(u, str) and LOCAL_PATH_RE.search(u):
                src["uri"] = "<REDACTED_URI>"

    # Redaction levels (minimal implementation)
    if redact in {"light", "heavy"}:
        # Heavy: also blank summary
        if redact == "heavy":
            if isinstance(mu.get("summary"), str):
                mu["summary"] = "[REDACTED]"

    # Never export absolute paths if present in legacy pointer fields
    if isinstance(mu.get("pointer"), list):
        cleaned = []
        for it in mu["pointer"]:
            if isinstance(it, dict) and isinstance(it.get("path"), str) and LOCAL_PATH_RE.search(it["path"]):
                it = dict(it)
                it["path"] = "<REDACTED_PATH>"
            cleaned.append(it)
        mu["pointer"] = cleaned

    mu["export"] = {
        "target_level": target_level,
        "applied": True,
        "allow_pointer": allow_pointer,
        "allow_snapshot": allow_snapshot,
    }
    return mu


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", required=True, help="Input MU file or directory")
    p.add_argument("--out", required=True, help="Output JSONL path")
    p.add_argument("--target-level", required=True, choices=["private", "org", "public"])
    ns = p.parse_args(argv)

    in_path = Path(ns.inp)
    out_path = Path(ns.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with out_path.open("w", encoding="utf-8") as f:
        for pth in iter_mimo_files(in_path):
            mu = load_mu(pth)
            if not isinstance(mu, dict):
                continue
            red = redact_mu(mu, target_level=ns.target_level)
            f.write(json.dumps(red, ensure_ascii=False) + "\n")
            count += 1

    print(f"exported={count} to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
