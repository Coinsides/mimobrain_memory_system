"""Index MU (.mimo YAML) into meta.sqlite (P1-A).

Usage:
  python tools/index_mu.py --mu-root <dir> --db <meta.sqlite> [--reset]

Notes:
- This is a full rebuild by default (append-only source; index is derived).
- Runtime dependency: PyYAML.
"""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from tools.meta_db import connect, init_db, reset_db


def iter_mimo_files(root: Path):
    for p in root.rglob("*.mimo"):
        if p.is_file():
            yield p


def parse_tags(mu: dict) -> list[str]:
    tags = mu.get("tags")
    if isinstance(tags, list):
        return [str(t) for t in tags if isinstance(t, (str, int, float))]
    meta = mu.get("meta") if isinstance(mu.get("meta"), dict) else {}
    tags2 = meta.get("tags")
    if isinstance(tags2, list):
        return [str(t) for t in tags2 if isinstance(t, (str, int, float))]
    return []


def index_mu_dir(mu_root: Path, db_path: Path, *, reset: bool = False) -> dict:
    if reset:
        reset_db(db_path)
    else:
        init_db(db_path)

    count = 0
    with connect(db_path) as conn:
        for path in iter_mimo_files(mu_root):
            try:
                mu = yaml.safe_load(path.read_text(encoding="utf-8", errors="ignore"))
            except Exception:
                continue
            if not isinstance(mu, dict):
                continue

            mu_id = mu.get("mu_id") or mu.get("id")
            if not isinstance(mu_id, str) or not mu_id:
                continue

            meta = mu.get("meta") if isinstance(mu.get("meta"), dict) else {}
            source = meta.get("source") if isinstance(meta.get("source"), dict) else {}
            privacy = mu.get("privacy") if isinstance(mu.get("privacy"), dict) else {}
            links = mu.get("links") if isinstance(mu.get("links"), dict) else {}

            time = meta.get("time")
            summary = mu.get("summary")
            content_hash = mu.get("content_hash")
            idem = (
                mu.get("idempotency") if isinstance(mu.get("idempotency"), dict) else {}
            )
            mu_key = idem.get("mu_key")
            privacy_level = privacy.get("level")
            corrects = links.get("corrects")
            tombstone = mu.get("tombstone")

            st = path.stat()
            mtime = st.st_mtime

            conn.execute(
                """
                INSERT OR REPLACE INTO mu
                  (mu_id, time, summary, content_hash, mu_key, privacy_level, corrects_json, tombstone_json,
                   source_kind, source_note, path, mtime)
                VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    mu_id,
                    time if isinstance(time, str) else None,
                    summary if isinstance(summary, str) else None,
                    content_hash if isinstance(content_hash, str) else None,
                    mu_key if isinstance(mu_key, str) else None,
                    privacy_level if isinstance(privacy_level, str) else None,
                    json.dumps(corrects, ensure_ascii=False)
                    if corrects is not None
                    else None,
                    json.dumps(tombstone, ensure_ascii=False)
                    if tombstone is not None
                    else None,
                    source.get("kind") if isinstance(source.get("kind"), str) else None,
                    source.get("note") if isinstance(source.get("note"), str) else None,
                    str(path),
                    float(mtime),
                ),
            )

            tags = parse_tags(mu)
            for t in tags:
                conn.execute("INSERT OR IGNORE INTO tag(tag) VALUES (?)", (t,))
                conn.execute(
                    "INSERT OR IGNORE INTO mu_tag(mu_id, tag) VALUES (?, ?)", (mu_id, t)
                )

            count += 1

        conn.commit()

    return {"indexed": count}


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--mu-root", required=True)
    p.add_argument("--db", required=True)
    p.add_argument("--reset", action="store_true")
    ns = p.parse_args(argv)

    mu_root = Path(ns.mu_root)
    db_path = Path(ns.db)

    if not mu_root.exists():
        raise SystemExit(f"missing --mu-root: {mu_root}")

    out = index_mu_dir(mu_root, db_path, reset=bool(ns.reset))
    print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
