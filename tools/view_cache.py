"""View cache (P1-D) backed by meta.sqlite.

A view is a cached, reusable rendering of some scope/template over a set of MU ids.

Key idea: avoid stale/hallucinated cache by recording dependencies:
- source_mu_ids
- optional source_mu_hash (future)

We implement minimal operations:
- put_view
- get_view
- invalidate_by_mu_ids

"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from tools.meta_db import connect, init_db


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_text(s: str) -> str:
    h = hashlib.sha256(s.encode("utf-8"))
    return "sha256:" + h.hexdigest()


@dataclass
class ViewRecord:
    view_id: str
    template: str
    scope: dict
    source_mu_ids: list[str]
    created_at: str
    expires_at: str | None
    stale: bool
    content: dict


def put_view(
    db_path: Path,
    *,
    view_id: str,
    template: str,
    scope: dict,
    source_mu_ids: list[str],
    content: dict,
    expires_at: str | None = None,
) -> None:
    init_db(db_path)

    scope_json = json.dumps(scope, ensure_ascii=False, sort_keys=True)
    src_json = json.dumps(sorted(source_mu_ids), ensure_ascii=False)
    source_mu_hash = sha256_text(scope_json + "|" + src_json)

    row = {
        "view_id": view_id,
        "template": template,
        "scope_json": scope_json,
        "source_mu_ids_json": src_json,
        "source_mu_hash": source_mu_hash,
        "created_at": utc_now(),
        "expires_at": expires_at,
        "stale": 0,
        "content_json": json.dumps(content, ensure_ascii=False),
    }

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO view_cache
              (view_id, template, scope_json, source_mu_ids_json, source_mu_hash, created_at, expires_at, stale, content_json)
            VALUES
              (:view_id, :template, :scope_json, :source_mu_ids_json, :source_mu_hash, :created_at, :expires_at, :stale, :content_json)
            """,
            row,
        )
        conn.commit()


def get_view(db_path: Path, view_id: str) -> ViewRecord | None:
    init_db(db_path)
    with connect(db_path) as conn:
        r = conn.execute(
            "SELECT view_id, template, scope_json, source_mu_ids_json, created_at, expires_at, stale, content_json FROM view_cache WHERE view_id=?",
            (view_id,),
        ).fetchone()
    if not r:
        return None
    return ViewRecord(
        view_id=r[0],
        template=r[1],
        scope=json.loads(r[2]),
        source_mu_ids=json.loads(r[3]),
        created_at=r[4],
        expires_at=r[5],
        stale=bool(r[6]),
        content=json.loads(r[7]),
    )


def invalidate_by_mu_ids(db_path: Path, changed_mu_ids: list[str]) -> int:
    """Mark views stale if their dependency set intersects changed_mu_ids.

    v0.1 implementation: brute-force scan.
    """

    init_db(db_path)
    changed = set(changed_mu_ids)
    if not changed:
        return 0

    to_stale: list[str] = []
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT view_id, source_mu_ids_json FROM view_cache WHERE stale=0"
        ).fetchall()
        for r in rows:
            view_id = r[0]
            deps = set(json.loads(r[1]))
            if deps & changed:
                to_stale.append(view_id)

        for vid in to_stale:
            conn.execute("UPDATE view_cache SET stale=1 WHERE view_id=?", (vid,))
        conn.commit()

    return len(to_stale)


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    p_put = sub.add_parser("put")
    p_put.add_argument("--db", required=True)
    p_put.add_argument("--view-id", required=True)
    p_put.add_argument("--template", required=True)
    p_put.add_argument("--scope", required=True, help="JSON")
    p_put.add_argument("--source-mu-ids", required=True, help="JSON array")
    p_put.add_argument("--content", required=True, help="JSON")

    p_get = sub.add_parser("get")
    p_get.add_argument("--db", required=True)
    p_get.add_argument("--view-id", required=True)

    p_inv = sub.add_parser("invalidate")
    p_inv.add_argument("--db", required=True)
    p_inv.add_argument("--changed", required=True, help="JSON array of mu_ids")

    ns = p.parse_args(argv)
    db = Path(ns.db)

    if ns.cmd == "put":
        put_view(
            db,
            view_id=ns.view_id,
            template=ns.template,
            scope=json.loads(ns.scope),
            source_mu_ids=json.loads(ns.source_mu_ids),
            content=json.loads(ns.content),
        )
        print("OK")
        return 0

    if ns.cmd == "get":
        v = get_view(db, ns.view_id)
        if not v:
            print("NOT_FOUND")
            return 2
        print(json.dumps(v.__dict__, ensure_ascii=False, indent=2))
        return 0

    if ns.cmd == "invalidate":
        n = invalidate_by_mu_ids(db, json.loads(ns.changed))
        print(json.dumps({"invalidated": n}, ensure_ascii=False))
        return 0

    raise SystemExit("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())
