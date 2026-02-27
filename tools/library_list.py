"""List workspace library (no UI).

MVP goal (G-6): provide a stable CLI that answers:
  "In workspace ws_design, what MU do we have?"

Usage:
  python -m tools.library_list \
    --db "C:/memobrain/data/memory_system/index/meta.sqlite" \
    --workspace ws_design \
    --limit 50

Output: JSON list of MU rows in the workspace fence (canonicalized).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from tools.meta_db import connect, init_db


@dataclass(frozen=True)
class LibraryRow:
    mu_id: str
    time: str | None
    summary: str | None
    privacy_level: str | None
    path: str | None


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--data-root", default=None)
    p.add_argument("--workspace", required=True)
    p.add_argument("--limit", type=int, default=50)
    ns = p.parse_args(argv)

    from tools.membership import (
        canonicalize_mu_ids_single_hop,
        infer_data_root_from_db,
        load_effective_membership,
    )

    db_path = Path(ns.db)
    init_db(db_path)

    data_root = Path(ns.data_root) if ns.data_root else infer_data_root_from_db(db_path)

    effective_set, membership_diag = load_effective_membership(
        data_root=data_root, workspace_id=str(ns.workspace)
    )
    canonical_set, canon_diag = canonicalize_mu_ids_single_hop(
        db_path=db_path, mu_ids=effective_set
    )

    if not canonical_set:
        obj = {
            "db": str(db_path),
            "data_root": str(data_root),
            "workspace": str(ns.workspace),
            "membership": {
                **membership_diag.__dict__,
                "canonicalized_count": len(canonical_set),
                "canonicalization": canon_diag,
            },
            "count": 0,
            "items": [],
        }
        print(json.dumps(obj, ensure_ascii=False, indent=2))
        return 0

    ids = sorted(canonical_set)
    params: dict[str, Any] = {}
    ph = []
    for i, mid in enumerate(ids):
        k = f"id_{i}"
        ph.append(f":{k}")
        params[k] = mid

    q = (
        "SELECT mu_id, time, summary, privacy_level, path "
        "FROM mu WHERE mu_id IN (" + ",".join(ph) + ") "
        "ORDER BY time DESC NULLS LAST LIMIT :limit"
    )
    params["limit"] = int(ns.limit)

    items: list[LibraryRow] = []
    with connect(db_path) as conn:
        rows = conn.execute(q, params).fetchall()
        for r in rows:
            items.append(
                LibraryRow(
                    mu_id=str(r["mu_id"]),
                    time=str(r["time"]) if r["time"] is not None else None,
                    summary=str(r["summary"]) if r["summary"] is not None else None,
                    privacy_level=str(r["privacy_level"])
                    if r["privacy_level"] is not None
                    else None,
                    path=str(r["path"]) if r["path"] is not None else None,
                )
            )

    obj = {
        "db": str(db_path),
        "data_root": str(data_root),
        "workspace": str(ns.workspace),
        "membership": {
            **membership_diag.__dict__,
            "canonicalized_count": len(canonical_set),
            "canonicalization": canon_diag,
        },
        "count": len(items),
        "items": [asdict(i) for i in items],
    }
    print(json.dumps(obj, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
