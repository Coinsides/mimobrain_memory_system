"""Build a minimal MemoryBundle v0.1 (P1-E).

This is a deterministic bundle builder that selects MU ids via search_mu and
emits a bundle aligned with docs/contracts/bundle_v0_1.schema.json.

Key invariants:
- bundle contains source_mu_ids (dependency set)
- evidence at least contains mu_id list
- no pointers/snapshot payload by default (privacy-safe)

Usage:
  python tools/build_bundle.py --db <meta.sqlite> --query "..." --out bundle.json
  python tools/build_bundle.py --db <meta.sqlite> --query "..." --days 7 --template time_overview_v1
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from tools.search_mu import search_mu


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_bundle_id() -> str:
    return "bndl_" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def iso_days_ago(days: int) -> str:
    t = datetime.now(timezone.utc) - timedelta(days=days)
    return t.isoformat().replace("+00:00", "Z")


def build_bundle(
    *,
    db_path: Path,
    query: str,
    days: int = 7,
    template: str = "time_overview_v1",
    target_level: str = "private",
    limit: int = 50,
) -> dict:
    since = iso_days_ago(days)

    results = search_mu(
        db_path,
        query=query,
        since=since,
        until=None,
        tag=None,
        privacy=None,
        target_level=target_level,
        include_snippet=False,
        limit=limit,
    )

    mu_ids = [r.mu_id for r in results]

    bundle = {
        "bundle_id": default_bundle_id(),
        "template": template,
        "scope": {"time_window_days": days, "since": since},
        "source_mu_ids": mu_ids,
        "created_at": utc_now(),
        "expires_at": None,
        "always_on": None,
        "session_on": None,
        "query_on": {"query": query},
        "evidence": [{"mu_id": mid} for mid in mu_ids],
    }

    # best-effort validate
    try:
        from tools.bundle_validate import validate_bundle

        errs = validate_bundle(bundle)
        if errs:
            bundle.setdefault("diagnostics", {})
            bundle["diagnostics"]["bundle_schema_errors"] = errs[:50]
    except Exception:
        pass

    return bundle


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--query", required=True)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--template", default="time_overview_v1")
    p.add_argument("--target-level", default="private", choices=["private", "org", "public"])
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--out", required=True)
    ns = p.parse_args(argv)

    out = build_bundle(
        db_path=Path(ns.db),
        query=ns.query,
        days=int(ns.days),
        template=ns.template,
        target_level=ns.target_level,
        limit=int(ns.limit),
    )

    out_path = Path(ns.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
