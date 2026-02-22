"""Search MU index (meta.sqlite) (P1-B).

Usage:
  python tools/search_mu.py --db <meta.sqlite> --query "..." --limit 20
  python tools/search_mu.py --db <meta.sqlite> --query "..." --since 2026-01-01 --until 2026-02-01
  python tools/search_mu.py --db <meta.sqlite> --tag travel --privacy private

Output: JSON with results (mu_id + reason + summary preview).

Notes:
- Uses FTS5 over mu_fts.summary.
- Time filtering is string-based; expects ISO timestamps where lexical order matches time.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from tools.meta_db import connect, init_db


@dataclass
class SearchResult:
    mu_id: str
    score: float | None
    summary: str | None
    reason: dict


def _rank_privacy(level: str | None) -> int:
    # Higher rank = more restrictive
    return {"public": 0, "org": 1, "private": 2}.get(level or "private", 2)


def _make_snippet(summary: str | None, query: str | None, *, max_chars: int = 220) -> str | None:
    if not summary:
        return None
    s = summary.strip()
    if len(s) <= max_chars:
        return s
    if query and query.strip():
        q = query.strip()
        i = s.lower().find(q.lower())
        if i >= 0:
            start = max(0, i - 60)
            end = min(len(s), i + len(q) + 120)
            chunk = s[start:end]
            if start > 0:
                chunk = "…" + chunk
            if end < len(s):
                chunk = chunk + "…"
            return chunk
    return s[: max_chars - 1] + "…"


def search_mu(
    db_path: Path,
    *,
    query: str | None = None,
    since: str | None = None,
    until: str | None = None,
    tag: str | None = None,
    privacy: str | None = None,
    target_level: str = "private",
    include_snippet: bool = False,
    limit: int = 20,
) -> list[SearchResult]:
    init_db(db_path)

    params: dict[str, object] = {"limit": int(limit)}
    where = []
    joins = []

    # base select
    if query and query.strip():
        joins.append("JOIN mu_fts ON mu_fts.mu_id = mu.mu_id")
        where.append("mu_fts MATCH :q")
        params["q"] = query
        score_expr = "bm25(mu_fts)"
    else:
        score_expr = "NULL"

    if since:
        where.append("mu.time >= :since")
        params["since"] = since
    if until:
        where.append("mu.time <= :until")
        params["until"] = until

    if privacy:
        where.append("mu.privacy_level = :privacy")
        params["privacy"] = privacy

    if tag:
        joins.append("JOIN mu_tag ON mu_tag.mu_id = mu.mu_id")
        where.append("mu_tag.tag = :tag")
        params["tag"] = tag

    q = f"SELECT mu.mu_id, mu.summary, mu.privacy_level, {score_expr} as score FROM mu " + " ".join(joins)
    if where:
        q += " WHERE " + " AND ".join(where)

    if query and query.strip():
        q += " ORDER BY score ASC"
    else:
        q += " ORDER BY mu.time DESC NULLS LAST"

    q += " LIMIT :limit"

    out: list[SearchResult] = []
    with connect(db_path) as conn:
        rows = conn.execute(q, params).fetchall()

    for r in rows:
        mu_id = r[0]
        summary = r[1]
        privacy_level = r[2]
        score = r[3]

        # Enforce target-level visibility
        if _rank_privacy(str(privacy_level) if privacy_level is not None else None) > _rank_privacy(target_level):
            continue

        reason: dict = {"filters": {}}
        if query and query.strip():
            reason["fts"] = {"query": query, "bm25": score}
        if since or until:
            reason["filters"]["time"] = {"since": since, "until": until}
        if tag:
            reason["filters"]["tag"] = tag
        if privacy:
            reason["filters"]["privacy"] = privacy

        if include_snippet:
            reason["snippet"] = {"max_chars": 220}
            snippet = _make_snippet(summary, query)
        else:
            snippet = None

        out.append(
            SearchResult(
                mu_id=mu_id,
                score=score,
                summary=snippet if include_snippet else summary,
                reason=reason,
            )
        )

    return out


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--query", default=None)
    p.add_argument("--since", default=None)
    p.add_argument("--until", default=None)
    p.add_argument("--tag", default=None)
    p.add_argument("--privacy", default=None, choices=[None, "private", "org", "public"])
    p.add_argument("--target-level", default="private", choices=["private", "org", "public"], help="Visibility level for returned results/snippets")
    p.add_argument("--snippets", action="store_true", help="Include snippet text (summary-based) in output")
    p.add_argument("--limit", type=int, default=20)
    ns = p.parse_args(argv)

    res = search_mu(
        Path(ns.db),
        query=ns.query,
        since=ns.since,
        until=ns.until,
        tag=ns.tag,
        privacy=ns.privacy,
        target_level=ns.target_level,
        include_snippet=bool(ns.snippets),
        limit=ns.limit,
    )

    obj = {
        "db": ns.db,
        "query": ns.query,
        "filters": {"since": ns.since, "until": ns.until, "tag": ns.tag, "privacy": ns.privacy},
        "results": [
            {"mu_id": r.mu_id, "score": r.score, "summary": r.summary, "reason": r.reason} for r in res
        ],
    }
    print(json.dumps(obj, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
