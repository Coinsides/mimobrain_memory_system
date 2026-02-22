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
    evidence_depth: str = "mu_ids",  # mu_ids|mu_snippets
    limit: int = 50,
    # P1-C: optional template/spec inputs (preferred)
    template_name: str | None = None,
    question_setup: dict | None = None,
    question_expect: dict | None = None,
    question_budget: dict | None = None,
    include_diagnostics: bool = True,
) -> dict:
    diagnostics: dict | None = None

    # If template_name provided, compile spec here (so Golden and future callers share one path).
    if template_name:
        from tools.granularity import merge_spec, plan_downgrades
        from tools.templates import load_and_validate_template

        tmpl = load_and_validate_template(str(template_name))
        compiled = merge_spec(
            template_name=str(template_name),
            template_defaults=tmpl.get("defaults") if isinstance(tmpl.get("defaults"), dict) else {},
            question_setup=question_setup,
            question_expect=question_expect,
            question_budget=question_budget,
        )
        final_spec, plan = plan_downgrades(compiled, mode="bundle")

        template = final_spec.template
        days = int(final_spec.scope_days)
        evidence_depth = str(final_spec.granularity.get("evidence_depth") or evidence_depth)
        limit = int(final_spec.budget.get("max_mu") or limit)

        if include_diagnostics:
            diagnostics = {
                "compiled_spec": {
                    "template": compiled.template,
                    "scope_days": compiled.scope_days,
                    "granularity": compiled.granularity,
                    "budget": compiled.budget,
                },
                "final_spec": {
                    "template": final_spec.template,
                    "scope_days": final_spec.scope_days,
                    "granularity": final_spec.granularity,
                    "budget": final_spec.budget,
                },
                "downgrade_plan": plan,
            }

    since = iso_days_ago(int(days))

    include_snippet = evidence_depth == "mu_snippets"

    results = search_mu(
        db_path,
        query=query,
        since=since,
        until=None,
        tag=None,
        privacy=None,
        target_level=target_level,
        include_snippet=include_snippet,
        limit=int(limit),
    )

    mu_ids = [r.mu_id for r in results]

    bundle = {
        "bundle_id": default_bundle_id(),
        "template": template,
        "scope": {"time_window_days": int(days), "since": since},
        "source_mu_ids": mu_ids,
        "created_at": utc_now(),
        "expires_at": None,
        "always_on": None,
        "session_on": None,
        "query_on": {"query": query},
        "evidence": [
            ({"mu_id": r.mu_id, "snippet": (r.summary if include_snippet else None)} if include_snippet else {"mu_id": r.mu_id})
            for r in results
        ],
    }

    if diagnostics:
        bundle["diagnostics"] = diagnostics

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
    p.add_argument("--evidence-depth", default="mu_ids", choices=["mu_ids", "mu_snippets"], help="Evidence detail level")
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--out", required=True)
    ns = p.parse_args(argv)

    out = build_bundle(
        db_path=Path(ns.db),
        query=ns.query,
        days=int(ns.days),
        template=ns.template,
        target_level=ns.target_level,
        evidence_depth=ns.evidence_depth,
        limit=int(ns.limit),
    )

    out_path = Path(ns.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
