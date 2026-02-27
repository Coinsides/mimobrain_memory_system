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
    workspace: str,
    data_root: Path | None = None,
    days: int = 7,
    template: str = "time_overview_v1",
    target_level: str = "private",
    evidence_depth: str = "mu_ids",  # mu_ids|mu_snippets|raw_quotes
    limit: int = 50,
    # P1-C: optional template/spec inputs (preferred)
    template_name: str | None = None,
    question_setup: dict | None = None,
    question_expect: dict | None = None,
    question_budget: dict | None = None,
    include_diagnostics: bool = True,
    # P1-G: optional evidence backtrace inputs
    vault_roots: dict[str, str] | None = None,
    raw_manifest_path: str | Path | None = None,
) -> dict:
    diagnostics: dict | None = None

    # If template_name provided, compile spec here (so Golden and future callers share one path).
    if template_name:
        from tools.granularity import merge_spec, plan_downgrades
        from tools.templates import load_and_validate_template

        tmpl = load_and_validate_template(str(template_name))
        compiled = merge_spec(
            template_name=str(template_name),
            template_defaults=tmpl.get("defaults")
            if isinstance(tmpl.get("defaults"), dict)
            else {},
            question_setup=question_setup,
            question_expect=question_expect,
            question_budget=question_budget,
        )
        final_spec, plan = plan_downgrades(compiled, mode="bundle")

        template = final_spec.template
        days = int(final_spec.scope_days)
        evidence_depth = str(
            final_spec.granularity.get("evidence_depth") or evidence_depth
        )
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
    include_raw_quotes = evidence_depth == "raw_quotes"

    from tools.membership import (
        canonicalize_mu_ids_single_hop,
        infer_data_root_from_db,
        load_effective_membership,
    )

    dr = data_root if data_root is not None else infer_data_root_from_db(db_path)
    effective_set, membership_diag = load_effective_membership(
        data_root=dr, workspace_id=str(workspace)
    )
    canonical_set, canon_diag = canonicalize_mu_ids_single_hop(
        db_path=db_path, mu_ids=effective_set
    )

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
        allow_mu_ids=canonical_set,
    )

    evidence_degraded_mu_ids: list[str] = []
    repair_tasks: list[dict] = []

    def _maybe_attach_quote(r):
        """Best-effort: resolve pointer snippet (quote)."""
        nonlocal evidence_degraded_mu_ids
        if not include_raw_quotes:
            return {"mu_id": r.mu_id}

        # Need MU path + vault mapping to resolve.
        if not r.path:
            return {"mu_id": r.mu_id}

        try:
            import yaml
            from tools.pointer_resolve import resolve_pointer

            mu_obj = yaml.safe_load(Path(r.path).read_text(encoding="utf-8"))
            if not isinstance(mu_obj, dict):
                return {"mu_id": r.mu_id}

            pointers = mu_obj.get("pointer")
            snapshot = mu_obj.get("snapshot")

            # Attempt to resolve any pointer in order.
            br = vault_roots
            if not isinstance(br, dict):
                br = {}

            rm = raw_manifest_path

            snippet = None
            resolved_pointer = None
            last_fail_diag = None
            last_fail_ptr = None
            if isinstance(pointers, list):
                for p in pointers:
                    if not isinstance(p, dict):
                        continue
                    out = resolve_pointer(p, vault_roots=br, raw_manifest_path=rm)
                    if out.ok and out.snippet:
                        snippet = out.snippet
                        resolved_pointer = dict(p)
                        resolved_pointer["uri"] = out.uri
                        break
                    # keep last failure diagnostics for repair suggestions
                    last_fail_diag = out.diagnostics
                    last_fail_ptr = p

            if snippet is not None:
                ev = {"mu_id": r.mu_id, "snippet": snippet}
                if target_level == "private" and resolved_pointer is not None:
                    ev["pointer"] = [resolved_pointer]
                return ev

            # degraded if snapshot exists but pointer couldn't be resolved
            if snapshot is not None:
                evidence_degraded_mu_ids.append(r.mu_id)

            # repair trigger (minimal): record a suggested task
            if isinstance(last_fail_ptr, dict):
                repair_tasks.append(
                    {
                        "type": "REPAIR_POINTER",
                        "mu_id": r.mu_id,
                        "mu_path": r.path,
                        "sha256": last_fail_ptr.get("sha256"),
                        "uri": last_fail_ptr.get("uri"),
                        "reason": (last_fail_diag or {}).get("error")
                        if isinstance(last_fail_diag, dict)
                        else None,
                        "hint": {
                            "need_vault_roots": (not bool(br)),
                            "need_raw_manifest": (rm is None),
                        },
                    }
                )

            return {"mu_id": r.mu_id}
        except Exception:
            return {"mu_id": r.mu_id}

    mu_ids = [r.mu_id for r in results]

    bundle = {
        "bundle_id": default_bundle_id(),
        "template": template,
        "scope": {
            "time_window_days": int(days),
            "since": since,
            "workspace": str(workspace),
        },
        "source_mu_ids": mu_ids,
        "created_at": utc_now(),
        "expires_at": None,
        "always_on": None,
        "session_on": None,
        "query_on": {"query": query},
        "evidence": [
            (
                {"mu_id": r.mu_id, "snippet": (r.summary if include_snippet else None)}
                if include_snippet
                else _maybe_attach_quote(r)
            )
            for r in results
        ],
    }

    if diagnostics or evidence_degraded_mu_ids or repair_tasks:
        bundle.setdefault("diagnostics", {})
        if diagnostics:
            bundle["diagnostics"].update(diagnostics)
        bundle["diagnostics"].setdefault(
            "membership",
            {
                **membership_diag.__dict__,
                "canonicalized_count": len(canonical_set),
                "canonicalization": canon_diag,
            },
        )
        if vault_roots:
            bundle["diagnostics"].setdefault("vault_roots", vault_roots)
        if raw_manifest_path is not None:
            bundle["diagnostics"].setdefault("raw_manifest", str(raw_manifest_path))
        if evidence_degraded_mu_ids:
            bundle["diagnostics"]["evidence_degraded"] = True
            bundle["diagnostics"]["evidence_degraded_mu_ids"] = sorted(
                set(evidence_degraded_mu_ids)
            )
        if repair_tasks:
            bundle["diagnostics"]["repair_tasks"] = repair_tasks

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
    p.add_argument(
        "--data-root",
        default=None,
        help="DATA_ROOT (used to locate workspaces/membership.jsonl)",
    )
    p.add_argument(
        "--workspace", required=True, help="workspace scope (membership fence)"
    )
    p.add_argument("--config", default=None, help="Path to ms_config.json (optional)")
    p.add_argument("--query", required=True)
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--template", default="time_overview_v1")
    p.add_argument(
        "--target-level", default="private", choices=["private", "org", "public"]
    )
    p.add_argument(
        "--evidence-depth",
        default="mu_ids",
        choices=["mu_ids", "mu_snippets", "raw_quotes"],
        help="Evidence detail level",
    )
    p.add_argument("--limit", type=int, default=50)
    p.add_argument(
        "--vault-root",
        default=None,
        help="Optional physical vault root for pointer resolve (default vault_id only)",
    )
    p.add_argument(
        "--raw-manifest",
        default=None,
        help="Optional raw_manifest.jsonl path for legacy pointer resolve",
    )
    p.add_argument("--out", required=True)
    ns = p.parse_args(argv)

    vault_roots = None
    raw_manifest_path = None

    if ns.config:
        from tools.ms_config import load_config

        cfg = load_config(ns.config)
        vault_roots = cfg.get("vault_roots")
        raw_manifest_path = cfg.get("raw_manifest_path")

    # explicit flags override config
    if ns.vault_root:
        vault_roots = {"default": str(ns.vault_root)}
    if ns.raw_manifest:
        raw_manifest_path = str(Path(ns.raw_manifest))

    out = build_bundle(
        db_path=Path(ns.db),
        data_root=(Path(ns.data_root) if ns.data_root else None),
        workspace=str(ns.workspace),
        query=ns.query,
        days=int(ns.days),
        template=ns.template,
        target_level=ns.target_level,
        evidence_depth=ns.evidence_depth,
        limit=int(ns.limit),
        vault_roots=vault_roots,
        raw_manifest_path=(Path(raw_manifest_path) if raw_manifest_path else None),
    )

    out_path = Path(ns.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
