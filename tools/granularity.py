"""GranularitySpec compiler + budget downgrade planner (P1-C).

At this stage we keep the spec minimal and deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass


def _rank_detail(x: str) -> int:
    return {"overview": 0, "normal": 1, "detailed": 2, "forensic": 3}.get(x, 1)


def _rank_time_res(x: str) -> int:
    return {"week": 0, "day": 1, "session": 2, "event": 3}.get(x, 1)


def _rank_evidence(x: str) -> int:
    return {"mu_ids": 0, "mu_snippets": 1}.get(x, 0)


DETAIL_ORDER = ["forensic", "detailed", "normal", "overview"]
TIME_RES_ORDER = ["event", "session", "day", "week"]
EVIDENCE_ORDER = ["mu_snippets", "mu_ids"]


@dataclass(frozen=True)
class CompiledSpec:
    template: str
    scope_days: int
    granularity: dict
    budget: dict


def merge_spec(*, template_name: str, template_defaults: dict, question_setup: dict | None, question_expect: dict | None, question_budget: dict | None) -> CompiledSpec:
    defaults = template_defaults or {}
    setup = question_setup or {}
    expect = question_expect or {}
    budget_in = question_budget or {}

    # Scope: question overrides template.
    scope_days = defaults.get("scope_days")
    scope_days = int(scope_days) if isinstance(scope_days, int) else 7

    scope = setup.get("scope") if isinstance(setup.get("scope"), dict) else {}
    twd = scope.get("time_window_days")
    if isinstance(twd, int):
        scope_days = int(twd)

    # Granularity: template defaults, then evidence expectations override.
    g = defaults.get("granularity") if isinstance(defaults.get("granularity"), dict) else {}
    detail_level = g.get("detail_level") if isinstance(g.get("detail_level"), str) else "normal"
    time_resolution = g.get("time_resolution") if isinstance(g.get("time_resolution"), str) else "day"
    evidence_depth = g.get("evidence_depth") if isinstance(g.get("evidence_depth"), str) else "mu_ids"

    ev = expect.get("evidence") if isinstance(expect.get("evidence"), dict) else {}
    if isinstance(ev.get("depth"), str) and ev.get("depth") in {"mu_ids", "mu_snippets"}:
        evidence_depth = ev.get("depth")

    # Budget: template defaults, then question budget can tighten.
    b = defaults.get("budget") if isinstance(defaults.get("budget"), dict) else {}
    max_mu = b.get("max_mu")
    max_mu = int(max_mu) if isinstance(max_mu, int) else 50
    max_tokens = b.get("max_tokens")
    max_tokens = int(max_tokens) if isinstance(max_tokens, int) else 900

    if isinstance(budget_in.get("max_tokens"), int):
        max_tokens = min(max_tokens, int(budget_in.get("max_tokens")))

    # Deterministic downgrade policy (budget-first). This is a stub: we don't estimate tokens yet.
    # Keep it here to make the order explicit and testable.
    gran = {
        "detail_level": detail_level,
        "time_resolution": time_resolution,
        "evidence_depth": evidence_depth,
    }
    budget = {"max_mu": max_mu, "max_tokens": max_tokens}

    return CompiledSpec(template=template_name, scope_days=scope_days, granularity=gran, budget=budget)


def estimate_tokens(spec: CompiledSpec) -> int:
    """Cheap deterministic token estimator.

    This is not meant to be accurate; it is meant to be stable so that we can
    apply a deterministic downgrade order before a real orchestrator exists.
    """

    g = spec.granularity or {}
    b = spec.budget or {}

    detail = str(g.get("detail_level") or "normal")
    time_res = str(g.get("time_resolution") or "day")
    evidence = str(g.get("evidence_depth") or "mu_ids")

    max_mu = b.get("max_mu")
    max_mu = int(max_mu) if isinstance(max_mu, int) else 50

    base = 220

    # evidence cost dominates
    per_mu = 18 if evidence == "mu_ids" else 55

    # more detail/time means more sections/verbosity
    detail_boost = {"overview": 0, "normal": 120, "detailed": 260, "forensic": 420}.get(detail, 120)
    time_boost = {"week": 0, "day": 80, "session": 160, "event": 260}.get(time_res, 80)

    scope_boost = int(min(600, max(0, spec.scope_days - 7) * 18))

    return int(base + max_mu * per_mu + detail_boost + time_boost + scope_boost)


def _downgrade_evidence(g: dict) -> dict:
    cur = str(g.get("evidence_depth") or "mu_ids")
    if cur == "mu_snippets":
        ng = dict(g)
        ng["evidence_depth"] = "mu_ids"
        return ng
    return g


def _downgrade_detail(g: dict) -> dict:
    cur = str(g.get("detail_level") or "normal")
    order = DETAIL_ORDER
    if cur in order:
        i = order.index(cur)
        if i + 1 < len(order):
            ng = dict(g)
            ng["detail_level"] = order[i + 1]
            return ng
    return g


def _downgrade_time(g: dict) -> dict:
    cur = str(g.get("time_resolution") or "day")
    order = TIME_RES_ORDER
    if cur in order:
        i = order.index(cur)
        if i + 1 < len(order):
            ng = dict(g)
            ng["time_resolution"] = order[i + 1]
            return ng
    return g


def _shrink_scope_days(days: int) -> int:
    # last resort (before reducing max_mu): shrink scope by half, but keep >= 1
    return max(1, int((days + 1) // 2))


def _shrink_max_mu(n: int) -> int:
    # absolute last resort: reduce evidence set size.
    return max(1, int((n + 1) // 2))


def downgrade_for_budget(spec: CompiledSpec) -> CompiledSpec:
    """Apply deterministic downgrade policy until under max_tokens.

    Order (fixed):
      1) evidence_depth (mu_snippets -> mu_ids)
      2) detail_level (forensic -> detailed -> normal -> overview)
      3) time_resolution (event -> session -> day -> week)
      4) scope_days shrink (halve until 1)
      5) max_mu shrink (halve until 1)  # absolute last resort

    This is a planner stub for P1-C; later we can replace estimate_tokens with
    measured stats.
    """

    max_tokens = spec.budget.get("max_tokens") if isinstance(spec.budget, dict) else None
    max_tokens = int(max_tokens) if isinstance(max_tokens, int) else None
    if not max_tokens:
        return spec

    cur = spec
    for _ in range(32):
        if estimate_tokens(cur) <= max_tokens:
            return cur

        g = cur.granularity or {}

        # 1) evidence
        ng = _downgrade_evidence(g)
        if ng is not g:
            cur = CompiledSpec(template=cur.template, scope_days=cur.scope_days, granularity=ng, budget=cur.budget)
            continue

        # 2) detail
        ng = _downgrade_detail(g)
        if ng is not g:
            cur = CompiledSpec(template=cur.template, scope_days=cur.scope_days, granularity=ng, budget=cur.budget)
            continue

        # 3) time
        ng = _downgrade_time(g)
        if ng is not g:
            cur = CompiledSpec(template=cur.template, scope_days=cur.scope_days, granularity=ng, budget=cur.budget)
            continue

        # 4) shrink scope
        ndays = _shrink_scope_days(int(cur.scope_days))
        if ndays != cur.scope_days:
            cur = CompiledSpec(template=cur.template, scope_days=ndays, granularity=cur.granularity, budget=cur.budget)
            continue

        # 5) shrink max_mu (absolute last resort)
        b = cur.budget if isinstance(cur.budget, dict) else {}
        cur_max_mu = b.get("max_mu")
        cur_max_mu = int(cur_max_mu) if isinstance(cur_max_mu, int) else 50
        nmu = _shrink_max_mu(cur_max_mu)
        if nmu != cur_max_mu:
            nb = dict(b)
            nb["max_mu"] = nmu
            cur = CompiledSpec(template=cur.template, scope_days=cur.scope_days, granularity=cur.granularity, budget=nb)
            continue

        return cur

    return cur
