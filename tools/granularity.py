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


def downgrade_for_budget(spec: CompiledSpec) -> CompiledSpec:
    # Placeholder: nothing to do yet, but keep stable API.
    return spec
