from tools.granularity import CompiledSpec, downgrade_for_budget, estimate_tokens


def test_downgrade_order_evidence_then_detail_then_time_then_scope():
    spec = CompiledSpec(
        template="t",
        scope_days=30,
        granularity={"detail_level": "forensic", "time_resolution": "event", "evidence_depth": "mu_snippets"},
        budget={"max_mu": 120, "max_tokens": 600},
    )

    before = estimate_tokens(spec)
    assert before > 600

    out = downgrade_for_budget(spec)
    assert estimate_tokens(out) <= 600

    # evidence must be downgraded first (snippets -> ids)
    assert out.granularity["evidence_depth"] == "mu_ids"


def test_downgrade_can_shrink_scope_as_last_resort():
    spec = CompiledSpec(
        template="t",
        scope_days=60,
        granularity={"detail_level": "overview", "time_resolution": "week", "evidence_depth": "mu_ids"},
        budget={"max_mu": 200, "max_tokens": 250},
    )

    out = downgrade_for_budget(spec)
    assert out.scope_days < 60
    assert out.scope_days >= 1
