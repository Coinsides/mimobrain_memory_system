import json


def test_templates_validate_all_repo_templates():
    from tools.templates import template_schema_path, templates_dir, validate_template

    schema = json.loads(template_schema_path().read_text(encoding="utf-8"))

    for p in templates_dir().glob("*.yaml"):
        import yaml

        obj = yaml.safe_load(p.read_text(encoding="utf-8"))
        errs = validate_template(obj, schema)
        assert errs == [], f"{p.name}: {errs}"


def test_granularity_merge_overrides_scope_and_evidence():
    from tools.granularity import merge_spec

    template_defaults = {
        "scope_days": 7,
        "granularity": {
            "detail_level": "normal",
            "time_resolution": "day",
            "evidence_depth": "mu_ids",
        },
        "budget": {"max_mu": 50, "max_tokens": 900},
    }

    spec = merge_spec(
        template_name="time_overview_v1",
        template_defaults=template_defaults,
        question_setup={"scope": {"time_window_days": 14}},
        question_expect={"evidence": {"depth": "mu_snippets"}},
        question_budget={"max_tokens": 800},
    )

    assert spec.scope_days == 14
    assert spec.granularity["evidence_depth"] == "mu_snippets"
    assert spec.budget["max_tokens"] == 800
