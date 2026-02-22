from pathlib import Path

import yaml


def _write_mu(p: Path, mu_id: str, summary: str, time: str):
    mu = {
        "schema_version": "1.1",
        "mu_id": mu_id,
        "summary": summary,
        "content_hash": "sha256:" + "0" * 64,
        "idempotency": {"mu_key": "sha256:" + "1" * 64},
        "meta": {"time": time, "source": {"kind": "chat", "note": "x"}, "tags": []},
        "links": {"corrects": []},
        "privacy": {"level": "private", "redact": "none"},
    }
    p.write_text(yaml.safe_dump(mu, allow_unicode=True), encoding="utf-8")


def test_build_bundle_can_compile_template_and_attach_diagnostics(tmp_path: Path):
    mu_root = tmp_path / "mu"
    mu_root.mkdir()

    _write_mu(mu_root / "a.mimo", "mu_a", "隐私 策略 private org public allow_snapshot allow_pointer", "2026-02-20T00:00:00Z")

    from tools.index_mu import index_mu_dir

    db = tmp_path / "meta.sqlite"
    index_mu_dir(mu_root, db, reset=True)

    from tools.build_bundle import build_bundle

    b = build_bundle(
        db_path=db,
        query="隐私",
        template_name="privacy_policy_v1",
        question_setup={"scope": {"time_window_days": 7}},
        question_expect={"evidence": {"depth": "mu_snippets"}},
        question_budget={"max_tokens": 200},
        target_level="private",
    )

    assert b["template"] == "privacy_policy_v1"
    assert "diagnostics" in b
    diag = b["diagnostics"]
    assert "downgrade_plan" in diag
    assert "downgrade_steps" in diag["downgrade_plan"]

    # should validate against bundle schema
    from tools.bundle_validate import validate_bundle

    assert validate_bundle(b) == []
