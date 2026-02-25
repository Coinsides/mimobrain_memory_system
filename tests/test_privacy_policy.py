from tools.privacy_policy import ensure_privacy_defaults, export_share_policy


def test_ensure_privacy_defaults_fills_private():
    mu = {"mu_id": "mu_x"}
    out = ensure_privacy_defaults(mu)
    assert out["privacy"]["level"] == "private"
    assert out["privacy"]["redact"] == "none"
    assert out["privacy"]["pii"] == []
    assert isinstance(out["privacy"]["share_policy"], dict)


def test_export_share_policy_defaults_deny_org_public():
    mu = {"mu_id": "mu_x", "privacy": {"level": "private", "redact": "none"}}
    mu2 = ensure_privacy_defaults(mu)
    assert export_share_policy(mu2, target_level="public") == {
        "allow_pointer": False,
        "allow_snapshot": False,
    }
    assert export_share_policy(mu2, target_level="org") == {
        "allow_pointer": False,
        "allow_snapshot": False,
    }


def test_export_share_policy_allows_when_explicit_true():
    mu = {
        "mu_id": "mu_x",
        "privacy": {
            "level": "public",
            "redact": "none",
            "share_policy": {"allow_pointer": True, "allow_snapshot": True},
        },
    }
    mu2 = ensure_privacy_defaults(mu)
    assert export_share_policy(mu2, target_level="public") == {
        "allow_pointer": True,
        "allow_snapshot": True,
    }
