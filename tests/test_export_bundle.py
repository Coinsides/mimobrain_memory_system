import json
from pathlib import Path


def test_export_bundle_strips_evidence_for_public(tmp_path: Path):
    bundle = {
        "bundle_id": "b1",
        "template": "t",
        "scope": {"time_window": ["2026-01-01", "2026-01-02"]},
        "source_mu_ids": ["mu_a"],
        "evidence": [
            {
                "mu_id": "mu_a",
                "pointer": [{"uri": "file://C:/secret.txt"}],
                "snapshot": {
                    "source_ref": {"uri": "file://C:/secret.txt"},
                    "payload": {"text": "secret"},
                },
                "privacy": {"level": "public", "redact": "none"},
            }
        ],
    }

    from tools.export_bundle import export_bundle

    out = export_bundle(bundle, target_level="public")
    assert out["export"]["target_level"] == "public"
    assert out["evidence"] == [{"mu_id": "mu_a"}]


def test_export_bundle_allows_when_share_policy_true(tmp_path: Path):
    bundle = {
        "bundle_id": "b1",
        "evidence": [
            {
                "mu_id": "mu_a",
                "pointer": [
                    {"uri": "vault://default/raw/x", "sha256": "sha256:" + "0" * 64}
                ],
                "snapshot": {
                    "source_ref": {"uri": "vault://default/raw/x"},
                    "payload": {"text": "ok"},
                },
                "privacy": {
                    "level": "public",
                    "redact": "none",
                    "share_policy": {"allow_pointer": True, "allow_snapshot": True},
                },
            }
        ],
    }

    from tools.export_bundle import export_bundle

    out = export_bundle(bundle, target_level="public")
    ev = out["evidence"][0]
    assert ev["pointer"]
    assert ev["snapshot"]["payload"]


def test_export_bundle_cli(tmp_path: Path):
    bundle = {
        "bundle_id": "b1",
        "evidence": [
            {"mu_id": "mu_a", "privacy": {"level": "public", "redact": "none"}}
        ],
    }
    inp = tmp_path / "bundle.json"
    outp = tmp_path / "out.json"
    inp.write_text(json.dumps(bundle), encoding="utf-8")

    from tools.export_bundle import main

    assert main(["--in", str(inp), "--out", str(outp), "--target-level", "public"]) == 0
    assert outp.exists()
