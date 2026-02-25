import json
from pathlib import Path

import yaml


def test_export_mu_respects_share_policy(tmp_path: Path):
    mu = {
        "schema_version": "1.1",
        "mu_id": "mu_01JTEST",
        "content_hash": "sha256:" + "0" * 64,
        "idempotency": {"mu_key": "sha256:" + "1" * 64},
        "meta": {
            "time": "t",
            "source": "s",
            "group_id": "g",
            "order": "1",
            "span": "1",
        },
        "summary": "hello",
        "pointer": [
            {
                "uri": "file://C:/secret.txt",
                "sha256": "sha256:" + "2" * 64,
                "locator": {"kind": "line_range", "start": 1, "end": 2},
            }
        ],
        "snapshot": {
            "kind": "text",
            "codec": "plain",
            "size_bytes": 5,
            "created_at": "t",
            "source_ref": {
                "uri": "file://C:/secret.txt",
                "sha256": "sha256:" + "2" * 64,
            },
            "payload": {"text": "secret"},
        },
        "links": {"corrects": [], "supersedes": [], "duplicate_of": []},
        "privacy": {
            "level": "public",
            "redact": "none",
            "pii": [],
            "share_policy": {"allow_snapshot": False, "allow_pointer": False},
        },
        "provenance": {
            "tool": "x",
            "tool_version": "0.1",
            "model": None,
            "prompt_version": None,
        },
    }

    p = tmp_path / "mu.mimo"
    p.write_text(yaml.safe_dump(mu, allow_unicode=True), encoding="utf-8")

    from tools.export_mu import main

    out = tmp_path / "out.jsonl"
    assert main(["--in", str(p), "--out", str(out), "--target-level", "public"]) == 0

    obj = json.loads(out.read_text(encoding="utf-8").splitlines()[0])
    assert obj["pointer"] == []
    assert obj["snapshot"]["payload"] == {}
    assert obj["snapshot"]["source_ref"]["uri"] == "<REDACTED_URI>"


def test_export_mu_skips_higher_privacy(tmp_path: Path):
    mu = {
        "schema_version": "1.1",
        "mu_id": "mu_private",
        "privacy": {"level": "private", "redact": "none"},
    }
    p = tmp_path / "mu.mimo"
    p.write_text(yaml.safe_dump(mu, allow_unicode=True), encoding="utf-8")

    from tools.export_mu import redact_mu

    red = redact_mu(mu, target_level="public")
    assert red.get("export", {}).get("skipped") is True
