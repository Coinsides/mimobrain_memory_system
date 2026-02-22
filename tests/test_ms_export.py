import json
from pathlib import Path

import yaml


def test_ms_export_mu(tmp_path: Path):
    mu = {
        "schema_version": "1.1",
        "mu_id": "mu_01JTEST",
        "privacy": {"level": "public", "redact": "none"},
    }
    d = tmp_path / "mu"
    d.mkdir()
    (d / "a.mimo").write_text(yaml.safe_dump(mu, allow_unicode=True), encoding="utf-8")

    out = tmp_path / "out.jsonl"

    from tools.ms_export import main

    assert main(["--in", str(d), "--out", str(out), "--target-level", "public"]) == 0
    assert out.exists()


def test_ms_export_bundle(tmp_path: Path):
    bundle = {"bundle_id": "b1", "evidence": [{"mu_id": "mu_a", "privacy": {"level": "public", "redact": "none"}}]}
    inp = tmp_path / "bundle.json"
    inp.write_text(json.dumps(bundle), encoding="utf-8")

    out = tmp_path / "out.json"

    from tools.ms_export import main

    assert main(["--in", str(inp), "--out", str(out), "--target-level", "public"]) == 0
    assert json.loads(out.read_text(encoding="utf-8"))["bundle_id"] == "b1"
