import json
from pathlib import Path


def test_validate_bundle_ok_minimal():
    b = {
        "bundle_id": "b1",
        "template": "t",
        "scope": {"time_window": ["2026-01-01", "2026-01-02"]},
        "source_mu_ids": ["mu_a"],
        "created_at": "2026-01-02T00:00:00Z",
        "evidence": [{"mu_id": "mu_a"}],
    }
    from tools.bundle_validate import validate_bundle

    assert validate_bundle(b) == []


def test_validate_bundle_missing_required():
    b = {"bundle_id": "b1"}
    from tools.bundle_validate import validate_bundle

    errs = validate_bundle(b)
    assert errs


def test_bundle_validate_cli(tmp_path: Path):
    b = {
        "bundle_id": "b1",
        "template": "t",
        "scope": {},
        "source_mu_ids": [],
        "created_at": "t",
        "evidence": [{"mu_id": "mu_a"}],
    }
    p = tmp_path / "b.json"
    p.write_text(json.dumps(b), encoding="utf-8")

    from tools.bundle_validate import main

    assert main(["--in", str(p)]) == 0
