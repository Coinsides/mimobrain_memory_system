import json
from pathlib import Path


def test_golden_run_produces_contract(tmp_path: Path):
    from tools.golden_run import main

    out_dir = tmp_path / "out"
    rc = main(["--out-dir", str(out_dir)])
    # should exit non-zero due to hard rules? placeholder should SKIP, so failures should be 0.
    assert rc == 0

    report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))
    assert report["summary"]["skipped"] >= 1
    assert report["summary"]["failed"] == 0

    # validate shape of one result
    r0 = report["results"][0]
    assert r0["status"] in {"PASS", "FAIL", "SKIP"}
    assert "answer" in r0 and "checks" in r0
    assert isinstance(r0["answer"]["source_mu_ids"], list)
