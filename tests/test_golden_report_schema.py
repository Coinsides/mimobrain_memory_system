import json
from pathlib import Path


def test_golden_run_writes_schema_valid_report(tmp_path: Path):
    # Minimal questions set (1 item), no db => SKIP allowed but report must match schema.
    questions = [
        {
            "id": "q1",
            "query": "hello",
            "expect": {"must_include": [], "must_not": []},
        }
    ]

    import yaml

    qpath = tmp_path / "q.yaml"
    qpath.write_text(yaml.safe_dump(questions, allow_unicode=True), encoding="utf-8")

    out_dir = tmp_path / "out"
    from tools.golden_run import main

    rc = main(
        [
            "--questions",
            str(qpath),
            "--out-dir",
            str(out_dir),
        ]
    )
    # No db => answerer not implemented => summary.failed==0 (SKIP) so rc==0
    assert rc == 0

    report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))

    from tools.golden_run import validate_report

    schema_path = Path("docs") / "contracts" / "golden_report_v0_1.schema.json"
    errs = validate_report(report, schema_path)
    assert errs == []
