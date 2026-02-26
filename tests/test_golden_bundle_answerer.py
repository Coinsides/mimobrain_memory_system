import json
from pathlib import Path

import yaml


def test_golden_run_bundle_answerer_can_pass(tmp_path: Path):
    # Prepare MU root + index
    mu_root = tmp_path / "mu"
    mu_root.mkdir()

    mu = {
        "schema_version": "1.1",
        "mu_id": "mu_a",
        "summary": "决策: go travel\n证据: mu_a",
        "content_hash": "sha256:" + "0" * 64,
        "idempotency": {"mu_key": "sha256:" + "1" * 64},
        "meta": {
            "time": "2026-01-01T00:00:00Z",
            "source": {"kind": "chat", "note": "x"},
            "tags": [],
        },
        "links": {"corrects": []},
        "privacy": {"level": "private", "redact": "none"},
    }
    (mu_root / "a.mimo").write_text(
        yaml.safe_dump(mu, allow_unicode=True), encoding="utf-8"
    )

    from tools.index_mu import index_mu_dir

    db = tmp_path / "meta.sqlite"
    index_mu_dir(mu_root, db, reset=True)

    # Minimal golden questions set (1 item)
    questions = [
        {
            "id": "q1",
            "query": "过去7天我做了哪些关键决策？",
            "setup": {"scope": {"time_window_days": 7}},
            "expect": {"must_include": ["决策", "证据"], "must_not": []},
        }
    ]
    qpath = tmp_path / "q.yaml"
    qpath.write_text(yaml.safe_dump(questions, allow_unicode=True), encoding="utf-8")

    out_dir = tmp_path / "out"
    from tools.golden_run import main

    # membership fence
    ws_dir = tmp_path / "workspaces"
    ws_dir.mkdir(exist_ok=True)
    (ws_dir / "membership.jsonl").write_text(
        '{"event":"add","workspace_id":"ws_test","mu_id":"mu_a","at":"2026-02-26T00:00:00Z","source":"test"}\n',
        encoding="utf-8",
    )

    rc = main(
        [
            "--questions",
            str(qpath),
            "--out-dir",
            str(out_dir),
            "--db",
            str(db),
            "--data-root",
            str(tmp_path),
            "--workspace",
            "ws_test",
        ]
    )
    assert rc == 0

    report = json.loads((out_dir / "report.json").read_text(encoding="utf-8"))
    assert report["summary"]["passed"] == 1
    assert report["summary"]["failed"] == 0
