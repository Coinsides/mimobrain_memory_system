from __future__ import annotations

from pathlib import Path


def test_run_bundle_repair_pipeline_smoke(tmp_path: Path):
    import sqlite3

    from tools.meta_db import init_db

    # Build minimal db + mu with missing pointer but snapshot so repair triggers.
    db = tmp_path / "meta.sqlite"
    init_db(db)

    mu_root = tmp_path / "mu"
    mu_root.mkdir()
    mu_path = mu_root / "mu_bad.mimo"
    mu_path.write_text(
        """schema_version: '1.1'
mu_id: mu_bad
summary: bad
privacy:
  level: private
pointer:
  - type: raw
    uri: vault://default/raw/2026/02/missing.txt
    sha256: sha256:0000000000000000000000000000000000000000000000000000000000000000
    locator:
      kind: line_range
      start: 1
      end: 1
snapshot:
  kind: text
  codec: plain
  size_bytes: 3
  created_at: 2026-02-22T00:00:00Z
  source_ref:
    raw_id: sha256:0000000000000000000000000000000000000000000000000000000000000000
    uri: vault://default/raw/2026/02/missing.txt
  payload:
    text: foo
""",
        encoding="utf-8",
    )

    con = sqlite3.connect(str(db))
    con.execute(
        "INSERT INTO mu(mu_id,time,summary,content_hash,mu_key,privacy_level,corrects_json,tombstone_json,source_kind,source_note,path,mtime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "mu_bad",
            "2026-02-22T00:00:00Z",
            "bad",
            None,
            None,
            "private",
            "[]",
            "null",
            None,
            None,
            str(mu_path),
            0.0,
        ),
    )
    con.commit()
    con.close()

    # Create a raw manifest with no matching sha so executor will PARTIAL.
    raw_manifest = tmp_path / "raw_manifest.jsonl"
    raw_manifest.write_text("", encoding="utf-8")

    # membership fence
    ws_dir = tmp_path / "workspaces"
    ws_dir.mkdir(exist_ok=True)
    (ws_dir / "membership.jsonl").write_text(
        '{"event":"add","workspace_id":"ws_test","mu_id":"mu_bad","at":"2026-02-26T00:00:00Z","source":"test"}\n',
        encoding="utf-8",
    )

    from tools.run_bundle_repair_pipeline import main

    rc = main(
        [
            "--db",
            str(db),
            "--data-root",
            str(tmp_path),
            "--workspace",
            "ws_test",
            "--query",
            "bad",
            "--runs-root",
            str(tmp_path / "runs"),
            "--vault-root",
            f"default={tmp_path / 'vault'}",
            "--raw-manifest",
            str(raw_manifest),
        ]
    )
    assert rc == 0

    # ensure run_dir exists with run_manifest.json
    runs = list((tmp_path / "runs").glob("RUN-*"))
    assert len(runs) == 1
    assert (runs[0] / "run_manifest.json").exists()
    assert (runs[0] / "bundle.json").exists()
    assert (runs[0] / "tasks").exists()
    assert (runs[0] / "task_results.jsonl").exists()
