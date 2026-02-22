from __future__ import annotations

from pathlib import Path


def test_build_bundle_emits_repair_tasks_when_degraded(tmp_path: Path):
    import sqlite3

    from tools.meta_db import init_db
    from tools.build_bundle import build_bundle

    # MU points to missing vault file, but has snapshot -> should mark degraded + emit repair task
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

    db = tmp_path / "meta.sqlite"
    init_db(db)
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

    bundle = build_bundle(
        db_path=db,
        query="bad",
        days=7,
        target_level="private",
        evidence_depth="raw_quotes",
        vault_roots={"default": str(tmp_path / "vault_does_not_exist")},
    )

    assert bundle.get("diagnostics", {}).get("evidence_degraded") is True
    tasks = bundle.get("diagnostics", {}).get("repair_tasks")
    assert isinstance(tasks, list)
    assert tasks and tasks[0]["type"] == "REPAIR_POINTER"
    assert tasks[0]["mu_id"] == "mu_bad"
