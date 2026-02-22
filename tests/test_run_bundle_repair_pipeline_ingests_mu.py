from __future__ import annotations

from pathlib import Path


def test_run_bundle_repair_pipeline_ingests_fixed_mu(tmp_path: Path):
    import sqlite3

    from tools.meta_db import init_db

    # prepare db with MU that will trigger repair (snapshot exists)
    db = tmp_path / "meta.sqlite"
    init_db(db)

    mu_root = tmp_path / "mu"
    mu_root.mkdir()
    mu_path = mu_root / "mu_bad.mimo"
    mu_path.write_text(
        """schema_version: '1.1'
mu_id: mu_bad
summary: bad
content_hash: sha256:%s
idempotency:
  mu_key: sha256:%s
privacy:
  level: private
pointer:
  - type: raw
    uri: file:///C:/tmp/missing.txt
    sha256: sha256:%s
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
    raw_id: sha256:%s
    uri: vault://default/raw/2026/02/x.txt
  payload:
    text: foo
"""
        % ("a" * 64, "b" * 64, "1" * 64, "1" * 64),
        encoding="utf-8",
    )

    con = sqlite3.connect(str(db))
    con.execute(
        "INSERT INTO mu(mu_id,time,summary,content_hash,mu_key,privacy_level,corrects_json,tombstone_json,source_kind,source_note,path,mtime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "mu_bad",
            "2026-02-22T00:00:00Z",
            "bad",
            "sha256:" + "a" * 64,
            "sha256:" + "b" * 64,
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

    # raw manifest has mapping for sha256:111.. -> vault uri so executor can autofix
    raw_manifest = tmp_path / "raw_manifest.jsonl"
    raw_manifest.write_text(
        '{"raw_id":"sha256:%s","uri":"vault://default/raw/2026/02/x.txt","sha256":"sha256:%s","size_bytes":0,"mtime":null,"mime":"text/plain","ingested_at":"2026-02-22T00:00:00Z"}\n'
        % ("1" * 64, "1" * 64),
        encoding="utf-8",
    )

    # vault root mapping
    vault_root = tmp_path / "vault"

    from tools.run_bundle_repair_pipeline import main

    rc = main(
        [
            "--db",
            str(db),
            "--query",
            "bad",
            "--runs-root",
            str(tmp_path / "runs"),
            "--vault-root",
            f"default={vault_root}",
            "--raw-manifest",
            str(raw_manifest),
        ]
    )
    assert rc == 0

    # mu_manifest should be written under vault_root/manifests
    mu_manifest = vault_root / "manifests" / "mu_manifest.jsonl"
    assert mu_manifest.exists()
    txt = mu_manifest.read_text(encoding="utf-8")
    assert "\"mu_id\"" in txt
