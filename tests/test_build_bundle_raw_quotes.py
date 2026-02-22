from __future__ import annotations

from pathlib import Path


def test_build_bundle_raw_quotes_resolves_pointer(tmp_path: Path):
    import sqlite3

    from tools.meta_db import init_db
    from tools.vault_ops import sha256_file
    from tools.build_bundle import build_bundle

    # Prepare vault + raw file
    vault_root = tmp_path / "vault"
    raw = vault_root / "raw" / "2026" / "02" / "q.txt"
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_text("a\nb\nc\n", encoding="utf-8")
    sha = sha256_file(raw)

    # Prepare MU file pointing to vault
    mu_root = tmp_path / "mu"
    mu_root.mkdir()
    mu_path = mu_root / "mu_1.mimo"
    mu_path.write_text(
        """schema_version: '1.1'
mu_id: mu_1
summary: hello
privacy:
  level: private
pointer:
  - type: raw
    uri: vault://default/raw/2026/02/q.txt
    sha256: %s
    locator:
      kind: line_range
      start: 2
      end: 3
"""
        % sha,
        encoding="utf-8",
    )

    # Prepare db with mu row
    db = tmp_path / "meta.sqlite"
    init_db(db)
    con = sqlite3.connect(str(db))
    con.execute(
        "INSERT INTO mu(mu_id,time,summary,content_hash,mu_key,privacy_level,corrects_json,tombstone_json,source_kind,source_note,path,mtime) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "mu_1",
            "2026-02-22T00:00:00Z",
            "hello",
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
        query="hello",
        days=7,
        target_level="private",
        evidence_depth="raw_quotes",
        vault_roots={"default": str(vault_root)},
    )

    assert bundle["evidence"][0]["mu_id"] == "mu_1"
    assert bundle["evidence"][0]["snippet"] == "b\nc"
    assert bundle.get("diagnostics") is None or bundle["diagnostics"].get("evidence_degraded") is None
