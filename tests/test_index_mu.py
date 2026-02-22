import sqlite3
from pathlib import Path

import yaml


def test_index_mu_builds_db(tmp_path: Path):
    mu_root = tmp_path / "mu"
    mu_root.mkdir()

    mu1 = {
        "schema_version": "1.1",
        "mu_id": "mu_01JTEST1",
        "summary": "hello world",
        "content_hash": "sha256:" + "0" * 64,
        "idempotency": {"mu_key": "sha256:" + "1" * 64},
        "meta": {"time": "2026-01-01T00:00:00Z", "source": {"kind": "chat", "note": "x"}, "tags": ["a", "b"]},
        "links": {"corrects": []},
        "privacy": {"level": "private", "redact": "none"},
    }
    (mu_root / "a.mimo").write_text(yaml.safe_dump(mu1, allow_unicode=True), encoding="utf-8")

    from tools.index_mu import index_mu_dir

    db = tmp_path / "meta.sqlite"
    out = index_mu_dir(mu_root, db, reset=True)
    assert out["indexed"] == 1

    conn = sqlite3.connect(str(db))
    row = conn.execute("SELECT mu_id, summary, privacy_level FROM mu").fetchone()
    assert row[0] == "mu_01JTEST1"
    assert row[1] == "hello world"
    assert row[2] == "private"

    tags = [r[0] for r in conn.execute("SELECT tag FROM tag ORDER BY tag").fetchall()]
    assert tags == ["a", "b"]
