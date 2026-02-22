from pathlib import Path

import yaml


def _write_mu(p: Path, mu_id: str, summary: str, time: str, tags=None, privacy="private"):
    mu = {
        "schema_version": "1.1",
        "mu_id": mu_id,
        "summary": summary,
        "content_hash": "sha256:" + "0" * 64,
        "idempotency": {"mu_key": "sha256:" + "1" * 64},
        "meta": {"time": time, "source": {"kind": "chat", "note": "x"}, "tags": tags or []},
        "links": {"corrects": []},
        "privacy": {"level": privacy, "redact": "none"},
    }
    p.write_text(yaml.safe_dump(mu, allow_unicode=True), encoding="utf-8")


def test_search_mu_fts_and_filters(tmp_path: Path):
    mu_root = tmp_path / "mu"
    mu_root.mkdir()
    _write_mu(mu_root / "a.mimo", "mu_a", "hello travel world", "2026-01-01T00:00:00Z", tags=["travel"], privacy="private")
    _write_mu(mu_root / "b.mimo", "mu_b", "hello math", "2026-01-10T00:00:00Z", tags=["study"], privacy="public")

    from tools.index_mu import index_mu_dir

    db = tmp_path / "meta.sqlite"
    index_mu_dir(mu_root, db, reset=True)

    from tools.search_mu import search_mu

    res = search_mu(db, query="travel")
    assert [r.mu_id for r in res] == ["mu_a"]

    res2 = search_mu(db, tag="study")
    assert [r.mu_id for r in res2] == ["mu_b"]

    res3 = search_mu(db, since="2026-01-05", until="2026-01-20")
    assert [r.mu_id for r in res3] == ["mu_b"]

    res4 = search_mu(db, privacy="public")
    assert [r.mu_id for r in res4] == ["mu_b"]
