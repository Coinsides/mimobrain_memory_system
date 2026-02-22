from pathlib import Path

import yaml


def test_search_mu_cjk_falls_back_to_like(tmp_path: Path):
    mu_root = tmp_path / "mu"
    mu_root.mkdir()

    mu = {
        "schema_version": "1.1",
        "mu_id": "mu_cjk",
        "summary": "决策: 去旅行\n证据",
        "content_hash": "sha256:" + "0" * 64,
        "idempotency": {"mu_key": "sha256:" + "1" * 64},
        "meta": {"time": "2026-02-20T00:00:00Z", "source": {"kind": "chat", "note": "x"}, "tags": []},
        "links": {"corrects": []},
        "privacy": {"level": "private", "redact": "none"},
    }
    (mu_root / "a.mimo").write_text(yaml.safe_dump(mu, allow_unicode=True), encoding="utf-8")

    from tools.index_mu import index_mu_dir

    db = tmp_path / "meta.sqlite"
    index_mu_dir(mu_root, db, reset=True)

    from tools.search_mu import search_mu

    res = search_mu(db, query="决策", since=None, until=None, tag=None, privacy=None, target_level="private", include_snippet=False, limit=10)
    assert [r.mu_id for r in res] == ["mu_cjk"]
