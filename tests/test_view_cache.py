from pathlib import Path


def test_view_cache_put_get_invalidate(tmp_path: Path):
    from tools.meta_db import init_db

    db = tmp_path / "meta.sqlite"
    init_db(db)

    from tools.view_cache import get_view, invalidate_by_mu_ids, put_view

    put_view(
        db,
        view_id="v1",
        template="time_daily_v1",
        scope={"days": 7},
        source_mu_ids=["mu_a", "mu_b"],
        content={"text": "hello"},
    )

    v = get_view(db, "v1")
    assert v is not None
    assert v.stale is False

    n = invalidate_by_mu_ids(db, ["mu_x"])
    assert n == 0

    n2 = invalidate_by_mu_ids(db, ["mu_b"])
    assert n2 == 1

    v2 = get_view(db, "v1")
    assert v2 is not None
    assert v2.stale is True
