from __future__ import annotations

import json
from pathlib import Path

from tools.meta_db import connect, init_db
from tools.membership import canonicalize_mu_ids_single_hop


def _ins_mu(
    db: Path,
    *,
    mu_id: str,
    corrects: list[str] | None = None,
    supersedes: list[str] | None = None,
    duplicate_of: list[str] | None = None,
    tombstone: bool | None = None,
):
    init_db(db)
    with connect(db) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO mu(
              mu_id, time, summary, content_hash, mu_key, privacy_level,
              corrects_json, supersedes_json, duplicate_of_json, tombstone_json,
              source_kind, source_note, path, mtime
            ) VALUES (?, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, ?, NULL, NULL, NULL, NULL)
            """,
            (
                mu_id,
                json.dumps(corrects) if corrects is not None else None,
                json.dumps(supersedes) if supersedes is not None else None,
                json.dumps(duplicate_of) if duplicate_of is not None else None,
                json.dumps(True) if tombstone else None,
            ),
        )
        conn.commit()


def test_canonicalize_supersedes_folds_old_to_new(tmp_path: Path):
    db = tmp_path / "meta.sqlite"
    # new MU supersedes old
    _ins_mu(db, mu_id="mu_new", supersedes=["mu_old"])

    out, diag = canonicalize_mu_ids_single_hop(db_path=db, mu_ids={"mu_old"})
    assert out == {"mu_new"}
    assert diag["folded_by_supersedes"] == 1


def test_canonicalize_duplicate_of_folds_dup_to_canonical(tmp_path: Path):
    db = tmp_path / "meta.sqlite"
    _ins_mu(db, mu_id="mu_dup", duplicate_of=["mu_can"])

    out, diag = canonicalize_mu_ids_single_hop(db_path=db, mu_ids={"mu_dup"})
    assert out == {"mu_can"}
    assert diag["folded_by_duplicate_of"] == 1


def test_canonicalize_priority_supersedes_over_duplicate_of(tmp_path: Path):
    db = tmp_path / "meta.sqlite"
    # mu_old is superseded by mu_new
    _ins_mu(db, mu_id="mu_new", supersedes=["mu_old"])
    # and also marked as duplicate_of mu_can (weird but possible)
    _ins_mu(db, mu_id="mu_old", duplicate_of=["mu_can"])

    out, diag = canonicalize_mu_ids_single_hop(db_path=db, mu_ids={"mu_old"})
    assert out == {"mu_new"}
    assert diag["folded_by_supersedes"] == 1


def test_canonicalize_tombstone_excludes(tmp_path: Path):
    db = tmp_path / "meta.sqlite"
    _ins_mu(db, mu_id="mu_dead", tombstone=True)

    out, diag = canonicalize_mu_ids_single_hop(db_path=db, mu_ids={"mu_dead"})
    assert out == set()
    assert diag["tombstoned_excluded"] == 1


def test_canonicalize_converges_multi_hop(tmp_path: Path):
    db = tmp_path / "meta.sqlite"
    # A is superseded by B, B is corrected by C
    _ins_mu(db, mu_id="mu_B", supersedes=["mu_A"])
    _ins_mu(db, mu_id="mu_C", corrects=["mu_B"])

    out, diag = canonicalize_mu_ids_single_hop(db_path=db, mu_ids={"mu_A"})
    assert out == {"mu_C"}
    assert diag["folded_by_supersedes"] == 1
    assert diag["folded_by_corrects"] == 1
