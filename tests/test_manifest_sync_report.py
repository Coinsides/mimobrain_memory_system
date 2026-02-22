import json
from pathlib import Path

import pytest


@pytest.fixture()
def tmp_manifests(tmp_path: Path):
    base = tmp_path / "base.jsonl"
    inc = tmp_path / "incoming.jsonl"
    return base, inc


def write_lines(p: Path, objs: list[dict]):
    p.write_text("\n".join(json.dumps(o) for o in objs) + "\n", encoding="utf-8")


def test_sync_detects_sha_uri_and_uri_sha_conflicts(tmp_manifests):
    base, inc = tmp_manifests

    # base has sha A at uri u1
    a = "sha256:" + "a" * 64
    b = "sha256:" + "b" * 64
    write_lines(
        base,
        [
            {
                "raw_id": a,
                "uri": "vault://default/raw/2026/02/21/a.txt",
                "sha256": a,
                "size_bytes": 1,
                "mtime": None,
                "mime": "text/plain",
                "ingested_at": "2026-02-21T00:00:00Z",
            }
        ],
    )

    # incoming: same sha A but uri changed (warn)
    # incoming: same uri but sha changed (error)
    write_lines(
        inc,
        [
            {
                "raw_id": a,
                "uri": "vault://default/raw/2026/02/22/a.txt",
                "sha256": a,
                "size_bytes": 1,
                "mtime": None,
                "mime": "text/plain",
                "ingested_at": "2026-02-22T00:00:00Z",
            },
            {
                "raw_id": b,
                "uri": "vault://default/raw/2026/02/21/a.txt",
                "sha256": b,
                "size_bytes": 1,
                "mtime": None,
                "mime": "text/plain",
                "ingested_at": "2026-02-22T00:00:00Z",
            },
        ],
    )

    from tools.manifest_sync import analyze_sync

    r = analyze_sync(kind="raw", base_path=base, incoming_path=inc)
    types = {c["type"] for c in r["conflicts"]}
    assert "SHA_COLLISION_DIFFERENT_URI" in types
    assert "URI_COLLISION_DIFFERENT_SHA" in types


def test_sync_detects_id_collision_different_sha(tmp_manifests):
    base, inc = tmp_manifests
    a = "sha256:" + "a" * 64
    b = "sha256:" + "b" * 64

    write_lines(
        base,
        [
            {
                "mu_id": "mu_01JTEST",
                "schema_version": "1.1",
                "uri": "vault://default/mu/2026/02/mu_01JTEST.mimo",
                "source_raw_ids": [],
                "mu_key": a,
                "content_hash": a,
                "created_at": "2026-02-21T00:00:00Z",
                "sha256": a,
            }
        ],
    )

    write_lines(
        inc,
        [
            {
                "mu_id": "mu_01JTEST",
                "schema_version": "1.1",
                "uri": "vault://default/mu/2026/02/mu_01JTEST.mimo",
                "source_raw_ids": [],
                "mu_key": b,
                "content_hash": b,
                "created_at": "2026-02-22T00:00:00Z",
                "sha256": b,
            }
        ],
    )

    from tools.manifest_sync import analyze_sync

    r = analyze_sync(kind="mu", base_path=base, incoming_path=inc)
    types = {c["type"] for c in r["conflicts"]}
    assert "ID_COLLISION_DIFFERENT_SHA" in types
