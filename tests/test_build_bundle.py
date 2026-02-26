import json
from pathlib import Path

import yaml


def _write_mu(
    p: Path, mu_id: str, summary: str, time: str, tags=None, privacy="private"
):
    mu = {
        "schema_version": "1.1",
        "mu_id": mu_id,
        "summary": summary,
        "content_hash": "sha256:" + "0" * 64,
        "idempotency": {"mu_key": "sha256:" + "1" * 64},
        "meta": {
            "time": time,
            "source": {"kind": "chat", "note": "x"},
            "tags": tags or [],
        },
        "links": {"corrects": []},
        "privacy": {"level": privacy, "redact": "none"},
    }
    p.write_text(yaml.safe_dump(mu, allow_unicode=True), encoding="utf-8")


def _write_membership(tmp_path: Path, workspace: str, mu_ids: list[str]):
    ws_dir = tmp_path / "workspaces"
    ws_dir.mkdir(exist_ok=True)
    # membership.jsonl (no BOM)
    mem = ws_dir / "membership.jsonl"
    lines = [
        json.dumps(
            {
                "event": "add",
                "workspace_id": workspace,
                "mu_id": mid,
                "at": "2026-02-26T00:00:00Z",
                "source": "test",
            },
            ensure_ascii=False,
        )
        for mid in mu_ids
    ]
    mem.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_build_bundle_minimal(tmp_path: Path):
    mu_root = tmp_path / "mu"
    mu_root.mkdir()
    _write_mu(
        mu_root / "a.mimo",
        "mu_a",
        "hello travel",
        "2026-01-01T00:00:00Z",
        tags=["travel"],
        privacy="private",
    )

    from tools.index_mu import index_mu_dir

    db = tmp_path / "meta.sqlite"
    index_mu_dir(mu_root, db, reset=True)

    from tools.build_bundle import build_bundle

    _write_membership(tmp_path, "ws_test", ["mu_a"])

    b = build_bundle(
        db_path=db,
        data_root=tmp_path,
        workspace="ws_test",
        query="travel",
        days=9999,
        limit=10,
    )
    assert b["bundle_id"].startswith("bndl_")
    assert b["template"]
    assert b["source_mu_ids"] == ["mu_a"]
    assert b["evidence"] == [{"mu_id": "mu_a"}]

    b2 = build_bundle(
        db_path=db,
        data_root=tmp_path,
        workspace="ws_test",
        query="travel",
        days=9999,
        evidence_depth="mu_snippets",
        limit=10,
    )
    assert b2["evidence"][0]["mu_id"] == "mu_a"
    assert b2["evidence"][0].get("snippet")

    # should validate against bundle schema
    from tools.bundle_validate import validate_bundle

    assert validate_bundle(b) == []


def test_build_bundle_cli(tmp_path: Path):
    # minimal DB not required for this smoke; just ensure CLI writes file
    # Create a tiny DB with no records
    from tools.meta_db import init_db

    db = tmp_path / "meta.sqlite"
    init_db(db)

    outp = tmp_path / "b.json"
    from tools.build_bundle import main

    # membership fence requires workspace
    _write_membership(tmp_path, "ws_test", [])

    assert (
        main([
            "--db",
            str(db),
            "--data-root",
            str(tmp_path),
            "--workspace",
            "ws_test",
            "--query",
            "x",
            "--days",
            "1",
            "--out",
            str(outp),
        ])
        == 0
    )
    assert json.loads(outp.read_text(encoding="utf-8"))["bundle_id"].startswith("bndl_")
