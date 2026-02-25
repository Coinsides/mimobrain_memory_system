import json
from pathlib import Path

import pytest


@pytest.fixture()
def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_verify_manifest_and_repair(tmp_path: Path, repo_root: Path):
    # Build a tiny vault structure
    vault_root = tmp_path / "vaults" / "default"
    (vault_root / "raw" / "2026" / "02" / "21").mkdir(parents=True)
    p = vault_root / "raw" / "2026" / "02" / "21" / "foo.txt"
    p.write_text("hello", encoding="utf-8")

    # Compute sha256
    import hashlib

    h = hashlib.sha256(b"hello").hexdigest()
    sha = "sha256:" + h

    # Write a raw_manifest.jsonl pointing at that file
    mani = tmp_path / "raw_manifest.jsonl"
    rec = {
        "raw_id": sha,
        "uri": "vault://default/raw/2026/02/21/foo.txt",
        "sha256": sha,
        "size_bytes": 5,
        "mtime": None,
        "mime": "text/plain",
        "ingested_at": "2026-02-21T00:00:00Z",
    }
    mani.write_text(json.dumps(rec) + "\n", encoding="utf-8")

    from tools.vault_doctor import verify_manifest, repair_suggest_by_sha256

    errs = verify_manifest(mani, vault_roots={"default": str(vault_root)})
    assert errs == []

    # repair by sha256 should find the uri
    sug = repair_suggest_by_sha256(mani, sha256=sha)
    assert sug == rec["uri"]


def test_verify_reports_missing_file(tmp_path: Path):
    mani = tmp_path / "raw_manifest.jsonl"
    sha = "sha256:" + "0" * 64
    rec = {
        "raw_id": sha,
        "uri": "vault://default/raw/2026/02/21/missing.txt",
        "sha256": sha,
        "size_bytes": 1,
        "mtime": None,
        "mime": "text/plain",
        "ingested_at": "2026-02-21T00:00:00Z",
    }
    mani.write_text(json.dumps(rec) + "\n", encoding="utf-8")

    from tools.vault_doctor import verify_manifest

    errs = verify_manifest(
        mani, vault_roots={"default": str(tmp_path / "vaults" / "default")}
    )
    assert errs
