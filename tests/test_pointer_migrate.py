from __future__ import annotations

import json
from pathlib import Path


def test_pointer_migrate_creates_superseding_mu(tmp_path: Path):
    from tools.manifest_io import append_jsonl
    from tools.vault_ops import sha256_file
    from tools.pointer_migrate import migrate_mu_pointers

    # Create a fake vault raw file and manifest
    vault_root = tmp_path / "vault"
    raw_p = vault_root / "raw" / "2026" / "02" / "hello.txt"
    raw_p.parent.mkdir(parents=True, exist_ok=True)
    raw_p.write_text("hello", encoding="utf-8")
    sha = sha256_file(raw_p)
    manifest_p = vault_root / "manifests" / "raw_manifest.jsonl"
    uri = "vault://default/raw/2026/02/hello.txt"
    append_jsonl(
        manifest_p,
        {
            "raw_id": sha,
            "uri": uri,
            "sha256": sha,
            "size_bytes": raw_p.stat().st_size,
            "mtime": None,
            "mime": "text/plain",
            "ingested_at": "2026-02-22T00:00:00Z",
        },
    )

    # Create MU pointing to legacy uri but with sha256 present
    mu_dir = tmp_path / "mu"
    mu_dir.mkdir()
    mu_path = mu_dir / "mu_OLD.mimo"
    mu_path.write_text(
        """schema_version: '1.1'\nmu_id: mu_OLD\nsummary: test\npointer:\n  - type: raw\n    uri: file:///C:/tmp/hello.txt\n    sha256: %s\n    locator:\n      kind: line_range\n      start: 1\n      end: 1\nlinks:\n  supersedes: []\n"""
        % sha,
        encoding="utf-8",
    )

    out_dir = tmp_path / "out"
    res = migrate_mu_pointers(mu_path, raw_manifest_path=manifest_p, out_dir=out_dir)
    assert res is not None
    assert res.source_mu_id == "mu_OLD"
    assert res.new_mu_path.exists()

    new_text = res.new_mu_path.read_text(encoding="utf-8")
    assert "vault://default/raw/2026/02/hello.txt" in new_text
    assert "supersedes" in new_text
    assert "mu_OLD" in new_text
