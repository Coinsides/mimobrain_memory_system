from __future__ import annotations

from pathlib import Path


def test_pointer_resolve_vault_uri_line_range(tmp_path: Path):
    from tools.vault_ops import sha256_file
    from tools.pointer_resolve import resolve_pointer

    vault_root = tmp_path / "vault"
    p = vault_root / "raw" / "2026" / "02" / "a.txt"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("l1\nl2\nl3\n", encoding="utf-8")
    sha = sha256_file(p)

    pointer = {
        "type": "raw",
        "uri": "vault://default/raw/2026/02/a.txt",
        "sha256": sha,
        "locator": {"kind": "line_range", "start": 2, "end": 3},
    }

    out = resolve_pointer(pointer, vault_roots={"default": str(vault_root)})
    assert out.ok is True
    assert out.snippet == "l2\nl3"


def test_pointer_resolve_legacy_uri_via_manifest(tmp_path: Path):
    import json

    from tools.manifest_io import append_jsonl
    from tools.vault_ops import sha256_file
    from tools.pointer_resolve import resolve_pointer

    vault_root = tmp_path / "vault"
    raw = vault_root / "raw" / "2026" / "02" / "b.txt"
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_text("hello\nworld\n", encoding="utf-8")
    sha = sha256_file(raw)

    manifest = vault_root / "manifests" / "raw_manifest.jsonl"
    append_jsonl(
        manifest,
        {
            "raw_id": sha,
            "uri": "vault://default/raw/2026/02/b.txt",
            "sha256": sha,
            "size_bytes": raw.stat().st_size,
            "mtime": None,
            "mime": "text/plain",
            "ingested_at": "2026-02-22T00:00:00Z",
        },
    )

    pointer = {
        "type": "raw",
        "uri": "file:///C:/tmp/b.txt",
        "sha256": sha,
        "locator": {"kind": "line_range", "start": 1, "end": 1},
    }

    out = resolve_pointer(pointer, vault_roots={"default": str(vault_root)}, raw_manifest_path=manifest)
    assert out.ok is True
    assert out.uri == "vault://default/raw/2026/02/b.txt"
    assert out.snippet == "hello"
    assert out.diagnostics.get("resolved_via_manifest") is True
