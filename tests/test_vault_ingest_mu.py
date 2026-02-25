from __future__ import annotations

import json
from pathlib import Path


def test_vault_ingest_mu_writes_mu_manifest_and_copies(tmp_path: Path):
    from tools.vault_ingest_mu import ingest_mu_file
    from tools.vault_doctor import doctor_manifest

    vault_root = tmp_path / "vault"

    mu_path = tmp_path / "mu_1.mimo"
    mu_path.write_text(
        """schema_version: '1.1'
mu_id: mu_1
content_hash: sha256:%s
idempotency:
  mu_key: sha256:%s
summary: hi
pointer:
  - type: raw
    uri: file:///C:/tmp/a.txt
    sha256: sha256:%s
    locator: {kind: line_range, start: 1, end: 1}
"""
        % ("a" * 64, "b" * 64, "c" * 64),
        encoding="utf-8",
    )

    res = ingest_mu_file(mu_path, vault_root=vault_root, vault_id="default")
    assert res.dest_path.exists()
    assert res.manifest_path.exists()

    last = res.manifest_path.read_text(encoding="utf-8").strip().splitlines()[-1]
    obj = json.loads(last)
    assert obj["mu_id"] == "mu_1"
    assert obj["uri"].startswith("vault://default/mu/")
    assert obj["mu_key"].startswith("sha256:")
    assert obj["content_hash"].startswith("sha256:")
    assert obj["source_raw_ids"] == ["sha256:" + "c" * 64]

    schema_path = (
        Path(__file__).resolve().parents[1]
        / "docs"
        / "contracts"
        / "mu_manifest_line_v0_1.schema.json"
    )
    errors = doctor_manifest(res.manifest_path, schema_path)
    assert errors == []
