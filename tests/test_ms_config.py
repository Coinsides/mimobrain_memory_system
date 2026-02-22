from __future__ import annotations

import json
from pathlib import Path


def test_ms_config_defaults_paths(tmp_path: Path):
    from tools.ms_config import load_config

    cfg_path = tmp_path / "ms_config.json"
    cfg_path.write_text(
        json.dumps({"vault_roots": {"default": str(tmp_path / "vault")}}, ensure_ascii=False),
        encoding="utf-8",
    )

    cfg = load_config(cfg_path)
    assert cfg["raw_manifest_path"].endswith("manifests\\raw_manifest.jsonl") or cfg["raw_manifest_path"].endswith("manifests/raw_manifest.jsonl")
    assert cfg["mu_root"].endswith("\\mu") or cfg["mu_root"].endswith("/mu")
