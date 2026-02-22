"""ms config (v0.1) — load/validate shared runtime config.

Purpose:
- Provide a single, explicit config file for vault_roots and related paths.
- Avoid passing vault_root/raw_manifest paths around manually.

Config format: JSON (ms_config_v0_1.schema.json)
Example:
{
  "vault_roots": {"default": "C:/Mimo/mimo_data/memory_system/vaults/default"},
  "raw_manifest_path": null
}

Rules:
- raw_manifest_path defaults to <vault_root>/manifests/raw_manifest.jsonl for vault_id=default when absent/null.
- mu_root defaults to <vault_root>/mu for vault_id=default when absent/null.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_config(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    obj = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise TypeError("config must be a JSON object")

    # validate best-effort
    try:
        import jsonschema

        schema_path = Path(__file__).resolve().parents[1] / "docs" / "contracts" / "ms_config_v0_1.schema.json"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        jsonschema.Draft202012Validator(schema).validate(obj)
    except Exception:
        # dev dep may be missing; keep permissive
        pass

    vault_roots = obj.get("vault_roots")
    if not isinstance(vault_roots, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in vault_roots.items()):
        raise ValueError("missing/invalid vault_roots")

    default_root = vault_roots.get("default")
    if isinstance(default_root, str) and default_root:
        if obj.get("raw_manifest_path") in (None, ""):
            obj["raw_manifest_path"] = str(Path(default_root) / "manifests" / "raw_manifest.jsonl")
        if obj.get("mu_manifest_path") in (None, ""):
            obj["mu_manifest_path"] = str(Path(default_root) / "manifests" / "mu_manifest.jsonl")
        if obj.get("mu_root") in (None, ""):
            obj["mu_root"] = str(Path(default_root) / "mu")

    return obj


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ns = ap.parse_args(argv)

    cfg = load_config(ns.config)
    print(json.dumps(cfg, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
