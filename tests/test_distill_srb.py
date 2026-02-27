from __future__ import annotations

import json
from pathlib import Path


def test_distill_srb_smoke(tmp_path: Path):
    from tools.distill_srb import distill

    bundle = {
        "bundle_id": "bndl_x",
        "scope": {
            "workspace": "ws_design",
            "since": "2026-02-20T00:00:00Z",
            "time_window_days": 7,
        },
        "query_on": {"query": "Space Marines"},
        "source_mu_ids": ["mu_1", "mu_2"],
        "diagnostics": {
            "membership": {
                "effective_count": 10,
                "canonicalized_count": 9,
                "canonicalization": {"folded_by_supersedes": 1},
            },
            "repair_tasks": [
                {
                    "type": "REPAIR_POINTER",
                    "mu_id": "mu_1",
                    "reason": "missing vault_roots",
                }
            ],
        },
    }

    md, obj = distill(bundle)
    assert "workspace: ws_design" in md
    assert obj["workspace"] == "ws_design"
    assert obj["mu_count"] == 2


def test_distill_srb_cli_writes_files(tmp_path: Path):
    from tools.distill_srb import main

    bundle_path = tmp_path / "bundle.json"
    out_dir = tmp_path / "out"

    bundle_path.write_text(
        json.dumps(
            {
                "scope": {
                    "workspace": "ws_design",
                    "since": "2026-02-20T00:00:00Z",
                    "time_window_days": 7,
                },
                "query_on": {"query": "q"},
                "source_mu_ids": [],
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    rc = main(["--bundle", str(bundle_path), "--out", str(out_dir)])
    assert rc == 0
    assert (out_dir / "srb.md").exists()
    assert (out_dir / "srb.json").exists()
