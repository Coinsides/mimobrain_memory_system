from __future__ import annotations

import json
from pathlib import Path

from tools.assign_membership import append_membership_events


def test_append_membership_events_skips_duplicates(tmp_path: Path):
    data_root = tmp_path / "data"

    res1 = append_membership_events(
        data_root=data_root,
        workspace="ws_design",
        mu_ids=["mu_1", "mu_2"],
        source="job:1",
    )
    assert res1.appended_events == 2

    res2 = append_membership_events(
        data_root=data_root,
        workspace="ws_design",
        mu_ids=["mu_1", "mu_2", "mu_3"],
        source="job:2",
    )
    assert res2.appended_events == 1

    membership_path = data_root / "workspaces" / "membership.jsonl"
    lines = membership_path.read_text(encoding="utf-8").splitlines()
    # 2 adds + 1 add = 3 events total
    assert len(lines) == 3
    objs = [json.loads(line) for line in lines]
    assert [o["mu_id"] for o in objs] == ["mu_1", "mu_2", "mu_3"]
