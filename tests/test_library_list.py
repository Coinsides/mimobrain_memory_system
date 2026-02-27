from __future__ import annotations

import json
from pathlib import Path

from tools.meta_db import connect, init_db


def test_library_list_smoke(tmp_path: Path, capsys):
    from tools.library_list import main

    data_root = tmp_path / "data"
    db = data_root / "index" / "meta.sqlite"
    init_db(db)

    # write membership events
    ws_dir = data_root / "workspaces"
    ws_dir.mkdir(parents=True, exist_ok=True)
    (ws_dir / "membership.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {"event": "add", "workspace_id": "ws_design", "mu_id": "mu_1"}
                ),
                json.dumps(
                    {"event": "add", "workspace_id": "ws_design", "mu_id": "mu_2"}
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    # index mu rows
    with connect(db) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO mu(mu_id, time, summary, privacy_level, path) VALUES (?,?,?,?,?)",
            ("mu_1", "2026-02-26T00:00:00Z", "s1", "private", "C:/x/mu_1.mimo"),
        )
        conn.execute(
            "INSERT OR REPLACE INTO mu(mu_id, time, summary, privacy_level, path) VALUES (?,?,?,?,?)",
            ("mu_2", "2026-02-25T00:00:00Z", "s2", "private", "C:/x/mu_2.mimo"),
        )
        conn.commit()

    rc = main(
        [
            "--db",
            str(db),
            "--data-root",
            str(data_root),
            "--workspace",
            "ws_design",
            "--limit",
            "10",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    obj = json.loads(out)
    assert obj["workspace"] == "ws_design"
    assert obj["count"] == 2
    got = [it["mu_id"] for it in obj["items"]]
    assert set(got) == {"mu_1", "mu_2"}
