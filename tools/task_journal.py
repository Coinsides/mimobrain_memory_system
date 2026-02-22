"""Task journal (P0-9) — append/query/replay.

This module provides a minimal, deterministic journaling layer for TaskSpec/TaskResult
executions.

Design goals:
- append-only
- easy to inspect (SQLite)
- idempotent-ish (unique(task_id))

Schema:
  tasks(task_id PRIMARY KEY, idempotency_key, type, status, created_at, elapsed_ms, spec_json, result_json)

Usage:
  python tools/task_journal.py init --db <path>
  python tools/task_journal.py append --db <path> --spec <task.json> --result <result.json>
  python tools/task_journal.py query --db <path> [--type SYNC_MANIFEST_APPLY] [--status OK]

"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY,
  idempotency_key TEXT,
  type TEXT,
  status TEXT,
  created_at TEXT,
  elapsed_ms INTEGER,
  spec_json TEXT NOT NULL,
  result_json TEXT NOT NULL,
  context_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_type ON tasks(type);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_idempotency_key ON tasks(idempotency_key);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA_SQL)


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


def append_task(db_path: Path, spec: dict, result: dict, *, context: dict | None = None) -> None:
    init_db(db_path)

    task_id = result.get("task_id") or spec.get("task_id") or spec.get("id")
    if not task_id:
        raise ValueError("missing task_id")

    row = {
        "task_id": task_id,
        "idempotency_key": spec.get("idempotency_key"),
        "type": spec.get("type"),
        "status": result.get("status"),
        "created_at": result.get("created_at") or utc_now(),
        "elapsed_ms": result.get("elapsed_ms"),
        "spec_json": _json_dumps(spec),
        "result_json": _json_dumps(result),
        "context_json": _json_dumps(context) if context is not None else None,
    }

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO tasks
              (task_id, idempotency_key, type, status, created_at, elapsed_ms, spec_json, result_json, context_json)
            VALUES
              (:task_id, :idempotency_key, :type, :status, :created_at, :elapsed_ms, :spec_json, :result_json, :context_json)
            """,
            row,
        )
        conn.commit()


def query_tasks(
    db_path: Path,
    *,
    type: str | None = None,
    status: str | None = None,
    limit: int = 50,
) -> list[dict]:
    init_db(db_path)
    q = "SELECT task_id, idempotency_key, type, status, created_at, elapsed_ms FROM tasks"
    where = []
    params: dict[str, Any] = {}
    if type:
        where.append("type = :type")
        params["type"] = type
    if status:
        where.append("status = :status")
        params["status"] = status
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY created_at DESC LIMIT :limit"
    params["limit"] = int(limit)

    with connect(db_path) as conn:
        rows = conn.execute(q, params).fetchall()
    return [dict(r) for r in rows]


def load_task(db_path: Path, task_id: str) -> tuple[dict, dict, dict | None]:
    init_db(db_path)
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT spec_json, result_json, context_json FROM tasks WHERE task_id=?", (task_id,)
        ).fetchone()
    if not row:
        raise KeyError(task_id)
    ctx = json.loads(row["context_json"]) if row["context_json"] else None
    return json.loads(row["spec_json"]), json.loads(row["result_json"]), ctx


def replay_task(db_path: Path, task_id: str) -> dict:
    spec, _, ctx_data = load_task(db_path, task_id)
    from tools.manifest_executor import ExecContext, exec_task

    vault_roots = {}
    if isinstance(ctx_data, dict) and isinstance(ctx_data.get("vault_roots"), dict):
        vault_roots = ctx_data.get("vault_roots")

    ctx = ExecContext(vault_roots=vault_roots)
    return exec_task(spec, ctx)


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init")
    p_init.add_argument("--db", required=True)

    p_append = sub.add_parser("append")
    p_append.add_argument("--db", required=True)
    p_append.add_argument("--spec", required=True)
    p_append.add_argument("--result", required=True)

    p_query = sub.add_parser("query")
    p_query.add_argument("--db", required=True)
    p_query.add_argument("--type", default=None)
    p_query.add_argument("--status", default=None)
    p_query.add_argument("--limit", type=int, default=50)

    p_replay = sub.add_parser("replay")
    p_replay.add_argument("--db", required=True)
    p_replay.add_argument("--task-id", required=True)

    ns = p.parse_args(argv)
    db = Path(ns.db)

    if ns.cmd == "init":
        init_db(db)
        print("OK")
        return 0

    if ns.cmd == "append":
        spec = json.loads(Path(ns.spec).read_text(encoding="utf-8"))
        result = json.loads(Path(ns.result).read_text(encoding="utf-8"))
        append_task(db, spec, result)
        print("OK")
        return 0

    if ns.cmd == "query":
        rows = query_tasks(db, type=ns.type, status=ns.status, limit=ns.limit)
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return 0

    if ns.cmd == "replay":
        out = replay_task(db, ns.task_id)
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0

    raise SystemExit("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())
