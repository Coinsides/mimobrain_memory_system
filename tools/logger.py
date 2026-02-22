"""Structured logger (P0-11) writing jsonl events.

This is intentionally minimal and append-only.

Default log root: <repo>/logs/

Event schema: docs/contracts/log_event_v0_1.schema.json

We do not hard-require validation at runtime; tests cover schema shape.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_event(
    *,
    event: str,
    log_path: Path,
    task_id: str | None = None,
    trace_id: str | None = None,
    run_id: str | None = None,
    run_dir: str | None = None,
    tool: str | None = None,
    tool_version: str | None = None,
    schema_version: str | None = None,
    inputs: list[dict] | None = None,
    outputs: list[dict] | None = None,
    stats: dict | None = None,
    diagnostics: dict | None = None,
    **extra: Any,
) -> dict:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    obj: dict[str, Any] = {
        "ts": utc_now(),
        "event": event,
        "task_id": task_id,
        "trace_id": trace_id,
        "run_id": run_id,
        "run_dir": run_dir,
        "tool": tool,
        "tool_version": tool_version,
        "schema_version": schema_version,
        "inputs": inputs,
        "outputs": outputs,
        "stats": stats,
        "diagnostics": diagnostics,
    }
    # attach extra fields (non-breaking)
    for k, v in extra.items():
        if k not in obj:
            obj[k] = v

    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    return obj


def default_log_path(name: str) -> Path:
    # repo_root/logs/<name>.jsonl
    repo_root = Path(__file__).resolve().parents[1]
    return repo_root / "logs" / f"{name}.jsonl"
