"""Manifest sync tasks (P0-7B) v0.1.

Convert a manifest sync report (from tools/manifest_sync.py) into TaskSpec v0.1
items for later execution.

We keep this conservative:
- Always emit VERIFY_MANIFEST tasks first.
- Emit REPAIR_MANIFEST_URI tasks for SHA_COLLISION_DIFFERENT_URI conflicts.
- Emit MANUAL_REVIEW conflicts as a single SYNC_MANIFEST_APPLY task with dry_run.

This is a planning layer only; execution is a separate component.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_task_id() -> str:
    return "t_" + uuid.uuid4().hex


def task(
    *,
    type: str,
    idempotency_key: str,
    inputs: list[dict],
    params: dict,
) -> dict[str, Any]:
    return {
        "task_id": new_task_id(),
        "type": type,
        "created_at": now_iso(),
        "parent_task_id": None,
        "idempotency_key": idempotency_key,
        "inputs": inputs,
        "params": params,
    }


def tasks_from_report(report: dict) -> list[dict[str, Any]]:
    kind = report.get("kind")
    base_path = (report.get("base") or {}).get("path")
    incoming_path = (report.get("incoming") or {}).get("path")

    tasks: list[dict[str, Any]] = []

    # Always verify both manifests (dry-run planning; actual executor supplies vault_roots).
    if isinstance(base_path, str):
        tasks.append(
            task(
                type="VERIFY_MANIFEST",
                idempotency_key=f"verify:{kind}:base:{base_path}",
                inputs=[{"kind": "TEXT", "ids": [base_path]}],
                params={"kind": kind, "manifest_path": base_path},
            )
        )
    if isinstance(incoming_path, str):
        tasks.append(
            task(
                type="VERIFY_MANIFEST",
                idempotency_key=f"verify:{kind}:incoming:{incoming_path}",
                inputs=[{"kind": "TEXT", "ids": [incoming_path]}],
                params={"kind": kind, "manifest_path": incoming_path},
            )
        )

    conflicts = report.get("conflicts") or []
    if not isinstance(conflicts, list):
        conflicts = []

    manual: list[dict] = []

    for c in conflicts:
        if not isinstance(c, dict):
            continue
        ctype = c.get("type")

        if ctype == "SHA_COLLISION_DIFFERENT_URI":
            # Suggest a manifest uri repair mapping (planning only)
            key = c.get("key")
            tasks.append(
                task(
                    type="REPAIR_MANIFEST_URI",
                    idempotency_key=f"repair_uri:{kind}:{key}",
                    inputs=[],
                    params={
                        "kind": kind,
                        "sha256": key,
                        "base_records": c.get("base_records") or [],
                        "incoming_records": c.get("incoming_records") or [],
                        "policy": "prefer_base_uri",
                        "dry_run": True,
                    },
                )
            )
        elif ctype in {
            "SCHEMA_ERROR",
            "ID_COLLISION_DIFFERENT_SHA",
            "URI_COLLISION_DIFFERENT_SHA",
        }:
            manual.append(c)

    # Always include a conservative apply planning task, even when there are no manual conflicts.
    # This lets the system append brand-new ids and produce a patch plan artifact under the run_dir.
    tasks.append(
        task(
            type="SYNC_MANIFEST_APPLY",
            idempotency_key=f"sync_apply:{kind}:{base_path}:{incoming_path}",
            inputs=[],
            params={
                "kind": kind,
                "base_path": base_path,
                "incoming_path": incoming_path,
                "dry_run": True,
                "manual_conflicts": manual,
                "policy": "conservative_no_overwrite",
            },
        )
    )

    return tasks


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--report", required=True, help="Sync report JSON (from manifest_sync.py)")
    p.add_argument("--out", required=True, help="Output tasks path (jsonl)")
    ns = p.parse_args(argv)

    report = json.loads(Path(ns.report).read_text(encoding="utf-8"))
    out_p = Path(ns.out)
    out_p.parent.mkdir(parents=True, exist_ok=True)

    tasks = tasks_from_report(report)
    with out_p.open("w", encoding="utf-8") as f:
        for t in tasks:
            f.write(json.dumps(t, ensure_ascii=False) + "\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
