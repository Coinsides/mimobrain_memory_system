"""Manifest apply planner (P0-7C) v0.1.

This takes base+incoming jsonl manifests and produces a conservative patch plan.

Principles:
- append-only: never rewrite or delete existing manifest lines
- safe defaults: only auto-append records that are clearly new by id_key
- conflicts block auto-apply; they are emitted as BLOCKED_CONFLICT actions

This planner does NOT apply changes by default. It can optionally apply appends
with --apply.

Note on URI changes:
- If same sha256 has different uri across replicas, we do NOT rewrite the old
  record. We emit SUGGEST_URI_ALIAS actions. A later iteration can define an
  explicit alias/redirect manifest.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tools.manifest_io import append_jsonl, iter_jsonl
from tools.manifest_sync import KIND_ID_KEY, analyze_sync, record_fingerprint


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Action:
    type: str
    message: str
    record: dict | None = None
    from_uri: str | None = None
    to_uri: str | None = None
    sha256: str | None = None


def plan_patch(*, kind: str, base_path: Path, incoming_path: Path) -> dict[str, Any]:
    if kind not in KIND_ID_KEY:
        raise ValueError(f"kind must be raw|mu|asset (got {kind!r})")

    id_key = KIND_ID_KEY[kind]

    base_records = list(iter_jsonl(base_path))
    incoming_records = list(iter_jsonl(incoming_path))

    # exact dupes and base ids
    base_fp = {record_fingerprint(r) for r in base_records}
    base_ids = {r.get(id_key) for r in base_records if isinstance(r.get(id_key), str)}

    actions: list[Action] = []
    append_count = 0
    skipped_dupes = 0

    # Block on conflicts from the analyzer
    report = analyze_sync(kind=kind, base_path=base_path, incoming_path=incoming_path)
    blocked = [
        c
        for c in report.get("conflicts", [])
        if isinstance(c, dict) and c.get("severity") == "ERROR"
    ]

    for c in blocked:
        actions.append(
            Action(
                type="BLOCKED_CONFLICT",
                message=f"blocked due to conflict: {c.get('type')} key={c.get('key')}",
                record=None,
            )
        )

    # Suggest uri alias when same sha differs uri
    for c in report.get("conflicts", []):
        if not isinstance(c, dict):
            continue
        if c.get("type") != "SHA_COLLISION_DIFFERENT_URI":
            continue
        sha = c.get("key")
        base_recs = c.get("base_records") or []
        inc_recs = c.get("incoming_records") or []
        if not (isinstance(sha, str) and base_recs and inc_recs):
            continue
        b_uri = next(
            (
                r.get("uri")
                for r in base_recs
                if isinstance(r, dict) and isinstance(r.get("uri"), str)
            ),
            None,
        )
        i_uri = next(
            (
                r.get("uri")
                for r in inc_recs
                if isinstance(r, dict) and isinstance(r.get("uri"), str)
            ),
            None,
        )
        if b_uri and i_uri and b_uri != i_uri:
            actions.append(
                Action(
                    type="SUGGEST_URI_ALIAS",
                    message=f"same sha256={sha} observed at different uris; consider alias/redirect",
                    from_uri=i_uri,
                    to_uri=b_uri,
                    sha256=sha,
                )
            )

    # If there are ERROR conflicts, we still allow planning appends for brand-new ids,
    # but the plan will be marked dry_run-only by caller unless --force-apply later.

    for r in incoming_records:
        if not isinstance(r, dict):
            continue
        rid = r.get(id_key)
        if not isinstance(rid, str):
            continue

        fp = record_fingerprint(r)
        if fp in base_fp:
            skipped_dupes += 1
            continue

        if rid in base_ids:
            # existing id but not an exact dupe: do not append automatically
            actions.append(
                Action(
                    type="NOTE",
                    message=f"record with existing {id_key}={rid} differs; not appending automatically",
                    record=r,
                )
            )
            continue

        actions.append(
            Action(type="APPEND_RECORD", message=f"append new {id_key}={rid}", record=r)
        )
        append_count += 1

    plan = {
        "plan_version": "0.1",
        "created_at": now_iso(),
        "kind": kind,
        "base_path": str(base_path),
        "incoming_path": str(incoming_path),
        "dry_run": True,
        "stats": {
            "append_new_records": append_count,
            "skipped_exact_dupes": skipped_dupes,
            "blocked_conflicts": len(blocked),
        },
        "actions": [asdict(a) for a in actions],
    }
    return plan


def apply_plan(plan: dict[str, Any]) -> None:
    base_path = Path(plan["base_path"])
    for a in plan.get("actions", []):
        if not isinstance(a, dict):
            continue
        if a.get("type") != "APPEND_RECORD":
            continue
        rec = a.get("record")
        if isinstance(rec, dict):
            append_jsonl(base_path, rec)


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--kind", required=True, choices=["raw", "mu", "asset"])
    p.add_argument("--base", required=True)
    p.add_argument("--incoming", required=True)
    p.add_argument("--out", required=True)
    p.add_argument(
        "--apply",
        action="store_true",
        help="Actually append safe new records to base manifest",
    )
    ns = p.parse_args(argv)

    plan = plan_patch(
        kind=ns.kind, base_path=Path(ns.base), incoming_path=Path(ns.incoming)
    )
    plan["dry_run"] = not bool(ns.apply)

    out_p = Path(ns.out)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    out_p.write_text(
        json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    if ns.apply:
        apply_plan(plan)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
