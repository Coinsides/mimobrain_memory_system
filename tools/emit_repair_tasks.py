"""Emit TaskSpec(s) from bundle.diagnostics.repair_tasks (P1-G).

This converts the *repair trigger signals* inside a bundle into concrete TaskSpec
JSON files (append-only artifacts) that can later be executed by an executor.

Input:
- bundle.json (MemoryBundle v0.1)
  - expects bundle.diagnostics.repair_tasks: list[object]

Output:
- writes one TaskSpec JSON per repair task

v0.1 scope:
- Only supports type=REPAIR_POINTER
- Deterministic idempotency_key based on mu_id + sha256 + uri
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _task_id(prefix: str, *, payload: str) -> str:
    h = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"{prefix}_{ts}_{h}"


def _idempotency_key(*parts: str) -> str:
    h = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return "sha256:" + h


@dataclass(frozen=True)
class EmitSummary:
    wrote: int
    out_dir: Path


def emit_repair_tasks(
    bundle_path: str | Path,
    *,
    out_dir: str | Path,
) -> EmitSummary:
    bpath = Path(bundle_path)
    out_dir_p = Path(out_dir)
    out_dir_p.mkdir(parents=True, exist_ok=True)

    bundle = json.loads(bpath.read_text(encoding="utf-8"))
    diag = bundle.get("diagnostics") if isinstance(bundle, dict) else None
    repair = diag.get("repair_tasks") if isinstance(diag, dict) else None

    if not isinstance(repair, list) or not repair:
        return EmitSummary(wrote=0, out_dir=out_dir_p)

    wrote = 0
    for t in repair:
        if not isinstance(t, dict):
            continue
        if t.get("type") != "REPAIR_POINTER":
            continue

        mu_id = t.get("mu_id")
        mu_path = t.get("mu_path")
        sha256 = t.get("sha256")
        uri = t.get("uri")
        reason = t.get("reason")
        hint = t.get("hint")

        if not isinstance(mu_id, str) or not mu_id:
            continue

        idem = _idempotency_key(
            "REPAIR_POINTER", mu_id, str(sha256 or ""), str(uri or "")
        )
        task_id = _task_id("t_repair_pointer", payload=idem)

        spec: dict[str, Any] = {
            "task_id": task_id,
            "type": "REPAIR_POINTER",
            "created_at": _utc_now_iso(),
            "parent_task_id": None,
            "idempotency_key": idem,
            "inputs": [{"kind": "MU_SET", "ids": [mu_id]}],
            "params": {
                "mu_id": mu_id,
                "mu_path": mu_path,
                "sha256": sha256,
                "uri": uri,
                "reason": reason,
                "hint": hint,
                # keep a backref for audit
                "source_bundle_id": bundle.get("bundle_id"),
            },
        }

        out_path = out_dir_p / f"{task_id}.task_spec.json"
        out_path.write_text(
            json.dumps(spec, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        wrote += 1

    return EmitSummary(wrote=wrote, out_dir=out_dir_p)


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--bundle", required=True, help="Bundle JSON path")
    ap.add_argument(
        "--out-dir", required=True, help="Output directory for TaskSpec JSON files"
    )
    ns = ap.parse_args(argv)

    s = emit_repair_tasks(ns.bundle, out_dir=ns.out_dir)
    print(json.dumps({"wrote": s.wrote, "out_dir": str(s.out_dir)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
