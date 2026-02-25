"""Manifest sync executor (P0-7C) v0.1.

This is a conservative executor for tasks emitted by tools/manifest_sync_tasks.py.

Supported TaskSpec types (v0.1):
- VERIFY_MANIFEST: verify sha256 of vault:// uris in a manifest (requires vault_roots)
- REPAIR_MANIFEST_URI: produce suggestions for uri alias (no rewrite) and emit patch plan notes
- SYNC_MANIFEST_APPLY: compute a conservative append-only patch plan and optionally apply safe appends

Design goals:
- default dry-run
- append-only
- no silent overwrite

Note: execution outputs are TaskResult v0.1 compatible JSON.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path

from tools.manifest_apply_plan import apply_plan, plan_patch
from tools.manifest_io import iter_jsonl
from tools.vault_doctor import verify_manifest


@dataclass
class ExecContext:
    vault_roots: dict[str, str]
    tool: str = "manifest_executor"
    tool_version: str = "0.1"


def task_result(
    *,
    task_id: str,
    status: str,
    outputs: list[dict],
    diagnostics: list[dict],
    elapsed_ms: int,
) -> dict:
    return {
        "task_id": task_id,
        "status": status,
        "outputs": outputs,
        "diagnostics": diagnostics,
        "stats": {"elapsed_ms": elapsed_ms, "tokens_in": 0, "tokens_out": 0},
        "provenance": {
            "tool": "mimobrain_memory_system",
            "tool_version": "0.1",
            "model": None,
            "prompt_version": None,
        },
    }


def exec_verify_manifest(task: dict, ctx: ExecContext) -> dict:
    t0 = time.time()
    task_id = task.get("task_id") or ""
    params = task.get("params") or {}
    manifest_path = params.get("manifest_path")

    if not isinstance(manifest_path, str):
        return task_result(
            task_id=task_id,
            status="ERROR",
            outputs=[],
            diagnostics=[{"code": "E_TASK", "msg": "missing params.manifest_path"}],
            elapsed_ms=int((time.time() - t0) * 1000),
        )

    errs = verify_manifest(Path(manifest_path), vault_roots=ctx.vault_roots)
    status = "OK" if not errs else "ERROR"
    diags = [{"code": "E_VERIFY", "msg": e} for e in errs]

    return task_result(
        task_id=task_id,
        status=status,
        outputs=[
            {
                "kind": "REPORT",
                "id": None,
                "uri": None,
                "meta": {"manifest": manifest_path},
            }
        ],
        diagnostics=diags,
        elapsed_ms=int((time.time() - t0) * 1000),
    )


def exec_repair_manifest_uri(task: dict, ctx: ExecContext) -> dict:
    t0 = time.time()
    task_id = task.get("task_id") or ""
    params = task.get("params") or {}

    # In v0.1 this is a no-op executor that returns notes/suggestions.
    sha256 = params.get("sha256")
    base_records = params.get("base_records") or []
    incoming_records = params.get("incoming_records") or []
    policy = params.get("policy")

    if not isinstance(sha256, str):
        return task_result(
            task_id=task_id,
            status="ERROR",
            outputs=[],
            diagnostics=[{"code": "E_TASK", "msg": "missing params.sha256"}],
            elapsed_ms=int((time.time() - t0) * 1000),
        )

    def first_uri(recs: list) -> str | None:
        for r in recs:
            if isinstance(r, dict) and isinstance(r.get("uri"), str):
                return r["uri"]
        return None

    preferred = (
        first_uri(base_records)
        if policy == "prefer_base_uri"
        else first_uri(incoming_records)
    )
    observed = list(
        {u for u in [first_uri(base_records), first_uri(incoming_records)] if u}
    )

    diagnostics = [
        {
            "code": "SUGGEST_URI_ALIAS",
            "msg": f"sha256={sha256} observed uris={observed} preferred={preferred}",
            "sha256": sha256,
            "observed": observed,
            "preferred": preferred,
        }
    ]

    return task_result(
        task_id=task_id,
        status="OK",
        outputs=[
            {
                "kind": "REPORT",
                "id": None,
                "uri": None,
                "meta": {"sha256": sha256, "preferred_uri": preferred},
            }
        ],
        diagnostics=diagnostics,
        elapsed_ms=int((time.time() - t0) * 1000),
    )


def exec_sync_manifest_apply(task: dict, ctx: ExecContext) -> dict:
    t0 = time.time()
    task_id = task.get("task_id") or ""
    params = task.get("params") or {}

    kind = params.get("kind")
    base_path = params.get("base_path")
    incoming_path = params.get("incoming_path")
    dry_run = params.get("dry_run", True)

    if not (
        isinstance(kind, str)
        and isinstance(base_path, str)
        and isinstance(incoming_path, str)
    ):
        return task_result(
            task_id=task_id,
            status="ERROR",
            outputs=[],
            diagnostics=[
                {"code": "E_TASK", "msg": "missing kind/base_path/incoming_path"}
            ],
            elapsed_ms=int((time.time() - t0) * 1000),
        )

    plan = plan_patch(
        kind=kind, base_path=Path(base_path), incoming_path=Path(incoming_path)
    )
    plan["dry_run"] = bool(dry_run)

    if not dry_run:
        apply_plan(plan)

    # Write plan to an explicit out_dir if provided, otherwise next to base manifest.
    out_dir = params.get("out_dir")
    if isinstance(out_dir, str) and out_dir.strip():
        out_path = Path(out_dir) / (Path(base_path).name + ".patch_plan.json")
    else:
        out_path = Path(base_path).with_suffix(".patch_plan.json")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    return task_result(
        task_id=task_id,
        status="OK",
        outputs=[
            {
                "kind": "FILE",
                "id": None,
                "uri": str(out_path),
                "meta": {"dry_run": bool(dry_run)},
            }
        ],
        diagnostics=[],
        elapsed_ms=int((time.time() - t0) * 1000),
    )


def exec_task(task: dict, ctx: ExecContext) -> dict:
    ttype = task.get("type")
    if ttype == "VERIFY_MANIFEST":
        return exec_verify_manifest(task, ctx)
    if ttype == "REPAIR_MANIFEST_URI":
        return exec_repair_manifest_uri(task, ctx)
    if ttype == "SYNC_MANIFEST_APPLY":
        return exec_sync_manifest_apply(task, ctx)

    return task_result(
        task_id=str(task.get("task_id") or ""),
        status="ERROR",
        outputs=[],
        diagnostics=[{"code": "E_TASK", "msg": f"unsupported task type: {ttype}"}],
        elapsed_ms=0,
    )


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--tasks", required=True, help="TaskSpec jsonl path")
    p.add_argument("--out", required=True, help="TaskResult jsonl output path")
    p.add_argument(
        "--vault-root",
        action="append",
        default=[],
        help="Vault root mapping like default=C:/Mimo/vaults/default (repeatable)",
    )
    ns = p.parse_args(argv)

    vault_roots: dict[str, str] = {}
    for item in ns.vault_root:
        if "=" not in item:
            raise SystemExit(f"invalid --vault-root {item!r} (expected vault_id=path)")
        k, v = item.split("=", 1)
        vault_roots[k] = v

    ctx = ExecContext(vault_roots=vault_roots)

    tasks_path = Path(ns.tasks)
    out_path = Path(ns.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as out_f:
        for t in iter_jsonl(tasks_path):
            if not isinstance(t, dict):
                continue
            res = exec_task(t, ctx)
            out_f.write(json.dumps(res, ensure_ascii=False) + "\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
