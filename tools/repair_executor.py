"""Repair executor (P1-G) — execute REPAIR_POINTER tasks.

v0.1 scope:
- Supports TaskSpec type=REPAIR_POINTER.
- Does not rewrite MU files. Emits suggestions / diagnostics only.

Inputs:
- TaskSpec.params should include: mu_id, sha256, uri (optional), hint (optional)
- ExecContext can include raw_manifest_path to suggest a vault:// uri by sha256.

Outputs:
- TaskResult v0.1 JSON dict.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tools.vault_doctor import repair_suggest_by_sha256


@dataclass
class ExecContext:
    vault_roots: dict[str, str]
    raw_manifest_path: str | None = None
    out_mu_dir: str | None = None
    tool: str = "repair_executor"
    tool_version: str = "0.1"


def task_result(*, task_id: str, status: str, outputs: list[dict], diagnostics: list[dict], elapsed_ms: int) -> dict:
    return {
        "task_id": task_id,
        "status": status,
        "outputs": outputs,
        "diagnostics": diagnostics,
        "stats": {"elapsed_ms": elapsed_ms, "tokens_in": 0, "tokens_out": 0},
        "provenance": {"tool": "mimobrain_memory_system", "tool_version": "0.1", "model": None, "prompt_version": None},
    }


def exec_repair_pointer(task: dict, ctx: ExecContext) -> dict:
    t0 = time.time()
    task_id = str(task.get("task_id") or "")
    params = task.get("params") or {}

    mu_id = params.get("mu_id")
    mu_path = params.get("mu_path")
    sha256 = params.get("sha256")
    uri = params.get("uri")

    if not isinstance(mu_id, str) or not mu_id:
        return task_result(
            task_id=task_id,
            status="ERROR",
            outputs=[],
            diagnostics=[{"code": "E_TASK", "msg": "missing params.mu_id"}],
            elapsed_ms=int((time.time() - t0) * 1000),
        )

    if not isinstance(sha256, str) or not sha256.startswith("sha256:"):
        return task_result(
            task_id=task_id,
            status="ERROR",
            outputs=[],
            diagnostics=[{"code": "E_TASK", "msg": "missing/invalid params.sha256"}],
            elapsed_ms=int((time.time() - t0) * 1000),
        )

    suggested_uri = None
    if ctx.raw_manifest_path:
        try:
            suggested_uri = repair_suggest_by_sha256(Path(ctx.raw_manifest_path), sha256=sha256)
        except Exception:
            suggested_uri = None

    outputs: list[dict] = [{"kind": "REPORT", "id": None, "uri": None, "meta": {"mu_id": mu_id, "sha256": sha256, "suggested_uri": suggested_uri}}]

    diags: list[dict[str, Any]] = []

    fixed_mu_path = None
    if suggested_uri and suggested_uri != uri:
        diags.append(
            {
                "code": "SUGGEST_POINTER_URI",
                "msg": f"suggest pointer uri by sha256: {suggested_uri}",
                "mu_id": mu_id,
                "sha256": sha256,
                "old_uri": uri,
                "suggested_uri": suggested_uri,
            }
        )

        # Optional auto-fix: write a superseding MU with updated pointer uri.
        if ctx.out_mu_dir and isinstance(mu_path, str) and mu_path:
            try:
                import yaml
                # local minimal dump/id helpers (avoid importing private helpers)
                import hashlib
                from datetime import datetime, timezone

                def _new_mu_id() -> str:
                    seed = f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}:{mu_path}".encode("utf-8")
                    rnd = hashlib.sha256(seed).hexdigest()[:10]
                    return f"mu_migr_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{rnd}"

                def _dump_mu(obj: dict) -> str:
                    return yaml.safe_dump(obj, sort_keys=False, allow_unicode=True)

                mu_obj = yaml.safe_load(Path(mu_path).read_text(encoding="utf-8"))
                if isinstance(mu_obj, dict):
                    pointers = mu_obj.get("pointer")
                    if isinstance(pointers, list):
                        new_pointers = []
                        changed = 0
                        for p in pointers:
                            if isinstance(p, dict) and p.get("sha256") == sha256:
                                p2 = dict(p)
                                p2["uri"] = suggested_uri
                                new_pointers.append(p2)
                                changed += 1
                            else:
                                new_pointers.append(p)
                        mu_obj["pointer"] = new_pointers

                        # supersedes
                        links = mu_obj.get("links")
                        if not isinstance(links, dict):
                            links = {}
                        supersedes = links.get("supersedes")
                        if supersedes is None:
                            supersedes = []
                        if not isinstance(supersedes, list):
                            supersedes = [supersedes]
                        if mu_id not in supersedes:
                            supersedes.append(mu_id)
                        links["supersedes"] = supersedes
                        mu_obj["links"] = links

                        new_id = _new_mu_id()
                        mu_obj["mu_id"] = new_id
                        out_dir = Path(ctx.out_mu_dir)
                        out_dir.mkdir(parents=True, exist_ok=True)
                        fixed_mu_path = out_dir / f"{new_id}.mimo"
                        fixed_mu_path.write_text(_dump_mu(mu_obj), encoding="utf-8")

                        outputs.append({"kind": "MU", "id": new_id, "uri": str(fixed_mu_path), "meta": {"supersedes": mu_id, "changed_pointers": changed}})
                        diags.append({"code": "AUTO_FIXED", "msg": "wrote superseding MU with migrated pointer", "new_mu_id": new_id, "new_mu_path": str(fixed_mu_path)})
            except Exception as e:
                diags.append({"code": "AUTO_FIX_FAILED", "msg": str(e)})

        status = "OK"
    else:
        diags.append(
            {
                "code": "NO_SUGGESTION",
                "msg": "no suggestion found (missing raw_manifest_path or sha256 not present)",
                "mu_id": mu_id,
                "sha256": sha256,
                "old_uri": uri,
            }
        )
        status = "PARTIAL"

    return task_result(
        task_id=task_id,
        status=status,
        outputs=outputs,
        diagnostics=diags,
        elapsed_ms=int((time.time() - t0) * 1000),
    )


def exec_task(task: dict, ctx: ExecContext) -> dict:
    ttype = task.get("type")
    if ttype == "REPAIR_POINTER":
        return exec_repair_pointer(task, ctx)
    return task_result(
        task_id=str(task.get("task_id") or ""),
        status="ERROR",
        outputs=[],
        diagnostics=[{"code": "E_TASK", "msg": f"unsupported task type: {ttype}"}],
        elapsed_ms=0,
    )
