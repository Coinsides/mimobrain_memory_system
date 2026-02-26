"""Workspace membership layer (file-first) (Patch A).

Authoritative semantics live in:
  Membership_Layer_Design_v0_1_2026-02-26.md (in MVP docs folder)

This module provides:
- effective membership set computation (add/remove event log)
- minimal canonicalization helpers (v0.1: single-hop) using meta.sqlite

Notes:
- MU must remain pure; no workspace_id fields or ws:* tags are used.
- Membership is local state under DATA_ROOT/workspaces/.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from tools.meta_db import connect, init_db


@dataclass(frozen=True)
class MembershipDiagnostics:
    workspace_id: str
    membership_path: str
    events_total: int
    adds: int
    removes: int
    effective_count: int


def infer_data_root_from_db(db_path: Path) -> Path:
    """Infer DATA_ROOT from <DATA_ROOT>/index/meta.sqlite."""
    p = Path(db_path)
    parent = p.parent
    if parent.name.lower() == "index":
        return parent.parent
    raise ValueError(
        f"Cannot infer DATA_ROOT from db path: {db_path}. "
        "Pass --data-root explicitly."
    )


def membership_paths(data_root: Path) -> tuple[Path, Path]:
    ws_dir = Path(data_root) / "workspaces"
    return ws_dir / "workspaces.json", ws_dir / "membership.jsonl"


def load_effective_membership(
    *, data_root: Path, workspace_id: str
) -> tuple[set[str], MembershipDiagnostics]:
    _, membership_path = membership_paths(data_root)

    if not membership_path.exists():
        raise FileNotFoundError(
            f"membership.jsonl not found: {membership_path} (workspace={workspace_id})"
        )

    effective: set[str] = set()
    events_total = 0
    adds = 0
    removes = 0

    for line in membership_path.read_text(encoding="utf-8").splitlines():
        s = line.strip().lstrip("\ufeff")
        if not s:
            continue
        events_total += 1
        try:
            obj = json.loads(s)
        except Exception:
            # Ignore malformed lines (but keep deterministic semantics for valid lines)
            continue
        if not isinstance(obj, dict):
            continue
        if obj.get("workspace_id") != workspace_id:
            continue
        ev = obj.get("event")
        mu_id = obj.get("mu_id")
        if not isinstance(mu_id, str) or not mu_id:
            continue
        if ev == "add":
            adds += 1
            effective.add(mu_id)
        elif ev == "remove":
            removes += 1
            effective.discard(mu_id)

    diag = MembershipDiagnostics(
        workspace_id=workspace_id,
        membership_path=str(membership_path),
        events_total=events_total,
        adds=adds,
        removes=removes,
        effective_count=len(effective),
    )
    return effective, diag


def _parse_json_list(maybe_json: str | None) -> list[str]:
    if not maybe_json:
        return []
    try:
        x = json.loads(maybe_json)
    except Exception:
        return []
    if isinstance(x, list):
        return [str(i) for i in x if isinstance(i, (str, int, float))]
    return []


def canonicalize_mu_ids_single_hop(
    *, db_path: Path, mu_ids: set[str]
) -> tuple[set[str], dict]:
    """v0.1 canonicalization.

    - Exclude tombstoned MU.
    - Apply single-hop corrects folding (reverse index built from corrects_json).

    Returns: (canonical_set, diagnostics)
    """

    init_db(db_path)

    if not mu_ids:
        return set(), {"input": 0, "output": 0}

    # Build reverse corrects map: old_mu_id -> new_mu_id (single-hop)
    reverse_corrects: dict[str, str] = {}
    tombstoned: set[str] = set()

    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT mu_id, corrects_json, tombstone_json FROM mu WHERE corrects_json IS NOT NULL OR tombstone_json IS NOT NULL"
        ).fetchall()

    for r in rows:
        mu_id = str(r["mu_id"])
        if r["tombstone_json"] not in (None, "null", ""):
            tombstoned.add(mu_id)
        for old in _parse_json_list(r["corrects_json"]):
            # single-hop: keep the first seen mapping (stable across runs given stable DB)
            if old not in reverse_corrects:
                reverse_corrects[old] = mu_id

    out: set[str] = set()
    mapped = 0
    dropped_tombstone = 0

    for mid in mu_ids:
        new_mid = reverse_corrects.get(mid, mid)
        if new_mid != mid:
            mapped += 1
        if new_mid in tombstoned:
            dropped_tombstone += 1
            continue
        out.add(new_mid)

    diag = {
        "input": len(mu_ids),
        "output": len(out),
        "mapped_by_corrects": mapped,
        "dropped_tombstone": dropped_tombstone,
        "reverse_corrects_size": len(reverse_corrects),
    }
    return out, diag
