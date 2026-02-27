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
        f"Cannot infer DATA_ROOT from db path: {db_path}. Pass --data-root explicitly."
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
    """v0.1 canonicalization (convergent single-hop).

    Authoritative semantics: see
      - Membership_Layer_Design_v0_1_2026-02-26.md §3.2 (Canonical MU resolution)

    Behavior (MVP):
    - Exclude tombstoned MU ids.
    - Fold to canonical heads using (in priority order):
        1) reverse_supersedes (new MU supersedes old)
        2) reverse_corrects (new MU corrects old)
        3) forward_duplicate_of (this MU is duplicate of canonical target)
    - Apply the above single-hop rewrites repeatedly until stable (bounded; cycle-safe).

    Returns: (canonical_set, diagnostics)
    """

    init_db(db_path)

    if not mu_ids:
        return set(), {"input": 0, "output": 0}

    reverse_corrects: dict[str, str] = {}
    reverse_supersedes: dict[str, str] = {}
    forward_duplicate_of: dict[str, str] = {}
    tombstoned: set[str] = set()

    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT mu_id, corrects_json, supersedes_json, duplicate_of_json, tombstone_json
            FROM mu
            WHERE corrects_json IS NOT NULL
               OR supersedes_json IS NOT NULL
               OR duplicate_of_json IS NOT NULL
               OR tombstone_json IS NOT NULL
            """
        ).fetchall()

    for r in rows:
        mu_id = str(r["mu_id"])
        if r["tombstone_json"] not in (None, "null", ""):
            tombstoned.add(mu_id)

        # reverse edges: old -> new
        for old in _parse_json_list(r["corrects_json"]):
            if old not in reverse_corrects:
                reverse_corrects[old] = mu_id
        for old in _parse_json_list(r["supersedes_json"]):
            if old not in reverse_supersedes:
                reverse_supersedes[old] = mu_id

        # forward edge: dup -> canonical
        dups = _parse_json_list(r["duplicate_of_json"])
        if dups:
            # single-hop: take first target only (stable)
            if mu_id not in forward_duplicate_of:
                forward_duplicate_of[mu_id] = dups[0]

    folded_by_corrects = 0
    folded_by_supersedes = 0
    folded_by_duplicate_of = 0
    tombstoned_excluded = 0
    cycles_detected = 0

    def step(mid: str) -> tuple[str, str | None]:
        """Return (new_mid, edge_type_used)."""
        if mid in reverse_supersedes:
            return reverse_supersedes[mid], "supersedes"
        if mid in reverse_corrects:
            return reverse_corrects[mid], "corrects"
        if mid in forward_duplicate_of:
            return forward_duplicate_of[mid], "duplicate_of"
        return mid, None

    out: set[str] = set()

    for start in mu_ids:
        cur = start
        seen: set[str] = set()
        # bounded convergence: still "single-hop" per iteration
        for _ in range(16):
            if cur in tombstoned:
                tombstoned_excluded += 1
                cur = ""
                break
            if cur in seen:
                cycles_detected += 1
                break
            seen.add(cur)
            nxt, edge = step(cur)
            if edge is None or nxt == cur:
                break
            if edge == "corrects":
                folded_by_corrects += 1
            elif edge == "supersedes":
                folded_by_supersedes += 1
            elif edge == "duplicate_of":
                folded_by_duplicate_of += 1
            cur = nxt

        if cur and (cur not in tombstoned):
            out.add(cur)

    diag = {
        "input": len(mu_ids),
        "output": len(out),
        "folded_by_corrects": folded_by_corrects,
        "folded_by_supersedes": folded_by_supersedes,
        "folded_by_duplicate_of": folded_by_duplicate_of,
        "tombstoned_excluded": tombstoned_excluded,
        "cycles_detected": cycles_detected,
        "reverse_corrects_size": len(reverse_corrects),
        "reverse_supersedes_size": len(reverse_supersedes),
        "forward_duplicate_of_size": len(forward_duplicate_of),
    }
    return out, diag
