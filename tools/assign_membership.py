"""Assign MU to a workspace via membership.jsonl (append-only).

This tool is the pipeline step that replaces legacy MU-embedded workspace fields.

Usage examples:
  python -m tools.assign_membership \
    --data-root "C:/memobrain/data/memory_system" \
    --workspace ws_design \
    --mu-dir "C:/memobrain/data/memory_system/staging/mu_out/regen_20260226" \
    --source job:regen_20260226

Notes:
- MU must remain pure. This tool is the *only* place where workspace scope is written.
- Writes JSONL events to: <DATA_ROOT>/workspaces/membership.jsonl
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


def now_iso_z() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def iter_mu_ids_from_dir(mu_dir: Path) -> list[str]:
    out: list[str] = []
    for p in sorted(mu_dir.rglob("*.mimo")):
        if p.is_file():
            out.append(p.stem)
    return out


@dataclass(frozen=True)
class AssignResult:
    data_root: str
    workspace: str
    membership_path: str
    mu_count: int
    appended_events: int
    source: str


def append_membership_events(
    *, data_root: Path, workspace: str, mu_ids: list[str], source: str
) -> AssignResult:
    ws_dir = data_root / "workspaces"
    ws_dir.mkdir(parents=True, exist_ok=True)
    membership_path = ws_dir / "membership.jsonl"

    at = now_iso_z()
    lines: list[str] = []
    for mid in mu_ids:
        lines.append(
            json.dumps(
                {
                    "event": "add",
                    "workspace_id": workspace,
                    "mu_id": mid,
                    "at": at,
                    "source": source,
                },
                ensure_ascii=False,
            )
        )

    if lines:
        # Avoid BOM; always write utf-8.
        membership_path.open("a", encoding="utf-8", newline="\n").write(
            "\n".join(lines) + "\n"
        )

    return AssignResult(
        data_root=str(data_root),
        workspace=str(workspace),
        membership_path=str(membership_path),
        mu_count=len(mu_ids),
        appended_events=len(lines),
        source=str(source),
    )


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--data-root", required=True)
    p.add_argument("--workspace", required=True)
    p.add_argument(
        "--mu-dir",
        required=True,
        help="Directory containing .mimo files (staging output or vault/mu subset)",
    )
    p.add_argument(
        "--source",
        required=True,
        help="Source label (job_id/batch_id). Example: job:import:JOB-123",
    )
    ns = p.parse_args(argv)

    data_root = Path(ns.data_root)
    mu_dir = Path(ns.mu_dir)
    if not mu_dir.exists():
        raise SystemExit(f"missing --mu-dir: {mu_dir}")

    mu_ids = iter_mu_ids_from_dir(mu_dir)
    res = append_membership_events(
        data_root=data_root,
        workspace=str(ns.workspace),
        mu_ids=mu_ids,
        source=str(ns.source),
    )

    print(json.dumps(asdict(res), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
