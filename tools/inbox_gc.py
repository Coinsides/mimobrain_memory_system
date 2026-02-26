"""Inbox garbage-collection (Phase 2) — dry-run first.

Usage:
  python -m tools.inbox_gc --data-root "C:/memobrain/data/memory_system" --days 30 --dry-run

Scope:
- Scans <DATA_ROOT>/inbox/<workspace_id>/_done and _failed
- Lists job folders older than N days.
- By default we only support --dry-run (safe MVP).
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class GcItem:
    path: str
    state: str
    age_days: float


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--data-root", required=True)
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--dry-run", action="store_true")
    ns = p.parse_args(argv)

    if not ns.dry_run:
        raise SystemExit("Only --dry-run is supported in MVP (safety).")

    data_root = Path(ns.data_root)
    inbox_root = data_root / "inbox"
    cutoff = time.time() - float(ns.days) * 86400.0

    items: list[GcItem] = []

    if inbox_root.exists():
        for ws_dir in sorted(inbox_root.iterdir()):
            if not ws_dir.is_dir():
                continue
            for state in ("_done", "_failed"):
                st_dir = ws_dir / state
                if not st_dir.exists():
                    continue
                for job_dir in sorted(st_dir.iterdir()):
                    if not job_dir.is_dir():
                        continue
                    try:
                        mtime = job_dir.stat().st_mtime
                    except OSError:
                        continue
                    if mtime <= cutoff:
                        age_days = (time.time() - mtime) / 86400.0
                        items.append(
                            GcItem(
                                path=str(job_dir),
                                state=state.lstrip("_"),
                                age_days=age_days,
                            )
                        )

    out = {
        "data_root": str(data_root),
        "days": int(ns.days),
        "dry_run": True,
        "count": len(items),
        "items": [asdict(i) for i in items],
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
