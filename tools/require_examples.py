"""Gate: require example artifacts update when sync contracts/tools change.

Rationale: repo should include a minimal, repo-safe "mirror" of authoritative
local runs, so contract/tool changes remain reproducible.

Rule (v0.1):
If any of these change:
- tools/manifest_sync.py
- tools/manifest_sync_tasks.py
- docs/contracts/manifest_sync_report_v0_1.schema.json
- docs/contracts/task_spec_v0_1.schema.json

Then at least one of these must change in the same diff:
- examples/sync_reports/
- examples/sync_tasks/
- examples/manifests/

Usage:
  python tools/require_examples.py --base origin/main --head HEAD
"""

from __future__ import annotations

import argparse
import subprocess


TRIGGERS = {
    "tools/manifest_sync.py",
    "tools/manifest_sync_tasks.py",
    "docs/contracts/manifest_sync_report_v0_1.schema.json",
    "docs/contracts/task_spec_v0_1.schema.json",
}

EXAMPLE_PREFIXES = (
    "examples/sync_reports/",
    "examples/sync_tasks/",
    "examples/manifests/",
)


def run_git(args: list[str]) -> str:
    p = subprocess.run(["git", *args], capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip() or p.stdout.strip() or "git command failed")
    return p.stdout


def changed_files(base: str, head: str) -> list[str]:
    out = run_git(["diff", "--name-only", f"{base}..{head}"])
    return [line.strip() for line in out.splitlines() if line.strip()]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="origin/main")
    ap.add_argument("--head", default="HEAD")
    ns = ap.parse_args()

    try:
        run_git(["rev-parse", "--show-toplevel"])
    except Exception as e:
        print(f"ERROR: not a git repo or git unavailable: {e}")
        return 3

    files = changed_files(ns.base, ns.head)
    if not files:
        print("OK: no changes detected")
        return 0

    triggered = any(f in TRIGGERS for f in files)
    if not triggered:
        print("OK: no example gate triggers")
        return 0

    has_example_change = any(
        any(f.startswith(pfx) for pfx in EXAMPLE_PREFIXES) for f in files
    )
    if has_example_change:
        print("OK: examples updated")
        return 0

    print("GATE_FAIL: sync tool/contract changed but examples not updated.")
    print("Please update at least one file under:")
    for pfx in EXAMPLE_PREFIXES:
        print(f"- {pfx}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
