"""Require an engineering journal update for code changes.

This script is intentionally simple and deterministic.
It enforces: if code/docs-contracts change, then at least one repo-safe log file
must also change.

Repo-safe logs:
- docs/LOG.md (human milestones)
- logs/task_journal.jsonl (machine journal, append-only)

Usage (local):
  python tools/require_journal.py --base origin/main --head HEAD

Usage (CI PR):
  python tools/require_journal.py --base $BASE_SHA --head $HEAD_SHA

Exit codes:
- 0 OK
- 2 gate failed (missing journal)
- 3 not a git repo / cannot diff
"""

from __future__ import annotations

import argparse
import subprocess


REPO_LOG_PATHS = {
    "docs/LOG.md",
    "logs/task_journal.jsonl",
}

# If any of these paths change, we require a log update.
TRIGGER_PREFIXES = (
    "src/",
    "tools/",
    "tests/",
    "docs/contracts/",
    "docs/adr/",
)


def run_git(args: list[str]) -> str:
    p = subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip() or p.stdout.strip() or "git command failed")
    return p.stdout


def changed_files(base: str, head: str) -> list[str]:
    out = run_git(["diff", "--name-only", f"{base}..{head}"])
    files = [line.strip() for line in out.splitlines() if line.strip()]
    return files


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="origin/main")
    parser.add_argument("--head", default="HEAD")
    ns = parser.parse_args()

    # Ensure we're in a git repo
    try:
        run_git(["rev-parse", "--show-toplevel"])
    except Exception as e:
        print(f"ERROR: not a git repo or git unavailable: {e}")
        return 3

    files = changed_files(ns.base, ns.head)
    if not files:
        print("OK: no changes detected")
        return 0

    changed_set = set(files)

    # Determine if any triggering paths were changed (excluding the logs themselves)
    triggered = False
    for f in files:
        if f in REPO_LOG_PATHS:
            continue
        if f.startswith(TRIGGER_PREFIXES):
            triggered = True
            break

    if not triggered:
        print("OK: no gated paths changed")
        return 0

    # Require that at least one log file changed in this diff
    if REPO_LOG_PATHS.intersection(changed_set):
        print("OK: journal updated")
        return 0

    print("GATE_FAIL: code changes detected but no journal update found.")
    print("Required to change at least one of:")
    for p in sorted(REPO_LOG_PATHS):
        print(f"- {p}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
