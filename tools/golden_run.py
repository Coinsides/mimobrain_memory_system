"""Run golden set evaluation (skeleton).

At this stage, we don't yet have a full orchestrator that can answer queries
from MU storage. We still want a stable runner that:
- validates questions
- runs a placeholder responder (or a future orchestrator)
- checks must_include/must_not as basic invariants
- writes a JSON report + Markdown summary

Usage:
  python tools/golden_run.py --out-dir runs/golden/RUN-... 
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from tools.golden_validate import load_questions


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_id() -> str:
    return datetime.now(timezone.utc).strftime("RUN-%Y%m%d-%H%M%S")


def placeholder_answer(q: dict) -> str:
    # Minimal deterministic placeholder. Real implementation will call orchestrator.
    return "[NOT_IMPLEMENTED] " + (q.get("query") or "")


def check_invariants(answer: str, expect: dict) -> dict:
    must_include = expect.get("must_include") or []
    must_not = expect.get("must_not") or []

    missing = [s for s in must_include if s and s not in answer]
    present_forbidden = [s for s in must_not if s and s in answer]

    return {
        "missing": missing,
        "forbidden_present": present_forbidden,
        "pass": (not missing) and (not present_forbidden),
    }


def render_markdown(report: dict) -> str:
    total = report["summary"]["total"]
    passed = report["summary"]["passed"]
    failed = report["summary"]["failed"]
    skipped = report["summary"]["skipped"]

    lines = []
    lines.append(f"# Golden Report ({report['run_id']})")
    lines.append("")
    lines.append(f"- created_at: {report['created_at']}")
    lines.append(f"- total: {total}")
    lines.append(f"- passed: {passed}")
    lines.append(f"- failed: {failed}")
    lines.append(f"- skipped: {skipped}")
    lines.append("")

    for r in report["results"]:
        lines.append(f"## {r['id']}: {r['status']}")
        lines.append(f"query: {r['query']}")
        inv = r.get("invariants")
        if inv and not inv.get("pass"):
            lines.append(f"- missing: {inv.get('missing')}")
            lines.append(f"- forbidden_present: {inv.get('forbidden_present')}")
        lines.append("")

    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--questions", default=str(Path("golden") / "questions.yaml"))
    p.add_argument("--out-dir", default=str(Path("runs") / "golden" / run_id()))
    ns = p.parse_args(argv)

    questions = load_questions(Path(ns.questions))

    out_dir = Path(ns.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for q in questions:
        qid = q.get("id")
        query = q.get("query")
        expect = q.get("expect") or {}

        ans = placeholder_answer(q)
        inv = check_invariants(ans, expect)
        status = "PASS" if inv["pass"] else "FAIL"

        results.append(
            {
                "id": qid,
                "query": query,
                "status": status,
                "answer": ans,
                "invariants": inv,
                "tags": q.get("tags", []),
            }
        )

    summary = {
        "total": len(results),
        "passed": sum(1 for r in results if r["status"] == "PASS"),
        "failed": sum(1 for r in results if r["status"] == "FAIL"),
        "skipped": 0,
    }

    report = {
        "run_id": out_dir.name,
        "created_at": utc_now(),
        "runner": "golden_run_v0.1",
        "summary": summary,
        "results": results,
    }

    (out_dir / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (out_dir / "report.md").write_text(render_markdown(report), encoding="utf-8")

    # exit non-zero on failures
    return 0 if summary["failed"] == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
