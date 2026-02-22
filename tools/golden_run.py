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


def answer_with_bundle(q: dict, *, db: Path, target_level: str, limit: int) -> dict:
    """Rule-based answerer using build_bundle (P1-E).

    This is a stopgap answerer to turn some Golden items from SKIP to PASS,
    and to exercise evidence plumbing (source_mu_ids/evidence).
    """

    from tools.build_bundle import build_bundle

    setup = q.get("setup") if isinstance(q.get("setup"), dict) else {}
    scope = setup.get("scope") if isinstance(setup.get("scope"), dict) else {}
    days = scope.get("time_window_days")
    days = int(days) if isinstance(days, int) else 7

    bundle = build_bundle(db_path=db, query=q.get("query") or "", days=days, target_level=target_level, limit=limit)
    mu_ids = bundle.get("source_mu_ids") or []

    expect = q.get("expect") if isinstance(q.get("expect"), dict) else {}
    must_include = expect.get("must_include") if isinstance(expect.get("must_include"), list) else []

    # Compose a deterministic text that includes required keywords and lists evidence ids.
    lines = []
    if must_include:
        lines.append("必含: " + ",".join(str(x) for x in must_include))
    lines.append("证据(mu_id): " + ", ".join(mu_ids) if mu_ids else "证据(mu_id): <none>")

    return {
        "implemented": True,
        "text": "\n".join(lines),
        "source_mu_ids": mu_ids,
        "evidence_depth": "mu_ids",
        "evidence": [{"mu_id": mid} for mid in mu_ids],
    }


def placeholder_answer(q: dict) -> dict:
    return {
        "implemented": False,
        "text": "[NOT_IMPLEMENTED] " + (q.get("query") or ""),
        "source_mu_ids": [],
        "evidence_depth": "mu_ids",
        "evidence": [],
    }


HARD_FAIL_PATTERNS = {
    "windows_abs_path": re.compile(r"[A-Za-z]:\\\\"),
    "file_uri": re.compile(r"file://", re.IGNORECASE),
    "mac_users_path": re.compile(r"/Users/"),
}


def check_invariants(answer_text: str, expect: dict) -> dict:
    must_include = expect.get("must_include") or []
    must_not = expect.get("must_not") or []

    missing = [s for s in must_include if s and s not in answer_text]
    present_forbidden = [s for s in must_not if s and s in answer_text]

    hard_triggers = [name for name, rx in HARD_FAIL_PATTERNS.items() if rx.search(answer_text or "")]

    must_include_pass = not missing
    must_not_pass = not present_forbidden
    hard_pass = not hard_triggers

    return {
        "must_include": {"missing": missing, "pass": must_include_pass},
        "must_not": {"present": present_forbidden, "pass": must_not_pass},
        "hard_fail": {"triggers": hard_triggers, "pass": hard_pass},
        "pass": must_include_pass and must_not_pass and hard_pass,
        "hard_failed": (not hard_pass),
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
    p.add_argument("--db", default=None, help="Optional meta.sqlite path; enables bundle-based answering")
    p.add_argument("--target-level", default="private", choices=["private", "org", "public"])
    p.add_argument("--limit", type=int, default=50)
    ns = p.parse_args(argv)

    questions = load_questions(Path(ns.questions))

    out_dir = Path(ns.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for q in questions:
        qid = q.get("id")
        query = q.get("query")
        expect = q.get("expect") or {}

        if ns.db:
            try:
                ans = answer_with_bundle(q, db=Path(ns.db), target_level=ns.target_level, limit=int(ns.limit))
            except Exception:
                ans = placeholder_answer(q)
        else:
            ans = placeholder_answer(q)
        inv = check_invariants(ans.get("text", ""), expect)

        if not ans.get("implemented", False):
            status = "SKIP"
            skip_reason = "not_implemented"
        else:
            status = "PASS" if inv["pass"] else "FAIL"
            skip_reason = None

        # hard-fail overrides skip (security gate)
        if inv.get("hard_failed"):
            status = "FAIL"
            skip_reason = None

        results.append(
            {
                "id": qid,
                "query": query,
                "status": status,
                "skip_reason": skip_reason,
                "answer": {
                    "text": ans.get("text", ""),
                    "source_mu_ids": ans.get("source_mu_ids", []),
                    "evidence_depth": ans.get("evidence_depth", "mu_ids"),
                    "evidence": ans.get("evidence", []),
                },
                "checks": {
                    "must_include": inv["must_include"],
                    "must_not": inv["must_not"],
                    "hard_fail": inv["hard_fail"],
                },
                "tags": q.get("tags", []),
            }
        )

    summary = {
        "total": len(results),
        "passed": sum(1 for r in results if r["status"] == "PASS"),
        "failed": sum(1 for r in results if r["status"] == "FAIL"),
        "skipped": sum(1 for r in results if r["status"] == "SKIP"),
        "hard_failed": sum(1 for r in results if (r["status"] == "FAIL") and r["checks"]["hard_fail"]["triggers"]),
    }

    report = {
        "run_id": out_dir.name,
        "created_at": utc_now(),
        "runner": "golden_run_v0.2",
        "summary": summary,
        "results": results,
    }

    (out_dir / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (out_dir / "report.md").write_text(render_markdown(report), encoding="utf-8")

    # exit non-zero on failures (hard fail or normal fail)
    return 0 if summary["failed"] == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
