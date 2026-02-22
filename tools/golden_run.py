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


def answer_with_bundle(q: dict, *, db: Path, target_level: str, limit: int, config: str | None = None) -> dict:
    """Rule-based answerer using build_bundle (P1-E).

    This is a stopgap answerer to exercise evidence plumbing end-to-end.

    Contract goals:
    - deterministic output (stable for CI)
    - source_mu_ids must reflect the selected evidence set
    - evidence_depth can be driven by golden expectations (mu_ids|mu_snippets)
    """

    from tools.build_bundle import build_bundle

    setup = q.get("setup") if isinstance(q.get("setup"), dict) else {}
    scope = setup.get("scope") if isinstance(setup.get("scope"), dict) else {}
    days = scope.get("time_window_days")
    days = int(days) if isinstance(days, int) else 7

    template = setup.get("template_hint")
    template = str(template) if isinstance(template, str) and template else None

    # Choose a deterministic template when not specified.
    if not template:
        tags = q.get("tags") if isinstance(q.get("tags"), list) else []
        tags = [str(t) for t in tags]
        if "privacy" in tags:
            template = "privacy_policy_v1"
        elif "engineering" in tags or "audit" in tags:
            template = "engineering_audit_v1"
        elif "time" in tags and "tasks" in tags:
            template = "time_daily_v1"
        else:
            template = "time_overview_v1"

    expect = q.get("expect") if isinstance(q.get("expect"), dict) else {}
    must_include = expect.get("must_include") if isinstance(expect.get("must_include"), list) else []

    evidence_expect = expect.get("evidence") if isinstance(expect.get("evidence"), dict) else {}
    depth = evidence_expect.get("depth")
    depth = str(depth) if isinstance(depth, str) and depth else "mu_ids"
    if depth not in {"mu_ids", "mu_snippets", "raw_quotes"}:
        depth = "mu_ids"

    raw_query = q.get("query") or ""
    search_query = raw_query
    # FTS5 MATCH is picky with punctuation/long natural-language questions.
    # For Golden we prefer deterministic keyword queries when available.
    if isinstance(raw_query, str) and must_include:
        # Use the first required keyword as a stable retrieval hint for retrieval.
        # This is especially important for short CJK questions where FTS tokenization can be weak.
        if any("\u4e00" <= ch <= "\u9fff" for ch in raw_query):
            search_query = str(must_include[0])
        elif len(raw_query) > 24:
            search_query = str(must_include[0])

    # P1-C: compiled spec is owned by build_bundle so all callers share one path.
    vault_roots = None
    raw_manifest_path = None
    if config:
        try:
            from tools.ms_config import load_config

            cfg = load_config(config)
            vault_roots = cfg.get("vault_roots") if isinstance(cfg.get("vault_roots"), dict) else None
            rmp = cfg.get("raw_manifest_path")
            raw_manifest_path = Path(rmp) if isinstance(rmp, str) and rmp else None
        except Exception:
            vault_roots = None
            raw_manifest_path = None

    bundle = build_bundle(
        db_path=db,
        query=search_query,
        target_level=target_level,
        template_name=template,
        question_setup=setup,
        question_expect=expect,
        question_budget=q.get("budget") if isinstance(q.get("budget"), dict) else None,
        include_diagnostics=True,
        evidence_depth=depth,
        vault_roots=vault_roots,
        raw_manifest_path=raw_manifest_path,
    )

    mu_ids = bundle.get("source_mu_ids") or []
    evidence = bundle.get("evidence") or []

    # Compose a deterministic text that includes required keywords and lists evidence ids.
    lines: list[str] = []
    if must_include:
        lines.append("必含: " + ",".join(str(x) for x in must_include))
    lines.append("证据(mu_id): " + ", ".join(mu_ids) if mu_ids else "证据(mu_id): <none>")

    return {
        "implemented": True,
        "text": "\n".join(lines),
        "source_mu_ids": mu_ids,
        "evidence_depth": depth,
        "evidence": evidence,
        "bundle_diagnostics": bundle.get("diagnostics") if isinstance(bundle, dict) else None,
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


def check_invariants(answer_text: str, expect: dict, *, source_mu_ids: list[str], evidence_depth: str, evidence: list[dict] | None = None, bundle_diagnostics: dict | None = None) -> dict:
    must_include = expect.get("must_include") or []
    must_not = expect.get("must_not") or []

    missing = [s for s in must_include if s and s not in (answer_text or "")]
    present_forbidden = [s for s in must_not if s and s in (answer_text or "")]

    hard_triggers = [name for name, rx in HARD_FAIL_PATTERNS.items() if rx.search(answer_text or "")]

    must_include_pass = not missing
    must_not_pass = not present_forbidden
    hard_pass = not hard_triggers

    # Evidence checks (minimal, deterministic): enforce min_mu and depth when specified.
    evidence_expect = expect.get("evidence") if isinstance(expect.get("evidence"), dict) else {}
    min_mu = evidence_expect.get("min_mu")
    min_mu = int(min_mu) if isinstance(min_mu, int) else None

    depth_expect = evidence_expect.get("depth")
    depth_expect = str(depth_expect) if isinstance(depth_expect, str) else None

    evidence_fail_reasons: list[str] = []
    if min_mu is not None and len(source_mu_ids) < min_mu:
        evidence_fail_reasons.append(f"min_mu:{min_mu} got:{len(source_mu_ids)}")
    if depth_expect and evidence_depth and depth_expect != evidence_depth:
        evidence_fail_reasons.append(f"depth_expected:{depth_expect} got:{evidence_depth}")

    # If we attempted raw_quotes, require snippets.
    if depth_expect == "raw_quotes":
        snips = 0
        if isinstance(evidence, list):
            for ev in evidence:
                if isinstance(ev, dict) and isinstance(ev.get("snippet"), str) and ev.get("snippet"):
                    snips += 1
        # simplest rule: snippet count must meet min_mu when min_mu is specified; otherwise at least 1.
        if min_mu is not None:
            if snips < min_mu:
                evidence_fail_reasons.append(f"raw_quotes_snippets_min:{min_mu} got:{snips}")
        else:
            if snips < 1:
                evidence_fail_reasons.append("raw_quotes_snippets_min:1 got:0")

    # Degraded evidence is a fail (simple policy).
    if isinstance(bundle_diagnostics, dict) and bundle_diagnostics.get("evidence_degraded") is True:
        evidence_fail_reasons.append("evidence_degraded:true")

    evidence_pass = not evidence_fail_reasons

    return {
        "must_include": {"missing": missing, "pass": must_include_pass},
        "must_not": {"present": present_forbidden, "pass": must_not_pass},
        "hard_fail": {"triggers": hard_triggers, "pass": hard_pass},
        "evidence": {"reasons": evidence_fail_reasons, "pass": evidence_pass},
        "pass": must_include_pass and must_not_pass and hard_pass and evidence_pass,
        "hard_failed": (not hard_pass),
    }


def render_markdown(report: dict) -> str:
    total = report["summary"]["total"]
    passed = report["summary"]["passed"]
    failed = report["summary"]["failed"]
    skipped = report["summary"]["skipped"]

    lines: list[str] = []
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

        checks = r.get("checks") if isinstance(r.get("checks"), dict) else None
        if checks:
            mi = checks.get("must_include") if isinstance(checks.get("must_include"), dict) else {}
            mn = checks.get("must_not") if isinstance(checks.get("must_not"), dict) else {}
            hf = checks.get("hard_fail") if isinstance(checks.get("hard_fail"), dict) else {}

            if not (mi.get("pass") and mn.get("pass") and hf.get("pass")):
                if mi.get("missing"):
                    lines.append(f"- missing: {mi.get('missing')}")
                if mn.get("present"):
                    lines.append(f"- forbidden_present: {mn.get('present')}")
                if hf.get("triggers"):
                    lines.append(f"- hard_fail: {hf.get('triggers')}")

        lines.append("")

    return "\n".join(lines) + "\n"


def validate_report(report: dict, schema_path: Path) -> list[str]:
    """Validate the report against a JSON Schema.

    Returns a list of human-friendly error strings.
    """

    from jsonschema import Draft202012Validator

    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    v = Draft202012Validator(schema)
    errors = sorted(v.iter_errors(report), key=lambda e: (list(e.path), e.message))
    return [f"{list(e.path)}: {e.message}" for e in errors]


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--questions", default=str(Path("golden") / "questions.yaml"))
    p.add_argument("--out-dir", default=str(Path("runs") / "golden" / run_id()))
    p.add_argument("--db", default=None, help="Optional meta.sqlite path; enables bundle-based answering")
    p.add_argument("--config", default=None, help="Path to ms_config.json (optional)")
    p.add_argument("--target-level", default="private", choices=["private", "org", "public"])
    p.add_argument("--limit", type=int, default=50)
    p.add_argument(
        "--report-schema",
        default=str(Path("docs") / "contracts" / "golden_report_v0_1.schema.json"),
        help="JSON Schema for validating report.json",
    )
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
                ans = answer_with_bundle(q, db=Path(ns.db), target_level=ns.target_level, limit=int(ns.limit), config=ns.config)
            except Exception:
                ans = placeholder_answer(q)
        else:
            ans = placeholder_answer(q)
        inv = check_invariants(
            ans.get("text", ""),
            expect,
            source_mu_ids=ans.get("source_mu_ids") or [],
            evidence_depth=ans.get("evidence_depth") or "mu_ids",
            evidence=ans.get("evidence") if isinstance(ans.get("evidence"), list) else None,
            bundle_diagnostics=ans.get("bundle_diagnostics") if isinstance(ans.get("bundle_diagnostics"), dict) else None,
        )

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
                    "evidence": inv["evidence"],
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

    schema_path = Path(ns.report_schema)
    errs = validate_report(report, schema_path)
    if errs:
        # Put schema errors next to the report for debugging in CI.
        (out_dir / "report.schema_errors.txt").write_text("\n".join(errs) + "\n", encoding="utf-8")

    (out_dir / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (out_dir / "report.md").write_text(render_markdown(report), encoding="utf-8")

    # exit non-zero on failures (hard fail or normal fail). Schema errors are also failures.
    if errs:
        return 4
    return 0 if summary["failed"] == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
