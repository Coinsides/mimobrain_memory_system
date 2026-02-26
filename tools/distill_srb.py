"""Distiller v0: generate SRB (Session Resume Bundle) from a bundle JSON.

MVP (G-7): deterministic, short, structured SRB text to paste into a new session.

Usage:
  python -m tools.distill_srb --bundle <bundle.json> --out <dir>

Outputs:
- <out>/srb.md
- <out>/srb.json

Notes:
- This is intentionally not an LLM; it is a rule-based summarizer using bundle diagnostics.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SrbOut:
    out_dir: str
    srb_md: str
    srb_json: str


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def distill(bundle: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    scope = bundle.get("scope") if isinstance(bundle.get("scope"), dict) else {}
    ws = scope.get("workspace")
    since = scope.get("since")
    days = scope.get("time_window_days")

    diagnostics = bundle.get("diagnostics") if isinstance(bundle.get("diagnostics"), dict) else {}
    mem = diagnostics.get("membership") if isinstance(diagnostics.get("membership"), dict) else {}

    query_on = bundle.get("query_on") if isinstance(bundle.get("query_on"), dict) else {}
    query = query_on.get("query")

    source_mu_ids = bundle.get("source_mu_ids")
    if not isinstance(source_mu_ids, list):
        source_mu_ids = []

    # Best-effort: surface last few repair tasks
    repair_tasks = diagnostics.get("repair_tasks") if isinstance(diagnostics.get("repair_tasks"), list) else []

    srb_obj = {
        "workspace": ws,
        "since": since,
        "days": days,
        "query": query,
        "mu_count": len(source_mu_ids),
        "membership": {
            "effective_count": mem.get("effective_count"),
            "canonicalized_count": mem.get("canonicalized_count"),
            "canonicalization": mem.get("canonicalization"),
        },
        "repair_tasks": repair_tasks[:5],
    }

    md_lines = []
    md_lines.append("# SRB v0 (Session Resume Bundle)")
    md_lines.append("")
    md_lines.append("## Scope")
    md_lines.append(f"- workspace: {ws}")
    md_lines.append(f"- since: {since}")
    md_lines.append(f"- days: {days}")
    md_lines.append(f"- query: {query}")
    md_lines.append("")
    md_lines.append("## Current state")
    md_lines.append(f"- mu_count (bundle.source_mu_ids): {len(source_mu_ids)}")
    md_lines.append(
        f"- membership effective_count: {mem.get('effective_count')} canonicalized_count: {mem.get('canonicalized_count')}"
    )
    md_lines.append("")
    if repair_tasks:
        md_lines.append("## Repair tasks (top 5)")
        for t in repair_tasks[:5]:
            if not isinstance(t, dict):
                continue
            md_lines.append(f"- {t.get('type')} mu_id={t.get('mu_id')} reason={t.get('reason')}")
        md_lines.append("")

    md_lines.append("## Next actions")
    md_lines.append("- Continue the execution list gaps in order (G-1..G-7).")
    md_lines.append("- If evidence is degraded, fix pointer resolution config and rerun build_bundle.")
    md_lines.append("")

    return "\n".join(md_lines).rstrip() + "\n", srb_obj


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--bundle", required=True)
    p.add_argument("--out", required=True)
    ns = p.parse_args(argv)

    bundle_path = Path(ns.bundle)
    out_dir = Path(ns.out)

    bundle = _read_json(bundle_path)
    md, obj = distill(bundle)

    md_path = out_dir / "srb.md"
    json_path = out_dir / "srb.json"

    _write(md_path, md)
    _write(json_path, json.dumps(obj, ensure_ascii=False, indent=2) + "\n")

    print(json.dumps(asdict(SrbOut(out_dir=str(out_dir), srb_md=str(md_path), srb_json=str(json_path))), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
