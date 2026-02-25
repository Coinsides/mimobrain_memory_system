"""Manifest sync/merge analysis (P0-F / P0-7A) v0.1.

Goal of v0.1:
- Compare two append-only jsonl manifests (base vs incoming)
- Produce a machine-first JSON report that classifies conflicts
- Do NOT mutate manifests (analysis-only)

We intentionally keep this conservative. The actual auto-merge and repair-task
emission is P0-7B.

Conflict types (v0.1):
- SCHEMA_ERROR: json decode issues or missing required keys
- ID_COLLISION_DIFFERENT_SHA: same record id but different sha256
- SHA_COLLISION_DIFFERENT_URI: same sha256 but different uri
- URI_COLLISION_DIFFERENT_SHA: same uri but different sha256

For each manifest kind, we define the record id key:
- raw:   raw_id
- mu:    mu_id
- asset: asset_id

Records are compared by exact JSON equality for exact_dupes.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


KIND_ID_KEY = {"raw": "raw_id", "mu": "mu_id", "asset": "asset_id"}


@dataclass
class Conflict:
    type: str
    severity: str
    key: str
    message: str
    base_records: list[dict]
    incoming_records: list[dict]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_jsonl_lines(path: Path) -> tuple[list[dict], list[Conflict], int]:
    """Return (records, schema_conflicts, line_count)."""
    records: list[dict] = []
    conflicts: list[Conflict] = []

    if not path.exists():
        conflicts.append(
            Conflict(
                type="SCHEMA_ERROR",
                severity="ERROR",
                key=str(path),
                message=f"missing manifest file: {path}",
                base_records=[],
                incoming_records=[],
            )
        )
        return records, conflicts, 0

    lines = path.read_text(encoding="utf-8").splitlines()
    for i, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception as e:
            conflicts.append(
                Conflict(
                    type="SCHEMA_ERROR",
                    severity="ERROR",
                    key=f"{path}:{i}",
                    message=f"invalid json: {e}",
                    base_records=[],
                    incoming_records=[],
                )
            )
            continue
        if not isinstance(obj, dict):
            conflicts.append(
                Conflict(
                    type="SCHEMA_ERROR",
                    severity="ERROR",
                    key=f"{path}:{i}",
                    message="manifest line must be an object",
                    base_records=[],
                    incoming_records=[],
                )
            )
            continue
        records.append(obj)

    return records, conflicts, len(lines)


def record_fingerprint(rec: dict) -> str:
    return json.dumps(rec, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def index_records(records: Iterable[dict], *, id_key: str) -> dict[str, list[dict]]:
    idx: dict[str, list[dict]] = {}
    for r in records:
        rid = r.get(id_key)
        if isinstance(rid, str):
            idx.setdefault(rid, []).append(r)
    return idx


def index_by(records: Iterable[dict], key: str) -> dict[str, list[dict]]:
    idx: dict[str, list[dict]] = {}
    for r in records:
        v = r.get(key)
        if isinstance(v, str):
            idx.setdefault(v, []).append(r)
    return idx


def analyze_sync(
    *, kind: str, base_path: str | Path, incoming_path: str | Path
) -> dict[str, Any]:
    if kind not in KIND_ID_KEY:
        raise ValueError(f"unknown kind={kind!r} (expected raw|mu|asset)")

    id_key = KIND_ID_KEY[kind]
    base_p = Path(base_path)
    inc_p = Path(incoming_path)

    base_recs, base_schema_conf, base_lines = read_jsonl_lines(base_p)
    inc_recs, inc_schema_conf, inc_lines = read_jsonl_lines(inc_p)

    conflicts: list[Conflict] = []
    conflicts.extend(base_schema_conf)
    conflicts.extend(inc_schema_conf)

    # exact dupes (line-level)
    base_fp = {record_fingerprint(r) for r in base_recs}
    inc_fp = {record_fingerprint(r) for r in inc_recs}
    exact_dupes = len(base_fp.intersection(inc_fp))

    base_by_id = index_records(base_recs, id_key=id_key)
    inc_by_id = index_records(inc_recs, id_key=id_key)

    # ID collision: same id but different sha256
    for rid, inc_list in inc_by_id.items():
        if rid not in base_by_id:
            continue
        base_list = base_by_id[rid]
        # compare sha256 sets
        b_sha = {r.get("sha256") for r in base_list if isinstance(r.get("sha256"), str)}
        i_sha = {r.get("sha256") for r in inc_list if isinstance(r.get("sha256"), str)}
        if b_sha and i_sha and b_sha != i_sha:
            conflicts.append(
                Conflict(
                    type="ID_COLLISION_DIFFERENT_SHA",
                    severity="ERROR",
                    key=rid,
                    message=f"same {id_key} but sha256 differs: base={sorted(b_sha)} incoming={sorted(i_sha)}",
                    base_records=base_list,
                    incoming_records=inc_list,
                )
            )

    # sha256 collision: same sha256 but different uri
    base_by_sha = index_by(base_recs, "sha256")
    inc_by_sha = index_by(inc_recs, "sha256")
    for sha, inc_list in inc_by_sha.items():
        if sha not in base_by_sha:
            continue
        base_list = base_by_sha[sha]
        b_uri = {r.get("uri") for r in base_list if isinstance(r.get("uri"), str)}
        i_uri = {r.get("uri") for r in inc_list if isinstance(r.get("uri"), str)}
        if b_uri and i_uri and b_uri != i_uri:
            conflicts.append(
                Conflict(
                    type="SHA_COLLISION_DIFFERENT_URI",
                    severity="WARN",
                    key=sha,
                    message=f"same sha256 but uri differs: base={sorted(b_uri)} incoming={sorted(i_uri)}",
                    base_records=base_list,
                    incoming_records=inc_list,
                )
            )

    # uri collision: same uri but different sha256
    base_by_uri = index_by(base_recs, "uri")
    inc_by_uri = index_by(inc_recs, "uri")
    for uri, inc_list in inc_by_uri.items():
        if uri not in base_by_uri:
            continue
        base_list = base_by_uri[uri]
        b_sha = {r.get("sha256") for r in base_list if isinstance(r.get("sha256"), str)}
        i_sha = {r.get("sha256") for r in inc_list if isinstance(r.get("sha256"), str)}
        if b_sha and i_sha and b_sha != i_sha:
            conflicts.append(
                Conflict(
                    type="URI_COLLISION_DIFFERENT_SHA",
                    severity="ERROR",
                    key=uri,
                    message=f"same uri but sha256 differs: base={sorted(b_sha)} incoming={sorted(i_sha)}",
                    base_records=base_list,
                    incoming_records=inc_list,
                )
            )

    # new records count by (id_key) presence
    new_ids = [rid for rid in inc_by_id.keys() if rid not in base_by_id]

    report = {
        "report_version": "0.1",
        "created_at": now_iso(),
        "kind": kind,
        "base": {"path": str(base_p), "line_count": base_lines},
        "incoming": {"path": str(inc_p), "line_count": inc_lines},
        "stats": {
            "base_unique": len(base_by_id),
            "incoming_unique": len(inc_by_id),
            "exact_dupes": exact_dupes,
            "new_records": len(new_ids),
        },
        "conflicts": [asdict(c) for c in conflicts],
    }
    return report


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--kind", required=True, choices=["raw", "mu", "asset"])
    p.add_argument("--base", required=True, help="Base manifest path (jsonl)")
    p.add_argument("--incoming", required=True, help="Incoming manifest path (jsonl)")
    p.add_argument("--out", required=True, help="Output report path (json)")
    ns = p.parse_args(argv)

    report = analyze_sync(kind=ns.kind, base_path=ns.base, incoming_path=ns.incoming)
    out_p = Path(ns.out)
    out_p.parent.mkdir(parents=True, exist_ok=True)
    out_p.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
