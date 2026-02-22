"""Bundle export tool (P0-8) v0.1.

This repo will eventually build view caches and bundles. Even before that,
we can define a conservative export/redaction pass for bundle-like artifacts.

Bundle (minimal) expected structure:
{
  "bundle_id": "...",
  "template": "...",
  "scope": {...},
  "source_mu_ids": ["mu_..."],
  "always_on": "..." | {...},
  "session_on": "..." | {...},
  "query_on": "..." | {...},
  "evidence": [
     {"mu_id": "mu_...", "pointer": [...], "snapshot": {...}}
  ]
}

Privacy rules (aligned with tools/privacy_policy.py):
- target=org/public: default deny pointer and snapshot payload.
- Always remove local absolute paths from any URIs.
- evidence is reduced to a list of mu_ids for org/public unless explicitly allowed.

Usage:
  python tools/export_bundle.py --in bundle.json --out exported_bundle.json --target-level public
"""

from __future__ import annotations

import json
from pathlib import Path

from tools.export_mu import LOCAL_PATH_RE  # reuse conservative path detector
from tools.privacy_policy import export_share_policy, ensure_privacy_defaults


def _deepcopy(o):
    return json.loads(json.dumps(o))


def _redact_evidence_item(item: dict, *, target_level: str) -> dict:
    # evidence items are shaped like MU fragments; apply privacy defaults
    mu_like = ensure_privacy_defaults(item)
    share_eff = export_share_policy(mu_like, target_level=target_level)

    allow_pointer = bool(share_eff.get("allow_pointer"))
    allow_snapshot = bool(share_eff.get("allow_snapshot"))

    out = _deepcopy(item)

    if target_level in {"org", "public"}:
        # Default behavior for org/public is to keep only mu_id (and maybe minimal metadata).
        if not (allow_pointer or allow_snapshot):
            return {"mu_id": out.get("mu_id")}

    # Pointer
    if not allow_pointer:
        out["pointer"] = []
    else:
        if isinstance(out.get("pointer"), list):
            cleaned = []
            for p in out["pointer"]:
                if not isinstance(p, dict):
                    continue
                uri = p.get("uri")
                if target_level in {"org", "public"} and isinstance(uri, str) and LOCAL_PATH_RE.search(uri):
                    continue
                cleaned.append(p)
            out["pointer"] = cleaned

    # Snapshot
    snap = out.get("snapshot")
    if isinstance(snap, dict):
        src = snap.get("source_ref")
        if target_level in {"org", "public"} and isinstance(src, dict):
            u = src.get("uri")
            if isinstance(u, str) and LOCAL_PATH_RE.search(u):
                src["uri"] = "<REDACTED_URI>"
        if not allow_snapshot:
            if "payload" in snap:
                snap["payload"] = {}
            snap["export_note"] = "snapshot payload removed by share_policy"

    out["export"] = {"target_level": target_level, "allow_pointer": allow_pointer, "allow_snapshot": allow_snapshot}
    return out


def export_bundle(bundle: dict, *, target_level: str) -> dict:
    b = _deepcopy(bundle)

    # Best-effort validation (do not fail hard; export should be usable as a sanitizer)
    try:
        from tools.bundle_validate import validate_bundle

        errs = validate_bundle(b)
        if errs:
            b.setdefault("diagnostics", {})
            b["diagnostics"]["bundle_schema_errors"] = errs[:50]
    except Exception:
        pass

    # Evidence
    ev = b.get("evidence")
    if isinstance(ev, list):
        b["evidence"] = [_redact_evidence_item(x, target_level=target_level) for x in ev if isinstance(x, dict)]

    # Strip any accidental local paths in top-level fields
    for key in ["source_path", "path", "uri"]:
        v = b.get(key)
        if isinstance(v, str) and target_level in {"org", "public"} and LOCAL_PATH_RE.search(v):
            b[key] = "<REDACTED_PATH>"

    b["export"] = {"target_level": target_level, "applied": True}
    return b


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--target-level", required=True, choices=["private", "org", "public"])
    ns = p.parse_args(argv)

    in_path = Path(ns.inp)
    out_path = Path(ns.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    bundle = json.loads(in_path.read_text(encoding="utf-8"))
    out = export_bundle(bundle, target_level=ns.target_level)
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
