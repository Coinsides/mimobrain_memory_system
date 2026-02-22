"""ms export (P0-8) — thin CLI wrapper.

We keep the implementation simple and deterministic.

Supported inputs:
- MU: .mimo file or directory containing *.mimo
- Bundle: .json file (bundle-like artifact)

Usage:
  python tools/ms_export.py --in <path> --out <path> --target-level public

Notes:
- For MU exports: output is JSONL
- For bundle exports: output is JSON
"""

from __future__ import annotations

from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", required=True, help="Input file/dir")
    p.add_argument("--out", required=True, help="Output path")
    p.add_argument("--target-level", required=True, choices=["private", "org", "public"])
    p.add_argument(
        "--kind",
        choices=["auto", "mu", "bundle"],
        default="auto",
        help="Force export kind (default: auto)",
    )
    p.add_argument(
        "--journal-db",
        default=None,
        help="Optional task journal sqlite path. When set, write a single summary record.",
    )
    ns = p.parse_args(argv)

    in_path = Path(ns.inp)

    kind = ns.kind
    if kind == "auto":
        if in_path.is_dir() or in_path.suffix.lower() == ".mimo":
            kind = "mu"
        elif in_path.suffix.lower() == ".json":
            kind = "bundle"
        else:
            raise SystemExit(f"cannot infer kind from input: {in_path}")

    # Optional journaling: write one summary record per invocation.
    def _journal(status: str, meta: dict):
        if not ns.journal_db:
            return
        try:
            from tools.task_journal import append_task
        except Exception:
            return

        import hashlib
        from datetime import datetime, timezone

        base = f"{kind}:{in_path}:{ns.out}:{ns.target_level}".encode("utf-8")
        task_id = "export_" + hashlib.sha256(base).hexdigest()[:16] + "_" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        spec = {
            "task_id": task_id,
            "type": "MS_EXPORT",
            "idempotency_key": "export:" + hashlib.sha256(base).hexdigest(),
            "params": {
                "kind": kind,
                "in": str(in_path),
                "out": str(ns.out),
                "target_level": ns.target_level,
            },
        }
        result = {
            "task_id": task_id,
            "status": status,
            "elapsed_ms": None,
            "outputs": [{"kind": "FILE", "uri": str(ns.out), "meta": meta}],
        }
        append_task(Path(ns.journal_db), spec, result, context={"cwd": str(Path.cwd())})

    if kind == "mu":
        from tools.export_mu import main as export_mu_main

        rc = export_mu_main(["--in", str(in_path), "--out", ns.out, "--target-level", ns.target_level])
        _journal("OK" if rc == 0 else "ERROR", {"export_kind": "mu"})
        return rc

    if kind == "bundle":
        from tools.export_bundle import main as export_bundle_main

        rc = export_bundle_main(["--in", str(in_path), "--out", ns.out, "--target-level", ns.target_level])
        _journal("OK" if rc == 0 else "ERROR", {"export_kind": "bundle"})
        return rc

    raise SystemExit(f"unknown kind: {kind}")


if __name__ == "__main__":
    raise SystemExit(main())
