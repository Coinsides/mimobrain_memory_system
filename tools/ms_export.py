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

    if kind == "mu":
        from tools.export_mu import main as export_mu_main

        return export_mu_main(["--in", str(in_path), "--out", ns.out, "--target-level", ns.target_level])

    if kind == "bundle":
        from tools.export_bundle import main as export_bundle_main

        return export_bundle_main(["--in", str(in_path), "--out", ns.out, "--target-level", ns.target_level])

    raise SystemExit(f"unknown kind: {kind}")


if __name__ == "__main__":
    raise SystemExit(main())
