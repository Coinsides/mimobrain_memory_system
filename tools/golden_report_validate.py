"""Validate golden run report.json against golden/report.schema.json."""

from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator


def validate_report(report: dict) -> list[str]:
    schema = json.loads(
        (Path("golden") / "report.schema.json").read_text(encoding="utf-8")
    )
    v = Draft202012Validator(schema)
    errors = sorted(v.iter_errors(report), key=lambda e: (list(e.path), e.message))
    return [f"{list(e.path)}: {e.message}" for e in errors]


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--report", required=True)
    ns = p.parse_args(argv)

    report = json.loads(Path(ns.report).read_text(encoding="utf-8"))
    errs = validate_report(report)
    if errs:
        for e in errs:
            print(e)
        return 2
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
