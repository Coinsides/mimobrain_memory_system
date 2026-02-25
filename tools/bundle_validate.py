"""Validate bundle-like artifacts against bundle_v0_1 schema."""

from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator


def load_schema() -> dict:
    schema_path = (
        Path(__file__).resolve().parents[1]
        / "docs"
        / "contracts"
        / "bundle_v0_1.schema.json"
    )
    return json.loads(schema_path.read_text(encoding="utf-8"))


def validate_bundle(bundle: dict) -> list[str]:
    schema = load_schema()
    v = Draft202012Validator(schema)
    errors = sorted(v.iter_errors(bundle), key=lambda e: e.path)
    return [f"{list(e.path)}: {e.message}" for e in errors]


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", required=True)
    ns = p.parse_args(argv)

    obj = json.loads(Path(ns.inp).read_text(encoding="utf-8"))
    errs = validate_bundle(obj)
    if errs:
        for e in errs:
            print(e)
        return 2
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
