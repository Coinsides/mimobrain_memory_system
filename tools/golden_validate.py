"""Validate golden/questions.yaml against golden/schema.json."""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator


def load_questions(path: Path) -> list[dict]:
    obj = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(obj, list):
        raise TypeError("questions must be a list")
    return obj


def validate_questions(questions: list[dict], schema: dict) -> list[str]:
    v = Draft202012Validator(schema)
    errors = sorted(v.iter_errors(questions), key=lambda e: (list(e.path), e.message))
    out = [f"{list(e.path)}: {e.message}" for e in errors]

    # extra checks: unique ids
    ids = [q.get("id") for q in questions if isinstance(q, dict)]
    dup = sorted({x for x in ids if x and ids.count(x) > 1})
    if dup:
        out.append(f"duplicate ids: {dup}")
    return out


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--questions", default=str(Path("golden") / "questions.yaml"))
    p.add_argument("--schema", default=str(Path("golden") / "schema.json"))
    ns = p.parse_args(argv)

    questions_path = Path(ns.questions)
    schema_path = Path(ns.schema)

    questions = load_questions(questions_path)
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    errs = validate_questions(questions, schema)
    if errs:
        for e in errs:
            print(e)
        return 2
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
