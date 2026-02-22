from pathlib import Path


def test_golden_validate_ok():
    import json

    from tools.golden_validate import load_questions, validate_questions

    schema = json.loads(Path("golden/schema.json").read_text(encoding="utf-8"))
    questions = load_questions(Path("golden/questions.yaml"))
    errs = validate_questions(questions, schema)
    assert errs == []
