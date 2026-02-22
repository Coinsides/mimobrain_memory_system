import json
from pathlib import Path


def test_logger_writes_jsonl(tmp_path: Path):
    from tools.logger import log_event

    p = tmp_path / "a.jsonl"
    log_event(event="X", log_path=p, tool="t", diagnostics={"k": 1})

    line = p.read_text(encoding="utf-8").splitlines()[0]
    obj = json.loads(line)
    assert obj["event"] == "X"
    assert obj["tool"] == "t"
