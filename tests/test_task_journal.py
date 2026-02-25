from pathlib import Path


def test_task_journal_append_and_query(tmp_path: Path):
    from tools.task_journal import append_task, query_tasks

    db = tmp_path / "j.sqlite"

    spec = {"task_id": "t1", "type": "X", "idempotency_key": "k"}
    res = {"task_id": "t1", "status": "OK", "elapsed_ms": 1}

    append_task(db, spec, res, context={"vault_roots": {}})
    rows = query_tasks(db, type="X")
    assert rows
    assert rows[0]["task_id"] == "t1"


def test_task_journal_load_and_replay_smoke(tmp_path: Path):
    from tools.task_journal import append_task, load_task

    db = tmp_path / "j.sqlite"
    spec = {
        "task_id": "t1",
        "type": "VERIFY_MANIFEST",
        "idempotency_key": "k",
        "params": {"kind": "raw", "manifest_path": "x"},
    }
    res = {"task_id": "t1", "status": "OK", "elapsed_ms": 1}

    append_task(
        db,
        spec,
        res,
        context={"vault_roots": {"default": "X"}, "run_id": "RUN-1", "run_dir": "D"},
    )
    s2, r2, ctx = load_task(db, "t1")
    assert s2["type"] == "VERIFY_MANIFEST"
    assert r2["status"] == "OK"
    assert ctx == {"vault_roots": {"default": "X"}, "run_id": "RUN-1", "run_dir": "D"}
