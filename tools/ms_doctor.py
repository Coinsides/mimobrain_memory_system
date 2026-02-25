"""ms doctor (P0-6/7) — thin CLI wrapper + optional task journal.

This tool wraps vault_doctor helpers and can optionally write a single summary
TaskSpec/TaskResult record into a task journal sqlite DB.

Usage:
  python tools/ms_doctor.py manifest --manifest <path> --schema <path> [--journal-db <db>]
  python tools/ms_doctor.py verify --manifest <path> --vault-root default=C:/vault [--journal-db <db>]
  python tools/ms_doctor.py repair --manifest <path> --sha256 <sha> [--journal-db <db>]
"""

from __future__ import annotations

from pathlib import Path


def _journal(db: str | None, *, spec: dict, result: dict):
    if not db:
        return
    try:
        from tools.task_journal import append_task

        append_task(Path(db), spec, result, context={"cwd": str(Path.cwd())})

        # structured logs (summary-only)
        try:
            from tools.logger import default_log_path, log_event

            log_event(
                event=spec.get("type") or "MS_DOCTOR",
                log_path=default_log_path("ms_doctor"),
                task_id=spec.get("task_id"),
                tool="ms_doctor",
                inputs=[{"kind": "PARAMS", **(spec.get("params") or {})}],
                outputs=result.get("outputs"),
                diagnostics={"status": result.get("status")},
            )
        except Exception:
            pass

    except Exception:
        return


def main(argv: list[str] | None = None) -> int:
    import argparse
    import hashlib
    from datetime import datetime, timezone

    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    p_man = sub.add_parser("manifest")
    p_man.add_argument("--manifest", required=True)
    p_man.add_argument("--schema", required=True)
    p_man.add_argument("--journal-db", default=None)

    p_ver = sub.add_parser("verify")
    p_ver.add_argument("--manifest", required=True)
    p_ver.add_argument("--vault-root", action="append", default=[])
    p_ver.add_argument("--journal-db", default=None)

    p_rep = sub.add_parser("repair")
    p_rep.add_argument("--manifest", required=True)
    p_rep.add_argument("--sha256", required=True)
    p_rep.add_argument("--journal-db", default=None)

    ns = p.parse_args(argv)

    cmd = ns.cmd
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    if cmd == "manifest":
        from tools.vault_doctor import doctor_manifest

        manifest = Path(ns.manifest)
        schema = Path(ns.schema)
        errs = doctor_manifest(manifest, schema)
        ok = not errs

        base = f"manifest:{manifest}:{schema}".encode("utf-8")
        task_id = "doctor_" + hashlib.sha256(base).hexdigest()[:16] + "_" + ts
        spec = {
            "task_id": task_id,
            "type": "MS_DOCTOR_MANIFEST",
            "idempotency_key": "doctor:" + hashlib.sha256(base).hexdigest(),
            "params": {"manifest": str(manifest), "schema": str(schema)},
        }
        result = {
            "task_id": task_id,
            "status": "OK" if ok else "ERROR",
            "elapsed_ms": None,
            "outputs": [
                {
                    "kind": "TEXT",
                    "uri": None,
                    "meta": {"errors": errs[:50], "error_count": len(errs)},
                }
            ],
        }
        _journal(ns.journal_db, spec=spec, result=result)

        if errs:
            for e in errs[:50]:
                print(e)
            return 2
        print("OK")
        return 0

    if cmd == "verify":
        from tools.vault_doctor import verify_manifest

        vault_roots = {}
        for item in ns.vault_root:
            if "=" not in item:
                raise SystemExit(
                    f"invalid --vault-root {item!r} (expected vault_id=path)"
                )
            k, v = item.split("=", 1)
            vault_roots[k] = v

        manifest = Path(ns.manifest)
        errs = verify_manifest(manifest, vault_roots=vault_roots)
        ok = not errs

        base = f"verify:{manifest}:{sorted(vault_roots.items())}".encode("utf-8")
        task_id = "doctor_" + hashlib.sha256(base).hexdigest()[:16] + "_" + ts
        spec = {
            "task_id": task_id,
            "type": "MS_VERIFY_MANIFEST",
            "idempotency_key": "verify:" + hashlib.sha256(base).hexdigest(),
            "params": {"manifest": str(manifest), "vault_roots": vault_roots},
        }
        result = {
            "task_id": task_id,
            "status": "OK" if ok else "ERROR",
            "elapsed_ms": None,
            "outputs": [
                {
                    "kind": "TEXT",
                    "uri": None,
                    "meta": {"errors": errs[:50], "error_count": len(errs)},
                }
            ],
        }
        _journal(ns.journal_db, spec=spec, result=result)

        if errs:
            for e in errs[:50]:
                print(e)
            return 2
        print("OK")
        return 0

    if cmd == "repair":
        from tools.vault_doctor import repair_suggest_by_sha256

        manifest = Path(ns.manifest)
        uri = repair_suggest_by_sha256(manifest, sha256=ns.sha256)
        ok = uri is not None

        base = f"repair:{manifest}:{ns.sha256}".encode("utf-8")
        task_id = "doctor_" + hashlib.sha256(base).hexdigest()[:16] + "_" + ts
        spec = {
            "task_id": task_id,
            "type": "MS_REPAIR_SUGGEST",
            "idempotency_key": "repair:" + hashlib.sha256(base).hexdigest(),
            "params": {"manifest": str(manifest), "sha256": ns.sha256},
        }
        result = {
            "task_id": task_id,
            "status": "OK" if ok else "ERROR",
            "elapsed_ms": None,
            "outputs": [{"kind": "TEXT", "uri": None, "meta": {"suggested_uri": uri}}],
        }
        _journal(ns.journal_db, spec=spec, result=result)

        if not ok:
            print("NO_SUGGESTION")
            return 2
        print(uri)
        return 0

    raise SystemExit("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())
