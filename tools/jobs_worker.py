"""Phase 2 Jobs worker (file-queue) — MVP implementation.

This worker consumes job folders under:
  <DATA_ROOT>/jobs/<job_id>/job.json

and runs the Phase 0 pipeline steps in order:
  ingest_raw -> pack_mu -> validate_mu -> assign_membership -> ingest_mu -> index

Design goals:
- LAN-first, Windows-friendly
- auditable: job folder holds status + logs
- retryable: failed jobs remain with last_error + step
- deterministic: no implicit global scope; workspace is mandatory

Notes:
- MU must remain pure (no workspace_id in MU). Workspace is stored via membership.jsonl.
- We intentionally keep storage file-first for jobs.

Usage:
  python -m tools.jobs_worker --data-root "C:/memobrain/data/memory_system" --once
  python -m tools.jobs_worker --data-root "C:/memobrain/data/memory_system" --loop --poll-seconds 2
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tools.assign_membership import append_membership_events
from tools.vault_ingest import ingest_file


REPO_ROOT = Path(__file__).resolve().parents[1]


def now_iso_z() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def read_json(path: Path) -> dict[str, Any]:
    # Be tolerant of BOM (Windows editors).
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def append_log(log_path: Path, line: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.open("a", encoding="utf-8", newline="\n").write(line.rstrip() + "\n")


@dataclass(frozen=True)
class JobPaths:
    job_dir: Path
    job_json: Path
    status_json: Path
    log_txt: Path
    lock_file: Path


def job_paths(job_dir: Path) -> JobPaths:
    return JobPaths(
        job_dir=job_dir,
        job_json=job_dir / "job.json",
        status_json=job_dir / "status.json",
        log_txt=job_dir / "log.txt",
        lock_file=job_dir / ".lock",
    )


def try_lock(lock_path: Path) -> bool:
    """Best-effort lock using O_EXCL."""
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, f"locked_at={now_iso_z()} pid={os.getpid()}\n".encode("utf-8"))
        os.close(fd)
        return True
    except FileExistsError:
        return False


def unlock(lock_path: Path) -> None:
    try:
        lock_path.unlink(missing_ok=True)  # py3.11+
    except TypeError:
        # fallback
        if lock_path.exists():
            lock_path.unlink()


def run_cmd(
    *args: str, cwd: Path, env: dict[str, str] | None, log_path: Path
) -> subprocess.CompletedProcess[str]:
    append_log(log_path, "$ " + " ".join(args))
    p = subprocess.run(
        list(args),
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if p.stdout:
        append_log(log_path, p.stdout.rstrip())
    if p.stderr:
        append_log(log_path, p.stderr.rstrip())
    if p.returncode != 0:
        raise RuntimeError(f"command failed rc={p.returncode}: {' '.join(args)}")
    return p


def consume_one_job(*, data_root: Path, job_dir: Path) -> bool:
    jp = job_paths(job_dir)
    if not jp.job_json.exists():
        return False

    def _move_inbox(job: dict, *, dest_state: str) -> None:
        """Move inbox folder from _queue/<job_id> to _done/_failed, best-effort."""
        try:
            inbox_path = job.get("inbox_path")
            if not isinstance(inbox_path, str) or not inbox_path:
                return
            inbox_dir = Path(inbox_path)
            if not inbox_dir.exists():
                return
            # Expect .../inbox/<ws>/_queue/<job_id>
            if inbox_dir.parent.name != "_queue":
                return
            ws_dir = inbox_dir.parent.parent
            src = inbox_dir
            dst = ws_dir / ("_" + dest_state) / src.name
            dst.parent.mkdir(parents=True, exist_ok=True)
            if dst.exists():
                return
            src.rename(dst)
        except Exception:
            return

    if not try_lock(jp.lock_file):
        return False

    try:
        job = read_json(jp.job_json)
        job_id = str(job.get("job_id") or job_dir.name)
        workspace_id = job.get("workspace_id")
        inbox_path = job.get("inbox_path")
        split = str(job.get("split") or "line_window:200")
        source_kind = str(job.get("source_kind") or "file")
        vault_id = str(job.get("vault_id") or "default")

        if not isinstance(workspace_id, str) or not workspace_id:
            raise RuntimeError("job missing workspace_id")
        if not isinstance(inbox_path, str) or not inbox_path:
            raise RuntimeError("job missing inbox_path")

        inbox = Path(inbox_path)
        if not inbox.exists():
            raise RuntimeError(f"inbox_path does not exist: {inbox}")

        status = {
            "job_id": job_id,
            "workspace_id": workspace_id,
            "status": "running",
            "step": None,
            "started_at": now_iso_z(),
            "updated_at": now_iso_z(),
            "last_error": None,
            "metrics": {
                "ingested_files": 0,
                "written_mus": 0,
                "validated": None,
                "membership_added": 0,
                "membership_skipped": 0,
                "ingested_mu_files": 0,
                "indexed": None,
            },
        }
        write_json(jp.status_json, status)

        def set_step(step: str):
            status["step"] = step
            status["updated_at"] = now_iso_z()
            write_json(jp.status_json, status)

        # Workspace-specific staging dirs under job_dir (auditable)
        raw_inputs_dir = job_dir / "raw_inputs"
        mu_out_dir = job_dir / "mu_out"
        raw_inputs_dir.mkdir(parents=True, exist_ok=True)
        mu_out_dir.mkdir(parents=True, exist_ok=True)

        # 1) INGEST_RAW (and collect the ingested raw dest paths)
        set_step("ingest_raw")
        append_log(jp.log_txt, f"[{now_iso_z()}] ingest_raw from {inbox}")
        ingested = []
        for p in sorted(inbox.rglob("*")):
            if p.is_file():
                r = ingest_file(
                    p, vault_root=data_root / "vaults" / "default", vault_id=vault_id
                )
                ingested.append(r)
                # Create a hardlink (or copy fallback) into job raw_inputs for pack.
                link_path = raw_inputs_dir / p.name
                if not link_path.exists():
                    try:
                        os.link(str(r.dest_path), str(link_path))
                        status["raw_inputs_provenance"] = "hardlink:vault/raw"
                    except Exception:
                        # fallback to copy
                        link_path.write_bytes(Path(r.dest_path).read_bytes())
                        status["raw_inputs_provenance"] = "copy:vault/raw"

        status["metrics"]["ingested_files"] = len(ingested)
        status["raw_ingest"] = {
            "vault_id": vault_id,
            "files": [
                {
                    "src": str(getattr(r, "src_path", ""))
                    if getattr(r, "src_path", None)
                    else None,
                    "dest": str(getattr(r, "dest_path", ""))
                    if getattr(r, "dest_path", None)
                    else None,
                    "sha256": str(getattr(r, "sha256", ""))
                    if getattr(r, "sha256", None)
                    else None,
                }
                for r in ingested
            ],
        }
        write_json(jp.status_json, status)
        append_log(jp.log_txt, f"ingested_files={len(ingested)}")

        env = dict(os.environ)
        # ensure `python -m tools.xxx` can resolve the local repo package
        env["PYTHONPATH"] = str(REPO_ROOT) + (
            os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
        )

        # 2) PACK_MU
        set_step("pack_mu")
        p_pack = run_cmd(
            sys.executable,
            "-m",
            "mimo_spec.tools.mimo_pack",
            "--in",
            str(raw_inputs_dir),
            "--out",
            str(mu_out_dir),
            "--source",
            source_kind,
            "--split",
            split,
            "--vault-id",
            vault_id,
            cwd=REPO_ROOT,
            env=env,
            log_path=jp.log_txt,
        )
        # parse: written_mus=N
        try:
            for ln in (p_pack.stdout or "").splitlines():
                if ln.strip().startswith("written_mus="):
                    status["metrics"]["written_mus"] = int(ln.split("=", 1)[1].strip())
                    break
        except Exception:
            pass
        write_json(jp.status_json, status)

        # 3) VALIDATE_MU
        set_step("validate_mu")
        p_val = run_cmd(
            sys.executable,
            "-m",
            "mimo_spec.tools.mimo_validate",
            "--in",
            str(mu_out_dir),
            cwd=REPO_ROOT,
            env=env,
            log_path=jp.log_txt,
        )
        # parse: checked=N failed=0 warnings=0
        status["metrics"]["validated"] = {"stdout": (p_val.stdout or "").strip()}
        write_json(jp.status_json, status)

        # 4) ASSIGN_MEMBERSHIP
        set_step("assign_membership")
        mu_ids = [p.stem for p in sorted(mu_out_dir.rglob("*.mimo")) if p.is_file()]
        res_mem = append_membership_events(
            data_root=data_root,
            workspace=workspace_id,
            mu_ids=mu_ids,
            source=f"job:{job_id}",
        )
        status["metrics"]["membership_added"] = int(res_mem.appended_events)
        status["metrics"]["membership_skipped"] = max(
            0, len(mu_ids) - int(res_mem.appended_events)
        )
        write_json(jp.status_json, status)
        append_log(
            jp.log_txt,
            f"membership_added={int(res_mem.appended_events)} skipped={max(0, len(mu_ids) - int(res_mem.appended_events))} workspace={workspace_id}",
        )

        # 5) INGEST_MU
        set_step("ingest_mu")
        p_ing_mu = run_cmd(
            sys.executable,
            "-m",
            "tools.vault_ingest_mu",
            "--in",
            str(mu_out_dir),
            "--vault-root",
            str(data_root / "vaults" / "default"),
            "--vault-id",
            vault_id,
            cwd=REPO_ROOT,
            env=env,
            log_path=jp.log_txt,
        )
        # parse: ingested_mu_files=N
        try:
            for ln in (p_ing_mu.stdout or "").splitlines():
                if ln.strip().startswith("ingested_mu_files="):
                    status["metrics"]["ingested_mu_files"] = int(
                        ln.split("=", 1)[1].strip()
                    )
                    break
        except Exception:
            pass
        write_json(jp.status_json, status)

        # 6) INDEX
        set_step("index")
        p_idx = run_cmd(
            sys.executable,
            "-m",
            "tools.index_mu",
            "--mu-root",
            str(data_root / "vaults" / "default" / "mu"),
            "--db",
            str(data_root / "index" / "meta.sqlite"),
            "--reset",
            cwd=REPO_ROOT,
            env=env,
            log_path=jp.log_txt,
        )
        # parse: {"indexed": N}
        try:
            obj = json.loads((p_idx.stdout or "").strip() or "{}")
            if isinstance(obj, dict) and isinstance(obj.get("indexed"), int):
                status["metrics"]["indexed"] = int(obj["indexed"])
        except Exception:
            pass
        write_json(jp.status_json, status)

        # done
        status["status"] = "done"
        status["step"] = None
        status["updated_at"] = now_iso_z()
        status["finished_at"] = now_iso_z()
        write_json(jp.status_json, status)
        append_log(jp.log_txt, f"[{now_iso_z()}] DONE")
        _move_inbox(job, dest_state="done")
        return True

    except Exception as e:
        # record failure
        try:
            status = read_json(jp.status_json) if jp.status_json.exists() else {}
        except Exception:
            status = {}
        status.update(
            {
                "status": "failed",
                "updated_at": now_iso_z(),
                "last_error": str(e),
            }
        )
        write_json(jp.status_json, status)
        append_log(jp.log_txt, f"[{now_iso_z()}] FAILED: {e}")
        _move_inbox(job, dest_state="failed")
        return True

    finally:
        unlock(jp.lock_file)


def find_job_dirs(data_root: Path) -> list[Path]:
    jobs_root = data_root / "jobs"
    if not jobs_root.exists():
        return []
    out = []
    for d in sorted(jobs_root.iterdir()):
        if d.is_dir() and (d / "job.json").exists():
            out.append(d)
    return out


def main(argv: list[str] | None = None) -> int:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--data-root", required=True)
    p.add_argument("--once", action="store_true")
    p.add_argument("--loop", action="store_true")
    p.add_argument("--poll-seconds", type=float, default=2.0)
    ns = p.parse_args(argv)

    data_root = Path(ns.data_root)

    if not ns.once and not ns.loop:
        raise SystemExit("pass --once or --loop")

    def tick() -> bool:
        did = False
        for d in find_job_dirs(data_root):
            # Consume only queued/unknown jobs. If status says done/failed, skip.
            st = d / "status.json"
            if st.exists():
                try:
                    s = read_json(st)
                    if s.get("status") in ("done", "failed"):
                        continue
                except Exception:
                    pass
            if consume_one_job(data_root=data_root, job_dir=d):
                did = True
        return did

    if ns.once:
        tick()
        return 0

    while True:
        did = tick()
        if not did:
            time.sleep(float(ns.poll_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
