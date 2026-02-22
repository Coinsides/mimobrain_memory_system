"""Manifest IO utilities (jsonl, append-only).

We keep raw/mu/asset manifests as jsonl. Each line must validate against a
schema.

This module is intentionally small and deterministic.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable


def append_jsonl(path: str | Path, record: dict) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def iter_jsonl(path: str | Path) -> Iterable[dict]:
    p = Path(path)
    if not p.exists():
        return
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)
