"""ms emit-repair-tasks — thin CLI wrapper."""

from __future__ import annotations


def main(argv: list[str] | None = None) -> int:
    from tools.emit_repair_tasks import main as _main

    return _main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
