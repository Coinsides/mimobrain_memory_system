"""ms migrate-pointers — thin CLI wrapper.

Usage:
  python tools/ms_migrate_pointers.py --mu <file|dir> --raw-manifest <raw_manifest.jsonl> --out-dir <dir>
"""

from __future__ import annotations


def main(argv: list[str] | None = None) -> int:
    from tools.pointer_migrate import main as migrate_main

    return migrate_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
