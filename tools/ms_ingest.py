"""ms ingest — thin CLI wrapper for vault ingest (P1-G).

Usage:
  python tools/ms_ingest.py --in <file|dir> --vault-root <vault_root>

This keeps `tools/vault_ingest.py` reusable as a library.
"""

from __future__ import annotations


def main(argv: list[str] | None = None) -> int:
    from tools.vault_ingest import main as ingest_main

    return ingest_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
