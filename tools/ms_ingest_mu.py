"""ms ingest-mu — thin CLI wrapper for MU ingest."""

from __future__ import annotations


def main(argv: list[str] | None = None) -> int:
    from tools.vault_ingest_mu import main as _main

    return _main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
