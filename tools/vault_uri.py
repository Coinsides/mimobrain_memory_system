"""Vault URI utilities (v0.1).

Vault URIs are stable logical identifiers independent of local filesystem paths.

Format:
  vault://<vault_id>/<kind>/<yyyy>/<mm>/<...>

Example:
  vault://default/raw/2026/02/21/foo.md

This module intentionally avoids mapping URIs to absolute paths; that belongs to
higher-level configuration (multi-machine replicas, etc.).
"""

from __future__ import annotations

from dataclasses import dataclass


ALLOWED_KINDS = {"raw", "mu", "assets", "manifests", "logs", "derived"}


@dataclass(frozen=True)
class VaultUri:
    vault_id: str
    kind: str
    path: str  # path inside kind (no leading slash)

    def __str__(self) -> str:
        return f"vault://{self.vault_id}/{self.kind}/{self.path}".rstrip("/")


def parse_vault_uri(uri: str) -> VaultUri:
    if not isinstance(uri, str) or not uri.startswith("vault://"):
        raise ValueError(f"not a vault uri: {uri!r}")

    rest = uri[len("vault://") :]
    parts = [p for p in rest.split("/") if p]
    if len(parts) < 3:
        raise ValueError(f"invalid vault uri (need vault_id/kind/path): {uri!r}")

    vault_id, kind = parts[0], parts[1]
    if kind not in ALLOWED_KINDS:
        raise ValueError(f"invalid kind={kind!r} in {uri!r}")

    path = "/".join(parts[2:])
    return VaultUri(vault_id=vault_id, kind=kind, path=path)


def format_vault_uri(*, vault_id: str, kind: str, path: str) -> str:
    if kind not in ALLOWED_KINDS:
        raise ValueError(f"invalid kind={kind!r}")
    path = path.lstrip("/")
    return str(VaultUri(vault_id=vault_id, kind=kind, path=path))
