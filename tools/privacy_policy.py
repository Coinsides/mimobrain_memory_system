"""Privacy policy utilities for the memory system (P0-8).

This module centralizes defaulting rules so that export/other pipelines
aren't forced to guess.

Design decisions (see execution_log.md):
- Default all MU to privacy.level=private.
- Default redact=none.
- For exports to org/public, default share_policy allow_pointer/allow_snapshot to False
  unless explicitly set True.

Note: This repo does not own MU pack/ingest; we provide deterministic utilities
that other tools (export, bundle builder, etc.) can reuse.
"""

from __future__ import annotations

import json
from typing import Any


DEFAULT_PRIVACY = {
    "level": "private",
    "redact": "none",
    "pii": [],
    "share_policy": {},
}


def _deepcopy(obj: Any) -> Any:
    return json.loads(json.dumps(obj))


def ensure_privacy_defaults(mu: dict) -> dict:
    """Return a copy of MU dict with privacy defaults filled in.

    This does not attempt to validate against mimo-spec schema; it only applies
    defaulting rules deterministically.
    """

    mu2 = _deepcopy(mu)

    privacy = mu2.get("privacy")
    if not isinstance(privacy, dict):
        privacy = {}
    # defaults
    for k, v in DEFAULT_PRIVACY.items():
        if k not in privacy:
            privacy[k] = _deepcopy(v)

    # normalize share_policy
    sp = privacy.get("share_policy")
    if not isinstance(sp, dict):
        sp = {}
    privacy["share_policy"] = sp

    # normalize pii
    pii = privacy.get("pii")
    if not isinstance(pii, list):
        privacy["pii"] = []

    # hard defaults for required fields
    if not isinstance(privacy.get("level"), str) or not privacy.get("level"):
        privacy["level"] = "private"
    if not isinstance(privacy.get("redact"), str) or not privacy.get("redact"):
        privacy["redact"] = "none"

    mu2["privacy"] = privacy
    return mu2


def export_share_policy(mu: dict, *, target_level: str) -> dict:
    """Compute effective share policy booleans for an export target.

    Returns: {allow_pointer: bool, allow_snapshot: bool}

    Defaults are deny for org/public unless explicitly enabled.
    """

    privacy = mu.get("privacy") if isinstance(mu.get("privacy"), dict) else {}
    sp = privacy.get("share_policy") if isinstance(privacy.get("share_policy"), dict) else {}

    # baseline defaults
    allow_pointer = False
    allow_snapshot = False

    if target_level == "private":
        # exporting privately (self) may choose to include by default only if explicitly allowed;
        # keep conservative baseline here too.
        allow_pointer = bool(sp.get("allow_pointer", False))
        allow_snapshot = bool(sp.get("allow_snapshot", False))
    else:
        # org/public: default deny unless explicitly True
        allow_pointer = bool(sp.get("allow_pointer", False))
        allow_snapshot = bool(sp.get("allow_snapshot", False))

    return {"allow_pointer": allow_pointer, "allow_snapshot": allow_snapshot}
