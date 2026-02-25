# ruff: noqa: E402

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.vault_uri import format_vault_uri, parse_vault_uri


def test_parse_and_format_roundtrip():
    u = "vault://default/raw/2026/02/21/foo.md"
    v = parse_vault_uri(u)
    assert v.vault_id == "default"
    assert v.kind == "raw"
    assert v.path.endswith("foo.md")
    assert str(v) == u


def test_invalid_kind_rejected():
    with pytest.raises(ValueError):
        parse_vault_uri("vault://default/nope/x")


def test_format_rejects_kind():
    with pytest.raises(ValueError):
        format_vault_uri(vault_id="default", kind="nope", path="x")
