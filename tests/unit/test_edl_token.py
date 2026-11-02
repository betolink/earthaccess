"""Tests for the public EDL token accessor."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import earthaccess
import pytest


def _auth(monkeypatch: pytest.MonkeyPatch, token: Any) -> None:
    # get_edl_token() checks the concrete class, so use a real Auth instance.
    auth = earthaccess.Auth()
    auth.token = token
    monkeypatch.setattr(earthaccess, "__auth__", auth, raising=False)


def test_get_edl_token_reads_the_access_token_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auth.token is the EDL token response mapping (access_token + metadata)."""
    _auth(
        monkeypatch,
        {
            "access_token": "the-token",
            "token_type": "Bearer",
            "expiration_date": "2099-01-01",
        },
    )

    assert earthaccess.get_edl_token() == "the-token"


def test_get_edl_token_accepts_a_bare_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _auth(monkeypatch, "bare-token")

    assert earthaccess.get_edl_token() == "bare-token"


def test_get_edl_token_raises_when_no_token(monkeypatch: pytest.MonkeyPatch) -> None:
    _auth(monkeypatch, None)

    with pytest.raises(RuntimeError, match="not available"):
        earthaccess.get_edl_token()


def test_get_edl_token_raises_without_login(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        earthaccess, "__auth__", SimpleNamespace(token=None), raising=False
    )

    with pytest.raises(RuntimeError, match="not properly initialized"):
        earthaccess.get_edl_token()
