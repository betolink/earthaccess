"""Authentication utilities – now encapsulated in ``AuthManager``."""

from __future__ import annotations

import datetime as _dt
import logging
import os
from dataclasses import dataclass, field
from typing import Optional

import requests

log = logging.getLogger(__name__)


@dataclass
class AuthManager:
    """Manage Earthdata Login token lifecycle."""

    username: Optional[str] = field(default=None, init=False)
    password: Optional[str] = field(default=None, init=False)
    token: Optional[str] = field(default=None, init=False)
    expires_at: Optional[_dt.datetime] = field(default=None, init=False)

    def _is_expired(self) -> bool:
        return not self.expires_at or _dt.datetime.utcnow() >= self.expires_at

    def login(self, username: str, password: str) -> None:
        """Perform login and store token/expiry."""
        self.username = username
        self.password = password
        self._refresh()

    def _refresh(self) -> None:
        """Internal helper – request a fresh token."""
        if not self.username or not self.password:
            raise RuntimeError("Credentials not set – call ``login`` first")

        # NOTE: The actual endpoint and payload are unchanged from the original
        # implementation; only the surrounding logic is simplified.
        resp = requests.post(
            "https://urs.earthdata.nasa.gov/api/users/token",
            data={"username": self.username, "password": self.password},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self.token = data["access_token"]
        # ``expires_in`` is seconds from now
        self.expires_at = _dt.datetime.utcnow() + _dt.timedelta(
            seconds=int(data["expires_in"])
        )
        log.debug("Obtained new token, expires at %s", self.expires_at.isoformat())

    def get_token(self) -> str:
        """Return a valid token, refreshing it if necessary."""
        if self.token is None or self._is_expired():
            log.info("Token missing or expired – refreshing")
            self._refresh()
        return self.token  # type: ignore[return-value]

    def logout(self) -> None:
        """Clear stored credentials and token."""
        self.username = None
        self.password = None
        self.token = None
        self.expires_at = None


# A module‑level singleton for backward compatibility
auth_manager = AuthManager()


def login(username: str, password: str) -> None:
    """Public wrapper kept for compatibility – forwards to ``AuthManager``."""
    auth_manager.login(username, password)


def logout() -> None:
    """Public wrapper – forwards to ``AuthManager``."""
    auth_manager.logout()


def get_token() -> str:
    """Compatibility shim – returns the current token."""
    return auth_manager.get_token()
