"""Public API – thin wrappers that delegate to the new core modules."""

from __future__ import annotations

from .auth import login, logout, get_token, auth_manager
from .search import (
    search_collections,
    search_granules,
    # add other search helpers as they are created
)
from .store import Store  # unified Store class (see step 7)
from .formatters import format_result

__all__ = [
    "login",
    "logout",
    "get_token",
    "search_collections",
    "search_granules",
    "Store",
    "format_result",
]
