"""Simple data models shared across the package."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Mapping


@dataclass(frozen=True)
class PaginatedResponse:
    """Container for a paginated CMR response."""

    items: List[Mapping[str, Any]]
    next_token: str | None = None
    total_hits: int | None = None

    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> "PaginatedResponse":
        items = payload.get("items", [])
        return cls(
            items=items,
            next_token=payload.get("nextToken"),
            total_hits=payload.get("hits"),
        )
