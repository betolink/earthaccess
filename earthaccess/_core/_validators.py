"""Validation helpers used by the public API."""

from __future__ import annotations

from typing import Any, Mapping, Sequence


def require_non_empty(mapping: Mapping[str, Any], keys: Sequence[str]) -> None:
    """Raise ``ValueError`` if any of *keys* are missing or empty in *mapping*."""
    missing = [k for k in keys if not mapping.get(k)]
    if missing:
        raise ValueError(f"Missing required parameters: {', '.join(missing)}")


def validate_date_range(start: str | None, end: str | None) -> None:
    """Very light validation – ensure both or none are supplied."""
    if (start is None) ^ (end is None):
        raise ValueError("Both start_date and end_date must be provided together")
