"""Result containers – now immutable dataclasses."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Mapping


@dataclass(frozen=True)
class ResultBase:
    """Common helpers for all result types."""

    def to_dict(self) -> Mapping[str, Any]:
        return asdict(self)

    def __repr__(self) -> str:
        # Compact representation useful for debugging / REPL
        fields = ", ".join(f"{k}={v!r}" for k, v in asdict(self).items())
        return f"{self.__class__.__name__}({fields})"


@dataclass(frozen=True)
class CollectionResult(ResultBase):
    id: str
    title: str
    short_name: str
    version_id: str | None = None
    # add any other fields you need; they will be populated from the CMR JSON


@dataclass(frozen=True)
class GranuleResult(ResultBase):
    id: str
    collection_id: str
    time_start: str | None = None
    time_end: str | None = None
    # additional granule‑specific fields …
