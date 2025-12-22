"""Query module for earthaccess.

This module provides query classes for building CMR and STAC compatible queries.

The query classes support both method chaining and named parameter construction,
and can be converted to CMR or STAC format for execution.

Example:
    >>> from earthaccess.query import GranuleQuery
    >>> query = GranuleQuery().short_name("ATL03").temporal("2020-01", "2020-02")
    >>> cmr_params = query.to_cmr()
"""

from .base import QueryBase
from .collection_query import CollectionQuery
from .granule_query import GranuleQuery
from .types import BoundingBox, DateRange, Point, PointLike, Polygon, PolygonLike
from .validation import ValidationError, ValidationResult

__all__ = [
    "QueryBase",
    "GranuleQuery",
    "CollectionQuery",
    "BoundingBox",
    "DateRange",
    "Point",
    "Polygon",
    "PointLike",
    "PolygonLike",
    "ValidationError",
    "ValidationResult",
]
