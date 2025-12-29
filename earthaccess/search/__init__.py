"""Search package for NASA CMR queries and results.

This package provides classes for searching NASA's Common Metadata Repository (CMR)
and working with search results.
"""

# Re-export base query classes from cmr package for backward compatibility
# These are used internally by DataCollections and DataGranules
from cmr import CollectionQuery as CmrCollectionQuery, GranuleQuery as CmrGranuleQuery
from earthaccess.search.queries import DataCollections, DataGranules

# New query builder classes (earthaccess-native, no cmr dependency)
from earthaccess.search.query import (
    BoundingBox,
    CollectionQuery,
    DateRange,
    GranuleQuery,
    Point,
    Polygon,
    QueryBase,
    StacItemQuery,
    ValidationError,
    ValidationResult,
)
from earthaccess.search.results import (
    CollectionResults,
    CustomDict,
    DataCollection,
    DataGranule,
    GranuleResults,
    SearchResults,
)
from earthaccess.search.services import DataServices

__all__ = [
    # New query builder classes (earthaccess-native)
    "GranuleQuery",
    "CollectionQuery",
    "QueryBase",
    "StacItemQuery",
    "BoundingBox",
    "DateRange",
    "Point",
    "Polygon",
    "ValidationError",
    "ValidationResult",
    # Legacy query classes (cmr-based, for internal use)
    "CmrCollectionQuery",
    "CmrGranuleQuery",
    # Query executor classes
    "DataCollections",
    "DataGranules",
    # Result classes
    "CustomDict",
    "DataCollection",
    "DataGranule",
    "SearchResults",
    "GranuleResults",
    "CollectionResults",
    # Service class
    "DataServices",
]
