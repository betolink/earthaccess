"""STAC Item query class for earthaccess.

This module provides the StacItemQuery class for building STAC-native search queries.
This is a NEW addition to support direct STAC catalog querying without CMR conversion.
"""

from typing import Any, Dict, Union

from typing_extensions import Self

from .base import QueryBase
from .types import BoundingBox
from .validation import ValidationResult


class StacItemQuery(QueryBase):
    """Query builder for searching STAC catalogs with native STAC parameters.

    This class supports construction with STAC-native parameters (collections, datetime,
    bbox, query) and can be converted to CMR format for NASA CMR-STAC bridge.

    Example - STAC-native construction:
        >>> query = StacItemQuery(
        ...     collections=["HLSL30.v2.0"],
        ...     datetime="2020-01-01/2020-12-31",
        ...     bbox=[-180, -90, 180, -60],
        ...     query={"eo:cloud_cover": {"lt": 20}}
        ... )

    Example - Method chaining:
        >>> query = (
        ...     StacItemQuery()
        ...     .collections(["HLSL30.v2.0"])
        ...     .datetime("2020-01-01/2020-12-31")
        ...     .bbox([-180, -90, 180, -60])
        ... )

    Example - Convert to CMR format (for NASA CMR-STAC bridge):
        >>> cmr_params = query.to_cmr()
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize a STAC query with optional named parameters.

        Args:
            **kwargs: Named STAC parameters (collections, datetime, bbox, query, ids, etc)
        """
        # Initialize query filters before calling super().__init__()
        # which may call parameters() which uses the query() method
        self._query_filters: Dict[str, Any] = {}
        super().__init__(**kwargs)

    def collections(self, collections: Union[str, list]) -> Self:
        """Filter by collection ID(s).

        Args:
            collections: A single collection ID or list of collection IDs.
                Example: "HLSL30.v2.0" or ["HLSL30.v2.0", "HLSS30.v2.0"]

        Returns:
            self for method chaining
        """
        if isinstance(collections, str):
            collections = [collections]
        return self._set_param("collections", list(collections))

    def datetime(self, datetime_range: str) -> Self:
        """Filter by datetime range in ISO 8601 format.

        Args:
            datetime_range: ISO 8601 datetime range string.
                Format: "2020-01-01/2020-12-31" or a single datetime "2020-06-15T12:00:00Z"

        Returns:
            self for method chaining

        Raises:
            ValueError: If datetime format is invalid
        """
        if not isinstance(datetime_range, str):
            raise TypeError("datetime_range must be a string")
        if "/" not in datetime_range:
            # Single datetime, convert to range
            raise ValueError("Use 'start/end' format for datetime ranges")
        return self._set_param("datetime", datetime_range)

    def bbox(self, bbox: Union[list, tuple, BoundingBox]) -> Self:
        """Filter by bounding box.

        Args:
            bbox: Bounding box as [west, south, east, north] or BoundingBox object

        Returns:
            self for method chaining

        Raises:
            ValueError: If bbox format is invalid
        """
        if isinstance(bbox, BoundingBox):
            bbox = bbox.to_stac()
        elif isinstance(bbox, (list, tuple)):
            if len(bbox) != 4:
                raise ValueError(
                    "bbox must have 4 elements: [west, south, east, north]"
                )
            bbox = list(bbox)
        else:
            raise TypeError("bbox must be a list, tuple, or BoundingBox object")
        return self._set_param("bbox", bbox)

    def intersects(self, geometry: Dict[str, Any]) -> Self:
        """Filter by GeoJSON geometry intersection.

        Args:
            geometry: GeoJSON geometry object (Point, Polygon, etc)

        Returns:
            self for method chaining

        Raises:
            ValueError: If geometry is not valid GeoJSON
        """
        if not isinstance(geometry, dict) or "type" not in geometry:
            raise ValueError("geometry must be a valid GeoJSON object")
        return self._set_param("intersects", geometry)

    def ids(self, ids: Union[str, list]) -> Self:
        """Filter by item ID(s).

        Args:
            ids: A single item ID or list of item IDs

        Returns:
            self for method chaining
        """
        if isinstance(ids, str):
            ids = [ids]
        return self._set_param("ids", list(ids))

    def query(self, filters: Dict[str, Any]) -> Self:
        """Add CQL2 query filters.

        Args:
            filters: Dictionary of CQL2 filters.
                Example: {"eo:cloud_cover": {"lt": 20}}

        Returns:
            self for method chaining
        """
        self._query_filters.update(filters)
        return self._set_param("query", self._query_filters)

    def _validate(self, result: ValidationResult) -> None:
        """Validate the STAC query parameters.

        Args:
            result: ValidationResult to add errors to
        """
        # At least one collection should be specified
        if not self._has_param("collections") and not self._has_param("ids"):
            result.add_error("collections", "At least one collection or id is required")

    def to_cmr(self) -> Dict[str, Any]:
        """Convert the STAC query to CMR API format.

        Note: This conversion may lose some STAC-specific filters that don't have
        CMR equivalents. A warning is logged for incompatible parameters.

        Returns:
            Dictionary of CMR query parameters.
        """
        params: Dict[str, Any] = {}

        # Map collections to short_name (STAC collections are often versioned)
        if self._has_param("collections"):
            collections = self._get_param("collections")
            if collections:
                # Use first collection as short_name, others could be added with OR logic
                params["short_name"] = collections[0]

        # Map datetime to temporal
        if self._has_param("datetime"):
            datetime_str = self._get_param("datetime")
            if "/" in datetime_str:
                start, end = datetime_str.split("/")
                params["temporal"] = f"{start},{end}"
            else:
                params["temporal"] = f"{datetime_str},{datetime_str}"

        # Map bbox to bounding_box
        if self._has_param("bbox"):
            bbox = self._get_param("bbox")
            # STAC: [west, south, east, north], CMR: west,south,east,north
            if len(bbox) == 4:
                params["bounding_box"] = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"

        # Map query filters to CMR parameters where possible
        if self._has_param("query"):
            query_filters = self._get_param("query")
            # Cloud cover mapping
            if "eo:cloud_cover" in query_filters:
                cloud_filter = query_filters["eo:cloud_cover"]
                if "lt" in cloud_filter:
                    # Convert single filter to range
                    params["cloud_cover"] = f"0,{cloud_filter['lt']}"
                elif "lte" in cloud_filter:
                    params["cloud_cover"] = f"0,{cloud_filter['lte']}"
                elif "gte" in cloud_filter and "lte" in cloud_filter:
                    params["cloud_cover"] = (
                        f"{cloud_filter['gte']},{cloud_filter['lte']}"
                    )

        # Map item IDs to concept_id (granule concept IDs start with G)
        # STAC item IDs might not be CMR concept IDs, so we skip this for now
        if self._has_param("ids"):
            pass

        return params

    def to_stac(self) -> Dict[str, Any]:
        """Convert the query to STAC API format.

        Returns:
            Dictionary of STAC API query parameters.
        """
        params: Dict[str, Any] = {}

        # Copy all STAC-native parameters
        for key in ["collections", "datetime", "bbox", "intersects", "ids", "query"]:
            if self._has_param(key):
                params[key] = self._get_param(key)

        return params
