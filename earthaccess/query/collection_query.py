"""Collection query class for earthaccess.

This module provides the CollectionQuery class for building collection search queries.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

from typing_extensions import Self

from .base import QueryBase
from .types import (
    BoundingBox,
    DateLike,
    DateRange,
    FloatLike,
    Point,
    PointLike,
    Polygon,
)


class CollectionQuery(QueryBase):
    """Query builder for searching NASA CMR collections (datasets).

    Supports both method chaining and named parameter construction.
    Can be converted to CMR or STAC format.

    Example - Method chaining:
        >>> query = (
        ...     CollectionQuery()
        ...     .keyword("sea surface temperature")
        ...     .daac("PODAAC")
        ...     .cloud_hosted(True)
        ... )

    Example - Named parameters:
        >>> query = CollectionQuery(
        ...     keyword="sea surface temperature",
        ...     daac="PODAAC",
        ...     cloud_hosted=True
        ... )
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize a collection query.

        Args:
            **kwargs: Named parameters matching method names.
        """
        super().__init__(**kwargs)
        self._fields: Optional[List[str]] = None

    def keyword(self, text: str) -> Self:
        """Search by keyword across multiple fields.

        Case-insensitive and wildcard (*) search through collection metadata
        fields including title, summary, and science keywords.

        Args:
            text: Text to search for (wildcards supported)

        Returns:
            self for method chaining
        """
        return self._set_param("keyword", text)

    def short_name(self, short_name: str) -> Self:
        """Filter by collection short name (product name).

        Args:
            short_name: The collection short name

        Returns:
            self for method chaining

        Raises:
            TypeError: If short_name is not a string.
        """
        if not isinstance(short_name, str):
            raise TypeError("short_name must be of type str")
        return self._set_param("short_name", short_name)

    def version(self, version: str) -> Self:
        """Filter by collection version.

        Args:
            version: The version string

        Returns:
            self for method chaining

        Raises:
            TypeError: If version is not a string.
        """
        if not isinstance(version, str):
            raise TypeError("version must be of type str")
        return self._set_param("version", version)

    def concept_id(self, ids: Union[str, Sequence[str]]) -> Self:
        """Filter by concept ID(s).

        Args:
            ids: A single concept ID or sequence of concept IDs

        Returns:
            self for method chaining
        """
        if isinstance(ids, str):
            ids = [ids]
        return self._set_param("concept_id", list(ids))

    def doi(self, doi: str) -> Self:
        """Filter by DOI.

        Note: Not all datasets have an associated DOI.

        Args:
            doi: DOI of a dataset (e.g., "10.5067/AQR50-3Q7CS")

        Returns:
            self for method chaining

        Raises:
            TypeError: If doi is not a string.
        """
        if not isinstance(doi, str):
            raise TypeError("doi must be of type str")
        return self._set_param("doi", doi)

    def provider(self, provider: str) -> Self:
        """Filter by data provider.

        Args:
            provider: Provider code (e.g., POCLOUD, NSIDC_CPRD)

        Returns:
            self for method chaining
        """
        return self._set_param("provider", provider)

    def daac(self, daac_short_name: str) -> Self:
        """Filter by DAAC (data center).

        Args:
            daac_short_name: DAAC shortname (e.g., NSIDC, PODAAC, GES_DISC)

        Returns:
            self for method chaining
        """
        return self._set_param("daac", daac_short_name)

    def data_center(self, data_center_name: str) -> Self:
        """Alias for daac().

        Args:
            data_center_name: DAAC shortname

        Returns:
            self for method chaining
        """
        return self.daac(data_center_name)

    def cloud_hosted(self, cloud_hosted: bool = True) -> Self:
        """Filter for cloud-hosted collections only.

        Note: Restricted collections will not be matched using this parameter.

        Args:
            cloud_hosted: If True, only return cloud-hosted collections

        Returns:
            self for method chaining

        Raises:
            TypeError: If cloud_hosted is not a boolean.
        """
        if not isinstance(cloud_hosted, bool):
            raise TypeError("cloud_hosted must be of type bool")
        return self._set_param("cloud_hosted", cloud_hosted)

    def has_granules(self, has_granules: Optional[bool] = True) -> Self:
        """Filter by whether collections have granules.

        Args:
            has_granules: If True, only collections with granules.
                         If False, only collections without granules.
                         If None, return both.

        Returns:
            self for method chaining

        Raises:
            TypeError: If has_granules is not a bool or None.
        """
        if has_granules is not None and not isinstance(has_granules, bool):
            raise TypeError("has_granules must be of type bool or None")

        if has_granules is not None:
            return self._set_param("has_granules", has_granules)
        # If None, remove any existing filter
        if self._has_param("has_granules"):
            del self._params["has_granules"]
        return self

    def instrument(self, instrument: str) -> Self:
        """Filter by instrument.

        Args:
            instrument: Instrument name (e.g., "GEDI")

        Returns:
            self for method chaining

        Raises:
            TypeError: If instrument is not a string.
        """
        if not isinstance(instrument, str):
            raise TypeError("instrument must be of type str")
        return self._set_param("instrument", instrument)

    def project(self, project: str) -> Self:
        """Filter by associated project.

        Args:
            project: Project name (e.g., "EMIT")

        Returns:
            self for method chaining

        Raises:
            TypeError: If project is not a string.
        """
        if not isinstance(project, str):
            raise TypeError("project must be of type str")
        return self._set_param("project", project)

    def temporal(
        self,
        date_from: Optional[DateLike] = None,
        date_to: Optional[DateLike] = None,
        exclude_boundary: bool = False,
    ) -> Self:
        """Filter by temporal coverage.

        Args:
            date_from: Start of temporal range
            date_to: End of temporal range
            exclude_boundary: Whether to exclude boundary dates

        Returns:
            self for method chaining
        """
        date_range = DateRange.from_dates(date_from, date_to, exclude_boundary)
        self._temporal_ranges.append(date_range)
        return self

    def bounding_box(
        self,
        west: FloatLike,
        south: FloatLike,
        east: FloatLike,
        north: FloatLike,
    ) -> Self:
        """Filter by bounding box.

        Args:
            west: Western longitude
            south: Southern latitude
            east: Eastern longitude
            north: Northern latitude

        Returns:
            self for method chaining
        """
        self._spatial = BoundingBox.from_coords(west, south, east, north)
        return self

    def point(self, lon: FloatLike, lat: FloatLike) -> Self:
        """Filter by a geographic point.

        Args:
            lon: Longitude
            lat: Latitude

        Returns:
            self for method chaining
        """
        self._spatial = Point.from_coords(lon, lat)
        return self

    def polygon(
        self,
        coordinates: Optional[Sequence[PointLike]] = None,
        *,
        file: Optional[Union[str, Path]] = None,
        max_points: int = 300,
    ) -> Self:
        """Filter by a polygonal area.

        Can accept either coordinate tuples or a geometry file path. When using
        a file, complex geometries are automatically simplified to meet CMR's
        point limit requirements.

        Args:
            coordinates: List of (lon, lat) tuples forming the polygon.
                Cannot be used together with 'file'.
            file: Path to geometry file (.geojson, .json, .shp, .kml, .wkt).
                Cannot be used together with 'coordinates'.
            max_points: Maximum points for file-based geometries (default: 300).
                Only used when 'file' is specified.

        Returns:
            self for method chaining

        Raises:
            ValueError: If both coordinates and file are provided.
            ImportError: If file is provided but shapely is not installed.

        Examples:
            Using coordinates:
            >>> query = CollectionQuery().keyword("temperature").polygon([
            ...     (-10, -10), (10, -10), (10, 10), (-10, 10), (-10, -10)
            ... ])

            Using a geometry file:
            >>> query = CollectionQuery().keyword("temperature").polygon(
            ...     file="boundary.geojson"
            ... )
        """
        if coordinates is not None and file is not None:
            raise ValueError(
                "Cannot specify both 'coordinates' and 'file'. Use one or the other."
            )
        if coordinates is None and file is None:
            raise ValueError("Must specify either 'coordinates' or 'file'.")

        if file is not None:
            self._spatial = Polygon.from_file(file, max_points=max_points)
        else:
            self._spatial = Polygon.from_coords(coordinates)  # type: ignore[arg-type]
        return self

    def fields(self, fields: Optional[List[str]] = None) -> Self:
        """Mask the response to only include specified fields.

        Args:
            fields: List of UMM field names to include (e.g., ["Abstract", "Title"])

        Returns:
            self for method chaining
        """
        self._fields = fields
        return self

    def to_cmr(self) -> Dict[str, Any]:
        """Convert the query to CMR API format.

        Returns:
            Dictionary of CMR query parameters.
        """
        params: Dict[str, Any] = dict(self._params)

        # Handle temporal ranges
        if self._temporal_ranges:
            temporal_strs = [tr.to_cmr() for tr in self._temporal_ranges]
            params["temporal"] = (
                temporal_strs if len(temporal_strs) > 1 else temporal_strs[0]
            )

        # Handle spatial
        if self._spatial is not None:
            if isinstance(self._spatial, BoundingBox):
                params["bounding_box"] = self._spatial.to_cmr()
            elif isinstance(self._spatial, Point):
                params["point"] = self._spatial.to_cmr()
            elif isinstance(self._spatial, Polygon):
                params["polygon"] = self._spatial.to_cmr()

        return params

    def to_stac(self) -> Dict[str, Any]:
        """Convert the query to STAC API format.

        Returns:
            Dictionary of STAC API query parameters.
        """
        params: Dict[str, Any] = {}

        # Map keyword to q (free text search)
        if self._has_param("keyword"):
            params["q"] = self._get_param("keyword")

        # Handle short_name as collection ID filter
        if self._has_param("short_name"):
            params["collections"] = [self._get_param("short_name")]

        # Handle concept_id
        if self._has_param("concept_id"):
            params["ids"] = self._get_param("concept_id")

        # Handle temporal
        if self._temporal_ranges:
            if len(self._temporal_ranges) == 1:
                params["datetime"] = self._temporal_ranges[0].to_stac()
            else:
                starts = [tr.start for tr in self._temporal_ranges if tr.start]
                ends = [tr.end for tr in self._temporal_ranges if tr.end]
                combined = DateRange(
                    start=min(starts) if starts else None,
                    end=max(ends) if ends else None,
                )
                params["datetime"] = combined.to_stac()

        # Handle spatial
        if isinstance(self._spatial, BoundingBox):
            params["bbox"] = self._spatial.to_stac()
        elif isinstance(self._spatial, Point):
            pt = self._spatial
            epsilon = 0.0001
            params["bbox"] = [
                pt.lon - epsilon,
                pt.lat - epsilon,
                pt.lon + epsilon,
                pt.lat + epsilon,
            ]
        elif isinstance(self._spatial, Polygon):
            params["intersects"] = {
                "type": "Polygon",
                "coordinates": self._spatial.to_stac(),
            }

        return params
