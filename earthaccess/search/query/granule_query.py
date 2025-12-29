"""Granule query class for earthaccess.

This module provides the GranuleQuery class for building granule search queries.
"""

from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Sequence, Union

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
from .validation import ValidationResult


class GranuleQuery(QueryBase):
    """Query builder for searching NASA CMR granules.

    Supports both method chaining and named parameter construction.
    Can be converted to CMR or STAC format.

    Example - Method chaining:
        >>> query = (
        ...     GranuleQuery()
        ...     .short_name("ATL03")
        ...     .temporal("2020-01", "2020-02")
        ...     .bounding_box(-180, -90, 180, 90)
        ... )

    Example - Named parameters:
        >>> query = GranuleQuery(
        ...     short_name="ATL03",
        ...     temporal=("2020-01", "2020-02"),
        ...     bounding_box=(-180, -90, 180, 90)
        ... )

    Example - Convert to CMR format:
        >>> cmr_params = query.to_cmr()

    Example - Convert to STAC format:
        >>> stac_params = query.to_stac()
    """

    __module__ = "earthaccess.search"

    def short_name(self, short_name: str) -> Self:
        """Filter by collection short name (product name).

        Args:
            short_name: The collection short name (e.g., "ATL03")

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

        Note: CMR defines version as a string. For example, MODIS version 6
        products must be searched for with "006".

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

        Concept IDs uniquely identify collections, granules, tools, or services.
        Examples: C1299783579-LPDAAC_ECS, G1327299284-LPDAAC_ECS

        Args:
            ids: A single concept ID or sequence of concept IDs

        Returns:
            self for method chaining
        """
        if isinstance(ids, str):
            ids = [ids]
        return self._set_param("concept_id", list(ids))

    def provider(self, provider: str) -> Self:
        """Filter by data provider.

        A NASA datacenter or DAAC can have one or more providers. For example,
        PODAAC is a data center, PODAAC is the default provider for on-prem data,
        and POCLOUD is the PODAAC provider for cloud data.

        Args:
            provider: Provider code (e.g., POCLOUD, NSIDC_CPRD)

        Returns:
            self for method chaining
        """
        return self._set_param("provider", provider)

    def temporal(
        self,
        date_from: Optional[DateLike] = None,
        date_to: Optional[DateLike] = None,
        exclude_boundary: bool = False,
    ) -> Self:
        """Filter by temporal range.

        Dates can be provided as date objects or ISO 8601 strings. Multiple
        ranges can be applied by calling this method multiple times.

        Tip: Using date (not datetime) for date_to includes the entire day
        (time set to 23:59:59). datetime objects use 00:00:00 by default.

        Args:
            date_from: Start of temporal range
            date_to: End of temporal range
            exclude_boundary: Whether to exclude boundary dates

        Returns:
            self for method chaining

        Raises:
            ValueError: If dates cannot be parsed or date_from > date_to.
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

        Must be used with a collection-limiting parameter like short_name or concept_id.

        Args:
            west: Western longitude (lower left)
            south: Southern latitude (lower left)
            east: Eastern longitude (upper right)
            north: Northern latitude (upper right)

        Returns:
            self for method chaining

        Raises:
            ValueError: If coordinates are invalid.
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

        Raises:
            ValueError: If coordinates are invalid.
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

        Must be used with a collection-limiting parameter like short_name or concept_id.
        The polygon should be closed (first and last points must match).

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
            ValueError: If coordinates are invalid, polygon is not closed,
                or both coordinates and file are provided.
            ImportError: If file is provided but shapely is not installed.

        Examples:
            Using coordinates:
            >>> query = GranuleQuery().short_name("ATL03").polygon([
            ...     (-10, -10), (10, -10), (10, 10), (-10, 10), (-10, -10)
            ... ])

            Using a geometry file:
            >>> query = GranuleQuery().short_name("ATL03").polygon(
            ...     file="boundary.geojson"
            ... )

            Using a shapefile with custom max points:
            >>> query = GranuleQuery().short_name("ATL03").polygon(
            ...     file="study_area.shp", max_points=200
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

    def line(self, coordinates: Sequence[PointLike]) -> Self:
        """Filter by a line (series of connected points).

        Must be used with a collection-limiting parameter like short_name or concept_id.

        Args:
            coordinates: List of (lon, lat) tuples forming the line

        Returns:
            self for method chaining

        Raises:
            ValueError: If coordinates are invalid.
        """
        if len(coordinates) < 2:
            raise ValueError("Line must have at least 2 points")

        points = [(float(lon), float(lat)) for lon, lat in coordinates]
        self._set_param("line", points)
        return self

    def granule_name(self, granule_name: Union[str, Iterable[str]]) -> Self:
        """Filter by granule name.

        Matches against granule_ur or producer_granule_id using the
        readable_granule_name field. Wildcards (*) are supported.

        Args:
            granule_name: Granule name or names (wildcards accepted)

        Returns:
            self for method chaining

        Raises:
            TypeError: If granule_name is not a string or iterable of strings.
        """
        if not isinstance(granule_name, (str, Iterable)):
            raise TypeError("granule_name must be of type str or Iterable[str]")

        if isinstance(granule_name, str):
            names = [granule_name]
        else:
            names = list(granule_name)

        return self._set_param("readable_granule_name", names)

    def day_night_flag(self, flag: str) -> Self:
        """Filter by period of the day.

        Args:
            flag: One of "day", "night", or "unspecified"

        Returns:
            self for method chaining

        Raises:
            ValueError: If flag is not valid.
        """
        valid_flags = {"day", "night", "unspecified"}
        if flag.lower() not in valid_flags:
            raise ValueError(f"day_night_flag must be one of {valid_flags}")
        return self._set_param("day_night_flag", flag.lower())

    def cloud_cover(
        self,
        min_cover: Optional[FloatLike] = 0,
        max_cover: Optional[FloatLike] = 100,
    ) -> Self:
        """Filter by cloud cover percentage.

        Args:
            min_cover: Minimum cloud cover percentage (default: 0)
            max_cover: Maximum cloud cover percentage (default: 100)

        Returns:
            self for method chaining

        Raises:
            ValueError: If min_cover > max_cover.
        """
        min_val = float(min_cover) if min_cover is not None else 0
        max_val = float(max_cover) if max_cover is not None else 100

        if min_val > max_val:
            raise ValueError("min_cover cannot be greater than max_cover")

        return self._set_param("cloud_cover", (min_val, max_val))

    def orbit_number(
        self,
        orbit1: FloatLike,
        orbit2: Optional[FloatLike] = None,
    ) -> Self:
        """Filter by orbit number.

        Args:
            orbit1: Single orbit or lower bound of range
            orbit2: Upper bound of range (optional)

        Returns:
            self for method chaining
        """
        if orbit2 is not None:
            return self._set_param("orbit_number", (float(orbit1), float(orbit2)))
        return self._set_param("orbit_number", float(orbit1))

    def instrument(self, instrument: str) -> Self:
        """Filter by instrument.

        Args:
            instrument: Instrument name

        Returns:
            self for method chaining

        Raises:
            ValueError: If instrument is empty.
        """
        if not instrument:
            raise ValueError("instrument must not be empty")
        return self._set_param("instrument", instrument)

    def platform(self, platform: str) -> Self:
        """Filter by satellite platform.

        Args:
            platform: Satellite name

        Returns:
            self for method chaining

        Raises:
            ValueError: If platform is empty.
        """
        if not platform:
            raise ValueError("platform must not be empty")
        return self._set_param("platform", platform)

    def downloadable(self, downloadable: bool = True) -> Self:
        """Filter for downloadable granules only.

        Args:
            downloadable: If True, only return downloadable granules

        Returns:
            self for method chaining

        Raises:
            TypeError: If downloadable is not a boolean.
        """
        if not isinstance(downloadable, bool):
            raise TypeError("downloadable must be of type bool")
        return self._set_param("downloadable", downloadable)

    def online_only(self, online_only: bool = True) -> Self:
        """Filter for online-only granules (not downloadable).

        Args:
            online_only: If True, only return online-only granules

        Returns:
            self for method chaining

        Raises:
            TypeError: If online_only is not a boolean.
        """
        if not isinstance(online_only, bool):
            raise TypeError("online_only must be of type bool")
        return self._set_param("online_only", online_only)

    def _validate(self, result: ValidationResult) -> None:
        """Validate the query parameters.

        Args:
            result: ValidationResult to add errors to
        """
        # Check for spatial without collection limiter
        spatial_set = self._spatial is not None or self._has_param("line")
        collection_keys = ["short_name", "entry_title", "concept_id"]

        if spatial_set and not any(self._has_param(k) for k in collection_keys):
            result.add_error(
                "spatial",
                "Spatial queries require a collection filter (short_name, entry_title, or concept_id)",
            )

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

        # Handle line
        if self._has_param("line"):
            line_coords = self._get_param("line")
            params["line"] = ",".join(f"{lon},{lat}" for lon, lat in line_coords)

        # Handle readable_granule_name with pattern matching
        if "readable_granule_name" in params:
            params["options[readable_granule_name][pattern]"] = True

        # Handle cloud cover
        if "cloud_cover" in params:
            min_cover, max_cover = params["cloud_cover"]
            params["cloud_cover"] = f"{min_cover},{max_cover}"

        # Handle orbit number
        if "orbit_number" in params:
            orbit = params["orbit_number"]
            if isinstance(orbit, tuple):
                params["orbit_number"] = f"{orbit[0]},{orbit[1]}"

        return params

    def to_stac(self) -> Dict[str, Any]:
        """Convert the query to STAC API format.

        Returns:
            Dictionary of STAC API query parameters.
        """
        params: Dict[str, Any] = {}

        # Map short_name to collection
        if self._has_param("short_name"):
            params["collections"] = [self._get_param("short_name")]

        # Handle concept_id as collection IDs
        if self._has_param("concept_id"):
            concept_ids = self._get_param("concept_id")
            # Filter for collection concept IDs (start with C)
            collection_ids = [cid for cid in concept_ids if cid.startswith("C")]
            if collection_ids:
                params["collections"] = collection_ids
            # Granule IDs (start with G) map to item IDs
            granule_ids = [cid for cid in concept_ids if cid.startswith("G")]
            if granule_ids:
                params["ids"] = granule_ids

        # Handle temporal - STAC uses datetime parameter
        if self._temporal_ranges:
            # STAC only supports single datetime range
            if len(self._temporal_ranges) == 1:
                params["datetime"] = self._temporal_ranges[0].to_stac()
            else:
                # Use the union of all ranges (first start to last end)
                starts = [tr.start for tr in self._temporal_ranges if tr.start]
                ends = [tr.end for tr in self._temporal_ranges if tr.end]
                combined = DateRange(
                    start=min(starts) if starts else None,
                    end=max(ends) if ends else None,
                )
                params["datetime"] = combined.to_stac()

        # Handle spatial - STAC uses bbox
        if isinstance(self._spatial, BoundingBox):
            params["bbox"] = self._spatial.to_stac()
        elif isinstance(self._spatial, Point):
            # STAC doesn't have point query, approximate with small bbox
            pt = self._spatial
            epsilon = 0.0001
            params["bbox"] = [
                pt.lon - epsilon,
                pt.lat - epsilon,
                pt.lon + epsilon,
                pt.lat + epsilon,
            ]
        elif isinstance(self._spatial, Polygon):
            # STAC uses intersects with GeoJSON geometry
            params["intersects"] = {
                "type": "Polygon",
                "coordinates": self._spatial.to_stac(),
            }

        # Map cloud cover to query extension
        if self._has_param("cloud_cover"):
            min_cover, max_cover = self._get_param("cloud_cover")
            params["query"] = params.get("query", {})
            params["query"]["eo:cloud_cover"] = {"gte": min_cover, "lte": max_cover}

        return params
