"""
Query architecture for earthaccess following SOLID principles.

Provides flexible, chainable query building with multiple backend support.
Implements STAC-compatible parameter structures.
"""

import datetime
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

from ..auth import Auth
from ..results import DataCollection, DataGranule, DataGranules

if TYPE_CHECKING:
    from .store_components.credentials import CredentialManager

logger = logging.getLogger(__name__)


# Flexible type aliases for spatial and temporal parameters
BBoxLike = Union[
    Tuple[float, float, float, float],  # west, south, east, north
    List[float],  # [west, south, east, north]
    Tuple[float, float, float, float, float],  # Same as first
    List[Tuple[float, float, float, float]],  # List of coordinates
]

PointLike = Union[
    Tuple[float, float],  # lon, lat
    List[float],  # [lon, lat]
    Tuple[float, float, float, float],  # Same as first
]

TemporalLike = Union[
    Tuple[Optional[str], Optional[str]],  # start, end
    List[Optional[str]],  # [start, end]
    str,  # ISO format or "2023" or "2023-01-01/2023-12-31"
]

CoordinatesLike = Union[
    List[Tuple[float, float]],  # [[lon1, lat1], [lon2, lat2], ...]
    List[List[float]],  # [[lon1, lat1], [lon2, lat2], ...]
]


class QueryValidationError(Exception):
    """Raised when query parameters are invalid."""

    def __init__(self, message: str, parameter: str = None, value: Any = None):
        super().__init__(message)
        self.parameter = parameter
        self.value = value


class BaseQuery:
    """Base class for all query types.

    Provides common functionality for parameter validation,
    query building, and backend execution.
    """

    def __init__(
        self,
        auth: Auth,
        backend: Literal["cmr", "stac"] = "cmr",
    ) -> None:
        """Initialize query.

        Args:
            auth: Auth instance for authentication
            backend: Query backend ('cmr' or 'stac')
        """
        self.auth = auth
        self.backend = backend
        self._params: Dict[str, Any] = {}
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def _validate_bbox(self, bbox: BBoxLike) -> Tuple[float, float, float, float]:
        """Validate and normalize bounding box."""
        if isinstance(bbox, (list, tuple)):
            # Handle both list and tuple inputs
            coords = list(bbox)
            if len(coords) == 4:
                return tuple(float(c) for c in coords)

        raise QueryValidationError(
            f"Invalid bounding box: {bbox}. Must be 4 coordinates "
            f"[west, south, east, north]",
            parameter="bounding_box",
            value=bbox,
        )

    def _validate_point(self, point: PointLike) -> Tuple[float, float]:
        """Validate and normalize point coordinates."""
        if isinstance(point, (list, tuple)):
            coords = list(point)
            if len(coords) == 2:
                try:
                    return float(coords[0]), float(coords[1])
                except (ValueError, TypeError):
                    pass

        raise QueryValidationError(
            f"Invalid point: {point}. Must be 2 coordinates [lon, lat]",
            parameter="point",
            value=point,
        )

    def _validate_temporal(
        self, temporal: TemporalLike
    ) -> Tuple[Optional[str], Optional[str]]:
        """Validate and normalize temporal parameters."""
        if isinstance(temporal, str):
            # Parse various string formats
            if "/" in temporal:
                # ISO interval: "2023-01-01/2023-12-31"
                start, end = temporal.split("/", 1)
                return start.strip() or None, end.strip() or None
            else:
                # Single year/date: "2023" or "2023-01-01"
                return temporal, None

        elif isinstance(temporal, (list, tuple)):
            temporal_list = list(temporal)
            if len(temporal_list) >= 2:
                return temporal_list[0] or None, temporal_list[1] or None

        raise QueryValidationError(
            f"Invalid temporal parameter: {temporal}. "
            f"Must be ISO string, year, or list/tuple of [start, end]",
            parameter="temporal",
            value=temporal,
        )

    def _validate_coordinates(
        self, coords: CoordinatesLike
    ) -> List[Tuple[float, float]]:
        """Validate and normalize coordinate lists."""
        if isinstance(coords, list):
            # Handle list of tuples or list of lists
            normalized = []
            for coord in coords:
                if isinstance(coord, (list, tuple)) and len(coord) >= 2:
                    try:
                        lon, lat = float(coord[0]), float(coord[1])
                        normalized.append((lon, lat))
                    except (ValueError, TypeError):
                        continue

            if normalized:
                return normalized

        raise QueryValidationError(
            f"Invalid coordinates: {coords}. Must be list of [lon, lat] pairs",
            parameter="coordinates",
            value=coords,
        )

    def build_query(self) -> Dict[str, Any]:
        """Build query parameters for backend execution."""
        return self._params.copy()

    def reset(self) -> "BaseQuery":
        """Reset query parameters and return self for chaining."""
        self._params.clear()
        return self


class GranuleQuery(BaseQuery):
    """Query for granule data with method chaining.

    Example:
        >>> query = GranuleQuery(auth)
        >>> query.short_name("MODIS_Aqua_L2").temporal(("2023", "2024")).bounding_box([-180, -90, 180, 90])
        >>> results = query.execute()
    """

    def short_name(self, *short_names: str) -> "GranuleQuery":
        """Filter by collection short name(s).

        Args:
            *short_names: One or more collection short names

        Returns:
            Self for method chaining

        Examples:
            >>> query.short_name("MODIS_Aqua_L2")
            >>> query.short_name("MODIS_Aqua_L2", "MODIS_Terra_L2")
        """
        if len(short_names) == 1:
            self._params["short_name"] = short_names[0]
        else:
            self._params["short_name"] = list(short_names)

        self._logger.debug(f"Added short_name filter: {short_names}")
        return self

    def concept_id(self, *concept_ids: str) -> "GranuleQuery":
        """Filter by collection concept ID(s).

        Args:
            *concept_ids: One or more collection concept IDs

        Returns:
            Self for method chaining

        Examples:
            >>> query.concept_id("C1234567890-POCLOUD")
            >>> query.concept_id("C1234567890-POCLOUD", "C2345678901-NSIDC_CPRD")
        """
        if len(concept_ids) == 1:
            self._params["concept_id"] = concept_ids[0]
        else:
            self._params["concept_id"] = list(concept_ids)

        self._logger.debug(f"Added concept_id filter: {concept_ids}")
        return self

    def temporal(
        self,
        temporal: TemporalLike,
    ) -> "GranuleQuery":
        """Filter by temporal range.

        Args:
            temporal: Temporal filter:
                - Tuple/list: [start, end]
                - String: ISO date, interval, or year
                - Examples: ("2023-01-01", "2023-12-31"), "2023", "2023-01-01/2023-12-31"

        Returns:
            Self for method chaining

        Examples:
            >>> query.temporal(("2023-01-01", "2023-12-31"))
            >>> query.temporal(["2023-01-01", None])  # Open-ended
            >>> query.temporal("2023")  # Entire year
            >>> query.temporal("2023-01-01/P1D")  # ISO duration
        """
        start, end = self._validate_temporal(temporal)

        if start is not None:
            self._params["temporal"] = start if end is None else f"{start}/{end}"

        self._logger.debug(f"Added temporal filter: {start} to {end}")
        return self

    def bounding_box(
        self,
        west_or_bbox: Union[float, BBoxLike],
        south: Optional[float] = None,
        east: Optional[float] = None,
        north: Optional[float] = None,
    ) -> "GranuleQuery":
        """Filter by bounding box.

        Args:
            west_or_bbox: Western longitude OR full bbox [west, south, east, north]
            south: Southern latitude (if west_or_bbox is a single value)
            east: Eastern longitude (if west_or_bbox is a single value)
            north: Northern latitude (if west_or_bbox is a single value)

        Returns:
            Self for method chaining

        Examples:
            >>> query.bounding_box(-180, -90, 180, 90)  # Four separate values
            >>> query.bounding_box([-180, -90, 180, 90])  # List
            >>> query.bounding_box((-180, -90, 180, 90))  # Tuple
            >>> query.bounding_box(gdf.total_bounds)  # From GeoPandas
        """
        bbox = self._normalize_bbox(west_or_bbox, south, east, north)
        west, south, east, north = self._validate_bbox(bbox)

        self._params["bounding_box"] = [west, south, east, north]
        self._logger.debug(f"Added bounding box: [{west}, {south}, {east}, {north}]")
        return self

    def _normalize_bbox(
        self,
        west_or_bbox: Union[float, BBoxLike],
        south: Optional[float],
        east: Optional[float],
        north: Optional[float],
    ) -> BBoxLike:
        """Normalize bbox input to standard format."""
        if south is None and east is None and north is None:
            # Single argument containing full bbox
            return west_or_bbox
        else:
            # Four separate arguments
            return (west_or_bbox, south, east, north)

    def point(
        self, lon_or_point: Union[float, PointLike], lat: Optional[float] = None
    ) -> "GranuleQuery":
        """Filter by point intersection.

        Args:
            lon_or_point: Longitude OR full point [lon, lat]
            lat: Latitude (if lon_or_point is a single value)

        Returns:
            Self for method chaining

        Examples:
            >>> query.point(-122.4194, 37.7749)  # Separate values
            >>> query.point([-122.4194, 37.7749])  # List
            >>> query.point((-122.4194, 37.7749))  # Tuple
        """
        lon, lat = self._normalize_point(lon_or_point, lat)
        lon, lat = self._validate_point((lon, lat))

        self._params["point"] = [lon, lat]
        self._logger.debug(f"Added point filter: [{lon}, {lat}]")
        return self

    def _normalize_point(
        self,
        lon_or_point: Union[float, PointLike],
        lat: Optional[float],
    ) -> PointLike:
        """Normalize point input to standard format."""
        if lat is None:
            # Single argument containing full point
            return lon_or_point
        else:
            # Two separate arguments
            return (lon_or_point, lat)

    def coordinates(
        self,
        coords: CoordinatesLike,
    ) -> "GranuleQuery":
        """Filter by coordinate list.

        Args:
            coords: List of [lon, lat] coordinate pairs

        Returns:
            Self for method chaining

        Examples:
            >>> query.coordinates([[-122.4, 37.8], [-121.9, 37.8]])  # List of tuples
            >>> query.coordinates([[-122.4, 37.8], [-121.9, 37.8]])  # List of lists
        """
        normalized = self._validate_coordinates(coords)
        self._params["polygon"] = [list(coord) for coord in normalized]
        self._logger.debug(f"Added coordinates filter with {len(normalized)} points")
        return self

    def cloud_hosted(self, cloud_hosted: bool = True) -> "GranuleQuery":
        """Filter by cloud hosting status.

        Args:
            cloud_hosted: True for cloud-hosted only, False for on-prem only

        Returns:
            Self for method chaining
        """
        self._params["cloud_hosted"] = cloud_hosted
        self._logger.debug(f"Added cloud_hosted filter: {cloud_hosted}")
        return self

    def online_only(self, online_only: bool = True) -> "GranuleQuery":
        """Filter by online accessibility.

        Args:
            online_only: True for online-only granules

        Returns:
            Self for method chaining
        """
        self._params["online_only"] = online_only
        self._logger.debug(f"Added online_only filter: {online_only}")
        return self

    def page_size(self, page_size: int) -> "GranuleQuery":
        """Set page size for paginated results.

        Args:
            page_size: Number of results per page

        Returns:
            Self for method chaining
        """
        self._params["page_size"] = page_size
        self._logger.debug(f"Set page size: {page_size}")
        return self

    def limit(self, limit: int) -> "GranuleQuery":
        """Limit total number of results.

        Args:
            limit: Maximum number of granules to return

        Returns:
            Self for method chaining
        """
        self._params["limit"] = limit
        self._logger.debug(f"Set result limit: {limit}")
        return self

    def execute(self) -> DataGranules:
        """Execute the query and return results.

        Returns:
            DataGranules instance with found granules

        Raises:
            QueryExecutionError: If query execution fails
        """
        self._logger.info(f"Executing {self.backend} granule query")

        if self.backend == "cmr":
            return self._execute_cmr()
        elif self.backend == "stac":
            return self._execute_stac()
        else:
            raise QueryValidationError(f"Unknown backend: {self.backend}")

    def _execute_cmr(self) -> DataGranules:
        """Execute query using CMR backend."""
        from ..search import granule_query

        # Convert parameters to CMR format
        cmr_params = self._convert_to_cmr_params()

        # Execute CMR search
        try:
            results = granule_query(auth=self.auth, **cmr_params)
            self._logger.info(f"CMR query returned {len(results)} granules")
            return results
        except Exception as e:
            self._logger.error(f"CMR query failed: {e}")
            raise QueryExecutionError(f"CMR query failed: {e}") from e

    def _execute_stac(self) -> DataGranules:
        """Execute query using STAC backend."""
        # This would integrate with pystac-client
        # For now, fallback to CMR
        self._logger.warning("STAC backend not yet implemented, falling back to CMR")
        return self._execute_cmr()

    def _convert_to_cmr_params(self) -> Dict[str, Any]:
        """Convert query parameters to CMR format."""
        params = {}

        # Direct parameter mappings
        if "short_name" in self._params:
            params["short_name"] = self._params["short_name"]
        if "concept_id" in self._params:
            params["concept_id"] = self._params["concept_id"]
        if "temporal" in self._params:
            params["temporal"] = self._params["temporal"]
        if "bounding_box" in self._params:
            params["bounding_box"] = self._params["bounding_box"]
        if "point" in self._params:
            params["point"] = self._params["point"]
        if "polygon" in self._params:
            params["polygon"] = self._params["polygon"]
        if "cloud_hosted" in self._params:
            params["cloud_hosted"] = self._params["cloud_hosted"]
        if "online_only" in self._params:
            params["online_only"] = self._params["online_only"]
        if "page_size" in self._params:
            params["page_size"] = self._params["page_size"]
        if "limit" in self._params:
            params["limit"] = self._params["limit"]

        return params

    def to_stac(self) -> Dict[str, Any]:
        """Convert query to STAC-compatible parameters.

        Returns:
            Dictionary suitable for pystac-client

        Example:
            >>> query = GranuleQuery(auth).short_name("MODIS_Aqua_L2")
            >>> stac_params = query.to_stac()
            >>> # stac_params = {"collections": ["MODIS_Aqua_L2"]}
        """
        stac_params = {}

        # Parameter mappings for STAC
        if "short_name" in self._params:
            stac_params["collections"] = (
                self._params["short_name"]
                if isinstance(self._params["short_name"], str)
                else self._params["short_name"]
            )

        if "temporal" in self._params:
            temporal = self._params["temporal"]
            if isinstance(temporal, str) and "/" in temporal:
                # Convert to STAC datetime format
                start, end = temporal.split("/", 1)
                stac_params["datetime"] = f"{start}T00:00:00Z/{end}T23:59:59Z"
            else:
                stac_params["datetime"] = temporal

        if "bounding_box" in self._params:
            bbox = self._params["bounding_box"]
            stac_params["bbox"] = list(bbox)

        if "point" in self._params:
            point = self._params["point"]
            stac_params["intersects"] = {
                "type": "Point",
                "coordinates": point,
            }

        if "polygon" in self._params:
            stac_params["intersects"] = {
                "type": "Polygon",
                "coordinates": self._params["polygon"],
            }

        return stac_params

    def to_cmr(self) -> Dict[str, Any]:
        """Convert query to CMR parameters.

        Returns:
            Dictionary suitable for earthaccess search functions
        """
        return self._convert_to_cmr_params()


class CollectionQuery(BaseQuery):
    """Query for collection metadata with method chaining.

    Similar to GranuleQuery but for collection-level searches.
    """

    def short_name(self, *short_names: str) -> "CollectionQuery":
        """Filter by collection short name(s)."""
        if len(short_names) == 1:
            self._params["short_name"] = short_names[0]
        else:
            self._params["short_name"] = list(short_names)

        self._logger.debug(f"Added short_name filter: {short_names}")
        return self

    def concept_id(self, *concept_ids: str) -> "CollectionQuery":
        """Filter by collection concept ID(s)."""
        if len(concept_ids) == 1:
            self._params["concept_id"] = concept_ids[0]
        else:
            self._params["concept_id"] = list(concept_ids)

        self._logger.debug(f"Added concept_id filter: {concept_ids}")
        return self

    def keyword(self, *keywords: str) -> "CollectionQuery":
        """Filter by keywords."""
        if len(keywords) == 1:
            self._params["keyword"] = keywords[0]
        else:
            self._params["keyword"] = list(keywords)

        self._logger.debug(f"Added keyword filter: {keywords}")
        return self

    def execute(self) -> List[DataCollection]:
        """Execute collection query and return results."""
        self._logger.info(f"Executing {self.backend} collection query")

        if self.backend == "cmr":
            return self._execute_cmr()
        elif self.backend == "stac":
            return self._execute_stac()
        else:
            raise QueryValidationError(f"Unknown backend: {self.backend}")

    def _execute_cmr(self) -> List[DataCollection]:
        """Execute collection query using CMR backend."""
        from ..search import collection_query

        params = self._convert_to_cmr_params()

        try:
            results = collection_query(auth=self.auth, **params)
            self._logger.info(
                f"CMR collection query returned {len(results)} collections"
            )
            return results
        except Exception as e:
            self._logger.error(f"CMR collection query failed: {e}")
            raise QueryExecutionError(f"Collection query failed: {e}") from e

    def _execute_stac(self) -> List[DataCollection]:
        """Execute collection query using STAC backend."""
        self._logger.warning("STAC backend not yet implemented, falling back to CMR")
        return self._execute_cmr()


class QueryExecutionError(Exception):
    """Raised when query execution fails."""

    pass
