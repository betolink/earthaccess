"""Type definitions for the query module.

This module contains type aliases and dataclasses used throughout the query package.
These types are designed to be compatible with both CMR and STAC query formats.
"""

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Sequence, Tuple, Union

from typing_extensions import SupportsFloat, TypeAlias

if TYPE_CHECKING:
    pass

# Basic type aliases
FloatLike: TypeAlias = Union[str, SupportsFloat]
"""A type that can be converted to a float (string or numeric)."""

PointLike: TypeAlias = Tuple[FloatLike, FloatLike]
"""A coordinate pair representing (longitude, latitude)."""

PolygonLike: TypeAlias = Sequence[PointLike]
"""A sequence of coordinate pairs forming a polygon (first and last must match)."""

LineLike: TypeAlias = Sequence[PointLike]
"""A sequence of coordinate pairs forming a line."""

DateLike: TypeAlias = Union[str, dt.date, dt.datetime]
"""A date that can be a string, date, or datetime object."""


@dataclass(frozen=True)
class BoundingBox:
    """A geographic bounding box.

    Attributes:
        west: Western longitude (lower left lon)
        south: Southern latitude (lower left lat)
        east: Eastern longitude (upper right lon)
        north: Northern latitude (upper right lat)
    """

    west: float
    south: float
    east: float
    north: float

    def __post_init__(self) -> None:
        """Validate the bounding box coordinates."""
        if not -180 <= self.west <= 180:
            raise ValueError(f"west must be between -180 and 180, got {self.west}")
        if not -180 <= self.east <= 180:
            raise ValueError(f"east must be between -180 and 180, got {self.east}")
        if not -90 <= self.south <= 90:
            raise ValueError(f"south must be between -90 and 90, got {self.south}")
        if not -90 <= self.north <= 90:
            raise ValueError(f"north must be between -90 and 90, got {self.north}")
        if self.south > self.north:
            raise ValueError(
                f"south ({self.south}) must be less than or equal to north ({self.north})"
            )

    def to_cmr(self) -> str:
        """Convert to CMR bounding box format.

        Returns:
            Comma-separated string: west,south,east,north
        """
        return f"{self.west},{self.south},{self.east},{self.north}"

    def to_stac(self) -> List[float]:
        """Convert to STAC bbox format.

        Returns:
            List of coordinates: [west, south, east, north]
        """
        return [self.west, self.south, self.east, self.north]

    @classmethod
    def from_coords(
        cls,
        west: FloatLike,
        south: FloatLike,
        east: FloatLike,
        north: FloatLike,
    ) -> "BoundingBox":
        """Create a BoundingBox from coordinate values.

        Args:
            west: Western longitude
            south: Southern latitude
            east: Eastern longitude
            north: Northern latitude

        Returns:
            A new BoundingBox instance
        """
        return cls(
            west=float(west),
            south=float(south),
            east=float(east),
            north=float(north),
        )


@dataclass(frozen=True)
class DateRange:
    """A temporal range for queries.

    Attributes:
        start: Start of the temporal range (inclusive)
        end: End of the temporal range (inclusive)
        exclude_boundary: Whether to exclude the boundary dates
    """

    start: Optional[dt.datetime] = None
    end: Optional[dt.datetime] = None
    exclude_boundary: bool = False

    def to_cmr(self) -> str:
        """Convert to CMR temporal format.

        Returns:
            ISO 8601 formatted string: start/end
        """
        start_str = self.start.isoformat() + "Z" if self.start else ""
        end_str = self.end.isoformat() + "Z" if self.end else ""
        return f"{start_str},{end_str}"

    def to_stac(self) -> str:
        """Convert to STAC datetime format.

        Returns:
            ISO 8601 formatted string: start/end
        """
        start_str = self.start.isoformat() + "Z" if self.start else ".."
        end_str = self.end.isoformat() + "Z" if self.end else ".."
        return f"{start_str}/{end_str}"

    @classmethod
    def from_dates(
        cls,
        start: Optional[DateLike] = None,
        end: Optional[DateLike] = None,
        exclude_boundary: bool = False,
    ) -> "DateRange":
        """Create a DateRange from date values.

        Args:
            start: Start date (string, date, or datetime)
            end: End date (string, date, or datetime)
            exclude_boundary: Whether to exclude boundary dates

        Returns:
            A new DateRange instance
        """
        start_dt = cls._parse_date(start) if start else None
        end_dt = cls._parse_date(end, is_end=True) if end else None

        if start_dt and end_dt and start_dt > end_dt:
            raise ValueError(
                f"start date ({start_dt}) must be before end date ({end_dt})"
            )

        return cls(start=start_dt, end=end_dt, exclude_boundary=exclude_boundary)

    @staticmethod
    def _parse_date(
        date_input: DateLike,
        is_end: bool = False,
    ) -> dt.datetime:
        """Parse a date input into a datetime object.

        Args:
            date_input: The date to parse
            is_end: If True and input is a date (not datetime), set time to 23:59:59

        Returns:
            A datetime object
        """
        if isinstance(date_input, dt.datetime):
            return date_input
        elif isinstance(date_input, dt.date):
            if is_end:
                return dt.datetime.combine(date_input, dt.time(23, 59, 59))
            return dt.datetime.combine(date_input, dt.time(0, 0, 0))
        elif isinstance(date_input, str):
            # Try parsing common formats
            for fmt in [
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d",
                "%Y-%m",
                "%Y",
            ]:
                try:
                    parsed = dt.datetime.strptime(date_input, fmt)
                    # For date-only strings at end of range, set to end of day
                    if is_end and fmt in ["%Y-%m-%d", "%Y-%m", "%Y"]:
                        if fmt == "%Y-%m-%d":
                            parsed = parsed.replace(hour=23, minute=59, second=59)
                        elif fmt == "%Y-%m":
                            # Last day of month at 23:59:59
                            import calendar

                            last_day = calendar.monthrange(parsed.year, parsed.month)[1]
                            parsed = parsed.replace(
                                day=last_day, hour=23, minute=59, second=59
                            )
                        elif fmt == "%Y":
                            # Last day of year at 23:59:59
                            parsed = parsed.replace(
                                month=12, day=31, hour=23, minute=59, second=59
                            )
                    return parsed
                except ValueError:
                    continue
            raise ValueError(f"Could not parse date string: {date_input}")
        else:
            raise TypeError(f"Expected date-like value, got {type(date_input)}")


@dataclass(frozen=True)
class Point:
    """A geographic point.

    Attributes:
        lon: Longitude
        lat: Latitude
    """

    lon: float
    lat: float

    def __post_init__(self) -> None:
        """Validate the point coordinates."""
        if not -180 <= self.lon <= 180:
            raise ValueError(f"lon must be between -180 and 180, got {self.lon}")
        if not -90 <= self.lat <= 90:
            raise ValueError(f"lat must be between -90 and 90, got {self.lat}")

    def to_cmr(self) -> str:
        """Convert to CMR point format.

        Returns:
            Comma-separated string: lon,lat
        """
        return f"{self.lon},{self.lat}"

    def to_stac(self) -> List[float]:
        """Convert to STAC coordinate format.

        Returns:
            List of coordinates: [lon, lat]
        """
        return [self.lon, self.lat]

    @classmethod
    def from_coords(cls, lon: FloatLike, lat: FloatLike) -> "Point":
        """Create a Point from coordinate values.

        Args:
            lon: Longitude
            lat: Latitude

        Returns:
            A new Point instance
        """
        return cls(lon=float(lon), lat=float(lat))


@dataclass(frozen=True)
class Polygon:
    """A geographic polygon.

    Attributes:
        coordinates: List of (lon, lat) coordinate tuples. Must be closed
            (first and last points must match) and have at least 4 points.
    """

    coordinates: Tuple[Tuple[float, float], ...]

    def __post_init__(self) -> None:
        """Validate the polygon coordinates."""
        if len(self.coordinates) < 4:
            raise ValueError("Polygon must have at least 4 points")
        if self.coordinates[0] != self.coordinates[-1]:
            raise ValueError(
                "Polygon must be closed (first and last points must match)"
            )
        # Validate each coordinate
        for lon, lat in self.coordinates:
            if not -180 <= lon <= 180:
                raise ValueError(f"lon must be between -180 and 180, got {lon}")
            if not -90 <= lat <= 90:
                raise ValueError(f"lat must be between -90 and 90, got {lat}")

    def to_cmr(self) -> str:
        """Convert to CMR polygon format.

        Returns:
            Comma-separated string of lon,lat pairs
        """
        return ",".join(f"{lon},{lat}" for lon, lat in self.coordinates)

    def to_stac(self) -> List[List[List[float]]]:
        """Convert to STAC GeoJSON polygon coordinates.

        Returns:
            GeoJSON coordinates array: [[[lon, lat], ...]]
        """
        return [[[lon, lat] for lon, lat in self.coordinates]]

    @classmethod
    def from_coords(cls, coordinates: Sequence[PointLike]) -> "Polygon":
        """Create a Polygon from coordinate values.

        Args:
            coordinates: List of (lon, lat) tuples

        Returns:
            A new Polygon instance
        """
        points = tuple((float(lon), float(lat)) for lon, lat in coordinates)
        return cls(coordinates=points)

    @classmethod
    def from_file(
        cls,
        file_path: Union[str, Path],
        max_points: int = 300,
    ) -> "Polygon":
        """Create a Polygon from a geometry file.

        Reads geometry from GeoJSON, Shapefile, KML, or WKT files and
        automatically simplifies complex geometries to meet CMR's point
        limit requirements (<300 points).

        Args:
            file_path: Path to geometry file (.geojson, .json, .shp, .kml, .wkt)
            max_points: Maximum number of points allowed (default: 300 for CMR)

        Returns:
            A new Polygon instance with simplified coordinates

        Raises:
            FileNotFoundError: If file does not exist
            ValueError: If geometry cannot be processed
            ImportError: If required dependencies are not installed

        Examples:
            >>> poly = Polygon.from_file("boundary.geojson")
            >>> poly = Polygon.from_file("study_area.shp", max_points=200)
        """
        from .geometry import load_and_simplify_polygon

        coords = load_and_simplify_polygon(file_path, max_points=max_points)
        return cls.from_coords(coords)


# Type alias for spatial types
SpatialType = Union[BoundingBox, Point, Polygon]
