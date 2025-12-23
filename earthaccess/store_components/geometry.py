"""Geometry handling for earthaccess.

Provides geometry loading from various sources including
GeoJSON files, shapely geometries, GeoDataFrames, and WKT strings.
Automatically simplifies complex geometries to comply with CMR's 300-point limit.
"""

import json
import logging
import warnings
from pathlib import Path
from typing import Any, Dict, List, Union

logger = logging.getLogger(__name__)

CMR_MAX_POLYGON_POINTS = 300

GeometryLike = Union[
    Dict[str, Any],
    str,
    Path,
]


def load_geometry(
    geometry: GeometryLike,
    *,
    simplify: bool = True,
    max_points: int = CMR_MAX_POLYGON_POINTS,
) -> Dict[str, Any]:
    """Load geometry from various sources and prepare for CMR.

    Accepts multiple geometry input formats for maximum flexibility:
    - GeoJSON dicts (dict with type/coordinates)
    - GeoJSON files (.geojson, .json)
    - WKT strings (Well-Known Text)
    - Shapely geometries (has __geo_interface__)
    - GeoDataFrames (has unary_union)
    - GeoSeries (has unary_union)

    Automatically simplifies complex geometries to comply with CMR's 300-point limit
    using Douglas-Peucker algorithm to preserve topology.

    Args:
        geometry: Geometry input (GeoJSON dict, file path, WKT string,
                     shapely geometry, or GeoDataFrame)
        simplify: If True, simplify geometries exceeding max_points (default: True)
        max_points: Maximum points allowed (default: 300 for CMR)

    Returns:
        GeoJSON geometry dict ready for CMR queries

    Raises:
        ValueError: If geometry cannot be loaded or is invalid

    Examples:
        Load from GeoJSON dict:
        >>> geom = load_geometry({
        ...     "type": "Polygon",
        ...     "coordinates": [[[-122, 37], [-121, 37], [-121, 38], [-122, 38], [-122, 37]]}
        ... })

        Load from GeoJSON file:
        >>> geom = load_geometry("region.geojson")
        >>> geom = load_geometry(Path("region.geojson"))

        Load from shapely geometry:
        >>> from shapely.geometry import box
        >>> geom = load_geometry(box(-122, 37, -121, 38))

        Load from GeoDataFrame:
        >>> import geopandas as gpd
        >>> gdf = gpd.read_file("countries.shp")
        >>> geom = load_geometry(gdf[gdf.name == "France"])

        Load from WKT string:
        >>> wkt = "POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))"
        >>> geom = load_geometry(wkt)

        Use in queries:
        >>> from earthaccess import granule_query
        >>> query = granule_query()
        >>> query.coordinates(geom)

        Disable auto-simplification:
        >>> geom = load_geometry(complex_polygon, simplify=False)
    """
    geojson = _to_geojson(geometry)

    geom_type = geojson.get("type")
    if geom_type not in ("Point", "Polygon", "MultiPolygon", "LineString"):
        raise ValueError(f"Unsupported geometry type: {geom_type}")

    if geom_type in ("Polygon", "MultiPolygon") and simplify:
        point_count = _count_polygon_points(geojson)
        if point_count > max_points:
            geojson = _simplify_geometry(geojson, max_points)
            new_count = _count_polygon_points(geojson)
            warnings.warn(
                f"Geometry simplified from {point_count} to {new_count} points "
                f"to comply with CMR's {max_points}-point limit."
            )

    return geojson


def _to_geojson(geometry: GeometryLike) -> Dict[str, Any]:
    """Convert various geometry inputs to GeoJSON dict.

    Args:
        geometry: Geometry input

    Returns:
        GeoJSON dict
    """
    if isinstance(geometry, dict):
        if geometry.get("type") == "FeatureCollection":
            features = geometry.get("features", [])
            if len(features) == 1:
                return features[0].get("geometry", features[0])
            if len(features) > 1:
                return _union_features(features)
        if geometry.get("type") == "Feature":
            return geometry.get("geometry", geometry)
        return geometry

    if isinstance(geometry, (str, Path)):
        path = Path(geometry)

        if path.exists() and path.is_file():
            with open(path) as f:
                data = json.load(f)
            return _to_geojson(data)

        if isinstance(geometry, str) and geometry.strip().upper().startswith(
            ("POINT", "POLYGON", "MULTIPOLYGON", "LINESTRING")
        ):
            return _wkt_to_geojson(geometry)

        raise ValueError(f"File not found or invalid WKT: {geometry}")

    if hasattr(geometry, "__geo_interface__"):
        return geometry.__geo_interface__

    if hasattr(geometry, "unary_union"):
        union_geom = geometry.unary_union
        return union_geom.__geo_interface__

    raise ValueError(f"Cannot convert {type(geometry)} to GeoJSON")


def _wkt_to_geojson(wkt: str) -> Dict[str, Any]:
    """Convert WKT string to GeoJSON."""
    try:
        from shapely import wkt as shapely_wkt

        geom = shapely_wkt.loads(wkt)
        return geom.__geo_interface__
    except ImportError:
        raise ImportError("shapely is required for WKT parsing: pip install shapely")


def _union_features(features: List[Dict]) -> Dict[str, Any]:
    """Union multiple GeoJSON features into a single geometry."""
    try:
        from shapely.geometry import shape
        from shapely.ops import unary_union

        geometries = [shape(f.get("geometry", f)) for f in features]
        union = unary_union(geometries)
        return union.__geo_interface__
    except ImportError:
        warnings.warn(
            "shapely not installed; using first feature only. "
            "Install shapely for multi-feature support: pip install shapely"
        )
        return features[0].get("geometry", features[0])


def _count_polygon_points(geojson: Dict[str, Any]) -> int:
    """Count total points in a polygon or multipolygon."""
    geom_type = geojson.get("type")

    if geom_type == "Point":
        return 1

    if geom_type == "Polygon":
        coords = geojson.get("coordinates", [])
        return len(coords[0])

    if geom_type == "MultiPolygon":
        polygons = geojson.get("coordinates", [])
        return sum(len(poly[0]) for poly in polygons)

    if geom_type == "LineString":
        coords = geojson.get("coordinates", [])
        return len(coords)

    return 0


def _simplify_geometry(geojson: Dict[str, Any], max_points: int) -> Dict[str, Any]:
    """Simplify geometry to fit within max_points constraint.

    Uses shapely's simplify algorithm with Douglas-Peucker.

    Args:
        geojson: GeoJSON geometry to simplify
        max_points: Maximum allowed points

    Returns:
        Simplified GeoJSON geometry
    """
    try:
        from shapely import simplify as shapely_simplify
        from shapely.geometry import shape

        geom = shape(geojson)

        # Start with a small tolerance and increase if needed
        tolerance = 0.001
        simplified = shapely_simplify(geom, tolerance=tolerance, preserve_topology=True)

        if _count_polygon_points(simplified.__geo_interface__) <= max_points:
            return simplified.__geo_interface__

        while tolerance < 1.0:
            simplified = shapely_simplify(
                geom, tolerance=tolerance, preserve_topology=True
            )
            if _count_polygon_points(simplified.__geo_interface__) <= max_points:
                return simplified.__geo_interface__
            tolerance *= 2

        return simplified.__geo_interface__

    except ImportError:
        warnings.warn(
            "shapely not installed; geometry simplification skipped. "
            "Install shapely for simplification: pip install shapely"
        )
        return geojson
