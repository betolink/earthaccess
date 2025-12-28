"""Geometry utilities for reading and simplifying spatial data.

This module provides functions for reading geometry from various file formats
(GeoJSON, Shapefile, KML, etc.) and simplifying complex polygons to meet
CMR's point limit requirements (<300 points).

Requires the optional 'geo' dependency: pip install earthaccess[geo]
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

logger = logging.getLogger(__name__)

# CMR has a limit of ~300 points for polygon queries
MAX_POLYGON_POINTS = 300


def _check_shapely() -> None:
    """Check if shapely is available, raise ImportError with helpful message if not."""
    try:
        import shapely  # noqa: F401
    except ImportError:
        raise ImportError(
            "shapely is required for geometry file support. "
            "Install it with: pip install earthaccess[geo]"
        )


def read_geometry_file(file_path: Union[str, Path]) -> Dict[str, Any]:
    """Read geometry from a file and return as GeoJSON.

    Supports GeoJSON (.geojson, .json) files natively. For other formats
    (Shapefile, KML, etc.), shapely is required.

    Args:
        file_path: Path to the geometry file

    Returns:
        GeoJSON geometry dictionary

    Raises:
        FileNotFoundError: If file does not exist
        ValueError: If file format is not supported or geometry is invalid
        ImportError: If shapely is required but not installed
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Geometry file not found: {path}")

    suffix = path.suffix.lower()

    if suffix in (".geojson", ".json"):
        return _read_geojson(path)
    elif suffix == ".shp":
        return _read_shapefile(path)
    elif suffix in (".kml", ".kmz"):
        return _read_kml(path)
    elif suffix == ".wkt":
        return _read_wkt(path)
    else:
        raise ValueError(
            f"Unsupported geometry file format: {suffix}. "
            "Supported formats: .geojson, .json, .shp, .kml, .kmz, .wkt"
        )


def _read_geojson(path: Path) -> Dict[str, Any]:
    """Read GeoJSON file and extract geometry."""
    with open(path) as f:
        data = json.load(f)

    # Handle different GeoJSON structures
    if data.get("type") == "FeatureCollection":
        if not data.get("features"):
            raise ValueError("GeoJSON FeatureCollection has no features")
        # Use first feature's geometry
        geometry = data["features"][0].get("geometry")
    elif data.get("type") == "Feature":
        geometry = data.get("geometry")
    elif data.get("type") in ("Polygon", "MultiPolygon", "Point", "LineString"):
        geometry = data
    else:
        raise ValueError(f"Unsupported GeoJSON type: {data.get('type')}")

    if not geometry:
        raise ValueError("No geometry found in GeoJSON file")

    return geometry


def _read_shapefile(path: Path) -> Dict[str, Any]:
    """Read Shapefile and convert to GeoJSON geometry."""
    _check_shapely()
    from shapely.geometry import mapping, shape

    try:
        # Try using fiona if available (more robust)
        import fiona  # type: ignore[import-not-found]

        with fiona.open(path) as src:
            if len(src) == 0:
                raise ValueError("Shapefile has no features")
            feature = next(iter(src))
            return feature["geometry"]
    except ImportError:
        # Fallback: try using pyshp
        try:
            import shapefile  # type: ignore[import-not-found]

            sf = shapefile.Reader(str(path))
            if len(sf.shapes()) == 0:
                raise ValueError("Shapefile has no shapes")
            shp = sf.shape(0)
            # Convert pyshp shape to shapely geometry
            geom = shape(shp.__geo_interface__)  # type: ignore[arg-type]
            return mapping(geom)
        except ImportError:
            raise ImportError(
                "Reading Shapefiles requires either 'fiona' or 'pyshp'. "
                "Install with: pip install fiona or pip install pyshp"
            )


def _read_kml(path: Path) -> Dict[str, Any]:
    """Read KML/KMZ file and convert to GeoJSON geometry."""
    _check_shapely()

    try:
        import fiona  # type: ignore[import-not-found]

        # fiona supports KML with the KML driver
        with fiona.open(path, driver="KML") as src:
            if len(src) == 0:
                raise ValueError("KML file has no features")
            feature = next(iter(src))
            return feature["geometry"]
    except ImportError:
        raise ImportError(
            "Reading KML files requires 'fiona'. Install with: pip install fiona"
        )


def _read_wkt(path: Path) -> Dict[str, Any]:
    """Read WKT file and convert to GeoJSON geometry."""
    _check_shapely()
    from shapely import wkt
    from shapely.geometry import mapping

    with open(path) as f:
        wkt_str = f.read().strip()

    geom = wkt.loads(wkt_str)
    return mapping(geom)


def simplify_geometry(
    geometry: Dict[str, Any],
    max_points: int = MAX_POLYGON_POINTS,
    preserve_topology: bool = True,
) -> Dict[str, Any]:
    """Simplify a GeoJSON geometry to have fewer than max_points vertices.

    Uses the Douglas-Peucker algorithm via shapely to iteratively simplify
    the geometry until it has fewer than max_points vertices.

    Args:
        geometry: GeoJSON geometry dictionary
        max_points: Maximum number of points allowed (default: 300 for CMR)
        preserve_topology: If True, preserve topology during simplification

    Returns:
        Simplified GeoJSON geometry dictionary

    Raises:
        ImportError: If shapely is not installed
        ValueError: If geometry cannot be simplified enough
    """
    _check_shapely()
    from shapely.geometry import mapping, shape

    geom = shape(geometry)
    point_count = _count_points(geometry)

    if point_count <= max_points:
        logger.debug(f"Geometry has {point_count} points, no simplification needed")
        return geometry

    logger.info(
        f"Simplifying geometry from {point_count} points to <{max_points} points"
    )

    # Start with a small tolerance and increase until we meet the point limit
    # Use the geometry bounds to calculate an appropriate starting tolerance
    bounds = geom.bounds  # (minx, miny, maxx, maxy)
    extent = max(bounds[2] - bounds[0], bounds[3] - bounds[1])
    tolerance = extent / 10000  # Start very small

    simplified = geom
    iterations = 0
    max_iterations = 50

    while (
        _count_points(mapping(simplified)) > max_points and iterations < max_iterations
    ):
        simplified = geom.simplify(tolerance, preserve_topology=preserve_topology)
        tolerance *= 2  # Double tolerance each iteration
        iterations += 1

    result = mapping(simplified)
    final_count = _count_points(result)

    if final_count > max_points:
        raise ValueError(
            f"Could not simplify geometry to fewer than {max_points} points. "
            f"Final point count: {final_count}. "
            "Consider using a simpler geometry or a bounding box instead."
        )

    logger.info(
        f"Simplified geometry to {final_count} points in {iterations} iterations"
    )
    return result


def _count_points(geometry: Dict[str, Any]) -> int:
    """Count the number of points in a GeoJSON geometry."""
    geom_type = geometry.get("type", "")
    coords = geometry.get("coordinates", [])

    if geom_type == "Point":
        return 1
    elif geom_type == "LineString":
        return len(coords)
    elif geom_type == "Polygon":
        # Sum of points in all rings (exterior + holes)
        return sum(len(ring) for ring in coords)
    elif geom_type == "MultiPolygon":
        total = 0
        for polygon in coords:
            total += sum(len(ring) for ring in polygon)
        return total
    elif geom_type == "MultiLineString":
        return sum(len(line) for line in coords)
    elif geom_type == "MultiPoint":
        return len(coords)
    elif geom_type == "GeometryCollection":
        return sum(_count_points(g) for g in geometry.get("geometries", []))
    else:
        return 0


def extract_polygon_coords(
    geometry: Dict[str, Any],
) -> List[Tuple[float, float]]:
    """Extract polygon coordinates from a GeoJSON geometry.

    For MultiPolygon, uses the first polygon. For GeometryCollection,
    uses the first polygon-type geometry found.

    Args:
        geometry: GeoJSON geometry dictionary

    Returns:
        List of (lon, lat) coordinate tuples forming the exterior ring

    Raises:
        ValueError: If geometry is not a polygon type
    """
    geom_type = geometry.get("type", "")
    coords = geometry.get("coordinates", [])

    if geom_type == "Polygon":
        # Return exterior ring (first ring)
        exterior = coords[0] if coords else []
        return [(float(lon), float(lat)) for lon, lat in exterior]

    elif geom_type == "MultiPolygon":
        # Use first polygon's exterior ring
        if not coords:
            raise ValueError("MultiPolygon has no polygons")
        first_polygon = coords[0]
        exterior = first_polygon[0] if first_polygon else []
        return [(float(lon), float(lat)) for lon, lat in exterior]

    elif geom_type == "GeometryCollection":
        # Find first polygon-type geometry
        for geom in geometry.get("geometries", []):
            if geom.get("type") in ("Polygon", "MultiPolygon"):
                return extract_polygon_coords(geom)
        raise ValueError("GeometryCollection contains no polygon geometries")

    else:
        raise ValueError(
            f"Cannot extract polygon coordinates from geometry type: {geom_type}. "
            "Expected Polygon, MultiPolygon, or GeometryCollection with polygons."
        )


def load_and_simplify_polygon(
    source: Union[str, Path],
    max_points: int = MAX_POLYGON_POINTS,
) -> List[Tuple[float, float]]:
    """Load a geometry file and return simplified polygon coordinates.

    This is the main entry point for loading geometry files for use with
    the query API. It reads the file, simplifies if needed, and extracts
    the polygon coordinates.

    Args:
        source: Path to geometry file (GeoJSON, Shapefile, KML, WKT)
        max_points: Maximum number of points (default: 300 for CMR)

    Returns:
        List of (lon, lat) coordinate tuples suitable for Polygon.from_coords()

    Raises:
        FileNotFoundError: If file does not exist
        ValueError: If geometry cannot be processed
        ImportError: If required dependencies are not installed

    Examples:
        >>> coords = load_and_simplify_polygon("boundary.geojson")
        >>> query = GranuleQuery().polygon(coords)

        >>> coords = load_and_simplify_polygon("study_area.shp", max_points=200)
        >>> query = GranuleQuery().polygon(coords)
    """
    path = Path(source)
    geometry = read_geometry_file(path)

    # Check if simplification is needed
    point_count = _count_points(geometry)
    if point_count > max_points:
        geometry = simplify_geometry(geometry, max_points=max_points)

    return extract_polygon_coords(geometry)
