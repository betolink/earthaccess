"""Interactive widget-based formatters for Jupyter notebook display.

This module provides interactive widgets using anywidget and lonboard
for rich visualization of earthaccess search results in Jupyter notebooks.

Requires the [widgets] extra: pip install earthaccess[widgets]
"""

from typing import TYPE_CHECKING, Any, List, Optional

from earthaccess.formatting.html import (
    _format_collection_temporal,
    _format_temporal_extent,
)

if TYPE_CHECKING:
    from earthaccess.search.results import DataCollection, DataGranule, SearchResults


def _check_widget_dependencies() -> None:
    """Check that widget dependencies are installed.

    Raises:
        ImportError: If anywidget, lonboard, or geopandas are not installed.
    """
    missing = []
    try:
        import anywidget  # noqa: F401
    except ImportError:
        missing.append("anywidget")

    try:
        import lonboard  # noqa: F401
    except ImportError:
        missing.append("lonboard")

    try:
        import geopandas  # noqa: F401
    except ImportError:
        missing.append("geopandas")

    if missing:
        raise ImportError(
            f"Widget support requires additional dependencies: {', '.join(missing)}. "
            f"Install with: pip install earthaccess[widgets]"
        )


def _extract_granule_bbox(granule: "DataGranule") -> Optional[List[float]]:
    """Extract bounding box from a granule.

    Parameters:
        granule: A DataGranule instance

    Returns:
        [west, south, east, north] or None if not available
    """
    try:
        spatial = granule.get("umm", {}).get("SpatialExtent", {})
        geometry = spatial.get("HorizontalSpatialDomain", {}).get("Geometry", {})

        # Try BoundingRectangles first
        bounding_rects = geometry.get("BoundingRectangles", [])
        if bounding_rects:
            rect = bounding_rects[0]
            return [
                rect.get("WestBoundingCoordinate", -180.0),
                rect.get("SouthBoundingCoordinate", -90.0),
                rect.get("EastBoundingCoordinate", 180.0),
                rect.get("NorthBoundingCoordinate", 90.0),
            ]

        # Try GPolygons
        gpolygons = geometry.get("GPolygons", [])
        if gpolygons:
            # Extract bounding box from polygon points
            points = gpolygons[0].get("Boundary", {}).get("Points", [])
            if points:
                lons = [p.get("Longitude", 0) for p in points]
                lats = [p.get("Latitude", 0) for p in points]
                return [min(lons), min(lats), max(lons), max(lats)]

    except Exception:
        pass

    return None


def _extract_collection_bbox(collection: "DataCollection") -> Optional[List[float]]:
    """Extract bounding box from a collection.

    Parameters:
        collection: A DataCollection instance

    Returns:
        [west, south, east, north] or None if not available
    """
    try:
        spatial = collection.get("umm", {}).get("SpatialExtent", {})
        geometry = spatial.get("HorizontalSpatialDomain", {}).get("Geometry", {})

        bounding_rects = geometry.get("BoundingRectangles", [])
        if bounding_rects:
            rect = bounding_rects[0]
            return [
                rect.get("WestBoundingCoordinate", -180.0),
                rect.get("SouthBoundingCoordinate", -90.0),
                rect.get("EastBoundingCoordinate", 180.0),
                rect.get("NorthBoundingCoordinate", 90.0),
            ]
    except Exception:
        pass

    return None


def _is_global_coverage(bbox: List[float]) -> bool:
    """Return True if a bounding box covers essentially the entire globe.

    Global gridded products (e.g. MUR SST) have granules whose spatial extent
    spans the whole planet. When visualized as filled polygons these all
    collapse into one giant rectangle that hides the basemap and hides any
    individual footprint boundaries.

    Args:
        bbox: [west, south, east, north] in degrees.

    Returns:
        True if the box covers the full (or near-full) globe.
    """
    west, south, east, north = bbox

    lon_span = east - west
    if lon_span <= 0:
        lon_span += 360  # antimeridian-crossing box

    lat_span = north - south
    return lon_span >= 350.0 and lat_span >= 170.0


def _cmr_record_link(concept_id: Optional[str]) -> str:
    """Build a CMR record URL for a granule or collection concept id.

    Args:
        concept_id: CMR concept id (e.g. "G3357328910-LPCLOUD").

    Returns:
        The URL of the record on the CMR API, or an empty string if no id.
    """
    if not concept_id:
        return ""
    return f"https://cmr.earthdata.nasa.gov/search/concepts/{concept_id}"


def _format_bbox(west: float, south: float, east: float, north: float) -> str:
    """Format a bounding box as a readable spatial-coverage string."""
    return f"W {west:.2f}, S {south:.2f}, E {east:.2f}, N {north:.2f}"


def _query_bounding_box(results: "SearchResults") -> Optional[List[float]]:
    """Return the bounding box used in the search query, if any.

    Reads the ``bounding_box`` parameter off the query object so the ROI can
    be drawn alongside the results for contrast. Supports both the legacy
    ``DataGranules``/``DataCollections`` param dicts (``"west,south,east,north"``)
    and the new ``GranuleQuery``/``CollectionQuery`` builders (``_spatial``).

    Args:
        results: A SearchResults instance with a query object.

    Returns:
        A ``[west, south, east, north]`` list, or ``None`` if the query has no
        bounding box (e.g. point/polygon/line or no spatial filter).
    """
    query = getattr(results, "query", None)
    if query is None:
        return None

    # New query builders store spatial filters as BoundingBox objects.
    spatial = getattr(query, "_spatial", None)
    if spatial is not None and hasattr(spatial, "to_stac"):
        from earthaccess.search.query.types import BoundingBox

        if isinstance(spatial, BoundingBox):
            return spatial.to_stac()

    # Legacy param dicts store it as "west,south,east,north".
    params = getattr(query, "params", {})
    bbox = params.get("bounding_box")
    if isinstance(bbox, str):
        parts = bbox.split(",")
        if len(parts) == 4:
            try:
                return [float(p) for p in parts]
            except ValueError:
                return None
    elif isinstance(bbox, (list, tuple)) and len(bbox) == 4:
        return [float(p) for p in bbox]

    return None


def _bboxes_to_geodataframe(
    items: List[Any], max_items: int = 10000
) -> "Any":  # Returns GeoDataFrame
    """Convert a list of granules/collections to a GeoDataFrame with bbox polygons.

    Parameters:
        items: List of DataGranule or DataCollection instances
        max_items: Maximum number of items to include (default 10000)

    Returns:
        A GeoDataFrame with polygon geometries and metadata
    """
    import geopandas as gpd
    from shapely.geometry import box

    geometries = []
    ids = []
    names = []
    sizes = []
    cloud_hosted = []
    coverage = []
    temporals = []
    spatials = []

    for i, item in enumerate(items[:max_items]):
        # Determine if granule or collection
        is_granule = "GranuleUR" in item.get("umm", {})

        if is_granule:
            bbox = _extract_granule_bbox(item)
            name = item.get("umm", {}).get("GranuleUR", "Unknown")[:50]
            size = item.size() if hasattr(item, "size") else 0
            temporal = _format_temporal_extent(
                item.get("umm", {}).get("TemporalExtent", {})
            )
        else:
            bbox = _extract_collection_bbox(item)
            name = item.get("umm", {}).get("ShortName", "Unknown")
            size = 0
            temporal = _format_collection_temporal(
                item.get("umm", {}).get("TemporalExtents")
            )

        if bbox is None:
            continue

        # Create polygon from bbox
        west, south, east, north = bbox

        # Handle antimeridian crossing
        if west > east:
            # Split into two polygons? For now, just use full extent
            west, east = -180, 180

        geometries.append(box(west, south, east, north))
        ids.append(
            _cmr_record_link(item.get("meta", {}).get("concept-id", f"item_{i}"))
        )
        names.append(name)
        sizes.append(size)
        cloud_hosted.append(getattr(item, "cloud_hosted", False))
        coverage.append("global" if _is_global_coverage(bbox) else "regional")
        temporals.append(temporal)
        spatials.append(_format_bbox(west, south, east, north))

    if not geometries:
        # Return empty GeoDataFrame
        gdf = gpd.GeoDataFrame(
            {
                "id": [],
                "name": [],
                "size_mb": [],
                "cloud": [],
                "coverage": [],
                "temporal": [],
                "spatial": [],
            },
            geometry=[],
            crs="EPSG:4326",
        )
        return gdf

    return gpd.GeoDataFrame(
        {
            "id": ids,
            "name": names,
            "size_mb": sizes,
            "cloud": cloud_hosted,
            "coverage": coverage,
            "temporal": temporals,
            "spatial": spatials,
        },
        geometry=geometries,
        crs="EPSG:4326",
    )


def plot(
    results: "SearchResults",
    max_items: int = 10000,
    fill_color: Optional[List[int]] = None,
    line_color: Optional[List[int]] = None,
) -> Any:
    """Display an interactive map with bounding boxes for search results.

    This function creates a lonboard map visualization showing the spatial
    extent of cached search results. Only the first `max_items` results are
    displayed to maintain performance.

    Granules with global coverage (e.g. MUR SST) are drawn as outline-only
    boxes so they do not fill the whole map and hide the basemap.

    When more than 20 footprints are shown, the default fill opacity is
    reduced so overlapping polygons stay distinguishable from the base map.

    Parameters:
        results: A SearchResults instance with cached results
        max_items: Maximum number of bounding boxes to display (default 10000)
        fill_color: RGBA fill color as [r, g, b, a] (default semi-transparent blue)
        line_color: RGBA line color as [r, g, b, a] (default blue)

    Returns:
        A lonboard Map widget for display in Jupyter

    Raises:
        ImportError: If widget dependencies are not installed

    Examples:
        >>> results = earthaccess.search_data(short_name="ATL06", count=100)
        >>> list(results)  # Fetch results first
        >>> from earthaccess.formatting.widgets import plot
        >>> plot(results)  # Display interactive map
    """
    _check_widget_dependencies()

    from lonboard import Map, PolygonLayer

    # Default colors
    fill_color_defaulted = fill_color is None
    if fill_color is None:
        fill_color = [0, 100, 200, 80]  # Semi-transparent blue
    if line_color is None:
        line_color = [0, 100, 200, 200]  # Solid blue

    # Get cached results
    cached = results._cached_results
    if not cached:
        raise ValueError(
            "No cached results to display. "
            "Iterate over the SearchResults first to populate the cache."
        )

    # With many overlapping footprints, stacked filled polygons get very dark.
    # Dial the opacity down as the number of items grows so the base map and
    # individual boundaries stay visible (only for the default color).
    n_results = min(len(cached), max_items)
    if n_results > 20 and fill_color_defaulted:
        fill_alpha = max(5, int(60 * (20 / n_results)))
        fill_color = [*fill_color[:3], fill_alpha]

    # Convert to GeoDataFrame
    gdf = _bboxes_to_geodataframe(cached, max_items=max_items)

    if len(gdf) == 0:
        raise ValueError("No valid bounding boxes found in results.")

    # Global-coverage granules (e.g. MUR SST) all share the full-globe extent,
    # so a filled polygon hides the basemap and any individual boundaries.
    # Render them as thin outline-only boxes instead so base layers stay
    # visible and global coverage is visually distinct from regional granules.
    regional_gdf = gdf[gdf["coverage"] == "regional"]
    global_gdf = gdf[gdf["coverage"] == "global"]

    layers = []
    if len(regional_gdf) > 0:
        layers.append(
            PolygonLayer.from_geopandas(
                regional_gdf,
                get_fill_color=fill_color,
                get_line_color=line_color,
                line_width_min_pixels=1,
            )
        )
    if len(global_gdf) > 0:
        layers.append(
            PolygonLayer.from_geopandas(
                global_gdf,
                get_fill_color=[*fill_color[:3], 0],
                get_line_color=line_color,
                line_width_min_pixels=1,
            )
        )

    # Draw the search ROI (bounding box) as a thin outline for contrast
    roi_bbox = _query_bounding_box(results)
    if roi_bbox is not None:
        import geopandas as gpd
        from shapely.geometry import box

        west, south, east, north = roi_bbox
        roi_gdf = gpd.GeoDataFrame(
            {"coverage": ["roi"]},
            geometry=[box(west, south, east, north)],
            crs="EPSG:4326",
        )
        layers.append(
            PolygonLayer.from_geopandas(
                roi_gdf,
                get_fill_color=[255, 0, 0, 0],
                get_line_color=[255, 0, 0, 255],
                line_width_min_pixels=2,
            )
        )

    # Create map centered on data
    bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
    center_lon = (bounds[0] + bounds[2]) / 2
    center_lat = (bounds[1] + bounds[3]) / 2

    m = Map(
        layers=layers,
        view_state={
            "longitude": center_lon,
            "latitude": center_lat,
            "zoom": 2,
        },
    )

    return m


def plot_granule(
    granule: "DataGranule",
    fill_color: Optional[List[int]] = None,
    line_color: Optional[List[int]] = None,
) -> Any:
    """Display an interactive map with the bounding box for a single granule.

    Parameters:
        granule: A DataGranule instance
        fill_color: RGBA fill color as [r, g, b, a]
        line_color: RGBA line color as [r, g, b, a]

    Returns:
        A lonboard Map widget for display in Jupyter

    Raises:
        ImportError: If widget dependencies are not installed
        ValueError: If granule has no spatial extent
    """
    _check_widget_dependencies()

    import geopandas as gpd
    from lonboard import Map, PolygonLayer
    from shapely.geometry import box

    # Default colors
    if fill_color is None:
        fill_color = [0, 150, 100, 100]  # Semi-transparent green
    if line_color is None:
        line_color = [0, 150, 100, 255]  # Solid green

    bbox = _extract_granule_bbox(granule)
    if bbox is None:
        raise ValueError("Granule has no valid bounding box.")

    west, south, east, north = bbox

    # Handle antimeridian
    if west > east:
        west, east = -180, 180

    # Global-coverage granules span the whole globe; fill the interior would
    # hide the basemap, so draw them as outline-only boxes.
    if _is_global_coverage([west, south, east, north]):
        fill_color = [*fill_color[:3], 0]

    geometry = box(west, south, east, north)
    gdf = gpd.GeoDataFrame(
        {
            "id": [_cmr_record_link(granule.get("meta", {}).get("concept-id"))],
            "name": [granule.get("umm", {}).get("GranuleUR", "Unknown")[:50]],
            "temporal": [
                _format_temporal_extent(
                    granule.get("umm", {}).get("TemporalExtent", {})
                )
            ],
            "spatial": [_format_bbox(west, south, east, north)],
        },
        geometry=[geometry],
        crs="EPSG:4326",
    )

    layer = PolygonLayer.from_geopandas(
        gdf,
        get_fill_color=fill_color,
        get_line_color=line_color,
        line_width_min_pixels=2,
    )

    center_lon = (west + east) / 2
    center_lat = (south + north) / 2

    # Calculate appropriate zoom level based on bbox size
    lat_diff = north - south
    lon_diff = east - west
    max_diff = max(lat_diff, lon_diff)

    if max_diff > 100:
        zoom = 1
    elif max_diff > 50:
        zoom = 2
    elif max_diff > 20:
        zoom = 3
    elif max_diff > 10:
        zoom = 4
    elif max_diff > 5:
        zoom = 5
    else:
        zoom = 6

    m = Map(
        layers=[layer],
        view_state={
            "longitude": center_lon,
            "latitude": center_lat,
            "zoom": zoom,
        },
    )

    return m


def plot_collection(
    collection: "DataCollection",
    fill_color: Optional[List[int]] = None,
    line_color: Optional[List[int]] = None,
) -> Any:
    """Display an interactive map with the spatial extent of a collection.

    Parameters:
        collection: A DataCollection instance
        fill_color: RGBA fill color as [r, g, b, a]
        line_color: RGBA line color as [r, g, b, a]

    Returns:
        A lonboard Map widget for display in Jupyter

    Raises:
        ImportError: If widget dependencies are not installed
        ValueError: If collection has no spatial extent
    """
    _check_widget_dependencies()

    import geopandas as gpd
    from lonboard import Map, PolygonLayer
    from shapely.geometry import box

    # Default colors
    if fill_color is None:
        fill_color = [200, 100, 0, 100]  # Semi-transparent orange
    if line_color is None:
        line_color = [200, 100, 0, 255]  # Solid orange

    bbox = _extract_collection_bbox(collection)
    if bbox is None:
        raise ValueError("Collection has no valid spatial extent.")

    west, south, east, north = bbox

    # Handle antimeridian
    if west > east:
        west, east = -180, 180

    # Global-coverage collections span the whole globe; fill the interior
    # would hide the basemap, so draw them as outline-only boxes.
    if _is_global_coverage([west, south, east, north]):
        fill_color = [*fill_color[:3], 0]

    geometry = box(west, south, east, north)
    short_name = collection.get("umm", {}).get("ShortName", "Unknown")
    version = collection.get("umm", {}).get("Version", "")

    gdf = gpd.GeoDataFrame(
        {
            "id": [_cmr_record_link(collection.get("meta", {}).get("concept-id"))],
            "name": [f"{short_name} v{version}" if version else short_name],
            "temporal": [
                _format_collection_temporal(
                    collection.get("umm", {}).get("TemporalExtents")
                )
            ],
            "spatial": [_format_bbox(west, south, east, north)],
        },
        geometry=[geometry],
        crs="EPSG:4326",
    )

    layer = PolygonLayer.from_geopandas(
        gdf,
        get_fill_color=fill_color,
        get_line_color=line_color,
        line_width_min_pixels=2,
    )

    center_lon = (west + east) / 2
    center_lat = (south + north) / 2

    m = Map(
        layers=[layer],
        view_state={
            "longitude": center_lon,
            "latitude": center_lat,
            "zoom": 1,
        },
    )

    return m


__all__ = [
    "plot",
    "plot_granule",
    "plot_collection",
]
