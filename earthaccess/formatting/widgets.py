"""Interactive widget-based formatters for Jupyter notebook display.

This module provides interactive widgets using anywidget and lonboard
for rich visualization of earthaccess search results in Jupyter notebooks.

Requires the [widgets] extra: pip install earthaccess[widgets]
"""

from typing import TYPE_CHECKING, Any, List, Optional

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

    for i, item in enumerate(items[:max_items]):
        # Determine if granule or collection
        is_granule = "GranuleUR" in item.get("umm", {})

        if is_granule:
            bbox = _extract_granule_bbox(item)
            name = item.get("umm", {}).get("GranuleUR", "Unknown")[:50]
            size = item.size() if hasattr(item, "size") else 0
        else:
            bbox = _extract_collection_bbox(item)
            name = item.get("umm", {}).get("ShortName", "Unknown")
            size = 0

        if bbox is None:
            continue

        # Create polygon from bbox
        west, south, east, north = bbox

        # Handle antimeridian crossing
        if west > east:
            # Split into two polygons? For now, just use full extent
            west, east = -180, 180

        geometries.append(box(west, south, east, north))
        ids.append(item.get("meta", {}).get("concept-id", f"item_{i}"))
        names.append(name)
        sizes.append(size)
        cloud_hosted.append(getattr(item, "cloud_hosted", False))

    if not geometries:
        # Return empty GeoDataFrame
        return gpd.GeoDataFrame(
            {"id": [], "name": [], "size_mb": [], "cloud": [], "geometry": []},
            crs="EPSG:4326",
        )

    return gpd.GeoDataFrame(
        {
            "id": ids,
            "name": names,
            "size_mb": sizes,
            "cloud": cloud_hosted,
        },
        geometry=geometries,
        crs="EPSG:4326",
    )


def show_map(
    results: "SearchResults",
    max_items: int = 10000,
    fill_color: Optional[List[int]] = None,
    line_color: Optional[List[int]] = None,
) -> Any:
    """Display an interactive map with bounding boxes for search results.

    This function creates a lonboard map visualization showing the spatial
    extent of cached search results. Only the first `max_items` results are
    displayed to maintain performance.

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
        >>> from earthaccess.formatting.widgets import show_map
        >>> show_map(results)  # Display interactive map
    """
    _check_widget_dependencies()

    from lonboard import Map, PolygonLayer

    # Default colors
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

    # Convert to GeoDataFrame
    gdf = _bboxes_to_geodataframe(cached, max_items=max_items)

    if len(gdf) == 0:
        raise ValueError("No valid bounding boxes found in results.")

    # Create polygon layer
    layer = PolygonLayer.from_geopandas(
        gdf,
        get_fill_color=fill_color,
        get_line_color=line_color,
        line_width_min_pixels=1,
    )

    # Create map centered on data
    bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
    center_lon = (bounds[0] + bounds[2]) / 2
    center_lat = (bounds[1] + bounds[3]) / 2

    m = Map(
        layers=[layer],
        _initial_view_state={
            "longitude": center_lon,
            "latitude": center_lat,
            "zoom": 2,
        },
    )

    return m


def show_granule_map(
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

    geometry = box(west, south, east, north)
    gdf = gpd.GeoDataFrame(
        {
            "id": [granule.get("meta", {}).get("concept-id", "granule")],
            "name": [granule.get("umm", {}).get("GranuleUR", "Unknown")[:50]],
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
        _initial_view_state={
            "longitude": center_lon,
            "latitude": center_lat,
            "zoom": zoom,
        },
    )

    return m


def show_collection_map(
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

    geometry = box(west, south, east, north)
    short_name = collection.get("umm", {}).get("ShortName", "Unknown")
    version = collection.get("umm", {}).get("Version", "")

    gdf = gpd.GeoDataFrame(
        {
            "id": [collection.get("meta", {}).get("concept-id", "collection")],
            "name": [f"{short_name} v{version}" if version else short_name],
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
        _initial_view_state={
            "longitude": center_lon,
            "latitude": center_lat,
            "zoom": 1,
        },
    )

    return m


__all__ = [
    "show_map",
    "show_granule_map",
    "show_collection_map",
]
