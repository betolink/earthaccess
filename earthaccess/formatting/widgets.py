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


def _check_ipywidgets() -> None:
    """Check that ipywidgets is installed.

    Raises:
        ImportError: If ipywidgets is not installed.
    """
    try:
        import ipywidgets  # noqa: F401
    except ImportError:
        raise ImportError(
            "Interactive browsing requires ipywidgets. "
            "Install with: pip install ipywidgets"
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


def browse_results(
    results: "SearchResults",
    page_size: int = 20,
) -> Any:
    """Create an interactive browser widget for SearchResults.

    This creates a Jupyter widget with bidirectional pagination - clicking
    'Next' will automatically fetch more results from CMR if needed.

    Parameters:
        results: A SearchResults instance
        page_size: Number of items per page (default 20)

    Returns:
        An ipywidgets VBox containing the interactive browser

    Raises:
        ImportError: If ipywidgets is not installed

    Examples:
        >>> results = earthaccess.search_data(short_name="ATL06", count=100)
        >>> results.browse()  # Returns interactive widget

        >>> # Or use directly:
        >>> from earthaccess.formatting.widgets import browse_results
        >>> browse_results(results, page_size=50)
    """
    _check_ipywidgets()

    import ipywidgets as widgets
    from IPython.display import HTML, display

    # State
    current_page = [0]  # Use list to allow mutation in closures

    def get_total_hits():
        if results._total_hits is None:
            results.total()
        return results._total_hits or 0

    def get_cached_count():
        return len(results._cached_results)

    def get_total_pages():
        total = get_total_hits()
        return max(1, (total + page_size - 1) // page_size)

    def ensure_page_cached(page_num: int):
        """Ensure results for the given page are cached."""
        needed = (page_num + 1) * page_size
        current = get_cached_count()
        if current < needed and not results._exhausted:
            results._ensure_cached(needed)

    def is_granule(item) -> bool:
        return "GranuleUR" in item.get("umm", {})

    def format_granule_row(item, idx: int) -> str:
        granule_ur = item.get("umm", {}).get("GranuleUR", "Unknown")
        name = granule_ur[:45] + "..." if len(granule_ur) > 45 else granule_ur
        temporal = item.get("umm", {}).get("TemporalExtent", {})
        range_dt = temporal.get("RangeDateTime", {})
        begin = range_dt.get("BeginningDateTime", "")
        date_str = begin[:10] if len(begin) >= 10 else (begin or "N/A")
        size = round(item.size(), 2) if hasattr(item, "size") else 0
        cloud = "Yes" if getattr(item, "cloud_hosted", False) else "No"
        links = item.data_links() if hasattr(item, "data_links") else []
        link_html = (
            f'<a href="{links[0]}" target="_blank">Download</a>' if links else "-"
        )
        return f"<tr><td>{idx + 1}</td><td><code>{name}</code></td><td>{date_str}</td><td>{size} MB</td><td>{cloud}</td><td>{link_html}</td></tr>"

    def format_collection_row(item, idx: int) -> str:
        short_name = item.get("umm", {}).get("ShortName", "Unknown")
        version = item.get("umm", {}).get("Version", "")
        name = f"{short_name} v{version}" if version else short_name
        if len(name) > 35:
            name = name[:32] + "..."
        concept_id = item.get("meta", {}).get("concept-id", "")
        doi = ""
        doi_obj = item.get("umm", {}).get("DOI", {})
        if isinstance(doi_obj, dict):
            doi = doi_obj.get("DOI", "")
        cloud_info = item.get("umm", {}).get("DirectDistributionInformation")
        cloud = "Yes" if cloud_info else "No"
        landing = ""
        for url_info in item.get("umm", {}).get("RelatedUrls", []):
            if url_info.get("Type") == "DATA SET LANDING PAGE":
                landing = url_info.get("URL", "")
                break
        link_html = (
            f'<a href="{landing}" target="_blank">Landing</a>' if landing else "-"
        )
        doi_html = (
            f'<a href="https://doi.org/{doi}" target="_blank">{doi[:25]}...</a>'
            if doi and len(doi) > 25
            else (doi or "-")
        )
        return f"<tr><td>{idx + 1}</td><td><code>{name}</code></td><td><code style='font-size:0.8em'>{concept_id}</code></td><td>{doi_html}</td><td>{cloud}</td><td>{link_html}</td></tr>"

    def render_table() -> str:
        page = current_page[0]
        start_idx = page * page_size
        end_idx = start_idx + page_size

        # Ensure we have the data for this page
        ensure_page_cached(page)

        items = results._cached_results[start_idx:end_idx]
        total_hits = get_total_hits()
        cached = get_cached_count()
        total_pages = get_total_pages()

        # Determine type
        is_granules = len(items) > 0 and is_granule(items[0])

        if is_granules:
            header = "<tr><th>#</th><th>Name</th><th>Date</th><th>Size</th><th>Cloud</th><th>Link</th></tr>"
            rows = [
                format_granule_row(item, start_idx + i) for i, item in enumerate(items)
            ]
            result_type = "granules"
        else:
            header = "<tr><th>#</th><th>Short Name</th><th>Concept ID</th><th>DOI</th><th>Cloud</th><th>Link</th></tr>"
            rows = [
                format_collection_row(item, start_idx + i)
                for i, item in enumerate(items)
            ]
            result_type = "collections"

        actual_end = min(end_idx, cached)
        showing = f"{start_idx + 1}-{actual_end}" if items else "0"

        return f"""
        <div style="font-family: sans-serif; font-size: 14px;">
            <div style="margin-bottom: 10px; padding: 8px; background: #f5f5f5; border-radius: 4px;">
                <b>Total in CMR:</b> {total_hits:,} {result_type} |
                <b>Loaded:</b> {cached:,} |
                <b>Showing:</b> {showing} |
                <b>Page:</b> {page + 1} of {total_pages}
            </div>
            <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
                <thead style="background: #e9e9e9;">{header}</thead>
                <tbody>{"".join(rows) if rows else "<tr><td colspan='6' style='text-align:center;'>No results</td></tr>"}</tbody>
            </table>
        </div>
        """

    # Create widgets
    output = widgets.Output()

    def update_display():
        with output:
            output.clear_output(wait=True)
            display(HTML(render_table()))

    def on_first(b):
        current_page[0] = 0
        update_buttons()
        update_display()

    def on_prev(b):
        if current_page[0] > 0:
            current_page[0] -= 1
        update_buttons()
        update_display()

    def on_next(b):
        total_pages = get_total_pages()
        if current_page[0] < total_pages - 1:
            current_page[0] += 1
        update_buttons()
        update_display()

    def on_last(b):
        current_page[0] = get_total_pages() - 1
        update_buttons()
        update_display()

    def on_page_size_change(change):
        nonlocal page_size
        page_size = change["new"]
        current_page[0] = 0  # Reset to first page
        update_buttons()
        update_display()

    # Navigation buttons
    btn_first = widgets.Button(
        description="⏮ First", layout=widgets.Layout(width="80px")
    )
    btn_prev = widgets.Button(description="◀ Prev", layout=widgets.Layout(width="80px"))
    btn_next = widgets.Button(description="Next ▶", layout=widgets.Layout(width="80px"))
    btn_last = widgets.Button(description="Last ⏭", layout=widgets.Layout(width="80px"))

    btn_first.on_click(on_first)
    btn_prev.on_click(on_prev)
    btn_next.on_click(on_next)
    btn_last.on_click(on_last)

    # Page size selector
    page_size_dropdown = widgets.Dropdown(
        options=[10, 20, 50, 100],
        value=page_size,
        description="Per page:",
        layout=widgets.Layout(width="150px"),
    )
    page_size_dropdown.observe(on_page_size_change, names="value")

    def update_buttons():
        total_pages = get_total_pages()
        btn_first.disabled = current_page[0] == 0
        btn_prev.disabled = current_page[0] == 0
        btn_next.disabled = current_page[0] >= total_pages - 1
        btn_last.disabled = current_page[0] >= total_pages - 1

    # Layout
    nav_box = widgets.HBox(
        [btn_first, btn_prev, btn_next, btn_last, page_size_dropdown],
        layout=widgets.Layout(margin="0 0 10px 0"),
    )

    # Initial render
    update_buttons()
    update_display()

    return widgets.VBox([nav_box, output])


__all__ = [
    "show_map",
    "show_granule_map",
    "show_collection_map",
    "browse_results",
]
