"""Static HTML formatters for Jupyter notebook display.

These formatters generate static HTML representations for earthaccess objects.
They work without any optional dependencies and provide a baseline visualization.
"""

from typing import TYPE_CHECKING, Any, List, Optional
from uuid import uuid4

import importlib_resources

STATIC_FILES = ["iso_bootstrap4.0.0min.css", "styles.css"]


def _load_css_files() -> List[str]:
    """Load CSS styles for HTML formatting.

    Returns:
        List of CSS file contents as strings.
    """
    return [
        importlib_resources.files("earthaccess.formatting.css")
        .joinpath(fname)
        .read_text("utf8")
        for fname in STATIC_FILES
    ]


if TYPE_CHECKING:
    from earthaccess.search.results import DataCollection, DataGranule, SearchResults


def _repr_granule_html(granule: "DataGranule") -> str:
    """Generate HTML representation for a single granule.

    Parameters:
        granule: A DataGranule instance

    Returns:
        HTML string for notebook display
    """
    css_styles = _load_css_files()
    css_inline = f"""<div id="{uuid4()}" style="height: 0px; display: none">
            {"".join([f"<style>{style}</style>" for style in css_styles])}
            </div>"""
    style = "max-height: 120px;"
    dataviz_img = "".join(
        [
            f'<a href="{link}"><img style="{style}" src="{link}" alt="Data Preview"/></a>'
            for link in granule.dataviz_links()[:2]
            if link.startswith("http")
        ]
    )
    data_links = "".join(
        [
            f'<a href="{link}" target="_blank" class="btn btn-secondary btn-sm">{link.split("/")[-1]}</a>'
            for link in granule.data_links()
            if link.startswith("http")
        ]
    )
    granule_size = round(granule.size(), 2)

    # Extract temporal info
    temporal = granule.get("umm", {}).get("TemporalExtent", {})
    temporal_str = _format_temporal_extent(temporal)

    # Get granule ID
    granule_ur = granule.get("umm", {}).get("GranuleUR", "Unknown")
    concept_id = granule.get("meta", {}).get("concept-id", "")

    return f"""
    {css_inline}
    <div class="bootstrap">
      <div class="container-fluid border" style="padding: 10px;">
        <div class="row">
          <div class="col-8">
            <p style="margin: 2px 0;"><b>Granule</b>: <code>{granule_ur}</code></p>
            <p style="margin: 2px 0;"><b>Concept ID</b>: <code>{concept_id}</code></p>
            <p style="margin: 2px 0;"><b>Temporal</b>: {temporal_str}</p>
            <p style="margin: 2px 0;"><b>Size</b>: {granule_size} MB</p>
            <p style="margin: 2px 0;"><b>Cloud Hosted</b>: {"☁️ Yes" if granule.cloud_hosted else "🖥️ No"}</p>
            <p style="margin: 2px 0;"><b>Data</b>: {data_links if data_links else "No direct links"}</p>
          </div>
          <div class="col-4 text-right">
            {dataviz_img}
          </div>
        </div>
      </div>
    </div>
    """


def _repr_collection_html(collection: "DataCollection") -> str:
    """Generate HTML representation for a single collection.

    Parameters:
        collection: A DataCollection instance

    Returns:
        HTML string for notebook display
    """
    css_styles = _load_css_files()
    css_inline = f"""<div id="{uuid4()}" style="height: 0px; display: none">
            {"".join([f"<style>{style}</style>" for style in css_styles])}
            </div>"""

    # Extract metadata
    short_name = collection.get_umm("ShortName") or "Unknown"
    version = collection.version() or ""
    title = collection.get_umm("EntryTitle") or short_name
    abstract = collection.abstract() or "No description available"

    # Truncate abstract if too long
    if len(abstract) > 300:
        abstract = abstract[:297] + "..."

    # Get DOI
    doi = collection.doi()
    doi_html = (
        f'<a href="https://doi.org/{doi}" target="_blank">{doi}</a>' if doi else "N/A"
    )

    # Get provider
    provider = collection.get("meta", {}).get("provider-id", "Unknown")

    # Cloud hosted
    cloud_info = collection.get_umm("DirectDistributionInformation")
    is_cloud = cloud_info is not None and bool(cloud_info)

    # Temporal extent
    temporal_extents = collection.get_umm("TemporalExtents")
    temporal_str = _format_collection_temporal(temporal_extents)

    # Links
    landing_page = collection.landing_page()
    landing_html = (
        f'<a href="{landing_page}" target="_blank" class="btn btn-primary btn-sm">Landing Page</a>'
        if landing_page
        else ""
    )

    get_data_links = collection.get_data()[:2]  # First 2 data links
    get_data_html = "".join(
        [
            f'<a href="{link}" target="_blank" class="btn btn-secondary btn-sm">Get Data</a>'
            for link in get_data_links
            if link.startswith("http")
        ]
    )

    concept_id = collection.concept_id()

    return f"""
    {css_inline}
    <div class="bootstrap">
      <div class="container-fluid border" style="padding: 10px;">
        <div class="row">
          <div class="col-12">
            <h5 style="margin-bottom: 5px;">{short_name} {f"v{version}" if version else ""}</h5>
            <p style="margin: 2px 0; color: #666; font-style: italic;">{title}</p>
          </div>
        </div>
        <hr style="margin: 8px 0;">
        <div class="row">
          <div class="col-12">
            <p style="margin: 5px 0; font-size: 0.9em;">{abstract}</p>
          </div>
        </div>
        <hr style="margin: 8px 0;">
        <div class="row">
          <div class="col-6">
            <p style="margin: 2px 0;"><b>Provider</b>: {provider}</p>
            <p style="margin: 2px 0;"><b>Concept ID</b>: <code>{concept_id}</code></p>
            <p style="margin: 2px 0;"><b>Temporal</b>: {temporal_str}</p>
          </div>
          <div class="col-6">
            <p style="margin: 2px 0;"><b>Cloud Hosted</b>: {"☁️ Yes" if is_cloud else "🖥️ No"}</p>
            <p style="margin: 2px 0;"><b>DOI</b>: {doi_html}</p>
          </div>
        </div>
        <div class="row" style="margin-top: 10px;">
          <div class="col-12">
            {landing_html} {get_data_html}
          </div>
        </div>
      </div>
    </div>
    """


def _repr_search_results_html(
    results: "SearchResults",
    page: int = 0,
    page_size: int = 20,
) -> str:
    """Generate HTML representation for SearchResults.

    Parameters:
        results: A SearchResults instance
        page: Current page number (0-indexed)
        page_size: Number of items per page

    Returns:
        HTML string for notebook display
    """
    css_styles = _load_css_files()
    css_inline = f"""<div id="{uuid4()}" style="height: 0px; display: none">
            {"".join([f"<style>{style}</style>" for style in css_styles])}
            </div>"""

    # Get counts
    total_hits = results._total_hits
    hits_str = f"{total_hits:,}" if total_hits is not None else "?"
    cached_count = len(results._cached_results)

    # Compute summary if we have cached results and total < 10k
    summary_html = ""
    if cached_count > 0 and (total_hits is None or total_hits < 10000):
        summary = _compute_summary(results._cached_results)
        summary_html = f"""
        <div class="row" style="margin-top: 5px;">
          <div class="col-12" style="font-size: 0.85em; color: #555;">
            <b>Cached Summary</b>: {summary["total_size_mb"]:.1f} MB total |
            {"☁️ " + str(summary["cloud_count"]) + " cloud-hosted" if summary["cloud_count"] > 0 else ""} |
            {summary["temporal_range"]}
          </div>
        </div>
        """

    # Generate table rows for current page
    start_idx = page * page_size
    end_idx = min(start_idx + page_size, cached_count)
    page_items = results._cached_results[start_idx:end_idx]

    table_rows = _generate_table_rows(page_items)

    # Pagination info
    showing_str = f"{start_idx + 1}-{end_idx}" if cached_count > 0 else "0"
    total_pages = (cached_count + page_size - 1) // page_size if cached_count > 0 else 0

    # Determine result type
    result_type = (
        "granules"
        if cached_count == 0 or _is_granule(page_items[0] if page_items else None)
        else "collections"
    )

    return f"""
    {css_inline}
    <div class="bootstrap">
      <div class="container-fluid border" style="padding: 10px;">
        <div class="row">
          <div class="col-12">
            <h5 style="margin-bottom: 5px;">SearchResults</h5>
          </div>
        </div>
        <div class="row">
          <div class="col-4">
            <p style="margin: 2px 0;"><b>Total Hits</b>: {hits_str}</p>
          </div>
          <div class="col-4">
            <p style="margin: 2px 0;"><b>Cached</b>: {cached_count:,} {result_type}</p>
          </div>
          <div class="col-4">
            <p style="margin: 2px 0;"><b>Pages</b>: {total_pages}</p>
          </div>
        </div>
        {summary_html}
        <details style="margin-top: 10px;">
          <summary style="cursor: pointer; padding: 5px; background: #f5f5f5; border-radius: 3px;">
            <b>Show Results</b> (showing {showing_str} of {cached_count:,} cached)
          </summary>
          <div style="margin-top: 10px; max-height: 400px; overflow-y: auto;">
            <table class="table table-sm table-striped" style="font-size: 0.85em;">
              <thead style="position: sticky; top: 0; background: white;">
                <tr>
                  <th>Name</th>
                  <th>Date</th>
                  <th>Size</th>
                  <th>Cloud</th>
                  <th>Link</th>
                </tr>
              </thead>
              <tbody>
                {table_rows}
              </tbody>
            </table>
          </div>
        </details>
      </div>
    </div>
    """


def _generate_table_rows(items: List[Any]) -> str:
    """Generate HTML table rows for a list of results."""
    if not items:
        return "<tr><td colspan='5' style='text-align: center;'>No results cached yet. Iterate over results to populate.</td></tr>"

    rows = []
    for item in items:
        if _is_granule(item):
            row = _granule_row(item)
        else:
            row = _collection_row(item)
        rows.append(row)

    return "\n".join(rows)


def _is_granule(item: Any) -> bool:
    """Check if an item is a DataGranule (vs DataCollection)."""
    if item is None:
        return True  # Default assumption
    # Granules have GranuleUR, collections have ShortName at top level of umm
    return "GranuleUR" in item.get("umm", {})


def _granule_row(granule: "DataGranule") -> str:
    """Generate a table row for a granule."""
    granule_ur = granule.get("umm", {}).get("GranuleUR", "Unknown")
    # Truncate name
    name_display = granule_ur[:45] + "..." if len(granule_ur) > 45 else granule_ur

    # Get temporal
    temporal = granule.get("umm", {}).get("TemporalExtent", {})
    date_str = _format_temporal_extent(temporal, short=True)

    # Size
    size = round(granule.size(), 2)

    # Cloud
    cloud_icon = "☁️" if granule.cloud_hosted else "🖥️"

    # First data link
    data_links = granule.data_links()
    link_html = ""
    if data_links:
        first_link = data_links[0]
        link_html = (
            f'<a href="{first_link}" target="_blank" title="{first_link}">📥</a>'
        )

    return f"""
    <tr>
      <td title="{granule_ur}"><code style="font-size: 0.8em;">{name_display}</code></td>
      <td>{date_str}</td>
      <td>{size} MB</td>
      <td>{cloud_icon}</td>
      <td>{link_html}</td>
    </tr>
    """


def _collection_row(collection: "DataCollection") -> str:
    """Generate a table row for a collection."""
    short_name = collection.get_umm("ShortName") or "Unknown"
    version = collection.version() or ""
    name_display = f"{short_name} v{version}" if version else short_name

    # Truncate if needed
    if len(name_display) > 40:
        name_display = name_display[:37] + "..."

    # Get temporal
    temporal_extents = collection.get_umm("TemporalExtents")
    date_str = _format_collection_temporal(temporal_extents, short=True)

    # Size - collections don't have size, show "-"
    size_str = "-"

    # Cloud
    cloud_info = collection.get_umm("DirectDistributionInformation")
    is_cloud = cloud_info is not None and bool(cloud_info)
    cloud_icon = "☁️" if is_cloud else "🖥️"

    # Landing page link
    landing = collection.landing_page()
    link_html = (
        f'<a href="{landing}" target="_blank" title="{landing}">🔗</a>'
        if landing
        else ""
    )

    return f"""
    <tr>
      <td title="{short_name}"><code style="font-size: 0.8em;">{name_display}</code></td>
      <td>{date_str}</td>
      <td>{size_str}</td>
      <td>{cloud_icon}</td>
      <td>{link_html}</td>
    </tr>
    """


def _compute_summary(items: List[Any]) -> dict:
    """Compute summary statistics for cached results."""
    if not items:
        return {
            "total_size_mb": 0.0,
            "cloud_count": 0,
            "temporal_range": "N/A",
        }

    total_size = 0.0
    cloud_count = 0
    min_date: Optional[str] = None
    max_date: Optional[str] = None

    for item in items:
        if _is_granule(item):
            total_size += item.size()
            if item.cloud_hosted:
                cloud_count += 1
            # Extract date
            temporal = item.get("umm", {}).get("TemporalExtent", {})
            range_dt = temporal.get("RangeDateTime", {})
            begin = range_dt.get("BeginningDateTime")
            end = range_dt.get("EndingDateTime")
            if begin:
                if min_date is None or begin < min_date:
                    min_date = begin
            if end:
                if max_date is None or end > max_date:
                    max_date = end
        else:
            # Collection
            cloud_info = item.get_umm("DirectDistributionInformation")
            if cloud_info:
                cloud_count += 1

    # Format temporal range
    if min_date and max_date:
        min_short = min_date[:10] if len(min_date) >= 10 else min_date
        max_short = max_date[:10] if len(max_date) >= 10 else max_date
        temporal_range = f"{min_short} to {max_short}"
    elif min_date:
        temporal_range = f"{min_date[:10]} to present"
    else:
        temporal_range = "N/A"

    return {
        "total_size_mb": total_size,
        "cloud_count": cloud_count,
        "temporal_range": temporal_range,
    }


def _format_temporal_extent(temporal: dict, short: bool = False) -> str:
    """Format a granule's temporal extent for display."""
    range_dt = temporal.get("RangeDateTime", {})
    begin = range_dt.get("BeginningDateTime", "")
    end = range_dt.get("EndingDateTime", "")

    if short:
        # Just show start date
        if begin:
            return begin[:10] if len(begin) >= 10 else begin
        return "N/A"

    if begin and end:
        begin_short = begin[:10] if len(begin) >= 10 else begin
        end_short = end[:10] if len(end) >= 10 else end
        return f"{begin_short} to {end_short}"
    elif begin:
        begin_short = begin[:10] if len(begin) >= 10 else begin
        return f"{begin_short}"
    return "N/A"


def _format_collection_temporal(
    temporal_extents: Optional[List[dict]], short: bool = False
) -> str:
    """Format a collection's temporal extent for display."""
    if not temporal_extents or not isinstance(temporal_extents, list):
        return "N/A"

    extent = temporal_extents[0] if temporal_extents else {}
    range_dts = extent.get("RangeDateTimes", [])

    if range_dts:
        range_dt = range_dts[0]
        begin = range_dt.get("BeginningDateTime", "")
        end = range_dt.get("EndingDateTime")

        if short:
            if begin:
                return begin[:10] if len(begin) >= 10 else begin
            return "N/A"

        begin_short = begin[:10] if len(begin) >= 10 and begin else ""
        if end:
            end_short = end[:10] if len(end) >= 10 else end
            return f"{begin_short} to {end_short}"
        else:
            return f"{begin_short} to present"

    return "N/A"


__all__ = [
    "_repr_granule_html",
    "_repr_collection_html",
    "_repr_search_results_html",
]
