"""Static HTML formatters for Jupyter notebook display.

These formatters generate static HTML representations for earthaccess objects.
They work without any optional dependencies and provide a baseline visualization.
"""

from typing import TYPE_CHECKING, Any, List, Optional
from uuid import uuid4

import importlib_resources

STATIC_FILES = ["styles.css"]


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
    from earthaccess.auth.auth import Auth
    from earthaccess.search.results import DataCollection, DataGranule, SearchResults


def _repr_auth_html(auth: "Auth") -> str:
    """Generate HTML representation for Auth status.

    Parameters:
        auth: An Auth instance

    Returns:
        HTML string for notebook display
    """
    css_styles = _load_css_files()
    css_inline = f"""<div id="{uuid4()}" style="height: 0px; display: none">
            {"".join([f"<style>{style}</style>" for style in css_styles])}
            </div>"""

    if not auth.authenticated:
        return f"""
        {css_inline}
        <div class="bootstrap">
          <div class="container-fluid border" style="padding: 15px; max-width: 500px;">
            <div class="row">
              <div class="col-12">
                <h5 style="margin-bottom: 10px; color: #666;">
                  <span style="font-size: 1.2em;">🔒</span> Earthdata Login
                </h5>
                <div style="background: #fff3cd; padding: 10px; border-radius: 5px; border-left: 4px solid #ffc107;">
                  <p style="margin: 0; color: #856404;">
                    <b>Not Authenticated</b><br>
                    <span style="font-size: 0.9em;">Use <code>earthaccess.login()</code> to authenticate.</span>
                  </p>
                </div>
              </div>
            </div>
          </div>
        </div>
        """

    # Get user info
    username = getattr(auth, "username", None)
    strategy = auth._login_strategy or "unknown"
    system_name = "Production" if "urs.earthdata" in auth.system.edl_hostname else "UAT"
    edl_host = auth.system.edl_hostname

    # Get token info
    token = auth.token or {}
    access_token = token.get("access_token", "")
    expiration = token.get("expiration_date", "")

    # Redact token (show last 4 chars)
    if access_token:
        token_display = (
            f"{'•' * 20}...{access_token[-4:]}"
            if len(access_token) > 4
            else "•" * len(access_token)
        )
    else:
        token_display = "N/A"

    # Format expiration date
    if expiration:
        # Parse and format nicely
        try:
            from datetime import datetime

            exp_dt = datetime.fromisoformat(expiration.replace("Z", "+00:00"))
            exp_display = exp_dt.strftime("%Y-%m-%d %H:%M UTC")
            # Check if expired
            now = datetime.now(exp_dt.tzinfo)
            if exp_dt < now:
                exp_status = '<span style="color: #dc3545;">Expired</span>'
            else:
                days_left = (exp_dt - now).days
                if days_left > 7:
                    exp_status = (
                        f'<span style="color: #28a745;">Valid ({days_left} days)</span>'
                    )
                else:
                    exp_status = f'<span style="color: #ffc107;">Expires soon ({days_left} days)</span>'
        except Exception:
            exp_display = expiration
            exp_status = ""
    else:
        exp_display = "N/A (token-based auth)"
        exp_status = ""

    # Strategy icons
    strategy_icons = {
        "netrc": "📁",
        "interactive": "⌨️",
        "environment": "🌍",
    }
    strategy_icon = strategy_icons.get(strategy, "🔑")
    strategy_display = strategy.capitalize()

    # User display
    if username:
        user_display = f"<code>{username}</code>"
    else:
        user_display = '<span style="color: #666; font-style: italic;">Token-based (no username)</span>'

    return f"""
    {css_inline}
    <div class="bootstrap">
      <div class="container-fluid border" style="padding: 15px; max-width: 550px;">
        <div class="row">
          <div class="col-12">
            <h5 style="margin-bottom: 10px;">
              <span style="font-size: 1.2em;">🌍</span> Earthdata Login
              <span class="badge" style="background: #28a745; color: white; font-size: 0.7em; padding: 3px 8px; border-radius: 10px; vertical-align: middle;">Authenticated</span>
            </h5>
          </div>
        </div>
        <hr style="margin: 8px 0;">
        <div class="row">
          <div class="col-6">
            <p style="margin: 4px 0;"><b>User:</b> {user_display}</p>
            <p style="margin: 4px 0;"><b>Strategy:</b> {strategy_icon} {strategy_display}</p>
          </div>
          <div class="col-6">
            <p style="margin: 4px 0;"><b>System:</b> {system_name}</p>
            <p style="margin: 4px 0;"><b>Host:</b> <code style="font-size: 0.85em;">{edl_host}</code></p>
          </div>
        </div>
        <hr style="margin: 8px 0;">
        <div class="row">
          <div class="col-12">
            <details>
              <summary style="cursor: pointer; padding: 5px; background: #f8f9fa; border-radius: 3px;">
                <b>Token Details</b>
              </summary>
              <div style="margin-top: 8px; padding: 10px; background: #f8f9fa; border-radius: 3px; font-size: 0.9em;">
                <p style="margin: 3px 0;"><b>Token:</b> <code>{token_display}</code></p>
                <p style="margin: 3px 0;"><b>Expires:</b> {exp_display} {exp_status}</p>
              </div>
            </details>
          </div>
        </div>
      </div>
    </div>
    """


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
    page_size: int = 20,
) -> str:
    """Generate HTML representation for SearchResults with interactive pagination.

    Parameters:
        results: A SearchResults instance
        page_size: Number of items per page

    Returns:
        HTML string for notebook display with JavaScript pagination
    """
    css_styles = _load_css_files()
    widget_id = str(uuid4()).replace("-", "")[:12]
    css_inline = f"""<div id="css-{widget_id}" style="height: 0px; display: none">
            {"".join([f"<style>{style}</style>" for style in css_styles])}
            </div>"""

    # Get counts
    total_hits = results._total_hits
    hits_str = f"{total_hits:,}" if total_hits is not None else "?"
    cached_count = len(results._cached_results)
    total_pages = (
        max(1, (cached_count + page_size - 1) // page_size) if cached_count > 0 else 1
    )

    # Compute summary if we have cached results and total < 10k
    summary_html = ""
    if cached_count > 0 and (total_hits is None or total_hits < 10000):
        summary = _compute_summary(results._cached_results)
        cloud_str = (
            f"☁️ {summary['cloud_count']} cloud-hosted | "
            if summary["cloud_count"] > 0
            else ""
        )
        summary_html = f"""
        <div class="row" style="margin-top: 5px;">
          <div class="col-12" style="font-size: 0.85em; color: #555;">
            <b>Summary</b>: {summary["total_size_mb"]:.1f} MB total | {cloud_str}{summary["temporal_range"]}
          </div>
        </div>
        """

    # Determine result type from class name or content
    result_class = results.__class__.__name__
    if "Granule" in result_class:
        result_type = "granules"
        result_title = "GranuleResults"
    elif "Collection" in result_class:
        result_type = "collections"
        result_title = "CollectionResults"
    else:
        result_type = (
            "granules"
            if (
                cached_count == 0
                or _is_granule(
                    results._cached_results[0] if results._cached_results else None
                )
            )
            else "collections"
        )
        result_title = "SearchResults"

    # Generate all table rows (we'll paginate client-side with JS)
    all_rows = []
    for idx, item in enumerate(results._cached_results):
        if _is_granule(item):
            row = _granule_row_with_index(item, idx)
        else:
            row = _collection_row_with_index(item, idx)
        all_rows.append(row)

    all_rows_html = (
        "\n".join(all_rows)
        if all_rows
        else "<tr data-idx='0'><td colspan='5' style='text-align: center;'>No results cached yet. Iterate over results to populate.</td></tr>"
    )

    # JavaScript for pagination
    js_code = f"""
    <script>
    (function() {{
        const widgetId = '{widget_id}';
        const pageSize = {page_size};
        const totalItems = {cached_count};
        const totalPages = {total_pages};
        let currentPage = 0;

        function updatePage() {{
            const tbody = document.getElementById('tbody-' + widgetId);
            const rows = tbody.querySelectorAll('tr[data-idx]');
            const startIdx = currentPage * pageSize;
            const endIdx = Math.min(startIdx + pageSize, totalItems);

            rows.forEach(row => {{
                const idx = parseInt(row.getAttribute('data-idx'));
                if (idx >= startIdx && idx < endIdx) {{
                    row.style.display = '';
                }} else {{
                    row.style.display = 'none';
                }}
            }});

            // Update pagination info
            const pageInfo = document.getElementById('pageinfo-' + widgetId);
            if (totalItems > 0) {{
                pageInfo.textContent = 'Page ' + (currentPage + 1) + ' of ' + totalPages + ' (' + (startIdx + 1) + '-' + endIdx + ' of ' + totalItems + ')';
            }} else {{
                pageInfo.textContent = 'No results';
            }}

            // Update button states
            document.getElementById('prev-' + widgetId).disabled = currentPage === 0;
            document.getElementById('next-' + widgetId).disabled = currentPage >= totalPages - 1;
            document.getElementById('first-' + widgetId).disabled = currentPage === 0;
            document.getElementById('last-' + widgetId).disabled = currentPage >= totalPages - 1;
        }}

        window['goFirst_' + widgetId] = function() {{ currentPage = 0; updatePage(); }};
        window['goPrev_' + widgetId] = function() {{ if (currentPage > 0) currentPage--; updatePage(); }};
        window['goNext_' + widgetId] = function() {{ if (currentPage < totalPages - 1) currentPage++; updatePage(); }};
        window['goLast_' + widgetId] = function() {{ currentPage = totalPages - 1; updatePage(); }};

        // Initial page setup
        setTimeout(updatePage, 50);
    }})();
    </script>
    """

    pagination_controls = f"""
    <div style="display: flex; align-items: center; justify-content: space-between; margin-top: 8px; padding: 8px; background: #f8f9fa; border-radius: 3px;">
      <div>
        <button id="first-{widget_id}" onclick="goFirst_{widget_id}()" style="padding: 4px 8px; margin-right: 4px; cursor: pointer;" title="First page">⏮</button>
        <button id="prev-{widget_id}" onclick="goPrev_{widget_id}()" style="padding: 4px 8px; margin-right: 4px; cursor: pointer;" title="Previous page">◀</button>
        <button id="next-{widget_id}" onclick="goNext_{widget_id}()" style="padding: 4px 8px; margin-right: 4px; cursor: pointer;" title="Next page">▶</button>
        <button id="last-{widget_id}" onclick="goLast_{widget_id}()" style="padding: 4px 8px; cursor: pointer;" title="Last page">⏭</button>
      </div>
      <div id="pageinfo-{widget_id}" style="font-size: 0.85em; color: #555;">
        Page 1 of {total_pages}
      </div>
    </div>
    """

    return f"""
    {css_inline}
    <div class="bootstrap" id="widget-{widget_id}">
      <div class="container-fluid border" style="padding: 10px;">
        <div class="row">
          <div class="col-12">
            <h5 style="margin-bottom: 5px;">{result_title}</h5>
          </div>
        </div>
        <div class="row">
          <div class="col-4">
            <p style="margin: 2px 0;"><b>Total in CMR</b>: {hits_str}</p>
          </div>
          <div class="col-4">
            <p style="margin: 2px 0;"><b>Loaded</b>: {cached_count:,} {result_type}</p>
          </div>
          <div class="col-4">
            <p style="margin: 2px 0;"><b>Pages</b>: {total_pages}</p>
          </div>
        </div>
        {summary_html}
        <details style="margin-top: 10px;" open>
          <summary style="cursor: pointer; padding: 5px; background: #f5f5f5; border-radius: 3px;">
            <b>Browse Results</b>
          </summary>
          {pagination_controls}
          <div style="margin-top: 8px; max-height: 400px; overflow-y: auto;">
            <table class="table table-sm table-striped" style="font-size: 0.85em;">
              <thead style="position: sticky; top: 0; background: white; z-index: 1;">
                <tr>
                  <th style="width: 5%;">#</th>
                  <th style="width: 40%;">Name</th>
                  <th style="width: 20%;">Date</th>
                  <th style="width: 15%;">Size</th>
                  <th style="width: 10%;">Cloud</th>
                  <th style="width: 10%;">Link</th>
                </tr>
              </thead>
              <tbody id="tbody-{widget_id}">
                {all_rows_html}
              </tbody>
            </table>
          </div>
        </details>
      </div>
    </div>
    {js_code}
    """


def _granule_row_with_index(granule: "DataGranule", idx: int) -> str:
    """Generate a table row for a granule with index for pagination."""
    granule_ur = granule.get("umm", {}).get("GranuleUR", "Unknown")
    # Truncate name
    name_display = granule_ur[:40] + "..." if len(granule_ur) > 40 else granule_ur

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
    <tr data-idx="{idx}">
      <td style="color: #888;">{idx + 1}</td>
      <td title="{granule_ur}"><code style="font-size: 0.8em;">{name_display}</code></td>
      <td>{date_str}</td>
      <td>{size} MB</td>
      <td>{cloud_icon}</td>
      <td>{link_html}</td>
    </tr>
    """


def _collection_row_with_index(collection: "DataCollection", idx: int) -> str:
    """Generate a table row for a collection with index for pagination."""
    short_name = collection.get_umm("ShortName") or "Unknown"
    version = collection.version() or ""
    name_display = f"{short_name} v{version}" if version else short_name

    # Truncate if needed
    if len(name_display) > 35:
        name_display = name_display[:32] + "..."

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
    <tr data-idx="{idx}">
      <td style="color: #888;">{idx + 1}</td>
      <td title="{short_name}"><code style="font-size: 0.8em;">{name_display}</code></td>
      <td>{date_str}</td>
      <td>{size_str}</td>
      <td>{cloud_icon}</td>
      <td>{link_html}</td>
    </tr>
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
    "_repr_auth_html",
    "_repr_granule_html",
    "_repr_collection_html",
    "_repr_search_results_html",
]
