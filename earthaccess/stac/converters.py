"""STAC conversion functions for earthaccess.

This module provides pure functions to convert between CMR UMM format and STAC format.
These functions work with raw dictionaries and are decoupled from the DataGranule
and DataCollection classes for flexibility and testing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from earthaccess.search import DataCollection, DataGranule


# =============================================================================
# Constants
# =============================================================================

CMR_API_BASE_URL = "https://cmr.earthdata.nasa.gov"

# STAC specification versions
STAC_VERSION = "1.0.0"

# Mapping from CMR URL types to STAC asset roles
CMR_URL_TYPE_TO_STAC_ROLE: Dict[str, List[str]] = {
    "GET DATA": ["data"],
    "GET DATA VIA DIRECT ACCESS": ["data"],
    "GET RELATED VISUALIZATION": ["visual"],
    "VIEW RELATED INFORMATION": ["metadata"],
    "USE SERVICE API": ["data", "service"],
    "EXTENDED METADATA": ["metadata"],
    "DOWNLOAD SOFTWARE": ["metadata"],
    "PROJECT HOME PAGE": ["metadata"],
    "DATA SET LANDING PAGE": ["metadata"],
}


# =============================================================================
# UMM to STAC Converters
# =============================================================================


def umm_granule_to_stac_item(
    umm_granule: Dict[str, Any],
    *,
    collection_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Convert a CMR UMM granule to a STAC Item dictionary.

    This is a standalone function that works with raw UMM dictionaries,
    useful for batch processing without creating DataGranule instances.

    Parameters:
        umm_granule: The CMR UMM granule dictionary (with 'umm' and 'meta' keys)
        collection_id: Optional collection ID for the parent link.
            If not provided, attempts to extract from the granule metadata.

    Returns:
        A dictionary representing a STAC Item.

    Example:
        >>> item = umm_granule_to_stac_item(granule_dict, collection_id="C1234-PROVIDER")
        >>> print(item["type"])
        'Feature'
    """
    umm = umm_granule.get("umm", {})
    meta = umm_granule.get("meta", {})

    # Extract identifiers
    granule_ur = umm.get("GranuleUR", "")
    concept_id = meta.get("concept-id", "")
    native_id = meta.get("native-id", granule_ur)
    coll_id = collection_id or meta.get("collection-concept-id", "")
    provider_id = meta.get("provider-id", "")

    # Extract temporal information
    temporal_extent = umm.get("TemporalExtent", {})
    datetime_val, start_datetime, end_datetime = _extract_granule_datetime(
        temporal_extent
    )

    # Extract geometry
    geometry, bbox = _extract_granule_geometry(umm.get("SpatialExtent", {}))

    # Build links
    links = _build_granule_links(concept_id, coll_id, provider_id)

    # Build assets
    assets = _build_granule_assets(umm.get("RelatedUrls", []))

    # Build the STAC Item
    stac_item: Dict[str, Any] = {
        "type": "Feature",
        "stac_version": STAC_VERSION,
        "stac_extensions": [],
        "id": native_id,
        "geometry": geometry,
        "bbox": bbox,
        "properties": {
            "datetime": datetime_val,
            "start_datetime": start_datetime,
            "end_datetime": end_datetime,
            "granule_ur": granule_ur,
            "cmr:concept_id": concept_id,
            "cmr:collection_concept_id": coll_id,
            "cmr:provider_id": provider_id,
        },
        "links": links,
        "assets": assets,
        "collection": coll_id,
    }

    # Add cloud cover if available
    cloud_cover = umm.get("CloudCover")
    if cloud_cover is not None:
        stac_item["properties"]["eo:cloud_cover"] = cloud_cover
        if "eo" not in stac_item["stac_extensions"]:
            stac_item["stac_extensions"].append(
                "https://stac-extensions.github.io/eo/v1.1.0/schema.json"
            )

    return stac_item


def umm_collection_to_stac_collection(
    umm_collection: Dict[str, Any],
) -> Dict[str, Any]:
    """Convert a CMR UMM collection to a STAC Collection dictionary.

    This is a standalone function that works with raw UMM dictionaries,
    useful for batch processing without creating DataCollection instances.

    Parameters:
        umm_collection: The CMR UMM collection dictionary (with 'umm' and 'meta' keys)

    Returns:
        A dictionary representing a STAC Collection.

    Example:
        >>> collection = umm_collection_to_stac_collection(collection_dict)
        >>> print(collection["type"])
        'Collection'
    """
    umm = umm_collection.get("umm", {})
    meta = umm_collection.get("meta", {})

    # Extract identifiers
    short_name = umm.get("ShortName", "")
    concept_id = meta.get("concept-id", "")
    provider_id = meta.get("provider-id", "")
    version = umm.get("Version", "")

    # Extract description
    abstract = umm.get("Abstract", "")
    description = abstract if abstract else f"Collection {short_name}"

    # Extract temporal extent
    temporal_extents = umm.get("TemporalExtents", [])
    start_date, end_date = _extract_collection_temporal_extent(temporal_extents)

    # Extract spatial extent
    spatial_extent = umm.get("SpatialExtent", {})
    bbox = _extract_collection_spatial_extent(spatial_extent)

    # Build links
    links = _build_collection_links(umm_collection, concept_id, provider_id)

    # Build providers
    providers = _build_collection_providers(umm.get("DataCenters", []))

    # Build the STAC Collection
    stac_collection: Dict[str, Any] = {
        "type": "Collection",
        "stac_version": STAC_VERSION,
        "stac_extensions": [],
        "id": concept_id,
        "title": short_name,
        "description": description,
        "license": "proprietary",  # Default, could be extracted from UseConstraints
        "keywords": _extract_keywords(umm),
        "providers": providers,
        "extent": {
            "spatial": {"bbox": [bbox]},
            "temporal": {"interval": [[start_date, end_date]]},
        },
        "links": links,
        "summaries": {},
        # CMR-specific properties
        "cmr:concept_id": concept_id,
        "cmr:short_name": short_name,
        "cmr:version": version,
        "cmr:provider_id": provider_id,
    }

    # Add DOI if available
    doi = umm.get("DOI", {})
    if isinstance(doi, dict) and doi.get("DOI"):
        stac_collection["sci:doi"] = doi["DOI"]
        if "sci" not in stac_collection["stac_extensions"]:
            stac_collection["stac_extensions"].append(
                "https://stac-extensions.github.io/scientific/v1.0.0/schema.json"
            )

    return stac_collection


# =============================================================================
# STAC to UMM Converters (for external STAC sources)
# =============================================================================


def stac_item_to_data_granule(
    stac_item: Dict[str, Any],
    *,
    cloud_hosted: bool = False,
) -> "DataGranule":
    """Convert a STAC Item to a DataGranule instance.

    This function enables earthaccess to work with granules from external
    STAC catalogs (e.g., Microsoft Planetary Computer, Element84).

    Parameters:
        stac_item: A STAC Item dictionary
        cloud_hosted: Whether the data is cloud-hosted

    Returns:
        A DataGranule instance that can be used with earthaccess functions.

    Example:
        >>> from earthaccess.stac import stac_item_to_data_granule
        >>> granule = stac_item_to_data_granule(stac_item, cloud_hosted=True)
        >>> print(granule.data_links())
    """
    # Import here to avoid circular imports
    from earthaccess.search import DataGranule

    # Extract temporal information
    props = stac_item.get("properties", {})
    start_datetime = props.get("start_datetime") or props.get("datetime")
    end_datetime = props.get("end_datetime") or props.get("datetime")

    # Extract spatial information
    geometry = stac_item.get("geometry", {})
    bbox = stac_item.get("bbox", [])

    # Build RelatedUrls from STAC assets
    related_urls = _stac_assets_to_related_urls(stac_item.get("assets", {}))

    # Construct UMM-like structure
    umm_granule: Dict[str, Any] = {
        "umm": {
            "GranuleUR": stac_item.get("id", ""),
            "TemporalExtent": {
                "RangeDateTime": {
                    "BeginningDateTime": start_datetime,
                    "EndingDateTime": end_datetime,
                }
            },
            "SpatialExtent": _geometry_to_spatial_extent(geometry, bbox),
            "RelatedUrls": related_urls,
            "DataGranule": {
                "DayNightFlag": "UNSPECIFIED",
            },
        },
        "meta": {
            "concept-id": props.get("cmr:concept_id", stac_item.get("id", "")),
            "native-id": stac_item.get("id", ""),
            "collection-concept-id": stac_item.get("collection", ""),
            "provider-id": props.get("cmr:provider_id", ""),
        },
    }

    # Add cloud cover if available
    cloud_cover = props.get("eo:cloud_cover")
    if cloud_cover is not None:
        umm_granule["umm"]["CloudCover"] = cloud_cover

    return DataGranule(umm_granule, cloud_hosted=cloud_hosted)


def stac_collection_to_data_collection(
    stac_collection: Dict[str, Any],
    *,
    cloud_hosted: bool = False,
) -> "DataCollection":
    """Convert a STAC Collection to a DataCollection instance.

    This function enables earthaccess to work with collections from external
    STAC catalogs (e.g., Microsoft Planetary Computer, Element84).

    Parameters:
        stac_collection: A STAC Collection dictionary
        cloud_hosted: Whether the data is cloud-hosted

    Returns:
        A DataCollection instance that can be used with earthaccess functions.

    Example:
        >>> from earthaccess.stac import stac_collection_to_data_collection
        >>> collection = stac_collection_to_data_collection(stac_collection)
        >>> print(collection.short_name())
    """
    # Import here to avoid circular imports
    from earthaccess.search import DataCollection

    # Extract extent information
    extent = stac_collection.get("extent", {})
    spatial = extent.get("spatial", {})
    temporal = extent.get("temporal", {})

    # Get bounding box
    bboxes = spatial.get("bbox", [[-180, -90, 180, 90]])
    bbox = bboxes[0] if bboxes else [-180, -90, 180, 90]

    # Get temporal interval
    intervals = temporal.get("interval", [[None, None]])
    interval = intervals[0] if intervals else [None, None]

    # Build RelatedUrls from STAC links
    related_urls = _stac_links_to_related_urls(stac_collection.get("links", []))

    # Construct UMM-like structure
    umm_collection: Dict[str, Any] = {
        "umm": {
            "ShortName": stac_collection.get("title", stac_collection.get("id", "")),
            "Version": stac_collection.get("cmr:version", "1"),
            "Abstract": stac_collection.get("description", ""),
            "DOI": {"DOI": stac_collection.get("sci:doi")}
            if stac_collection.get("sci:doi")
            else {},
            "TemporalExtents": [
                {
                    "RangeDateTimes": [
                        {
                            "BeginningDateTime": interval[0],
                            "EndingDateTime": interval[1],
                        }
                    ]
                }
            ],
            "SpatialExtent": {
                "HorizontalSpatialDomain": {
                    "Geometry": {
                        "BoundingRectangles": [
                            {
                                "WestBoundingCoordinate": bbox[0],
                                "SouthBoundingCoordinate": bbox[1],
                                "EastBoundingCoordinate": bbox[2],
                                "NorthBoundingCoordinate": bbox[3],
                            }
                        ]
                    }
                }
            },
            "RelatedUrls": related_urls,
            "DataCenters": _stac_providers_to_data_centers(
                stac_collection.get("providers", [])
            ),
        },
        "meta": {
            "concept-id": stac_collection.get(
                "cmr:concept_id", stac_collection.get("id", "")
            ),
            "native-id": stac_collection.get("id", ""),
            "provider-id": stac_collection.get("cmr:provider_id", ""),
            "granule-count": 0,  # Unknown for external STAC
        },
    }

    return DataCollection(umm_collection, cloud_hosted=cloud_hosted)


# =============================================================================
# Helper Functions for UMM to STAC
# =============================================================================


def _extract_granule_datetime(
    temporal_extent: Dict[str, Any],
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract datetime, start_datetime, and end_datetime from UMM temporal extent."""
    range_datetime = temporal_extent.get("RangeDateTime", {})
    single_datetime = temporal_extent.get("SingleDateTime")

    if single_datetime:
        return single_datetime, None, None

    begin = range_datetime.get("BeginningDateTime")
    end = range_datetime.get("EndingDateTime")

    if begin and end:
        return None, begin, end
    elif begin:
        return begin, None, None
    elif end:
        return end, None, None
    else:
        return None, None, None


def _extract_granule_geometry(
    spatial_extent: Dict[str, Any],
) -> tuple[Optional[Dict[str, Any]], Optional[List[float]]]:
    """Extract geometry and bbox from UMM spatial extent."""
    horizontal = spatial_extent.get("HorizontalSpatialDomain", {})
    geometry_data = horizontal.get("Geometry", {})

    # Check for bounding rectangles
    bounding_rects = geometry_data.get("BoundingRectangles", [])
    if bounding_rects:
        rect = bounding_rects[0]
        west = rect.get("WestBoundingCoordinate", -180)
        south = rect.get("SouthBoundingCoordinate", -90)
        east = rect.get("EastBoundingCoordinate", 180)
        north = rect.get("NorthBoundingCoordinate", 90)

        bbox = [west, south, east, north]
        geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [west, south],
                    [east, south],
                    [east, north],
                    [west, north],
                    [west, south],
                ]
            ],
        }
        return geometry, bbox

    # Check for GPolygons
    gpolygons = geometry_data.get("GPolygons", [])
    if gpolygons:
        polygon = gpolygons[0]
        boundary = polygon.get("Boundary", {})
        points = boundary.get("Points", [])

        if points:
            coordinates = [[p.get("Longitude"), p.get("Latitude")] for p in points]
            # Close the polygon if not already closed
            if coordinates and coordinates[0] != coordinates[-1]:
                coordinates.append(coordinates[0])

            # Calculate bbox
            lons = [c[0] for c in coordinates if c[0] is not None]
            lats = [c[1] for c in coordinates if c[1] is not None]

            if lons and lats:
                bbox = [min(lons), min(lats), max(lons), max(lats)]
                geometry = {"type": "Polygon", "coordinates": [coordinates]}
                return geometry, bbox

    # Default: global extent
    return None, None


def _build_granule_links(
    concept_id: str,
    collection_id: str,
    provider_id: str,
) -> List[Dict[str, str]]:
    """Build STAC links for a granule."""
    links = []

    if concept_id:
        links.append(
            {
                "rel": "self",
                "href": f"{CMR_API_BASE_URL}/search/concepts/{concept_id}.stac",
                "type": "application/geo+json",
            }
        )

    if collection_id:
        links.append(
            {
                "rel": "parent",
                "href": f"{CMR_API_BASE_URL}/search/concepts/{collection_id}.stac",
                "type": "application/json",
            }
        )
        links.append(
            {
                "rel": "collection",
                "href": f"{CMR_API_BASE_URL}/search/concepts/{collection_id}.stac",
                "type": "application/json",
            }
        )

    links.append(
        {
            "rel": "root",
            "href": f"{CMR_API_BASE_URL}/stac",
            "type": "application/json",
        }
    )

    return links


def _build_granule_assets(
    related_urls: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """Build STAC assets from CMR RelatedUrls."""
    assets: Dict[str, Dict[str, Any]] = {}

    for i, url_info in enumerate(related_urls):
        url = url_info.get("URL", "")
        url_type = url_info.get("Type", "")
        description = url_info.get("Description", "")
        mime_type = url_info.get("MimeType", "")

        if not url:
            continue

        # Generate asset key
        if url_type in ("GET DATA", "GET DATA VIA DIRECT ACCESS"):
            # Extract filename from URL for data assets
            key = url.split("/")[-1] if "/" in url else f"data_{i}"
        else:
            key = f"asset_{i}"

        # Map CMR type to STAC roles
        roles = CMR_URL_TYPE_TO_STAC_ROLE.get(url_type, ["metadata"])

        asset: Dict[str, Any] = {
            "href": url,
            "roles": roles,
        }

        if description:
            asset["title"] = description
        if mime_type:
            asset["type"] = mime_type

        assets[key] = asset

    return assets


def _extract_collection_temporal_extent(
    temporal_extents: List[Dict[str, Any]],
) -> tuple[Optional[str], Optional[str]]:
    """Extract start and end dates from UMM temporal extents."""
    if not temporal_extents:
        return None, None

    extent = temporal_extents[0]

    # Check for RangeDateTimes
    range_date_times = extent.get("RangeDateTimes", [])
    if range_date_times:
        rdt = range_date_times[0]
        return rdt.get("BeginningDateTime"), rdt.get("EndingDateTime")

    # Check for SingleDateTimes
    single_date_times = extent.get("SingleDateTimes", [])
    if single_date_times:
        return single_date_times[0], single_date_times[-1]

    return None, None


def _extract_collection_spatial_extent(
    spatial_extent: Dict[str, Any],
) -> List[float]:
    """Extract bounding box from UMM spatial extent."""
    horizontal = spatial_extent.get("HorizontalSpatialDomain", {})
    geometry = horizontal.get("Geometry", {})

    # Check for bounding rectangles
    bounding_rects = geometry.get("BoundingRectangles", [])
    if bounding_rects:
        rect = bounding_rects[0]
        return [
            rect.get("WestBoundingCoordinate", -180),
            rect.get("SouthBoundingCoordinate", -90),
            rect.get("EastBoundingCoordinate", 180),
            rect.get("NorthBoundingCoordinate", 90),
        ]

    # Default: global extent
    return [-180, -90, 180, 90]


def _build_collection_links(
    umm_collection: Dict[str, Any],
    concept_id: str,
    provider_id: str,
) -> List[Dict[str, str]]:
    """Build STAC links for a collection."""
    links = []

    if concept_id:
        links.append(
            {
                "rel": "self",
                "href": f"{CMR_API_BASE_URL}/search/concepts/{concept_id}.stac",
                "type": "application/json",
            }
        )

    links.append(
        {
            "rel": "root",
            "href": f"{CMR_API_BASE_URL}/stac",
            "type": "application/json",
        }
    )

    # Add landing page from RelatedUrls if available
    related_urls = umm_collection.get("umm", {}).get("RelatedUrls", [])
    for url_info in related_urls:
        if url_info.get("Type") == "DATA SET LANDING PAGE":
            links.append(
                {
                    "rel": "about",
                    "href": url_info.get("URL", ""),
                    "type": "text/html",
                    "title": "Dataset Landing Page",
                }
            )
            break

    return links


def _build_collection_providers(
    data_centers: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Build STAC providers from CMR DataCenters."""
    providers = []

    for dc in data_centers:
        short_name = dc.get("ShortName", "")
        long_name = dc.get("LongName", "")
        roles_raw = dc.get("Roles", [])

        # Map CMR roles to STAC roles
        role_mapping = {
            "ARCHIVER": "host",
            "DISTRIBUTOR": "host",
            "PROCESSOR": "processor",
            "ORIGINATOR": "producer",
        }
        roles = [role_mapping.get(r, "host") for r in roles_raw]
        if not roles:
            roles = ["host"]

        provider: Dict[str, Any] = {
            "name": short_name,
            "roles": list(set(roles)),  # Deduplicate
        }

        if long_name:
            provider["description"] = long_name

        # Extract URL from ContactInformation
        contact_info = dc.get("ContactInformation", {})
        related_urls = contact_info.get("RelatedUrls", [])
        if related_urls:
            provider["url"] = related_urls[0].get("URL", "")

        providers.append(provider)

    return providers


def _extract_keywords(umm: Dict[str, Any]) -> List[str]:
    """Extract keywords from UMM collection."""
    keywords = set()

    # Science keywords
    for sk in umm.get("ScienceKeywords", []):
        if sk.get("Category"):
            keywords.add(sk["Category"])
        if sk.get("Topic"):
            keywords.add(sk["Topic"])
        if sk.get("Term"):
            keywords.add(sk["Term"])

    # Platform names
    for platform in umm.get("Platforms", []):
        if platform.get("ShortName"):
            keywords.add(platform["ShortName"])

    # Instrument names
    for platform in umm.get("Platforms", []):
        for instrument in platform.get("Instruments", []):
            if instrument.get("ShortName"):
                keywords.add(instrument["ShortName"])

    return sorted(keywords)


# =============================================================================
# Helper Functions for STAC to UMM
# =============================================================================


def _stac_assets_to_related_urls(
    assets: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Convert STAC assets to CMR RelatedUrls."""
    related_urls = []

    # Reverse role mapping
    role_to_cmr_type = {
        "data": "GET DATA",
        "visual": "GET RELATED VISUALIZATION",
        "metadata": "VIEW RELATED INFORMATION",
        "thumbnail": "GET RELATED VISUALIZATION",
        "overview": "GET RELATED VISUALIZATION",
    }

    for key, asset in assets.items():
        href = asset.get("href", "")
        roles = asset.get("roles", ["data"])
        title = asset.get("title", "")
        mime_type = asset.get("type", "")

        # Determine CMR type from first role
        cmr_type = "GET DATA"  # Default
        for role in roles:
            if role in role_to_cmr_type:
                cmr_type = role_to_cmr_type[role]
                break

        # Check for S3 URLs
        if href.startswith("s3://"):
            cmr_type = "GET DATA VIA DIRECT ACCESS"

        related_url: Dict[str, Any] = {
            "URL": href,
            "Type": cmr_type,
        }

        if title:
            related_url["Description"] = title
        if mime_type:
            related_url["MimeType"] = mime_type

        related_urls.append(related_url)

    return related_urls


def _stac_links_to_related_urls(
    links: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Convert STAC links to CMR RelatedUrls."""
    related_urls = []

    link_rel_to_cmr_type = {
        "self": "VIEW RELATED INFORMATION",
        "root": "PROJECT HOME PAGE",
        "parent": "VIEW RELATED INFORMATION",
        "child": "VIEW RELATED INFORMATION",
        "item": "VIEW RELATED INFORMATION",
        "about": "DATA SET LANDING PAGE",
        "license": "VIEW RELATED INFORMATION",
        "derived_from": "VIEW RELATED INFORMATION",
    }

    for link in links:
        href = link.get("href", "")
        rel = link.get("rel", "")
        title = link.get("title", "")
        link_type = link.get("type", "")

        cmr_type = link_rel_to_cmr_type.get(rel, "VIEW RELATED INFORMATION")

        related_url: Dict[str, Any] = {
            "URL": href,
            "Type": cmr_type,
        }

        if title:
            related_url["Description"] = title
        if link_type:
            related_url["MimeType"] = link_type

        related_urls.append(related_url)

    return related_urls


def _stac_providers_to_data_centers(
    providers: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Convert STAC providers to CMR DataCenters."""
    data_centers = []

    stac_role_to_cmr = {
        "host": "DISTRIBUTOR",
        "processor": "PROCESSOR",
        "producer": "ORIGINATOR",
        "licensor": "DISTRIBUTOR",
    }

    for provider in providers:
        name = provider.get("name", "")
        description = provider.get("description", "")
        url = provider.get("url", "")
        roles = provider.get("roles", ["host"])

        cmr_roles = [stac_role_to_cmr.get(r, "DISTRIBUTOR") for r in roles]

        data_center: Dict[str, Any] = {
            "ShortName": name,
            "Roles": list(set(cmr_roles)),
        }

        if description:
            data_center["LongName"] = description

        if url:
            data_center["ContactInformation"] = {
                "RelatedUrls": [{"URL": url, "Type": "HOME PAGE"}]
            }

        data_centers.append(data_center)

    return data_centers


def _geometry_to_spatial_extent(
    geometry: Dict[str, Any],
    bbox: List[float],
) -> Dict[str, Any]:
    """Convert GeoJSON geometry to CMR SpatialExtent."""
    spatial_extent: Dict[str, Any] = {"HorizontalSpatialDomain": {"Geometry": {}}}

    geom_type = geometry.get("type", "")
    coordinates = geometry.get("coordinates", [])

    if geom_type == "Polygon" and coordinates:
        # Convert to GPolygon
        points = [
            {"Longitude": coord[0], "Latitude": coord[1]} for coord in coordinates[0]
        ]
        spatial_extent["HorizontalSpatialDomain"]["Geometry"]["GPolygons"] = [
            {"Boundary": {"Points": points}}
        ]
    elif geom_type == "Point" and coordinates:
        # Convert to a point (represented as tiny bounding box in CMR)
        spatial_extent["HorizontalSpatialDomain"]["Geometry"]["Points"] = [
            {"Longitude": coordinates[0], "Latitude": coordinates[1]}
        ]
    elif bbox:
        # Use bounding box
        spatial_extent["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"] = [
            {
                "WestBoundingCoordinate": bbox[0],
                "SouthBoundingCoordinate": bbox[1],
                "EastBoundingCoordinate": bbox[2],
                "NorthBoundingCoordinate": bbox[3],
            }
        ]

    return spatial_extent
