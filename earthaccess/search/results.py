"""Result classes for CMR search results.

This module provides the DataCollection, DataGranule, and SearchResults classes
for representing and working with NASA CMR search results.
"""

import json
import uuid
from functools import cache
from typing import Any, Dict, List, Optional, Union

import requests

import earthaccess
from earthaccess.formatting import (
    _repr_collection_html,
    _repr_granule_html,
    _repr_search_results_html,
)
from earthaccess.search.services import DataServices


@cache
def _citation(*, doi: str, format: str, language: str) -> str:
    response = requests.get(
        "https://citation.doi.org/format",
        params={"doi": doi, "style": format, "lang": language},
    )
    response.raise_for_status()
    return response.text


class CustomDict(dict):
    _basic_umm_fields_: List = []
    _basic_meta_fields_: List = []

    def __init__(
        self,
        collection: Dict[str, Any],
        fields: Optional[List[str]] = None,
        cloud_hosted: bool = False,
    ):
        super().__init__(collection)
        self.cloud_hosted = cloud_hosted
        self.uuid = str(uuid.uuid4())

        self.render_dict: Any
        if fields is None:
            self.render_dict = self
        elif fields[0] == "basic":
            self.render_dict = self._filter_fields_(self._basic_umm_fields_)
        else:
            self.render_dict = self._filter_fields_(fields)

    def _filter_fields_(self, fields: List[str]) -> Dict[str, Any]:
        filtered_dict = {
            "umm": dict(
                (field, self["umm"][field]) for field in fields if field in self["umm"]
            )
        }
        basic_dict = {
            "meta": dict(
                (field, self["meta"][field])
                for field in self._basic_meta_fields_
                if field in self["meta"]
            )
        }
        basic_dict.update(filtered_dict)
        return basic_dict

    def _filter_related_links(self, filter: str) -> List[str]:
        """Filter RelatedUrls from the UMM fields on CMR."""
        matched_links: List = []
        if "RelatedUrls" in self["umm"]:
            for link in self["umm"]["RelatedUrls"]:
                if link["Type"] == filter:
                    matched_links.append(link["URL"])
        return matched_links


class DataCollection(CustomDict):
    """Dictionary-like object to represent a data collection from CMR."""

    __module__ = "earthaccess.search"

    _basic_meta_fields_ = [
        "concept-id",
        "granule-count",
        "provider-id",
    ]

    _basic_umm_fields_ = [
        "ShortName",
        "Abstract",
        "SpatialExtent",
        "TemporalExtents",
        "DataCenters",
        "RelatedUrls",
        "ArchiveAndDistributionInformation",
        "DirectDistributionInformation",
    ]

    def summary(self) -> Dict[str, Any]:
        """Summary containing short_name, concept-id, file-type, and cloud-info (if cloud-hosted).

        Returns:
            A summary of the collection metadata.
        """
        # we can print only the concept-id

        summary_dict: Dict[str, Any]
        summary_dict = {
            "short-name": self.get_umm("ShortName"),
            "concept-id": self.concept_id(),
            "version": self.version(),
            "file-type": self.data_type(),
            "get-data": self.get_data(),
        }
        if "Region" in self.s3_bucket():
            summary_dict["cloud-info"] = self.s3_bucket()
        return summary_dict

    def get_umm(self, umm_field: str) -> Union[str, Dict[str, Any]]:
        """Placeholder.

        Parameters:
            umm_field: Valid UMM item, i.e. `TemporalExtent`.

        Returns:
            The value of a given field inside the UMM (Unified Metadata Model).
        """
        return self["umm"].get(umm_field, "")

    def doi(self) -> str | None:
        """Retrieve the Digital Object Identifier (DOI) for this collection.

        Returns:
            This collection's DOI information, or `None`, if it has none.
        """
        doi = self["umm"].get("DOI", {})
        if isinstance(doi, dict):
            return doi.get("DOI", None)
        return None

    def citation(self, *, format: str, language: str) -> str | None:
        """Fetch a formatted citation for this collection using its DOI.

        Parameters:
            format: Citation format style (e.g., 'apa', 'bibtex', 'ris').
                 For a full list of valid formats, visit <https://citation.doi.org/>
            language: Language code (e.g., 'en-US').
                 For a full list of valid language codes, visit <https://citation.doi.org/>

        Returns:
             The formatted citation as a string, or `None`, if this collection does not have a DOI.

        Raises:
             requests.RequestException: if fetching citation information from citations.doi.org failed.
        """
        return (
            None
            if not (doi := self.doi())
            else _citation(doi=doi, format=format, language=language)
        )

    def concept_id(self) -> str:
        """Placeholder.

        Returns:
            A collection's `concept_id`.This id is the most relevant search field on granule queries.
        """
        return self["meta"]["concept-id"]

    def data_type(self) -> str:
        """Placeholder.

        Returns:
            The collection data type, i.e. HDF5, CSV etc., if available.
        """
        return str(
            self["umm"]
            .get("ArchiveAndDistributionInformation", {})
            .get("FileDistributionInformation", "")
        )

    def version(self) -> str:
        """Placeholder.

        Returns:
            The collection's version.
        """
        return self["umm"].get("Version", "")

    def abstract(self) -> str:
        """Placeholder.

        Returns:
            The abstract of a collection.
        """
        return self["umm"].get("Abstract", "")

    def landing_page(self) -> str:
        """Placeholder.

        Returns:
            The first landing page for the collection (can be many), if available.
        """
        links = self._filter_related_links("LANDING PAGE")
        return links[0] if len(links) > 0 else ""

    def get_data(self) -> List[str]:
        """Placeholder.

        Returns:
            The GET DATA links (usually a landing page link, a DAAC portal, or an FTP location).
        """
        return self._filter_related_links("GET DATA")

    def s3_bucket(self) -> Dict[str, Any]:
        """Placeholder.

        Returns:
            The S3 bucket information if the collection has it (**cloud hosted collections only**).
        """
        return self["umm"].get("DirectDistributionInformation", {})

    def services(self) -> Dict[Any, List[Dict[str, Any]]]:
        """Return list of services available for this collection."""
        services = self.get("meta", {}).get("associations", {}).get("services", [])
        queries = (
            DataServices(auth=earthaccess.__auth__).parameters(concept_id=service)
            for service in services
        )

        return {service: query.get_all() for service, query in zip(services, queries)}

    def to_dict(self) -> Dict[str, Any]:
        """Convert the collection to a plain dictionary.

        Returns:
            A dictionary representation of the collection.
        """
        return dict(self)

    def to_stac(self) -> Dict[str, Any]:
        """Convert the CMR UMM collection to a STAC Collection.

        Returns a dictionary representation of a STAC Collection following
        the STAC spec. This can be used with pystac or pystac-client.

        Returns:
            A dictionary representing a STAC Collection.
        """
        # Extract basic metadata
        concept_id = self.concept_id()
        short_name = self.get_umm("ShortName") or concept_id
        version = self.version()
        abstract = self.abstract()

        # Build collection ID
        collection_id = f"{short_name}_v{version}" if version else str(short_name)

        # Extract temporal extent
        temporal_extents = self.get_umm("TemporalExtents")
        temporal_extent = self._extract_temporal_extent(temporal_extents)

        # Extract spatial extent
        spatial_extents = self.get_umm("SpatialExtent")
        spatial_extent = self._extract_spatial_extent(spatial_extents)

        # Build STAC Collection
        stac_collection: Dict[str, Any] = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "stac_extensions": [],
            "id": collection_id,
            "title": str(short_name),
            "description": str(abstract) if abstract else "No description available",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [spatial_extent]},
                "temporal": {"interval": [temporal_extent]},
            },
            "links": self._build_stac_links(),
            "providers": self._build_stac_providers(),
        }

        # Add DOI if available
        doi = self.doi()
        if doi:
            stac_collection["sci:doi"] = doi
            stac_collection["stac_extensions"].append(
                "https://stac-extensions.github.io/scientific/v1.0.0/schema.json"
            )

        # Add CMR-specific properties
        stac_collection["cmr:concept_id"] = concept_id
        stac_collection["cmr:provider_id"] = self["meta"].get("provider-id", "")

        return stac_collection

    def _extract_temporal_extent(self, temporal_extents: Any) -> List[Optional[str]]:
        """Extract temporal extent from UMM TemporalExtents."""
        if not temporal_extents:
            return [None, None]

        if isinstance(temporal_extents, list) and len(temporal_extents) > 0:
            extent = temporal_extents[0]
            if "RangeDateTimes" in extent and extent["RangeDateTimes"]:
                range_dt = extent["RangeDateTimes"][0]
                start = range_dt.get("BeginningDateTime")
                end = range_dt.get("EndingDateTime")
                return [start, end]
            elif "SingleDateTimes" in extent and extent["SingleDateTimes"]:
                single = extent["SingleDateTimes"][0]
                return [single, single]

        return [None, None]

    def _extract_spatial_extent(self, spatial_extent: Any) -> List[float]:
        """Extract spatial extent from UMM SpatialExtent."""
        default_bbox = [-180.0, -90.0, 180.0, 90.0]

        if not spatial_extent:
            return default_bbox

        if isinstance(spatial_extent, dict):
            granule_spatial = spatial_extent.get("HorizontalSpatialDomain", {}).get(
                "Geometry", {}
            )

            bounding_rects = granule_spatial.get("BoundingRectangles", [])
            if bounding_rects:
                rect = bounding_rects[0]
                return [
                    rect.get("WestBoundingCoordinate", -180.0),
                    rect.get("SouthBoundingCoordinate", -90.0),
                    rect.get("EastBoundingCoordinate", 180.0),
                    rect.get("NorthBoundingCoordinate", 90.0),
                ]

        return default_bbox

    def _build_stac_links(self) -> List[Dict[str, str]]:
        """Build STAC links from collection metadata."""
        links: List[Dict[str, str]] = []

        # Self link
        concept_id = self.concept_id()
        links.append(
            {
                "rel": "self",
                "href": f"https://cmr.earthdata.nasa.gov/search/concepts/{concept_id}",
                "type": "application/json",
            }
        )

        # Landing page
        landing = self.landing_page()
        if landing:
            links.append(
                {
                    "rel": "about",
                    "href": landing,
                    "type": "text/html",
                    "title": "Landing Page",
                }
            )

        # Get data links
        for url in self.get_data():
            links.append(
                {
                    "rel": "via",
                    "href": url,
                    "title": "Data Access",
                }
            )

        return links

    def _build_stac_providers(self) -> List[Dict[str, Any]]:
        """Build STAC providers from collection metadata."""
        providers: List[Dict[str, Any]] = []

        provider_id = self["meta"].get("provider-id", "")
        if provider_id:
            providers.append(
                {
                    "name": provider_id,
                    "roles": ["producer", "host"],
                }
            )

        return providers

    def __repr__(self) -> str:
        return json.dumps(
            self.render_dict, sort_keys=False, indent=2, separators=(",", ": ")
        )

    def _repr_html_(self) -> str:
        """Return HTML representation for Jupyter notebook display.

        Returns:
            HTML string with collection metadata card.
        """
        return _repr_collection_html(self)

    def show_map(self, **kwargs):
        """Display an interactive map with the spatial extent of this collection.

        Requires the [widgets] extra: pip install earthaccess[widgets]

        Parameters:
            **kwargs: Additional arguments passed to loneboard (fill_color, line_color)

        Returns:
            A loneboard Map widget for display in Jupyter

        Raises:
            ImportError: If widget dependencies are not installed
            ValueError: If collection has no spatial extent

        Examples:
            >>> collection = earthaccess.search_datasets(short_name="ATL06")[0]
            >>> collection.show_map()  # Display interactive map
        """
        from earthaccess.formatting.widgets import show_collection_map

        return show_collection_map(self, **kwargs)


class DataGranule(CustomDict):
    """Dictionary-like object to represent a granule from CMR."""

    __module__ = "earthaccess.search"

    _basic_meta_fields_ = [
        "concept-id",
        "provider-id",
    ]

    _basic_umm_fields_ = [
        "GranuleUR",
        "SpatialExtent",
        "TemporalExtent",
        "RelatedUrls",
        "DataGranule",
    ]

    def __init__(
        self,
        collection: Dict[str, Any],
        fields: Optional[List[str]] = None,
        cloud_hosted: bool = False,
    ):
        super().__init__(collection)
        self.cloud_hosted = cloud_hosted
        # TODO: maybe add area, start date and all that as an instance value
        self["size"] = self.size()
        self.uuid = str(uuid.uuid4())
        self.render_dict: Any
        if fields is None:
            self.render_dict = self
        elif fields[0] == "basic":
            self.render_dict = self._filter_fields_(self._basic_umm_fields_)
        else:
            self.render_dict = self._filter_fields_(fields)

    def __repr__(self) -> str:
        """Placeholder.

        Returns:
            A basic representation of a data granule.
        """
        data_links = [link for link in self.data_links()]
        rep_str = f"""
        Collection: {self["umm"]["CollectionReference"]}
        Spatial coverage: {self["umm"]["SpatialExtent"]}
        Temporal coverage: {self["umm"]["TemporalExtent"]}
        Size(MB): {self.size()}
        Data: {data_links}\n\n
        """.strip().replace("  ", "")
        return rep_str

    def _repr_html_(self) -> str:
        """Placeholder.

        Returns:
            A rich representation for a data granule if we are in a Jupyter notebook.
        """
        granule_html_repr = _repr_granule_html(self)
        return granule_html_repr

    def show_map(self, **kwargs):
        """Display an interactive map with the bounding box for this granule.

        Requires the [widgets] extra: pip install earthaccess[widgets]

        Parameters:
            **kwargs: Additional arguments passed to loneboard (fill_color, line_color)

        Returns:
            A loneboard Map widget for display in Jupyter

        Raises:
            ImportError: If widget dependencies are not installed
            ValueError: If granule has no spatial extent

        Examples:
            >>> granule = results[0]
            >>> granule.show_map()  # Display interactive map
        """
        from earthaccess.formatting.widgets import show_granule_map

        return show_granule_map(self, **kwargs)

    def __hash__(self) -> int:  # type: ignore[override]
        return hash(self["meta"]["concept-id"])

    def get_s3_credentials_endpoint(self) -> Union[str, None]:
        for link in self["umm"]["RelatedUrls"]:
            if "/s3credentials" in link["URL"]:
                return link["URL"]
        return None

    def size(self) -> float:
        """Placeholder.

        Returns:
            The total size for the granule in MB.
        """
        try:
            data_granule = self["umm"]["DataGranule"]
            total_size = sum(
                [
                    float(s["Size"])
                    for s in data_granule["ArchiveAndDistributionInformation"]
                    if "ArchiveAndDistributionInformation" in data_granule
                ]
            )
        except Exception:
            try:
                data_granule = self["umm"]["DataGranule"]
                total_size = sum(
                    [
                        float(s["SizeInBytes"])
                        for s in data_granule["ArchiveAndDistributionInformation"]
                        if "ArchiveAndDistributionInformation" in data_granule
                    ]
                ) / (1024 * 1024)
            except Exception:
                total_size = 0
        return total_size

    def _derive_s3_link(self, links: List[str]) -> List[str]:
        s3_links = []
        for link in links:
            if link.startswith("s3"):
                s3_links.append(link)
            elif link.startswith("https://") and (
                "cumulus" in link or "protected" in link
            ):
                s3_links.append(f"s3://{links[0].split('nasa.gov/')[1]}")
        return s3_links

    def data_links(self, access: Optional[str] = None) -> List[str]:
        """Returns the data links from a granule.

        Parameters:
            access: direct or external.
                Direct means in-region access for cloud-hosted collections.

        Returns:
            The data links for the requested access type.
        """
        https_links = self._filter_related_links("GET DATA")
        s3_links = self._filter_related_links("GET DATA VIA DIRECT ACCESS")

        if access == "direct":
            return s3_links
        elif access in ("external", "indirect", "on_prem"):
            return https_links
        else:
            # Default behavior: return all links? Or prefer HTTPS?
            # The previous logic was complex and depended on in_region.
            # Now, if we want to let the Store decide, we should probably provide what is asked.
            # If access is None, let's return HTTPS links as they are more universally usable,
            # BUT the Store will explicitly ask for "direct" if it wants to try S3.

            # If the user asks for links without specifying access, they probably want the download links.
            if self.cloud_hosted:
                if len(s3_links) > 0:
                    return s3_links + https_links
                else:
                    return https_links
            return https_links

    def dataviz_links(self) -> List[str]:
        """Placeholder.

        Returns:
            The data visualization links, usually the browse images.
        """
        links = self._filter_related_links("GET RELATED VISUALIZATION")
        return links

    def to_dict(self) -> Dict[str, Any]:
        """Convert the granule to a plain dictionary.

        This is useful for serialization, especially when shipping granule
        metadata to distributed workers.

        Returns:
            A dictionary representation of the granule.
        """
        return dict(self)

    def to_stac(self) -> Dict[str, Any]:
        """Convert the CMR UMM granule to a STAC Item.

        Returns a dictionary representation of a STAC Item following
        the STAC spec. This can be used with pystac or pystac-client.

        Returns:
            A dictionary representing a STAC Item.
        """
        # Extract basic metadata
        concept_id = self["meta"]["concept-id"]
        granule_ur = self["umm"].get("GranuleUR", concept_id)
        collection_ref = self["umm"].get("CollectionReference", {})

        # Get collection ID from reference
        collection_id = collection_ref.get(
            "ShortName", collection_ref.get("EntryTitle", "unknown")
        )
        collection_version = collection_ref.get("Version", "")
        if collection_version:
            collection_id = f"{collection_id}_v{collection_version}"

        # Extract temporal
        temporal_extent = self["umm"].get("TemporalExtent", {})
        datetime_str = self._extract_item_datetime(temporal_extent)
        start_datetime, end_datetime = self._extract_item_datetime_range(
            temporal_extent
        )

        # Extract spatial
        spatial_extent = self["umm"].get("SpatialExtent", {})
        geometry, bbox = self._extract_item_geometry(spatial_extent)

        # Build properties
        properties: Dict[str, Any] = {}

        # Set datetime (STAC requires either datetime or start/end)
        if datetime_str:
            properties["datetime"] = datetime_str
        elif start_datetime:
            properties["datetime"] = None
            properties["start_datetime"] = start_datetime
            properties["end_datetime"] = end_datetime
        else:
            properties["datetime"] = None

        # Add size if available
        if self.size() > 0:
            properties["file:size"] = int(
                self.size() * 1024 * 1024
            )  # Convert MB to bytes

        # Build STAC Item
        stac_item: Dict[str, Any] = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "stac_extensions": [],
            "id": granule_ur,
            "geometry": geometry,
            "bbox": bbox,
            "properties": properties,
            "links": self._build_item_links(collection_id),
            "assets": self._build_item_assets(),
            "collection": collection_id,
        }

        # Add CMR-specific properties
        stac_item["properties"]["cmr:concept_id"] = concept_id
        stac_item["properties"]["cmr:provider_id"] = self["meta"].get("provider-id", "")

        return stac_item

    def _extract_item_datetime(self, temporal_extent: Dict[str, Any]) -> Optional[str]:
        """Extract a single datetime from temporal extent."""
        if "SingleDateTime" in temporal_extent:
            return temporal_extent["SingleDateTime"]

        range_dt = temporal_extent.get("RangeDateTime", {})
        if "BeginningDateTime" in range_dt:
            return range_dt["BeginningDateTime"]

        return None

    def _extract_item_datetime_range(
        self, temporal_extent: Dict[str, Any]
    ) -> tuple[Optional[str], Optional[str]]:
        """Extract datetime range from temporal extent."""
        range_dt = temporal_extent.get("RangeDateTime", {})
        start = range_dt.get("BeginningDateTime")
        end = range_dt.get("EndingDateTime")
        return start, end

    def _extract_item_geometry(
        self, spatial_extent: Dict[str, Any]
    ) -> tuple[Optional[Dict[str, Any]], Optional[List[float]]]:
        """Extract geometry and bbox from spatial extent."""
        h_domain = spatial_extent.get("HorizontalSpatialDomain", {})
        geometry_data = h_domain.get("Geometry", {})

        # Try bounding rectangles first
        bounding_rects = geometry_data.get("BoundingRectangles", [])
        if bounding_rects:
            rect = bounding_rects[0]
            west = rect.get("WestBoundingCoordinate", -180.0)
            south = rect.get("SouthBoundingCoordinate", -90.0)
            east = rect.get("EastBoundingCoordinate", 180.0)
            north = rect.get("NorthBoundingCoordinate", 90.0)

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

        # Try GPolygons
        gpolygons = geometry_data.get("GPolygons", [])
        if gpolygons:
            polygon = gpolygons[0]
            boundary = polygon.get("Boundary", {}).get("Points", [])
            if boundary:
                coords = [
                    [p.get("Longitude", 0), p.get("Latitude", 0)] for p in boundary
                ]
                # Close the polygon if not closed
                if coords and coords[0] != coords[-1]:
                    coords.append(coords[0])

                # Calculate bbox
                lons = [c[0] for c in coords]
                lats = [c[1] for c in coords]
                bbox = [min(lons), min(lats), max(lons), max(lats)]

                geometry = {"type": "Polygon", "coordinates": [coords]}
                return geometry, bbox

        # Try points
        points = geometry_data.get("Points", [])
        if points:
            point = points[0]
            lon = point.get("Longitude", 0)
            lat = point.get("Latitude", 0)
            geometry = {"type": "Point", "coordinates": [lon, lat]}
            bbox = [lon, lat, lon, lat]
            return geometry, bbox

        # Default to global extent
        return None, None

    def _build_item_links(self, collection_id: str) -> List[Dict[str, str]]:
        """Build STAC links for the item."""
        links: List[Dict[str, str]] = []

        # Self link
        concept_id = self["meta"]["concept-id"]
        links.append(
            {
                "rel": "self",
                "href": f"https://cmr.earthdata.nasa.gov/search/concepts/{concept_id}",
                "type": "application/json",
            }
        )

        # Parent collection link
        links.append(
            {
                "rel": "parent",
                "href": f"#/collections/{collection_id}",
                "type": "application/json",
            }
        )

        # Collection link
        links.append(
            {
                "rel": "collection",
                "href": f"#/collections/{collection_id}",
                "type": "application/json",
            }
        )

        return links

    def _extract_asset_key(self, url: str, url_type: str = "") -> str:
        """Extract a meaningful asset key from a URL.

        This method attempts to derive a meaningful name from the URL by:
        1. Extracting the filename from the URL
        2. Removing the granule ID prefix if present (to get band/layer suffix)
        3. Handling special cases like thumbnails via the URL type

        Parameters:
            url: The URL to extract the key from
            url_type: The CMR URL type (e.g., "GET DATA", "GET RELATED VISUALIZATION")

        Returns:
            A meaningful asset key (e.g., "B02", "Fmask", "thumbnail")
        """
        # Extract filename from URL
        filename = url.split("/")[-1] if "/" in url else url

        # Remove file extension for cleaner keys
        known_extensions = (
            ".tif",
            ".tiff",
            ".nc",
            ".nc4",
            ".h5",
            ".hdf",
            ".he5",
            ".zarr",
            ".png",
            ".jpg",
            ".jpeg",
            ".xml",
            ".json",
        )
        base_name = filename
        for ext in known_extensions:
            if filename.lower().endswith(ext):
                base_name = filename[: -len(ext)]
                break

        # Get granule ID for prefix removal
        granule_id = self["umm"].get("GranuleUR", "")

        # Try to extract a suffix after the granule ID (common pattern for multi-band data)
        # E.g., "HLS.L30.T10SEG.2023001T185019.v2.0.B02.tif" -> "B02"
        if granule_id and base_name.startswith(granule_id):
            suffix = base_name[len(granule_id) :]
            # Remove leading dots or underscores
            suffix = suffix.lstrip("._-")
            if suffix:
                return suffix

        # For visualization links, use "browse" as the key when filename matches granule ID
        # This prevents browse images from being named "data"
        if url_type == "GET RELATED VISUALIZATION":
            if base_name == granule_id or not base_name:
                return "browse"
            return base_name

        # If filename matches granule ID, use "data" as the key
        if base_name == granule_id:
            return "data"

        # Otherwise, use the base filename as the key
        return base_name if base_name else "data"

    def _infer_media_type(self, url: str) -> Optional[str]:
        """Infer MIME type from file extension.

        Parameters:
            url: The URL to infer media type from

        Returns:
            MIME type string or None if unknown
        """
        url_lower = url.lower()
        if url_lower.endswith(".nc") or url_lower.endswith(".nc4"):
            return "application/x-netcdf"
        elif url_lower.endswith(".tif") or url_lower.endswith(".tiff"):
            return "image/tiff; application=geotiff"
        elif (
            url_lower.endswith(".hdf")
            or url_lower.endswith(".h5")
            or url_lower.endswith(".he5")
        ):
            return "application/x-hdf5"
        elif url_lower.endswith(".zarr"):
            return "application/vnd+zarr"
        elif url_lower.endswith(".png"):
            return "image/png"
        elif url_lower.endswith(".jpg") or url_lower.endswith(".jpeg"):
            return "image/jpeg"
        return None

    def _build_item_assets(self) -> Dict[str, Dict[str, Any]]:
        """Build STAC assets from granule data links.

        This method creates STAC-compatible asset dictionaries with meaningful
        keys derived from filenames (e.g., "B02", "Fmask" for HLS data).
        S3 and HTTPS versions of the same file are grouped together, with
        the preferred access method as primary href and the other as alternate.
        """
        assets: Dict[str, Dict[str, Any]] = {}

        # Group URLs by their asset key
        # Key: asset_key, Value: {"s3": url, "https": url}
        asset_groups: Dict[str, Dict[str, str]] = {}

        # Process data links
        if "RelatedUrls" in self["umm"]:
            for url_info in self["umm"]["RelatedUrls"]:
                url = url_info.get("URL", "")
                url_type = url_info.get("Type", "")

                if not url:
                    continue

                # Only process data links here
                if url_type not in ("GET DATA", "GET DATA VIA DIRECT ACCESS"):
                    continue

                asset_key = self._extract_asset_key(url, url_type)

                if asset_key not in asset_groups:
                    asset_groups[asset_key] = {}

                if url.startswith("s3://"):
                    asset_groups[asset_key]["s3"] = url
                else:
                    asset_groups[asset_key]["https"] = url

        # Build assets from groups
        for asset_key, urls in asset_groups.items():
            # Prefer S3 for cloud-hosted data, HTTPS otherwise
            if self.cloud_hosted and "s3" in urls:
                primary_url = urls["s3"]
                alternate_url = urls.get("https")
            else:
                primary_url = urls.get("https") or urls.get("s3", "")
                alternate_url = urls.get("s3") if "https" in urls else None

            asset: Dict[str, Any] = {
                "href": primary_url,
                "roles": ["data"],
            }

            # Add cloud-optimized role for S3 links
            if primary_url.startswith("s3://"):
                asset["roles"].append("cloud-optimized")

            # Add HTTPS as alternate if we have both
            if alternate_url:
                asset["alternate"] = {"href": alternate_url}

            # Infer media type
            media_type = self._infer_media_type(primary_url)
            if media_type:
                asset["type"] = media_type

            assets[asset_key] = asset

        # Add browse/thumbnail assets
        viz_links = self.dataviz_links()
        for link in viz_links:
            asset_key = self._extract_asset_key(link, "GET RELATED VISUALIZATION")

            # Handle duplicate keys by appending number
            if asset_key in assets:
                counter = 1
                while f"{asset_key}_{counter}" in assets:
                    counter += 1
                asset_key = f"{asset_key}_{counter}"

            assets[asset_key] = {
                "href": link,
                "roles": ["thumbnail"],
                "type": "image/png" if link.endswith(".png") else "image/jpeg",
            }

        return assets

    def assets(self) -> List[Any]:
        """Get all assets from this granule as Asset objects.

        Returns a list of Asset objects representing all granule files,
        including data files, thumbnails, and metadata. Assets are created
        from STAC-style asset dictionaries built by _build_item_assets().

        Returns:
            List of Asset objects representing granule files

        Example:
            >>> granule = results[0]
            >>> all_assets = granule.assets()
            >>> data_assets = [a for a in all_assets if a.is_data()]
        """
        from earthaccess.store.assets import Asset

        assets_dict = self._build_item_assets()
        return [
            Asset(
                href=asset.get("href", ""),
                title=asset.get("title"),
                description=asset.get("description"),
                type=asset.get("type"),
                roles=asset.get("roles", []),
                size=asset.get("size"),
            )
            for asset in assets_dict.values()
        ]

    def data_assets(self) -> List[Any]:
        """Get only data assets (those with 'data' role).

        Convenience method for filtering assets to only those with the 'data' role.
        Excludes thumbnails, browse images, and other metadata assets.

        Returns:
            List of Asset objects with 'data' role only

        Example:
            >>> granule = results[0]
            >>> data_files = granule.data_assets()
            >>> print(f"Found {len(data_files)} data files")
        """
        return [a for a in self.assets() if a.is_data()]


# =============================================================================
# SearchResults - Lazy Pagination Wrapper
# =============================================================================


class SearchResults:
    """Base class for CMR search results with lazy pagination.

    This class provides an interface for iterating through large result sets
    without loading all results into memory at once. It supports both direct
    iteration and page-by-page iteration.

    SearchResults fetches data lazily from NASA's CMR (Common Metadata Repository),
    only requesting pages of results as they are accessed. This enables efficient
    processing of large result sets that may contain thousands of items.

    Attributes:
        query: The CMR query object (DataGranules or DataCollections)
        limit: Maximum number of results to fetch (None for unlimited)

    Examples:
        Iterate through results:

        >>> results = earthaccess.search_data(short_name="ATL06", count=100)
        >>> for granule in results:  # doctest: +SKIP
        ...     print(granule["meta"]["concept-id"])

        Check how many results are currently loaded:

        >>> print(f"Loaded {len(results)} granules")  # doctest: +SKIP

        Get total matching results in CMR:

        >>> print(f"Total available: {results.total()}")  # doctest: +SKIP

        Access by index (lazy fetches as needed):

        >>> first = results[0]  # doctest: +SKIP
        >>> fifth = results[4]  # doctest: +SKIP

        Convert to list (fetches all results up to limit):

        >>> granule_list = list(results)  # doctest: +SKIP
    """

    __module__ = "earthaccess.search"

    def __init__(self, query: Any, limit: Optional[int] = None) -> None:
        """Initialize SearchResults.

        Parameters:
            query: The CMR query object that will be used to fetch results
            limit: Maximum number of results to fetch, None for unlimited
        """
        self.query = query
        self.limit = limit
        self._cached_results: List[Union[DataGranule, DataCollection]] = []
        self._total_hits: Optional[int] = None
        self._exhausted = False
        self._last_search_after: Optional[str] = None

    def total(self) -> int:
        """Return the total number of results matching the query in CMR.

        This makes a request to CMR to get the hit count if not already cached.
        Use this to know how many total results exist before fetching them all.

        Returns:
            The total number of results matching the query

        Examples:
            >>> results = earthaccess.search_data(short_name="ATL06", count=100)
            >>> print(f"Total matching: {results.total()}")  # doctest: +SKIP
        """
        if self._total_hits is None:
            self._total_hits = self.query.hits()
        return self._total_hits

    def hits(self) -> int:
        """Return the total number of results matching the query in CMR.

        .. deprecated::
            Use :meth:`total` instead. This method will be removed in a future version.

        Returns:
            The total number of results matching the query
        """
        import warnings

        warnings.warn(
            "hits() is deprecated, use total() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.total()

    def __len__(self) -> int:
        """Return the number of currently cached/materialized results.

        This returns the count of results that have been fetched so far,
        not the total number of matching results in CMR. Use `total()`
        to get the total number of matching results.

        Returns:
            The number of results currently loaded in memory
        """
        return len(self._cached_results)

    def __iter__(self):
        """Iterate through all results, fetching pages as needed.

        This enables direct iteration:
            for granule in search_results:
                print(granule)

        Yields:
            DataGranule or DataCollection instances
        """
        # If we've already cached all results, use the cache
        if self._exhausted:
            yield from self._cached_results
            return

        # Otherwise, fetch pages as needed
        page_size = 2000
        search_after = self._last_search_after

        # First yield already cached results
        for result in self._cached_results:
            yield result

        results_yielded = len(self._cached_results)

        while not self._exhausted:
            # Check if we've hit the limit
            if self.limit and results_yielded >= self.limit:
                self._exhausted = True
                break

            # Fetch next page
            page = self._fetch_page(page_size, search_after)

            if not page:
                self._exhausted = True
                break

            # Check if this is a partial page (CMR returns fewer results than requested)
            # We'll mark as exhausted AFTER processing this page
            is_last_page = len(page) < page_size

            for result in page:
                # Check limit
                if self.limit and results_yielded >= self.limit:
                    self._exhausted = True
                    break

                self._cached_results.append(result)
                results_yielded += 1
                yield result

            # Mark exhausted after processing the last page
            if is_last_page:
                self._exhausted = True
                break

            # Get search_after header for next page
            search_after = self._last_search_after

    def pages(self):
        """Iterate through results page by page.

        Each page is a list of results, allowing for batch processing.
        Pages are fetched lazily from the CMR.

        Yields:
            List[DataGranule] or List[DataCollection]: A page of results
        """
        page_size = 2000
        search_after = None
        results_fetched = 0

        while True:
            # Check if we've hit the limit
            if self.limit and results_fetched >= self.limit:
                break

            # Fetch next page
            page = self._fetch_page(
                min(page_size, self.limit - results_fetched)
                if self.limit
                else page_size,
                search_after,
            )

            if not page:
                break

            results_fetched += len(page)
            yield page

            # Check if this is a partial page
            if len(page) < page_size:
                break

            # Get search_after header for next page
            search_after = self._last_search_after

    def _fetch_page(
        self, page_size: int, search_after: Optional[str] = None
    ) -> List[Union[DataGranule, DataCollection]]:
        """Fetch a single page of results from CMR.

        Parameters:
            page_size: Number of results to fetch
            search_after: Pagination token for retrieving subsequent pages

        Returns:
            A list of DataGranule or DataCollection instances
        """
        url = self.query._build_url()
        headers = dict(self.query.headers or {})

        if search_after:
            headers["cmr-search-after"] = search_after

        # Use getattr to safely access session (available on DataGranules/DataCollections subclasses)
        session = getattr(self.query, "session", requests.session())
        response = session.get(url, headers=headers, params={"page_size": page_size})

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            raise RuntimeError(ex.response.text) from ex

        # Store search_after for next page
        if cmr_search_after := response.headers.get("cmr-search-after"):
            self._last_search_after = cmr_search_after

        # Store total hits
        if self._total_hits is None and (hits := response.headers.get("CMR-Hits")):
            self._total_hits = int(hits)

        results_data = response.json().get("items", [])

        return self._convert_results(results_data)

    def _convert_results(
        self, results_data: List[Dict[str, Any]]
    ) -> List[Union[DataGranule, DataCollection]]:
        """Convert raw CMR response data to result objects.

        Override in subclasses for type-specific conversion.

        Parameters:
            results_data: Raw JSON items from CMR response

        Returns:
            List of DataGranule or DataCollection instances
        """
        # Import here to avoid circular imports
        from earthaccess.search.queries import DataGranules

        if isinstance(self.query, DataGranules):
            cloud = len(results_data) > 0 and self.query._is_cloud_hosted(
                results_data[0]
            )
            return [DataGranule(item, cloud_hosted=cloud) for item in results_data]
        else:
            return [DataCollection(item) for item in results_data]

    def __getitem__(
        self, index: Union[int, slice]
    ) -> Union[DataGranule, DataCollection, List[Union[DataGranule, DataCollection]]]:
        """Get item(s) by index or slice.

        Supports both single indexing and slicing. Results are fetched lazily
        as needed to satisfy the request.

        Parameters:
            index: Integer index or slice object

        Returns:
            Single result for integer index, list of results for slice

        Raises:
            IndexError: If index is out of range

        Examples:
            >>> results = earthaccess.search_data(short_name="ATL06", count=100)
            >>> first = results[0]  # Get first result
            >>> fifth = results[4]  # Get fifth result (fetches if needed)
            >>> batch = results[0:10]  # Get first 10 results
        """
        # Handle slices
        if isinstance(index, slice):
            # For slices, we need to ensure we have enough results
            # Use limit or a reasonable max if unbounded
            max_index = (
                index.stop if index.stop is not None else (self.limit or self.total())
            )
            self._ensure_cached(max_index)
            return self._cached_results[index]

        # Handle negative indices - need to know total to resolve
        if index < 0:
            total = self.total()
            index = total + index
            if index < 0:
                raise IndexError("Index out of range")

        # Ensure we have cached up to this index
        self._ensure_cached(index + 1)

        if index >= len(self._cached_results):
            raise IndexError(
                f"Index {index} out of range for {len(self._cached_results)} cached results"
            )

        return self._cached_results[index]

    def _ensure_cached(self, count: int) -> None:
        """Ensure at least `count` results are cached.

        Fetches additional pages as needed to cache the requested number
        of results.

        Parameters:
            count: Minimum number of results to cache
        """
        if len(self._cached_results) >= count:
            return

        if self._exhausted:
            return

        # Apply limit constraint
        if self.limit:
            count = min(count, self.limit)

        # Fetch results until we have enough
        for _ in self:
            if len(self._cached_results) >= count or self._exhausted:
                break

    def __repr__(self) -> str:
        """String representation of SearchResults."""
        total = self._total_hits if self._total_hits is not None else "?"
        return f"{self.__class__.__name__}(total={total}, loaded={len(self._cached_results)})"

    def _repr_html_(self) -> str:
        """Return HTML representation for Jupyter notebook display.

        Returns:
            HTML string with collapsible results table.
        """
        return _repr_search_results_html(self)

    def summary(self) -> dict:
        """Compute aggregated metadata for cached results.

        Only computes detailed statistics if total_hits < 10,000 to avoid
        performance issues with large result sets.

        Returns:
            Dictionary containing:
            - total: Total number of matching results in CMR
            - loaded: Number of results currently cached
            - total_size_mb: Total size of cached granules in MB (granules only)
            - cloud_count: Number of cloud-hosted results
            - temporal_range: Date range of cached results (if available)

        Examples:
            >>> results = earthaccess.search_data(short_name="ATL06", count=100)
            >>> summary = results.summary()
            >>> print(f"Total size: {summary['total_size_mb']:.1f} MB")
        """
        cached_count = len(self._cached_results)
        total_hits = self._total_hits

        # Basic info always available
        result: dict = {
            "total": total_hits,
            "loaded": cached_count,
            "total_size_mb": 0.0,
            "cloud_count": 0,
            "temporal_range": None,
        }

        # Only compute detailed stats if reasonable number of results
        if cached_count == 0 or (total_hits is not None and total_hits >= 10000):
            return result

        # Compute detailed statistics
        total_size = 0.0
        cloud_count = 0
        min_date: Optional[str] = None
        max_date: Optional[str] = None

        for item in self._cached_results:
            # Check if granule (has size method) or collection
            if hasattr(item, "size") and callable(item.size):
                total_size += item.size()

            if getattr(item, "cloud_hosted", False):
                cloud_count += 1
            else:
                # For collections, check DirectDistributionInformation
                cloud_info = item.get("umm", {}).get("DirectDistributionInformation")
                if cloud_info:
                    cloud_count += 1

            # Extract temporal info
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

        # Format temporal range
        if min_date and max_date:
            min_short = min_date[:10] if len(min_date) >= 10 else min_date
            max_short = max_date[:10] if len(max_date) >= 10 else max_date
            result["temporal_range"] = f"{min_short} to {max_short}"
        elif min_date:
            result["temporal_range"] = f"{min_date[:10]} to present"

        result["total_size_mb"] = total_size
        result["cloud_count"] = cloud_count

        return result

    def to_stac(self) -> List[Dict[str, Any]]:
        """Convert all cached results to STAC format.

        Converts each cached search result to its STAC representation.
        For granules, returns STAC Items. For collections, returns STAC Collections.

        Note: This only converts currently cached results. To convert all matching
        results, iterate over the SearchResults first to cache them.

        Returns:
            List of STAC-compatible dictionaries (Items or Collections)

        Examples:
            >>> results = earthaccess.search_data(short_name="ATL06", count=10)
            >>> stac_items = results.to_stac()
            >>> print(len(stac_items))  # 10 STAC Items
            >>> print(stac_items[0]["type"])  # "Feature"

            >>> collections = earthaccess.search_datasets(keyword="temperature")
            >>> stac_collections = collections.to_stac()
            >>> print(stac_collections[0]["type"])  # "Collection"
        """
        return [item.to_stac() for item in self._cached_results]

    def show_map(self, max_items: int = 10000, **kwargs):
        """Display an interactive map with bounding boxes for cached results.

        This method creates a loneboard map visualization showing the spatial
        extent of cached search results. Requires the [widgets] extra.

        Parameters:
            max_items: Maximum number of bounding boxes to display (default 10000)
            **kwargs: Additional arguments passed to loneboard (fill_color, line_color)

        Returns:
            A loneboard Map widget for display in Jupyter

        Raises:
            ImportError: If widget dependencies are not installed
            ValueError: If no results are cached

        Examples:
            >>> results = earthaccess.search_data(short_name="ATL06", count=100)
            >>> list(results)  # Fetch results first
            >>> results.show_map()  # Display interactive map
        """
        from earthaccess.formatting.widgets import show_map

        return show_map(self, max_items=max_items, **kwargs)


class GranuleResults(SearchResults):
    """Search results containing DataGranule objects.

    This subclass is returned by `earthaccess.search_data()` and provides
    granule-specific functionality.

    Examples:
        >>> results = earthaccess.search_data(short_name="ATL06", count=10)
        >>> for granule in results:
        ...     print(granule.data_links())
    """

    __module__ = "earthaccess.search"

    def _convert_results(self, results_data: List[Dict[str, Any]]) -> List[DataGranule]:
        """Convert raw CMR response data to DataGranule objects."""
        cloud = len(results_data) > 0 and self.query._is_cloud_hosted(results_data[0])
        return [DataGranule(item, cloud_hosted=cloud) for item in results_data]

    def __repr__(self) -> str:
        """String representation of GranuleResults."""
        total = self._total_hits if self._total_hits is not None else "?"
        return f"GranuleResults(total={total}, loaded={len(self._cached_results)})"


class CollectionResults(SearchResults):
    """Search results containing DataCollection objects.

    This subclass is returned by `earthaccess.search_datasets()` and provides
    collection-specific functionality.

    Examples:
        >>> results = earthaccess.search_datasets(keyword="temperature", count=10)
        >>> for collection in results:
        ...     print(collection.concept_id())
    """

    __module__ = "earthaccess.search"

    def _convert_results(
        self, results_data: List[Dict[str, Any]]
    ) -> List[DataCollection]:
        """Convert raw CMR response data to DataCollection objects."""
        return [DataCollection(item) for item in results_data]

    def __repr__(self) -> str:
        """String representation of CollectionResults."""
        total = self._total_hits if self._total_hits is not None else "?"
        return f"CollectionResults(total={total}, loaded={len(self._cached_results)})"
