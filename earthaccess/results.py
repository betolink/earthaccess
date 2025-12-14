import json
import uuid
from functools import cache
from typing import Any, Dict, List, Optional, Union

import requests

import earthaccess

from .formatters import _repr_granule_html
from .services import DataServices


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

    def __repr__(self) -> str:
        return json.dumps(
            self.render_dict, sort_keys=False, indent=2, separators=(",", ": ")
        )


class DataGranule(CustomDict):
    """Dictionary-like object to represent a granule from CMR."""

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

    def data_links(
        self, access: Optional[str] = None
    ) -> List[str]:
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
        elif access == "external" or access == "on_prem":
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
