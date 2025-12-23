"""Asset access and filtering for earthaccess granules.

Provides Asset and AssetFilter dataclasses with comprehensive
filtering capabilities and enhanced granule operations.
"""

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Asset:
    """Immutable representation of a granule asset.

    Encapsulates asset metadata and provides filtering
    capabilities via method chaining patterns.
    """

    href: str
    title: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    roles: Set[str] = field(default_factory=set)

    # Additional metadata fields that might be available
    bands: Optional[List[str]] = None
    gsd: Optional[float] = None
    file_size: Optional[int] = None
    checksum: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate asset data."""
        if not self.href:
            raise ValueError("Asset href is required")

    def has_role(self, role: str) -> bool:
        """Check if asset has a specific role."""
        return role in self.roles

    def is_data(self) -> bool:
        """Check if asset is a data file."""
        data_types = {"data", "science-data"}
        data_roles = {"data", "science-data"}
        if self.type in data_types:
            return True
        if self.roles and any(role in data_roles for role in self.roles):
            return True
        return False

    def is_thumbnail(self) -> bool:
        """Check if asset is a thumbnail."""
        return self.has_role("thumbnail")

    def is_metadata(self) -> bool:
        """Check if asset is metadata."""
        return self.has_role("metadata")

    def is_browse(self) -> bool:
        """Check if asset is browse image."""
        return self.has_role("browse")

    def matches_type(self, asset_type: str) -> bool:
        """Check if asset matches a specific type."""
        return self.type == asset_type

    def matches_types(self, asset_types: List[str]) -> bool:
        """Check if asset matches any of the specified types."""
        return self.type in asset_types

    def with_role(self, role: str) -> "Asset":
        """Return new Asset with additional role (immutable)."""
        return Asset(
            href=self.href,
            title=self.title,
            description=self.description,
            type=self.type,
            bands=self.bands,
            gsd=self.gsd,
            file_size=self.file_size,
            checksum=self.checksum,
            roles=self.roles.union({role}),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert asset to dictionary representation."""
        result = {
            "href": self.href,
            "title": self.title,
            "description": self.description,
            "type": self.type,
            "roles": sorted(list(self.roles)),
        }

        # Add optional fields if present
        if self.bands:
            result["bands"] = self.bands
        if self.gsd is not None:
            result["gsd"] = self.gsd
        if self.file_size is not None:
            result["file_size"] = self.file_size
        if self.checksum:
            result["checksum"] = self.checksum

        return result


@dataclass(frozen=True)
class AssetFilter:
    """Immutable asset filter configuration.

    Provides comprehensive filtering capabilities for granule
    assets with method chaining support.
    """

    # Content type filters
    content_types: Optional[List[str]] = None
    exclude_content_types: Optional[List[str]] = None

    # Role-based filters
    include_roles: Optional[Set[str]] = None
    exclude_roles: Optional[Set[str]] = None

    # Band/spectral filters
    bands: Optional[List[str]] = None
    exclude_bands: Optional[List[str]] = None

    # File size filters
    min_size: Optional[int] = None
    max_size: Optional[int] = None

    # Checksum/quality filters
    checksums: Optional[Dict[str, str]] = None
    exclude_missing_checksum: bool = False

    # Filename pattern filters
    filename_patterns: Optional[List[str]] = None
    exclude_filename_patterns: Optional[List[str]] = None

    def __post_init__(self) -> None:
        """Convert mutable collections to immutable for frozen dataclass."""
        # Convert lists to tuples for immutability
        if self.content_types is not None and isinstance(self.content_types, list):
            object.__setattr__(self, "content_types", tuple(self.content_types))
        if self.exclude_content_types is not None and isinstance(
            self.exclude_content_types, list
        ):
            object.__setattr__(
                self, "exclude_content_types", tuple(self.exclude_content_types)
            )
        if self.bands is not None and isinstance(self.bands, list):
            object.__setattr__(self, "bands", tuple(self.bands))
        if self.exclude_bands is not None and isinstance(self.exclude_bands, list):
            object.__setattr__(self, "exclude_bands", tuple(self.exclude_bands))
        if self.filename_patterns is not None and isinstance(
            self.filename_patterns, list
        ):
            object.__setattr__(self, "filename_patterns", tuple(self.filename_patterns))
        if self.exclude_filename_patterns is not None and isinstance(
            self.exclude_filename_patterns, list
        ):
            object.__setattr__(
                self, "exclude_filename_patterns", tuple(self.exclude_filename_patterns)
            )

    def copy(self, **kwargs: Any) -> "AssetFilter":
        """Create new AssetFilter with updated parameters.

        Args:
            **kwargs: AssetFilter parameters to update

        Returns:
            New AssetFilter instance
        """
        current = self.to_dict()
        current.update(kwargs)

        # Convert lists to sets for roles
        if "include_roles" in current and isinstance(current["include_roles"], list):
            current["include_roles"] = set(current["include_roles"])
        if "exclude_roles" in current and isinstance(current["exclude_roles"], list):
            current["exclude_roles"] = set(current["exclude_roles"])

        return AssetFilter(**current)

    def to_dict(self) -> Dict[str, Any]:
        """Convert filter to dictionary representation."""
        result = {}

        if self.content_types is not None:
            result["content_types"] = self.content_types
        if self.exclude_content_types is not None:
            result["exclude_content_types"] = self.exclude_content_types
        if self.include_roles is not None:
            result["include_roles"] = list(self.include_roles)
        if self.exclude_roles is not None:
            result["exclude_roles"] = list(self.exclude_roles)
        if self.bands is not None:
            result["bands"] = self.bands
        if self.exclude_bands is not None:
            result["exclude_bands"] = self.exclude_bands
        if self.min_size is not None:
            result["min_size"] = self.min_size
        if self.max_size is not None:
            result["max_size"] = self.max_size
        if self.checksums is not None:
            result["checksums"] = self.checksums
        if self.exclude_missing_checksum is not None:
            result["exclude_missing_checksum"] = self.exclude_missing_checksum
        if self.filename_patterns is not None:
            result["filename_patterns"] = self.filename_patterns
        if self.exclude_filename_patterns is not None:
            result["exclude_filename_patterns"] = self.exclude_filename_patterns

        return result

    def content_type_filter(self, content_types: List[str]) -> "AssetFilter":
        """Add content type filter."""
        return self.copy(content_types=list(content_types))

    def exclude_content_type_filter(self, content_types: List[str]) -> "AssetFilter":
        """Add content type exclusion filter."""
        return self.copy(exclude_content_types=list(content_types))

    def role_filter(
        self, include_roles: Set[str] = None, exclude_roles: Set[str] = None
    ) -> "AssetFilter":
        """Add role-based filtering."""
        return self.copy(include_roles=include_roles, exclude_roles=exclude_roles)

    def band_filter(
        self, bands: List[str] = None, exclude_bands: List[str] = None
    ) -> "AssetFilter":
        """Add band-based filtering."""
        return self.copy(bands=bands, exclude_bands=exclude_bands)

    def size_filter(self, min_size: int = None, max_size: int = None) -> "AssetFilter":
        """Add file size filtering."""
        return self.copy(min_size=min_size, max_size=max_size)

    def checksum_filter(
        self, checksums: Dict[str, str] = None, exclude_missing: bool = False
    ) -> "AssetFilter":
        """Add checksum-based filtering."""
        return self.copy(checksums=checksums, exclude_missing_checksum=exclude_missing)

    def filename_filter(
        self, patterns: List[str] = None, exclude_patterns: List[str] = None
    ) -> "AssetFilter":
        """Add filename pattern filtering."""
        return self.copy(
            filename_patterns=patterns, exclude_filename_patterns=exclude_patterns
        )

    def combine(self, other_filter: "AssetFilter") -> "AssetFilter":
        """Combine with another filter (AND logic)."""
        filter1 = self.to_dict()
        filter2 = other_filter.to_dict()

        # Merge filter parameters
        result_dict = {}

        for key in set(filter1.keys()) | set(filter2.keys()):
            values1 = filter1.get(key)
            values2 = filter2.get(key)

            if values1 is not None and values2 is not None:
                # Both present, combine with AND (intersection for sets, list for others)
                if isinstance(values1, set) and isinstance(values2, set):
                    result_dict[key] = list(values1.intersection(values2))
                elif isinstance(values1, (list, tuple)) and isinstance(
                    values2, (list, tuple)
                ):
                    result_dict[key] = list(set(values1) & set(values2))
                else:
                    # For non-iterable types (min_size, max_size, etc.), use the second filter
                    result_dict[key] = values2
            elif values1 is not None:
                result_dict[key] = values1
            elif values2 is not None:
                result_dict[key] = values2
            # If both None, skip

        return AssetFilter(**result_dict)


def filter_assets(
    assets: List[Asset], asset_filter: Optional[AssetFilter] = None
) -> List[Asset]:
    """Apply filter to a list of assets.

    Args:
        assets: List of Asset objects
        asset_filter: Optional AssetFilter to apply

    Returns:
        Filtered list of assets
    """
    if not assets:
        return []

    if asset_filter is None:
        return assets.copy()

    filtered_assets = []
    filter_dict = asset_filter.to_dict()

    for asset in assets:
        if _asset_matches_filter(asset, filter_dict):
            filtered_assets.append(asset)

    logger.debug(f"Filtered {len(filtered_assets)}/{len(assets)} assets")
    return filtered_assets


def _asset_matches_filter(asset: Asset, filter_dict: Dict[str, Any]) -> bool:
    """Check if a single asset matches the filter criteria."""
    # Content type filtering
    if "content_types" in filter_dict:
        content_types = set(filter_dict["content_types"])
        if not asset.matches_types(list(content_types)):
            return False

    if "exclude_content_types" in filter_dict:
        exclude_types = set(filter_dict["exclude_content_types"])
        if asset.matches_types(list(exclude_types)):
            return False

    # Role-based filtering
    if "include_roles" in filter_dict:
        include_roles = filter_dict["include_roles"]
        if not asset.roles.issuperset(include_roles):
            return False

    if "exclude_roles" in filter_dict:
        exclude_roles = filter_dict["exclude_roles"]
        if asset.roles.intersection(exclude_roles):
            return False

    # Band filtering
    if "bands" in filter_dict:
        bands = set(filter_dict["bands"])
        if not (asset.bands and set(asset.bands).issuperset(bands)):
            return False

    if "exclude_bands" in filter_dict:
        exclude_bands = set(filter_dict["exclude_bands"])
        if asset.bands and set(asset.bands) & exclude_bands:
            return False

    # Size filtering
    if "min_size" in filter_dict and asset.file_size is not None:
        if asset.file_size < filter_dict["min_size"]:
            return False

    if "max_size" in filter_dict and asset.file_size is not None:
        if asset.file_size > filter_dict["max_size"]:
            return False

    # Checksum filtering
    if "checksums" in filter_dict and asset.checksum is not None:
        required_checksums = filter_dict["checksums"]
        if asset.checksum not in required_checksums.values():
            return False

    if (
        "exclude_missing_checksum" in filter_dict
        and filter_dict["exclude_missing_checksum"]
    ):
        if asset.checksum is None:
            return False

    # Filename pattern filtering
    if "filename_patterns" in filter_dict:
        import fnmatch

        filename = asset.href.split("/")[-1]  # Extract filename
        if not any(
            fnmatch.fnmatch(filename, pattern)
            for pattern in filter_dict["filename_patterns"]
        ):
            return False

    if "exclude_filename_patterns" in filter_dict:
        import fnmatch

        filename = asset.href.split("/")[-1]
        if any(
            fnmatch.fnmatch(filename, pattern)
            for pattern in filter_dict["exclude_filename_patterns"]
        ):
            return False

    return True


def get_data_assets(assets: List[Asset]) -> List[Asset]:
    """Get all data assets from granule asset list.

    Args:
        assets: List of Asset objects

    Returns:
        List of data assets
    """
    data_filter = AssetFilter(content_types=["data", "science-data"])
    return filter_assets(assets, data_filter)


def get_thumbnail_assets(assets: List[Asset]) -> List[Asset]:
    """Get thumbnail assets.

    Args:
        assets: List of Asset objects

    Returns:
        List of thumbnail assets
    """
    thumbnail_filter = AssetFilter(include_roles=set(["thumbnail"]))
    return filter_assets(assets, thumbnail_filter)


def get_browse_assets(assets: List[Asset]) -> List[Asset]:
    """Get browse assets.

    Args:
        assets: List of Asset objects

    Returns:
        List of browse assets
    """
    browse_filter = AssetFilter(include_roles=set(["browse"]))
    return filter_assets(assets, browse_filter)


def get_assets_by_band(assets: List[Asset], bands: List[str]) -> List[Asset]:
    """Get assets matching specific bands.

    Args:
        assets: List of Asset objects
        bands: List of band names

    Returns:
        List of assets matching the bands
    """
    band_filter = AssetFilter(bands=bands)
    return filter_assets(assets, band_filter)


def get_assets_by_size_range(
    assets: List[Asset], min_size: int, max_size: int
) -> List[Asset]:
    """Get assets within size range.

    Args:
        assets: List of Asset objects
        min_size: Minimum file size in bytes
        max_size: Maximum file size in bytes

    Returns:
        List of assets within size range
    """
    size_filter = AssetFilter(min_size=min_size, max_size=max_size)
    return filter_assets(assets, size_filter)
