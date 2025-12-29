"""Asset model and filtering for granule data - SOLID principles.

This module provides type-safe, immutable asset objects and flexible filtering
for working with granule files. All classes follow SOLID principles:
- Single Responsibility: Each class has one reason to change
- Frozen dataclasses: Immutable for thread safety
- Open/Closed: Easy to extend filter criteria
- Composable: Filters can be combined
"""

from __future__ import annotations

from dataclasses import dataclass, field
from fnmatch import fnmatch
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class Asset:
    """Immutable representation of a granule asset (file).

    SOLID Principles:
    - Single Responsibility: Represents asset metadata only
    - Frozen dataclass: Thread-safe, immutable, hashable

    Attributes:
        href: URL to the asset (http://, s3://, gs://, etc)
        title: Human-readable name for the asset
        description: Description of the asset's content
        type: Media type (e.g., "image/tiff", "application/x-netcdf")
        roles: List of semantic roles (e.g., ["data"], ["thumbnail"], ["cloud-optimized"])
        size: File size in bytes (optional)
    """

    __module__ = "earthaccess.store"

    href: str
    title: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    roles: List[str] = field(default_factory=list, hash=False)
    size: Optional[int] = None

    def is_data(self) -> bool:
        """Check if this asset has the 'data' role.

        Returns:
            True if asset has 'data' role, False otherwise
        """
        return "data" in self.roles

    def is_thumbnail(self) -> bool:
        """Check if this asset has the 'thumbnail' role.

        Returns:
            True if asset has 'thumbnail' role, False otherwise
        """
        return "thumbnail" in self.roles

    def is_metadata(self) -> bool:
        """Check if this asset has the 'metadata' role.

        Returns:
            True if asset has 'metadata' role, False otherwise
        """
        return "metadata" in self.roles

    def is_cloud_optimized(self) -> bool:
        """Check if this asset is cloud-optimized.

        Returns:
            True if asset has 'cloud-optimized' role, False otherwise
        """
        return "cloud-optimized" in self.roles

    def has_role(self, role: str) -> bool:
        """Check if asset has a specific role.

        Parameters:
            role: Role name to check for

        Returns:
            True if asset has the specified role, False otherwise
        """
        return role in self.roles

    def matches_filter(self, filter: AssetFilter) -> bool:
        """Check if this asset passes the given filter.

        Parameters:
            filter: AssetFilter to apply

        Returns:
            True if asset matches all filter criteria, False otherwise
        """
        return filter.matches(self)


@dataclass(frozen=True)
class AssetFilter:
    """Immutable filter for selecting assets based on various criteria.

    SOLID Principles:
    - Single Responsibility: Filter logic only
    - Frozen dataclass: Thread-safe, immutable
    - Open/Closed: Easy to add new filter criteria
    - Composable: Can combine filters with combine()

    Attributes:
        include_patterns: List of glob patterns to include (e.g., ["*.tif", "*.jp2"])
        exclude_patterns: List of glob patterns to exclude (e.g., ["*_thumbnail*"])
        include_roles: List of roles to include (e.g., ["data", "cloud-optimized"])
        exclude_roles: List of roles to exclude (e.g., ["thumbnail"])
        min_size: Minimum file size in bytes
        max_size: Maximum file size in bytes
    """

    __module__ = "earthaccess.store"

    include_patterns: List[str] = field(default_factory=list)
    exclude_patterns: List[str] = field(default_factory=list)
    include_roles: List[str] = field(default_factory=list)
    exclude_roles: List[str] = field(default_factory=list)
    min_size: Optional[int] = None
    max_size: Optional[int] = None

    def matches(self, asset: Asset) -> bool:
        """Check if an asset passes all filter criteria.

        An asset matches if:
        - Its href matches include_patterns (if any) OR no patterns specified
        - Its href does NOT match exclude_patterns
        - It has at least one include_role (if any) OR no roles specified
        - It does NOT have any exclude_roles
        - Its size >= min_size (if min_size is set and asset has size)
        - Its size <= max_size (if max_size is set and asset has size)

        Parameters:
            asset: Asset to check

        Returns:
            True if asset passes all filter criteria, False otherwise
        """
        # Pattern matching
        if self.include_patterns:
            if not self._matches_any_pattern(asset.href, self.include_patterns):
                return False

        if self.exclude_patterns:
            if self._matches_any_pattern(asset.href, self.exclude_patterns):
                return False

        # Role filtering
        if self.include_roles:
            if not any(role in asset.roles for role in self.include_roles):
                return False

        if self.exclude_roles:
            if any(role in asset.roles for role in self.exclude_roles):
                return False

        # Size filtering
        if asset.size is not None:
            if self.min_size is not None and asset.size < self.min_size:
                return False
            if self.max_size is not None and asset.size > self.max_size:
                return False

        return True

    def combine(self, other: AssetFilter) -> AssetFilter:
        """Combine this filter with another, creating a new filter.

        The combined filter requires both filters to be satisfied:
        - Pattern lists are merged (union)
        - Role lists are merged (union)
        - Size bounds are tightened (intersection)

        Parameters:
            other: Another AssetFilter to combine with this one

        Returns:
            New AssetFilter with combined criteria
        """
        # Merge pattern lists (union)
        include_patterns = list(
            set(self.include_patterns) | set(other.include_patterns)
        )
        exclude_patterns = list(
            set(self.exclude_patterns) | set(other.exclude_patterns)
        )

        # Merge role lists (union)
        include_roles = list(set(self.include_roles) | set(other.include_roles))
        exclude_roles = list(set(self.exclude_roles) | set(other.exclude_roles))

        # Tighten size bounds (intersection)
        min_size = max(filter(None, [self.min_size, other.min_size]), default=None)
        max_size = min(filter(None, [self.max_size, other.max_size]), default=None)

        return AssetFilter(
            include_patterns=include_patterns,
            exclude_patterns=exclude_patterns,
            include_roles=include_roles,
            exclude_roles=exclude_roles,
            min_size=min_size,
            max_size=max_size,
        )

    @staticmethod
    def _matches_any_pattern(filename: str, patterns: List[str]) -> bool:
        """Check if filename matches any glob pattern.

        Parameters:
            filename: Filename or URL to check
            patterns: List of glob patterns

        Returns:
            True if filename matches any pattern, False otherwise
        """
        return any(fnmatch(filename, pattern) for pattern in patterns)

    @classmethod
    def from_dict(cls, filter_dict: Dict[str, Any]) -> AssetFilter:
        """Create filter from simple dictionary for backward compatibility.

        Supports keys like:
        - include_patterns, exclude_patterns
        - include_roles, exclude_roles
        - min_size, max_size

        Parameters:
            filter_dict: Dictionary with filter criteria

        Returns:
            New AssetFilter instance

        Example:
            >>> filter = AssetFilter.from_dict({"include_patterns": ["*.nc"]})
        """
        return cls(
            **{k: v for k, v in filter_dict.items() if k in cls.__dataclass_fields__}
        )


def filter_assets(assets: List[Asset], asset_filter: AssetFilter) -> List[Asset]:
    """Apply filter to a list of assets.

    Parameters:
        assets: List of Asset objects
        asset_filter: AssetFilter to apply

    Returns:
        Filtered list of Asset objects

    Example:
        >>> assets = [Asset(href="file.nc", roles=["data"]), ...]
        >>> filter = AssetFilter(include_patterns=["*.nc"])
        >>> filtered = filter_assets(assets, filter)
    """
    return [asset for asset in assets if asset_filter.matches(asset)]
