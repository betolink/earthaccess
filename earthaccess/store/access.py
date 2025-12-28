"""Access strategy utilities for earthaccess store.

This module provides functions for determining the best access method
(S3 direct vs HTTPS) for granule data. It implements S3 probing logic
that tests connectivity before committing to an access strategy.

Following SOLID principles, these utilities are pure functions that
can be used by the Store class or independently.
"""

import logging
from enum import Enum
from itertools import chain
from typing import Any, Dict, List, Optional

import fsspec

logger = logging.getLogger(__name__)

__all__ = [
    "AccessMethod",
    "probe_s3_access",
    "determine_access_method",
    "extract_s3_credentials_endpoint",
    "get_data_links",
]


class AccessMethod(Enum):
    """Enum representing data access methods.

    Attributes:
        DIRECT: Access data directly via S3 (requires in-region or credentials).
        EXTERNAL: Access data via HTTPS (works from anywhere).
    """

    DIRECT = "direct"
    EXTERNAL = "external"

    def __str__(self) -> str:
        """Return string value for use with data_links()."""
        return self.value


def probe_s3_access(
    s3_fs: fsspec.AbstractFileSystem,
    s3_url: str,
    *,
    probe_bytes: int = 10,
) -> bool:
    """Probe S3 access by attempting to read a small chunk.

    This function tests whether the provided S3 filesystem can access
    the given URL. It reads a small number of bytes to verify connectivity
    without downloading significant data.

    Parameters:
        s3_fs: An authenticated S3 filesystem.
        s3_url: The S3 URL to probe (s3://bucket/key).
        probe_bytes: Number of bytes to read for probing (default: 10).

    Returns:
        True if access is successful, False otherwise.

    Example:
        >>> s3_fs = get_s3_filesystem(provider="POCLOUD")
        >>> if probe_s3_access(s3_fs, "s3://bucket/file.nc"):
        ...     print("S3 access confirmed")
    """
    if not s3_url:
        return False

    try:
        with s3_fs.open(s3_url, "rb") as f:
            f.read(probe_bytes)
        return True
    except Exception as e:
        logger.debug(f"S3 probe failed for {s3_url}: {e}")
        return False


def determine_access_method(
    granule: Any,
    s3_fs: Optional[fsspec.AbstractFileSystem],
) -> AccessMethod:
    """Determine the best access method for a granule.

    This function checks if a granule is cloud-hosted and whether
    S3 access is available, returning the appropriate access method.

    Parameters:
        granule: A DataGranule instance.
        s3_fs: An authenticated S3 filesystem, or None if not available.

    Returns:
        AccessMethod.DIRECT if S3 access is available and working.
        AccessMethod.EXTERNAL if HTTPS should be used.

    Example:
        >>> access = determine_access_method(granule, s3_fs)
        >>> urls = granule.data_links(access=access.value)
    """
    # Non-cloud granules always use external access
    if not getattr(granule, "cloud_hosted", False):
        return AccessMethod.EXTERNAL

    # No S3 filesystem means external access
    if s3_fs is None:
        return AccessMethod.EXTERNAL

    # Get S3 links and probe
    try:
        s3_links = granule.data_links(access="direct")
        if not s3_links:
            return AccessMethod.EXTERNAL

        # Probe the first link
        if probe_s3_access(s3_fs, s3_links[0]):
            logger.info("Accessing data via S3 (direct access)")
            return AccessMethod.DIRECT
        else:
            return AccessMethod.EXTERNAL

    except Exception as e:
        logger.debug(f"Error determining access method: {e}")
        return AccessMethod.EXTERNAL


def extract_s3_credentials_endpoint(
    related_urls: List[Dict[str, Any]],
) -> Optional[str]:
    """Extract S3 credentials endpoint from RelatedUrls metadata.

    Searches the RelatedUrls list for a URL with Type "USE SERVICE API"
    and Subtype "S3 CREDENTIALS", which indicates an S3 credentials endpoint.

    Parameters:
        related_urls: List of RelatedUrls from granule UMM metadata.

    Returns:
        The S3 credentials endpoint URL, or None if not found.

    Example:
        >>> endpoint = extract_s3_credentials_endpoint(granule["umm"]["RelatedUrls"])
        >>> if endpoint:
        ...     s3_fs = get_s3_filesystem(endpoint=endpoint)
    """
    if not related_urls:
        return None

    for url_info in related_urls:
        url_type = url_info.get("Type", "")
        url_subtype = url_info.get("Subtype", "")

        if url_type == "USE SERVICE API" and url_subtype == "S3 CREDENTIALS":
            return url_info.get("URL")

    return None


def get_data_links(
    granules: List[Any],
    access: str,
) -> List[str]:
    """Get all data links from a list of granules.

    Collects data links from all granules using the specified access type.

    Parameters:
        granules: List of DataGranule instances.
        access: Access type ("direct" for S3, "external" for HTTPS).

    Returns:
        Flat list of all data URLs from the granules.

    Example:
        >>> links = get_data_links(granules, access="direct")
        >>> print(f"Found {len(links)} S3 links")
    """
    return list(
        chain.from_iterable(granule.data_links(access=access) for granule in granules)
    )
