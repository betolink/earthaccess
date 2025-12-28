"""Internal store package for NASA Earthdata access.

This package provides internal components for data access operations.
Users should NOT import from this package directly - use the top-level
earthaccess API instead:

    >>> import earthaccess
    >>> earthaccess.login()
    >>> granules = earthaccess.search_data(short_name="ATL06", count=5)
    >>> files = earthaccess.open(granules)  # Stream granules
    >>> paths = earthaccess.download(granules, "./data")  # Download granules

Internal Modules:
    store: Store class (internal, used by earthaccess.open/download)
    file_wrapper: EarthAccessFile class and file handling utilities
    download: Download operations for HTTP and S3
    access: S3 access probing and strategy determination

Note:
    The Store class is an internal implementation detail. It is instantiated
    automatically by earthaccess when needed. Users should not create Store
    instances directly.
"""

# Internal Store class (not part of public API, but exported for internal use)
# Export access utilities
from .access import (
    AccessMethod,
    determine_access_method,
    extract_s3_credentials_endpoint,
    get_data_links,
    probe_s3_access,
)

# Export download components
from .download import (
    DEFAULT_CHUNK_SIZE,
    clone_session,
    download_cloud_file,
    download_cloud_granules,
    download_file,
    download_granules,
)

# Export file wrapper components
from .file_wrapper import (
    EarthAccessFile,
    get_url_granule_mapping,
    is_interactive,
    make_instance,
    open_files,
    optimal_block_size,
)
from .store import Store

# Backward compatibility aliases (private names used in legacy code)
_is_interactive = is_interactive
_optimal_fsspec_block_size = optimal_block_size
_open_files = open_files
_get_url_granule_mapping = get_url_granule_mapping

__all__ = [
    # Legacy exports
    "Store",
    # Access utilities
    "AccessMethod",
    "probe_s3_access",
    "determine_access_method",
    "extract_s3_credentials_endpoint",
    "get_data_links",
    # File wrapper exports
    "EarthAccessFile",
    "make_instance",
    "optimal_block_size",
    "is_interactive",
    "open_files",
    "get_url_granule_mapping",
    # Download exports
    "download_file",
    "download_cloud_file",
    "download_granules",
    "download_cloud_granules",
    "clone_session",
    "DEFAULT_CHUNK_SIZE",
    # Backward compatibility (private names)
    "_is_interactive",
    "_optimal_fsspec_block_size",
    "_open_files",
    "_get_url_granule_mapping",
]
