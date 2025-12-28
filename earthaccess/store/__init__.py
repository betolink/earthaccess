"""Earthaccess Store package.

This package contains modular components for data access operations:
- file_wrapper: EarthAccessFile class and related utilities
- download: Download operations for HTTP and S3
- _store_legacy: Store class (legacy monolith, being refactored)
"""

# Re-export Store from legacy module for backward compatibility
from ._store_legacy import Store

# Export new modular components
from .download import (
    DEFAULT_CHUNK_SIZE,
    clone_session,
    download_cloud_file,
    download_cloud_granules,
    download_file,
    download_granules,
)
from .file_wrapper import (
    EarthAccessFile,
    get_url_granule_mapping,
    is_interactive,
    make_instance,
    open_files,
    optimal_block_size,
)

# Backward compatibility aliases (private names used in legacy code)
_is_interactive = is_interactive
_optimal_fsspec_block_size = optimal_block_size
_open_files = open_files
_get_url_granule_mapping = get_url_granule_mapping

__all__ = [
    # Legacy exports
    "Store",
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
