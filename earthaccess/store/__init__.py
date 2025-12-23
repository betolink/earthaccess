"""Store package for earthaccess data access operations.

NOTE: This package is being consolidated into store_components to eliminate code duplication.
All store components (CredentialManager, FileSystemFactory, etc.) are now in earthaccess/store_components.
This file provides backward compatibility by re-exporting from store_components and main_store.
"""

from ..main_store import EarthAccessFile, Store, _open_files
from ..store_components import (
    AuthContext,
    CredentialManager,
    FileSystemFactory,
    infer_provider_from_url,
)

__all__ = [
    "CredentialManager",
    "AuthContext",
    "infer_provider_from_url",
    "FileSystemFactory",
    "Store",
    "EarthAccessFile",
    "_open_files",
]
