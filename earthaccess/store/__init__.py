"""Store package for earthaccess data access operations."""

from ..main_store import EarthAccessFile, Store, _open_files
from .credentials import AuthContext, CredentialManager, infer_provider_from_url
from .filesystems import FileSystemFactory

__all__ = [
    "CredentialManager",
    "AuthContext",
    "infer_provider_from_url",
    "FileSystemFactory",
    "Store",
    "EarthAccessFile",
    "_open_files",
]
