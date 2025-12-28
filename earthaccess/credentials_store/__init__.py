"""Earthaccess store submodule - credential management.

This submodule provides SOLID-compliant credential handling for
supporting both local and distributed execution contexts.

The main Store class remains in earthaccess.store (module level).
"""

from .credentials import (
    AuthContext,
    CredentialManager,
    HTTPHeaders,
    S3Credentials,
)
from .filesystems import (
    DefaultFileSystemFactory,
    FileSystemFactory,
    MockFileSystemFactory,
)
from .streaming import (
    StreamingIterator,
    WorkerContext,
    process_granule_in_worker,
)

__all__ = [
    "S3Credentials",
    "HTTPHeaders",
    "AuthContext",
    "CredentialManager",
    "FileSystemFactory",
    "DefaultFileSystemFactory",
    "MockFileSystemFactory",
    "WorkerContext",
    "StreamingIterator",
    "process_granule_in_worker",
]
