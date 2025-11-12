"""
Unified target filesystem interface for earthaccess.

This module provides a unified interface for writing downloaded files to different
target filesystems including local POSIX paths and cloud storage systems.
"""

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional, Union, IO, BinaryIO
from urllib.parse import urlparse

import fsspec


class TargetFilesystem(ABC):
    """Abstract base class for target filesystem implementations."""

    @abstractmethod
    def open(self, path: str, mode: str = "rb") -> IO:
        """Open a file for reading or writing.

        Args:
            path: File path relative to the target location
            mode: File mode ('r', 'w', 'rb', 'wb', etc.)

        Returns:
            File-like object
        """
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if a file exists.

        Args:
            path: File path relative to the target location

        Returns:
            True if file exists, False otherwise
        """
        pass

    @abstractmethod
    def mkdir(self, path: str, exist_ok: bool = True) -> None:
        """Create a directory.

        Args:
            path: Directory path relative to the target location
            exist_ok: If True, don't raise error if directory already exists
        """
        pass

    @abstractmethod
    def join(self, *paths: str) -> str:
        """Join path components.

        Args:
            *paths: Path components to join

        Returns:
            Joined path string
        """
        pass

    @abstractmethod
    def basename(self, path: str) -> str:
        """Get the basename of a path.

        Args:
            path: File path

        Returns:
            Basename (filename without directory)
        """
        pass


class LocalFilesystem(TargetFilesystem):
    """Local POSIX filesystem implementation."""

    def __init__(self, base_path: str):
        """Initialize local filesystem.

        Args:
            base_path: Base directory path
        """
        self.base_path = Path(base_path).resolve()

    def open(self, path: str, mode: str = "rb") -> IO:
        """Open a local file."""
        full_path = self.base_path / path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        return open(full_path, mode)

    def exists(self, path: str) -> bool:
        """Check if local file exists."""
        full_path = self.base_path / path
        return full_path.exists()

    def mkdir(self, path: str, exist_ok: bool = True) -> None:
        """Create a local directory."""
        full_path = self.base_path / path
        full_path.mkdir(parents=True, exist_ok=exist_ok)

    def join(self, *paths: str) -> str:
        """Join path components for local filesystem."""
        return str(Path(*paths))

    def basename(self, path: str) -> str:
        """Get basename of local path."""
        return Path(path).name


class FsspecFilesystem(TargetFilesystem):
    """Fsspec-based filesystem implementation for cloud storage."""

    def __init__(
        self, base_path: str, storage_options: Optional[Dict[str, Any]] = None
    ):
        """Initialize fsspec filesystem.

        Args:
            base_path: Base path (e.g., 's3://bucket/path')
            storage_options: Storage options for fsspec
        """
        self.base_path = base_path.rstrip("/")
        self.storage_options = storage_options or {}

        # Extract protocol from base_path
        if "://" in base_path:
            self.protocol = base_path.split("://")[0]
        else:
            self.protocol = "file"

        # Create fsspec filesystem
        self.fs = fsspec.filesystem(self.protocol, **self.storage_options)

    def open(self, path: str, mode: str = "rb") -> IO:
        """Open a file using fsspec."""
        full_path = self._get_full_path(path)

        # Ensure parent directory exists
        parent_dir = str(Path(path).parent)
        if parent_dir and parent_dir != ".":
            self.fs.makedirs(self._get_full_path(parent_dir), exist_ok=True)

        return self.fs.open(full_path, mode)

    def exists(self, path: str) -> bool:
        """Check if file exists using fsspec."""
        full_path = self._get_full_path(path)
        return self.fs.exists(full_path)

    def mkdir(self, path: str, exist_ok: bool = True) -> None:
        """Create a directory using fsspec."""
        full_path = self._get_full_path(path)
        self.fs.makedirs(full_path, exist_ok=exist_ok)

    def join(self, *paths: str) -> str:
        """Join path components for cloud filesystem."""
        # For cloud paths, we use simple string joining
        return "/".join(str(p).strip("/") for p in paths if p)

    def basename(self, path: str) -> str:
        """Get basename of cloud path."""
        return path.split("/")[-1] if "/" in path else path

    def _get_full_path(self, path: str) -> str:
        """Get full path including base path."""
        if not path:
            return self.base_path

        path = path.lstrip("/")
        if self.base_path.endswith("/"):
            return self.base_path + path
        else:
            return f"{self.base_path}/{path}"


class TargetLocation:
    """Unified target location that can handle multiple filesystem backends."""

    def __init__(
        self,
        path: Union[str, Path],
        backend: str = "auto",
        storage_options: Optional[Dict[str, Any]] = None,
    ):
        """Initialize target location.

        Args:
            path: Target path (local or cloud URI)
            backend: Filesystem backend ("auto", "local", "fsspec")
            storage_options: Storage options for cloud filesystems
        """
        self.path = str(path)
        self.backend = backend
        self.storage_options = storage_options or {}
        self._filesystem: Optional[TargetFilesystem] = None

    def _detect_backend(self) -> str:
        """Auto-detect appropriate backend based on path."""
        if self.path.startswith(("s3://", "gs://", "az://", "adl://")):
            return "fsspec"
        elif self.path.startswith(("http://", "https://")):
            return "fsspec"
        else:
            return "local"

    def get_filesystem(self) -> TargetFilesystem:
        """Get appropriate filesystem instance."""
        if self._filesystem is None:
            backend = self.backend if self.backend != "auto" else self._detect_backend()

            if backend == "local":
                self._filesystem = LocalFilesystem(self.path)
            elif backend == "fsspec":
                self._filesystem = FsspecFilesystem(self.path, self.storage_options)
            else:
                raise ValueError(f"Unsupported backend: {backend}")

        return self._filesystem

    def is_local(self) -> bool:
        """Check if this is a local filesystem target."""
        return self._detect_backend() == "local"

    def is_cloud(self) -> bool:
        """Check if this is a cloud storage target."""
        return self._detect_backend() == "fsspec"

    def __str__(self) -> str:
        """String representation."""
        return f"TargetLocation({self.path}, backend={self.backend})"

    def __repr__(self) -> str:
        """Detailed string representation."""
        return (
            f"TargetLocation(path='{self.path}', backend='{self.backend}', "
            f"storage_options={self.storage_options})"
        )
