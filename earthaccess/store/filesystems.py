"""Filesystem factory for creating authenticated file system instances.

This module provides a factory pattern for creating fsspec and s3fs filesystems
with proper credential management. It abstracts away the details of filesystem
creation, allowing for dependency injection and testing.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import fsspec
import s3fs

from earthaccess.auth.credentials import HTTPHeaders, S3Credentials


class FileSystemFactory(ABC):
    """Abstract base class for creating authenticated filesystems.

    This factory pattern allows for multiple implementations of filesystem
    creation with different credential sources and configurations.
    """

    @abstractmethod
    def create_s3_filesystem(self, credentials: S3Credentials) -> s3fs.S3FileSystem:
        """Create an s3fs.S3FileSystem instance with provided credentials.

        Parameters:
            credentials: S3 credentials containing access key, secret key, and session token.

        Returns:
            An authenticated s3fs.S3FileSystem instance.

        Raises:
            ValueError: If credentials are expired or invalid.
        """
        ...

    @abstractmethod
    def create_https_filesystem(
        self, headers: HTTPHeaders
    ) -> fsspec.AbstractFileSystem:
        """Create an fsspec HTTPS filesystem with provided headers.

        Parameters:
            headers: HTTP headers and cookies for authentication.

        Returns:
            An authenticated fsspec HTTPS filesystem instance.
        """
        ...

    @abstractmethod
    def create_default_filesystem(self) -> fsspec.AbstractFileSystem:
        """Create a default HTTPS filesystem without authentication.

        Returns:
            An fsspec HTTPS filesystem instance without authentication.
        """
        ...


class DefaultFileSystemFactory(FileSystemFactory):
    """Default implementation of FileSystemFactory using s3fs and fsspec.

    This factory creates filesystems using the standard s3fs and fsspec
    libraries with optional credential-based authentication.
    """

    def create_s3_filesystem(self, credentials: S3Credentials) -> s3fs.S3FileSystem:
        """Create an s3fs.S3FileSystem instance with S3Credentials.

        Parameters:
            credentials: S3 credentials containing access key, secret key, and session token.

        Returns:
            An authenticated s3fs.S3FileSystem instance.

        Raises:
            ValueError: If credentials are expired.
        """
        if credentials.is_expired():
            raise ValueError(
                f"S3 credentials expired at {credentials.expiration_time}. "
                "Please refresh your credentials."
            )

        # Convert credentials to s3fs kwargs
        s3_kwargs = credentials.to_dict()

        return s3fs.S3FileSystem(**s3_kwargs)

    def create_https_filesystem(
        self, headers: HTTPHeaders
    ) -> fsspec.AbstractFileSystem:
        """Create an fsspec HTTPS filesystem with HTTP headers.

        Parameters:
            headers: HTTP headers and cookies for authentication.

        Returns:
            An authenticated fsspec HTTPS filesystem instance.
        """
        # Build client kwargs from headers
        client_kwargs: Dict[str, Any] = {
            "headers": headers.headers.copy() if headers.headers else {},
            # Trust env should be False when we have explicit headers
            "trust_env": False,
        }

        # Add cookies if present
        if headers.cookies:
            client_kwargs["cookies"] = headers.cookies.copy()

        return fsspec.filesystem("https", client_kwargs=client_kwargs)

    def create_default_filesystem(self) -> fsspec.AbstractFileSystem:
        """Create a default HTTPS filesystem without authentication.

        Returns:
            An fsspec HTTPS filesystem instance without authentication.
        """
        return fsspec.filesystem("https")


class MockFileSystemFactory(FileSystemFactory):
    """Mock implementation of FileSystemFactory for testing.

    This factory allows for injecting mock or stub filesystems in tests.
    """

    def __init__(
        self,
        s3_filesystem: Optional[s3fs.S3FileSystem] = None,
        https_filesystem: Optional[fsspec.AbstractFileSystem] = None,
        default_filesystem: Optional[fsspec.AbstractFileSystem] = None,
    ) -> None:
        """Initialize mock factory with optional mock filesystems.

        Parameters:
            s3_filesystem: Optional mock S3FileSystem to return.
            https_filesystem: Optional mock HTTPS filesystem to return.
            default_filesystem: Optional mock default filesystem to return.
        """
        self._s3_filesystem = s3_filesystem
        self._https_filesystem = https_filesystem
        self._default_filesystem = default_filesystem

    def create_s3_filesystem(self, credentials: S3Credentials) -> s3fs.S3FileSystem:
        """Return the mock S3 filesystem or raise if not configured.

        Parameters:
            credentials: S3 credentials (not used in mock).

        Returns:
            The configured mock S3FileSystem.

        Raises:
            RuntimeError: If no mock S3FileSystem was configured.
        """
        if self._s3_filesystem is None:
            raise RuntimeError("Mock S3FileSystem not configured")
        return self._s3_filesystem

    def create_https_filesystem(
        self, headers: HTTPHeaders
    ) -> fsspec.AbstractFileSystem:
        """Return the mock HTTPS filesystem or raise if not configured.

        Parameters:
            headers: HTTP headers (not used in mock).

        Returns:
            The configured mock HTTPS filesystem.

        Raises:
            RuntimeError: If no mock HTTPS filesystem was configured.
        """
        if self._https_filesystem is None:
            raise RuntimeError("Mock HTTPS filesystem not configured")
        return self._https_filesystem

    def create_default_filesystem(self) -> fsspec.AbstractFileSystem:
        """Return the mock default filesystem or raise if not configured.

        Returns:
            The configured mock default filesystem.

        Raises:
            RuntimeError: If no mock default filesystem was configured.
        """
        if self._default_filesystem is None:
            raise RuntimeError("Mock default filesystem not configured")
        return self._default_filesystem
