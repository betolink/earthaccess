"""Filesystem factory for earthaccess.

Provides centralized filesystem creation with proper credential handling.
Follows SOLID principles with single responsibility for filesystem operations.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

import fsspec
import s3fs

if TYPE_CHECKING:
    from .credentials import CredentialManager

logger = logging.getLogger(__name__)


class FileSystemFactory:
    """Factory for creating filesystem instances with proper authentication.

    Single Responsibility: Create and configure filesystems
    - Handles credential injection via CredentialManager
    - Supports multiple protocols (s3, https, file)
    - Provides consistent interface across protocols
    """

    def __init__(
        self, credential_manager: Optional["CredentialManager"] = None
    ) -> None:
        """Initialize filesystem factory.

        Args:
            credential_manager: CredentialManager for S3 credentials and sessions
        """
        self.credential_manager = credential_manager
        self._fs_cache: Dict[str, Any] = {}
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def get_s3_filesystem(
        self,
        provider: Optional[str] = None,
        credentials: Optional[Dict[str, str]] = None,
        **fs_kwargs: Any,
    ) -> fsspec.AbstractFileSystem:
        """Get authenticated S3 filesystem.

        Args:
            provider: NASA provider code for fetching credentials
            credentials: Pre-fetched credentials dict (skips fetching)
            **fs_kwargs: Additional arguments for s3fs.S3FileSystem

        Returns:
            Configured S3 filesystem

        Raises:
            ValueError: If neither provider nor credentials provided
        """
        s3_creds: Dict[str, Any] = {}

        if credentials:
            s3_creds = credentials
            self._logger.debug("Using provided S3 credentials")
        elif provider and self.credential_manager:
            # Fetch credentials using credential manager
            s3_creds_obj = self.credential_manager.get_credentials(provider)
            s3_creds = s3_creds_obj.to_dict()
            self._logger.debug(f"Fetched S3 credentials for {provider}")
        elif provider:
            # No credential manager available, return anonymous filesystem
            self._logger.debug(
                "Creating anonymous S3 filesystem (no credential manager)"
            )
            s3_creds = {"anon": True}
        else:
            # No provider, no credentials - allow anonymous filesystem
            self._logger.debug(
                "Creating anonymous S3 filesystem (no provider specified)"
            )
            s3_creds = {"anon": True}

        # Merge with additional kwargs
        fs_kwargs.update(s3_creds)

        return s3fs.S3FileSystem(**fs_kwargs)

    def get_https_filesystem(
        self,
        session: Optional[Any] = None,
        **fs_kwargs: Any,
    ) -> fsspec.AbstractFileSystem:
        """Get authenticated HTTPS filesystem.

        Args:
            session: requests Session with authentication cookies
            **fs_kwargs: Additional arguments for fsspec.filesystem

        Returns:
            Configured HTTPS filesystem
        """
        if session:
            fs_kwargs.setdefault("client_kwargs", {}).update({"session": session})
            self._logger.debug("Using provided HTTPS session")
        else:
            # Use credential manager to get session
            if self.credential_manager:
                auth_context = self.credential_manager.get_auth_context()
                session = (
                    auth_context.https_headers if auth_context.https_headers else None
                )
            else:
                session = None

            fs_kwargs.setdefault("client_kwargs", {}).update({"headers": session})
            self._logger.debug("Using HTTPS session headers from credential manager")

        return fsspec.filesystem("https", **fs_kwargs)

    def get_filesystem_for_url(
        self,
        url: str,
        *,
        provider: Optional[str] = None,
        **fs_kwargs: Any,
    ) -> fsspec.AbstractFileSystem:
        """Get appropriate filesystem for a URL.

        Args:
            url: URL or path to access
            provider: Provider for S3 URLs (optional, will infer if needed)
            **fs_kwargs: Additional filesystem arguments

        Returns:
            Configured filesystem for the URL protocol

        Raises:
            ValueError: If URL protocol is unsupported
        """
        # Check cache first
        protocol = url.split("://")[0] if "://" in url else "file"
        cache_items = (protocol, provider, frozenset(fs_kwargs.items()))
        cache_key = f"{cache_items[0]}|{cache_items[1]}|{hash(str(cache_items[2]))}"
        if cache_key in self._fs_cache:
            return self._fs_cache[cache_key]

        if url.startswith("s3://"):
            fs = self.get_s3_filesystem(provider=provider, **fs_kwargs)
        elif url.startswith(("http://", "https://")):
            fs = self.get_https_filesystem(**fs_kwargs)
        elif "://" not in url:
            # Local filesystem
            fs = fsspec.filesystem("file", **fs_kwargs)
        else:
            # Try as fsspec protocol
            protocol = url.split("://")[0]
            try:
                fs = fsspec.filesystem(protocol, **fs_kwargs)
            except Exception as e:
                raise ValueError(
                    f"Unsupported protocol '{protocol}' in URL '{url}': {e}"
                )

        # Cache and return
        self._fs_cache[cache_key] = fs
        return fs

    def clear_cache(self) -> None:
        """Clear filesystem cache."""
        self._fs_cache.clear()
        self._logger.debug("Filesystem cache cleared")

    def cache_info(self) -> Dict[str, Any]:
        """Get information about cached filesystems."""
        protocols = [key.split("|")[0] for key in self._fs_cache.keys()]
        return {
            "cached_filesystems": len(self._fs_cache),
            "protocols": list(set(protocols)),
        }
