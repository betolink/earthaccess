"""Credential management utilities for earthaccess.

This module provides a centralized credential manager that handles:
- S3 credential caching with automatic expiration
- Provider/DAAC inference from URLs
- Thread-safe credential access

The CredentialManager follows the Single Responsibility Principle by
focusing only on credential lifecycle management.
"""

from __future__ import annotations

import re
import threading
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

if TYPE_CHECKING:
    from earthaccess.auth import Auth


__all__ = [
    "CredentialManager",
    "S3Credentials",
    "CredentialCache",
]


# =============================================================================
# Constants
# =============================================================================

# S3 bucket prefix to provider mapping
# This mapping helps infer which provider hosts the data based on bucket name
BUCKET_PREFIX_TO_PROVIDER: Dict[str, str] = {
    "lp-prod-protected": "LPDAAC",
    "lp-prod-public": "LPDAAC",
    "ornl-cumulus-prod-protected": "ORNL",
    "ornl-cumulus-prod-public": "ORNL",
    "podaac-ops-cumulus-protected": "PODAAC",
    "podaac-ops-cumulus-public": "PODAAC",
    "nsidc-cumulus-prod-protected": "NSIDC",
    "nsidc-cumulus-prod-public": "NSIDC",
    "asf-cumulus-prod-protected": "ASF",
    "ghrc-cumulus-dev": "GHRC",
    "gesdisc-cumulus-prod-protected": "GES_DISC",
}

# Provider to S3 credentials endpoint mapping
PROVIDER_TO_ENDPOINT: Dict[str, str] = {
    "LPDAAC": "https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials",
    "ORNL": "https://data.ornldaac.earthdata.nasa.gov/s3credentials",
    "PODAAC": "https://archive.podaac.earthdata.nasa.gov/s3credentials",
    "NSIDC": "https://data.nsidc.earthdatacloud.nasa.gov/s3credentials",
    "ASF": "https://cumulus.asf.alaska.edu/s3credentials",
    "GHRC": "https://data.ghrc.earthdata.nasa.gov/s3credentials",
    "GES_DISC": "https://data.gesdisc.earthdata.nasa.gov/s3credentials",
}

# Default credential expiration (1 hour minus buffer)
DEFAULT_EXPIRATION_SECONDS = 3600 - 300  # 55 minutes

# Regex pattern for S3 URIs
S3_URI_PATTERN = re.compile(r"s3://([^/]+)/.*")


# =============================================================================
# S3Credentials Dataclass
# =============================================================================


class S3Credentials:
    """Container for S3 temporary credentials.

    Attributes:
        access_key_id: AWS access key ID
        secret_access_key: AWS secret access key
        session_token: AWS session token
        expiration: When these credentials expire
        endpoint: The endpoint these credentials are for
    """

    def __init__(
        self,
        access_key_id: str,
        secret_access_key: str,
        session_token: str,
        expiration: Optional[datetime] = None,
        endpoint: Optional[str] = None,
    ):
        """Initialize S3 credentials.

        Parameters:
            access_key_id: AWS access key ID
            secret_access_key: AWS secret access key
            session_token: AWS session token
            expiration: When these credentials expire (UTC)
            endpoint: The endpoint these credentials are for
        """
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.session_token = session_token
        self.expiration = expiration or datetime.now(timezone.utc) + timedelta(
            seconds=DEFAULT_EXPIRATION_SECONDS
        )
        self.endpoint = endpoint

    @classmethod
    def from_dict(
        cls, data: Dict[str, Any], endpoint: Optional[str] = None
    ) -> "S3Credentials":
        """Create S3Credentials from a dictionary.

        Parameters:
            data: Dictionary with credential fields (from auth.get_s3_credentials)
            endpoint: Optional endpoint URL

        Returns:
            A new S3Credentials instance
        """
        expiration = None
        if "expiration" in data:
            exp_str = data["expiration"]
            if isinstance(exp_str, str):
                # Parse ISO format datetime
                try:
                    expiration = datetime.fromisoformat(exp_str.replace("Z", "+00:00"))
                except ValueError:
                    pass
            elif isinstance(exp_str, datetime):
                expiration = exp_str

        return cls(
            access_key_id=data.get("accessKeyId", ""),
            secret_access_key=data.get("secretAccessKey", ""),
            session_token=data.get("sessionToken", ""),
            expiration=expiration,
            endpoint=endpoint,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for fsspec/boto3.

        Returns:
            Dictionary with credential fields
        """
        return {
            "key": self.access_key_id,
            "secret": self.secret_access_key,
            "token": self.session_token,
        }

    def to_boto3_dict(self) -> Dict[str, str]:
        """Convert to boto3-compatible dictionary.

        Returns:
            Dictionary with boto3 credential parameter names
        """
        return {
            "aws_access_key_id": self.access_key_id,
            "aws_secret_access_key": self.secret_access_key,
            "aws_session_token": self.session_token,
        }

    def is_expired(self, buffer_seconds: int = 300) -> bool:
        """Check if credentials have expired or will expire soon.

        Parameters:
            buffer_seconds: Buffer time before expiration to consider expired

        Returns:
            True if credentials are expired or expiring soon
        """
        if self.expiration is None:
            return False
        buffer = timedelta(seconds=buffer_seconds)
        return datetime.now(timezone.utc) > (self.expiration - buffer)

    def __repr__(self) -> str:
        """Return string representation."""
        return (
            f"S3Credentials(access_key_id='{self.access_key_id[:4]}...', "
            f"expired={self.is_expired()}, endpoint='{self.endpoint}')"
        )


# =============================================================================
# Credential Cache
# =============================================================================


class CredentialCache:
    """Thread-safe cache for S3 credentials.

    This cache stores credentials by endpoint/location and automatically
    removes expired credentials.
    """

    def __init__(self) -> None:
        """Initialize the credential cache."""
        self._cache: Dict[str, S3Credentials] = {}
        self._lock = threading.RLock()

    def get(self, key: str) -> Optional[S3Credentials]:
        """Get credentials from cache if valid.

        Parameters:
            key: Cache key (endpoint URL or provider name)

        Returns:
            S3Credentials if found and valid, None otherwise
        """
        with self._lock:
            creds = self._cache.get(key)
            if creds is None:
                return None
            if creds.is_expired():
                # Remove expired credentials
                del self._cache[key]
                return None
            return creds

    def put(self, key: str, credentials: S3Credentials) -> None:
        """Store credentials in cache.

        Parameters:
            key: Cache key (endpoint URL or provider name)
            credentials: The credentials to cache
        """
        with self._lock:
            self._cache[key] = credentials

    def invalidate(self, key: str) -> None:
        """Remove credentials from cache.

        Parameters:
            key: Cache key to invalidate
        """
        with self._lock:
            self._cache.pop(key, None)

    def clear(self) -> None:
        """Clear all cached credentials."""
        with self._lock:
            self._cache.clear()

    def keys(self) -> List[str]:
        """Get all cache keys.

        Returns:
            List of cache keys
        """
        with self._lock:
            return list(self._cache.keys())

    def __len__(self) -> int:
        """Return number of cached credentials."""
        with self._lock:
            return len(self._cache)


# =============================================================================
# Credential Manager
# =============================================================================


class CredentialManager:
    """Centralized manager for S3 credentials.

    This class handles credential lifecycle including:
    - Fetching credentials from NASA Earthdata
    - Caching credentials with automatic expiration
    - Inferring providers from URLs/buckets
    - Thread-safe access

    Example:
        >>> from earthaccess.credentials import CredentialManager
        >>> manager = CredentialManager(auth)
        >>> creds = manager.get_credentials_for_url("s3://lp-prod-protected/data.h5")
        >>> fs = s3fs.S3FileSystem(**creds.to_dict())
    """

    def __init__(
        self,
        auth: Optional["Auth"] = None,
        fetch_callback: Optional[Callable[..., Dict[str, Any]]] = None,
    ):
        """Initialize the credential manager.

        Parameters:
            auth: An earthaccess Auth instance for fetching credentials
            fetch_callback: Optional custom callback for fetching credentials
        """
        self._auth = auth
        self._fetch_callback = fetch_callback
        self._cache = CredentialCache()

    def get_credentials(
        self,
        *,
        endpoint: Optional[str] = None,
        provider: Optional[str] = None,
        daac: Optional[str] = None,
        force_refresh: bool = False,
    ) -> S3Credentials:
        """Get S3 credentials for a given endpoint, provider, or DAAC.

        At least one of endpoint, provider, or daac must be provided.

        Parameters:
            endpoint: S3 credentials endpoint URL
            provider: Provider name (e.g., "LPDAAC")
            daac: DAAC name (same as provider for most cases)
            force_refresh: If True, bypass cache and fetch new credentials

        Returns:
            S3Credentials for the requested location

        Raises:
            ValueError: If no location specifier is provided
            RuntimeError: If auth is not configured
        """
        # Determine cache key
        cache_key = self._get_cache_key(endpoint=endpoint, provider=provider, daac=daac)

        if cache_key is None:
            raise ValueError(
                "At least one of endpoint, provider, or daac must be provided"
            )

        # Check cache first (unless force refresh)
        if not force_refresh:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached

        # Fetch new credentials
        creds = self._fetch_credentials(endpoint=endpoint, provider=provider, daac=daac)

        # Cache and return
        self._cache.put(cache_key, creds)
        return creds

    def get_credentials_for_url(
        self,
        url: str,
        *,
        force_refresh: bool = False,
    ) -> S3Credentials:
        """Get S3 credentials for a given S3 URL.

        Infers the provider from the bucket prefix and fetches appropriate credentials.

        Parameters:
            url: S3 URL (e.g., "s3://lp-prod-protected/path/to/file.h5")
            force_refresh: If True, bypass cache and fetch new credentials

        Returns:
            S3Credentials for accessing the URL

        Raises:
            ValueError: If provider cannot be inferred from URL
        """
        provider = self.infer_provider_from_url(url)
        if provider is None:
            raise ValueError(f"Cannot infer provider from URL: {url}")

        endpoint = PROVIDER_TO_ENDPOINT.get(provider)
        return self.get_credentials(
            endpoint=endpoint, provider=provider, force_refresh=force_refresh
        )

    def get_credentials_for_bucket(
        self,
        bucket: str,
        *,
        force_refresh: bool = False,
    ) -> S3Credentials:
        """Get S3 credentials for a given bucket.

        Parameters:
            bucket: S3 bucket name (e.g., "lp-prod-protected")
            force_refresh: If True, bypass cache

        Returns:
            S3Credentials for accessing the bucket

        Raises:
            ValueError: If provider cannot be inferred from bucket
        """
        provider = self.infer_provider_from_bucket(bucket)
        if provider is None:
            raise ValueError(f"Cannot infer provider from bucket: {bucket}")

        endpoint = PROVIDER_TO_ENDPOINT.get(provider)
        return self.get_credentials(
            endpoint=endpoint, provider=provider, force_refresh=force_refresh
        )

    def infer_provider_from_url(self, url: str) -> Optional[str]:
        """Infer provider from an S3 URL.

        Parameters:
            url: S3 URL

        Returns:
            Provider name or None if cannot be inferred
        """
        match = S3_URI_PATTERN.match(url)
        if match:
            bucket = match.group(1)
            return self.infer_provider_from_bucket(bucket)
        return None

    def infer_provider_from_bucket(self, bucket: str) -> Optional[str]:
        """Infer provider from bucket name.

        Parameters:
            bucket: S3 bucket name

        Returns:
            Provider name or None if cannot be inferred
        """
        # Check for exact match first
        if bucket in BUCKET_PREFIX_TO_PROVIDER:
            return BUCKET_PREFIX_TO_PROVIDER[bucket]

        # Check for prefix match
        for prefix, provider in BUCKET_PREFIX_TO_PROVIDER.items():
            if bucket.startswith(prefix):
                return provider

        return None

    def invalidate(
        self,
        *,
        endpoint: Optional[str] = None,
        provider: Optional[str] = None,
    ) -> None:
        """Invalidate cached credentials.

        Parameters:
            endpoint: Endpoint to invalidate
            provider: Provider to invalidate
        """
        cache_key = endpoint or provider
        if cache_key:
            self._cache.invalidate(cache_key)

    def clear_cache(self) -> None:
        """Clear all cached credentials."""
        self._cache.clear()

    def _get_cache_key(
        self,
        *,
        endpoint: Optional[str] = None,
        provider: Optional[str] = None,
        daac: Optional[str] = None,
    ) -> Optional[str]:
        """Get cache key for given parameters."""
        if endpoint:
            return endpoint
        if provider:
            return provider
        if daac:
            return daac
        return None

    def _fetch_credentials(
        self,
        *,
        endpoint: Optional[str] = None,
        provider: Optional[str] = None,
        daac: Optional[str] = None,
    ) -> S3Credentials:
        """Fetch credentials from the auth source.

        Parameters:
            endpoint: S3 credentials endpoint URL
            provider: Provider name
            daac: DAAC name

        Returns:
            Fetched S3Credentials

        Raises:
            RuntimeError: If auth is not configured
        """
        # Use custom callback if provided
        if self._fetch_callback is not None:
            creds_dict = self._fetch_callback(
                endpoint=endpoint, provider=provider, daac=daac
            )
            return S3Credentials.from_dict(creds_dict, endpoint=endpoint)

        # Use auth instance
        if self._auth is None:
            raise RuntimeError(
                "Auth instance required to fetch credentials. "
                "Either pass auth to CredentialManager or use a custom fetch_callback."
            )

        # Fetch via auth
        creds_dict = self._auth.get_s3_credentials(
            endpoint=endpoint, provider=provider, daac=daac
        )
        return S3Credentials.from_dict(creds_dict, endpoint=endpoint)

    @property
    def cached_providers(self) -> List[str]:
        """Get list of providers with cached credentials.

        Returns:
            List of cache keys (endpoints or providers)
        """
        return self._cache.keys()

    def __repr__(self) -> str:
        """Return string representation."""
        return f"CredentialManager(cached_providers={len(self._cache)})"


# =============================================================================
# Utility Functions
# =============================================================================


def get_provider_endpoint(provider: str) -> Optional[str]:
    """Get the S3 credentials endpoint for a provider.

    Parameters:
        provider: Provider name (e.g., "LPDAAC")

    Returns:
        Endpoint URL or None if provider is unknown
    """
    return PROVIDER_TO_ENDPOINT.get(provider)


def get_bucket_provider(bucket: str) -> Optional[str]:
    """Get the provider for a bucket.

    Parameters:
        bucket: S3 bucket name

    Returns:
        Provider name or None if cannot be determined
    """
    for prefix, provider in BUCKET_PREFIX_TO_PROVIDER.items():
        if bucket.startswith(prefix):
            return provider
    return None
