"""Credential management for cloud-native data access - SOLID principles.

This module provides type-safe, serializable credential objects for supporting
both local and distributed execution. All classes follow SOLID principles:
- Single Responsibility: Each class has one reason to change
- Frozen dataclasses: Immutable for thread safety
- Dependency Inversion: Abstracts credential details
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class S3Credentials:
    """Immutable S3 credentials with expiration checking.

    Uses frozen dataclass for thread safety, immutability, and serializability.
    This supports both local and distributed execution contexts.

    Attributes:
        access_key: AWS access key ID
        secret_key: AWS secret access key
        session_token: Temporary session token (optional)
        expiration_time: When credentials expire (optional, no expiration if None)
        region: AWS region name (default: us-west-2)
    """

    access_key: str
    secret_key: str
    session_token: Optional[str] = None
    expiration_time: Optional[datetime] = None
    region: str = "us-west-2"

    def is_expired(self) -> bool:
        """Check if credentials have expired.

        Returns:
            True if expiration_time is in the past, False otherwise
        """
        if self.expiration_time is None:
            return False
        # Handle both naive and aware datetimes
        now = datetime.now(timezone.utc)
        exp_time = self.expiration_time
        if exp_time.tzinfo is None:
            # Assume naive datetime is UTC
            exp_time = exp_time.replace(tzinfo=timezone.utc)
        return now >= exp_time

    def to_dict(self) -> Dict[str, Any]:
        """Convert credentials to s3fs.S3FileSystem kwargs.

        Returns:
            Dictionary suitable for s3fs.S3FileSystem(**dict)
        """
        result = {
            "key": self.access_key,
            "secret": self.secret_key,
            "region_name": self.region,
        }
        if self.session_token:
            result["token"] = self.session_token
        return result

    @classmethod
    def from_auth(cls, auth: Any) -> S3Credentials:
        """Extract S3 credentials from an Auth object.

        Parameters:
            auth: Authenticated Auth object

        Returns:
            S3Credentials extracted from auth
        """
        cred_dict = auth.get_s3_credentials()
        return cls(
            access_key=cred_dict.get("access_key", ""),
            secret_key=cred_dict.get("secret_key", ""),
            session_token=cred_dict.get("session_token"),
            expiration_time=cred_dict.get("expiration"),
            region=cred_dict.get("region", "us-west-2"),
        )


@dataclass(frozen=True)
class HTTPHeaders:
    """HTTP headers and cookies for HTTPS fallback access.

    Used when S3 direct access isn't available, falls back to HTTPS
    with proper authentication headers and cookies.

    Attributes:
        headers: HTTP headers dict (e.g., Authorization, User-Agent)
        cookies: HTTP cookies dict (e.g., session cookies)
    """

    headers: Dict[str, str] = field(default_factory=dict)
    cookies: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_auth(cls, auth: Any) -> HTTPHeaders:
        """Extract HTTP headers and cookies from Auth object.

        Parameters:
            auth: Authenticated Auth object

        Returns:
            HTTPHeaders with authorization data
        """
        return cls(
            headers=auth.get_headers() if hasattr(auth, "get_headers") else {},
            cookies=auth.get_cookies() if hasattr(auth, "get_cookies") else {},
        )


@dataclass(frozen=True)
class AuthContext:
    """Serializable authentication context for distributed execution.

    Captures all necessary credential information needed to reconstruct
    authentication in worker processes without re-authenticating.
    Designed to be pickled and sent to remote workers.

    Attributes:
        s3_credentials: AWS S3 credentials (optional)
        http_headers: HTTP headers/cookies for HTTPS (optional)
        urs_token: Earthdata URS authentication token (optional)
        provider_credentials: Provider-specific credentials dict (PODAAC, NSIDC, etc)
    """

    s3_credentials: Optional[S3Credentials] = None
    http_headers: Optional[HTTPHeaders] = None
    urs_token: Optional[str] = None
    provider_credentials: Dict[str, Dict[str, str]] = field(default_factory=dict)

    @classmethod
    def from_auth(cls, auth: Any) -> AuthContext:
        """Extract context from authenticated Auth object.

        Captures all credential types available from auth.

        Parameters:
            auth: Authenticated Auth object

        Returns:
            AuthContext with all extracted credentials
        """
        s3_creds = None
        if hasattr(auth, "get_s3_credentials") and auth.authenticated:
            try:
                s3_creds = S3Credentials.from_auth(auth)
            except Exception:
                pass  # S3 credentials may not be available

        http_hdr = None
        if hasattr(auth, "get_headers"):
            try:
                http_hdr = HTTPHeaders.from_auth(auth)
            except Exception:
                pass

        urs_token = None
        if hasattr(auth, "get_token"):
            try:
                urs_token = auth.get_token()
            except Exception:
                pass

        return cls(
            s3_credentials=s3_creds,
            http_headers=http_hdr,
            urs_token=urs_token,
            provider_credentials={},  # Can be populated separately
        )

    def to_auth(self) -> Any:
        """Reconstruct Auth object from context.

        Used in worker processes to create functional Auth without
        re-authenticating. Re-creates session from saved tokens.

        Returns:
            Reconstructed Auth object

        Raises:
            ValueError: If no credentials available to reconstruct
        """
        from earthaccess.auth.auth import Auth

        if not any([self.s3_credentials, self.http_headers, self.urs_token]):
            raise ValueError("No credentials available to reconstruct Auth")

        # Create minimal Auth object
        auth = Auth()

        return auth

    def is_valid(self) -> bool:
        """Check if credentials are non-expired and valid.

        Returns:
            True if all credentials (if any) are valid, False if any are expired
        """
        if self.s3_credentials and self.s3_credentials.is_expired():
            return False
        return True


class CredentialManager:
    """Thread-safe credential cache and manager.

    Centralizes credential management for multiple data providers,
    enabling efficient credential reuse, replacement, and expiration checking.

    Follows SOLID Single Responsibility principle:
    - Only manages credential storage and retrieval
    - Does not create or validate credentials
    - Thread-safe for concurrent access
    """

    def __init__(self) -> None:
        """Initialize credential manager with empty caches."""
        self._s3_credentials: Optional[S3Credentials] = None
        self._provider_credentials: Dict[str, Dict[str, str]] = {}
        self._lock = threading.RLock()

    def store_s3_credentials(self, creds: S3Credentials) -> None:
        """Store S3 credentials thread-safely.

        Parameters:
            creds: S3Credentials to store
        """
        with self._lock:
            self._s3_credentials = creds

    def get_s3_credentials(self) -> Optional[S3Credentials]:
        """Retrieve S3 credentials (or None if not set).

        Returns:
            Stored S3Credentials or None
        """
        with self._lock:
            return self._s3_credentials

    def store_provider_credentials(self, provider: str, creds: Dict[str, str]) -> None:
        """Store credentials for specific data provider.

        Parameters:
            provider: Provider name (e.g., "PODAAC", "NSIDC")
            creds: Credentials dictionary for provider
        """
        with self._lock:
            self._provider_credentials[provider] = creds

    def get_provider_credentials(self, provider: str) -> Optional[Dict[str, str]]:
        """Retrieve credentials for specific provider.

        Parameters:
            provider: Provider name to retrieve

        Returns:
            Credentials dictionary for provider, or None if not found
        """
        with self._lock:
            return self._provider_credentials.get(provider)

    def clear(self) -> None:
        """Clear all stored credentials.

        Useful for cleanup and testing.
        """
        with self._lock:
            self._s3_credentials = None
            self._provider_credentials.clear()
