"""Credential management for earthaccess.

Provides centralized credential management with caching and expiration handling.
Follows SOLID principles with single responsibility for credential operations.
"""

import datetime
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Mapping, Optional, Union

if TYPE_CHECKING:
    from ..auth import Auth

logger = logging.getLogger(__name__)


@dataclass
class S3Credentials:
    """Immutable S3 credential representation.

    Encapsulates S3 credentials with expiration handling.
    Provides fsspec-compatible dictionary format.
    """

    access_key_id: str
    secret_access_key: str
    session_token: str
    expiration: datetime.datetime
    region: str = "us-west-2"

    def __post_init__(self) -> None:
        """Validate credential format."""
        if not all([self.access_key_id, self.secret_access_key, self.session_token]):
            raise ValueError("All S3 credential fields must be non-empty")

    @property
    def is_expired(self) -> bool:
        """Check if credentials are expired."""
        # Add 5-minute buffer before expiration
        buffer = datetime.timedelta(minutes=5)
        return datetime.datetime.now(datetime.timezone.utc) >= (
            self.expiration - buffer
        )

    def to_dict(self) -> Dict[str, str]:
        """Convert to fsspec-compatible dictionary.

        Returns:
            Dictionary suitable for s3fs.S3FileSystem constructor
        """
        return {
            "key": self.access_key_id,
            "secret": self.secret_access_key,
            "token": self.session_token,
            "region": self.region,
        }

    def to_boto3_dict(self) -> Dict[str, str]:
        """Convert to boto3-compatible dictionary."""
        return {
            "aws_access_key_id": self.access_key_id,
            "aws_secret_access_key": self.secret_access_key,
            "aws_session_token": self.session_token,
            "region_name": self.region,
        }


@dataclass
class AuthContext:
    """Immutable authentication context for distributed execution.

    Centralizes authentication state that can be safely shared
    across processes or threads without exposing session objects.
    """

    s3_credentials: Optional["S3Credentials"] = None
    https_headers: Optional[Mapping[str, str]] = None
    https_cookies: Optional[Mapping[str, str]] = None
    provider: Optional[str] = None
    cloud_hosted: bool = True
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to serializable dictionary."""
        result = {
            "https_headers": self.https_headers,
            "https_cookies": self.https_cookies,
            "provider": self.provider,
            "cloud_hosted": self.cloud_hosted,
            "created_at": self.created_at.isoformat(),
        }
        if self.s3_credentials:
            result["s3_credentials"] = self.s3_credentials.to_dict()
        else:
            result["s3_credentials"] = None
        return result


class CredentialManager:
    """Manages credential caching and refresh operations.

    Single Responsibility: Manage credential lifecycle
    - Fetch credentials when needed
    - Cache valid credentials
    - Refresh expired credentials
    - Provide credential context for distributed execution
    """

    def __init__(self, auth: Optional["Auth"]) -> None:
        """Initialize credential manager.

        Args:
            auth: EarthAccess Auth instance for fetching credentials
        """
        self.auth = auth
        self._credential_cache: Dict[str, S3Credentials] = {}
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def get_credentials(self, provider: Optional[str] = None) -> S3Credentials:
        """Get S3 credentials for a provider.

        Fetches from cache if valid, otherwise fetches fresh credentials.

        Args:
            provider: NASA provider code (e.g., "POCLOUD", "NSIDC_CPRD")

        Returns:
            S3Credentials instance

        Raises:
            ValueError: If provider is required but not specified
        """
        if provider is None:
            raise ValueError("Provider must be specified for S3 credentials")

        # Check cache first
        cached = self._credential_cache.get(provider)
        if cached and not cached.is_expired:
            self._logger.debug(f"Using cached credentials for {provider}")
            return cached

        # Fetch fresh credentials
        self._logger.info(f"Fetching fresh credentials for {provider}")
        credentials_dict = self.auth.get_s3_credentials(provider=provider)

        # Extract expiration from NASA response
        expiration_str = credentials_dict.get("expiration")
        if expiration_str:
            # Parse ISO 8601 format from NASA
            expiration = datetime.datetime.fromisoformat(
                expiration_str.replace("Z", "+00:00")
            )
        else:
            # Default to 1 hour from now if not specified
            expiration = datetime.datetime.now(
                datetime.timezone.utc
            ) + datetime.timedelta(hours=1)

        s3_creds = S3Credentials(
            access_key_id=credentials_dict["accessKeyId"],
            secret_access_key=credentials_dict["secretAccessKey"],
            session_token=credentials_dict["sessionToken"],
            expiration=expiration,
            region=credentials_dict.get("region", "us-west-2"),
        )

        # Cache credentials
        self._credential_cache[provider] = s3_creds

        return s3_creds

    def get_auth_context(
        self,
        provider: Optional[str] = None,
        cloud_hosted: bool = True,
    ) -> AuthContext:
        """Get authentication context for distributed execution.

        Creates a serializable authentication context that can be
        safely shared across processes or threads.

        Args:
            provider: NASA provider code
            cloud_hosted: Whether accessing cloud-hosted data

        Returns:
            AuthContext instance
        """
        if cloud_hosted and self.auth:
            # S3 credentials for cloud data
            s3_creds = self.get_credentials(provider)
            return AuthContext(
                s3_credentials=s3_creds,
                provider=provider,
                cloud_hosted=True,
            )
        elif self.auth:
            # HTTPS session state for on-prem data
            session = self.auth.get_session()

            # Extract serializable components
            headers = {k: str(v) for k, v in session.headers.items()}
            cookies = {k: v for k, v in session.cookies.items()}

            return AuthContext(
                https_headers=headers,
                https_cookies=cookies,
                provider=provider,
                cloud_hosted=False,
            )
        else:
            # No auth available
            return AuthContext(
                s3_credentials=None,
                https_headers=None,
                https_cookies=None,
                provider=provider,
                cloud_hosted=cloud_hosted,
            )

    def invalidate_cache(self, provider: Optional[str] = None) -> None:
        """Invalidate cached credentials.

        Args:
            provider: Specific provider to invalidate, or None for all
        """
        if provider:
            self._credential_cache.pop(provider, None)
            self._logger.debug(f"Invalidated cache for {provider}")
        else:
            count = len(self._credential_cache)
            self._credential_cache.clear()
            self._logger.debug(f"Invalidated cache for {count} providers")

    def list_cached_providers(self) -> list[str]:
        """List providers with cached credentials.

        Returns:
            List of provider codes with cached credentials
        """
        return list(self._credential_cache.keys())

    def cache_status(self) -> Dict[str, Dict[str, Union[str, bool]]]:
        """Get status of cached credentials.

        Returns:
            Dictionary with cache status for each provider
        """
        status = {}
        for provider, creds in self._credential_cache.items():
            status[provider] = {
                "expires_at": creds.expiration.isoformat(),
                "is_expired": creds.is_expired,
                "has_session_token": bool(creds.session_token),
            }
        return status


def infer_provider_from_url(url: str) -> Optional[str]:
    """Attempt to infer provider from S3 URL bucket name.

    Uses known bucket naming patterns to map to providers.

    Args:
        url: S3 URL (e.g., "s3://podaac-ccmp-zonal/file.nc")

    Returns:
        Inferred provider code or None if unknown

    Examples:
        >>> infer_provider_from_url("s3://podaac-ccmp-zonal/data.nc")
        'POCLOUD'
        >>> infer_provider_from_url("s3://nsidc-cumulus-g01/data.h5")
        'NSIDC_CPRD'
        >>> infer_provider_from_url("s3://unknown-bucket/data.nc")
        None
    """
    if not url.startswith("s3://"):
        return None

    # Known bucket prefixes to provider mapping
    # Note: Patterns should match bucket name prefixes without trailing hyphen
    # to support both "bucket-name" and "bucket" style naming
    BUCKET_PROVIDER_MAP = {
        "podaac": "POCLOUD",
        "nsidc-cumulus": "NSIDC_CPRD",
        "lp-prod": "LPCLOUD",
        "gesdisc-cumulus": "GES_DISC",
        "ghrc-cumulus": "GHRC_DAAC",
        "ornldaac-cumulus": "ORNL_CLOUD",
        "asf-cumulus": "ASF",
        "gesdisc-ecostress": "GES_DISC",
        "obdaac": "OB_DAAC",
        "laads": "LAADS",
        "eclipse": "NSIDC_ECS",
        "noaa": "NOAA_NCEI",
        "usgs": "USGS_EROS",
    }

    # Extract bucket name
    bucket = url.split("/", 3)[2]  # s3://bucket/key...

    # Check against known patterns
    for prefix, provider in BUCKET_PROVIDER_MAP.items():
        if bucket.startswith(prefix):
            return provider

    return None
