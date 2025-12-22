"""Tests for earthaccess.credentials module."""

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from earthaccess.credentials import (
    BUCKET_PREFIX_TO_PROVIDER,
    PROVIDER_TO_ENDPOINT,
    CredentialCache,
    CredentialManager,
    S3Credentials,
    get_bucket_provider,
    get_provider_endpoint,
)

# =============================================================================
# S3Credentials Tests
# =============================================================================


class TestS3Credentials:
    """Tests for S3Credentials class."""

    def test_create_credentials(self):
        """Test creating S3 credentials."""
        creds = S3Credentials(
            access_key_id="AKID123",
            secret_access_key="secret123",
            session_token="token123",
        )

        assert creds.access_key_id == "AKID123"
        assert creds.secret_access_key == "secret123"
        assert creds.session_token == "token123"
        assert creds.expiration is not None

    def test_from_dict(self):
        """Test creating credentials from dictionary."""
        data = {
            "accessKeyId": "AKID123",
            "secretAccessKey": "secret123",
            "sessionToken": "token123",
            "expiration": "2024-01-01T12:00:00Z",
        }

        creds = S3Credentials.from_dict(data, endpoint="https://example.com/s3")

        assert creds.access_key_id == "AKID123"
        assert creds.secret_access_key == "secret123"
        assert creds.session_token == "token123"
        assert creds.endpoint == "https://example.com/s3"
        assert creds.expiration is not None

    def test_to_dict(self):
        """Test converting credentials to dict."""
        creds = S3Credentials(
            access_key_id="AKID123",
            secret_access_key="secret123",
            session_token="token123",
        )

        result = creds.to_dict()

        assert result["key"] == "AKID123"
        assert result["secret"] == "secret123"
        assert result["token"] == "token123"

    def test_to_boto3_dict(self):
        """Test converting credentials to boto3 format."""
        creds = S3Credentials(
            access_key_id="AKID123",
            secret_access_key="secret123",
            session_token="token123",
        )

        result = creds.to_boto3_dict()

        assert result["aws_access_key_id"] == "AKID123"
        assert result["aws_secret_access_key"] == "secret123"
        assert result["aws_session_token"] == "token123"

    def test_is_expired_future(self):
        """Test credentials not expired with future expiration."""
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        creds = S3Credentials(
            access_key_id="AKID",
            secret_access_key="secret",
            session_token="token",
            expiration=future,
        )

        assert creds.is_expired() is False

    def test_is_expired_past(self):
        """Test credentials expired with past expiration."""
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        creds = S3Credentials(
            access_key_id="AKID",
            secret_access_key="secret",
            session_token="token",
            expiration=past,
        )

        assert creds.is_expired() is True

    def test_is_expired_within_buffer(self):
        """Test credentials considered expired within buffer."""
        # Expiring in 2 minutes (within 5 minute buffer)
        soon = datetime.now(timezone.utc) + timedelta(minutes=2)
        creds = S3Credentials(
            access_key_id="AKID",
            secret_access_key="secret",
            session_token="token",
            expiration=soon,
        )

        assert creds.is_expired(buffer_seconds=300) is True
        assert creds.is_expired(buffer_seconds=60) is False

    def test_repr(self):
        """Test string representation."""
        creds = S3Credentials(
            access_key_id="AKID123",
            secret_access_key="secret",
            session_token="token",
        )

        repr_str = repr(creds)
        assert "AKID" in repr_str
        assert "secret" not in repr_str  # Should not expose secrets


# =============================================================================
# CredentialCache Tests
# =============================================================================


class TestCredentialCache:
    """Tests for CredentialCache class."""

    def test_put_and_get(self):
        """Test basic put and get operations."""
        cache = CredentialCache()
        creds = S3Credentials(
            access_key_id="AKID",
            secret_access_key="secret",
            session_token="token",
        )

        cache.put("test_key", creds)
        result = cache.get("test_key")

        assert result is creds

    def test_get_missing_key(self):
        """Test getting a missing key returns None."""
        cache = CredentialCache()

        result = cache.get("missing_key")

        assert result is None

    def test_get_expired_returns_none(self):
        """Test getting expired credentials returns None and removes from cache."""
        cache = CredentialCache()
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        creds = S3Credentials(
            access_key_id="AKID",
            secret_access_key="secret",
            session_token="token",
            expiration=past,
        )

        cache.put("test_key", creds)
        result = cache.get("test_key")

        assert result is None
        assert len(cache) == 0

    def test_invalidate(self):
        """Test invalidating a cache entry."""
        cache = CredentialCache()
        creds = S3Credentials(
            access_key_id="AKID",
            secret_access_key="secret",
            session_token="token",
        )

        cache.put("test_key", creds)
        cache.invalidate("test_key")

        assert cache.get("test_key") is None

    def test_clear(self):
        """Test clearing all cache entries."""
        cache = CredentialCache()
        for i in range(3):
            creds = S3Credentials(
                access_key_id=f"AKID{i}",
                secret_access_key="secret",
                session_token="token",
            )
            cache.put(f"key_{i}", creds)

        assert len(cache) == 3

        cache.clear()

        assert len(cache) == 0

    def test_keys(self):
        """Test listing cache keys."""
        cache = CredentialCache()
        for key in ["key1", "key2", "key3"]:
            creds = S3Credentials(
                access_key_id="AKID",
                secret_access_key="secret",
                session_token="token",
            )
            cache.put(key, creds)

        keys = cache.keys()

        assert set(keys) == {"key1", "key2", "key3"}


# =============================================================================
# CredentialManager Tests
# =============================================================================


class TestCredentialManager:
    """Tests for CredentialManager class."""

    def test_create_manager(self):
        """Test creating a credential manager."""
        manager = CredentialManager()

        assert manager._auth is None
        assert len(manager._cache) == 0

    def test_get_credentials_with_callback(self):
        """Test getting credentials with custom callback."""
        mock_callback = Mock(
            return_value={
                "accessKeyId": "AKID123",
                "secretAccessKey": "secret",
                "sessionToken": "token",
            }
        )

        manager = CredentialManager(fetch_callback=mock_callback)
        creds = manager.get_credentials(endpoint="https://example.com/s3")

        assert creds.access_key_id == "AKID123"
        mock_callback.assert_called_once()

    def test_get_credentials_caching(self):
        """Test that credentials are cached."""
        mock_callback = Mock(
            return_value={
                "accessKeyId": "AKID123",
                "secretAccessKey": "secret",
                "sessionToken": "token",
            }
        )

        manager = CredentialManager(fetch_callback=mock_callback)

        # First call fetches
        creds1 = manager.get_credentials(endpoint="https://example.com/s3")

        # Second call uses cache
        creds2 = manager.get_credentials(endpoint="https://example.com/s3")

        assert creds1.access_key_id == creds2.access_key_id
        assert mock_callback.call_count == 1

    def test_get_credentials_force_refresh(self):
        """Test force refresh bypasses cache."""
        call_count = [0]

        def mock_fetch(**kwargs):
            call_count[0] += 1
            return {
                "accessKeyId": f"AKID{call_count[0]}",
                "secretAccessKey": "secret",
                "sessionToken": "token",
            }

        manager = CredentialManager(fetch_callback=mock_fetch)

        creds1 = manager.get_credentials(endpoint="https://example.com/s3")
        creds2 = manager.get_credentials(
            endpoint="https://example.com/s3", force_refresh=True
        )

        assert creds1.access_key_id == "AKID1"
        assert creds2.access_key_id == "AKID2"

    def test_get_credentials_no_location_raises(self):
        """Test that missing location specifier raises ValueError."""
        manager = CredentialManager()

        with pytest.raises(ValueError, match="At least one of"):
            manager.get_credentials()

    def test_get_credentials_no_auth_raises(self):
        """Test that missing auth raises RuntimeError."""
        manager = CredentialManager()

        with pytest.raises(RuntimeError, match="Auth instance required"):
            manager.get_credentials(provider="LPDAAC")

    def test_infer_provider_from_url(self):
        """Test inferring provider from S3 URL."""
        manager = CredentialManager()

        # Test various bucket URLs
        assert (
            manager.infer_provider_from_url("s3://lp-prod-protected/path/file.h5")
            == "LPDAAC"
        )
        assert (
            manager.infer_provider_from_url("s3://podaac-ops-cumulus-protected/file.nc")
            == "PODAAC"
        )
        assert manager.infer_provider_from_url("s3://unknown-bucket/file.h5") is None

    def test_infer_provider_from_bucket(self):
        """Test inferring provider from bucket name."""
        manager = CredentialManager()

        assert manager.infer_provider_from_bucket("lp-prod-protected") == "LPDAAC"
        assert (
            manager.infer_provider_from_bucket("nsidc-cumulus-prod-protected")
            == "NSIDC"
        )
        assert manager.infer_provider_from_bucket("unknown-bucket") is None

    def test_get_credentials_for_url(self):
        """Test getting credentials for URL."""
        mock_callback = Mock(
            return_value={
                "accessKeyId": "AKID123",
                "secretAccessKey": "secret",
                "sessionToken": "token",
            }
        )

        manager = CredentialManager(fetch_callback=mock_callback)
        creds = manager.get_credentials_for_url("s3://lp-prod-protected/path/file.h5")

        assert creds.access_key_id == "AKID123"

    def test_get_credentials_for_url_unknown_provider(self):
        """Test getting credentials for unknown URL raises ValueError."""
        manager = CredentialManager()

        with pytest.raises(ValueError, match="Cannot infer provider"):
            manager.get_credentials_for_url("s3://unknown-bucket/file.h5")

    def test_get_credentials_for_bucket(self):
        """Test getting credentials for bucket."""
        mock_callback = Mock(
            return_value={
                "accessKeyId": "AKID123",
                "secretAccessKey": "secret",
                "sessionToken": "token",
            }
        )

        manager = CredentialManager(fetch_callback=mock_callback)
        creds = manager.get_credentials_for_bucket("podaac-ops-cumulus-protected")

        assert creds.access_key_id == "AKID123"

    def test_invalidate(self):
        """Test invalidating cached credentials."""
        mock_callback = Mock(
            return_value={
                "accessKeyId": "AKID123",
                "secretAccessKey": "secret",
                "sessionToken": "token",
            }
        )

        manager = CredentialManager(fetch_callback=mock_callback)
        manager.get_credentials(endpoint="https://example.com/s3")

        assert len(manager._cache) == 1

        manager.invalidate(endpoint="https://example.com/s3")

        assert len(manager._cache) == 0

    def test_clear_cache(self):
        """Test clearing all cached credentials."""
        mock_callback = Mock(
            return_value={
                "accessKeyId": "AKID123",
                "secretAccessKey": "secret",
                "sessionToken": "token",
            }
        )

        manager = CredentialManager(fetch_callback=mock_callback)
        manager.get_credentials(endpoint="https://example.com/s3")
        manager.get_credentials(provider="LPDAAC")

        assert len(manager._cache) == 2

        manager.clear_cache()

        assert len(manager._cache) == 0

    def test_cached_providers(self):
        """Test getting list of cached providers."""
        mock_callback = Mock(
            return_value={
                "accessKeyId": "AKID123",
                "secretAccessKey": "secret",
                "sessionToken": "token",
            }
        )

        manager = CredentialManager(fetch_callback=mock_callback)
        manager.get_credentials(endpoint="https://example.com/s3")
        manager.get_credentials(provider="LPDAAC")

        providers = manager.cached_providers

        assert len(providers) == 2
        assert "https://example.com/s3" in providers
        assert "LPDAAC" in providers


# =============================================================================
# Utility Function Tests
# =============================================================================


class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_get_provider_endpoint(self):
        """Test getting endpoint for provider."""
        endpoint = get_provider_endpoint("LPDAAC")
        assert "lpdaac" in endpoint.lower()

        endpoint = get_provider_endpoint("UNKNOWN")
        assert endpoint is None

    def test_get_bucket_provider(self):
        """Test getting provider for bucket."""
        provider = get_bucket_provider("lp-prod-protected")
        assert provider == "LPDAAC"

        provider = get_bucket_provider("unknown-bucket")
        assert provider is None


# =============================================================================
# Mapping Validation Tests
# =============================================================================


class TestMappings:
    """Tests for provider/bucket mappings."""

    def test_bucket_prefix_mapping_has_providers(self):
        """Test that all bucket prefixes map to known providers."""
        for bucket, provider in BUCKET_PREFIX_TO_PROVIDER.items():
            assert provider in PROVIDER_TO_ENDPOINT, (
                f"Provider {provider} has no endpoint"
            )

    def test_all_providers_have_endpoints(self):
        """Test that all providers have endpoints."""
        for provider, endpoint in PROVIDER_TO_ENDPOINT.items():
            assert endpoint.startswith("https://"), f"Invalid endpoint for {provider}"
            assert "s3credentials" in endpoint, (
                f"Endpoint missing s3credentials for {provider}"
            )
