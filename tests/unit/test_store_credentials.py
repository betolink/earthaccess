"""Unit tests for credential management."""

import datetime
import pytest
from unittest.mock import Mock

from earthaccess.store.credentials import (
    S3Credentials,
    AuthContext,
    CredentialManager,
    infer_provider_from_url,
)


class TestS3Credentials:
    """Test S3Credentials dataclass."""

    def test_valid_credentials(self):
        """Test valid credential creation."""
        expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            hours=1
        )
        creds = S3Credentials(
            "AKID123", "SECRET456", "TOKEN789", expiration, "us-east-1"
        )

        assert creds.access_key_id == "AKID123"
        assert creds.secret_access_key == "SECRET456"
        assert creds.session_token == "TOKEN789"
        assert creds.expiration == expiration
        assert creds.region == "us-east-1"

    def test_missing_credentials_raises_error(self):
        """Test that missing fields raise ValueError."""
        with pytest.raises(
            ValueError, match="All S3 credential fields must be non-empty"
        ):
            S3Credentials("", "SECRET", "TOKEN", datetime.datetime.now())

        with pytest.raises(
            ValueError, match="All S3 credential fields must be non-empty"
        ):
            S3Credentials("AKID", None, "TOKEN", datetime.datetime.now())

    def test_not_expired(self):
        """Test credential expiration check - not expired."""
        future_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            hours=1
        )
        creds = S3Credentials("A", "S", "T", future_time)
        assert not creds.is_expired

    def test_expired(self):
        """Test credential expiration check - expired."""
        past_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            hours=1
        )
        creds = S3Credentials("A", "S", "T", past_time)
        assert creds.is_expired

    def test_expires_soon_with_buffer(self):
        """Test that credentials within 5 minutes are considered expired."""
        near_future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            minutes=3
        )
        creds = S3Credentials("A", "S", "T", near_future)
        assert creds.is_expired  # Should be expired due to 5-minute buffer

    def test_to_dict(self):
        """Test conversion to fsspec dictionary."""
        creds = S3Credentials(
            "AKID", "SECRET", "TOKEN", datetime.datetime.now(), "eu-west-1"
        )
        expected = {
            "key": "AKID",
            "secret": "SECRET",
            "token": "TOKEN",
            "region": "eu-west-1",
        }
        assert creds.to_dict() == expected

    def test_to_boto3_dict(self):
        """Test conversion to boto3 dictionary."""
        creds = S3Credentials("AKID", "SECRET", "TOKEN", datetime.datetime.now())
        result = creds.to_boto3_dict()

        assert result["aws_access_key_id"] == "AKID"
        assert result["aws_secret_access_key"] == "SECRET"
        assert result["aws_session_token"] == "TOKEN"
        assert result["region_name"] == "us-west-2"  # Default


class TestAuthContext:
    """Test AuthContext dataclass."""

    def test_s3_context_creation(self):
        """Test AuthContext for S3 credentials."""
        s3_creds = S3Credentials("A", "S", "T", datetime.datetime.now())
        context = AuthContext(
            s3_credentials=s3_creds, provider="POCLOUD", cloud_hosted=True
        )

        assert context.s3_credentials == s3_creds
        assert context.provider == "POCLOUD"
        assert context.cloud_hosted is True
        assert context.https_headers is None
        assert context.https_cookies is None

    def test_https_context_creation(self):
        """Test AuthContext for HTTPS session."""
        headers = {"Authorization": "Bearer token"}
        cookies = {"session": "abc123"}
        context = AuthContext(
            https_headers=headers,
            https_cookies=cookies,
            provider="NSIDC",
            cloud_hosted=False,
        )

        assert context.s3_credentials is None
        assert context.provider == "NSIDC"
        assert context.cloud_hosted is False
        assert context.https_headers == headers
        assert context.https_cookies == cookies

    def test_to_dict(self):
        """Test AuthContext serialization."""
        s3_creds = S3Credentials(
            "A", "S", "T", datetime.datetime.now(datetime.timezone.utc)
        )
        context = AuthContext(
            s3_credentials=s3_creds, provider="POCLOUD", cloud_hosted=True
        )

        result = context.to_dict()

        assert "s3_credentials" in result
        assert result["s3_credentials"]["key"] == "A"
        assert result["provider"] == "POCLOUD"
        assert result["cloud_hosted"] is True
        assert "created_at" in result


class TestInferProviderFromUrl:
    """Test provider inference from URLs."""

    def test_podaac_bucket(self):
        """Test PODAAC bucket pattern."""
        url = "s3://podaac-ccmp-zonal/data/file.nc"
        provider = infer_provider_from_url(url)
        assert provider == "POCLOUD"

    def test_nsidc_bucket(self):
        """Test NSIDC bucket pattern."""
        url = "s3://nsidc-cumulus-g01/granules/file.h5"
        provider = infer_provider_from_url(url)
        assert provider == "NSIDC_CPRD"

    def test_lp_cloud_bucket(self):
        """Test LP DAAC cloud bucket pattern."""
        url = "s3://lp-prod-public/data/file.nc"
        provider = infer_provider_from_url(url)
        assert provider == "LPCLOUD"

    def test_unknown_bucket(self):
        """Test unknown bucket returns None."""
        url = "s3://unknown-bucket/data/file.nc"
        provider = infer_provider_from_url(url)
        assert provider is None

    def test_non_s3_url(self):
        """Test non-S3 URLs return None."""
        urls = [
            "https://example.com/file.nc",
            "file://local/path/file.nc",
            "gs://bucket/file.nc",
        ]
        for url in urls:
            provider = infer_provider_from_url(url)
            assert provider is None

    def test_various_providers(self):
        """Test various known provider patterns."""
        test_cases = [
            ("s3://gesdisc-cumulus-prod-protected/data.nc", "GES_DISC"),
            ("s3://ghrc-cumulus-protected/data.h5", "GHRC_DAAC"),
            ("s3://ornldaac-cumulus/file.nc", "ORNL_CLOUD"),
            ("s3://asf-cumulus-granules/file.tif", "ASF"),
            ("s3://obdaac-protected/data.nc", "OB_DAAC"),
            ("s3://laads-cloud/data.h5", "LAADS"),
        ]

        for url, expected_provider in test_cases:
            provider = infer_provider_from_url(url)
            assert provider == expected_provider


class TestCredentialManager:
    """Test CredentialManager caching and refresh."""

    @pytest.fixture
    def mock_auth(self):
        """Mock Auth instance."""
        auth = Mock()
        auth.get_s3_credentials.return_value = {
            "accessKeyId": "AKID123",
            "secretAccessKey": "SECRET456",
            "sessionToken": "TOKEN789",
            "expiration": "2024-12-31T23:59:59Z",
        }
        return auth

    def test_fetches_credentials_when_cache_empty(self, mock_auth):
        """Test that credentials are fetched when cache is empty."""
        manager = CredentialManager(mock_auth)

        creds = manager.get_credentials("POCLOUD")

        # Verify auth.get_s3_credentials was called
        mock_auth.get_s3_credentials.assert_called_once_with(provider="POCLOUD")

        # Verify returned credentials
        assert creds.access_key_id == "AKID123"
        assert creds.secret_access_key == "SECRET456"
        assert creds.session_token == "TOKEN789"

    def test_uses_cached_credentials(self, mock_auth):
        """Test that cached credentials are reused."""
        manager = CredentialManager(mock_auth)

        # First call should fetch
        creds1 = manager.get_credentials("POCLOUD")
        first_call_count = mock_auth.get_s3_credentials.call_count

        # Second call should use cache
        creds2 = manager.get_credentials("POCLOUD")
        second_call_count = mock_auth.get_s3_credentials.call_count

        # Verify auth was called only once
        assert first_call_count == 1
        assert second_call_count == 1
        assert creds1.access_key_id == creds2.access_key_id

    def test_refreshes_expired_credentials(self, mock_auth):
        """Test that expired credentials are refreshed."""
        manager = CredentialManager(mock_auth)

        # First call
        creds1 = manager.get_credentials("POCLOUD")

        # Manually expire credentials
        creds1.expiration = datetime.datetime.now(
            datetime.timezone.utc
        ) - datetime.timedelta(hours=1)

        # Second call should refresh
        creds2 = manager.get_credentials("POCLOUD")

        # Verify auth was called twice
        assert mock_auth.get_s3_credentials.call_count == 2
        assert creds1.access_key_id == creds2.access_key_id  # Same data, refreshed

    def test_requires_provider_for_s3(self, mock_auth):
        """Test that provider is required for S3 credentials."""
        manager = CredentialManager(mock_auth)

        with pytest.raises(ValueError, match="Provider must be specified"):
            manager.get_credentials()

    def test_separate_cache_for_providers(self, mock_auth):
        """Test that different providers have separate cache entries."""
        manager = CredentialManager(mock_auth)

        # Get credentials for different providers
        manager.get_credentials("POCLOUD")
        manager.get_credentials("NSIDC_CPRD")

        # Verify both providers were requested
        calls = mock_auth.get_s3_credentials.call_args_list
        providers = [call.kwargs["provider"] for call in calls]

        assert "POCLOUD" in providers
        assert "NSIDC_CPRD" in providers
        assert len(providers) == 2

    def test_get_auth_context_cloud(self, mock_auth):
        """Test AuthContext creation for cloud data."""
        manager = CredentialManager(mock_auth)

        context = manager.get_auth_context(provider="POCLOUD", cloud_hosted=True)

        assert context.cloud_hosted is True
        assert context.provider == "POCLOUD"
        assert context.s3_credentials is not None
        assert context.https_headers is None

    def test_get_auth_context_https(self, mock_auth):
        """Test AuthContext creation for on-prem data."""
        mock_auth.get_session.return_value.headers = {"Auth": "Bearer token"}
        mock_auth.get_session.return_value.cookies.items.return_value = [
            ("session", "abc123")
        ]

        manager = CredentialManager(mock_auth)

        context = manager.get_auth_context(provider="NSIDC", cloud_hosted=False)

        assert context.cloud_hosted is False
        assert context.provider == "NSIDC"
        assert context.s3_credentials is None
        assert context.https_headers == {"Auth": "Bearer token"}
        assert context.https_cookies == {"session": "abc123"}

    def test_invalidate_cache_single_provider(self, mock_auth):
        """Test invalidating cache for single provider."""
        manager = CredentialManager(mock_auth)

        # Populate cache
        manager.get_credentials("POCLOUD")
        assert manager.list_cached_providers() == ["POCLOUD"]

        # Invalidate specific provider
        manager.invalidate_cache("POCLOUD")
        assert manager.list_cached_providers() == []

    def test_invalidate_cache_all_providers(self, mock_auth):
        """Test invalidating cache for all providers."""
        manager = CredentialManager(mock_auth)

        # Populate cache
        manager.get_credentials("POCLOUD")
        manager.get_credentials("NSIDC_CPRD")
        assert len(manager.list_cached_providers()) == 2

        # Invalidate all
        manager.invalidate_cache()
        assert len(manager.list_cached_providers()) == 0

    def test_cache_status(self, mock_auth):
        """Test cache status reporting."""
        manager = CredentialManager(mock_auth)

        # Add credentials to cache
        creds = manager.get_credentials("POCLOUD")

        status = manager.cache_status()

        assert "POCLOUD" in status
        assert status["POCLOUD"]["has_session_token"] is True
        assert "expires_at" in status["POCLOUD"]
        assert isinstance(status["POCLOUD"]["is_expired"], bool)
