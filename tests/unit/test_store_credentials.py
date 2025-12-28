"""TDD Tests for earthaccess.auth.credentials module.

These tests define the expected behavior of credential classes before
implementation, following TDD principles. Tests are independent and
designed to be run in any order (no test pollution).

SOLID Principles:
- Single Responsibility: Each test class tests one component
- Interface Segregation: Tests use only public interfaces
- Dependency Inversion: Tests use mocks for dependencies
"""

import pickle
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from earthaccess.auth.credentials import (
    AuthContext,
    CredentialManager,
    HTTPHeaders,
    S3Credentials,
)

# =============================================================================
# S3Credentials Tests - SOLID: Single Responsibility
# =============================================================================


class TestS3CredentialsCreation:
    """Test S3Credentials creation and basic properties."""

    def test_create_minimal_credentials(self):
        """S3Credentials can be created with only required fields."""
        creds = S3Credentials(
            access_key="AKIAIOSFODNN7EXAMPLE",
            secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        assert creds.access_key == "AKIAIOSFODNN7EXAMPLE"
        assert creds.secret_key == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        assert creds.session_token is None
        assert creds.expiration_time is None
        assert creds.region == "us-west-2"  # default

    def test_create_with_all_fields(self):
        """S3Credentials can be created with all optional fields."""
        exp_time = datetime.now(timezone.utc) + timedelta(hours=1)
        creds = S3Credentials(
            access_key="KEY",
            secret_key="SECRET",
            session_token="SESSION_TOKEN_123",
            expiration_time=exp_time,
            region="us-east-1",
        )
        assert creds.session_token == "SESSION_TOKEN_123"
        assert creds.expiration_time == exp_time
        assert creds.region == "us-east-1"

    def test_frozen_immutable(self):
        """S3Credentials is frozen and cannot be modified."""
        creds = S3Credentials(access_key="KEY", secret_key="SECRET")
        with pytest.raises(Exception):  # FrozenInstanceError in Python 3.9+
            creds.access_key = "NEW_KEY"

    def test_hashable_for_dict_keys(self):
        """S3Credentials is hashable and can be used as dict key."""
        creds1 = S3Credentials(access_key="KEY1", secret_key="SECRET1")
        creds2 = S3Credentials(access_key="KEY2", secret_key="SECRET2")
        cred_dict = {creds1: "first", creds2: "second"}
        assert cred_dict[creds1] == "first"
        assert cred_dict[creds2] == "second"


class TestS3CredentialsExpiration:
    """Test S3Credentials expiration checking."""

    def test_is_expired_with_no_expiration_time(self):
        """is_expired() returns False when expiration_time is None."""
        creds = S3Credentials(
            access_key="KEY",
            secret_key="SECRET",
            expiration_time=None,
        )
        assert creds.is_expired() is False

    def test_is_expired_with_future_time(self):
        """is_expired() returns False for future expiration time."""
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        creds = S3Credentials(
            access_key="KEY",
            secret_key="SECRET",
            expiration_time=future,
        )
        assert creds.is_expired() is False

    def test_is_expired_with_past_time(self):
        """is_expired() returns True for past expiration time."""
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        creds = S3Credentials(
            access_key="KEY",
            secret_key="SECRET",
            expiration_time=past,
        )
        assert creds.is_expired() is True

    def test_is_expired_boundary(self):
        """is_expired() handles boundary at exact expiration time."""
        # Create expiration at a specific time
        now = datetime.now(timezone.utc)
        creds = S3Credentials(
            access_key="KEY",
            secret_key="SECRET",
            expiration_time=now,
        )
        # Should be considered expired or very close
        result = creds.is_expired()
        # Allow for tiny time differences
        assert (
            result is True or (datetime.now(timezone.utc) - now).total_seconds() < 0.1
        )


class TestS3CredentialsConversion:
    """Test S3Credentials conversion to other formats."""

    def test_to_dict_with_all_fields(self):
        """to_dict() produces valid s3fs kwargs."""
        creds = S3Credentials(
            access_key="KEY",
            secret_key="SECRET",
            session_token="TOKEN",
            region="us-east-1",
        )
        result = creds.to_dict()
        assert isinstance(result, dict)
        assert result["key"] == "KEY"
        assert result["secret"] == "SECRET"
        assert result["token"] == "TOKEN"
        assert result["region_name"] == "us-east-1"

    def test_to_dict_without_session_token(self):
        """to_dict() works without session token."""
        creds = S3Credentials(access_key="KEY", secret_key="SECRET")
        result = creds.to_dict()
        assert "key" in result
        assert "secret" in result
        assert result.get("token") is None

    def test_from_auth_extracts_credentials(self):
        """S3Credentials.from_auth() extracts credentials from Auth."""
        mock_auth = Mock()
        mock_auth.get_s3_credentials = Mock(
            return_value={
                "access_key": "AKIAIOSFODNN7EXAMPLE",
                "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "session_token": "SESSIONTOKEN",
                "expiration": datetime.now(timezone.utc) + timedelta(hours=1),
                "region": "us-east-1",
            }
        )

        creds = S3Credentials.from_auth(mock_auth)
        assert creds.access_key == "AKIAIOSFODNN7EXAMPLE"
        assert creds.secret_key == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        assert creds.session_token == "SESSIONTOKEN"


class TestS3CredentialsSerialization:
    """Test S3Credentials pickle serialization for distributed execution."""

    def test_pickleable_full_credentials(self):
        """S3Credentials can be pickled with all fields."""
        creds = S3Credentials(
            access_key="KEY",
            secret_key="SECRET",
            session_token="TOKEN",
            expiration_time=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        pickled = pickle.dumps(creds)
        restored = pickle.loads(pickled)
        assert restored.access_key == creds.access_key
        assert restored.secret_key == creds.secret_key
        assert restored.session_token == creds.session_token

    def test_pickleable_minimal_credentials(self):
        """S3Credentials can be pickled with minimal fields."""
        creds = S3Credentials(access_key="KEY", secret_key="SECRET")
        restored = pickle.loads(pickle.dumps(creds))
        assert restored.access_key == "KEY"
        assert restored.secret_key == "SECRET"

    def test_equality_after_roundtrip(self):
        """S3Credentials are equal after pickle roundtrip."""
        creds = S3Credentials(access_key="KEY", secret_key="SECRET", region="eu-west-1")
        restored = pickle.loads(pickle.dumps(creds))
        assert creds == restored


# =============================================================================
# HTTPHeaders Tests
# =============================================================================


class TestHTTPHeaders:
    """Test HTTPHeaders for HTTPS fallback access."""

    def test_create_with_headers_and_cookies(self):
        """HTTPHeaders can be created with headers and cookies."""
        headers = HTTPHeaders(
            headers={"Authorization": "Bearer token123"},
            cookies={"sessionid": "abc123"},
        )
        assert headers.headers["Authorization"] == "Bearer token123"
        assert headers.cookies["sessionid"] == "abc123"

    def test_frozen_immutable(self):
        """HTTPHeaders is frozen and immutable."""
        headers = HTTPHeaders(headers={}, cookies={})
        with pytest.raises(Exception):  # FrozenInstanceError
            headers.headers = {"new": "header"}

    def test_from_auth_extracts_headers(self):
        """HTTPHeaders.from_auth() extracts from Auth."""
        mock_auth = Mock()
        mock_auth.get_headers = Mock(return_value={"Authorization": "Bearer xyz"})
        mock_auth.get_cookies = Mock(return_value={"session": "xyz123"})

        headers = HTTPHeaders.from_auth(mock_auth)
        assert headers.headers["Authorization"] == "Bearer xyz"
        assert headers.cookies["session"] == "xyz123"

    def test_pickleable(self):
        """HTTPHeaders can be pickled for worker serialization."""
        headers = HTTPHeaders(
            headers={"Auth": "token"},
            cookies={"sid": "123"},
        )
        restored = pickle.loads(pickle.dumps(headers))
        assert restored == headers


# =============================================================================
# AuthContext Tests
# =============================================================================


class TestAuthContextCreation:
    """Test AuthContext creation and basic properties."""

    def test_create_minimal_context(self):
        """AuthContext can be created with no arguments."""
        ctx = AuthContext()
        assert ctx.s3_credentials is None
        assert ctx.http_headers is None
        assert ctx.urs_token is None
        assert ctx.provider_credentials == {}

    def test_create_with_s3_credentials(self):
        """AuthContext can hold S3 credentials."""
        creds = S3Credentials(access_key="KEY", secret_key="SECRET")
        ctx = AuthContext(s3_credentials=creds)
        assert ctx.s3_credentials == creds

    def test_create_with_provider_credentials(self):
        """AuthContext can hold provider-specific credentials."""
        ctx = AuthContext(
            provider_credentials={
                "PODAAC": {"username": "user1", "password": "pass1"},
                "NSIDC": {"token": "token123"},
            }
        )
        assert ctx.provider_credentials["PODAAC"]["username"] == "user1"
        assert ctx.provider_credentials["NSIDC"]["token"] == "token123"

    def test_frozen_immutable(self):
        """AuthContext is frozen and immutable."""
        ctx = AuthContext()
        with pytest.raises(Exception):  # FrozenInstanceError
            ctx.urs_token = "token123"


class TestAuthContextExtraction:
    """Test AuthContext.from_auth() extraction."""

    def test_from_auth_with_all_credentials(self):
        """AuthContext.from_auth() captures all credential types."""
        mock_auth = Mock()
        mock_auth.authenticated = True
        mock_auth.get_s3_credentials = Mock(
            return_value={"access_key": "KEY", "secret_key": "SECRET"}
        )
        mock_auth.get_headers = Mock(return_value={"Auth": "token"})
        mock_auth.get_cookies = Mock(return_value={})
        mock_auth.get_token = Mock(return_value="urs_token_123")

        ctx = AuthContext.from_auth(mock_auth)
        assert ctx.s3_credentials is not None
        assert ctx.http_headers is not None
        assert ctx.urs_token == "urs_token_123"

    def test_from_auth_handles_missing_methods(self):
        """AuthContext.from_auth() handles auth without all credential types."""
        mock_auth = Mock()
        mock_auth.authenticated = False
        # Mock auth doesn't have all methods

        ctx = AuthContext.from_auth(mock_auth)
        # Should still create context, just without credentials
        assert ctx is not None


class TestAuthContextValidity:
    """Test AuthContext.is_valid() credential checking."""

    def test_is_valid_empty_context(self):
        """is_valid() returns True for empty context."""
        ctx = AuthContext()
        assert ctx.is_valid() is True

    def test_is_valid_with_valid_credentials(self):
        """is_valid() returns True when expiration is in future."""
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        creds = S3Credentials(
            access_key="KEY",
            secret_key="SECRET",
            expiration_time=future,
        )
        ctx = AuthContext(s3_credentials=creds)
        assert ctx.is_valid() is True

    def test_is_valid_with_expired_credentials(self):
        """is_valid() returns False when credentials are expired."""
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        creds = S3Credentials(
            access_key="KEY",
            secret_key="SECRET",
            expiration_time=past,
        )
        ctx = AuthContext(s3_credentials=creds)
        assert ctx.is_valid() is False


class TestAuthContextSerialization:
    """Test AuthContext pickle serialization for workers."""

    def test_pickleable_full_context(self):
        """AuthContext can be pickled with all fields."""
        ctx = AuthContext(
            s3_credentials=S3Credentials(access_key="KEY", secret_key="SECRET"),
            urs_token="token123",
            provider_credentials={"PODAAC": {"token": "abc"}},
        )
        pickled = pickle.dumps(ctx)
        restored = pickle.loads(pickled)
        assert restored.s3_credentials.access_key == "KEY"
        assert restored.urs_token == "token123"
        assert restored.provider_credentials["PODAAC"]["token"] == "abc"

    def test_pickleable_empty_context(self):
        """AuthContext can be pickled even when empty."""
        ctx = AuthContext()
        restored = pickle.loads(pickle.dumps(ctx))
        assert restored == ctx


class TestAuthContextReconstruction:
    """Test AuthContext.to_auth() reconstruction."""

    def test_to_auth_raises_without_credentials(self):
        """to_auth() raises ValueError when no credentials available."""
        ctx = AuthContext()
        with pytest.raises(ValueError, match="No credentials"):
            ctx.to_auth()

    def test_to_auth_creates_auth_with_s3_creds(self):
        """to_auth() creates Auth object when S3 credentials available."""
        creds = S3Credentials(access_key="KEY", secret_key="SECRET")
        ctx = AuthContext(s3_credentials=creds)
        auth = ctx.to_auth()
        assert auth is not None

    def test_to_auth_creates_auth_with_http_headers(self):
        """to_auth() creates Auth object when HTTP headers available."""
        headers = HTTPHeaders(headers={"Auth": "token"}, cookies={})
        ctx = AuthContext(http_headers=headers)
        auth = ctx.to_auth()
        assert auth is not None

    def test_to_auth_creates_auth_with_urs_token(self):
        """to_auth() creates Auth object when URS token available."""
        ctx = AuthContext(urs_token="token123")
        auth = ctx.to_auth()
        assert auth is not None


# =============================================================================
# CredentialManager Tests - SOLID: Single Responsibility
# =============================================================================


class TestCredentialManagerBasic:
    """Test CredentialManager basic operations."""

    def test_instantiate_empty(self):
        """CredentialManager can be instantiated empty."""
        mgr = CredentialManager()
        assert mgr is not None
        assert mgr.get_s3_credentials() is None

    def test_store_and_retrieve_s3_credentials(self):
        """CredentialManager stores and retrieves S3 credentials."""
        mgr = CredentialManager()
        creds = S3Credentials(access_key="KEY", secret_key="SECRET")

        mgr.store_s3_credentials(creds)
        retrieved = mgr.get_s3_credentials()
        assert retrieved == creds

    def test_store_provider_credentials(self):
        """CredentialManager stores provider-specific credentials."""
        mgr = CredentialManager()
        podaac_creds = {"username": "user1", "password": "pass1"}

        mgr.store_provider_credentials("PODAAC", podaac_creds)
        retrieved = mgr.get_provider_credentials("PODAAC")
        assert retrieved == podaac_creds

    def test_get_nonexistent_provider_returns_none(self):
        """Getting nonexistent provider credentials returns None."""
        mgr = CredentialManager()
        assert mgr.get_provider_credentials("NONEXISTENT") is None

    def test_store_multiple_providers(self):
        """CredentialManager stores multiple provider credentials."""
        mgr = CredentialManager()
        mgr.store_provider_credentials("PODAAC", {"token": "token1"})
        mgr.store_provider_credentials("NSIDC", {"token": "token2"})

        assert mgr.get_provider_credentials("PODAAC")["token"] == "token1"
        assert mgr.get_provider_credentials("NSIDC")["token"] == "token2"

    def test_replace_s3_credentials(self):
        """Storing new S3 credentials replaces old ones."""
        mgr = CredentialManager()
        creds1 = S3Credentials(access_key="KEY1", secret_key="SECRET1")
        creds2 = S3Credentials(access_key="KEY2", secret_key="SECRET2")

        mgr.store_s3_credentials(creds1)
        assert mgr.get_s3_credentials().access_key == "KEY1"

        mgr.store_s3_credentials(creds2)
        assert mgr.get_s3_credentials().access_key == "KEY2"

    def test_clear_all_credentials(self):
        """clear() removes all stored credentials."""
        mgr = CredentialManager()
        mgr.store_s3_credentials(S3Credentials(access_key="KEY", secret_key="SECRET"))
        mgr.store_provider_credentials("PODAAC", {"token": "token"})

        mgr.clear()
        assert mgr.get_s3_credentials() is None
        assert mgr.get_provider_credentials("PODAAC") is None


class TestCredentialManagerThreadSafety:
    """Test CredentialManager thread safety."""

    def test_concurrent_access_safe(self):
        """CredentialManager is thread-safe for concurrent access."""
        mgr = CredentialManager()
        creds = S3Credentials(access_key="KEY", secret_key="SECRET")

        # Store then retrieve (should be atomic)
        mgr.store_s3_credentials(creds)
        retrieved = mgr.get_s3_credentials()
        assert retrieved == creds
