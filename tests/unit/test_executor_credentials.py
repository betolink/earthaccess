"""Test credential distribution and reconstruction with parallel executors.

Tests that credentials can be passed through executors to worker functions
and that workers can reconstruct Auth from AuthContext.

This module verifies the Phase 5 credential distribution integration:
- Passing AuthContext through executors
- Worker reconstruction of Auth from AuthContext
- Credential availability in worker functions
- Expiration checking in workers
- Different executors handling credentials
"""

import datetime
from typing import Any, List
from unittest.mock import Mock

import pytest
from earthaccess.credentials_store.credentials import (
    AuthContext,
    HTTPHeaders,
    S3Credentials,
)
from earthaccess.parallel import SerialExecutor, ThreadPoolExecutorWrapper, get_executor


class TestExecutorWithCredentialContext:
    """Test executing operations with credential context."""

    def test_serial_executor_receives_auth_context(self) -> None:
        """Test that serial executor receives and passes AuthContext to worker."""
        # Create mock auth context
        auth_context = AuthContext(
            s3_credentials=S3Credentials(
                access_key="test_key",
                secret_key="test_secret",
                region="us-west-2",
            ),
            http_headers=HTTPHeaders(headers={"Authorization": "Bearer token"}),
        )

        # Track whether worker received context
        received_contexts: List[AuthContext] = []

        def worker_function(item: int, context: AuthContext) -> int:
            """Worker that receives auth context."""
            received_contexts.append(context)
            return item * 2

        # Execute with serial executor
        executor = SerialExecutor()
        items = [1, 2, 3]

        def item_with_context(item: int) -> int:
            return worker_function(item, auth_context)

        results = list(executor.map(item_with_context, items))

        # Verify worker received context
        assert len(received_contexts) == 3
        assert all(ctx == auth_context for ctx in received_contexts)
        assert results == [2, 4, 6]

    def test_thread_pool_executor_receives_auth_context(self) -> None:
        """Test that thread pool executor passes AuthContext to worker."""
        auth_context = AuthContext(
            s3_credentials=S3Credentials(
                access_key="test_key",
                secret_key="test_secret",
                region="us-west-2",
            ),
        )

        received_contexts: List[AuthContext] = []

        def worker_function(item: int, context: AuthContext) -> int:
            """Worker that receives auth context."""
            received_contexts.append(context)
            return item * 2

        executor = ThreadPoolExecutorWrapper(max_workers=2, show_progress=False)
        items = [1, 2, 3]

        def item_with_context(item: int) -> int:
            return worker_function(item, auth_context)

        results = list(executor.map(item_with_context, items))
        executor.shutdown()

        # Verify worker received context (order may vary with threads)
        assert len(received_contexts) == 3
        assert all(ctx == auth_context for ctx in received_contexts)
        assert sorted(results) == [2, 4, 6]

    def test_auth_context_is_serializable(self) -> None:
        """Test that AuthContext can be pickled for worker serialization."""
        import pickle

        auth_context = AuthContext(
            s3_credentials=S3Credentials(
                access_key="test_key",
                secret_key="test_secret",
                session_token="temp_token",
                region="us-west-2",
                expiration_time=datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(hours=1),
            ),
            http_headers=HTTPHeaders(headers={"Authorization": "Bearer token"}),
            urs_token="urs_token_value",
            provider_credentials={"PODAAC": {"key": "value"}},
        )

        # Pickle and unpickle
        pickled = pickle.dumps(auth_context)
        unpickled = pickle.loads(pickled)

        # Verify equality after roundtrip
        assert unpickled == auth_context
        assert unpickled.s3_credentials == auth_context.s3_credentials
        assert unpickled.http_headers == auth_context.http_headers
        assert unpickled.urs_token == auth_context.urs_token

    def test_s3_credentials_pickleable(self) -> None:
        """Test that S3Credentials can be pickled for worker serialization."""
        import pickle

        future_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            hours=1
        )
        creds = S3Credentials(
            access_key="test_key",
            secret_key="test_secret",
            session_token="temp_token",
            region="us-west-2",
            expiration_time=future_time,
        )

        # Pickle and unpickle
        pickled = pickle.dumps(creds)
        unpickled = pickle.loads(pickled)

        # Verify equality after roundtrip
        assert unpickled == creds
        assert unpickled.access_key == creds.access_key

    def test_http_headers_pickleable(self) -> None:
        """Test that HTTPHeaders can be pickled for worker serialization."""
        import pickle

        headers = HTTPHeaders(
            headers={"Authorization": "Bearer token", "User-Agent": "test"},
            cookies={"session_id": "abc123"},
        )

        # Pickle and unpickle
        pickled = pickle.dumps(headers)
        unpickled = pickle.loads(pickled)

        # Verify equality after roundtrip
        assert unpickled == headers
        assert unpickled.headers == headers.headers


class TestWorkerAuthReconstruction:
    """Test that workers can reconstruct Auth from AuthContext."""

    def test_auth_context_to_auth_creates_auth_object(self) -> None:
        """Test that to_auth() creates a functional Auth object."""
        auth_context = AuthContext(
            s3_credentials=S3Credentials(
                access_key="test_key",
                secret_key="test_secret",
            ),
            http_headers=HTTPHeaders(headers={"Authorization": "Bearer token"}),
        )

        # Call to_auth to reconstruct
        auth = auth_context.to_auth()

        # Verify Auth object was created
        assert auth is not None
        assert hasattr(auth, "authenticated")

    def test_auth_context_with_s3_credentials_only(self) -> None:
        """Test to_auth with only S3 credentials."""
        auth_context = AuthContext(
            s3_credentials=S3Credentials(
                access_key="test_key",
                secret_key="test_secret",
            ),
        )

        auth = auth_context.to_auth()
        assert auth is not None

    def test_auth_context_with_http_headers_only(self) -> None:
        """Test to_auth with only HTTP headers."""
        auth_context = AuthContext(
            http_headers=HTTPHeaders(headers={"Authorization": "Bearer token"}),
        )

        auth = auth_context.to_auth()
        assert auth is not None

    def test_auth_context_with_urs_token_only(self) -> None:
        """Test to_auth with only URS token."""
        auth_context = AuthContext(urs_token="token_value")

        auth = auth_context.to_auth()
        assert auth is not None

    def test_auth_context_with_no_credentials_raises(self) -> None:
        """Test that to_auth raises when no credentials available."""
        auth_context = AuthContext()

        with pytest.raises(ValueError, match="No credentials available"):
            auth_context.to_auth()

    def test_auth_context_is_valid_with_unexpired_s3_credentials(self) -> None:
        """Test is_valid returns True when S3 credentials not expired."""
        future_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            hours=1
        )
        auth_context = AuthContext(
            s3_credentials=S3Credentials(
                access_key="test_key",
                secret_key="test_secret",
                expiration_time=future_time,
            ),
        )

        assert auth_context.is_valid() is True

    def test_auth_context_is_invalid_with_expired_s3_credentials(self) -> None:
        """Test is_valid returns False when S3 credentials expired."""
        past_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            hours=1
        )
        auth_context = AuthContext(
            s3_credentials=S3Credentials(
                access_key="test_key",
                secret_key="test_secret",
                expiration_time=past_time,
            ),
        )

        assert auth_context.is_valid() is False


class TestCredentialExpirationInWorkers:
    """Test credential expiration checking in worker execution."""

    def test_worker_can_check_credential_expiration(self) -> None:
        """Test that worker function can check if credentials expired."""
        future_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            hours=1
        )
        auth_context = AuthContext(
            s3_credentials=S3Credentials(
                access_key="test_key",
                secret_key="test_secret",
                expiration_time=future_time,
            ),
        )

        def worker_with_expiration_check(
            item: int, context: AuthContext
        ) -> tuple[int, bool]:
            """Worker that checks credential expiration."""
            is_valid = context.is_valid()
            return item, is_valid

        executor = SerialExecutor()

        def item_with_context(item: int) -> tuple[int, bool]:
            return worker_with_expiration_check(item, auth_context)

        results = list(executor.map(item_with_context, [1]))

        # Verify credential is still valid
        assert results[0][1] is True

    def test_worker_detects_expired_credentials(self) -> None:
        """Test that worker detects expired credentials."""
        past_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            hours=1
        )
        auth_context = AuthContext(
            s3_credentials=S3Credentials(
                access_key="test_key",
                secret_key="test_secret",
                expiration_time=past_time,
            ),
        )

        def worker_with_expiration_check(
            item: int, context: AuthContext
        ) -> tuple[int, bool]:
            """Worker that checks credential expiration."""
            is_valid = context.is_valid()
            return item, is_valid

        executor = SerialExecutor()

        def item_with_context(item: int) -> tuple[int, bool]:
            return worker_with_expiration_check(item, auth_context)

        results = list(executor.map(item_with_context, [1]))

        # Verify credential is expired
        assert results[0][1] is False


class TestMultipleExecutorsWithCredentials:
    """Test credential distribution across different executor types."""

    def test_serial_executor_with_credentials(self) -> None:
        """Test serial executor distributes credentials correctly."""
        auth_context = AuthContext(
            s3_credentials=S3Credentials(
                access_key="test_key",
                secret_key="test_secret",
            ),
        )

        def worker(item: int, context: AuthContext) -> int:
            if context.s3_credentials:
                return item + 100
            return item

        executor = SerialExecutor()

        def item_with_context(item: int) -> int:
            return worker(item, auth_context)

        results = list(executor.map(item_with_context, [1, 2, 3]))
        assert results == [101, 102, 103]

    def test_thread_pool_executor_with_credentials(self) -> None:
        """Test thread pool executor distributes credentials correctly."""
        auth_context = AuthContext(
            s3_credentials=S3Credentials(
                access_key="test_key",
                secret_key="test_secret",
            ),
        )

        def worker(item: int, context: AuthContext) -> int:
            if context.s3_credentials:
                return item + 100
            return item

        executor = ThreadPoolExecutorWrapper(max_workers=2, show_progress=False)

        def item_with_context(item: int) -> int:
            return worker(item, auth_context)

        results = list(executor.map(item_with_context, [1, 2, 3]))
        executor.shutdown()

        assert sorted(results) == [101, 102, 103]

    def test_get_executor_serial_with_credentials(self) -> None:
        """Test get_executor with serial backend."""
        executor = get_executor("serial")
        assert isinstance(executor, SerialExecutor)

        auth_context = AuthContext(
            s3_credentials=S3Credentials(
                access_key="test_key",
                secret_key="test_secret",
            ),
        )

        def worker(item: int, context: AuthContext) -> int:
            return item * 2 if context.is_valid() else 0

        def item_with_context(item: int) -> int:
            return worker(item, auth_context)

        results = list(executor.map(item_with_context, [1, 2, 3]))
        assert results == [2, 4, 6]

    def test_get_executor_threads_with_credentials(self) -> None:
        """Test get_executor with threads backend."""
        executor = get_executor("threads", max_workers=2, show_progress=False)
        assert isinstance(executor, ThreadPoolExecutorWrapper)

        auth_context = AuthContext(
            s3_credentials=S3Credentials(
                access_key="test_key",
                secret_key="test_secret",
            ),
        )

        def worker(item: int, context: AuthContext) -> int:
            return item * 2 if context.is_valid() else 0

        def item_with_context(item: int) -> int:
            return worker(item, auth_context)

        results = list(executor.map(item_with_context, [1, 2, 3]))
        executor.shutdown()

        assert sorted(results) == [2, 4, 6]


class TestCredentialAccessInWorkerFunction:
    """Test that worker functions can access credential details."""

    def test_worker_accesses_s3_credentials(self) -> None:
        """Test worker can access S3 credentials from AuthContext."""
        auth_context = AuthContext(
            s3_credentials=S3Credentials(
                access_key="AKIAIOSFODNN7EXAMPLE",
                secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                region="us-west-2",
            ),
        )

        def worker(item: int, context: AuthContext) -> str:
            """Worker that accesses S3 credentials."""
            if context.s3_credentials:
                return context.s3_credentials.access_key
            return ""

        executor = SerialExecutor()

        def item_with_context(item: int) -> str:
            return worker(item, auth_context)

        results = list(executor.map(item_with_context, [1]))
        assert results[0] == "AKIAIOSFODNN7EXAMPLE"

    def test_worker_accesses_http_headers(self) -> None:
        """Test worker can access HTTP headers from AuthContext."""
        auth_context = AuthContext(
            http_headers=HTTPHeaders(
                headers={"Authorization": "Bearer test_token"},
                cookies={"session_id": "abc123"},
            ),
        )

        def worker(item: int, context: AuthContext) -> dict:
            """Worker that accesses HTTP headers."""
            if context.http_headers:
                return context.http_headers.headers
            return {}

        executor = SerialExecutor()

        def item_with_context(item: int) -> dict:
            return worker(item, auth_context)

        results = list(executor.map(item_with_context, [1]))
        assert "Authorization" in results[0]

    def test_worker_accesses_urs_token(self) -> None:
        """Test worker can access URS token from AuthContext."""
        auth_context = AuthContext(urs_token="test_urs_token_value")

        def worker(item: int, context: AuthContext) -> str:
            """Worker that accesses URS token."""
            return context.urs_token or ""

        executor = SerialExecutor()

        def item_with_context(item: int) -> str:
            return worker(item, auth_context)

        results = list(executor.map(item_with_context, [1]))
        assert results[0] == "test_urs_token_value"

    def test_worker_converts_credentials_to_dict(self) -> None:
        """Test worker can convert S3 credentials to dict for filesystem."""
        auth_context = AuthContext(
            s3_credentials=S3Credentials(
                access_key="test_key",
                secret_key="test_secret",
                session_token="temp_token",
                region="us-east-1",
            ),
        )

        def worker(item: int, context: AuthContext) -> dict:
            """Worker that converts credentials to dict."""
            if context.s3_credentials:
                return context.s3_credentials.to_dict()
            return {}

        executor = SerialExecutor()

        def item_with_context(item: int) -> dict:
            return worker(item, auth_context)

        results = list(executor.map(item_with_context, [1]))
        cred_dict = results[0]

        assert cred_dict["key"] == "test_key"
        assert cred_dict["secret"] == "test_secret"
        assert cred_dict["token"] == "temp_token"
        assert cred_dict["region_name"] == "us-east-1"


class TestAuthContextCreationFromAuth:
    """Test creating AuthContext from Auth object."""

    def test_auth_context_from_authenticated_auth(self) -> None:
        """Test creating AuthContext from authenticated Auth object."""
        # Create mock authenticated Auth object
        mock_auth = Mock()
        mock_auth.authenticated = True
        mock_auth.get_s3_credentials.return_value = {
            "access_key": "test_key",
            "secret_key": "test_secret",
            "region": "us-west-2",
        }
        mock_auth.get_headers.return_value = {"Authorization": "Bearer token"}
        mock_auth.get_cookies.return_value = {}
        mock_auth.get_token.return_value = "urs_token"

        # Create context from auth
        auth_context = AuthContext.from_auth(mock_auth)

        # Verify context created correctly
        assert auth_context.s3_credentials is not None
        assert auth_context.s3_credentials.access_key == "test_key"
        assert auth_context.http_headers is not None
        assert auth_context.urs_token == "urs_token"

    def test_auth_context_from_auth_with_missing_s3_credentials(self) -> None:
        """Test creating AuthContext when S3 credentials not available."""
        mock_auth = Mock()
        mock_auth.authenticated = False
        mock_auth.get_headers.return_value = {"Authorization": "Bearer token"}
        # Simulate S3 credentials not available (raises exception)
        mock_auth.get_s3_credentials.side_effect = Exception("Not available")

        # Create context from auth
        auth_context = AuthContext.from_auth(mock_auth)

        # Verify S3 credentials are None but HTTP headers captured
        assert auth_context.s3_credentials is None
        assert auth_context.http_headers is not None

    def test_auth_context_from_auth_handles_missing_methods(self) -> None:
        """Test creating AuthContext when Auth has missing methods."""
        mock_auth = Mock(spec=[])  # Empty spec - no methods

        # Should not raise, just create context with None
        auth_context = AuthContext.from_auth(mock_auth)

        # All should be None/empty since no methods are available
        assert auth_context.s3_credentials is None
        assert auth_context.http_headers is None
        assert auth_context.urs_token is None
        assert auth_context.provider_credentials == {}


class TestExecutorCredentialIntegration:
    """Integration tests for executors with credentials."""

    def test_execute_granule_like_operation_with_credentials(self) -> None:
        """Test executing granule-like operations with credential distribution."""
        auth_context = AuthContext(
            s3_credentials=S3Credentials(
                access_key="test_key",
                secret_key="test_secret",
            ),
        )

        # Simulate granule objects
        class MockGranule:
            def __init__(self, granule_id: str) -> None:
                self.granule_id = granule_id

            def download(self, auth_context: AuthContext) -> str:
                if auth_context.is_valid():
                    return f"Downloaded {self.granule_id}"
                return "Failed"

        granules = [MockGranule(f"G{i}") for i in range(3)]

        def process_granule(granule: Any, context: AuthContext) -> str:
            return granule.download(context)

        executor = SerialExecutor()

        def granule_with_context(granule: Any) -> str:
            return process_granule(granule, auth_context)

        results = list(executor.map(granule_with_context, granules))

        assert all("Downloaded" in result for result in results)
        assert len(results) == 3
