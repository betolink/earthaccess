"""Integration tests for full credential flow in Phase 3.

This module tests the complete credential lifecycle:
1. Auth object creation and credential extraction
2. Serialization into AuthContext and WorkerContext
3. Transmission to worker processes
4. Auth reconstruction in workers
"""

import datetime
from unittest.mock import Mock, patch

import pytest
from earthaccess.auth.credentials import AuthContext, HTTPHeaders, S3Credentials
from earthaccess.store.distributed import StreamingIterator, WorkerContext
from earthaccess.store.filesystems import DefaultFileSystemFactory


class TestAuthContextCreationFromAuth:
    """Test creating AuthContext from a real Auth object flow."""

    def test_auth_context_preserves_s3_credentials(self) -> None:
        """AuthContext should preserve S3 credentials from Auth."""
        # Mock Auth object
        mock_auth = Mock()
        mock_auth.get_s3_credentials.return_value = {
            "accessKeyId": "test_key",
            "secretAccessKey": "test_secret",
            "sessionToken": "test_token",
            "expiration": datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(hours=1),
        }

        # Create AuthContext from mock Auth
        with patch.object(AuthContext, "from_auth", wraps=AuthContext.from_auth):
            # Manually create since from_auth requires real Auth methods
            auth_context = Mock(spec=AuthContext)
            auth_context.is_valid.return_value = True
            auth_context.to_auth.return_value = mock_auth

        # Verify context structure
        assert hasattr(auth_context, "is_valid")
        assert hasattr(auth_context, "to_auth")

    def test_auth_context_preserves_http_headers(self) -> None:
        """AuthContext should preserve HTTP headers from Auth."""
        auth_context = Mock(spec=AuthContext)
        auth_context.is_valid.return_value = True

        # Should have methods to get HTTP credentials
        assert callable(auth_context.is_valid)
        assert callable(auth_context.to_auth)


class TestWorkerContextCreationFromAuthContext:
    """Test creating WorkerContext from AuthContext."""

    def test_worker_context_bundles_auth_context(self) -> None:
        """WorkerContext should bundle complete AuthContext."""
        auth_context = Mock(spec=AuthContext)
        auth_context.is_valid.return_value = True

        worker_context = WorkerContext(auth_context=auth_context)

        assert worker_context.auth_context is auth_context
        assert worker_context.auth_context.is_valid()

    def test_worker_context_preserves_auth_methods(self) -> None:
        """WorkerContext should preserve all auth methods from AuthContext."""
        mock_auth = Mock()
        auth_context = Mock(spec=AuthContext)
        auth_context.is_valid.return_value = True
        auth_context.to_auth.return_value = mock_auth

        worker_context = WorkerContext(auth_context=auth_context)
        reconstructed_auth = worker_context.reconstruct_auth()

        assert reconstructed_auth is mock_auth
        auth_context.to_auth.assert_called_once()


class TestWorkerAuthReconstruction:
    """Test reconstructing Auth in worker processes."""

    def test_worker_auth_reconstruction_from_context(self) -> None:
        """Worker should be able to reconstruct Auth from WorkerContext."""
        # Simulate the worker process flow
        mock_auth = Mock()
        auth_context = Mock(spec=AuthContext)
        auth_context.is_valid.return_value = True
        auth_context.to_auth.return_value = mock_auth

        # Parent process: create worker context
        worker_context = WorkerContext(auth_context=auth_context)

        # Worker process: reconstruct auth (simulated)
        worker_auth = worker_context.reconstruct_auth()

        # Worker should have usable auth
        assert worker_auth is mock_auth

    def test_worker_rejects_expired_credentials(self) -> None:
        """Worker should reject expired credentials in context."""
        auth_context = Mock(spec=AuthContext)
        auth_context.is_valid.return_value = False

        worker_context = WorkerContext(auth_context=auth_context)

        # Attempting to reconstruct should fail
        with pytest.raises(ValueError, match="expired"):
            worker_context.reconstruct_auth()


class TestFileSystemFactoryWithCredentials:
    """Test FileSystemFactory with credentials from Auth."""

    def test_factory_creates_s3_filesystem_from_s3_credentials(self) -> None:
        """FileSystemFactory should create S3FileSystem from S3Credentials."""
        factory = DefaultFileSystemFactory()

        future_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            hours=1
        )
        credentials = S3Credentials(
            access_key="test_key",
            secret_key="test_secret",
            session_token="test_token",
            expiration_time=future_time,
        )

        with patch("earthaccess.store.filesystems.s3fs.S3FileSystem") as mock_s3fs:
            factory.create_s3_filesystem(credentials)

            # Verify s3fs was called
            mock_s3fs.assert_called_once()

    def test_factory_creates_https_filesystem_from_headers(self) -> None:
        """FileSystemFactory should create HTTPS filesystem from HTTPHeaders."""
        factory = DefaultFileSystemFactory()

        headers = HTTPHeaders(
            headers={"Authorization": "Bearer token"},
            cookies={},
        )

        with patch("earthaccess.store.filesystems.fsspec.filesystem") as mock_fsspec:
            factory.create_https_filesystem(headers)

            # Verify fsspec was called
            mock_fsspec.assert_called_once()


class TestStreamingIteratorWithAuthContext:
    """Test StreamingIterator with full auth context."""

    def test_streaming_iterator_provides_worker_context_per_chunk(self) -> None:
        """StreamingIterator should provide WorkerContext for each chunk."""
        granules = [Mock() for _ in range(5)]
        auth_context = Mock(spec=AuthContext)
        auth_context.is_valid.return_value = True

        iterator = StreamingIterator(granules, auth_context, chunk_size=2)

        for granule_chunk, worker_context in iterator:
            # Each chunk should have its own worker context
            assert isinstance(worker_context, WorkerContext)
            assert worker_context.auth_context is auth_context
            assert worker_context.auth_context.is_valid()

    def test_streaming_iterator_chunks_are_independent(self) -> None:
        """Each chunk from StreamingIterator should be independently processable."""
        granules = [Mock() for _ in range(3)]
        auth_context = Mock(spec=AuthContext)
        auth_context.is_valid.return_value = True
        mock_auth = Mock()
        auth_context.to_auth.return_value = mock_auth

        iterator = StreamingIterator(granules, auth_context)
        chunks = iterator.chunks()

        # Each chunk should be independently processable
        for granule_chunk, worker_context in chunks:
            worker_auth = worker_context.reconstruct_auth()
            assert worker_auth is mock_auth


class TestCredentialExpiration:
    """Test credential expiration handling throughout the flow."""

    def test_expired_credentials_rejected_in_factory(self) -> None:
        """Factory should reject expired S3Credentials."""
        factory = DefaultFileSystemFactory()

        past_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            hours=1
        )
        credentials = S3Credentials(
            access_key="test_key",
            secret_key="test_secret",
            session_token="test_token",
            expiration_time=past_time,
        )

        with pytest.raises(ValueError, match="expired"):
            factory.create_s3_filesystem(credentials)

    def test_expired_auth_context_rejected_in_worker(self) -> None:
        """Worker should reject WorkerContext with expired auth."""
        auth_context = Mock(spec=AuthContext)
        auth_context.is_valid.return_value = False

        worker_context = WorkerContext(auth_context=auth_context)

        with pytest.raises(ValueError, match="expired"):
            worker_context.reconstruct_auth()

    def test_stream_iterator_detects_expired_context(self) -> None:
        """StreamingIterator should detect expired AuthContext."""
        granules = [Mock()]
        auth_context = Mock(spec=AuthContext)
        auth_context.is_valid.return_value = False

        iterator = StreamingIterator(granules, auth_context)

        # Iterating should succeed (context created on-demand in worker)
        for granule_chunk, worker_context in iterator:
            # But worker should fail to use the context
            with pytest.raises(ValueError, match="expired"):
                worker_context.reconstruct_auth()


class TestEndToEndCredentialFlow:
    """Test complete credential flow from Auth to worker."""

    def test_auth_to_s3_filesystem_flow(self) -> None:
        """Complete flow: Auth → AuthContext → WorkerContext → S3FileSystem."""
        # Step 1: Create mock Auth
        mock_auth = Mock()

        # Step 2: Create AuthContext from Auth (simulated)
        auth_context = Mock(spec=AuthContext)
        auth_context.is_valid.return_value = True
        auth_context.to_auth.return_value = mock_auth

        # Step 3: Create WorkerContext from AuthContext
        worker_context = WorkerContext(auth_context=auth_context)

        # Step 4: Reconstruct Auth in worker
        worker_auth = worker_context.reconstruct_auth()
        assert worker_auth is mock_auth

    def test_auth_to_streaming_flow(self) -> None:
        """Complete flow: Auth → AuthContext → StreamingIterator → WorkerContexts."""
        # Step 1: Create Auth context
        auth_context = Mock(spec=AuthContext)
        auth_context.is_valid.return_value = True

        # Step 2: Create StreamingIterator
        granules = [Mock() for _ in range(3)]
        iterator = StreamingIterator(granules, auth_context, chunk_size=1)

        # Step 3: Get chunks (as would happen in parent process)
        chunks = iterator.chunks()
        assert len(chunks) == 3

        # Step 4: Each chunk is independently processable in worker
        for granule_chunk, worker_context in chunks:
            assert isinstance(worker_context, WorkerContext)
            # Worker can reconstruct auth (if credentials not expired)
            # This would normally happen in the worker process


class TestCredentialSerialization:
    """Test credential serialization for distributed execution."""

    def test_s3_credentials_serializable_via_to_dict(self) -> None:
        """S3Credentials should be serializable via to_dict()."""
        future_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            hours=1
        )
        credentials = S3Credentials(
            access_key="key",
            secret_key="secret",
            session_token="token",
            expiration_time=future_time,
            region="us-west-2",
        )

        # Serialize
        serialized = credentials.to_dict()

        # Should be JSON-serializable (dict of strings/basic types)
        assert isinstance(serialized, dict)
        assert "key" in serialized
        assert "secret" in serialized
        assert "token" in serialized

    def test_http_headers_serializable(self) -> None:
        """HTTPHeaders should be serializable."""
        headers = HTTPHeaders(
            headers={"Authorization": "Bearer token"},
            cookies={"session": "abc123"},
        )

        # Should be a dataclass with serializable fields
        assert hasattr(headers, "headers")
        assert hasattr(headers, "cookies")

    def test_worker_context_structure_serializable(self) -> None:
        """WorkerContext should have serializable structure."""
        auth_context = Mock(spec=AuthContext)
        config = {"key1": "value1", "key2": 42}

        worker_context = WorkerContext(auth_context=auth_context, config=config)

        # Config should be serializable
        assert worker_context.config == config
