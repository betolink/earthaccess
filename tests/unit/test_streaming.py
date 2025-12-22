"""Tests for earthaccess.streaming module."""

import queue
import threading
import time
from typing import List

import pytest
from earthaccess.streaming import (
    AuthContext,
    StreamingExecutor,
    StreamingIterator,
    WorkerContext,
    stream_process,
)

# =============================================================================
# AuthContext Tests
# =============================================================================


class TestAuthContext:
    """Tests for AuthContext dataclass."""

    def test_create_empty_context(self):
        """Test creating an empty AuthContext."""
        ctx = AuthContext()

        assert ctx.username is None
        assert ctx.password is None
        assert ctx.token is None
        assert ctx.s3_credentials == {}
        assert ctx.created_at is not None

    def test_create_with_credentials(self):
        """Test creating AuthContext with credentials."""
        ctx = AuthContext(
            username="testuser",
            password="testpass",
            token="testtoken",
        )

        assert ctx.username == "testuser"
        assert ctx.password == "testpass"
        assert ctx.token == "testtoken"

    def test_is_expired_no_expiry(self):
        """Test is_expired returns False when no expiry set."""
        ctx = AuthContext()

        assert ctx.is_expired() is False

    def test_with_s3_credentials(self):
        """Test adding S3 credentials creates new context."""
        ctx = AuthContext(username="testuser")
        creds = {
            "accessKeyId": "AKID123",
            "secretAccessKey": "secret",
            "sessionToken": "token",
        }

        new_ctx = ctx.with_s3_credentials("https://example.com/s3", creds)

        # Original unchanged
        assert ctx.s3_credentials == {}
        # New has credentials
        assert "https://example.com/s3" in new_ctx.s3_credentials
        assert (
            new_ctx.s3_credentials["https://example.com/s3"]["accessKeyId"] == "AKID123"
        )
        # Other fields preserved
        assert new_ctx.username == "testuser"

    def test_get_s3_credentials(self):
        """Test getting cached S3 credentials."""
        creds = {"accessKeyId": "AKID123"}
        ctx = AuthContext(s3_credentials={"https://example.com/s3": creds})

        result = ctx.get_s3_credentials("https://example.com/s3")
        assert result == creds

        result = ctx.get_s3_credentials("https://other.com/s3")
        assert result is None

    def test_frozen_dataclass(self):
        """Test that AuthContext is immutable."""
        ctx = AuthContext(username="testuser")

        with pytest.raises(AttributeError):
            ctx.username = "newuser"  # type: ignore


# =============================================================================
# WorkerContext Tests
# =============================================================================


class TestWorkerContext:
    """Tests for WorkerContext class."""

    def test_context_manager(self):
        """Test WorkerContext as context manager."""
        ctx = WorkerContext()

        # Before entering, current should be None
        assert WorkerContext.current() is None

        with ctx:
            # Inside context, current should return ctx
            assert WorkerContext.current() is ctx

        # After exiting, current should be None
        assert WorkerContext.current() is None

    def test_with_auth_context(self):
        """Test WorkerContext with AuthContext."""
        auth_ctx = AuthContext(username="testuser")
        ctx = WorkerContext(auth_context=auth_ctx)

        with ctx:
            current_auth = WorkerContext.get_auth_context()
            assert current_auth is auth_ctx
            assert current_auth.username == "testuser"

    def test_activate_with_new_auth(self):
        """Test activate method with new auth context."""
        ctx = WorkerContext()
        auth_ctx = AuthContext(username="testuser")

        with ctx.activate(auth_ctx):
            assert WorkerContext.get_auth_context() is auth_ctx

    def test_thread_isolation(self):
        """Test that WorkerContext is thread-local."""
        results: List[str] = []

        def worker(name: str):
            ctx = WorkerContext(metadata={"name": name})
            with ctx:
                time.sleep(0.01)  # Small delay to interleave
                current = WorkerContext.current()
                if current:
                    results.append(current.metadata["name"])

        threads = [
            threading.Thread(target=worker, args=("thread1",)),
            threading.Thread(target=worker, args=("thread2",)),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Each thread should see its own context
        assert "thread1" in results
        assert "thread2" in results


# =============================================================================
# StreamingIterator Tests
# =============================================================================


class TestStreamingIterator:
    """Tests for StreamingIterator class."""

    def test_basic_iteration(self):
        """Test basic iteration over items."""

        def producer(q: queue.Queue):
            for i in range(5):
                q.put(i)

        iterator = StreamingIterator(producer)
        results = list(iterator)

        assert results == [0, 1, 2, 3, 4]

    def test_lazy_evaluation(self):
        """Test that producer runs lazily."""
        produced = []

        def producer(q: queue.Queue):
            for i in range(3):
                produced.append(i)
                q.put(i)

        iterator = StreamingIterator(producer)

        # Producer hasn't started yet
        assert produced == []

        # Start consuming
        first = next(iterator)
        assert first == 0

        # Now producer has run
        # (may have produced more due to buffering)
        assert len(produced) > 0

    def test_backpressure(self):
        """Test that small buffer creates backpressure."""
        producer_blocked = threading.Event()
        buffer_full = threading.Event()

        def producer(q: queue.Queue):
            for i in range(10):
                if i == 5:
                    buffer_full.set()
                q.put(i)
            producer_blocked.set()

        # Small buffer
        iterator = StreamingIterator(producer, maxsize=3)

        # Start iteration to trigger producer
        first = next(iterator)
        assert first == 0

        # Give producer time to fill buffer
        time.sleep(0.1)

        # Buffer should block producer
        assert not producer_blocked.is_set()

        # Consume all items
        list(iterator)

        # Now producer should have finished
        assert producer_blocked.is_set()

    def test_error_propagation(self):
        """Test that producer errors are propagated."""

        def producer(q: queue.Queue):
            q.put(1)
            raise ValueError("Producer error")

        iterator = StreamingIterator(producer)

        # First item succeeds
        assert next(iterator) == 1

        # Next item raises error
        with pytest.raises(ValueError, match="Producer error"):
            next(iterator)

    def test_close_drains_queue(self):
        """Test that close() drains the queue."""

        def producer(q: queue.Queue):
            for i in range(100):
                q.put(i)

        iterator = StreamingIterator(producer, maxsize=10)

        # Get a few items
        next(iterator)
        next(iterator)

        # Close should not block
        iterator.close()

        # Iterator should be exhausted
        with pytest.raises(StopIteration):
            next(iterator)


# =============================================================================
# StreamingExecutor Tests
# =============================================================================


class TestStreamingExecutor:
    """Tests for StreamingExecutor class."""

    def test_basic_map(self):
        """Test basic map operation."""
        executor = StreamingExecutor(max_workers=2)

        results = list(executor.map(lambda x: x * 2, [1, 2, 3, 4, 5]))

        assert results == [2, 4, 6, 8, 10]

    def test_ordered_results(self):
        """Test that ordered=True maintains order."""
        executor = StreamingExecutor(max_workers=4)

        def slow_double(x: int) -> int:
            time.sleep(0.01 * (5 - x))  # Reverse order timing
            return x * 2

        results = list(executor.map(slow_double, [1, 2, 3, 4, 5], ordered=True))

        assert results == [2, 4, 6, 8, 10]

    def test_unordered_results(self):
        """Test that ordered=False returns as completed."""
        executor = StreamingExecutor(max_workers=4)

        def slow_double(x: int) -> int:
            time.sleep(0.01 * x)
            return x * 2

        results = list(executor.map(slow_double, [5, 1, 3], ordered=False))

        # Should have all results, order may vary
        assert sorted(results) == [2, 6, 10]

    def test_with_auth_context(self):
        """Test that auth context is passed to workers."""
        auth_ctx = AuthContext(username="testuser")
        executor = StreamingExecutor(max_workers=2, auth_context=auth_ctx)

        def check_auth(x: int) -> str:
            ctx = WorkerContext.get_auth_context()
            if ctx:
                return f"{x}:{ctx.username}"
            return f"{x}:no_auth"

        results = list(executor.map(check_auth, [1, 2, 3]))

        assert results == ["1:testuser", "2:testuser", "3:testuser"]

    def test_error_handling_raise(self):
        """Test error handling with on_error='raise'."""
        executor = StreamingExecutor(max_workers=2)

        def failing_func(x: int) -> int:
            if x == 3:
                raise ValueError("Error on 3")
            return x

        with pytest.raises(ValueError, match="Error on 3"):
            list(executor.map(failing_func, [1, 2, 3, 4]))

    def test_error_handling_skip(self):
        """Test error handling with on_error='skip'."""
        executor = StreamingExecutor(max_workers=2)

        def failing_func(x: int) -> int:
            if x == 3:
                raise ValueError("Error on 3")
            return x

        # on_error='skip' should skip errors and continue
        # Note: current implementation may not fully support skip, test what happens
        results = list(executor.map(failing_func, [1, 2, 3, 4], on_error="skip"))
        # If skip works, we should get [1, 2, 4]
        # If skip is not implemented, we'd raise ValueError
        assert 1 in results
        assert 2 in results
        assert 4 in results

    def test_context_manager(self):
        """Test executor as context manager."""
        with StreamingExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda x: x + 1, [1, 2, 3]))
            assert results == [2, 3, 4]


# =============================================================================
# stream_process Function Tests
# =============================================================================


class TestStreamProcess:
    """Tests for stream_process convenience function."""

    def test_basic_usage(self):
        """Test basic stream_process usage."""
        results = list(stream_process(lambda x: x * 2, [1, 2, 3]))

        assert results == [2, 4, 6]

    def test_with_auth(self):
        """Test stream_process with auth context."""
        auth_ctx = AuthContext(username="testuser")

        def check_auth(x: int) -> str:
            ctx = WorkerContext.get_auth_context()
            return f"{x}:{ctx.username if ctx else 'none'}"

        results = list(stream_process(check_auth, [1, 2], auth_context=auth_ctx))

        assert results == ["1:testuser", "2:testuser"]

    def test_custom_workers_and_buffer(self):
        """Test stream_process with custom workers and buffer."""
        results = list(
            stream_process(
                lambda x: x,
                range(10),
                max_workers=2,
                buffer_size=5,
            )
        )

        assert results == list(range(10))
