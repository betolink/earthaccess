"""Unit tests for the parallel execution module."""

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

import pytest
from earthaccess.store.parallel import (
    SerialExecutor,
    ThreadPoolExecutorWrapper,
    get_executor,
)


class TestSerialExecutor:
    """Test the SerialExecutor class."""

    def test_submit_basic(self):
        """Test basic submit functionality."""
        executor = SerialExecutor()

        def add(a, b):
            return a + b

        future = executor.submit(add, 2, 3)
        assert future.result() == 5

    def test_submit_with_exception(self):
        """Test submit with exception."""
        executor = SerialExecutor()

        def raise_error():
            raise ValueError("Test error")

        future = executor.submit(raise_error)
        with pytest.raises(ValueError, match="Test error"):
            future.result()

    def test_map_basic(self):
        """Test basic map functionality."""
        executor = SerialExecutor()

        def square(x):
            return x * x

        results = list(executor.map(square, [1, 2, 3, 4]))
        assert results == [1, 4, 9, 16]

    def test_shutdown(self):
        """Test shutdown is a no-op."""
        executor = SerialExecutor()
        executor.shutdown()  # Should not raise any exception
        executor.shutdown(wait=False, cancel_futures=True)  # Should not raise


class TestThreadPoolExecutorWrapper:
    """Test the ThreadPoolExecutorWrapper class."""

    def test_submit_basic(self):
        """Test basic submit functionality."""
        executor = ThreadPoolExecutorWrapper(max_workers=2)

        def add(a, b):
            return a + b

        future = executor.submit(add, 2, 3)
        assert future.result() == 5

        executor.shutdown()

    def test_map_basic(self):
        """Test basic map functionality."""
        executor = ThreadPoolExecutorWrapper(max_workers=2)

        def square(x):
            return x * x

        results = list(executor.map(square, [1, 2, 3, 4]))
        assert results == [1, 4, 9, 16]

        executor.shutdown()

    def test_shutdown(self):
        """Test shutdown functionality."""
        executor = ThreadPoolExecutorWrapper(max_workers=2)
        executor.shutdown()  # Should not raise any exception


class TestGetExecutor:
    """Test the get_executor factory function."""

    def test_default_true(self):
        """Test default behavior (True)."""
        executor = get_executor(True)
        assert isinstance(executor, ThreadPoolExecutorWrapper)

    def test_threads_string(self):
        """Test 'threads' string."""
        executor = get_executor("threads")
        assert isinstance(executor, ThreadPoolExecutorWrapper)

    def test_thread_string(self):
        """Test 'thread' string."""
        executor = get_executor("thread")
        assert isinstance(executor, ThreadPoolExecutorWrapper)

    def test_threadpool_string(self):
        """Test 'threadpool' string."""
        executor = get_executor("threadpool")
        assert isinstance(executor, ThreadPoolExecutorWrapper)

    def test_false(self):
        """Test False for serial execution."""
        executor = get_executor(False)
        assert isinstance(executor, SerialExecutor)

    def test_serial_string(self):
        """Test 'serial' string."""
        executor = get_executor("serial")
        assert isinstance(executor, SerialExecutor)

    def test_none_string(self):
        """Test 'none' string."""
        executor = get_executor("none")
        assert isinstance(executor, SerialExecutor)

    def test_disabled_string(self):
        """Test 'disabled' string."""
        executor = get_executor("disabled")
        assert isinstance(executor, SerialExecutor)

    def test_none_parameter(self):
        """Test None parameter."""
        executor = get_executor(None)
        assert isinstance(executor, ThreadPoolExecutorWrapper)

    def test_custom_executor(self):
        """Test passing a custom executor."""
        custom_executor = ThreadPoolExecutor(max_workers=4)
        executor = get_executor(custom_executor)
        assert executor is custom_executor
        custom_executor.shutdown()

    def test_max_workers_parameter(self):
        """Test max_workers parameter."""
        executor = get_executor("threads", max_workers=4)
        # We can't easily test the max_workers without accessing internals
        # but we can verify it's the right type
        assert isinstance(executor, ThreadPoolExecutorWrapper)
        executor.shutdown()

    def test_invalid_string(self):
        """Test invalid string raises ValueError."""
        with pytest.raises(ValueError, match="Unrecognized parallel backend"):
            get_executor("invalid")

    def test_invalid_type(self):
        """Test invalid type raises ValueError."""
        with pytest.raises(ValueError, match="Invalid parallel argument"):
            get_executor(123)  # type: ignore

    @patch("earthaccess.store.parallel.DaskDelayedExecutor")
    def test_dask_string_available(self, mock_dask_executor):
        """Test 'dask' string when dask is available."""
        mock_executor = Mock()
        mock_dask_executor.return_value = mock_executor

        executor = get_executor("dask")
        assert executor is mock_executor
        mock_dask_executor.assert_called_once_with(max_workers=None, show_progress=True)

    @patch("earthaccess.store.parallel.DaskDelayedExecutor")
    @patch("earthaccess.store.parallel.warnings")
    def test_dask_string_unavailable(self, mock_warnings, mock_dask_executor):
        """Test 'dask' string when dask is unavailable."""
        mock_dask_executor.side_effect = ImportError("Dask not available")

        executor = get_executor("dask")
        assert isinstance(executor, ThreadPoolExecutorWrapper)
        mock_warnings.warn.assert_called_once()
        assert "Dask is not installed" in str(mock_warnings.warn.call_args)
