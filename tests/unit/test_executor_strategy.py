import unittest
from unittest.mock import Mock, patch

from earthaccess.store import Store


class TestExecutorAwareSessionStrategy(unittest.TestCase):
    """Test the executor-aware session strategy logic."""

    def setUp(self):
        """Set up a minimal mock auth for testing."""
        self.mock_auth = Mock()
        self.mock_auth.authenticated = True
        self.mock_auth.system = Mock()
        self.mock_auth.system.edl_hostname = "urs.earthdata.nasa.gov"

        # Mock the session to avoid actual HTTP calls
        mock_session = Mock()
        mock_session.request.return_value = Mock(status_code=200)
        mock_session.request.return_value.cookies = {}
        self.mock_auth.get_session.return_value = mock_session

    @patch("earthaccess.store._store_legacy.requests.Session")
    def test_executor_type_detection(self, mock_requests_session):
        """Test that executor types are correctly detected and session strategies selected."""
        # Mock the requests session to avoid actual HTTP calls
        mock_requests_session.return_value.request.return_value = Mock(status_code=200)

        # Create store with mocked auth
        store = Store(self.mock_auth, pre_authorize=False)

        # Test ThreadPoolExecutor detection
        store._set_executor_type("threads")
        self.assertEqual(store._current_executor_type, "threads")
        self.assertTrue(store._use_session_cloning())

        # Test serial execution detection
        store._set_executor_type("serial")
        self.assertEqual(store._current_executor_type, "serial")
        self.assertTrue(store._use_session_cloning())

        # Test Dask detection
        store._set_executor_type("dask")
        self.assertEqual(store._current_executor_type, "dask")
        self.assertFalse(store._use_session_cloning())

        # Test Lithops detection
        store._set_executor_type("lithops")
        self.assertEqual(store._current_executor_type, "lithops")
        self.assertFalse(store._use_session_cloning())

        # Test default fallback
        store._set_executor_type("unknown")
        self.assertEqual(store._current_executor_type, "threads")
        self.assertTrue(store._use_session_cloning())

        # Test boolean True (default threads)
        store._set_executor_type(True)
        self.assertEqual(store._current_executor_type, "threads")
        self.assertTrue(store._use_session_cloning())

        # Test boolean False (serial)
        store._set_executor_type(False)
        self.assertEqual(store._current_executor_type, "serial")
        self.assertTrue(store._use_session_cloning())

        # Test None (default threads)
        store._set_executor_type(None)
        self.assertEqual(store._current_executor_type, "threads")
        self.assertTrue(store._use_session_cloning())

    @patch("earthaccess.store._store_legacy.requests.Session")
    def test_session_strategy_consistency(self, mock_requests_session):
        """Test that session strategy is consistent across different executor types."""
        mock_requests_session.return_value.request.return_value = Mock(status_code=200)

        store = Store(self.mock_auth, pre_authorize=False)

        # Test all executor types that should use session cloning
        cloning_executors = ["threads", "threadpool", "serial", True, None]
        for executor in cloning_executors:
            store._set_executor_type(executor)
            self.assertTrue(
                store._use_session_cloning(),
                f"Executor {executor} should use session cloning",
            )

        # Test all executor types that should use per-worker authentication
        distributed_executors = ["dask", "lithops"]
        for executor in distributed_executors:
            store._set_executor_type(executor)
            self.assertFalse(
                store._use_session_cloning(),
                f"Executor {executor} should use per-worker authentication",
            )

    @patch("earthaccess.store._store_legacy.requests.Session")
    def test_download_method_sets_executor_type(self, mock_requests_session):
        """Test that download methods correctly set executor type."""
        mock_requests_session.return_value.request.return_value = Mock(status_code=200)

        store = Store(self.mock_auth, pre_authorize=False)

        # Test that _set_executor_type works with various inputs
        test_cases = [
            ("threads", "threads"),
            ("dask", "dask"),
            ("lithops", "lithops"),
            ("serial", "serial"),
            (True, "threads"),
            (False, "serial"),
            (None, "threads"),
        ]

        for input_val, expected in test_cases:
            store._set_executor_type(input_val)
            self.assertEqual(
                store._current_executor_type,
                expected,
                f"Input {input_val} should result in {expected}",
            )


if __name__ == "__main__":
    unittest.main()
