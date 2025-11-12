import tempfile
from pathlib import Path
from unittest.mock import Mock
from earthaccess import Store


def test_executor_type_detection():
    """Test that executor types are correctly detected and session strategies selected."""
    # Create a mock auth object that passes the auth check
    mock_auth = Mock()
    mock_auth.authenticated = True
    mock_auth.system = Mock()
    mock_auth.system.edl_hostname = "urs.earthdata.nasa.gov"
    mock_auth.get_session = Mock()

    # Create store with mock auth
    store = Store(mock_auth)

    # Test ThreadPoolExecutor detection
    store._set_executor_type("threads")
    assert store._current_executor_type == "threads"
    assert store._use_session_cloning() is True

    # Test serial execution detection
    store._set_executor_type("serial")
    assert store._current_executor_type == "serial"
    assert store._use_session_cloning() is True

    # Test Dask detection
    store._set_executor_type("dask")
    assert store._current_executor_type == "dask"
    assert store._use_session_cloning() is False

    # Test Lithops detection
    store._set_executor_type("lithops")
    assert store._current_executor_type == "lithops"
    assert store._use_session_cloning() is False

    # Test default fallback
    store._set_executor_type("unknown")
    assert store._current_executor_type == "threads"
    assert store._use_session_cloning() is True

    # Test boolean True (default threads)
    store._set_executor_type(True)
    assert store._current_executor_type == "threads"
    assert store._use_session_cloning() is True

    # Test boolean False (serial)
    store._set_executor_type(False)
    assert store._current_executor_type == "serial"
    assert store._use_session_cloning() is True

    # Test None (default threads)
    store._set_executor_type(None)
    assert store._current_executor_type == "threads"
    assert store._use_session_cloning() is True


def test_session_strategy_consistency():
    """Test that session strategy is consistent across different executor types."""
    # Create a mock auth object
    mock_auth = Mock()
    mock_auth.authenticated = True
    mock_auth.system = Mock()
    mock_auth.system.edl_hostname = "urs.earthdata.nasa.gov"
    mock_auth.get_session = Mock()

    store = Store(mock_auth)

    # Test all executor types that should use session cloning
    cloning_executors = ["threads", "threadpool", "serial", True, None]
    for executor in cloning_executors:
        store._set_executor_type(executor)
        assert store._use_session_cloning() is True, (
            f"Executor {executor} should use session cloning"
        )

    # Test all executor types that should use per-worker authentication
    distributed_executors = ["dask", "lithops"]
    for executor in distributed_executors:
        store._set_executor_type(executor)
        assert store._use_session_cloning() is False, (
            f"Executor {executor} should use per-worker authentication"
        )


def test_download_method_sets_executor_type():
    """Test that download methods correctly set executor type."""
    # Create a mock auth object
    mock_auth = Mock()
    mock_auth.authenticated = True
    mock_auth.system = Mock()
    mock_auth.system.edl_hostname = "urs.earthdata.nasa.gov"
    mock_auth.get_session = Mock()

    store = Store(mock_auth)

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
        assert store._current_executor_type == expected, (
            f"Input {input_val} should result in {expected}"
        )
