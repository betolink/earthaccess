"""Unit tests for external STAC catalog search functionality."""

import pytest

PYSTAC_AVAILABLE = False

try:
    from importlib.util import find_spec

    PYSTAC_AVAILABLE = find_spec("pystac_client") is not None
except ImportError:
    pass


class TestSearchStac:
    """Test search_stac function."""

    def test_search_stac_requires_pystac_client(self):
        """Test that pystac-client is required."""
        with pytest.raises(ImportError, match="pystac-client is required"):
            # Mock environment where pystac-client is not installed
            import sys

            pystac_client = sys.modules.get("pystac_client")
            if pystac_client:
                del sys.modules["pystac_client"]

            # This should raise ImportError
            try:
                from earthaccess.store_components.stac_search import search_stac

                search_stac("https://earth-search.aws.element84.com/v1")
            except ImportError:
                raise

    def test_search_stac_invalid_url(self):
        """Test that invalid URL raises ValueError."""
        # Test with pystac_client unavailable
        try:
            from earthaccess.store_components.stac_search import search_stac

            with pytest.raises(ValueError, match="Invalid STAC URL"):
                search_stac("")
        except ImportError:
            pytest.skip("pystac-client not installed")


class TestSTACItemResults:
    """Test STACItemResults class."""

    def test_stac_item_results_creation(self):
        """Test creating STACItemResults with mock search."""
        from unittest.mock import Mock

        from earthaccess.store_components.stac_search import STACItemResults

        mock_search = Mock()
        mock_search.matched.return_value = 10
        mock_search.items.return_value = []

        results = STACItemResults(
            search=mock_search,
            url="https://test.com",
            search_params={},
        )

        assert len(results) == 10
        assert results.matched() == 10
        assert results.count() == 10

    def test_stac_item_results_repr(self):
        """Test string representation."""
        from unittest.mock import Mock

        from earthaccess.store_components.stac_search import STACItemResults

        mock_search = Mock()
        mock_search.matched.return_value = 5
        mock_search.items.return_value = []

        results = STACItemResults(
            search=mock_search,
            url="https://test.com",
            search_params={},
        )

        assert "STACItemResults" in str(results)
        assert "5 items" in str(results)

    def test_stac_item_results_first_last(self):
        """Test getting first and last items."""
        from unittest.mock import Mock

        from earthaccess.store_components.stac_search import STACItemResults

        mock_items = [Mock(id=f"item{i}") for i in range(5)]
        mock_search = Mock()
        mock_search.matched.return_value = 5
        mock_search.items.return_value = mock_items

        results = STACItemResults(
            search=mock_search,
            url="https://test.com",
            search_params={},
        )

        first = results.first()
        last = results.last()

        assert first is not None
        assert first.id == "item0"
        assert last is not None
        assert last.id == "item4"

    def test_stac_item_results_preview(self):
        """Test previewing items."""
        from unittest.mock import Mock

        from earthaccess.store_components.stac_search import STACItemResults

        mock_items = [Mock(id=f"item{i}") for i in range(10)]
        mock_search = Mock()
        mock_search.matched.return_value = 10
        mock_search.items.return_value = mock_items

        results = STACItemResults(
            search=mock_search,
            url="https://test.com",
            search_params={},
        )

        # Use get_all() instead of items() to avoid consuming iterator
        preview = results.get_all()[:3]
        assert len(preview) == 3

    def test_stac_item_results_limit_items(self):
        """Test limiting number of items returned."""
        from unittest.mock import Mock

        from earthaccess.store_components.stac_search import STACItemResults

        mock_items = [Mock(id=f"item{i}") for i in range(10)]
        mock_search = Mock()
        mock_search.matched.return_value = 10
        mock_search.items.return_value = iter(mock_items)

        results = STACItemResults(
            search=mock_search,
            url="https://test.com",
            search_params={},
        )

        items = results.get_all()[:5]
        assert len(items) == 5
