"""Unit tests for external STAC catalog search functionality."""

import pytest


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
            from unittest.mock import Mock

            # Mock search to avoid import error
            import earthaccess.store_components.stac_search as stac_search_module
            from earthaccess.store_components.stac_search import (
                STACItemResults,
            )

            stac_search_module.search_stac = lambda *args, **kwargs: STACItemResults(
                search=Mock(matched=lambda: 10, items=lambda: []),
                url=args[0] if args else "mock-url",
                search_params=kwargs,
            )

            with pytest.raises(ValueError, match="Invalid STAC URL"):
                stac_search_module.search_stac("")
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
        mock_search.items.return_value = iter(mock_items)

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
        mock_search.items.return_value = iter(mock_items)

        results = STACItemResults(
            search=mock_search,
            url="https://test.com",
            search_params={},
        )

        preview = results.preview(limit=3)
        assert len(preview) == 3

    def test_stac_item_results_pages(self):
        """Test pagination."""
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

        pages = list(results.pages(page_size=3))
        assert len(pages) == 4
        assert len(pages[0]) == 3
        assert len(pages[1]) == 3
        assert len(pages[2]) == 3
        assert len(pages[3]) == 1

    def test_stac_item_results_to_dict(self):
        """Test converting results to dictionary."""
        from unittest.mock import Mock

        from earthaccess.store_components.stac_search import STACItemResults

        mock_item = Mock()
        mock_item.id = "test-id"
        mock_item.geometry = {"type": "Point", "coordinates": [0, 0]}
        mock_item.bbox = [-180, -90, 180, 90]
        mock_item.properties = {"test": "value"}
        mock_item.assets = {}
        mock_item.collection_id = "test-collection"
        mock_item.datetime.isoformat.return_value = "2023-01-01T00:00:00Z"

        mock_search = Mock()
        mock_search.matched.return_value = 1
        mock_search.items.return_value = iter([mock_item])

        results = STACItemResults(
            search=mock_search,
            url="https://test.com",
            search_params={"limit": 10},
        )

        result_dict = results.to_dict()

        assert result_dict["url"] == "https://test.com"
        assert result_dict["matched"] == 1
        assert result_dict["search_params"]["limit"] == 10
        assert len(result_dict["items"]) == 1

    def test_stac_item_results_empty(self):
        """Test empty results."""
        from unittest.mock import Mock

        from earthaccess.store_components.stac_search import STACItemResults

        mock_search = Mock()
        mock_search.matched.return_value = 0
        mock_search.items.return_value = iter([])

        results = STACItemResults(
            search=mock_search,
            url="https://test.com",
            search_params={},
        )

        assert len(results) == 0
        assert results.first() is None
        assert results.last() is None

    def test_stac_item_results_getitem(self):
        """Test getting items by index."""
        from unittest.mock import Mock

        from earthaccess.store_components.stac_search import STACItemResults

        mock_items = [Mock(id=f"item{i}") for i in range(5)]
        mock_search = Mock()
        mock_search.matched.return_value = 5
        mock_search.items.return_value = iter(mock_items)

        results = STACItemResults(
            search=mock_search,
            url="https://test.com",
            search_params={},
        )

        assert results[0].id == "item0"
        assert results[2].id == "item2"
        assert results[-1].id == "item4"

    def test_stac_item_results_iteration(self):
        """Test iterating over results."""
        from unittest.mock import Mock

        from earthaccess.store_components.stac_search import STACItemResults

        mock_items = [Mock(id=f"item{i}") for i in range(3)]
        mock_search = Mock()
        mock_search.matched.return_value = 3
        mock_search.items.return_value = iter(mock_items)

        results = STACItemResults(
            search=mock_search,
            url="https://test.com",
            search_params={},
        )

        collected_ids = [item.id for item in results]
        assert collected_ids == ["item0", "item1", "item2"]

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

        items = results.items(limit=5)
        assert len(items) == 5

    def test_stac_item_results_get_all(self):
        """Test get_all() alias."""
        from unittest.mock import Mock

        from earthaccess.store_components.stac_search import STACItemResults

        mock_items = [Mock(id=f"item{i}") for i in range(3)]
        mock_search = Mock()
        mock_search.matched.return_value = 3
        mock_search.items.return_value = iter(mock_items)

        results = STACItemResults(
            search=mock_search,
            url="https://test.com",
            search_params={},
        )

        items = results.get_all()
        assert len(items) == 3
