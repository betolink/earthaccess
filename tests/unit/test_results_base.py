"""Test results base classes with minimal type constraints."""

import pytest
from unittest.mock import Mock

from earthaccess.store_components.results import (
    ResultsBase,
    LazyResultsBase,
    StreamingExecutor,
)


class TestResultsBase:
    """Test ResultsBase functionality."""

    def test_init_empty(self):
        """Test initialization with no items."""
        results = ResultsBase()

        assert len(results) == 0
        assert results.get_all() == []
        assert results.matched() == 0

    def test_init_with_items(self):
        """Test initialization with items."""
        items = [1, 2, 3]
        results = ResultsBase(items)

        assert len(results) == 3
        assert results.get_all() == [1, 2, 3]
        assert results.matched() == 3

    def test_iteration(self):
        """Test iteration over items."""
        items = ["a", "b", "c"]
        results = ResultsBase(items)

        assert list(results) == ["a", "b", "c"]
        assert [item for item in results] == ["a", "b", "c"]

    def test_indexing(self):
        """Test item access by index."""
        items = ["a", "b", "c"]
        results = ResultsBase(items)

        assert results[0] == "a"
        assert results[1] == "b"
        assert results[2] == "c"

    def test_first_last(self):
        """Test first() and last() methods."""
        items = ["a", "b", "c"]
        results = ResultsBase(items)

        assert results.first() == "a"
        assert results.last() == "c"

    def test_empty_first_last(self):
        """Test first() and last() with empty results."""
        results = ResultsBase([])

        assert results.first() is None
        assert results.last() is None

    def test_filter(self):
        """Test filter functionality."""
        items = [1, 2, 3, 4, 5]
        results = ResultsBase(items)

        # Filter even numbers
        filtered = results.filter(lambda x: x % 2 == 0)
        assert list(filtered) == [2, 4]
        assert filtered.matched() == 2

    def test_map(self):
        """Test map functionality."""
        items = [1, 2, 3]
        results = ResultsBase(items)

        # Map to strings
        mapped = results.map(str)
        assert list(mapped) == ["1", "2", "3"]
        assert mapped.matched() == 3

    def test_preview(self):
        """Test preview functionality."""
        items = ["a", "b", "c", "d", "e"]
        results = ResultsBase(items)

        # Preview first 3
        preview = results.preview(3)
        assert list(preview) == ["a", "b", "c"]


class TestStreamingExecutor:
    """Test StreamingExecutor functionality."""

    def test_init(self):
        """Test executor initialization."""
        executor = StreamingExecutor()

        assert executor.max_workers == 8
        assert executor.prefetch_pages == 2
        assert executor.show_progress is True

    def test_map_functionality(self):
        """Test map method with mock data."""
        executor = StreamingExecutor(max_workers=2, show_progress=False)

        # Mock lazy results
        class MockLazyResults:
            def __init__(self, items):
                self.items = items

            def map(self, func):
                return [func(x) for x in self.items]

        results = MockLazyResults([1, 2, 3])

        # Simple function to double items
        def double(x):
            return x * 2

        mapped_results = list(executor.map(double, results))
        assert mapped_results == [2, 4, 6]

    def test_process_and_collect(self):
        """Test process_and_collect method."""
        executor = StreamingExecutor(max_workers=2, show_progress=False)

        class MockLazyResults:
            def __init__(self, items):
                self.items = items

            def map(self, func):
                return [func(x) for x in self.items]

        results = MockLazyResults([1, 2, 3])

        # Function to double items
        def double(x):
            return x * 2

        collected = executor.process_and_collect(double, results)
        assert collected == [2, 4, 6]


if __name__ == "__main__":
    # Run simple tests
    pytest.main([__file__])
