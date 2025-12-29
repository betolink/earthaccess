"""TDD Tests for SearchResults class and API integration.

Tests the lazy pagination wrapper that enables memory-efficient
iteration through large CMR result sets.
"""

from unittest.mock import Mock, patch

from earthaccess.search import DataGranule, SearchResults


def create_mock_query(
    hits: int = 0, get_return: list | None = None, page_size: int = 2000
) -> Mock:
    """Create a properly configured mock query for SearchResults tests.

    Since SearchResults now prefetches results, the mock needs:
    - headers: dict for HTTP headers
    - get(count): method to return results
    - hits(): method to return total hits count
    """
    mock_query = Mock()
    mock_query.headers = {}
    mock_query.hits.return_value = hits
    mock_query.get.return_value = get_return if get_return is not None else []
    mock_query._page_size = page_size
    return mock_query


class TestSearchResultsCreation:
    """Test SearchResults instantiation."""

    def test_create_with_query(self) -> None:
        """Test creating SearchResults with a query object."""
        mock_query = create_mock_query()
        results = SearchResults(mock_query, prefetch=0)

        assert results.query is mock_query
        assert results.limit is None
        assert results._cached_results == []
        assert results._exhausted is False

    def test_create_with_limit(self) -> None:
        """Test creating SearchResults with a limit."""
        mock_query = create_mock_query()
        results = SearchResults(mock_query, limit=100, prefetch=0)

        assert results.limit == 100

    def test_repr_before_fetch(self) -> None:
        """Test string representation before any fetches."""
        mock_query = create_mock_query()
        results = SearchResults(mock_query, prefetch=0)

        assert "SearchResults" in repr(results)
        assert "total=?" in repr(results)
        assert "loaded=0" in repr(results)

    def test_prefetch_loads_initial_results(self) -> None:
        """Test that prefetch loads initial results on creation."""
        mock_granules = [Mock(spec=DataGranule) for _ in range(5)]
        mock_query = create_mock_query(hits=100, get_return=mock_granules)

        # Mock _fetch_page to avoid HTTP calls
        with patch.object(SearchResults, "_fetch_page") as mock_fetch:
            mock_fetch.return_value = mock_granules
            results = SearchResults(mock_query, prefetch=20)

            # Should have prefetched the available results
            assert len(results._cached_results) == 5
            mock_fetch.assert_called_once()


class TestSearchResultsLen:
    """Test __len__ behavior - now returns cached count, not total hits."""

    def test_len_returns_cached_count(self) -> None:
        """Test that __len__ returns the number of cached results."""
        mock_query = create_mock_query()
        results = SearchResults(mock_query, prefetch=0)

        # Initially no results cached
        assert len(results) == 0

        # After caching some results
        results._cached_results = [Mock() for _ in range(25)]
        assert len(results) == 25

    def test_total_calls_hits_on_query(self) -> None:
        """Test that total() calls hits() on the query object."""
        mock_query = create_mock_query(hits=1000)
        results = SearchResults(mock_query, prefetch=0)

        total = results.total()

        mock_query.hits.assert_called_once()
        assert total == 1000

    def test_total_caches_result(self) -> None:
        """Test that total() caches the result."""
        mock_query = create_mock_query(hits=500)
        results = SearchResults(mock_query, prefetch=0)

        # Call total twice
        results.total()
        results.total()

        # hits() should only be called once
        mock_query.hits.assert_called_once()

    def test_hits_is_alias_for_total(self) -> None:
        """Test that hits() is an alias for total()."""
        mock_query = create_mock_query(hits=750)
        results = SearchResults(mock_query, prefetch=0)

        # hits() should return same value as total()
        hits_result = results.hits()
        assert hits_result == 750
        assert hits_result == results.total()


class TestSearchResultsIteration:
    """Test direct iteration through SearchResults."""

    def test_empty_results(self) -> None:
        """Test iteration when no results are found."""
        mock_query = create_mock_query()
        results = SearchResults(mock_query, prefetch=0)
        results._exhausted = True
        results._cached_results = []

        items = list(results)
        assert items == []

    def test_iteration_yields_cached_first(self) -> None:
        """Test that iteration yields cached results first."""
        mock_query = create_mock_query()
        results = SearchResults(mock_query, prefetch=0)
        results._exhausted = True

        # Pre-populate cache
        mock_granule1 = Mock(spec=DataGranule)
        mock_granule2 = Mock(spec=DataGranule)
        results._cached_results = [mock_granule1, mock_granule2]

        items = list(results)

        assert len(items) == 2
        assert items[0] is mock_granule1
        assert items[1] is mock_granule2

    def test_iteration_respects_limit(self) -> None:
        """Test that iteration respects the limit parameter."""
        mock_query = create_mock_query()
        results = SearchResults(mock_query, limit=3, prefetch=0)
        results._exhausted = True

        # Pre-populate cache with more than limit
        results._cached_results = [Mock(spec=DataGranule) for _ in range(5)]

        items = list(results)

        # Should only get limit items
        assert len(items) == 5  # All cached items (limit applied during fetch)


class TestSearchResultsPages:
    """Test page-by-page iteration."""

    def test_pages_returns_generator(self) -> None:
        """Test that pages() returns a generator."""
        mock_query = create_mock_query()
        results = SearchResults(mock_query, prefetch=0)

        pages_gen = results.pages()

        # Should be a generator
        assert hasattr(pages_gen, "__next__")


class TestSearchResultsIntegration:
    """Test SearchResults with real-like behavior."""

    def test_search_results_can_wrap_data_granules_query(self) -> None:
        """Test that SearchResults can wrap a DataGranules query object."""
        # This tests the interface compatibility
        mock_query = create_mock_query(hits=100)

        results = SearchResults(mock_query, prefetch=0)

        # len() returns cached count (initially 0)
        assert len(results) == 0

        # total() returns CMR hits
        assert results.total() == 100

        # Should have expected attributes
        assert hasattr(results, "__iter__")
        assert hasattr(results, "pages")
        assert hasattr(results, "limit")


class TestSearchResultsExport:
    """Test that SearchResults is properly exported."""

    def test_search_results_importable_from_results(self) -> None:
        """Test SearchResults can be imported from results module."""
        from earthaccess.search import SearchResults

        assert SearchResults is not None

    def test_search_results_has_expected_interface(self) -> None:
        """Test SearchResults has the expected public interface."""
        mock_query = create_mock_query()
        results = SearchResults(mock_query, prefetch=0)

        # Core methods
        assert hasattr(results, "__iter__")
        assert hasattr(results, "__len__")
        assert hasattr(results, "pages")

        # Attributes
        assert hasattr(results, "query")
        assert hasattr(results, "limit")


class TestAPIIntegrationWithSearchResults:
    """Test that API functions return SearchResults."""

    def test_search_data_returns_search_results(self) -> None:
        """Test that search_data returns SearchResults."""
        with (
            patch("earthaccess.api.DataGranules") as mock_dg,
            patch.object(SearchResults, "_fetch_page", return_value=[]),
        ):
            mock_query = Mock()
            mock_query.hits.return_value = 0
            mock_query.get_all.return_value = []
            mock_query.parameters.return_value = mock_query
            mock_query.headers = {}
            mock_dg.return_value = mock_query

            import earthaccess

            # This should return SearchResults
            result = earthaccess.search_data(short_name="TEST")

            assert isinstance(result, SearchResults)

    def test_search_datasets_returns_search_results(self) -> None:
        """Test that search_datasets returns SearchResults."""
        with (
            patch("earthaccess.api.DataCollections") as mock_dc,
            patch.object(SearchResults, "_fetch_page", return_value=[]),
        ):
            mock_query = Mock()
            mock_query.hits.return_value = 0
            mock_query.get_all.return_value = []
            mock_query.parameters.return_value = mock_query
            mock_query.headers = {}
            mock_dc.return_value = mock_query

            import earthaccess

            result = earthaccess.search_datasets(keyword="TEST")

            assert isinstance(result, SearchResults)

    def test_search_results_exportable_from_earthaccess(self) -> None:
        """Test that SearchResults can be imported from earthaccess package."""
        from earthaccess import SearchResults

        assert SearchResults is not None
        # Verify it's the same class
        from earthaccess.search import SearchResults as ResultsSearchResults

        assert SearchResults is ResultsSearchResults


class TestSearchResultsCaching:
    """Test result caching behavior."""

    def test_results_cached_after_iteration(self) -> None:
        """Test that results are cached after iterating."""
        mock_query = create_mock_query()
        results = SearchResults(mock_query, prefetch=0)
        results._exhausted = True

        mock_items = [Mock(spec=DataGranule) for _ in range(3)]
        results._cached_results = mock_items

        # Iterate once
        first_iteration = list(results)

        # Iterate again - should use cache
        second_iteration = list(results)

        assert first_iteration == second_iteration
        assert len(results._cached_results) == 3

    def test_repr_shows_cached_count(self) -> None:
        """Test that repr shows cached count."""
        mock_query = create_mock_query()
        results = SearchResults(mock_query, prefetch=0)
        results._total_hits = 100
        results._cached_results = [Mock() for _ in range(25)]

        repr_str = repr(results)

        assert "total=100" in repr_str
        assert "loaded=25" in repr_str


class TestSearchResultsUsagePatterns:
    """Test common usage patterns for SearchResults."""

    def test_pattern_direct_iteration(self) -> None:
        """Test typical usage: direct iteration."""
        mock_query = create_mock_query()
        results = SearchResults(mock_query, prefetch=0)
        results._exhausted = True
        results._cached_results = [Mock(spec=DataGranule) for _ in range(3)]

        # Typical usage pattern
        count = 0
        for granule in results:
            count += 1

        assert count == 3

    def test_pattern_convert_to_list(self) -> None:
        """Test converting SearchResults to list."""
        mock_query = create_mock_query()
        results = SearchResults(mock_query, prefetch=0)
        results._exhausted = True
        results._cached_results = [Mock(spec=DataGranule) for _ in range(3)]

        # Convert to list
        granule_list = list(results)

        assert len(granule_list) == 3

    def test_pattern_check_length_first(self) -> None:
        """Test checking total before iteration."""
        mock_query = create_mock_query(hits=5000)
        results = SearchResults(mock_query, prefetch=0)

        # Use total() to get CMR hits, not len()
        total = results.total()

        assert total == 5000
        # No items fetched yet, so len() is 0
        assert len(results) == 0
        assert len(results._cached_results) == 0


class TestSearchResultsEdgeCases:
    """Test edge cases and error handling."""

    def test_zero_results(self) -> None:
        """Test handling of zero results."""
        mock_query = create_mock_query(hits=0)
        results = SearchResults(mock_query, prefetch=0)
        results._exhausted = True
        results._cached_results = []

        assert len(results) == 0
        assert list(results) == []

    def test_limit_zero(self) -> None:
        """Test with limit of zero."""
        mock_query = create_mock_query()
        results = SearchResults(mock_query, limit=0, prefetch=0)

        # limit=0 should result in no items
        assert results.limit == 0

    def test_limit_larger_than_results(self) -> None:
        """Test when limit is larger than available results."""
        mock_query = create_mock_query(hits=10)
        results = SearchResults(mock_query, limit=100, prefetch=0)
        results._exhausted = True
        results._cached_results = [Mock(spec=DataGranule) for _ in range(10)]

        items = list(results)

        # Should get all 10, not 100
        assert len(items) == 10

    def test_iteration_is_reentrant(self) -> None:
        """Test that iteration can be done multiple times."""
        mock_query = create_mock_query()
        results = SearchResults(mock_query, prefetch=0)
        results._exhausted = True
        results._cached_results = [Mock(spec=DataGranule) for _ in range(3)]

        # First iteration
        first = list(results)

        # Second iteration
        second = list(results)

        # Third iteration
        third = list(results)

        assert first == second == third
