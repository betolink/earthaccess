from unittest.mock import MagicMock, patch

import pytest
from earthaccess.formatting import (
    STATIC_FILES,
    _load_static_files,
    _repr_collection_html,
    _repr_granule_html,
    _repr_search_results_html,
    has_widget_support,
)
from earthaccess.formatting.html import (
    _compute_summary,
    _format_collection_temporal,
    _format_temporal_extent,
    _generate_table_rows,
    _is_granule,
)
from earthaccess.search import DataCollection, DataGranule
from earthaccess.search.results import SearchResults


def test_load_static_files():
    # We simply test that the number of static files loaded is the same as the
    # number of files in the STATIC_FILES list.  If we were to add logic to
    # check the contents of the files, then we would end up duplicating the
    # logic in the _load_static_files function, which wouldn't make sense to do.
    # If _load_static_files contains a bug, then this test will likely fail due
    # to the function raising an exception.
    assert len(_load_static_files()) == len(STATIC_FILES)


def test_repr_granule_html():
    static_contents = _load_static_files()
    size1 = 128573
    size2 = 2713600
    umm = {
        "RelatedUrls": [
            {
                "URL": "https://data.csdap.earthdata.nasa.gov/data.h5",
                "Type": "GET DATA",
            },
            {
                "URL": "s3://csda-cumulus-prod-protected-5047/data.h5",
                "Type": "GET DATA VIA DIRECT ACCESS",
            },
            {
                "URL": "https://data.csdap.earthdata.nasa.gov/thumb.jpg",
                "Type": "GET RELATED VISUALIZATION",
            },
        ],
        "DataGranule": {
            "ArchiveAndDistributionInformation": [
                {"SizeInBytes": size1},
                {"SizeInBytes": size2},
            ],
        },
    }

    html = _repr_granule_html(DataGranule({"umm": umm}, cloud_hosted=True))

    assert f"{round((size1 + size2) / 1024 / 1024, 2)} MB" in html
    assert [url["URL"] in html for url in umm["RelatedUrls"]] == [True, False, True]
    assert all(content in html for content in static_contents)


# =============================================================================
# Tests for _repr_collection_html
# =============================================================================


def test_repr_collection_html_basic():
    """Test that _repr_collection_html returns valid HTML with expected content."""
    umm = {
        "ShortName": "ATL06",
        "Version": "005",
        "EntryTitle": "ATLAS/ICESat-2 L3A Land Ice Height V005",
        "Abstract": "This data set contains land ice surface heights.",
        "DOI": {"DOI": "10.5067/ATLAS/ATL06.005"},
        "RelatedUrls": [
            {
                "URL": "https://nsidc.org/data/ATL06",
                "Type": "LANDING PAGE",
            },
            {
                "URL": "https://n5eil01u.ecs.nsidc.org/ATLAS/ATL06.005/",
                "Type": "GET DATA",
            },
        ],
        "TemporalExtents": [
            {
                "RangeDateTimes": [
                    {
                        "BeginningDateTime": "2018-10-14T00:00:00.000Z",
                        "EndingDateTime": None,
                    }
                ]
            }
        ],
    }

    collection = DataCollection(
        {"umm": umm, "meta": {"concept-id": "C1234567-NSIDC", "provider-id": "NSIDC"}}
    )
    html = _repr_collection_html(collection)

    # Check that key elements are present
    assert "ATL06" in html
    assert "v005" in html or "005" in html
    assert "concept-id" in html.lower() or "C1234567-NSIDC" in html
    assert "NSIDC" in html
    assert "10.5067/ATLAS/ATL06.005" in html
    assert "bootstrap" in html.lower()


def test_repr_collection_html_no_doi():
    """Test that _repr_collection_html handles collections without DOI."""
    umm = {
        "ShortName": "TEST_COLLECTION",
        "Version": "1",
        "EntryTitle": "Test Collection",
        "Abstract": "A test collection without DOI.",
    }

    collection = DataCollection(
        {"umm": umm, "meta": {"concept-id": "C9999-TEST", "provider-id": "TEST"}}
    )
    html = _repr_collection_html(collection)

    assert "TEST_COLLECTION" in html
    assert "N/A" in html  # DOI should show N/A


def test_repr_collection_html_truncates_long_abstract():
    """Test that long abstracts are truncated."""
    long_abstract = "A" * 500  # 500 character abstract
    umm = {
        "ShortName": "LONG_ABSTRACT",
        "Abstract": long_abstract,
    }

    collection = DataCollection(
        {"umm": umm, "meta": {"concept-id": "C1-TEST", "provider-id": "TEST"}}
    )
    html = _repr_collection_html(collection)

    # Abstract should be truncated (300 chars + "...")
    assert "..." in html
    assert long_abstract not in html  # Full abstract should NOT be present


# =============================================================================
# Tests for _repr_search_results_html
# =============================================================================


def test_repr_search_results_html_empty():
    """Test HTML representation of empty SearchResults."""
    # Create a mock query
    mock_query = MagicMock()
    mock_query.hits.return_value = 0

    results = SearchResults(mock_query)
    results._total_hits = 0
    results._cached_results = []

    html = _repr_search_results_html(results)

    assert "SearchResults" in html
    assert "Total Hits" in html
    assert "0" in html
    assert "Cached" in html


def test_repr_search_results_html_with_granules():
    """Test HTML representation of SearchResults with cached granules."""
    mock_query = MagicMock()

    # Create mock granules
    granule_umm = {
        "GranuleUR": "SC:ATL06.005:123456",
        "TemporalExtent": {
            "RangeDateTime": {
                "BeginningDateTime": "2020-01-15T00:00:00.000Z",
                "EndingDateTime": "2020-01-15T01:00:00.000Z",
            }
        },
        "RelatedUrls": [{"URL": "https://example.com/data.h5", "Type": "GET DATA"}],
        "DataGranule": {
            "ArchiveAndDistributionInformation": [{"Size": 50.0, "SizeUnit": "MB"}]
        },
    }

    granules = [
        DataGranule(
            {"umm": granule_umm, "meta": {"concept-id": f"G{i}-TEST"}},
            cloud_hosted=True,
        )
        for i in range(5)
    ]

    results = SearchResults(mock_query)
    results._total_hits = 100
    results._cached_results = granules

    html = _repr_search_results_html(results)

    assert "SearchResults" in html
    assert "100" in html  # Total hits
    assert "5" in html  # Cached count
    assert "SC:ATL06.005:123456" in html or "ATL06" in html
    assert "Show Results" in html  # Collapsible section


def test_repr_search_results_html_with_collections():
    """Test HTML representation of SearchResults with cached collections."""
    mock_query = MagicMock()

    # Create mock collections
    collection_umm = {
        "ShortName": "ATL06",
        "Version": "005",
        "TemporalExtents": [
            {"RangeDateTimes": [{"BeginningDateTime": "2018-10-14T00:00:00.000Z"}]}
        ],
        "RelatedUrls": [
            {"URL": "https://nsidc.org/data/ATL06", "Type": "LANDING PAGE"}
        ],
    }

    collections = [
        DataCollection(
            {
                "umm": collection_umm,
                "meta": {"concept-id": f"C{i}-TEST", "provider-id": "TEST"},
            }
        )
        for i in range(3)
    ]

    results = SearchResults(mock_query)
    results._total_hits = 50
    results._cached_results = collections

    html = _repr_search_results_html(results)

    assert "SearchResults" in html
    assert "50" in html  # Total hits
    assert "collections" in html.lower()


# =============================================================================
# Tests for helper functions
# =============================================================================


def test_format_temporal_extent():
    """Test _format_temporal_extent function."""
    temporal = {
        "RangeDateTime": {
            "BeginningDateTime": "2020-01-15T10:30:00.000Z",
            "EndingDateTime": "2020-01-15T11:30:00.000Z",
        }
    }

    result = _format_temporal_extent(temporal)
    assert "2020-01-15" in result

    # Test short format
    result_short = _format_temporal_extent(temporal, short=True)
    assert result_short == "2020-01-15"


def test_format_temporal_extent_empty():
    """Test _format_temporal_extent with empty input."""
    result = _format_temporal_extent({})
    assert result == "N/A"


def test_format_collection_temporal():
    """Test _format_collection_temporal function."""
    temporal_extents = [
        {
            "RangeDateTimes": [
                {
                    "BeginningDateTime": "2018-10-14T00:00:00.000Z",
                    "EndingDateTime": "2023-12-31T23:59:59.999Z",
                }
            ]
        }
    ]

    result = _format_collection_temporal(temporal_extents)
    assert "2018-10-14" in result
    assert "2023-12-31" in result


def test_format_collection_temporal_ongoing():
    """Test _format_collection_temporal for ongoing collections."""
    temporal_extents = [
        {
            "RangeDateTimes": [
                {
                    "BeginningDateTime": "2018-10-14T00:00:00.000Z",
                    # No EndingDateTime = ongoing
                }
            ]
        }
    ]

    result = _format_collection_temporal(temporal_extents)
    assert "2018-10-14" in result
    assert "present" in result.lower()


def test_is_granule():
    """Test _is_granule function."""
    granule = DataGranule(
        {"umm": {"GranuleUR": "test-granule"}, "meta": {"concept-id": "G1-TEST"}}
    )
    collection = DataCollection(
        {"umm": {"ShortName": "TEST"}, "meta": {"concept-id": "C1-TEST"}}
    )

    assert _is_granule(granule) is True
    assert _is_granule(collection) is False
    assert _is_granule(None) is True  # Default assumption


def test_compute_summary_empty():
    """Test _compute_summary with empty list."""
    result = _compute_summary([])

    assert result["total_size_mb"] == 0.0
    assert result["cloud_count"] == 0
    assert result["temporal_range"] == "N/A"


def test_compute_summary_with_granules():
    """Test _compute_summary with granule data."""
    granule_umm = {
        "GranuleUR": "test-granule",
        "TemporalExtent": {
            "RangeDateTime": {
                "BeginningDateTime": "2020-01-15T00:00:00.000Z",
                "EndingDateTime": "2020-01-15T01:00:00.000Z",
            }
        },
        "DataGranule": {"ArchiveAndDistributionInformation": [{"Size": 100.0}]},
    }

    granules = [
        DataGranule(
            {"umm": granule_umm, "meta": {"concept-id": f"G{i}-TEST"}},
            cloud_hosted=(i % 2 == 0),  # Alternate cloud hosted
        )
        for i in range(4)
    ]

    result = _compute_summary(granules)

    assert result["total_size_mb"] == 400.0  # 4 * 100 MB
    assert result["cloud_count"] == 2  # 0, 2 are cloud hosted
    assert "2020-01-15" in result["temporal_range"]


def test_generate_table_rows_empty():
    """Test _generate_table_rows with empty list."""
    result = _generate_table_rows([])

    assert "No results cached" in result


def test_generate_table_rows_with_granules():
    """Test _generate_table_rows with granule data."""
    granule = DataGranule(
        {
            "umm": {
                "GranuleUR": "SC:ATL06.005:123456789",
                "TemporalExtent": {
                    "RangeDateTime": {
                        "BeginningDateTime": "2020-06-15T00:00:00.000Z",
                    }
                },
                "RelatedUrls": [
                    {"URL": "https://example.com/data.h5", "Type": "GET DATA"}
                ],
                "DataGranule": {"ArchiveAndDistributionInformation": [{"Size": 25.5}]},
            },
            "meta": {"concept-id": "G123-TEST"},
        },
        cloud_hosted=True,
    )

    result = _generate_table_rows([granule])

    assert "<tr>" in result
    assert "2020-06-15" in result
    assert "25.5" in result


# =============================================================================
# Tests for SearchResults methods
# =============================================================================


def test_search_results_repr_html():
    """Test that SearchResults._repr_html_() returns valid HTML."""
    mock_query = MagicMock()

    results = SearchResults(mock_query)
    results._total_hits = 42
    results._cached_results = []

    html = results._repr_html_()

    assert isinstance(html, str)
    assert "SearchResults" in html
    assert "42" in html


def test_search_results_summary_empty():
    """Test SearchResults.summary() with no cached results."""
    mock_query = MagicMock()

    results = SearchResults(mock_query)
    results._total_hits = 0
    results._cached_results = []

    summary = results.summary()

    assert summary["total_hits"] == 0
    assert summary["cached_count"] == 0
    assert summary["total_size_mb"] == 0.0
    assert summary["cloud_count"] == 0
    assert summary["temporal_range"] is None


def test_search_results_summary_with_data():
    """Test SearchResults.summary() with cached granules."""
    mock_query = MagicMock()

    granule_umm = {
        "GranuleUR": "test-granule",
        "TemporalExtent": {
            "RangeDateTime": {
                "BeginningDateTime": "2020-03-01T00:00:00.000Z",
                "EndingDateTime": "2020-03-01T12:00:00.000Z",
            }
        },
        "DataGranule": {"ArchiveAndDistributionInformation": [{"Size": 50.0}]},
    }

    granules = [
        DataGranule(
            {"umm": granule_umm, "meta": {"concept-id": f"G{i}-TEST"}},
            cloud_hosted=True,
        )
        for i in range(3)
    ]

    results = SearchResults(mock_query)
    results._total_hits = 3
    results._cached_results = granules

    summary = results.summary()

    assert summary["total_hits"] == 3
    assert summary["cached_count"] == 3
    assert summary["total_size_mb"] == 150.0  # 3 * 50 MB
    assert summary["cloud_count"] == 3
    assert "2020-03-01" in summary["temporal_range"]


def test_search_results_summary_skips_for_large_results():
    """Test that SearchResults.summary() skips detailed computation for large result sets."""
    mock_query = MagicMock()

    results = SearchResults(mock_query)
    results._total_hits = 15000  # > 10000 threshold
    results._cached_results = []

    summary = results.summary()

    # Should return basic info without detailed computation
    assert summary["total_hits"] == 15000
    assert summary["cached_count"] == 0
    assert summary["total_size_mb"] == 0.0


# =============================================================================
# Tests for has_widget_support
# =============================================================================


def test_has_widget_support_returns_bool():
    """Test that has_widget_support returns a boolean."""
    result = has_widget_support()
    assert isinstance(result, bool)


def test_has_widget_support_with_missing_deps():
    """Test has_widget_support when dependencies are missing."""
    with patch.dict("sys.modules", {"anywidget": None, "lonboard": None}):
        # Force reimport

        # The function should handle ImportError gracefully
        # Note: This is a simplified test; actual behavior depends on import caching
        result = has_widget_support()
        assert isinstance(result, bool)


# =============================================================================
# Tests for DataCollection._repr_html_ and show_map
# =============================================================================


def test_data_collection_repr_html():
    """Test that DataCollection._repr_html_() returns valid HTML."""
    collection = DataCollection(
        {
            "umm": {
                "ShortName": "TEST_COLLECTION",
                "Version": "1",
                "Abstract": "A test collection.",
            },
            "meta": {"concept-id": "C1-TEST", "provider-id": "TEST"},
        }
    )

    html = collection._repr_html_()

    assert isinstance(html, str)
    assert "TEST_COLLECTION" in html
    assert "bootstrap" in html.lower()


def test_data_granule_repr_html():
    """Test that DataGranule._repr_html_() returns valid HTML."""
    granule = DataGranule(
        {
            "umm": {
                "GranuleUR": "test-granule-123",
                "RelatedUrls": [
                    {"URL": "https://example.com/data.h5", "Type": "GET DATA"}
                ],
                "DataGranule": {"ArchiveAndDistributionInformation": [{"Size": 100.0}]},
            },
            "meta": {"concept-id": "G1-TEST"},
        },
        cloud_hosted=False,
    )

    html = granule._repr_html_()

    assert isinstance(html, str)
    assert "test-granule-123" in html
    assert "100" in html  # Size


# =============================================================================
# Tests for show_map methods (ImportError handling)
# =============================================================================


def test_search_results_show_map_import_error():
    """Test that SearchResults.show_map() raises ImportError when deps missing."""
    mock_query = MagicMock()
    results = SearchResults(mock_query)
    results._cached_results = [
        DataGranule(
            {
                "umm": {
                    "GranuleUR": "test",
                    "SpatialExtent": {
                        "HorizontalSpatialDomain": {
                            "Geometry": {
                                "BoundingRectangles": [
                                    {
                                        "WestBoundingCoordinate": -180,
                                        "SouthBoundingCoordinate": -90,
                                        "EastBoundingCoordinate": 180,
                                        "NorthBoundingCoordinate": 90,
                                    }
                                ]
                            }
                        }
                    },
                },
                "meta": {"concept-id": "G1-TEST"},
            }
        )
    ]

    # Mock the import to raise ImportError
    with patch(
        "earthaccess.formatting.widgets._check_widget_dependencies",
        side_effect=ImportError("Widget dependencies not installed"),
    ):
        with pytest.raises(ImportError):
            results.show_map()


def test_data_granule_show_map_import_error():
    """Test that DataGranule.show_map() raises ImportError when deps missing."""
    granule = DataGranule(
        {
            "umm": {
                "GranuleUR": "test-granule",
                "SpatialExtent": {
                    "HorizontalSpatialDomain": {
                        "Geometry": {
                            "BoundingRectangles": [
                                {
                                    "WestBoundingCoordinate": -10,
                                    "SouthBoundingCoordinate": 40,
                                    "EastBoundingCoordinate": 10,
                                    "NorthBoundingCoordinate": 50,
                                }
                            ]
                        }
                    }
                },
            },
            "meta": {"concept-id": "G1-TEST"},
        }
    )

    with patch(
        "earthaccess.formatting.widgets._check_widget_dependencies",
        side_effect=ImportError("Widget dependencies not installed"),
    ):
        with pytest.raises(ImportError):
            granule.show_map()


def test_data_collection_show_map_import_error():
    """Test that DataCollection.show_map() raises ImportError when deps missing."""
    collection = DataCollection(
        {
            "umm": {
                "ShortName": "TEST",
                "SpatialExtent": {
                    "HorizontalSpatialDomain": {
                        "Geometry": {
                            "BoundingRectangles": [
                                {
                                    "WestBoundingCoordinate": -180,
                                    "SouthBoundingCoordinate": -90,
                                    "EastBoundingCoordinate": 180,
                                    "NorthBoundingCoordinate": 90,
                                }
                            ]
                        }
                    }
                },
            },
            "meta": {"concept-id": "C1-TEST", "provider-id": "TEST"},
        }
    )

    with patch(
        "earthaccess.formatting.widgets._check_widget_dependencies",
        side_effect=ImportError("Widget dependencies not installed"),
    ):
        with pytest.raises(ImportError):
            collection.show_map()
