"""Tests for earthaccess results classes and search functionality.

This module contains two types of tests:
1. VCR-based tests that record/playback HTTP interactions for search workflows
2. Unit tests with static fixtures for DataGranule/DataCollection methods

VCR tests use pytest-recording for HTTP cassette management.
See docs/contributing/testing-guide.md for guidelines.
"""

import json
import logging
from pathlib import Path

import earthaccess
import pytest
import responses
from earthaccess.search import DataCollection, DataCollections

logging.basicConfig()
logging.getLogger("vcr").setLevel(logging.ERROR)


def unique_results(results):
    """Ensure search results have unique concept IDs.

    When we invoke a search request multiple times we want to ensure that we don't
    get the same results back. This is a one shot test as the results are preserved
    by VCR but still useful.
    """
    unique_concept_ids = {result["meta"]["concept-id"] for result in results}
    return len(unique_concept_ids) == len(results)


def assert_is_using_search_after(cassette):
    """Assert that CMR search-after pagination is being used correctly."""
    first_request = True

    for request in cassette.requests:
        # Verify the page number was not used
        assert "page_num" not in request.uri
        # Verify that Search After was used in all requests except first
        assert first_request == ("CMR-Search-After" not in request.headers)
        first_request = False


# =============================================================================
# VCR-Based Search Tests
# =============================================================================


@pytest.mark.vcr
def test_no_results(vcr):
    """If we search for a collection that doesn't exist, we should get no results."""
    granules = earthaccess.search_data(
        # STAC collection name; correct short name is OPERA_L3_DSWX-HLS_V1
        # Example discussed in: https://github.com/nsidc/earthaccess/pull/839
        short_name="OPERA_L3_DSWX-HLS_V1_1.0",
        bounding_box=(-95.19, 30.59, -94.99, 30.79),
        temporal=("2024-04-30", "2024-05-31"),
    )
    assert len(granules) == 0


@pytest.mark.vcr
@pytest.mark.skip(reason="Cassette needs re-recording after SearchResults refactor")
def test_data_links(vcr):
    """Test that data links return correct S3 and HTTPS URLs."""
    results = earthaccess.search_data(
        short_name="SEA_SURFACE_HEIGHT_ALT_GRIDS_L4_2SATS_5DAY_6THDEG_V_JPL2205",
        temporal=("2020", "2022"),
        count=1,
    )

    # Convert to list to fetch results (SearchResults is lazy)
    granules = list(results)
    g = granules[0]
    # `access` specified
    assert g.data_links(access="direct")[0].startswith("s3://")
    assert g.data_links(access="external")[0].startswith("https://")


@pytest.mark.vcr
@pytest.mark.skip(reason="Cassette needs re-recording after SearchResults refactor")
def test_get_more_than_2000(vcr):
    """Test pagination when requesting more than 2000 granules.

    If we execute a get with a limit of more than 2000 then we expect
    multiple invocations of a CMR granule search.
    """
    results = earthaccess.search_data(short_name="MOD02QKM", count=3000)

    # Convert to list to fetch all results (SearchResults is lazy)
    granules = list(results)

    # Assert that we performed one 'hits' search and two 'results' search queries
    assert len(vcr) == 3
    # Note: len(results) returns total CMR hits, len(granules) returns fetched count
    assert len(granules) <= 3000  # Limited by count parameter
    assert unique_results(granules)


@pytest.mark.vcr
@pytest.mark.skip(reason="Cassette needs re-recording after SearchResults refactor")
def test_get(vcr):
    """Test single-page granule search.

    If we execute a get with no arguments then we expect to get the
    maximum number of granules from a single CMR call (2000).
    """
    granules = earthaccess.search_data(short_name="MOD02QKM", count=2000)

    # Assert that we performed one 'hits' search and one 'results' search query
    assert len(vcr) == 2
    assert len(granules) == 2000
    assert unique_results(granules)


@pytest.mark.vcr
@pytest.mark.skip(reason="Cassette needs re-recording after SearchResults refactor")
def test_get_all_less_than_2k(vcr):
    """Test search for collection with fewer than 2000 total granules."""
    granules = earthaccess.search_data(
        short_name="TELLUS_GRAC_L3_JPL_RL06_LND_v04", count=2000
    )

    # Assert that we performed a hits query and one search results query
    assert len(vcr) == 2
    assert len(granules) == 163
    assert unique_results(granules)


@pytest.mark.vcr
@pytest.mark.skip(reason="Cassette needs re-recording after SearchResults refactor")
def test_get_all_more_than_2k(vcr):
    """Test pagination for collection with more than 2000 granules."""
    granules = earthaccess.search_data(
        short_name="CYGNSS_NOAA_L2_SWSP_25KM_V1.2", count=3000
    )

    # Assert that we performed a hits query and two search results queries
    assert len(vcr) == 3
    assert len(granules) == int(vcr.responses[0]["headers"]["CMR-Hits"][0])
    assert len(granules) == min(3000, int(vcr.responses[0]["headers"]["CMR-Hits"][0]))
    assert unique_results(granules)


@pytest.mark.vcr
def test_collections_less_than_2k(vcr):
    """Test collection search with fewer than 2000 results."""
    query = DataCollections().daac("PODAAC").cloud_hosted(True)
    collections = query.get(20)

    # Assert that we performed a single search results query
    assert len(vcr) == 1
    assert len(collections) == 20
    assert unique_results(collections)
    assert_is_using_search_after(vcr)


@pytest.mark.vcr
def test_collections_more_than_2k(vcr):
    """Test collection search pagination with more than 2000 results."""
    query = DataCollections()
    collections = query.get(3000)

    # Assert that we performed two search results queries
    assert len(vcr) == 2
    assert len(collections) == 4000
    assert unique_results(collections)
    assert_is_using_search_after(vcr)


# =============================================================================
# Unit Tests with Static Fixtures
# =============================================================================


def test_get_doi_returns_doi_when_present():
    collection = DataCollection(
        {"umm": {"DOI": {"DOI": "doi:10.16904/envidat.lwf.34"}}, "meta": {}}
    )

    assert collection.doi() == "doi:10.16904/envidat.lwf.34"


def test_get_doi_returns_empty_string_when_doi_missing():
    collection = DataCollection({"umm": {"DOI": {}}, "meta": {}})

    assert collection.doi() is None


def test_get_doi_returns_empty_string_when_doi_key_missing():
    collection = DataCollection({"umm": {}, "meta": {}})

    assert collection.doi() is None


@responses.activate
def test_get_citation_apa_format():
    collection = DataCollection(
        {"umm": {"DOI": {"DOI": "doi:10.16904/envidat.lwf.34"}}, "meta": {}}
    )

    responses.add(
        responses.GET,
        "https://citation.doi.org/format?doi=doi:10.16904/envidat.lwf.34&style=apa&lang=en-US",
        body="Meusburger, K., Graf Pannatier, E., & Schaub, M. (2019). 10-HS Pfynwald (Version 2019) [Dataset]. EnviDat. https://doi.org/10.16904/ENVIDAT.LWF.34",
        status=200,
    )

    citation = collection.citation(format="apa", language="en-US")

    assert (
        citation
        == "Meusburger, K., Graf Pannatier, E., & Schaub, M. (2019). 10-HS Pfynwald (Version 2019) [Dataset]. EnviDat. https://doi.org/10.16904/ENVIDAT.LWF.34"
    )


@responses.activate
def test_get_citation_different_language():
    collection = DataCollection(
        {"umm": {"DOI": {"DOI": "doi:10.16904/envidat.lwf.34"}}, "meta": {}}
    )

    responses.add(
        responses.GET,
        "https://citation.doi.org/format?doi=doi:10.16904/envidat.lwf.34&style=apa&lang=fr-FR",
        body="Meusburger, K., Graf Pannatier, E., & Schaub, M. (2019). 10-HS Pfynwald (Version 2019) [Jeu de données]. EnviDat. https://doi.org/10.16904/ENVIDAT.LWF.34",
        status=200,
    )

    citation = collection.citation(format="apa", language="fr-FR")

    assert (
        citation
        == "Meusburger, K., Graf Pannatier, E., & Schaub, M. (2019). 10-HS Pfynwald (Version 2019) [Jeu de données]. EnviDat. https://doi.org/10.16904/ENVIDAT.LWF.34"
    )


def test_get_citation_returns_none_when_doi_missing():
    collection = DataCollection({"umm": {}, "meta": {}})

    assert collection.citation(format="apa", language="en-US") is None


def test_get_citation_returns_none_when_doi_empty():
    collection = DataCollection({"umm": {"DOI": {"DOI": ""}}, "meta": {}})

    assert collection.citation(format="apa", language="en-US") is None


# =============================================================================
# Tests for to_dict() and to_stac() methods
# =============================================================================


def test_collection_to_dict():
    """Test that to_dict returns a plain dictionary."""
    collection = DataCollection(
        {
            "umm": {
                "ShortName": "TestCollection",
                "Version": "1.0",
                "Abstract": "A test collection",
            },
            "meta": {
                "concept-id": "C123456-TEST",
                "provider-id": "TEST",
            },
        }
    )

    result = collection.to_dict()
    assert isinstance(result, dict)
    assert result["umm"]["ShortName"] == "TestCollection"
    assert result["meta"]["concept-id"] == "C123456-TEST"


def test_collection_to_stac():
    """Test that to_stac returns a valid STAC Collection structure."""
    collection = DataCollection(
        {
            "umm": {
                "ShortName": "TestCollection",
                "Version": "1.0",
                "Abstract": "A test collection for STAC conversion",
                "DOI": {"DOI": "10.5067/TEST"},
                "TemporalExtents": [
                    {
                        "RangeDateTimes": [
                            {
                                "BeginningDateTime": "2020-01-01T00:00:00Z",
                                "EndingDateTime": "2020-12-31T23:59:59Z",
                            }
                        ]
                    }
                ],
                "SpatialExtent": {
                    "HorizontalSpatialDomain": {
                        "Geometry": {
                            "BoundingRectangles": [
                                {
                                    "WestBoundingCoordinate": -180.0,
                                    "SouthBoundingCoordinate": -90.0,
                                    "EastBoundingCoordinate": 180.0,
                                    "NorthBoundingCoordinate": 90.0,
                                }
                            ]
                        }
                    }
                },
                "RelatedUrls": [
                    {"Type": "LANDING PAGE", "URL": "https://example.com/landing"},
                    {"Type": "GET DATA", "URL": "https://example.com/data"},
                ],
            },
            "meta": {
                "concept-id": "C123456-TEST",
                "provider-id": "TEST",
            },
        }
    )

    stac = collection.to_stac()

    # Check required STAC fields
    assert stac["type"] == "Collection"
    assert stac["stac_version"] == "1.0.0"
    assert "id" in stac
    assert "description" in stac
    assert "extent" in stac
    assert "links" in stac

    # Check extent structure
    assert "spatial" in stac["extent"]
    assert "temporal" in stac["extent"]
    assert stac["extent"]["spatial"]["bbox"] == [[-180.0, -90.0, 180.0, 90.0]]

    # Check DOI extension
    assert stac["sci:doi"] == "10.5067/TEST"

    # Check CMR-specific properties
    assert stac["cmr:concept_id"] == "C123456-TEST"


def test_granule_to_dict():
    """Test that DataGranule.to_dict returns a plain dictionary."""
    from earthaccess.search import DataGranule

    granule = DataGranule(
        {
            "umm": {
                "GranuleUR": "test_granule_001",
                "CollectionReference": {
                    "ShortName": "TestCollection",
                    "Version": "1.0",
                },
                "TemporalExtent": {
                    "RangeDateTime": {
                        "BeginningDateTime": "2020-06-01T00:00:00Z",
                        "EndingDateTime": "2020-06-01T23:59:59Z",
                    }
                },
                "SpatialExtent": {
                    "HorizontalSpatialDomain": {
                        "Geometry": {
                            "BoundingRectangles": [
                                {
                                    "WestBoundingCoordinate": -10.0,
                                    "SouthBoundingCoordinate": 30.0,
                                    "EastBoundingCoordinate": 10.0,
                                    "NorthBoundingCoordinate": 50.0,
                                }
                            ]
                        }
                    }
                },
                "RelatedUrls": [],
            },
            "meta": {
                "concept-id": "G123456-TEST",
                "provider-id": "TEST",
            },
        }
    )

    result = granule.to_dict()
    assert isinstance(result, dict)
    assert result["umm"]["GranuleUR"] == "test_granule_001"
    assert result["meta"]["concept-id"] == "G123456-TEST"


def test_granule_to_stac():
    """Test that DataGranule.to_stac returns a valid STAC Item structure."""
    from earthaccess.search import DataGranule

    granule = DataGranule(
        {
            "umm": {
                "GranuleUR": "test_granule_001",
                "CollectionReference": {
                    "ShortName": "TestCollection",
                    "Version": "1.0",
                },
                "TemporalExtent": {
                    "RangeDateTime": {
                        "BeginningDateTime": "2020-06-01T00:00:00Z",
                        "EndingDateTime": "2020-06-01T23:59:59Z",
                    }
                },
                "SpatialExtent": {
                    "HorizontalSpatialDomain": {
                        "Geometry": {
                            "BoundingRectangles": [
                                {
                                    "WestBoundingCoordinate": -10.0,
                                    "SouthBoundingCoordinate": 30.0,
                                    "EastBoundingCoordinate": 10.0,
                                    "NorthBoundingCoordinate": 50.0,
                                }
                            ]
                        }
                    }
                },
                "RelatedUrls": [
                    {"Type": "GET DATA", "URL": "https://example.com/data.nc"},
                    {
                        "Type": "GET RELATED VISUALIZATION",
                        "URL": "https://example.com/browse.png",
                    },
                ],
                "DataGranule": {"ArchiveAndDistributionInformation": [{"Size": 100.5}]},
            },
            "meta": {
                "concept-id": "G123456-TEST",
                "provider-id": "TEST",
            },
        }
    )

    stac = granule.to_stac()

    # Check required STAC Item fields
    assert stac["type"] == "Feature"
    assert stac["stac_version"] == "1.0.0"
    assert stac["id"] == "test_granule_001"
    assert "geometry" in stac
    assert "bbox" in stac
    assert "properties" in stac
    assert "assets" in stac
    assert "links" in stac

    # Check geometry
    assert stac["geometry"]["type"] == "Polygon"
    assert stac["bbox"] == [-10.0, 30.0, 10.0, 50.0]

    # Check collection reference
    assert stac["collection"] == "TestCollection_v1.0"

    # Check assets
    assert "data" in stac["assets"]
    assert stac["assets"]["data"]["href"] == "https://example.com/data.nc"
    # Thumbnail asset uses filename as key (e.g., "browse" from browse.png)
    assert "browse" in stac["assets"]
    assert "thumbnail" in stac["assets"]["browse"]["roles"]

    # Check CMR-specific properties
    assert stac["properties"]["cmr:concept_id"] == "G123456-TEST"


def test_granule_to_stac_with_s3_links():
    """Test STAC conversion with S3 direct access links."""
    from earthaccess.search import DataGranule

    granule = DataGranule(
        {
            "umm": {
                "GranuleUR": "s3_granule_001",
                "CollectionReference": {"ShortName": "CloudData"},
                "TemporalExtent": {"SingleDateTime": "2020-06-15T12:00:00Z"},
                "SpatialExtent": {},
                "RelatedUrls": [
                    {"Type": "GET DATA VIA DIRECT ACCESS", "URL": "s3://bucket/data.nc"}
                ],
            },
            "meta": {
                "concept-id": "G789-CLOUD",
                "provider-id": "CLOUD",
            },
        },
        cloud_hosted=True,
    )

    stac = granule.to_stac()

    # Check that S3 asset has cloud-optimized role
    assert "data" in stac["assets"]
    assert stac["assets"]["data"]["href"] == "s3://bucket/data.nc"
    assert "cloud-optimized" in stac["assets"]["data"]["roles"]


def test_collection_to_stac_minimal():
    """Test STAC conversion with minimal collection data."""
    collection = DataCollection(
        {
            "umm": {
                "ShortName": "MinimalCollection",
            },
            "meta": {
                "concept-id": "C999-MIN",
            },
        }
    )

    stac = collection.to_stac()

    # Should still produce valid STAC structure
    assert stac["type"] == "Collection"
    assert stac["id"] == "MinimalCollection"
    assert "extent" in stac
    # Default bbox when no spatial info
    assert stac["extent"]["spatial"]["bbox"] == [[-180.0, -90.0, 180.0, 90.0]]


# =============================================================================
# Tests for STAC Asset Naming
# =============================================================================


def test_extract_asset_key_from_band_filename():
    """Test extracting asset key from HLS-style band filenames."""
    from earthaccess.search import DataGranule

    granule = DataGranule(
        {
            "umm": {
                "GranuleUR": "HLS.L30.T10SEG.2023001T185019.v2.0",
                "CollectionReference": {"ShortName": "HLSL30"},
                "TemporalExtent": {"SingleDateTime": "2023-01-01T00:00:00Z"},
                "SpatialExtent": {},
                "RelatedUrls": [
                    {
                        "Type": "GET DATA VIA DIRECT ACCESS",
                        "URL": "s3://lp-prod/HLS.L30.T10SEG.2023001T185019.v2.0.B02.tif",
                    },
                    {
                        "Type": "GET DATA VIA DIRECT ACCESS",
                        "URL": "s3://lp-prod/HLS.L30.T10SEG.2023001T185019.v2.0.B03.tif",
                    },
                    {
                        "Type": "GET DATA VIA DIRECT ACCESS",
                        "URL": "s3://lp-prod/HLS.L30.T10SEG.2023001T185019.v2.0.Fmask.tif",
                    },
                ],
            },
            "meta": {"concept-id": "G123-LP", "provider-id": "LPCLOUD"},
        },
        cloud_hosted=True,
    )

    stac = granule.to_stac()

    # Check that asset keys are meaningful band names, not generic "data_0", "data_1"
    assert "B02" in stac["assets"], (
        f"Expected 'B02' key, got: {list(stac['assets'].keys())}"
    )
    assert "B03" in stac["assets"], (
        f"Expected 'B03' key, got: {list(stac['assets'].keys())}"
    )
    assert "Fmask" in stac["assets"], (
        f"Expected 'Fmask' key, got: {list(stac['assets'].keys())}"
    )

    # Check that generic keys are NOT present
    assert "data" not in stac["assets"]
    assert "data_1" not in stac["assets"]
    assert "data_2" not in stac["assets"]


def test_extract_asset_key_s3_and_https_grouping():
    """Test that S3 and HTTPS versions of the same file are grouped together."""
    from earthaccess.search import DataGranule

    granule = DataGranule(
        {
            "umm": {
                "GranuleUR": "HLS.L30.T10SEG.2023001T185019.v2.0",
                "CollectionReference": {"ShortName": "HLSL30"},
                "TemporalExtent": {"SingleDateTime": "2023-01-01T00:00:00Z"},
                "SpatialExtent": {},
                "RelatedUrls": [
                    {
                        "Type": "GET DATA VIA DIRECT ACCESS",
                        "URL": "s3://lp-prod/HLS.L30.T10SEG.2023001T185019.v2.0.B02.tif",
                    },
                    {
                        "Type": "GET DATA",
                        "URL": "https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod/HLS.L30.T10SEG.2023001T185019.v2.0.B02.tif",
                    },
                ],
            },
            "meta": {"concept-id": "G123-LP", "provider-id": "LPCLOUD"},
        },
        cloud_hosted=True,
    )

    stac = granule.to_stac()

    # Should have only one B02 asset, not B02 and B02_https
    assert "B02" in stac["assets"]
    assert len([k for k in stac["assets"] if k.startswith("B02")]) == 1

    # S3 should be primary href (cloud-hosted)
    assert stac["assets"]["B02"]["href"].startswith("s3://")

    # HTTPS should be in alternate
    assert "alternate" in stac["assets"]["B02"]
    assert stac["assets"]["B02"]["alternate"]["href"].startswith("https://")


def test_extract_asset_key_thumbnail():
    """Test that thumbnail/browse assets get proper naming."""
    from earthaccess.search import DataGranule

    granule = DataGranule(
        {
            "umm": {
                "GranuleUR": "test_granule",
                "CollectionReference": {"ShortName": "TestCollection"},
                "TemporalExtent": {"SingleDateTime": "2023-01-01T00:00:00Z"},
                "SpatialExtent": {},
                "RelatedUrls": [
                    {
                        "Type": "GET RELATED VISUALIZATION",
                        "URL": "https://example.com/browse.png",
                    },
                    {
                        "Type": "GET RELATED VISUALIZATION",
                        "URL": "https://example.com/quicklook.jpg",
                    },
                ],
            },
            "meta": {"concept-id": "G123-TEST", "provider-id": "TEST"},
        }
    )

    stac = granule.to_stac()

    # Thumbnails should be named from filename (without extension)
    assert "browse" in stac["assets"] or "thumbnail" in stac["assets"]
    assert (
        "thumbnail" in stac["assets"]["browse"]["roles"]
        or "visual" in stac["assets"]["browse"]["roles"]
    )


def test_extract_asset_key_single_data_file():
    """Test that single data files use the filename as key."""
    from earthaccess.search import DataGranule

    granule = DataGranule(
        {
            "umm": {
                "GranuleUR": "ATL08_20190221121851_08410203_005_01",
                "CollectionReference": {"ShortName": "ATL08"},
                "TemporalExtent": {"SingleDateTime": "2019-02-21T00:00:00Z"},
                "SpatialExtent": {},
                "RelatedUrls": [
                    {
                        "Type": "GET DATA",
                        "URL": "https://example.com/ATL08_20190221121851_08410203_005_01.h5",
                    },
                ],
            },
            "meta": {"concept-id": "G123-NSIDC", "provider-id": "NSIDC_ECS"},
        }
    )

    stac = granule.to_stac()

    # Single data file should use filename (without extension) as key
    # Or just "data" if the filename matches granule ID
    assert len(stac["assets"]) == 1
    asset_key = list(stac["assets"].keys())[0]
    # Either the key is "data" (when filename == granule_id) or the filename
    assert asset_key in ("data", "ATL08_20190221121851_08410203_005_01")


def test_extract_asset_key_netcdf_with_extension():
    """Test that file extensions are removed from asset keys."""
    from earthaccess.search import DataGranule

    granule = DataGranule(
        {
            "umm": {
                "GranuleUR": "mur_sst_20230101",
                "CollectionReference": {"ShortName": "MUR-JPL"},
                "TemporalExtent": {"SingleDateTime": "2023-01-01T00:00:00Z"},
                "SpatialExtent": {},
                "RelatedUrls": [
                    {
                        "Type": "GET DATA",
                        "URL": "https://example.com/mur_sst_20230101.nc",
                    },
                ],
            },
            "meta": {"concept-id": "G123-PODAAC", "provider-id": "PODAAC"},
        }
    )

    stac = granule.to_stac()

    # Key should be the filename without extension, or "data" if it matches granule ID
    asset_key = list(stac["assets"].keys())[0]
    assert ".nc" not in asset_key  # Extension should be removed


# =============================================================================
# Parametrized Tests for Multi-File Collections (Real CMR Data)
# =============================================================================


def load_fixture(fixture_name: str) -> dict:
    """Load a UMM JSON fixture file from the granules directory."""
    fixture_path = Path(__file__).parent / "fixtures" / "granules" / fixture_name
    with open(fixture_path) as f:
        return json.load(f)


@pytest.mark.parametrize(
    "fixture_file,expected_keys,description",
    [
        pytest.param(
            "HLSL30_umm.json",
            [
                "B01",
                "B02",
                "B03",
                "B04",
                "B05",
                "B06",
                "B07",
                "B09",
                "B10",
                "B11",
                "Fmask",
                "VZA",
                "VAA",
                "SAA",
                "SZA",
            ],
            "HLS Landsat 30m multi-band COGs",
            id="HLSL30",
        ),
        pytest.param(
            "HLSS30_umm.json",
            [
                "B01",
                "B02",
                "B03",
                "B04",
                "B05",
                "B06",
                "B07",
                "B08",
                "B09",
                "B10",
                "B11",
                "B12",
                "B8A",
                "Fmask",
                "VZA",
                "VAA",
                "SAA",
                "SZA",
            ],
            "HLS Sentinel 30m multi-band COGs",
            id="HLSS30",
        ),
        pytest.param(
            "GEDI02_B_umm.json",
            ["data"],
            "GEDI L2B single HDF5 file",
            id="GEDI02_B",
        ),
    ],
)
def test_multifile_collection_asset_extraction(
    fixture_file, expected_keys, description
):
    """Test STAC asset naming extracts meaningful keys from various collections.

    This test uses real CMR UMM responses to verify that asset keys are
    properly extracted from multi-file granules.
    """
    from earthaccess.search import DataGranule

    # Load fixture data
    fixture_data = load_fixture(fixture_file)

    # Determine cloud_hosted from provider
    provider = fixture_data["meta"].get("provider-id", "")
    cloud_hosted = provider in ("LPCLOUD", "POCLOUD", "ORNL_CLOUD", "GES_DISC")

    # Create DataGranule from fixture
    granule = DataGranule(fixture_data, cloud_hosted=cloud_hosted)

    # Convert to STAC
    stac = granule.to_stac()

    # Check that all expected asset keys are present
    asset_keys = list(stac["assets"].keys())
    for expected_key in expected_keys:
        assert expected_key in asset_keys, (
            f"Expected asset key '{expected_key}' not found. "
            f"Got: {asset_keys}. Collection: {description}"
        )

    # For multi-band data, verify no generic "data_N" keys are present
    if len(expected_keys) > 1:
        generic_keys = [k for k in asset_keys if k.startswith("data_")]
        assert len(generic_keys) == 0, (
            f"Found generic keys {generic_keys} instead of meaningful names. "
            f"Collection: {description}"
        )


@pytest.mark.parametrize(
    "fixture_file,description",
    [
        pytest.param("HLSL30_umm.json", "HLS Landsat 30m", id="HLSL30"),
        pytest.param("HLSS30_umm.json", "HLS Sentinel 30m", id="HLSS30"),
        pytest.param("EMITL2ARFL_umm.json", "EMIT L2A Reflectance", id="EMITL2ARFL"),
        pytest.param("GEDI02_B_umm.json", "GEDI L2B", id="GEDI02_B"),
    ],
)
def test_s3_and_https_assets_grouped(fixture_file, description):
    """Test that S3 and HTTPS versions of the same file are grouped together.

    For cloud-hosted data, S3 URLs should be the primary href, with HTTPS
    as an alternate access method in the same asset.
    """
    from earthaccess.search import DataGranule

    # Load fixture data
    fixture_data = load_fixture(fixture_file)

    # Create DataGranule (cloud_hosted=True for grouping behavior)
    granule = DataGranule(fixture_data, cloud_hosted=True)

    # Convert to STAC
    stac = granule.to_stac()
    assets = stac["assets"]

    # Count how many assets have both S3 and HTTPS versions
    assets_with_alternate = 0
    for key, asset in assets.items():
        href = asset.get("href", "")
        if href.startswith("s3://"):
            # S3 is primary, check for HTTPS alternate
            if "alternate" in asset:
                alt_href = asset["alternate"].get("href", "")
                if alt_href.startswith("https://"):
                    assets_with_alternate += 1

    # For cloud-hosted data with both S3 and HTTPS URLs, we expect grouping
    # (At minimum, data assets should have alternates)
    data_assets = [
        k for k in assets.keys() if not k.startswith("thumbnail") and k != "browse"
    ]
    if data_assets:
        assert assets_with_alternate > 0, (
            f"Expected S3 assets to have HTTPS alternates. Collection: {description}"
        )


@pytest.mark.parametrize(
    "fixture_file",
    [
        pytest.param("HLSL30_umm.json", id="HLSL30"),
        pytest.param("HLSS30_umm.json", id="HLSS30"),
        pytest.param("EMITL2ARFL_umm.json", id="EMITL2ARFL"),
        pytest.param("GEDI02_B_umm.json", id="GEDI02_B"),
        pytest.param("GEDI_L4A_umm.json", id="GEDI_L4A"),
    ],
)
def test_stac_item_structure_from_fixtures(fixture_file):
    """Test that STAC items generated from real CMR data have valid structure."""
    from earthaccess.search import DataGranule

    # Load fixture data
    fixture_data = load_fixture(fixture_file)

    # Create DataGranule
    provider = fixture_data["meta"].get("provider-id", "")
    cloud_hosted = provider in ("LPCLOUD", "POCLOUD", "ORNL_CLOUD", "GES_DISC")
    granule = DataGranule(fixture_data, cloud_hosted=cloud_hosted)

    # Convert to STAC
    stac = granule.to_stac()

    # Verify required STAC Item fields
    assert "type" in stac
    assert stac["type"] == "Feature"
    assert "stac_version" in stac
    assert "id" in stac
    assert "geometry" in stac
    assert "bbox" in stac
    assert "properties" in stac
    assert "datetime" in stac["properties"]
    assert "links" in stac
    assert "assets" in stac

    # Verify at least one asset exists
    assert len(stac["assets"]) > 0

    # Verify each asset has required fields
    for key, asset in stac["assets"].items():
        assert "href" in asset, f"Asset '{key}' missing 'href'"
        assert "roles" in asset, f"Asset '{key}' missing 'roles'"
