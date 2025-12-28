from unittest.mock import patch

import pytest
from earthaccess.search import DataCollection, DataGranule
from earthaccess.virtual import (
    SUPPORTED_PARSERS,
    get_granule_credentials_endpoint_and_region,
)
from earthaccess.virtual.dmrpp import _get_parser, _get_urls_for_parser

granule_no_credentials_endpoint = DataGranule(
    {
        "meta": {
            "collection-concept-id": "C1234-PROV",
        },
        "umm": {
            "RelatedUrls": [
                {
                    "URL": "https://data.earthdata.nasa.gov/data.h5",
                    "Type": "GET DATA",
                },
            ],
        },
    },
    cloud_hosted=True,
)


@patch("earthaccess.search_datasets")
def test_get_granule_credentials_and_region_from_granule(mock_search_datasets):
    """If credentials endpoint is populated in the granule it is used."""
    granule_credentials_endpoint = (
        "https://archive.daac.earthdata.nasa.gov/s3credentials"
    )
    granule = DataGranule(
        {
            "meta": {
                "collection-concept-id": "C1234-PROV",
            },
            "umm": {
                "RelatedUrls": [
                    {
                        "URL": "https://data.earthdata.nasa.gov/data.h5",
                        "Type": "GET DATA",
                    },
                    {
                        "URL": "s3://daac-cumulus-prod-protected/data.h5",
                        "Type": "GET DATA VIA DIRECT ACCESS",
                    },
                    {
                        "URL": granule_credentials_endpoint,
                        "Type": "VIEW RELATED INFORMATION",
                    },
                ],
            },
        },
        cloud_hosted=True,
    )

    # Credentials endpoint is retrieved from the granule, the region is set to
    # a default value of us-west-2.
    assert get_granule_credentials_endpoint_and_region(granule) == (
        granule_credentials_endpoint,
        "us-west-2",
    )

    # Ensure no attempt was made to retrieve collection information
    mock_search_datasets.assert_not_called()


@patch("earthaccess.search_datasets")
def test_get_granule_credentials_and_region_from_collection(mock_search_datasets):
    """If granule does not have credentials, those for a collection are retrieved."""
    collection_credentials_endpoint = (
        "https://archive.other-daac.earthdata.nasa.gov/s3credentials"
    )
    collection_region = "us-east-1"

    mock_search_datasets.return_value = [
        DataCollection(
            {
                "meta": {
                    "concept-id": "C1234-PROV",
                },
                "umm": {
                    "DirectDistributionInformation": {
                        "Region": collection_region,
                        "S3CredentialsAPIEndpoint": collection_credentials_endpoint,
                    },
                },
            }
        ),
    ]
    # Credentials endpoint is retrieved from the collection, the region is also
    # retrieved from the collection.
    assert get_granule_credentials_endpoint_and_region(
        granule_no_credentials_endpoint
    ) == (
        collection_credentials_endpoint,
        collection_region,
    )

    # An attempt was made to retrieve collection information
    mock_search_datasets.assert_called_once_with(count=1, concept_id="C1234-PROV")


@patch("earthaccess.search_datasets")
def test_get_granule_credentials_from_collection_default_region(mock_search_datasets):
    """If a collection does not have a region, the default of us-west-2 is used."""
    collection_credentials_endpoint = (
        "https://archive.other-daac.earthdata.nasa.gov/s3credentials"
    )

    mock_search_datasets.return_value = [
        DataCollection(
            {
                "meta": {
                    "concept-id": "C1234-PROV",
                },
                "umm": {
                    "DirectDistributionInformation": {
                        "S3CredentialsAPIEndpoint": collection_credentials_endpoint,
                    },
                },
            }
        ),
    ]
    # Credentials endpoint is retrieved from the collection, the region is also
    # retrieved from the collection.
    assert get_granule_credentials_endpoint_and_region(
        granule_no_credentials_endpoint
    ) == (
        collection_credentials_endpoint,
        "us-west-2",
    )

    # An attempt was made to retrieve collection information
    mock_search_datasets.assert_called_once_with(count=1, concept_id="C1234-PROV")


@patch("earthaccess.search_datasets")
def test_get_granule_credentials_no_collection_endpoint(mock_search_datasets):
    """Exception raised if a collection does not have an S3CredentialsAPIEndpoint."""
    mock_search_datasets.return_value = [
        DataCollection(
            {
                "meta": {
                    "concept-id": "C1234-PROV",
                },
                "umm": {
                    "DirectDistributionInformation": {
                        "Region": "us-east-1",
                    },
                },
            }
        ),
    ]
    # Credentials endpoint is retrieved from the collection, the region is also
    # retrieved from the collection.

    with pytest.raises(ValueError, match="did not provide an S3CredentialsAPIEndpoint"):
        get_granule_credentials_endpoint_and_region(granule_no_credentials_endpoint)

    # An attempt was made to retrieve collection information
    mock_search_datasets.assert_called_once_with(count=1, concept_id="C1234-PROV")


# =============================================================================
# Tests for parser selection and URL generation
# =============================================================================


class TestSupportedParsers:
    """Tests for the SUPPORTED_PARSERS constant."""

    def test_supported_parsers_contains_expected_values(self):
        """SUPPORTED_PARSERS should contain all expected parser names."""
        assert "DMRPPParser" in SUPPORTED_PARSERS
        assert "HDFParser" in SUPPORTED_PARSERS
        assert "NetCDF3Parser" in SUPPORTED_PARSERS

    def test_supported_parsers_is_a_set(self):
        """SUPPORTED_PARSERS should be a set for O(1) lookups."""
        assert isinstance(SUPPORTED_PARSERS, set)


class TestGetParser:
    """Tests for the _get_parser helper function."""

    def test_get_parser_unknown_raises_value_error(self):
        """Unknown parser names should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown parser"):
            _get_parser("UnknownParser")

    def test_get_parser_with_invalid_type_string(self):
        """Invalid parser string should list supported options."""
        with pytest.raises(ValueError, match="Supported parsers are"):
            _get_parser("InvalidParser")

    def test_get_parser_passthrough_for_instances(self):
        """Non-string values should be returned as-is (parser instances)."""

        class MockParser:
            pass

        mock_parser = MockParser()
        result = _get_parser(mock_parser)
        assert result is mock_parser


class TestGetUrlsForParser:
    """Tests for the _get_urls_for_parser helper function."""

    @pytest.fixture
    def sample_granules(self):
        """Create sample granules for testing."""
        return [
            DataGranule(
                {
                    "meta": {"collection-concept-id": "C1234-PROV"},
                    "umm": {
                        "RelatedUrls": [
                            {
                                "URL": "https://data.earthdata.nasa.gov/file1.nc",
                                "Type": "GET DATA",
                            },
                        ],
                    },
                },
                cloud_hosted=True,
            ),
            DataGranule(
                {
                    "meta": {"collection-concept-id": "C1234-PROV"},
                    "umm": {
                        "RelatedUrls": [
                            {
                                "URL": "https://data.earthdata.nasa.gov/file2.nc",
                                "Type": "GET DATA",
                            },
                        ],
                    },
                },
                cloud_hosted=True,
            ),
        ]

    def test_dmrpp_parser_appends_dmrpp_extension(self, sample_granules):
        """DMRPPParser should append .dmrpp to data URLs."""
        urls = _get_urls_for_parser(sample_granules, "DMRPPParser", "indirect")
        assert urls == [
            "https://data.earthdata.nasa.gov/file1.nc.dmrpp",
            "https://data.earthdata.nasa.gov/file2.nc.dmrpp",
        ]

    def test_hdf_parser_returns_data_urls_directly(self, sample_granules):
        """HDFParser should return data URLs without modification."""
        urls = _get_urls_for_parser(sample_granules, "HDFParser", "indirect")
        assert urls == [
            "https://data.earthdata.nasa.gov/file1.nc",
            "https://data.earthdata.nasa.gov/file2.nc",
        ]

    def test_netcdf3_parser_returns_data_urls_directly(self, sample_granules):
        """NetCDF3Parser should return data URLs without modification."""
        urls = _get_urls_for_parser(sample_granules, "NetCDF3Parser", "indirect")
        assert urls == [
            "https://data.earthdata.nasa.gov/file1.nc",
            "https://data.earthdata.nasa.gov/file2.nc",
        ]

    def test_parser_instance_dmrpp_detection(self, sample_granules):
        """Parser instances should be detected by class name for DMRPPParser."""

        class DMRPPParser:
            """Mock DMRPPParser class."""

            pass

        mock_parser = DMRPPParser()
        urls = _get_urls_for_parser(sample_granules, mock_parser, "indirect")
        assert urls == [
            "https://data.earthdata.nasa.gov/file1.nc.dmrpp",
            "https://data.earthdata.nasa.gov/file2.nc.dmrpp",
        ]

    def test_parser_instance_non_dmrpp_detection(self, sample_granules):
        """Non-DMRPPParser instances should return data URLs directly."""

        class HDFParser:
            """Mock HDFParser class."""

            pass

        mock_parser = HDFParser()
        urls = _get_urls_for_parser(sample_granules, mock_parser, "indirect")
        assert urls == [
            "https://data.earthdata.nasa.gov/file1.nc",
            "https://data.earthdata.nasa.gov/file2.nc",
        ]
