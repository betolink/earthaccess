"""Tests for Group B: Enhanced Results Classes.

Tests for DataGranules and DataCollections with new functionality
including asset access, STAC conversion, and method chaining.
"""

import pytest
from earthaccess.results import DataCollection, DataGranule
from earthaccess.store_components.asset import Asset, AssetFilter


@pytest.fixture
def sample_granule_dict():
    """Sample granule dictionary for testing."""
    return {
        "meta": {
            "concept-id": "G1234567890-NSIDC_CPRD",
            "provider-id": "NSIDC_CPRD",
        },
        "umm": {
            "GranuleUR": "test_granule",
            "CollectionReference": {"ShortName": "ATL03", "Version": "005"},
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
            "TemporalExtent": {
                "RangeDateTime": {
                    "BeginningDateTime": "2023-01-01T00:00:00Z",
                    "EndingDateTime": "2023-01-01T23:59:59Z",
                }
            },
            "RelatedUrls": [
                {
                    "Type": "GET DATA VIA DIRECT ACCESS",
                    "URL": "s3://nsidc-cumulus-test/data.nc",
                },
                {"Type": "GET DATA", "URL": "https://example.com/data.nc"},
                {
                    "Type": "GET RELATED VISUALIZATION",
                    "URL": "https://example.com/browse.jpg",
                },
            ],
            "DataGranule": {"ArchiveAndDistributionInformation": [{"Size": 1000000}]},
        },
    }


@pytest.fixture
def sample_granule(sample_granule_dict):
    """Sample DataGranule instance."""
    return DataGranule(sample_granule_dict, cloud_hosted=True)


@pytest.fixture
def sample_collection_dict():
    """Sample collection dictionary for testing."""
    return {
        "meta": {
            "concept-id": "C1234567890-NSIDC_CPRD",
            "provider-id": "NSIDC_CPRD",
            "granule-count": 1000,
        },
        "umm": {
            "ShortName": "ATL03",
            "Abstract": "Test collection abstract",
            "Version": "005",
            "DOI": {"DOI": "10.5067/TEST"},
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
            "TemporalExtents": [
                {
                    "RangeDateTime": {
                        "BeginningDateTime": "2023-01-01T00:00:00Z",
                        "EndingDateTime": "2023-12-31T23:59:59Z",
                    }
                }
            ],
            "DataCenters": [
                {"ShortName": "NSIDC", "Roles": ["ARCHIVER", "DISTRIBUTOR"]}
            ],
            "RelatedUrls": [
                {"Type": "LANDING PAGE", "URL": "https://example.com/landing"},
                {"Type": "GET DATA", "URL": "https://example.com/getdata"},
            ],
            "ArchiveAndDistributionInformation": {
                "FileDistributionInformation": "HDF5"
            },
            "DirectDistributionInformation": {
                "S3BucketAndObjectPrefixNames": ["s3://nsidc-cumulus-test/"],
                "Region": "us-west-2",
                "S3CredentialsEndpoint": "https://example.com/s3credentials",
            },
        },
    }


@pytest.fixture
def sample_collection(sample_collection_dict):
    """Sample DataCollection instance."""
    return DataCollection(sample_collection_dict, cloud_hosted=True)


class TestDataGranuleAssets:
    """Test asset access methods for DataGranule."""

    def test_get_assets_returns_list(self, sample_granule):
        """Test that get_assets returns a list of Asset objects."""
        assets = sample_granule.get_assets()
        assert isinstance(assets, list)
        assert len(assets) > 0
        assert all(isinstance(asset, Asset) for asset in assets)

    def test_get_data_assets_filters_by_type(self, sample_granule):
        """Test that get_data_assets returns only data assets."""
        data_assets = sample_granule.get_data_assets()
        assert all(
            asset.is_data() or asset.type in ["data", "science-data"]
            for asset in data_assets
        )

    def test_get_thumbnail_assets_returns_thumbnails(self, sample_granule):
        """Test that get_thumbnail_assets returns only thumbnails."""
        thumbnails = sample_granule.get_thumbnail_assets()
        assert all(asset.has_role("thumbnail") for asset in thumbnails)

    def test_get_browse_assets_returns_browse_images(self, sample_granule):
        """Test that get_browse_assets returns browse images."""
        browse_assets = sample_granule.get_browse_assets()
        assert all(asset.has_role("browse") for asset in browse_assets)

    def test_filter_assets_with_filter_object(self, sample_granule):
        """Test filtering assets with AssetFilter object."""
        sample_granule.get_assets()
        asset_filter = AssetFilter(filename_patterns=["*.nc"])

        filtered = sample_granule.filter_assets(asset_filter)
        assert all(".nc" in asset.href for asset in filtered)

    def test_filter_assets_with_lambda(self, sample_granule):
        """Test filtering assets with lambda function."""
        sample_granule.get_assets()

        filtered = sample_granule.filter_assets(lambda a: a.href.endswith(".nc"))
        assert all(asset.href.endswith(".nc") for asset in filtered)


class TestDataGranuleSTAC:
    """Test STAC conversion methods for DataGranule."""

    def test_to_stac_returns_dict(self, sample_granule):
        """Test that to_stac returns a STAC Item dictionary."""
        stac_item = sample_granule.to_stac()
        assert isinstance(stac_item, dict)

    def test_to_stac_has_required_fields(self, sample_granule):
        """Test that to_stac includes required STAC Item fields."""
        stac_item = sample_granule.to_stac()

        required_fields = ["type", "id", "geometry", "properties", "assets", "links"]
        for field in required_fields:
            assert field in stac_item, f"Missing required field: {field}"

    def test_to_stac_type_is_feature(self, sample_granule):
        """Test that STAC item has type 'Feature'."""
        stac_item = sample_granule.to_stac()
        assert stac_item["type"] == "Feature"

    def test_to_stac_id_from_concept_id(self, sample_granule):
        """Test that STAC item ID comes from concept ID."""
        stac_item = sample_granule.to_stac()
        assert stac_item["id"] == "G1234567890-NSIDC_CPRD"

    def test_to_stac_geometry_from_spatial_extent(self, sample_granule):
        """Test that geometry is derived from spatial extent."""
        stac_item = sample_granule.to_stac()
        assert "geometry" in stac_item
        assert isinstance(stac_item["geometry"], dict)

    def test_to_stac_properties_includes_metadata(self, sample_granule):
        """Test that properties include relevant metadata."""
        stac_item = sample_granule.to_stac()
        assert "properties" in stac_item
        assert "datetime" in stac_item["properties"]
        assert "collection" in stac_item["properties"]

    def test_to_stac_assets_include_data_links(self, sample_granule):
        """Test that assets include data links."""
        stac_item = sample_granule.to_stac()
        assert "assets" in stac_item
        assert len(stac_item["assets"]) > 0

        for asset_key, asset in stac_item["assets"].items():
            assert "href" in asset
            assert "type" in asset or "roles" in asset


class DataGranulePagination:
    """Test pagination methods for DataGranules list."""

    def test_basic_iteration(self, sample_granule_dict):
        """Test basic iteration of granule list."""
        granules = [DataGranule(sample_granule_dict) for _ in range(10)]

        # Test basic iteration
        assert len(granules) == 10
        assert granules[0]["meta"]["concept-id"] == "G1234567890-NSIDC_CPRD"


class TestDataCollectionSTAC:
    """Test STAC conversion methods for DataCollection."""

    def test_to_stac_collection_returns_dict(self, sample_collection):
        """Test that to_stac returns a STAC Collection dictionary."""
        stac_collection = sample_collection.to_stac()
        assert isinstance(stac_collection, dict)

    def test_to_stac_collection_has_required_fields(self, sample_collection):
        """Test that to_stac includes required STAC Collection fields."""
        stac_collection = sample_collection.to_stac()

        required_fields = [
            "type",
            "id",
            "stac_version",
            "description",
            "extent",
            "links",
        ]
        for field in required_fields:
            assert field in stac_collection, f"Missing required field: {field}"

    def test_to_stac_collection_type_is_collection(self, sample_collection):
        """Test that STAC collection has type 'Collection'."""
        stac_collection = sample_collection.to_stac()
        assert stac_collection["type"] == "Collection"

    def test_to_stac_collection_id_from_short_name(self, sample_collection):
        """Test that STAC collection ID comes from short name."""
        stac_collection = sample_collection.to_stac()
        assert stac_collection["id"] == "ATL03"

    def test_to_stac_collection_extent_from_temporal_spatial(self, sample_collection):
        """Test that extent is derived from temporal and spatial extent."""
        stac_collection = sample_collection.to_stac()
        assert "extent" in stac_collection
        assert "spatial" in stac_collection["extent"]
        assert "temporal" in stac_collection["extent"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
