"""Integration tests for Asset and AssetFilter with DataGranule.

These tests verify that Asset and AssetFilter work correctly with
real DataGranule instances and demonstrate common use cases.
"""

from earthaccess.assets import Asset, AssetFilter, filter_assets
from earthaccess.results import DataGranule


class TestDataGranuleAssets:
    """Test DataGranule.assets() and data_assets() methods."""

    def _create_test_granule(self, data_links=None, viz_links=None):
        """Helper to create a test DataGranule."""
        if data_links is None:
            data_links = []
        if viz_links is None:
            viz_links = []

        related_urls = []
        for link in data_links:
            related_urls.append({"Type": "GET DATA", "URL": link})
        for link in viz_links:
            related_urls.append({"Type": "GET RELATED VISUALIZATION", "URL": link})

        test_granule = {
            "meta": {"concept-id": "test-id", "provider-id": "test-provider"},
            "umm": {
                "GranuleUR": "test-granule",
                "CollectionReference": {"ShortName": "test"},
                "SpatialExtent": {"HorizontalSpatialDomain": {}},
                "TemporalExtent": {},
                "RelatedUrls": related_urls,
            },
        }
        return DataGranule(test_granule)

    def test_assets_returns_asset_objects(self):
        """DataGranule.assets() returns list of Asset objects."""
        granule = self._create_test_granule(data_links=["https://example.com/file.nc"])
        assets = granule.assets()
        assert len(assets) == 1
        assert isinstance(assets[0], Asset)
        assert assets[0].href == "https://example.com/file.nc"

    def test_assets_includes_data_and_thumbnails(self):
        """DataGranule.assets() includes both data and thumbnail assets."""
        granule = self._create_test_granule(
            data_links=["https://example.com/file.nc"],
            viz_links=["https://example.com/thumb.png"],
        )
        assets = granule.assets()
        assert len(assets) == 2

        data_assets = [a for a in assets if a.is_data()]
        thumb_assets = [a for a in assets if a.is_thumbnail()]

        assert len(data_assets) == 1
        assert len(thumb_assets) == 1

    def test_assets_infers_media_types(self):
        """DataGranule.assets() infers media types from file extensions."""
        granule = self._create_test_granule(
            data_links=[
                "https://example.com/data_netcdf.nc",
                "https://example.com/data_geotiff.tif",
                "https://example.com/data_hdf5.h5",
            ]
        )
        assets = granule.assets()

        # Map href to type for verification
        types = {a.href.split("/")[-1]: a.type for a in assets}
        assert types["data_netcdf.nc"] == "application/x-netcdf"
        assert types["data_geotiff.tif"] == "image/tiff; application=geotiff"
        assert types["data_hdf5.h5"] == "application/x-hdf5"

    def test_assets_marks_s3_as_cloud_optimized(self):
        """DataGranule.assets() marks S3 assets with cloud-optimized role."""
        # Use different filenames to test both S3 and HTTPS separately
        granule = self._create_test_granule(
            data_links=["s3://bucket/data_s3.nc", "https://example.com/data_https.nc"]
        )
        assets = granule.assets()

        s3_assets = [a for a in assets if a.href.startswith("s3://")]
        https_assets = [a for a in assets if a.href.startswith("https://")]

        assert len(s3_assets) == 1
        assert len(https_assets) == 1
        assert all(a.is_cloud_optimized() for a in s3_assets)
        assert not any(a.is_cloud_optimized() for a in https_assets)

    def test_data_assets_filters_to_data_role(self):
        """DataGranule.data_assets() returns only data-role assets."""
        granule = self._create_test_granule(
            data_links=["https://example.com/file1.nc", "https://example.com/file2.nc"],
            viz_links=["https://example.com/thumb.png"],
        )
        data_assets = granule.data_assets()
        assert len(data_assets) == 2
        assert all(a.is_data() for a in data_assets)
        assert not any(a.is_thumbnail() for a in data_assets)

    def test_data_assets_with_no_data_returns_empty(self):
        """DataGranule.data_assets() returns empty list if no data assets."""
        granule = self._create_test_granule(viz_links=["https://example.com/thumb.png"])
        data_assets = granule.data_assets()
        assert data_assets == []

    def test_assets_multiple_data_files(self):
        """DataGranule.assets() handles multiple data files correctly."""
        granule = self._create_test_granule(
            data_links=[
                "https://example.com/file1.nc",
                "https://example.com/file2.nc",
                "https://example.com/file3.tif",
            ]
        )
        assets = granule.assets()
        assert len(assets) == 3


class TestAssetFilteringWithGranules:
    """Test AssetFilter with real DataGranule assets."""

    def _create_test_granule(self, data_links=None, viz_links=None):
        """Helper to create a test DataGranule."""
        if data_links is None:
            data_links = []
        if viz_links is None:
            viz_links = []

        related_urls = []
        for link in data_links:
            related_urls.append({"Type": "GET DATA", "URL": link})
        for link in viz_links:
            related_urls.append({"Type": "GET RELATED VISUALIZATION", "URL": link})

        test_granule = {
            "meta": {"concept-id": "test-id", "provider-id": "test-provider"},
            "umm": {
                "GranuleUR": "test-granule",
                "CollectionReference": {"ShortName": "test"},
                "SpatialExtent": {"HorizontalSpatialDomain": {}},
                "TemporalExtent": {},
                "RelatedUrls": related_urls,
            },
        }
        return DataGranule(test_granule)

    def test_filter_netcdf_files(self):
        """Filter assets to get only NetCDF files."""
        granule = self._create_test_granule(
            data_links=[
                "https://example.com/file1.nc",
                "https://example.com/file2.tif",
                "https://example.com/file3.nc4",
            ]
        )
        assets = granule.assets()
        filter = AssetFilter(include_patterns=["*.nc", "*.nc4"])
        filtered = filter_assets(assets, filter)
        assert len(filtered) == 2
        assert all(a.href.endswith((".nc", ".nc4")) for a in filtered)

    def test_filter_geotiff_files(self):
        """Filter assets to get only GeoTIFF files."""
        granule = self._create_test_granule(
            data_links=[
                "https://example.com/file1.tif",
                "https://example.com/file2.tiff",
                "https://example.com/file3.nc",
            ]
        )
        assets = granule.assets()
        filter = AssetFilter(include_patterns=["*.tif", "*.tiff"])
        filtered = filter_assets(assets, filter)
        assert len(filtered) == 2
        assert all(a.href.endswith((".tif", ".tiff")) for a in filtered)

    def test_filter_exclude_thumbnails(self):
        """Filter assets to exclude thumbnails."""
        granule = self._create_test_granule(
            data_links=["https://example.com/file.nc"],
            viz_links=["https://example.com/thumb.png"],
        )
        assets = granule.assets()
        filter = AssetFilter(exclude_roles=["thumbnail"])
        filtered = filter_assets(assets, filter)
        assert len(filtered) == 1
        assert filtered[0].is_data()

    def test_filter_cloud_optimized_only(self):
        """Filter assets to get only cloud-optimized (S3) files."""
        granule = self._create_test_granule(
            data_links=[
                "s3://bucket/file1.nc",
                "s3://bucket/file2.tif",
                "https://example.com/file3.nc",
            ]
        )
        assets = granule.assets()
        filter = AssetFilter(include_roles=["cloud-optimized"])
        filtered = filter_assets(assets, filter)
        assert len(filtered) == 2
        assert all(a.href.startswith("s3://") for a in filtered)

    def test_filter_data_files_excluding_browse(self):
        """Filter to get data files but exclude browse images."""
        granule = self._create_test_granule(
            data_links=[
                "https://example.com/file_data.nc",
                "https://example.com/file_browse.tif",
            ],
            viz_links=["https://example.com/thumb.png"],
        )
        assets = granule.assets()
        filter = AssetFilter(include_roles=["data"], exclude_patterns=["*_browse*"])
        filtered = filter_assets(assets, filter)
        assert len(filtered) == 1
        assert filtered[0].href == "https://example.com/file_data.nc"

    def test_filter_combine_patterns_and_roles(self):
        """Test combining pattern and role filters."""
        granule = self._create_test_granule(
            data_links=[
                "s3://bucket/file1.nc",
                "s3://bucket/file2.tif",
                "https://example.com/file3.nc",
            ]
        )
        assets = granule.assets()

        # Filter 1: only NetCDF files
        filter1 = AssetFilter(include_patterns=["*.nc"])
        # Filter 2: only cloud-optimized
        filter2 = AssetFilter(include_roles=["cloud-optimized"])
        # Combine: NetCDF files that are cloud-optimized
        combined = filter1.combine(filter2)
        filtered = filter_assets(assets, combined)

        assert len(filtered) == 1
        assert filtered[0].href == "s3://bucket/file1.nc"


class TestAssetFilterDictCreation:
    """Test creating filters from dictionaries."""

    def test_filter_from_dict_simple_pattern(self):
        """Create filter from dict with simple pattern."""
        filter = AssetFilter.from_dict({"include_patterns": ["*.nc"]})
        asset = Asset(href="file.nc", roles=["data"])
        assert filter.matches(asset)

    def test_filter_from_dict_multiple_criteria(self):
        """Create filter from dict with multiple criteria."""
        filter = AssetFilter.from_dict(
            {
                "include_patterns": ["*.nc", "*.tif"],
                "exclude_roles": ["thumbnail"],
                "min_size": 1024,
            }
        )
        asset = Asset(
            href="file.nc",
            roles=["data"],
            size=2048,
        )
        assert filter.matches(asset)

    def test_filter_from_dict_ignores_unknown_keys(self):
        """from_dict ignores unknown keys gracefully."""
        filter = AssetFilter.from_dict(
            {
                "include_patterns": ["*.nc"],
                "unknown_key": "some_value",
            }
        )
        assert filter.include_patterns == ["*.nc"]


class TestAssetUsagePatterns:
    """Test common usage patterns for assets."""

    def _create_test_granule(self, data_links=None, viz_links=None):
        """Helper to create a test DataGranule."""
        if data_links is None:
            data_links = []
        if viz_links is None:
            viz_links = []

        related_urls = []
        for link in data_links:
            related_urls.append({"Type": "GET DATA", "URL": link})
        for link in viz_links:
            related_urls.append({"Type": "GET RELATED VISUALIZATION", "URL": link})

        test_granule = {
            "meta": {"concept-id": "test-id", "provider-id": "test-provider"},
            "umm": {
                "GranuleUR": "test-granule",
                "CollectionReference": {"ShortName": "test"},
                "SpatialExtent": {"HorizontalSpatialDomain": {}},
                "TemporalExtent": {},
                "RelatedUrls": related_urls,
            },
        }
        return DataGranule(test_granule)

    def test_pattern_list_comprehension_usage(self):
        """Common pattern: get specific asset types using list comprehension."""
        granule = self._create_test_granule(
            data_links=[
                "https://example.com/file1.nc",
                "https://example.com/file2.tif",
            ],
            viz_links=["https://example.com/thumb.png"],
        )

        # Get only NetCDF files
        netcdf_files = [a for a in granule.assets() if a.href.endswith(".nc")]
        assert len(netcdf_files) == 1

    def test_role_filtering_pattern(self):
        """Common pattern: filter by role using helper methods."""
        granule = self._create_test_granule(
            data_links=["https://example.com/file.nc"],
            viz_links=["https://example.com/thumb.png"],
        )

        data = granule.data_assets()
        assert len(data) == 1
        assert data[0].is_data()

    def test_cloud_access_pattern(self):
        """Common pattern: get only cloud-accessible (S3) files."""
        granule = self._create_test_granule(
            data_links=[
                "s3://bucket/file1.nc",
                "https://example.com/file2.nc",
            ]
        )

        # Get only cloud-accessible files
        cloud_files = [a for a in granule.assets() if a.is_cloud_optimized()]
        assert len(cloud_files) == 1
        assert cloud_files[0].href.startswith("s3://")
