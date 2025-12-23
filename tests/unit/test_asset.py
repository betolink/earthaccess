"""Unit tests for Asset and AssetFilter classes."""

import pytest
from earthaccess.store_components.asset import (
    Asset,
    AssetFilter,
    filter_assets,
    get_assets_by_band,
    get_assets_by_size_range,
    get_browse_assets,
    get_data_assets,
    get_thumbnail_assets,
)


class TestAsset:
    """Test Asset dataclass functionality."""

    def test_asset_creation(self):
        """Test basic asset creation."""
        asset = Asset(href="https://example.com/file.nc")
        assert asset.href == "https://example.com/file.nc"
        assert asset.title is None
        assert asset.description is None
        assert asset.type is None
        assert asset.roles == set()

    def test_asset_with_all_fields(self):
        """Test asset creation with all fields."""
        asset = Asset(
            href="https://example.com/file.nc",
            title="Data File",
            description="NetCDF data file",
            type="application/netcdf",
            roles={"data", "science-data"},
            bands=["band1", "band2"],
            gsd=30.0,
            file_size=1024000,
            checksum="abc123",
        )
        assert asset.href == "https://example.com/file.nc"
        assert asset.title == "Data File"
        assert asset.description == "NetCDF data file"
        assert asset.type == "application/netcdf"
        assert asset.roles == {"data", "science-data"}
        assert asset.bands == ["band1", "band2"]
        assert asset.gsd == 30.0
        assert asset.file_size == 1024000
        assert asset.checksum == "abc123"

    def test_asset_href_required(self):
        """Test that href is required."""
        with pytest.raises(ValueError, match="href is required"):
            Asset(
                href="",
                title="Data File",
            )

    def test_has_role(self):
        """Test role checking."""
        asset = Asset(href="https://example.com/file.nc", roles={"data", "metadata"})
        assert asset.has_role("data")
        assert asset.has_role("metadata")
        assert not asset.has_role("thumbnail")

    def test_is_data(self):
        """Test data asset detection."""
        data_asset = Asset(href="https://example.com/file.nc", type="data")
        assert data_asset.is_data()

        role_data_asset = Asset(
            href="https://example.com/file.nc", roles={"data", "science-data"}
        )
        assert role_data_asset.is_data()

        non_data_asset = Asset(
            href="https://example.com/file.jpg", type="image/jpeg", roles={"thumbnail"}
        )
        assert not non_data_asset.is_data()

    def test_is_thumbnail(self):
        """Test thumbnail detection."""
        thumb_asset = Asset(href="https://example.com/thumb.jpg", roles={"thumbnail"})
        assert thumb_asset.is_thumbnail()

        non_thumb_asset = Asset(href="https://example.com/data.nc", roles={"data"})
        assert not non_thumb_asset.is_thumbnail()

    def test_is_metadata(self):
        """Test metadata detection."""
        metadata_asset = Asset(href="https://example.com/meta.xml", roles={"metadata"})
        assert metadata_asset.is_metadata()

        non_metadata_asset = Asset(href="https://example.com/data.nc", roles={"data"})
        assert not non_metadata_asset.is_metadata()

    def test_is_browse(self):
        """Test browse image detection."""
        browse_asset = Asset(href="https://example.com/browse.jpg", roles={"browse"})
        assert browse_asset.is_browse()

        non_browse_asset = Asset(href="https://example.com/data.nc", roles={"data"})
        assert not non_browse_asset.is_browse()

    def test_matches_type(self):
        """Test type matching."""
        asset = Asset(href="https://example.com/file.nc", type="application/netcdf")
        assert asset.matches_type("application/netcdf")
        assert not asset.matches_type("image/jpeg")

    def test_matches_types(self):
        """Test multiple type matching."""
        asset = Asset(href="https://example.com/file.nc", type="application/netcdf")
        assert asset.matches_types(["application/netcdf", "image/tiff"])
        assert not asset.matches_types(["image/jpeg", "image/png"])

    def test_with_role(self):
        """Test adding role to asset (immutable)."""
        asset = Asset(href="https://example.com/file.nc", roles={"data"})
        new_asset = asset.with_role("processed")

        assert asset.roles == {"data"}  # Original unchanged
        assert new_asset.roles == {"data", "processed"}
        assert new_asset.href == asset.href

    def test_to_dict(self):
        """Test asset to dictionary conversion."""
        asset = Asset(
            href="https://example.com/file.nc",
            title="Data File",
            type="application/netcdf",
            roles={"data"},
            file_size=1024,
        )
        result = asset.to_dict()

        assert result == {
            "href": "https://example.com/file.nc",
            "title": "Data File",
            "description": None,
            "type": "application/netcdf",
            "roles": ["data"],
            "file_size": 1024,
        }


class TestAssetFilter:
    """Test AssetFilter dataclass functionality."""

    def test_empty_filter(self):
        """Test empty filter."""
        filter_obj = AssetFilter()
        assert filter_obj.content_types is None
        assert filter_obj.include_roles is None
        assert filter_obj.exclude_roles is None
        assert filter_obj.bands is None
        assert filter_obj.min_size is None
        assert filter_obj.max_size is None

    def test_content_type_filter(self):
        """Test content type filtering."""
        filter_obj = AssetFilter(content_types=["application/netcdf"])
        assert filter_obj.content_types == ("application/netcdf",)

        new_filter = filter_obj.content_type_filter(["image/tiff"])
        assert new_filter.content_types == ("image/tiff",)

    def test_role_filter(self):
        """Test role-based filtering."""
        filter_obj = AssetFilter(include_roles={"data"})
        assert filter_obj.include_roles == {"data"}

        new_filter = filter_obj.role_filter(include_roles={"data", "processed"})
        assert new_filter.include_roles == {"data", "processed"}

    def test_band_filter(self):
        """Test band-based filtering."""
        filter_obj = AssetFilter(bands=["band1"])
        assert filter_obj.bands == ("band1",)

        new_filter = filter_obj.band_filter(bands=["band1", "band2"])
        assert new_filter.bands == ("band1", "band2")

    def test_size_filter(self):
        """Test size-based filtering."""
        filter_obj = AssetFilter(min_size=100, max_size=10000)
        assert filter_obj.min_size == 100
        assert filter_obj.max_size == 10000

    def test_copy(self):
        """Test filter copying with updates."""
        filter_obj = AssetFilter(content_types=["application/netcdf"])
        new_filter = filter_obj.copy(
            content_types=["image/tiff"], include_roles={"data"}
        )

        assert filter_obj.content_types == ("application/netcdf",)
        assert new_filter.content_types == ("image/tiff",)
        assert new_filter.include_roles == {"data"}

    def test_to_dict(self):
        """Test filter to dictionary conversion."""
        filter_obj = AssetFilter(
            content_types=["application/netcdf"],
            include_roles={"data"},
            min_size=100,
        )
        result = filter_obj.to_dict()

        assert result["content_types"] == ("application/netcdf",)
        assert result["include_roles"] == ["data"]
        assert result["min_size"] == 100

    def test_combine_filters(self):
        """Test combining two filters."""
        filter1 = AssetFilter(content_types=["application/netcdf"])
        filter2 = AssetFilter(include_roles={"data"})

        combined = filter1.combine(filter2)
        assert combined.content_types == ("application/netcdf",)
        # combine() returns a list, convert to set for comparison
        assert set(combined.include_roles) == {"data"}

    def test_method_chaining(self):
        """Test method chaining for building complex filters."""
        filter_obj = (
            AssetFilter()
            .content_type_filter(["application/netcdf"])
            .role_filter(include_roles={"data"})
            .size_filter(min_size=100, max_size=10000)
        )

        assert filter_obj.content_types == ("application/netcdf",)
        assert filter_obj.include_roles == {"data"}
        assert filter_obj.min_size == 100
        assert filter_obj.max_size == 10000


class TestFilterAssets:
    """Test filter_assets function."""

    def test_filter_empty_list(self):
        """Test filtering empty asset list."""
        result = filter_assets([], AssetFilter(content_types=["application/netcdf"]))
        assert result == []

    def test_filter_no_filter(self):
        """Test filtering without filter returns copy."""
        assets = [
            Asset(href="https://example.com/file1.nc"),
            Asset(href="https://example.com/file2.nc"),
        ]
        result = filter_assets(assets, None)

        assert len(result) == 2
        assert result[0].href == "https://example.com/file1.nc"
        assert result[1].href == "https://example.com/file2.nc"

    def test_filter_by_content_type(self):
        """Test filtering by content type."""
        assets = [
            Asset(href="https://example.com/file1.nc", type="application/netcdf"),
            Asset(href="https://example.com/file2.jpg", type="image/jpeg"),
            Asset(href="https://example.com/file3.tif", type="image/tiff"),
        ]

        result = filter_assets(
            assets, AssetFilter(content_types=["application/netcdf"])
        )
        assert len(result) == 1
        assert result[0].type == "application/netcdf"

        result = filter_assets(
            assets, AssetFilter(content_types=["image/jpeg", "image/tiff"])
        )
        assert len(result) == 2

    def test_filter_exclude_content_type(self):
        """Test excluding content types."""
        assets = [
            Asset(href="https://example.com/file1.nc", type="application/netcdf"),
            Asset(href="https://example.com/file2.jpg", type="image/jpeg"),
            Asset(href="https://example.com/file3.tif", type="image/tiff"),
        ]

        result = filter_assets(
            assets, AssetFilter(exclude_content_types=["image/jpeg"])
        )
        assert len(result) == 2
        assert all(a.type != "image/jpeg" for a in result)

    def test_filter_by_include_roles(self):
        """Test filtering by included roles."""
        assets = [
            Asset(href="https://example.com/file1.nc", roles={"data", "science-data"}),
            Asset(href="https://example.com/file2.jpg", roles={"thumbnail"}),
            Asset(href="https://example.com/file3.xml", roles={"metadata"}),
        ]

        result = filter_assets(assets, AssetFilter(include_roles={"data"}))
        assert len(result) == 1
        assert "data" in result[0].roles

    def test_filter_by_exclude_roles(self):
        """Test filtering by excluded roles."""
        assets = [
            Asset(href="https://example.com/file1.nc", roles={"data"}),
            Asset(href="https://example.com/file2.jpg", roles={"thumbnail", "browse"}),
            Asset(href="https://example.com/file3.xml", roles={"metadata"}),
        ]

        result = filter_assets(assets, AssetFilter(exclude_roles={"thumbnail"}))
        assert len(result) == 2
        assert all("thumbnail" not in a.roles for a in result)

    def test_filter_by_bands(self):
        """Test filtering by bands."""
        assets = [
            Asset(href="https://example.com/file1.nc", bands=["band1", "band2"]),
            Asset(href="https://example.com/file2.nc", bands=["band3"]),
            Asset(href="https://example.com/file3.nc", bands=None),
        ]

        result = filter_assets(assets, AssetFilter(bands=["band1", "band2"]))
        assert len(result) == 1
        assert set(result[0].bands) == {"band1", "band2"}

    def test_filter_by_size_range(self):
        """Test filtering by size range."""
        assets = [
            Asset(href="https://example.com/file1.nc", file_size=1000),
            Asset(href="https://example.com/file2.nc", file_size=5000),
            Asset(href="https://example.com/file3.nc", file_size=10000),
        ]

        result = filter_assets(assets, AssetFilter(min_size=2000, max_size=8000))
        assert len(result) == 1
        assert result[0].file_size == 5000

    def test_filter_by_checksum(self):
        """Test filtering by checksum."""
        assets = [
            Asset(href="https://example.com/file1.nc", checksum="abc123"),
            Asset(href="https://example.com/file2.nc", checksum="def456"),
            Asset(href="https://example.com/file3.nc", checksum=None),
        ]

        # Filter returns assets that have a checksum matching any value in the dict
        result = filter_assets(
            assets, AssetFilter(checksums={"file1": "abc123", "file2": "def456"})
        )
        assert len(result) == 3  # None checksum passes the test

    def test_filter_exclude_missing_checksum(self):
        """Test excluding assets without checksums."""
        assets = [
            Asset(href="https://example.com/file1.nc", checksum="abc123"),
            Asset(href="https://example.com/file2.nc", checksum=None),
        ]

        result = filter_assets(assets, AssetFilter(exclude_missing_checksum=True))
        assert len(result) == 1
        assert result[0].checksum == "abc123"

    def test_filter_by_filename_pattern(self):
        """Test filtering by filename pattern."""
        assets = [
            Asset(href="https://example.com/data_file.nc"),
            Asset(href="https://example.com/metadata.xml"),
            Asset(href="https://example.com/browse.jpg"),
        ]

        result = filter_assets(
            assets, AssetFilter(filename_patterns=["data_*.nc", "*.xml"])
        )
        assert len(result) == 2

    def test_filter_by_exclude_filename_pattern(self):
        """Test excluding assets by filename pattern."""
        assets = [
            Asset(href="https://example.com/data_file.nc"),
            Asset(href="https://example.com/metadata.xml"),
            Asset(href="https://example.com/browse.jpg"),
        ]

        result = filter_assets(
            assets, AssetFilter(exclude_filename_patterns=["*metadata*"])
        )
        assert len(result) == 2
        assert "metadata" not in result[0].href
        assert "metadata" not in result[1].href


class TestAssetHelpers:
    """Test asset helper functions."""

    def test_get_data_assets(self):
        """Test getting data assets."""
        assets = [
            Asset(href="https://example.com/file1.nc", type="data"),
            Asset(href="https://example.com/file2.nc", type="science-data"),
            Asset(href="https://example.com/thumb.jpg", type="image"),
        ]

        result = get_data_assets(assets)
        assert len(result) == 2

    def test_get_thumbnail_assets(self):
        """Test getting thumbnail assets."""
        assets = [
            Asset(href="https://example.com/file1.nc", roles={"data"}),
            Asset(href="https://example.com/thumb.jpg", roles={"thumbnail"}),
        ]

        result = get_thumbnail_assets(assets)
        assert len(result) == 1
        assert "thumbnail" in result[0].roles

    def test_get_browse_assets(self):
        """Test getting browse assets."""
        assets = [
            Asset(href="https://example.com/file1.nc", roles={"data"}),
            Asset(href="https://example.com/browse.jpg", roles={"browse"}),
        ]

        result = get_browse_assets(assets)
        assert len(result) == 1
        assert "browse" in result[0].roles

    def test_get_assets_by_band(self):
        """Test getting assets by band."""
        assets = [
            Asset(href="https://example.com/file1.nc", bands=["band1", "band2"]),
            Asset(href="https://example.com/file2.nc", bands=["band2", "band3"]),
        ]

        result = get_assets_by_band(assets, ["band1"])
        assert len(result) == 1

        result = get_assets_by_band(assets, ["band2"])
        assert len(result) == 2

    def test_get_assets_by_size_range(self):
        """Test getting assets by size range."""
        assets = [
            Asset(href="https://example.com/file1.nc", file_size=1000),
            Asset(href="https://example.com/file2.nc", file_size=5000),
            Asset(href="https://example.com/file3.nc", file_size=10000),
        ]

        result = get_assets_by_size_range(assets, 2000, 8000)
        assert len(result) == 1
        assert result[0].file_size == 5000
