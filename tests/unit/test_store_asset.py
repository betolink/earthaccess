"""TDD Tests for earthaccess.assets module.

These tests define the expected behavior of asset classes before
implementation, following TDD principles. Tests are independent and
designed to be run in any order (no test pollution).

SOLID Principles:
- Single Responsibility: Each test class tests one component
- Interface Segregation: Tests use only public interfaces
- Dependency Inversion: Tests use mocks for dependencies
"""

import pytest
from earthaccess.assets import Asset, AssetFilter, filter_assets

# =============================================================================
# Asset Tests - SOLID: Single Responsibility
# =============================================================================


class TestAssetCreation:
    """Test Asset creation and basic properties."""

    def test_create_minimal_asset(self):
        """Asset can be created with only required fields."""
        asset = Asset(href="https://example.com/file.nc")
        assert asset.href == "https://example.com/file.nc"
        assert asset.title is None
        assert asset.description is None
        assert asset.type is None
        assert asset.roles == []
        assert asset.size is None

    def test_create_with_all_fields(self):
        """Asset can be created with all optional fields."""
        asset = Asset(
            href="s3://bucket/file.tif",
            title="Temperature Data",
            description="Monthly temperature anomalies",
            type="image/tiff; application=geotiff",
            roles=["data", "cloud-optimized"],
            size=1024 * 1024 * 100,  # 100 MB
        )
        assert asset.href == "s3://bucket/file.tif"
        assert asset.title == "Temperature Data"
        assert asset.description == "Monthly temperature anomalies"
        assert asset.type == "image/tiff; application=geotiff"
        assert asset.roles == ["data", "cloud-optimized"]
        assert asset.size == 1024 * 1024 * 100

    def test_frozen_immutable(self):
        """Asset is frozen and cannot be modified."""
        asset = Asset(href="file.nc")
        with pytest.raises(Exception):  # FrozenInstanceError
            asset.href = "other.nc"

    def test_hashable_for_dict_keys(self):
        """Asset is hashable and can be used as dict key when roles is empty."""
        # Note: Assets with empty roles are hashable because roles
        # has hash=False, so it doesn't affect the hash
        asset1 = Asset(href="file1.nc")
        asset2 = Asset(href="file2.nc")
        asset_dict = {asset1: "first", asset2: "second"}
        assert asset_dict[asset1] == "first"
        assert asset_dict[asset2] == "second"


class TestAssetRoleChecking:
    """Test Asset role checking helper methods."""

    def test_is_data_with_data_role(self):
        """is_data() returns True when 'data' role present."""
        asset = Asset(href="file.nc", roles=["data"])
        assert asset.is_data() is True

    def test_is_data_without_data_role(self):
        """is_data() returns False when 'data' role absent."""
        asset = Asset(href="file.png", roles=["thumbnail"])
        assert asset.is_data() is False

    def test_is_data_with_multiple_roles(self):
        """is_data() returns True with 'data' in multiple roles."""
        asset = Asset(href="file.nc", roles=["data", "cloud-optimized"])
        assert asset.is_data() is True

    def test_is_thumbnail_with_thumbnail_role(self):
        """is_thumbnail() returns True when 'thumbnail' role present."""
        asset = Asset(href="thumb.png", roles=["thumbnail"])
        assert asset.is_thumbnail() is True

    def test_is_thumbnail_without_thumbnail_role(self):
        """is_thumbnail() returns False when 'thumbnail' role absent."""
        asset = Asset(href="file.nc", roles=["data"])
        assert asset.is_thumbnail() is False

    def test_is_metadata_with_metadata_role(self):
        """is_metadata() returns True when 'metadata' role present."""
        asset = Asset(href="meta.xml", roles=["metadata"])
        assert asset.is_metadata() is True

    def test_is_metadata_without_metadata_role(self):
        """is_metadata() returns False when 'metadata' role absent."""
        asset = Asset(href="file.nc", roles=["data"])
        assert asset.is_metadata() is False

    def test_is_cloud_optimized_with_role(self):
        """is_cloud_optimized() returns True with 'cloud-optimized' role."""
        asset = Asset(href="s3://bucket/file.tif", roles=["data", "cloud-optimized"])
        assert asset.is_cloud_optimized() is True

    def test_is_cloud_optimized_without_role(self):
        """is_cloud_optimized() returns False without 'cloud-optimized' role."""
        asset = Asset(href="https://server/file.tif", roles=["data"])
        assert asset.is_cloud_optimized() is False

    def test_has_role_with_existing_role(self):
        """has_role() returns True for roles present in asset."""
        asset = Asset(href="file.nc", roles=["data", "metadata"])
        assert asset.has_role("data") is True
        assert asset.has_role("metadata") is True

    def test_has_role_with_missing_role(self):
        """has_role() returns False for roles not in asset."""
        asset = Asset(href="file.nc", roles=["data"])
        assert asset.has_role("thumbnail") is False
        assert asset.has_role("cloud-optimized") is False


class TestAssetFilterMatching:
    """Test AssetFilter matching logic."""

    def test_empty_filter_matches_all(self):
        """Empty filter matches any asset."""
        filter = AssetFilter()
        assets = [
            Asset(href="file.nc", roles=["data"]),
            Asset(href="thumb.png", roles=["thumbnail"]),
            Asset(href="meta.xml", roles=["metadata"]),
        ]
        assert all(filter.matches(a) for a in assets)

    def test_include_pattern_matching(self):
        """Filter with include_patterns matches only matching files."""
        filter = AssetFilter(include_patterns=["*.nc"])
        assert filter.matches(Asset(href="file.nc"))
        assert not filter.matches(Asset(href="file.tif"))
        assert not filter.matches(Asset(href="file.xml"))

    def test_multiple_include_patterns(self):
        """Filter with multiple include patterns uses OR logic."""
        filter = AssetFilter(include_patterns=["*.nc", "*.hdf5"])
        assert filter.matches(Asset(href="file.nc"))
        assert filter.matches(Asset(href="file.hdf5"))
        assert not filter.matches(Asset(href="file.tif"))

    def test_exclude_pattern_matching(self):
        """Filter with exclude_patterns rejects matching files."""
        filter = AssetFilter(exclude_patterns=["*_thumbnail*"])
        assert filter.matches(Asset(href="file.nc"))
        assert not filter.matches(Asset(href="file_thumbnail.png"))

    def test_include_and_exclude_patterns(self):
        """Filter combines include and exclude patterns correctly."""
        filter = AssetFilter(
            include_patterns=["*.tif"],
            exclude_patterns=["*_browse*"],
        )
        assert filter.matches(Asset(href="file.tif"))
        assert not filter.matches(Asset(href="file_browse.tif"))
        assert not filter.matches(Asset(href="file.nc"))

    def test_include_role_matching(self):
        """Filter with include_roles requires matching roles."""
        filter = AssetFilter(include_roles=["data"])
        assert filter.matches(Asset(href="file.nc", roles=["data"]))
        assert not filter.matches(Asset(href="thumb.png", roles=["thumbnail"]))

    def test_multiple_include_roles(self):
        """Filter with multiple include_roles uses OR logic."""
        filter = AssetFilter(include_roles=["data", "metadata"])
        assert filter.matches(Asset(href="file.nc", roles=["data"]))
        assert filter.matches(Asset(href="meta.xml", roles=["metadata"]))
        assert not filter.matches(Asset(href="thumb.png", roles=["thumbnail"]))

    def test_exclude_role_matching(self):
        """Filter with exclude_roles rejects matching roles."""
        filter = AssetFilter(exclude_roles=["thumbnail"])
        assert filter.matches(Asset(href="file.nc", roles=["data"]))
        assert not filter.matches(Asset(href="thumb.png", roles=["thumbnail"]))

    def test_include_and_exclude_roles(self):
        """Filter combines include and exclude roles correctly."""
        filter = AssetFilter(
            include_roles=["data"],
            exclude_roles=["thumbnail"],
        )
        assert filter.matches(Asset(href="file.nc", roles=["data"]))
        assert not filter.matches(Asset(href="file.nc", roles=["thumbnail"]))
        assert not filter.matches(Asset(href="file.nc", roles=["metadata"]))

    def test_min_size_filtering(self):
        """Filter with min_size rejects smaller files."""
        min_size = 1024 * 1024  # 1 MB
        filter = AssetFilter(min_size=min_size)
        assert filter.matches(Asset(href="file.nc", size=min_size))
        assert filter.matches(Asset(href="file.nc", size=min_size + 1))
        assert not filter.matches(Asset(href="file.nc", size=min_size - 1))

    def test_max_size_filtering(self):
        """Filter with max_size rejects larger files."""
        max_size = 1024 * 1024 * 100  # 100 MB
        filter = AssetFilter(max_size=max_size)
        assert filter.matches(Asset(href="file.nc", size=max_size))
        assert filter.matches(Asset(href="file.nc", size=max_size - 1))
        assert not filter.matches(Asset(href="file.nc", size=max_size + 1))

    def test_min_max_size_filtering(self):
        """Filter with min and max size bounds correctly."""
        min_size = 1024  # 1 KB
        max_size = 1024 * 1024  # 1 MB
        filter = AssetFilter(min_size=min_size, max_size=max_size)
        assert filter.matches(Asset(href="file.nc", size=min_size))
        assert filter.matches(Asset(href="file.nc", size=max_size))
        assert filter.matches(Asset(href="file.nc", size=(min_size + max_size) // 2))
        assert not filter.matches(Asset(href="file.nc", size=min_size - 1))
        assert not filter.matches(Asset(href="file.nc", size=max_size + 1))

    def test_size_filtering_ignores_none(self):
        """Filter ignores size criteria when asset size is None."""
        filter = AssetFilter(min_size=1024, max_size=1024 * 1024)
        assert filter.matches(Asset(href="file.nc", size=None))


class TestAssetFilterCombination:
    """Test AssetFilter combine() method."""

    def test_combine_empty_filters(self):
        """Combining empty filters results in empty filter."""
        filter1 = AssetFilter()
        filter2 = AssetFilter()
        combined = filter1.combine(filter2)
        assert combined.include_patterns == []
        assert combined.exclude_patterns == []

    def test_combine_include_patterns(self):
        """Combining filters merges include patterns."""
        filter1 = AssetFilter(include_patterns=["*.nc"])
        filter2 = AssetFilter(include_patterns=["*.hdf5"])
        combined = filter1.combine(filter2)
        assert set(combined.include_patterns) == {"*.nc", "*.hdf5"}

    def test_combine_exclude_patterns(self):
        """Combining filters merges exclude patterns."""
        filter1 = AssetFilter(exclude_patterns=["*_thumbnail*"])
        filter2 = AssetFilter(exclude_patterns=["*_browse*"])
        combined = filter1.combine(filter2)
        assert set(combined.exclude_patterns) == {"*_thumbnail*", "*_browse*"}

    def test_combine_include_roles(self):
        """Combining filters merges include roles."""
        filter1 = AssetFilter(include_roles=["data"])
        filter2 = AssetFilter(include_roles=["metadata"])
        combined = filter1.combine(filter2)
        assert set(combined.include_roles) == {"data", "metadata"}

    def test_combine_exclude_roles(self):
        """Combining filters merges exclude roles."""
        filter1 = AssetFilter(exclude_roles=["thumbnail"])
        filter2 = AssetFilter(exclude_roles=["browse"])
        combined = filter1.combine(filter2)
        assert set(combined.exclude_roles) == {"thumbnail", "browse"}

    def test_combine_min_size_takes_maximum(self):
        """Combining filters with min_size takes the larger value."""
        filter1 = AssetFilter(min_size=1024)
        filter2 = AssetFilter(min_size=2048)
        combined = filter1.combine(filter2)
        assert combined.min_size == 2048

    def test_combine_max_size_takes_minimum(self):
        """Combining filters with max_size takes the smaller value."""
        filter1 = AssetFilter(max_size=1024 * 1024 * 100)
        filter2 = AssetFilter(max_size=1024 * 1024 * 50)
        combined = filter1.combine(filter2)
        assert combined.max_size == 1024 * 1024 * 50

    def test_combine_complex_filters(self):
        """Combining complex filters combines all criteria correctly."""
        filter1 = AssetFilter(
            include_patterns=["*.nc"],
            exclude_roles=["thumbnail"],
            min_size=1024,
        )
        filter2 = AssetFilter(
            include_patterns=["*.hdf5"],
            exclude_roles=["browse"],
            max_size=1024 * 1024 * 100,
        )
        combined = filter1.combine(filter2)
        assert set(combined.include_patterns) == {"*.nc", "*.hdf5"}
        assert set(combined.exclude_roles) == {"thumbnail", "browse"}
        assert combined.min_size == 1024
        assert combined.max_size == 1024 * 1024 * 100


class TestAssetFilterFromDict:
    """Test AssetFilter.from_dict() creation method."""

    def test_from_dict_empty(self):
        """from_dict with empty dict creates empty filter."""
        filter = AssetFilter.from_dict({})
        assert filter.include_patterns == []
        assert filter.exclude_patterns == []

    def test_from_dict_with_include_patterns(self):
        """from_dict extracts include_patterns."""
        filter = AssetFilter.from_dict({"include_patterns": ["*.nc", "*.hdf5"]})
        assert filter.include_patterns == ["*.nc", "*.hdf5"]

    def test_from_dict_with_exclude_patterns(self):
        """from_dict extracts exclude_patterns."""
        filter = AssetFilter.from_dict({"exclude_patterns": ["*_thumbnail*"]})
        assert filter.exclude_patterns == ["*_thumbnail*"]

    def test_from_dict_with_include_roles(self):
        """from_dict extracts include_roles."""
        filter = AssetFilter.from_dict({"include_roles": ["data"]})
        assert filter.include_roles == ["data"]

    def test_from_dict_with_exclude_roles(self):
        """from_dict extracts exclude_roles."""
        filter = AssetFilter.from_dict({"exclude_roles": ["thumbnail"]})
        assert filter.exclude_roles == ["thumbnail"]

    def test_from_dict_with_size_constraints(self):
        """from_dict extracts size constraints."""
        filter = AssetFilter.from_dict(
            {
                "min_size": 1024,
                "max_size": 1024 * 1024 * 100,
            }
        )
        assert filter.min_size == 1024
        assert filter.max_size == 1024 * 1024 * 100

    def test_from_dict_ignores_unknown_keys(self):
        """from_dict ignores unknown dictionary keys."""
        filter = AssetFilter.from_dict(
            {
                "include_patterns": ["*.nc"],
                "unknown_key": "unknown_value",
                "another_unknown": 123,
            }
        )
        assert filter.include_patterns == ["*.nc"]


class TestFilterAssetsHelper:
    """Test filter_assets() helper function."""

    def test_filter_assets_empty_list(self):
        """filter_assets with empty list returns empty list."""
        filter = AssetFilter(include_patterns=["*.nc"])
        result = filter_assets([], filter)
        assert result == []

    def test_filter_assets_no_matches(self):
        """filter_assets with no matching assets returns empty list."""
        assets = [
            Asset(href="file1.tif", roles=["data"]),
            Asset(href="file2.tif", roles=["data"]),
        ]
        filter = AssetFilter(include_patterns=["*.nc"])
        result = filter_assets(assets, filter)
        assert result == []

    def test_filter_assets_all_match(self):
        """filter_assets with all matching assets returns all."""
        assets = [
            Asset(href="file1.nc", roles=["data"]),
            Asset(href="file2.nc", roles=["data"]),
        ]
        filter = AssetFilter(include_patterns=["*.nc"])
        result = filter_assets(assets, filter)
        assert result == assets

    def test_filter_assets_partial_match(self):
        """filter_assets returns only matching assets."""
        assets = [
            Asset(href="file1.nc", roles=["data"]),
            Asset(href="file2.tif", roles=["data"]),
            Asset(href="file3.nc", roles=["data"]),
        ]
        filter = AssetFilter(include_patterns=["*.nc"])
        result = filter_assets(assets, filter)
        assert len(result) == 2
        assert all(a.href.endswith(".nc") for a in result)

    def test_filter_assets_by_role(self):
        """filter_assets filters by role correctly."""
        assets = [
            Asset(href="file.nc", roles=["data"]),
            Asset(href="thumb.png", roles=["thumbnail"]),
            Asset(href="meta.xml", roles=["metadata"]),
        ]
        filter = AssetFilter(include_roles=["data"])
        result = filter_assets(assets, filter)
        assert len(result) == 1
        assert result[0].href == "file.nc"


class TestAssetMatchesFilter:
    """Test Asset.matches_filter() delegation."""

    def test_matches_filter_delegates_to_filter(self):
        """Asset.matches_filter() delegates to filter.matches()."""
        asset = Asset(href="file.nc", roles=["data"])
        filter = AssetFilter(include_patterns=["*.nc"])
        assert asset.matches_filter(filter) is True

    def test_matches_filter_returns_false(self):
        """Asset.matches_filter() returns False for non-matching filter."""
        asset = Asset(href="file.tif", roles=["data"])
        filter = AssetFilter(include_patterns=["*.nc"])
        assert asset.matches_filter(filter) is False


class TestAssetPatternMatching:
    """Test glob pattern matching edge cases."""

    def test_pattern_matching_with_s3_paths(self):
        """Pattern matching works with S3 paths."""
        filter = AssetFilter(include_patterns=["*.tif"])
        assert filter.matches(Asset(href="s3://bucket/path/to/file.tif"))
        assert not filter.matches(Asset(href="s3://bucket/path/to/file.nc"))

    def test_pattern_matching_with_https_urls(self):
        """Pattern matching works with HTTPS URLs."""
        filter = AssetFilter(include_patterns=["*.nc"])
        assert filter.matches(Asset(href="https://server.com/path/to/file.nc"))
        assert not filter.matches(Asset(href="https://server.com/path/to/file.tif"))

    def test_pattern_matching_wildcards(self):
        """Pattern matching supports wildcards correctly."""
        filter = AssetFilter(include_patterns=["file_*.nc"])
        assert filter.matches(Asset(href="file_001.nc"))
        assert filter.matches(Asset(href="file_abc.nc"))
        assert not filter.matches(Asset(href="file.nc"))

    def test_pattern_matching_case_sensitive(self):
        """Pattern matching is case-sensitive."""
        filter = AssetFilter(include_patterns=["*.NC"])
        assert filter.matches(Asset(href="file.NC"))
        assert not filter.matches(Asset(href="file.nc"))
