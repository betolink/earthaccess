"""Tests for earthaccess.assets module.

These tests verify the behavior of Asset and AssetFilter classes.
Tests are consolidated using parametrization where appropriate.
"""

import pytest
from earthaccess.assets import Asset, AssetFilter, filter_assets

# =============================================================================
# Asset Creation Tests
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
            size=1024 * 1024 * 100,
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
        with pytest.raises(Exception):
            asset.href = "other.nc"

    def test_hashable_for_dict_keys(self):
        """Asset is hashable and can be used as dict key."""
        asset1 = Asset(href="file1.nc")
        asset2 = Asset(href="file2.nc")
        asset_dict = {asset1: "first", asset2: "second"}
        assert asset_dict[asset1] == "first"
        assert asset_dict[asset2] == "second"


# =============================================================================
# Asset Role Checking Tests (Parametrized)
# =============================================================================


class TestAssetRoleChecking:
    """Test Asset role checking helper methods."""

    @pytest.mark.parametrize(
        "roles,method,expected",
        [
            # is_data tests
            (["data"], "is_data", True),
            (["thumbnail"], "is_data", False),
            (["data", "cloud-optimized"], "is_data", True),
            # is_thumbnail tests
            (["thumbnail"], "is_thumbnail", True),
            (["data"], "is_thumbnail", False),
            # is_metadata tests
            (["metadata"], "is_metadata", True),
            (["data"], "is_metadata", False),
            # is_cloud_optimized tests
            (["data", "cloud-optimized"], "is_cloud_optimized", True),
            (["data"], "is_cloud_optimized", False),
        ],
        ids=[
            "is_data-with_data_role",
            "is_data-without_data_role",
            "is_data-with_multiple_roles",
            "is_thumbnail-with_thumbnail_role",
            "is_thumbnail-without_thumbnail_role",
            "is_metadata-with_metadata_role",
            "is_metadata-without_metadata_role",
            "is_cloud_optimized-with_role",
            "is_cloud_optimized-without_role",
        ],
    )
    def test_role_checking_methods(self, roles, method, expected):
        """Test role checking methods return correct boolean values."""
        asset = Asset(href="file.nc", roles=roles)
        assert getattr(asset, method)() is expected

    @pytest.mark.parametrize(
        "roles,check_role,expected",
        [
            (["data", "metadata"], "data", True),
            (["data", "metadata"], "metadata", True),
            (["data"], "thumbnail", False),
            (["data"], "cloud-optimized", False),
        ],
        ids=[
            "has_data",
            "has_metadata",
            "missing_thumbnail",
            "missing_cloud_optimized",
        ],
    )
    def test_has_role(self, roles, check_role, expected):
        """Test has_role() returns correct value for various roles."""
        asset = Asset(href="file.nc", roles=roles)
        assert asset.has_role(check_role) is expected


# =============================================================================
# AssetFilter Matching Tests (Parametrized)
# =============================================================================


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

    @pytest.mark.parametrize(
        "patterns,href,expected",
        [
            # Single pattern
            (["*.nc"], "file.nc", True),
            (["*.nc"], "file.tif", False),
            (["*.nc"], "file.xml", False),
            # Multiple patterns (OR logic)
            (["*.nc", "*.hdf5"], "file.nc", True),
            (["*.nc", "*.hdf5"], "file.hdf5", True),
            (["*.nc", "*.hdf5"], "file.tif", False),
            # Wildcard patterns
            (["file_*.nc"], "file_001.nc", True),
            (["file_*.nc"], "file_abc.nc", True),
            (["file_*.nc"], "file.nc", False),
            # Case sensitivity
            (["*.NC"], "file.NC", True),
            (["*.NC"], "file.nc", False),
        ],
        ids=[
            "single_match",
            "single_no_match_tif",
            "single_no_match_xml",
            "multi_match_nc",
            "multi_match_hdf5",
            "multi_no_match",
            "wildcard_match_001",
            "wildcard_match_abc",
            "wildcard_no_match",
            "case_match_upper",
            "case_no_match_lower",
        ],
    )
    def test_include_pattern_matching(self, patterns, href, expected):
        """Filter with include_patterns matches correctly."""
        filter = AssetFilter(include_patterns=patterns)
        assert filter.matches(Asset(href=href)) is expected

    @pytest.mark.parametrize(
        "patterns,href,expected",
        [
            (["*_thumbnail*"], "file.nc", True),
            (["*_thumbnail*"], "file_thumbnail.png", False),
        ],
        ids=["exclude_no_match", "exclude_match"],
    )
    def test_exclude_pattern_matching(self, patterns, href, expected):
        """Filter with exclude_patterns rejects correctly."""
        filter = AssetFilter(exclude_patterns=patterns)
        assert filter.matches(Asset(href=href)) is expected

    def test_include_and_exclude_patterns(self):
        """Filter combines include and exclude patterns correctly."""
        filter = AssetFilter(
            include_patterns=["*.tif"],
            exclude_patterns=["*_browse*"],
        )
        assert filter.matches(Asset(href="file.tif"))
        assert not filter.matches(Asset(href="file_browse.tif"))
        assert not filter.matches(Asset(href="file.nc"))

    @pytest.mark.parametrize(
        "include_roles,roles,expected",
        [
            # Single role
            (["data"], ["data"], True),
            (["data"], ["thumbnail"], False),
            # Multiple include roles (OR logic)
            (["data", "metadata"], ["data"], True),
            (["data", "metadata"], ["metadata"], True),
            (["data", "metadata"], ["thumbnail"], False),
        ],
        ids=[
            "include_match",
            "include_no_match",
            "multi_include_data",
            "multi_include_metadata",
            "multi_include_no_match",
        ],
    )
    def test_include_role_matching(self, include_roles, roles, expected):
        """Filter with include_roles requires matching roles."""
        filter = AssetFilter(include_roles=include_roles)
        assert filter.matches(Asset(href="file.nc", roles=roles)) is expected

    @pytest.mark.parametrize(
        "exclude_roles,roles,expected",
        [
            (["thumbnail"], ["data"], True),
            (["thumbnail"], ["thumbnail"], False),
        ],
        ids=["exclude_no_match", "exclude_match"],
    )
    def test_exclude_role_matching(self, exclude_roles, roles, expected):
        """Filter with exclude_roles rejects matching roles."""
        filter = AssetFilter(exclude_roles=exclude_roles)
        assert filter.matches(Asset(href="file.nc", roles=roles)) is expected

    def test_include_and_exclude_roles(self):
        """Filter combines include and exclude roles correctly."""
        filter = AssetFilter(include_roles=["data"], exclude_roles=["thumbnail"])
        assert filter.matches(Asset(href="file.nc", roles=["data"]))
        assert not filter.matches(Asset(href="file.nc", roles=["thumbnail"]))
        assert not filter.matches(Asset(href="file.nc", roles=["metadata"]))

    @pytest.mark.parametrize(
        "min_size,max_size,size,expected",
        [
            # Min size only
            (1024, None, 1024, True),
            (1024, None, 1025, True),
            (1024, None, 1023, False),
            # Max size only
            (None, 1024 * 1024, 1024 * 1024, True),
            (None, 1024 * 1024, 1024 * 1024 - 1, True),
            (None, 1024 * 1024, 1024 * 1024 + 1, False),
            # Both min and max
            (1024, 1024 * 1024, 1024, True),
            (1024, 1024 * 1024, 1024 * 1024, True),
            (1024, 1024 * 1024, 512 * 1024, True),
            (1024, 1024 * 1024, 1023, False),
            (1024, 1024 * 1024, 1024 * 1024 + 1, False),
        ],
        ids=[
            "min_equal",
            "min_above",
            "min_below",
            "max_equal",
            "max_below",
            "max_above",
            "both_at_min",
            "both_at_max",
            "both_in_range",
            "both_below_min",
            "both_above_max",
        ],
    )
    def test_size_filtering(self, min_size, max_size, size, expected):
        """Filter size bounds work correctly."""
        filter = AssetFilter(min_size=min_size, max_size=max_size)
        assert filter.matches(Asset(href="file.nc", size=size)) is expected

    def test_size_filtering_ignores_none(self):
        """Filter ignores size criteria when asset size is None."""
        filter = AssetFilter(min_size=1024, max_size=1024 * 1024)
        assert filter.matches(Asset(href="file.nc", size=None))

    @pytest.mark.parametrize(
        "href",
        [
            "s3://bucket/path/to/file.tif",
            "https://server.com/path/to/file.tif",
        ],
        ids=["s3_path", "https_url"],
    )
    def test_pattern_matching_with_different_protocols(self, href):
        """Pattern matching works with S3 and HTTPS paths."""
        filter = AssetFilter(include_patterns=["*.tif"])
        assert filter.matches(Asset(href=href))


# =============================================================================
# AssetFilter Combination Tests (Parametrized)
# =============================================================================


class TestAssetFilterCombination:
    """Test AssetFilter combine() method."""

    def test_combine_empty_filters(self):
        """Combining empty filters results in empty filter."""
        combined = AssetFilter().combine(AssetFilter())
        assert combined.include_patterns == []
        assert combined.exclude_patterns == []

    @pytest.mark.parametrize(
        "attr,val1,val2,expected",
        [
            ("include_patterns", ["*.nc"], ["*.hdf5"], {"*.nc", "*.hdf5"}),
            (
                "exclude_patterns",
                ["*_thumb*"],
                ["*_browse*"],
                {"*_thumb*", "*_browse*"},
            ),
            ("include_roles", ["data"], ["metadata"], {"data", "metadata"}),
            ("exclude_roles", ["thumbnail"], ["browse"], {"thumbnail", "browse"}),
        ],
        ids=["include_patterns", "exclude_patterns", "include_roles", "exclude_roles"],
    )
    def test_combine_merges_lists(self, attr, val1, val2, expected):
        """Combining filters merges list attributes."""
        filter1 = AssetFilter(**{attr: val1})
        filter2 = AssetFilter(**{attr: val2})
        combined = filter1.combine(filter2)
        assert set(getattr(combined, attr)) == expected

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


# =============================================================================
# AssetFilter.from_dict Tests (Parametrized)
# =============================================================================


class TestAssetFilterFromDict:
    """Test AssetFilter.from_dict() creation method."""

    @pytest.mark.parametrize(
        "input_dict,attr,expected",
        [
            ({}, "include_patterns", []),
            ({}, "exclude_patterns", []),
            (
                {"include_patterns": ["*.nc", "*.hdf5"]},
                "include_patterns",
                ["*.nc", "*.hdf5"],
            ),
            (
                {"exclude_patterns": ["*_thumbnail*"]},
                "exclude_patterns",
                ["*_thumbnail*"],
            ),
            ({"include_roles": ["data"]}, "include_roles", ["data"]),
            ({"exclude_roles": ["thumbnail"]}, "exclude_roles", ["thumbnail"]),
            ({"min_size": 1024}, "min_size", 1024),
            ({"max_size": 1024 * 1024 * 100}, "max_size", 1024 * 1024 * 100),
        ],
        ids=[
            "empty_include",
            "empty_exclude",
            "include_patterns",
            "exclude_patterns",
            "include_roles",
            "exclude_roles",
            "min_size",
            "max_size",
        ],
    )
    def test_from_dict_extracts_values(self, input_dict, attr, expected):
        """from_dict extracts values correctly."""
        filter = AssetFilter.from_dict(input_dict)
        assert getattr(filter, attr) == expected

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


# =============================================================================
# filter_assets Helper Tests
# =============================================================================


class TestFilterAssetsHelper:
    """Test filter_assets() helper function."""

    @pytest.mark.parametrize(
        "assets,patterns,expected_count",
        [
            # Empty list
            ([], ["*.nc"], 0),
            # No matches
            (
                [Asset(href="file1.tif"), Asset(href="file2.tif")],
                ["*.nc"],
                0,
            ),
            # All match
            (
                [Asset(href="file1.nc"), Asset(href="file2.nc")],
                ["*.nc"],
                2,
            ),
            # Partial match
            (
                [
                    Asset(href="file1.nc"),
                    Asset(href="file2.tif"),
                    Asset(href="file3.nc"),
                ],
                ["*.nc"],
                2,
            ),
        ],
        ids=["empty", "no_matches", "all_match", "partial_match"],
    )
    def test_filter_assets_by_pattern(self, assets, patterns, expected_count):
        """filter_assets filters by pattern correctly."""
        filter = AssetFilter(include_patterns=patterns)
        result = filter_assets(assets, filter)
        assert len(result) == expected_count

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


# =============================================================================
# Asset.matches_filter Tests
# =============================================================================


class TestAssetMatchesFilter:
    """Test Asset.matches_filter() delegation."""

    @pytest.mark.parametrize(
        "href,patterns,expected",
        [
            ("file.nc", ["*.nc"], True),
            ("file.tif", ["*.nc"], False),
        ],
        ids=["matches", "no_match"],
    )
    def test_matches_filter_delegates_to_filter(self, href, patterns, expected):
        """Asset.matches_filter() delegates to filter.matches()."""
        asset = Asset(href=href, roles=["data"])
        filter = AssetFilter(include_patterns=patterns)
        assert asset.matches_filter(filter) is expected
