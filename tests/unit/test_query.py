"""Unit tests for the new query package."""

import datetime as dt

import pytest
from earthaccess.query import (
    BoundingBox,
    CollectionQuery,
    DateRange,
    GranuleQuery,
    Point,
    Polygon,
    ValidationResult,
)


class TestBoundingBox:
    """Tests for BoundingBox type."""

    def test_valid_bounding_box(self):
        """Test creating a valid bounding box."""
        bbox = BoundingBox(west=-180, south=-90, east=180, north=90)
        assert bbox.west == -180
        assert bbox.south == -90
        assert bbox.east == 180
        assert bbox.north == 90

    def test_bounding_box_validation(self):
        """Test that invalid bounding boxes raise errors."""
        with pytest.raises(ValueError, match="west must be between"):
            BoundingBox(west=-200, south=0, east=0, north=10)

        with pytest.raises(
            ValueError, match="south.*must be less than or equal to north"
        ):
            BoundingBox(west=0, south=50, east=10, north=10)

    def test_bounding_box_to_cmr(self):
        """Test CMR format conversion."""
        bbox = BoundingBox(west=-10, south=20, east=30, north=40)
        assert bbox.to_cmr() == "-10,20,30,40"

    def test_bounding_box_to_stac(self):
        """Test STAC format conversion."""
        bbox = BoundingBox(west=-10, south=20, east=30, north=40)
        assert bbox.to_stac() == [-10, 20, 30, 40]

    def test_bounding_box_from_coords(self):
        """Test creating from coordinate values."""
        bbox = BoundingBox.from_coords("-10", "20", "30", "40")
        assert bbox == BoundingBox(west=-10, south=20, east=30, north=40)


class TestPoint:
    """Tests for Point type."""

    def test_valid_point(self):
        """Test creating a valid point."""
        pt = Point(lon=-122.4, lat=37.8)
        assert pt.lon == -122.4
        assert pt.lat == 37.8

    def test_point_validation(self):
        """Test that invalid points raise errors."""
        with pytest.raises(ValueError, match="lon must be between"):
            Point(lon=200, lat=0)

        with pytest.raises(ValueError, match="lat must be between"):
            Point(lon=0, lat=-100)

    def test_point_to_cmr(self):
        """Test CMR format conversion."""
        pt = Point(lon=-122.4, lat=37.8)
        assert pt.to_cmr() == "-122.4,37.8"

    def test_point_to_stac(self):
        """Test STAC format conversion."""
        pt = Point(lon=-122.4, lat=37.8)
        assert pt.to_stac() == [-122.4, 37.8]


class TestPolygon:
    """Tests for Polygon type."""

    def test_valid_polygon(self):
        """Test creating a valid polygon."""
        coords = ((0, 0), (10, 0), (10, 10), (0, 10), (0, 0))
        poly = Polygon(coordinates=coords)
        assert len(poly.coordinates) == 5

    def test_polygon_validation(self):
        """Test that invalid polygons raise errors."""
        # Not enough points
        with pytest.raises(ValueError, match="at least 4 points"):
            Polygon(coordinates=((0, 0), (10, 0), (10, 10)))

        # Not closed
        with pytest.raises(ValueError, match="must be closed"):
            Polygon(coordinates=((0, 0), (10, 0), (10, 10), (0, 10)))

    def test_polygon_to_cmr(self):
        """Test CMR format conversion."""
        poly = Polygon.from_coords([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
        assert poly.to_cmr() == "0.0,0.0,10.0,0.0,10.0,10.0,0.0,10.0,0.0,0.0"

    def test_polygon_to_stac(self):
        """Test STAC format conversion."""
        poly = Polygon.from_coords([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
        expected = [[[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]]]
        assert poly.to_stac() == expected


class TestDateRange:
    """Tests for DateRange type."""

    def test_date_range_from_strings(self):
        """Test creating from date strings."""
        dr = DateRange.from_dates("2020-01-01", "2020-12-31")
        assert dr.start == dt.datetime(2020, 1, 1, 0, 0, 0)
        assert dr.end == dt.datetime(2020, 12, 31, 23, 59, 59)

    def test_date_range_from_datetime(self):
        """Test creating from datetime objects."""
        start = dt.datetime(2020, 1, 1, 12, 0, 0)
        end = dt.datetime(2020, 12, 31, 12, 0, 0)
        dr = DateRange.from_dates(start, end)
        assert dr.start == start
        assert dr.end == end

    def test_date_range_month_format(self):
        """Test YYYY-MM format parsing."""
        dr = DateRange.from_dates("2020-01", "2020-02")
        assert dr.start == dt.datetime(2020, 1, 1, 0, 0, 0)
        assert dr.end == dt.datetime(2020, 2, 29, 23, 59, 59)  # 2020 is leap year

    def test_date_range_validation(self):
        """Test that invalid date ranges raise errors."""
        with pytest.raises(ValueError, match="must be before"):
            DateRange.from_dates("2020-12-31", "2020-01-01")

    def test_date_range_to_cmr(self):
        """Test CMR format conversion."""
        dr = DateRange.from_dates("2020-01-01", "2020-01-31")
        assert "2020-01-01" in dr.to_cmr()
        assert "2020-01-31" in dr.to_cmr()

    def test_date_range_to_stac(self):
        """Test STAC format conversion."""
        dr = DateRange.from_dates("2020-01-01", "2020-01-31")
        stac_str = dr.to_stac()
        assert "/" in stac_str  # STAC uses / separator


class TestValidation:
    """Tests for validation utilities."""

    def test_validation_result_success(self):
        """Test successful validation."""
        result = ValidationResult()
        assert result.is_valid
        assert len(result.errors) == 0

    def test_validation_result_add_error(self):
        """Test adding errors."""
        result = ValidationResult()
        result.add_error("field", "is required")
        assert not result.is_valid
        assert len(result.errors) == 1
        assert result.errors[0].field == "field"

    def test_validation_result_raise_if_invalid(self):
        """Test raising on invalid."""
        result = ValidationResult()
        result.add_error("field", "is required")
        with pytest.raises(ValueError, match="field"):
            result.raise_if_invalid()


class TestGranuleQuery:
    """Tests for GranuleQuery class."""

    def test_short_name(self):
        """Test short_name filter."""
        q = GranuleQuery().short_name("ATL03")
        assert q.to_cmr()["short_name"] == "ATL03"

    def test_version(self):
        """Test version filter."""
        q = GranuleQuery().version("006")
        assert q.to_cmr()["version"] == "006"

    def test_concept_id(self):
        """Test concept_id filter."""
        q = GranuleQuery().concept_id(["C123", "C456"])
        assert q.to_cmr()["concept_id"] == ["C123", "C456"]

    def test_temporal(self):
        """Test temporal filter."""
        q = GranuleQuery().temporal("2020-01", "2020-02")
        cmr = q.to_cmr()
        assert "temporal" in cmr
        assert "2020-01" in cmr["temporal"]

    def test_bounding_box(self):
        """Test bounding_box filter."""
        q = GranuleQuery().bounding_box(-180, -90, 180, 90)
        assert q.to_cmr()["bounding_box"] == "-180.0,-90.0,180.0,90.0"

    def test_point(self):
        """Test point filter."""
        q = GranuleQuery().point(-122.4, 37.8)
        assert q.to_cmr()["point"] == "-122.4,37.8"

    def test_polygon(self):
        """Test polygon filter."""
        coords = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
        q = GranuleQuery().polygon(coords)
        assert "polygon" in q.to_cmr()

    def test_cloud_cover(self):
        """Test cloud_cover filter."""
        q = GranuleQuery().cloud_cover(0, 50)
        assert q.to_cmr()["cloud_cover"] == "0.0,50.0"

    def test_method_chaining(self):
        """Test that methods can be chained."""
        q = (
            GranuleQuery()
            .short_name("ATL03")
            .version("006")
            .temporal("2020-01", "2020-02")
            .bounding_box(-180, -90, 180, 90)
        )
        cmr = q.to_cmr()
        assert cmr["short_name"] == "ATL03"
        assert cmr["version"] == "006"
        assert "temporal" in cmr
        assert "bounding_box" in cmr

    def test_named_parameters(self):
        """Test construction with named parameters."""
        q = GranuleQuery(
            short_name="ATL03",
            version="006",
            temporal=("2020-01", "2020-02"),
        )
        cmr = q.to_cmr()
        assert cmr["short_name"] == "ATL03"
        assert cmr["version"] == "006"
        assert "temporal" in cmr

    def test_to_stac_collections(self):
        """Test STAC conversion maps short_name to collections."""
        q = GranuleQuery().short_name("ATL03")
        stac = q.to_stac()
        assert stac["collections"] == ["ATL03"]

    def test_to_stac_bbox(self):
        """Test STAC conversion for bounding box."""
        q = GranuleQuery().bounding_box(-10, 20, 30, 40)
        stac = q.to_stac()
        assert stac["bbox"] == [-10.0, 20.0, 30.0, 40.0]

    def test_to_stac_datetime(self):
        """Test STAC conversion for temporal."""
        q = GranuleQuery().temporal("2020-01-01", "2020-12-31")
        stac = q.to_stac()
        assert "datetime" in stac
        assert "/" in stac["datetime"]

    def test_to_stac_polygon(self):
        """Test STAC conversion for polygon."""
        coords = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
        q = GranuleQuery().polygon(coords)
        stac = q.to_stac()
        assert "intersects" in stac
        assert stac["intersects"]["type"] == "Polygon"

    def test_copy(self):
        """Test query copying."""
        q1 = GranuleQuery().short_name("ATL03")
        q2 = q1.copy()
        q2.short_name("ATL06")
        assert q1.to_cmr()["short_name"] == "ATL03"
        assert q2.to_cmr()["short_name"] == "ATL06"

    def test_validation(self):
        """Test query validation."""
        # Valid query
        q = GranuleQuery().short_name("ATL03").bounding_box(-180, -90, 180, 90)
        result = q.validate()
        assert result.is_valid

        # Invalid: spatial without collection filter
        q = GranuleQuery().bounding_box(-180, -90, 180, 90)
        result = q.validate()
        assert not result.is_valid

    def test_repr(self):
        """Test string representation."""
        q = GranuleQuery().short_name("ATL03")
        repr_str = repr(q)
        assert "GranuleQuery" in repr_str
        assert "ATL03" in repr_str

    def test_equality(self):
        """Test query equality."""
        q1 = GranuleQuery().short_name("ATL03")
        q2 = GranuleQuery().short_name("ATL03")
        q3 = GranuleQuery().short_name("ATL06")
        assert q1 == q2
        assert q1 != q3


class TestCollectionQuery:
    """Tests for CollectionQuery class."""

    def test_keyword(self):
        """Test keyword search."""
        q = CollectionQuery().keyword("sea surface temperature")
        assert q.to_cmr()["keyword"] == "sea surface temperature"

    def test_cloud_hosted(self):
        """Test cloud_hosted filter."""
        q = CollectionQuery().cloud_hosted(True)
        assert q.to_cmr()["cloud_hosted"] is True

    def test_has_granules(self):
        """Test has_granules filter."""
        q = CollectionQuery().has_granules(True)
        assert q.to_cmr()["has_granules"] is True

    def test_doi(self):
        """Test DOI filter."""
        q = CollectionQuery().doi("10.5067/AQR50-3Q7CS")
        assert q.to_cmr()["doi"] == "10.5067/AQR50-3Q7CS"

    def test_project(self):
        """Test project filter."""
        q = CollectionQuery().project("EMIT")
        assert q.to_cmr()["project"] == "EMIT"

    def test_instrument(self):
        """Test instrument filter."""
        q = CollectionQuery().instrument("GEDI")
        assert q.to_cmr()["instrument"] == "GEDI"

    def test_to_stac_keyword(self):
        """Test STAC conversion maps keyword to q."""
        q = CollectionQuery().keyword("temperature")
        stac = q.to_stac()
        assert stac["q"] == "temperature"

    def test_method_chaining(self):
        """Test that methods can be chained."""
        q = (
            CollectionQuery()
            .keyword("temperature")
            .cloud_hosted(True)
            .has_granules(True)
        )
        cmr = q.to_cmr()
        assert cmr["keyword"] == "temperature"
        assert cmr["cloud_hosted"] is True
        assert cmr["has_granules"] is True


class TestQueryInteroperability:
    """Test interoperability between query types."""

    def test_granule_query_with_all_params(self):
        """Test GranuleQuery with many parameters."""
        q = (
            GranuleQuery()
            .short_name("ATL03")
            .version("006")
            .provider("NSIDC_CPRD")
            .temporal("2020-01", "2020-02")
            .bounding_box(-180, -90, 180, 90)
            .cloud_cover(0, 50)
            .day_night_flag("day")
        )

        cmr = q.to_cmr()
        assert "short_name" in cmr
        assert "version" in cmr
        assert "provider" in cmr
        assert "temporal" in cmr
        assert "bounding_box" in cmr
        assert "cloud_cover" in cmr
        assert "day_night_flag" in cmr

        stac = q.to_stac()
        assert "collections" in stac
        assert "datetime" in stac
        assert "bbox" in stac
        assert "query" in stac
        assert "eo:cloud_cover" in stac["query"]

    def test_collection_query_with_all_params(self):
        """Test CollectionQuery with many parameters."""
        q = (
            CollectionQuery()
            .keyword("temperature")
            .short_name("SST")
            .cloud_hosted(True)
            .has_granules(True)
            .temporal("2020-01", "2020-12")
            .bounding_box(-180, -90, 180, 90)
        )

        cmr = q.to_cmr()
        assert "keyword" in cmr
        assert "short_name" in cmr
        assert "cloud_hosted" in cmr
        assert "has_granules" in cmr
        assert "temporal" in cmr
        assert "bounding_box" in cmr
