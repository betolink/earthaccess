"""Tests for Group F: Flexible Input Types.

Tests for flexible input handling in query methods accepting
various iterable types for spatial and temporal parameters.
"""

import numpy as np
import pytest


class TestFlexibleInputBBox:
    """Test flexible bounding box input types."""

    def test_bbox_accepts_tuple(self):
        """Test that bounding_box accepts tuple."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        bbox = (-180.0, -90.0, 180.0, 90.0)
        result = query.bounding_box(bbox)

        assert result is query
        # The query stores bbox as a tuple for immutability
        assert query._params.get("bounding_box") == bbox

    def test_bbox_accepts_list(self):
        """Test that bounding_box accepts list."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        bbox = [-180.0, -90.0, 180.0, 90.0]
        result = query.bounding_box(bbox)

        assert result is query
        assert query._params.get("bounding_box") == tuple(bbox)

    def test_bbox_accepts_numpy_array(self):
        """Test that bounding_box accepts numpy array."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        bbox = np.array([-180.0, -90.0, 180.0, 90.0])
        result = query.bounding_box(bbox)

        assert result is query
        expected = tuple(bbox.tolist())
        assert query._params.get("bounding_box") == expected

    def test_bbox_accepts_separate_values(self):
        """Test that bounding_box accepts separate values."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        result = query.bounding_box(-180.0, -90.0, 180.0, 90.0)

        assert result is query
        assert query._params.get("bounding_box") == (-180.0, -90.0, 180.0, 90.0)

    def test_bbox_validates_four_values(self):
        """Test that bounding_box validates 4 values."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)

        with pytest.raises(ValueError, match="must have 4 values"):
            query.bounding_box([-180, -90, 180])


class TestFlexibleInputPoint:
    """Test flexible point input types."""

    def test_point_accepts_tuple(self):
        """Test that point accepts tuple."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        point = (-122.4, 37.8)
        result = query.point(point)

        assert result is query
        assert query._params.get("point") == point

    def test_point_accepts_list(self):
        """Test that point accepts list."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        point = [-122.4, 37.8]
        result = query.point(point)

        assert result is query
        assert query._params.get("point") == tuple(point)

    def test_point_accepts_numpy_array(self):
        """Test that point accepts numpy array."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        point = np.array([-122.4, 37.8])
        result = query.point(point)

        assert result is query
        expected = tuple(point.tolist())
        assert query._params.get("point") == expected

    def test_point_accepts_separate_values(self):
        """Test that point accepts separate values."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        result = query.point(-122.4, 37.8)

        assert result is query
        assert query._params.get("point") == (-122.4, 37.8)

    def test_point_validates_two_values(self):
        """Test that point validates 2 values."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)

        with pytest.raises(ValueError, match="must have 2 values"):
            query.point([-122.4])


class TestFlexibleInputTemporal:
    """Test flexible temporal input types."""

    def test_temporal_accepts_interval_string(self):
        """Test that temporal accepts interval string."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        interval = "2023-01-01T00:00:00Z/2023-12-31T23:59:59Z"
        result = query.temporal(interval)

        assert result is query

    def test_temporal_accepts_tuple(self):
        """Test that temporal accepts tuple."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        interval = ("2023-01-01T00:00:00Z", "2023-12-31T23:59:59Z")
        result = query.temporal(interval)

        assert result is query

    def test_temporal_accepts_list(self):
        """Test that temporal accepts list."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        interval = ["2023-01-01T00:00:00Z", "2023-12-31T23:59:59Z"]
        result = query.temporal(interval)

        assert result is query

    def test_temporal_accepts_separate_values(self):
        """Test that temporal accepts separate values."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        result = query.temporal("2023-01-01T00:00:00Z", "2023-12-31T23:59:59Z")

        assert result is query

    def test_temporal_accepts_single_datetime(self):
        """Test that temporal accepts single datetime."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        result = query.temporal("2023-01-01T00:00:00Z")

        assert result is query


class TestFlexibleInputPolygon:
    """Test flexible polygon input types."""

    def test_polygon_accepts_list_of_tuples(self):
        """Test that polygon accepts list of tuples."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        polygon = [(-122.0, 37.0), (-121.0, 37.0), (-121.0, 38.0), (-122.0, 38.0)]
        result = query.polygon(polygon)

        assert result is query

    def test_polygon_accepts_list_of_lists(self):
        """Test that polygon accepts list of lists."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        polygon = [[-122.0, 37.0], [-121.0, 37.0], [-121.0, 38.0], [-122.0, 38.0]]
        result = query.polygon(polygon)

        assert result is query

    def test_polygon_accepts_numpy_array(self):
        """Test that polygon accepts numpy array."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        polygon = np.array(
            [[-122.0, 37.0], [-121.0, 37.0], [-121.0, 38.0], [-122.0, 38.0]]
        )
        result = query.polygon(polygon)

        assert result is query

    def test_polygon_accepts_generator(self):
        """Test that polygon accepts generator."""
        import earthaccess
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        points = [(-122.0, 37.0), (-121.0, 37.0), (-121.0, 38.0), (-122.0, 38.0)]
        polygon = (p for p in points)  # Generator
        result = query.polygon(polygon)

        assert result is query


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
