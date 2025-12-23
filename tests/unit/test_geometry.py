"""Tests for Group G: Geometry Handling.

Tests for geometry loading from various sources with
auto-simplification for CMR compliance.
"""

import json
import tempfile
from pathlib import Path

import pytest

try:
    import geopandas as gpd

    HAS_GEOPANDAS = True
except ImportError:
    HAS_GEOPANDAS = False

try:
    from shapely.geometry import Point, Polygon, box

    HAS_SHAPELY = True
except ImportError:
    HAS_SHAPELY = False


class TestLoadGeometry:
    """Test geometry loading from various sources."""

    def test_load_geojson_dict(self):
        """Test loading geometry from GeoJSON dict."""
        from earthaccess.store_components.geometry import load_geometry

        geojson_dict = {
            "type": "Polygon",
            "coordinates": [
                [
                    [-122.0, 37.0],
                    [-121.0, 37.0],
                    [-121.0, 38.0],
                    [-122.0, 38.0],
                    [-122.0, 37.0],
                ]
            ],
        }

        result = load_geometry(geojson_dict)

        assert result["type"] == "Polygon"
        assert len(result["coordinates"][0]) == 5

    def test_load_geojson_file(self):
        """Test loading geometry from GeoJSON file."""
        from earthaccess.store_components.geometry import load_geometry

        geojson_dict = {
            "type": "Polygon",
            "coordinates": [
                [
                    [-122.0, 37.0],
                    [-121.0, 37.0],
                    [-121.0, 38.0],
                    [-122.0, 38.0],
                    [-122.0, 37.0],
                ]
            ],
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".geojson", delete=False
        ) as f:
            json.dump(geojson_dict, f)
            temp_path = Path(f.name)

        try:
            result = load_geometry(temp_path)

            assert result["type"] == "Polygon"
            assert len(result["coordinates"][0]) == 5
        finally:
            temp_path.unlink()

    def test_load_wkt_string(self):
        """Test loading geometry from WKT string."""
        from earthaccess.store_components.geometry import load_geometry

        if not HAS_SHAPELY:
            pytest.skip("shapely not installed")

        wkt = "POLYGON((-122 37, -121 37, -121 38, -122 38, -122 37))"
        result = load_geometry(wkt)

        assert result["type"] == "Polygon"
        assert len(result["coordinates"][0]) == 5

    def test_load_shapely_polygon(self):
        """Test loading geometry from shapely Polygon."""
        from earthaccess.store_components.geometry import load_geometry

        if not HAS_SHAPELY:
            pytest.skip("shapely not installed")

        polygon = Polygon([(-122, 37), (-121, 37), (-121, 38), (-122, 38), (-122, 37)])
        result = load_geometry(polygon)

        assert result["type"] == "Polygon"

    def test_load_shapely_box(self):
        """Test loading geometry from shapely box."""
        from earthaccess.store_components.geometry import load_geometry

        if not HAS_SHAPELY:
            pytest.skip("shapely not installed")

        bbox = box(-122, 37, -121, 38)
        result = load_geometry(bbox)

        assert result["type"] == "Polygon"

    def test_load_shapely_point(self):
        """Test loading geometry from shapely Point."""
        from earthaccess.store_components.geometry import load_geometry

        if not HAS_SHAPELY:
            pytest.skip("shapely not installed")

        point = Point(-122, 37)
        result = load_geometry(point)

        assert result["type"] == "Point"

    @pytest.mark.skipif(not HAS_GEOPANDAS, reason="geopandas not installed")
    def test_load_geodataframe(self):
        """Test loading geometry from GeoDataFrame."""
        from earthaccess.store_components.geometry import load_geometry

        if not HAS_GEOPANDAS:
            pytest.skip("geopandas not installed")

        # Create simple GeoDataFrame
        gdf = gpd.GeoDataFrame(
            {
                "name": ["Region A"],
                "geometry": [
                    Polygon(
                        [(-122, 37), (-121, 37), (-121, 38), (-122, 38), (-122, 37)]
                    )
                ],
            }
        )

        result = load_geometry(gdf)

        assert result["type"] == "Polygon"


class TestGeometrySimplification:
    """Test geometry simplification for CMR compliance."""

    def test_no_simplification_for_small_geometry(self):
        """Test that small geometry is not simplified."""
        from earthaccess.store_components.geometry import load_geometry

        if not HAS_SHAPELY:
            pytest.skip("shapely not installed")

        polygon = Polygon(
            [
                (-122.0, 37.0),
                (-121.0, 37.0),
                (-121.0, 38.0),
                (-122.0, 38.0),
                (-122.0, 37.0),
            ]
        )
        result = load_geometry(polygon, simplify=True, max_points=300)

        assert len(result["coordinates"][0]) == 5

    def test_simplification_for_complex_geometry(self):
        """Test that complex geometry is simplified."""
        from earthaccess.store_components.geometry import load_geometry

        if not HAS_SHAPELY:
            pytest.skip("shapely not installed")

        # Create polygon with 400 points
        points = []
        for i in range(400):
            angle = 2 * 3.14159 * i / 400
            lon = -122 + 1.0 * (1 + 0.5 * (angle / 3.14159)) / 2
            lat = 37.5 + 0.5 * (angle / 3.14159)
            points.append((lon, lat))
        points.append(points[0])  # Close polygon

        complex_polygon = Polygon(points)
        result = load_geometry(complex_polygon, simplify=True, max_points=300)

        assert len(result["coordinates"][0]) <= 300


class TestGeometryValidation:
    """Test geometry validation."""

    def test_valid_geometry_types(self):
        """Test that valid geometry types are accepted."""
        from earthaccess.store_components.geometry import load_geometry

        geojson_dict = {"type": "Point", "coordinates": [-122.0, 37.0]}
        result = load_geometry(geojson_dict)
        assert result["type"] == "Point"

        geojson_dict = {
            "type": "Polygon",
            "coordinates": [
                [
                    [-122.0, 37.0],
                    [-121.0, 37.0],
                    [-121.0, 38.0],
                    [-122.0, 38.0],
                    [-122.0, 37.0],
                ]
            ],
        }
        result = load_geometry(geojson_dict)
        assert result["type"] == "Polygon"

    def test_invalid_geometry_type_raises_error(self):
        """Test that invalid geometry type raises error."""
        from earthaccess.store_components.geometry import load_geometry

        geojson_dict = {"type": "InvalidType", "coordinates": [[-122, 37]]}

        with pytest.raises(ValueError, match="Unsupported geometry type"):
            load_geometry(geojson_dict)

    def test_invalid_file_raises_error(self):
        """Test that invalid file path raises error."""
        from earthaccess.store_components.geometry import load_geometry

        with pytest.raises(ValueError, match="File not found"):
            load_geometry("/nonexistent/path.geojson")


class TestFeatureCollection:
    """Test GeoJSON FeatureCollection handling."""

    def test_feature_collection_single_feature(self):
        """Test that single feature returns its geometry."""
        from earthaccess.store_components.geometry import load_geometry

        feature_collection = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-122.0, 37.0]},
                    "properties": {},
                }
            ],
        }

        result = load_geometry(feature_collection)
        assert result["type"] == "Point"

    def test_feature_returns_geometry(self):
        """Test that Feature returns its geometry."""
        from earthaccess.store_components.geometry import load_geometry

        feature = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-122.0, 37.0]},
            "properties": {},
        }

        result = load_geometry(feature)
        assert result["type"] == "Point"


class TestGeometryInQuery:
    """Test geometry integration with query methods."""

    def test_coordinates_accepts_geojson_dict(self):
        """Test that coordinates() accepts GeoJSON dict."""
        import earthaccess
        from earthaccess.store_components.geometry import load_geometry
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        geojson_dict = {
            "type": "Polygon",
            "coordinates": [
                [
                    [-122.0, 37.0],
                    [-121.0, 37.0],
                    [-121.0, 38.0],
                    [-122.0, 38.0],
                    [-122.0, 37.0],
                ]
            ],
        }

        result = query.coordinates(load_geometry(geojson_dict))
        assert result is query
        assert query._params.get("polygon") is not None

    def test_coordinates_accepts_geometry_file(self):
        """Test that coordinates() accepts geometry from file."""
        import earthaccess
        from earthaccess.store_components.geometry import load_geometry
        from earthaccess.store_components.query import GranuleQuery

        query = GranuleQuery(auth=earthaccess.__auth__)
        geojson_dict = {
            "type": "Polygon",
            "coordinates": [
                [
                    [-122.0, 37.0],
                    [-121.0, 37.0],
                    [-121.0, 38.0],
                    [-122.0, 38.0],
                    [-122.0, 37.0],
                ]
            ],
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".geojson", delete=False
        ) as f:
            json.dump(geojson_dict, f)
            temp_path = Path(f.name)

        try:
            result = query.coordinates(load_geometry(temp_path))
            assert result is query
            assert query._params.get("polygon") is not None
        finally:
            temp_path.unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
