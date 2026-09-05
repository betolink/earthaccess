"""Tests for DataGranule.to_geopandas() and DataCollection.to_geopandas()."""

import pytest
from earthaccess.search.results import DataCollection, DataGranule

pytest.importorskip("geopandas")

POINT_GEOMETRY = {"Points": [{"Longitude": 112.19, "Latitude": 42.54}]}

BBOX_GEOMETRY = {
    "BoundingRectangles": [
        {
            "WestBoundingCoordinate": -98,
            "SouthBoundingCoordinate": 19,
            "EastBoundingCoordinate": -82,
            "NorthBoundingCoordinate": 31,
        }
    ]
}


def _granule(geometry, *, granule_ur="granule"):
    return DataGranule(
        {
            "meta": {"concept-id": "G123-TEST", "provider-id": "TEST"},
            "umm": {
                "GranuleUR": granule_ur,
                "CollectionReference": {"ShortName": "TEST", "Version": "1"},
                "TemporalExtent": {
                    "RangeDateTime": {
                        "BeginningDateTime": "2025-01-01T00:00:00Z",
                        "EndingDateTime": "2025-01-01T01:00:00Z",
                    }
                },
                "SpatialExtent": {"HorizontalSpatialDomain": {"Geometry": geometry}},
            },
        }
    )


def test_granule_to_geopandas_flattens_umm_by_default():
    gdf = _granule(POINT_GEOMETRY).to_geopandas()

    assert len(gdf) == 1
    # Full UMM record flattened into dotted columns.
    assert "umm.GranuleUR" in gdf.columns
    assert "umm.CollectionReference.ShortName" in gdf.columns
    assert "umm.TemporalExtent.RangeDateTime.BeginningDateTime" in gdf.columns
    # Basic meta block always present.
    assert gdf["meta.concept-id"].iloc[0] == "G123-TEST"
    assert gdf["meta.provider-id"].iloc[0] == "TEST"
    # Geometry is the active column.
    assert gdf.geometry.iloc[0].geom_type == "MultiPoint"
    assert gdf.crs.to_epsg() == 4326


def test_granule_to_geopandas_fields_subset():
    gdf = _granule(POINT_GEOMETRY).to_geopandas(fields=["GranuleUR"])

    assert "umm.GranuleUR" in gdf.columns
    assert "umm.TemporalExtent.RangeDateTime.BeginningDateTime" not in gdf.columns
    assert (
        "umm.SpatialExtent.HorizontalSpatialDomain.Geometry.Points" not in gdf.columns
    )
    assert "meta.concept-id" in gdf.columns  # meta is always kept
    assert len(gdf) == 1


def test_collection_to_geopandas():
    collection = DataCollection(
        {
            "meta": {"concept-id": "C123-TEST", "provider-id": "TEST"},
            "umm": {
                "ShortName": "TEST",
                "Version": "1",
                "SpatialExtent": {
                    "HorizontalSpatialDomain": {"Geometry": BBOX_GEOMETRY}
                },
            },
        }
    )

    gdf = collection.to_geopandas(fields=["ShortName", "Version"])

    assert len(gdf) == 1
    assert gdf["umm.ShortName"].iloc[0] == "TEST"
    assert gdf["umm.Version"].iloc[0] == "1"
    assert gdf.geometry.iloc[0].geom_type == "MultiPolygon"


def test_to_geopandas_missing_spatial_extent_raises():
    granule = DataGranule({"meta": {"concept-id": "G1"}, "umm": {"GranuleUR": "x"}})

    with pytest.raises(ValueError):
        granule.to_geopandas()
