"""Tests for earthaccess.stac.converters module."""

import pytest
from earthaccess.stac.converters import (
    _build_granule_assets,
    _extract_collection_temporal_extent,
    _extract_granule_datetime,
    _extract_granule_geometry,
    _stac_assets_to_related_urls,
    stac_collection_to_data_collection,
    stac_item_to_data_granule,
    umm_collection_to_stac_collection,
    umm_granule_to_stac_item,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def sample_umm_granule():
    """Sample CMR UMM granule dictionary."""
    return {
        "umm": {
            "GranuleUR": "SC:ATL08_005_20231015121830_03521001_002.h5",
            "TemporalExtent": {
                "RangeDateTime": {
                    "BeginningDateTime": "2023-10-15T12:18:30.000Z",
                    "EndingDateTime": "2023-10-15T12:19:45.000Z",
                }
            },
            "SpatialExtent": {
                "HorizontalSpatialDomain": {
                    "Geometry": {
                        "BoundingRectangles": [
                            {
                                "WestBoundingCoordinate": -120.5,
                                "SouthBoundingCoordinate": 35.0,
                                "EastBoundingCoordinate": -118.0,
                                "NorthBoundingCoordinate": 37.5,
                            }
                        ]
                    }
                }
            },
            "RelatedUrls": [
                {
                    "URL": "https://data.example.com/ATL08_005_20231015_http.h5",
                    "Type": "GET DATA",
                    "Description": "Data file",
                    "MimeType": "application/x-hdf5",
                },
                {
                    "URL": "s3://bucket/ATL08_005_20231015_s3.h5",
                    "Type": "GET DATA VIA DIRECT ACCESS",
                },
            ],
            "CloudCover": 15.5,
        },
        "meta": {
            "concept-id": "G1234567890-NSIDC_ECS",
            "native-id": "ATL08_005_20231015",
            "collection-concept-id": "C1234-NSIDC_ECS",
            "provider-id": "NSIDC_ECS",
        },
    }


@pytest.fixture
def sample_umm_collection():
    """Sample CMR UMM collection dictionary."""
    return {
        "umm": {
            "ShortName": "ATL08",
            "Version": "005",
            "Abstract": "This data set contains along-track heights for the ground and canopy.",
            "DOI": {"DOI": "10.5067/ATLAS/ATL08.005"},
            "TemporalExtents": [
                {
                    "RangeDateTimes": [
                        {
                            "BeginningDateTime": "2018-10-14T00:00:00.000Z",
                            "EndingDateTime": None,
                        }
                    ]
                }
            ],
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
            "RelatedUrls": [
                {
                    "URL": "https://nsidc.org/data/atl08",
                    "Type": "DATA SET LANDING PAGE",
                }
            ],
            "DataCenters": [
                {
                    "ShortName": "NSIDC",
                    "LongName": "National Snow and Ice Data Center",
                    "Roles": ["ARCHIVER", "DISTRIBUTOR"],
                    "ContactInformation": {
                        "RelatedUrls": [{"URL": "https://nsidc.org"}]
                    },
                }
            ],
            "ScienceKeywords": [
                {
                    "Category": "EARTH SCIENCE",
                    "Topic": "LAND SURFACE",
                    "Term": "TOPOGRAPHY",
                }
            ],
        },
        "meta": {
            "concept-id": "C1234-NSIDC_ECS",
            "native-id": "ATL08_V005",
            "provider-id": "NSIDC_ECS",
            "granule-count": 1000000,
        },
    }


@pytest.fixture
def sample_stac_item():
    """Sample STAC Item dictionary."""
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "LC08_L2SP_042034_20231015_02_T1",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-122.5, 37.0],
                    [-120.5, 37.0],
                    [-120.5, 39.0],
                    [-122.5, 39.0],
                    [-122.5, 37.0],
                ]
            ],
        },
        "bbox": [-122.5, 37.0, -120.5, 39.0],
        "properties": {
            "datetime": None,
            "start_datetime": "2023-10-15T18:30:00Z",
            "end_datetime": "2023-10-15T18:30:30Z",
            "eo:cloud_cover": 5.2,
            "cmr:concept_id": "G9876543-LP_DAAC",
            "cmr:provider_id": "LP_DAAC",
        },
        "links": [],
        "assets": {
            "B4": {
                "href": "https://data.example.com/LC08_B4.tif",
                "roles": ["data"],
                "type": "image/tiff",
                "title": "Band 4 - Red",
            },
            "B5": {
                "href": "s3://bucket/LC08_B5.tif",
                "roles": ["data"],
                "type": "image/tiff",
            },
            "thumbnail": {
                "href": "https://data.example.com/LC08_thumb.png",
                "roles": ["thumbnail"],
                "type": "image/png",
            },
        },
        "collection": "landsat-c2-l2",
    }


@pytest.fixture
def sample_stac_collection():
    """Sample STAC Collection dictionary."""
    return {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "landsat-c2-l2",
        "title": "Landsat Collection 2 Level-2",
        "description": "Atmospherically corrected global Landsat data.",
        "license": "proprietary",
        "keywords": ["Landsat", "USGS", "satellite"],
        "providers": [
            {
                "name": "USGS",
                "description": "United States Geological Survey",
                "roles": ["producer", "host"],
                "url": "https://www.usgs.gov",
            }
        ],
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["1982-08-22T00:00:00Z", None]]},
        },
        "links": [
            {"rel": "self", "href": "https://example.com/collections/landsat"},
            {"rel": "about", "href": "https://www.usgs.gov/landsat"},
        ],
        "sci:doi": "10.5066/P9C7I3R3",
        "cmr:concept_id": "C1234-LP_DAAC",
        "cmr:short_name": "LANDSAT_C2_L2",
        "cmr:version": "2",
        "cmr:provider_id": "LP_DAAC",
    }


# =============================================================================
# UMM to STAC Conversion Tests
# =============================================================================


class TestUmmGranuleToStacItem:
    """Tests for umm_granule_to_stac_item function."""

    def test_basic_conversion(self, sample_umm_granule):
        """Test basic UMM granule to STAC Item conversion."""
        item = umm_granule_to_stac_item(sample_umm_granule)

        assert item["type"] == "Feature"
        assert item["stac_version"] == "1.0.0"
        assert item["id"] == "ATL08_005_20231015"

    def test_temporal_extraction(self, sample_umm_granule):
        """Test temporal information is correctly extracted."""
        item = umm_granule_to_stac_item(sample_umm_granule)
        props = item["properties"]

        assert props["start_datetime"] == "2023-10-15T12:18:30.000Z"
        assert props["end_datetime"] == "2023-10-15T12:19:45.000Z"

    def test_spatial_extraction(self, sample_umm_granule):
        """Test spatial information is correctly extracted."""
        item = umm_granule_to_stac_item(sample_umm_granule)

        assert item["bbox"] == [-120.5, 35.0, -118.0, 37.5]
        assert item["geometry"]["type"] == "Polygon"

    def test_assets_created(self, sample_umm_granule):
        """Test assets are created from RelatedUrls."""
        item = umm_granule_to_stac_item(sample_umm_granule)

        assert len(item["assets"]) == 2
        # Check that data assets have the correct roles
        for key, asset in item["assets"].items():
            assert "href" in asset
            assert "roles" in asset

    def test_cloud_cover_extension(self, sample_umm_granule):
        """Test cloud cover triggers EO extension."""
        item = umm_granule_to_stac_item(sample_umm_granule)

        assert item["properties"]["eo:cloud_cover"] == 15.5
        assert any("eo" in ext for ext in item["stac_extensions"])

    def test_cmr_properties(self, sample_umm_granule):
        """Test CMR-specific properties are included."""
        item = umm_granule_to_stac_item(sample_umm_granule)
        props = item["properties"]

        assert props["cmr:concept_id"] == "G1234567890-NSIDC_ECS"
        assert props["cmr:collection_concept_id"] == "C1234-NSIDC_ECS"
        assert props["cmr:provider_id"] == "NSIDC_ECS"

    def test_links_include_self_and_collection(self, sample_umm_granule):
        """Test links include self and collection references."""
        item = umm_granule_to_stac_item(sample_umm_granule)

        rels = [link["rel"] for link in item["links"]]
        assert "self" in rels
        assert "collection" in rels
        assert "root" in rels

    def test_custom_collection_id(self, sample_umm_granule):
        """Test custom collection ID can be provided."""
        item = umm_granule_to_stac_item(
            sample_umm_granule, collection_id="C9999-CUSTOM"
        )

        assert item["collection"] == "C9999-CUSTOM"


class TestUmmCollectionToStacCollection:
    """Tests for umm_collection_to_stac_collection function."""

    def test_basic_conversion(self, sample_umm_collection):
        """Test basic UMM collection to STAC Collection conversion."""
        coll = umm_collection_to_stac_collection(sample_umm_collection)

        assert coll["type"] == "Collection"
        assert coll["stac_version"] == "1.0.0"
        assert coll["id"] == "C1234-NSIDC_ECS"
        assert coll["title"] == "ATL08"

    def test_description(self, sample_umm_collection):
        """Test description is extracted from Abstract."""
        coll = umm_collection_to_stac_collection(sample_umm_collection)

        assert "along-track heights" in coll["description"]

    def test_temporal_extent(self, sample_umm_collection):
        """Test temporal extent is correctly extracted."""
        coll = umm_collection_to_stac_collection(sample_umm_collection)

        interval = coll["extent"]["temporal"]["interval"][0]
        assert interval[0] == "2018-10-14T00:00:00.000Z"
        assert interval[1] is None  # Ongoing collection

    def test_spatial_extent(self, sample_umm_collection):
        """Test spatial extent is correctly extracted."""
        coll = umm_collection_to_stac_collection(sample_umm_collection)

        bbox = coll["extent"]["spatial"]["bbox"][0]
        assert bbox == [-180, -90, 180, 90]

    def test_providers(self, sample_umm_collection):
        """Test providers are created from DataCenters."""
        coll = umm_collection_to_stac_collection(sample_umm_collection)

        assert len(coll["providers"]) == 1
        provider = coll["providers"][0]
        assert provider["name"] == "NSIDC"
        assert "host" in provider["roles"]

    def test_doi_extension(self, sample_umm_collection):
        """Test DOI triggers scientific extension."""
        coll = umm_collection_to_stac_collection(sample_umm_collection)

        assert coll["sci:doi"] == "10.5067/ATLAS/ATL08.005"
        assert any("scientific" in ext for ext in coll["stac_extensions"])

    def test_keywords_extracted(self, sample_umm_collection):
        """Test keywords are extracted from ScienceKeywords."""
        coll = umm_collection_to_stac_collection(sample_umm_collection)

        assert "EARTH SCIENCE" in coll["keywords"]
        assert "TOPOGRAPHY" in coll["keywords"]

    def test_cmr_properties(self, sample_umm_collection):
        """Test CMR-specific properties are included."""
        coll = umm_collection_to_stac_collection(sample_umm_collection)

        assert coll["cmr:concept_id"] == "C1234-NSIDC_ECS"
        assert coll["cmr:short_name"] == "ATL08"
        assert coll["cmr:version"] == "005"


# =============================================================================
# STAC to UMM Conversion Tests
# =============================================================================


class TestStacItemToDataGranule:
    """Tests for stac_item_to_data_granule function."""

    def test_basic_conversion(self, sample_stac_item):
        """Test STAC Item to DataGranule conversion."""
        granule = stac_item_to_data_granule(sample_stac_item)

        assert granule["meta"]["native-id"] == "LC08_L2SP_042034_20231015_02_T1"
        assert granule["umm"]["GranuleUR"] == "LC08_L2SP_042034_20231015_02_T1"

    def test_temporal_extraction(self, sample_stac_item):
        """Test temporal information is correctly extracted."""
        granule = stac_item_to_data_granule(sample_stac_item)
        temporal = granule["umm"]["TemporalExtent"]["RangeDateTime"]

        assert temporal["BeginningDateTime"] == "2023-10-15T18:30:00Z"
        assert temporal["EndingDateTime"] == "2023-10-15T18:30:30Z"

    def test_cloud_cover(self, sample_stac_item):
        """Test cloud cover is extracted."""
        granule = stac_item_to_data_granule(sample_stac_item)

        assert granule["umm"]["CloudCover"] == 5.2

    def test_cloud_hosted_flag(self, sample_stac_item):
        """Test cloud_hosted flag is set."""
        granule = stac_item_to_data_granule(sample_stac_item, cloud_hosted=True)

        assert granule.cloud_hosted is True

    def test_related_urls_from_assets(self, sample_stac_item):
        """Test RelatedUrls are created from STAC assets."""
        granule = stac_item_to_data_granule(sample_stac_item)
        urls = granule["umm"]["RelatedUrls"]

        assert len(urls) == 3
        # Check S3 URLs get direct access type
        s3_url = next((u for u in urls if "s3://" in u["URL"]), None)
        assert s3_url is not None
        assert s3_url["Type"] == "GET DATA VIA DIRECT ACCESS"

    def test_cmr_properties_preserved(self, sample_stac_item):
        """Test CMR properties from STAC are preserved."""
        granule = stac_item_to_data_granule(sample_stac_item)

        assert granule["meta"]["concept-id"] == "G9876543-LP_DAAC"
        assert granule["meta"]["provider-id"] == "LP_DAAC"


class TestStacCollectionToDataCollection:
    """Tests for stac_collection_to_data_collection function."""

    def test_basic_conversion(self, sample_stac_collection):
        """Test STAC Collection to DataCollection conversion."""
        collection = stac_collection_to_data_collection(sample_stac_collection)

        assert collection["umm"]["ShortName"] == "Landsat Collection 2 Level-2"
        assert collection["meta"]["native-id"] == "landsat-c2-l2"

    def test_description(self, sample_stac_collection):
        """Test description is preserved."""
        collection = stac_collection_to_data_collection(sample_stac_collection)

        assert "Atmospherically corrected" in collection["umm"]["Abstract"]

    def test_temporal_extent(self, sample_stac_collection):
        """Test temporal extent is correctly extracted."""
        collection = stac_collection_to_data_collection(sample_stac_collection)
        temporal = collection["umm"]["TemporalExtents"][0]["RangeDateTimes"][0]

        assert temporal["BeginningDateTime"] == "1982-08-22T00:00:00Z"
        assert temporal["EndingDateTime"] is None

    def test_spatial_extent(self, sample_stac_collection):
        """Test spatial extent is correctly extracted."""
        collection = stac_collection_to_data_collection(sample_stac_collection)
        bbox = collection["umm"]["SpatialExtent"]["HorizontalSpatialDomain"][
            "Geometry"
        ]["BoundingRectangles"][0]

        assert bbox["WestBoundingCoordinate"] == -180
        assert bbox["EastBoundingCoordinate"] == 180

    def test_data_centers_from_providers(self, sample_stac_collection):
        """Test DataCenters are created from STAC providers."""
        collection = stac_collection_to_data_collection(sample_stac_collection)
        centers = collection["umm"]["DataCenters"]

        assert len(centers) == 1
        assert centers[0]["ShortName"] == "USGS"
        assert "ORIGINATOR" in centers[0]["Roles"]  # producer -> ORIGINATOR

    def test_cmr_properties_preserved(self, sample_stac_collection):
        """Test CMR properties from STAC are preserved."""
        collection = stac_collection_to_data_collection(sample_stac_collection)

        assert collection["meta"]["concept-id"] == "C1234-LP_DAAC"
        assert collection["meta"]["provider-id"] == "LP_DAAC"

    def test_cloud_hosted_flag(self, sample_stac_collection):
        """Test cloud_hosted flag is set."""
        collection = stac_collection_to_data_collection(
            sample_stac_collection, cloud_hosted=True
        )

        assert collection.cloud_hosted is True


# =============================================================================
# Helper Function Tests
# =============================================================================


class TestExtractGranuleDatetime:
    """Tests for _extract_granule_datetime helper."""

    @pytest.mark.parametrize(
        "temporal,expected_dt,expected_start,expected_end",
        [
            pytest.param(
                {
                    "RangeDateTime": {
                        "BeginningDateTime": "2023-01-01T00:00:00Z",
                        "EndingDateTime": "2023-01-01T01:00:00Z",
                    }
                },
                None,
                "2023-01-01T00:00:00Z",
                "2023-01-01T01:00:00Z",
                id="range_datetime",
            ),
            pytest.param(
                {"SingleDateTime": "2023-01-01T00:00:00Z"},
                "2023-01-01T00:00:00Z",
                None,
                None,
                id="single_datetime",
            ),
            pytest.param({}, None, None, None, id="empty_temporal"),
        ],
    )
    def test_datetime_extraction(
        self, temporal, expected_dt, expected_start, expected_end
    ):
        """Test datetime extraction from various temporal extent formats."""
        dt, start, end = _extract_granule_datetime(temporal)

        assert dt == expected_dt
        assert start == expected_start
        assert end == expected_end


class TestExtractGranuleGeometry:
    """Tests for _extract_granule_geometry helper."""

    def test_bounding_rectangle(self):
        """Test extraction from BoundingRectangles."""
        spatial = {
            "HorizontalSpatialDomain": {
                "Geometry": {
                    "BoundingRectangles": [
                        {
                            "WestBoundingCoordinate": -120,
                            "SouthBoundingCoordinate": 35,
                            "EastBoundingCoordinate": -118,
                            "NorthBoundingCoordinate": 37,
                        }
                    ]
                }
            }
        }
        geometry, bbox = _extract_granule_geometry(spatial)

        assert bbox == [-120, 35, -118, 37]
        assert geometry["type"] == "Polygon"

    def test_gpolygon(self):
        """Test extraction from GPolygons."""
        spatial = {
            "HorizontalSpatialDomain": {
                "Geometry": {
                    "GPolygons": [
                        {
                            "Boundary": {
                                "Points": [
                                    {"Longitude": -120, "Latitude": 35},
                                    {"Longitude": -118, "Latitude": 35},
                                    {"Longitude": -118, "Latitude": 37},
                                    {"Longitude": -120, "Latitude": 37},
                                    {"Longitude": -120, "Latitude": 35},
                                ]
                            }
                        }
                    ]
                }
            }
        }
        geometry, bbox = _extract_granule_geometry(spatial)

        assert geometry["type"] == "Polygon"
        assert len(geometry["coordinates"][0]) == 5
        assert bbox == [-120, 35, -118, 37]

    def test_empty_spatial(self):
        """Test handling of empty spatial extent."""
        geometry, bbox = _extract_granule_geometry({})

        assert geometry is None
        assert bbox is None


class TestBuildGranuleAssets:
    """Tests for _build_granule_assets helper."""

    def test_data_urls(self):
        """Test data URL conversion to assets."""
        related_urls = [{"URL": "https://data.example.com/file.h5", "Type": "GET DATA"}]
        assets = _build_granule_assets(related_urls)

        assert "file.h5" in assets
        assert assets["file.h5"]["roles"] == ["data"]

    def test_multiple_url_types(self):
        """Test different URL types get correct roles."""
        related_urls = [
            {"URL": "https://example.com/data.nc", "Type": "GET DATA"},
            {
                "URL": "https://example.com/preview.png",
                "Type": "GET RELATED VISUALIZATION",
            },
            {
                "URL": "https://example.com/metadata.xml",
                "Type": "VIEW RELATED INFORMATION",
            },
        ]
        assets = _build_granule_assets(related_urls)

        assert len(assets) == 3


class TestExtractCollectionTemporalExtent:
    """Tests for _extract_collection_temporal_extent helper."""

    @pytest.mark.parametrize(
        "temporal_extents,expected_start,expected_end",
        [
            pytest.param(
                [
                    {
                        "RangeDateTimes": [
                            {
                                "BeginningDateTime": "2020-01-01T00:00:00Z",
                                "EndingDateTime": "2023-12-31T23:59:59Z",
                            }
                        ]
                    }
                ],
                "2020-01-01T00:00:00Z",
                "2023-12-31T23:59:59Z",
                id="range_date_times",
            ),
            pytest.param([], None, None, id="empty_extents"),
        ],
    )
    def test_temporal_extent_extraction(
        self, temporal_extents, expected_start, expected_end
    ):
        """Test temporal extent extraction from various formats."""
        start, end = _extract_collection_temporal_extent(temporal_extents)

        assert start == expected_start
        assert end == expected_end


class TestStacAssetsToRelatedUrls:
    """Tests for _stac_assets_to_related_urls helper."""

    @pytest.mark.parametrize(
        "assets,expected_type",
        [
            pytest.param(
                {"data": {"href": "https://example.com/data.nc", "roles": ["data"]}},
                "GET DATA",
                id="data_asset",
            ),
            pytest.param(
                {"data": {"href": "s3://bucket/data.nc", "roles": ["data"]}},
                "GET DATA VIA DIRECT ACCESS",
                id="s3_asset",
            ),
            pytest.param(
                {
                    "thumbnail": {
                        "href": "https://example.com/thumb.png",
                        "roles": ["thumbnail"],
                    }
                },
                "GET RELATED VISUALIZATION",
                id="thumbnail_asset",
            ),
        ],
    )
    def test_asset_to_related_url_conversion(self, assets, expected_type):
        """Test STAC assets are converted to RelatedUrls with correct types."""
        urls = _stac_assets_to_related_urls(assets)

        assert len(urls) == 1
        assert urls[0]["Type"] == expected_type


# =============================================================================
# Round-trip Tests
# =============================================================================


class TestRoundTrip:
    """Tests for round-trip conversion (UMM -> STAC -> UMM)."""

    def test_granule_roundtrip(self, sample_umm_granule):
        """Test granule can be converted to STAC and back."""
        # UMM -> STAC
        stac_item = umm_granule_to_stac_item(sample_umm_granule)

        # STAC -> UMM (DataGranule)
        granule = stac_item_to_data_granule(stac_item)

        # Verify key properties are preserved
        # Note: native-id becomes id in STAC, so GranuleUR may differ
        assert granule["meta"]["native-id"] == sample_umm_granule["meta"]["native-id"]
        # Collection reference is preserved
        assert (
            granule["meta"]["collection-concept-id"]
            == sample_umm_granule["meta"]["collection-concept-id"]
        )

    def test_collection_roundtrip(self, sample_umm_collection):
        """Test collection can be converted to STAC and back."""
        # UMM -> STAC
        stac_coll = umm_collection_to_stac_collection(sample_umm_collection)

        # STAC -> UMM (DataCollection)
        collection = stac_collection_to_data_collection(stac_coll)

        # Verify key properties are preserved
        # Note: ShortName in UMM becomes title in STAC
        assert (
            collection["meta"]["concept-id"]
            == sample_umm_collection["meta"]["concept-id"]
        )
