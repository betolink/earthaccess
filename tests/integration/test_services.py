"""Integration tests for earthaccess service search."""

import earthaccess
import pytest


@pytest.mark.vcr
def test_services():
    """Test that a list of services can be retrieved."""
    datasets = earthaccess.search_datasets(
        short_name="MUR-JPL-L4-GLOB-v4.1",
        cloud_hosted=True,
        temporal=("2024-02-27T00:00:00Z", "2024-02-29T00:00:00Z"),
    )

    dataset_services = {
        dataset["umm"]["ShortName"]: dataset.services() for dataset in datasets
    }

    assert list(dataset_services.keys())[0] == "MUR-JPL-L4-GLOB-v4.1"
    assert (
        dataset_services["MUR-JPL-L4-GLOB-v4.1"]["S2606110201-XYZ_PROV"][0]["umm"][
            "LongName"
        ]
        == "Harmony GDAL Adapter (HGA)"
    )
    assert (
        dataset_services["MUR-JPL-L4-GLOB-v4.1"]["S2839491596-XYZ_PROV"][0]["umm"][
            "URL"
        ]["Description"]
        == "https://harmony.earthdata.nasa.gov"
    )
