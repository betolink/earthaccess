"""Tests for earthaccess service search and results."""

import earthaccess
import pytest
from earthaccess.api import search_datasets


@pytest.mark.vcr
def test_services():
    """Test DataService get function return of service metadata results."""
    earthaccess._auth.authenticated = False
    actual = earthaccess.search_services(concept_id="S2004184019-POCLOUD")

    assert actual[0]["umm"]["Type"] == "OPeNDAP"
    assert actual[0]["umm"]["ServiceOrganizations"][0]["ShortName"] == "UCAR/UNIDATA"
    assert actual[0]["umm"]["Description"] == "Earthdata OPEnDAP in the cloud"
    assert actual[0]["umm"]["LongName"] == "PO.DAAC OPeNDADP In the Cloud"


@pytest.mark.vcr
def test_service_results():
    """Test results.DataCollection.services to return available services."""
    datasets = search_datasets(
        short_name="MUR-JPL-L4-GLOB-v4.1",
        cloud_hosted=True,
        temporal=("2024-02-27T00:00:00Z", "2024-02-29T00:00:00Z"),
    )
    earthaccess._auth.authenticated = False

    # Convert to list to fetch results
    datasets_list = list(datasets)
    assert len(datasets_list) > 0
    collection = datasets_list[0]
    results = collection.services()  # type: ignore[attr-defined]

    # services() returns a dict keyed by service concept-id; each value is a
    # non-empty list of service records carrying UMM + meta info.
    assert results
    for concept_id, records in results.items():
        assert records
        for record in records:
            assert record["meta"]["concept-id"] == concept_id
            assert record["umm"]["Name"]
            assert record["umm"]["Type"]
