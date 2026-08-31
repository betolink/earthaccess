"""Tests for search persistence: save(), load(), fingerprinting, and verification.

These tests exercise the save/load round-trip entirely offline by building
SearchResults from static fixtures and mocking the CMR search functions used
by verification.
"""

import gzip
import json
from unittest.mock import MagicMock, patch

import earthaccess
import pytest
from earthaccess.search import (
    DataCollection,
    DataGranule,
    DataGranules,
    GranuleResults,
    SearchResults,
    load_search,
    save_search,
)
from earthaccess.search.persistence import (
    _serialize_query_params,
    compute_fingerprint,
)

from tests.unit.fixtures import load_granule_fixture


def make_granule(fixture_name="HLSS30_umm", concept_id="G1-TEST"):
    """Build a DataGranule from a fixture with an overridable concept id."""
    data = load_granule_fixture(fixture_name)
    data["meta"]["concept-id"] = concept_id
    return DataGranule(data, cloud_hosted=True)


def make_results(concept_ids, query=None, limit=10, hits=12070):
    """Build a populated GranuleResults (offline) for saving."""
    results = GranuleResults.__new__(GranuleResults)
    SearchResults.__init__(results, query=query, limit=limit, prefetch=0)
    results._cached_results = [make_granule(concept_id=cid) for cid in concept_ids]
    results._exhausted = True
    results._total_hits = hits
    return results


def make_query(**kwargs):
    return DataGranules().parameters(**kwargs)


# =============================================================================
# Fingerprint
# =============================================================================


def test_fingerprint_is_stable_regardless_of_order():
    """The fingerprint only depends on the set of concept-IDs, not their order."""
    ids_a = ["G1", "G2", "G3"]
    ids_b = ["G3", "G1", "G2"]

    fp_a = compute_fingerprint(make_results(ids_a))
    fp_b = compute_fingerprint(make_results(ids_b))

    assert fp_a == fp_b
    assert fp_a.startswith("sha256:")


def test_fingerprint_changes_when_results_change():
    """Different concept-ID sets produce different fingerprints."""
    fp_1 = compute_fingerprint(make_results(["G1", "G2"]))
    fp_2 = compute_fingerprint(make_results(["G1", "G2", "G3"]))

    assert fp_1 != fp_2


# =============================================================================
# Query serialization
# =============================================================================


def test_serialize_query_params_handles_temporal():
    """Temporal ranges are serialized as (start, end) tuples for replay."""
    results = make_results(
        ["G1"],
        query=make_query(short_name="ATL06", temporal=("2024-01-01", "2024-12-31")),
    )
    params = _serialize_query_params(results)

    assert params["short_name"] == "ATL06"
    assert params["temporal"] == (
        "2024-01-01T00:00:00Z",
        "2024-12-31T23:59:59Z",
    )


def test_serialize_query_params_no_temporal():
    """Queries without temporal round-trip unchanged."""
    results = make_results(["G1"], query=make_query(short_name="ATL06"))
    params = _serialize_query_params(results)

    assert params == {"short_name": "ATL06"}


# =============================================================================
# save() / load() round-trip
# =============================================================================


def test_save_and_load_roundtrip(tmp_path):
    """Saved results reload with the same granules, hits, and fingerprint."""
    results = make_results(
        ["G1", "G2"], query=make_query(short_name="HLSS30"), limit=10, hits=12070
    )
    path = results.save(tmp_path / "search.gz")

    assert path.exists()

    loaded = SearchResults.load(path, verify=False)

    assert len(loaded) == 2
    assert [g["meta"]["concept-id"] for g in loaded] == ["G1", "G2"]
    assert loaded.total() == 12070
    assert loaded._stored_fingerprint == compute_fingerprint(results)
    assert loaded.verification is None


def test_save_raises_without_loaded_results(tmp_path):
    """Saving requires at least one loaded result."""
    results = make_results([], query=make_query(short_name="HLSS30"))
    with pytest.raises(ValueError, match="No results are loaded"):
        results.save(tmp_path / "empty.gz")


def test_save_payload_is_compressed_and_valid(tmp_path):
    """The payload is gzipped JSON with the expected metadata."""
    results = make_results(["G1"], query=make_query(short_name="HLSS30"))
    path = results.save(tmp_path / "search.gz")

    with gzip.open(path, "rt") as f:
        payload = json.load(f)

    assert payload["format"] == "earthaccess-search-v1"
    assert payload["kind"] == "granules"
    assert payload["limit"] == 10
    assert payload["cmr_hits"] == 12070
    assert payload["fingerprint"].startswith("sha256:")
    assert len(payload["results"]) == 1
    assert payload["results"][0]["meta"]["concept-id"] == "G1"


def test_load_rejects_unknown_format(tmp_path):
    """Loading a payload with an unknown format raises ValueError."""
    path = tmp_path / "bad.gz"
    with gzip.open(path, "wt") as f:
        json.dump({"format": "other-v1"}, f)

    with pytest.raises(ValueError, match="Unrecognized search payload format"):
        SearchResults.load(path, verify=False)


def test_module_level_functions_roundtrip(tmp_path):
    """save_search()/load_search() mirror the methods."""
    results = make_results(["G1"], query=make_query(short_name="HLSS30"))
    path = save_search(results, tmp_path / "module.gz")

    loaded = load_search(path, verify=False)

    assert len(loaded) == 1
    assert loaded._stored_fingerprint == compute_fingerprint(results)


# =============================================================================
# Verification
# =============================================================================


def _fake_fresh_results(concept_ids, hits):
    """Build a fake fresh SearchResults to be returned by mocked search_data."""
    fresh = make_results(concept_ids, query=make_query(short_name="HLSS30"), hits=hits)
    return fresh


def test_load_verify_unchanged(tmp_path):
    """verify=True reports unchanged when the search is identical."""
    results = make_results(["G1", "G2"], query=make_query(short_name="HLSS30"))
    path = results.save(tmp_path / "search.gz")

    with patch.object(
        earthaccess,
        "search_data",
        return_value=_fake_fresh_results(["G2", "G1"], 12070),
    ) as mock_search:
        loaded = SearchResults.load(path, verify=True)

    mock_search.assert_called_once()
    report = loaded.verification
    assert report is not None
    assert report is not None
    assert report["unchanged"] is True
    assert report["fingerprint_match"] is True
    assert report["cmr_hits_match"] is True
    assert report["added"] == []
    assert report["removed"] == []


def test_load_verify_detects_removed_granule(tmp_path):
    """A granule that disappears is reported as removed."""
    results = make_results(["G1", "G2"], query=make_query(short_name="HLSS30"))
    path = results.save(tmp_path / "search.gz")

    with patch.object(
        earthaccess,
        "search_data",
        return_value=_fake_fresh_results(["G1"], 11999),
    ):
        loaded = SearchResults.load(path, verify=True)

    report = loaded.verification
    assert report is not None
    assert report["unchanged"] is False
    assert report["fingerprint_match"] is False
    assert report["removed"] == ["G2"]
    assert report["added"] == []


def test_load_verify_detects_added_granule(tmp_path):
    """A granule that appears is reported as added."""
    results = make_results(["G1"], query=make_query(short_name="HLSS30"))
    path = results.save(tmp_path / "search.gz")

    with patch.object(
        earthaccess,
        "search_data",
        return_value=_fake_fresh_results(["G1", "G2"], 12071),
    ):
        loaded = SearchResults.load(path, verify=True)

    report = loaded.verification
    assert report is not None
    assert report["unchanged"] is False
    assert report["added"] == ["G2"]
    assert report["removed"] == []


def test_load_verify_detects_hit_count_change(tmp_path):
    """A change in cmr-hits alone is reported even with matching granules."""
    results = make_results(["G1"], query=make_query(short_name="HLSS30"), hits=12070)
    path = results.save(tmp_path / "search.gz")

    with patch.object(
        earthaccess,
        "search_data",
        return_value=_fake_fresh_results(["G1"], 13000),
    ):
        loaded = SearchResults.load(path, verify=True)

    report = loaded.verification
    assert report is not None
    assert report["unchanged"] is False
    assert report["fingerprint_match"] is True
    assert report["cmr_hits_match"] is False
    assert report["saved_cmr_hits"] == 12070
    assert report["current_cmr_hits"] == 13000


def test_load_verify_replays_saved_query(tmp_path):
    """Verification re-runs the search with the saved query parameters."""
    results = make_results(["G1"], query=make_query(short_name="HLSS30"))
    path = results.save(tmp_path / "search.gz")

    with patch.object(earthaccess, "search_data", return_value=MagicMock()) as m:
        m.return_value._cached_results = [make_granule(concept_id="G1")]
        m.return_value._total_hits = 12070
        m.return_value.limit = None
        SearchResults.load(path, verify=True)

    # search_data was called with the saved params
    call_kwargs = m.call_args.kwargs
    assert call_kwargs["short_name"] == "HLSS30"


def test_load_verify_offline_has_no_report(tmp_path):
    """verify=False does not touch the network and leaves verification=None."""
    results = make_results(["G1"], query=make_query(short_name="HLSS30"))
    path = results.save(tmp_path / "search.gz")

    with patch.object(earthaccess, "search_data", side_effect=AssertionError("no net")):
        loaded = SearchResults.load(path, verify=False)

    assert loaded.verification is None
    assert len(loaded) == 1


def test_load_verify_collections(tmp_path):
    """Collections verify through search_datasets with a collections payload."""
    collection = DataCollection(
        {
            "umm": {"ShortName": "ATL06", "EntryTitle": "test"},
            "meta": {"concept-id": "C1-TEST", "provider-id": "TEST"},
        }
    )
    from earthaccess.search import CollectionResults

    results = CollectionResults.__new__(CollectionResults)
    SearchResults.__init__(
        results, query=make_query(short_name="ATL06"), limit=10, prefetch=0
    )
    results._cached_results = [collection]
    results._exhausted = True
    results._total_hits = 5

    path = results.save(tmp_path / "collections.gz")

    fresh = CollectionResults.__new__(CollectionResults)
    SearchResults.__init__(
        fresh, query=make_query(short_name="ATL06"), limit=10, prefetch=0
    )
    fresh._cached_results = [collection]
    fresh._exhausted = True
    fresh._total_hits = 5

    with patch.object(earthaccess, "search_datasets", return_value=fresh):
        loaded = SearchResults.load(path, verify=True)

    assert loaded.verification is not None
    assert loaded.verification["unchanged"] is True
    assert loaded._cached_results[0]["meta"]["concept-id"] == "C1-TEST"
