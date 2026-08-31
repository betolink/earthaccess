"""Search persistence: save, load, and verify reproducible searches.

This module lets you save a ``SearchResults`` (granules or collections) to a
compressed JSON payload, reload it later, and verify that the underlying CMR
search has not changed since it was saved.

Design notes
------------
- **Fingerprint**: a SHA-256 over the *sorted* concept-IDs of the saved results.
  Sorting makes it order-insensitive, because CMR does not guarantee a stable
  result ordering between requests (so ``content-sha1``/``content-md5`` from CMR
  are not usable as fingerprints).
- **Payload**: gzipped JSON containing the replay query parameters, the number
  of loaded results, the CMR hit count at save time, the fingerprint, and the
  loaded result dictionaries.
- **Verification**: ``load()`` re-runs the saved query against CMR by default,
  recomputes the fingerprint over the freshly loaded result set, and returns a
  comparison report (fingerprint and hit-count differences). Pass
  ``verify=False`` to load entirely from disk without a network round-trip.
"""

import gzip
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import earthaccess

if TYPE_CHECKING:
    from earthaccess.search.results import SearchResults

FORMAT_VERSION = "earthaccess-search-v1"


def compute_fingerprint(results: "SearchResults") -> str:
    """Compute a stable, order-insensitive fingerprint for search results.

    The fingerprint is a SHA-256 of the sorted concept-IDs of the *currently
    loaded* results. Sorting makes it independent of CMR's result ordering, so
    the same result set always hashes to the same value.

    Parameters:
        results: A SearchResults instance (granules or collections).

    Returns:
        A hex SHA-256 digest prefixed with ``sha256:``.
    """
    concept_ids = sorted(
        item.get("meta", {}).get("concept-id", "") for item in results._cached_results
    )
    digest = hashlib.sha256("\n".join(concept_ids).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _serialize_query_params(results: "SearchResults") -> Dict[str, Any]:
    """Extract replayable query parameters from a SearchResults query object.

    The query object (``DataGranules``/``DataCollections``) stores formatted
    params; ``temporal`` is held as a list of pre-joined strings that cannot be
    replayed through ``parameters()`` as-is. We normalize it back to a list of
    ``(start, end)`` tuples so the saved params can be passed straight to
    ``search_data()``/``search_datasets()``.

    Parameters:
        results: A SearchResults instance.

    Returns:
        A JSON-serializable dict of kwargs suitable for replaying the search.
    """
    params = dict(getattr(results.query, "params", {}))
    temporal = params.pop("temporal", None)
    if temporal:
        # temporal may be a list of "start,end" strings or a single string
        if isinstance(temporal, str):
            temporal = [temporal]
        ranges = []
        for item in temporal:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                ranges.append((item[0], item[1]))
            elif isinstance(item, str) and "," in item:
                start, _, end = item.partition(",")
                ranges.append((start, end))
        if ranges:
            params["temporal"] = ranges if len(ranges) > 1 else ranges[0]
    return params


def _serialize_results(results: "SearchResults") -> List[Dict[str, Any]]:
    """Serialize the currently loaded results as plain dictionaries."""
    return [item.to_dict() for item in results._cached_results]


def save(results: "SearchResults", path: Union[str, Path]) -> Path:
    """Save a SearchResults object to a compressed JSON payload.

    The payload records the replayable query parameters, how many results were
    loaded, the CMR hit count at save time, a content fingerprint over the
    sorted concept-IDs, and the loaded results themselves.

    Parameters:
        results: A SearchResults instance (granules or collections).
        path: Where to write the payload (``.gz`` recommended).

    Returns:
        The path the payload was written to.

    Raises:
        ValueError: If no results are loaded (nothing to persist).
    """
    if not results._cached_results:
        raise ValueError(
            "No results are loaded. Materialize the results first with "
            "list(results) or results.all() before saving."
        )

    kind = (
        "granules"
        if any("GranuleUR" in item.get("umm", {}) for item in results._cached_results)
        else "collections"
    )

    payload = {
        "format": FORMAT_VERSION,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "kind": kind,
        "query_params": _serialize_query_params(results),
        "limit": results.limit,
        "cmr_hits": results.total(),
        "fingerprint": compute_fingerprint(results),
        "results": _serialize_results(results),
    }

    path = Path(path)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(payload, f)

    return path


def load(
    path: Union[str, Path],
    verify: bool = True,
) -> "SearchResults":
    """Load a saved search from a compressed JSON payload.

    By default the saved query is re-run against CMR and the result is compared
    with what was saved. If the search changed (different fingerprint or hit
    count), the returned results expose a comparison report via
    ``results.verification``.

    Parameters:
        path: Path to the saved payload.
        verify: If True (default), re-run the saved query and compare. If False,
            load entirely from disk without a network round-trip.

    Returns:
        A SearchResults instance populated with the saved results. When
        ``verify=True``, ``results.verification`` holds the comparison report;
        when ``verify=False`` it is ``None``.

    Raises:
        FileNotFoundError: If the payload does not exist.
        ValueError: If the payload format is not recognized.
    """
    path = Path(path)
    with gzip.open(path, "rt", encoding="utf-8") as f:
        payload = json.load(f)

    if payload.get("format") != FORMAT_VERSION:
        raise ValueError(
            f"Unrecognized search payload format: {payload.get('format')!r}"
        )

    kind = payload.get("kind", "granules")
    limit = payload.get("limit")
    results = _rebuild_from_payload(payload, kind, limit)

    if verify:
        results.verification = _verify(payload, kind, limit)
    else:
        results.verification = None

    return results


def _rebuild_from_payload(
    payload: Dict[str, Any], kind: str, limit: Optional[int]
) -> "SearchResults":
    """Reconstruct a SearchResults from saved result dictionaries (offline)."""
    from earthaccess.search.results import (
        CollectionResults,
        DataCollection,
        DataGranule,
        GranuleResults,
        SearchResults,
    )

    if kind == "collections":
        cls = CollectionResults
        items: List[Any] = [DataCollection(item) for item in payload["results"]]
    else:
        cls = GranuleResults
        items = [DataGranule(item) for item in payload["results"]]

    results = cls.__new__(cls)  # type: ignore[call-arg]
    SearchResults.__init__(results, query=None, limit=limit, prefetch=0)
    results._cached_results = items  # type: ignore[assignment]
    results._exhausted = True
    results._total_hits = payload.get("cmr_hits")
    results._stored_fingerprint = payload.get("fingerprint")
    results._saved_at = payload.get("saved_at")
    return results


def _verify(payload: Dict[str, Any], kind: str, limit: Optional[int]) -> Dict[str, Any]:
    """Re-run the saved query against CMR and compare with what was saved.

    Returns:
        A comparison report dict with:
        - ``unchanged``: True if fingerprint and hit count both match
        - ``fingerprint_match``: bool
        - ``cmr_hits_match``: bool
        - ``saved_fingerprint`` / ``current_fingerprint``
        - ``saved_cmr_hits`` / ``current_cmr_hits``
        - ``added``: concept-IDs present now but not at save time
        - ``removed``: concept-IDs saved but no longer present
    """
    query_params = payload.get("query_params", {})
    saved_fingerprint = payload.get("fingerprint")
    saved_cmr_hits = payload.get("cmr_hits")
    saved_ids = {r.get("meta", {}).get("concept-id") for r in payload["results"]}

    if kind == "collections":
        fresh = earthaccess.search_datasets(**query_params, count=limit or -1)
    else:
        fresh = earthaccess.search_data(**query_params, count=limit or -1)
    list(fresh)  # materialize for a like-for-like comparison
    current_ids = {
        item.get("meta", {}).get("concept-id") for item in fresh._cached_results
    }

    current_fingerprint = compute_fingerprint(fresh)
    current_cmr_hits = fresh.total()

    fingerprint_match = current_fingerprint == saved_fingerprint
    cmr_hits_match = current_cmr_hits == saved_cmr_hits

    return {
        "unchanged": fingerprint_match and cmr_hits_match,
        "fingerprint_match": fingerprint_match,
        "cmr_hits_match": cmr_hits_match,
        "saved_fingerprint": saved_fingerprint,
        "current_fingerprint": current_fingerprint,
        "saved_cmr_hits": saved_cmr_hits,
        "current_cmr_hits": current_cmr_hits,
        "added": sorted(current_ids - saved_ids),
        "removed": sorted(saved_ids - current_ids),
    }


def save_search(results: "SearchResults", path: Union[str, Path]) -> Path:
    """Save a SearchResults object to a compressed JSON payload.

    Convenience wrapper around :func:`save`, mirroring the
    ``results.save(path)`` method.

    Parameters:
        results: A SearchResults instance (granules or collections).
        path: Where to write the payload (``.gz`` recommended).

    Returns:
        The path the payload was written to.
    """
    return save(results, path)


def load_search(path: Union[str, Path], verify: bool = True) -> "SearchResults":
    """Load a saved search from a compressed JSON payload.

    Convenience wrapper around :func:`load`, mirroring the
    ``SearchResults.load(path)`` class method.

    Parameters:
        path: Path to the saved payload.
        verify: If True (default), re-run the saved query against CMR and
            compare. If False, load offline.

    Returns:
        A SearchResults instance populated with the saved results.
    """
    return load(path, verify=verify)
