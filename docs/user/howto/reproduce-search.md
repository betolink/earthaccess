# Reproduce, save, and verify searches

Search results are live — CMR keeps adding granules to collections over time,
so the same query executed on two different days can return different results.
If you run an analysis from a set of granules, you usually want to **pin** the
exact result set you used so the analysis can be reproduced or audited later.

`earthaccess` lets you:

- **Save** a `SearchResults` object to a compressed JSON payload
  (`results.save(path)` or `save_search(results, path)`).
- **Load** it back later (`SearchResults.load(path)` or `load_search(path)`).
- **Verify** that the underlying CMR search has not changed since it was saved
  (this is the default on load).

## Save a search

`save()` writes everything needed to reproduce the search later:

- the **replayable query parameters** (so the search can be re-run),
- the **results** themselves (the exact granules/collections you want),
- the **CMR hit count** at save time,
- a **fingerprint** of the result set.

By default `save()` persists the **first page** of results (`limit=2000`) and
logs a warning, so a huge search is never materialized just to save it. Use the
`limit` argument to control how many results are saved:

```python
import earthaccess

results = earthaccess.search_data(
    short_name="ATL06",
    temporal=("2024-01-01", "2024-12-31"),
    bounding_box=(-46.5, 61.0, -42.5, 63.0),
)

results.save("atl06_first_page.json.gz")      # default: first 2000
results.save("atl06_first1k.json.gz", limit=1000)  # first 1000
results.save("atl06_all.json.gz", limit=-1)   # every match
results.save("atl06_3k.json.gz", limit=1000, offset=2000)  # results 2000-2999
```

Results are streamed page by page into a gzipped JSON Lines payload with a
progress bar, so memory stays bounded and an interrupted save keeps every
completed page (only the in-flight line is lost). Iterating a `SearchResults`
is a stream, so `save()` fetches fresh from CMR for the requested `limit` and
does not depend on how much you happened to iterate first.

This also works for collections returned by `earthaccess.search_datasets()`.

## Load a search

```python
loaded = earthaccess.load_search("atl06_2024_search.json.gz")
```

The payload is read line by line, so you can load a **slice** of a large saved
set without materializing all of it:

```python
first_page = earthaccess.load_search("atl06_all.json.gz")          # all
page2 = earthaccess.load_search("atl06_all.json.gz", offset=2000, limit=2000)
```

Verification is offline by default and only performed on a full load when you
pass `verify=True`; loading a slice skips the network round-trip.

## Re-run the saved query

The loaded object keeps the original query parameters, so you can inspect them
or re-run a fresh query with the same filters:

```python
# Inspect the parameters that produced the saved results
loaded.query_params
# {'short_name': 'ATL06', 'temporal': ('2024-01-01T00:00:00', ...),
#  'bounding_box': (-46.5, 61.0, -42.5, 63.0)}

# Re-run a brand new query with the exact same parameters
fresh = earthaccess.search_data(**loaded.query_params, count=100)

# Or rebuild a query object and hand it to search_data
query = loaded.rebuild_query()
fresh = earthaccess.search_data(query=query)
```

`query_params` are stored in a clean, replayable form — spatial filters stay as
`(west, south, east, north)` tuples and temporal ranges as `(start, end)`
pairs, so they can be passed straight back to `search_data()` /
`search_datasets()`.

or, equivalently:

```python
from earthaccess import SearchResults

loaded = SearchResults.load("atl06_2024_search.json.gz")
```

## Verify the search hasn't changed

By default `load()` is **offline** (no network). Pass `verify=True` to re-run
the saved query against CMR and compare it with what was saved. The returned
object carries a comparison report in `results.verification`:

```python
loaded = earthaccess.load_search(
    "atl06_2024_search.json.gz", verify=True, limit=None
)

loaded.verification
# {
#   "unchanged": True,               # False if anything changed
#   "fingerprint_match": True,       # same granule set?
#   "cmr_hits_match": True,          # same total hit count?
#   "saved_fingerprint": "sha256:...",
#   "current_fingerprint": "sha256:...",
#   "saved_cmr_hits": 12070,
#   "current_cmr_hits": 12070,
#   "added": [],                     # concept-IDs present now but not saved
#   "removed": [],                   # concept-IDs saved but now gone
# }
```

### How the fingerprint works

CMR does not guarantee a stable ordering of results between requests, so two
identical queries can return the same granules in a different order (and CMR's
`content-sha1`/`content-md5` headers therefore differ between requests). To get
a stable fingerprint, `earthaccess` hashes the **sorted concept-IDs** of the
loaded results. The same result set always fingerprints identically, regardless
of order, and the fingerprint changes as soon as any granule is added or
removed.

### Load without verifying

Loading is offline by default, so `results.verification` is `None` unless you
pass `verify=True`:

```python
loaded = earthaccess.load_search("atl06_2024_search.json.gz")
# loaded.verification is None (offline, no network)
```

## When would verification report a change?

- A new granule matched the query since you saved (e.g. new data ingested) →
  it shows up in `added`.
- A granule you saved no longer matches the query (e.g. reprocessed and moved,
  or your search window excluded it) → it shows up in `removed`.
- The collection kept the same loaded granules but the total hit count changed
  (e.g. new data outside your saved window) → `cmr_hits_match` is `False`.

Verification only reports what changed; it does not raise. Check
`loaded.verification["unchanged"]` to decide whether to trust the loaded
results for a reproducible workflow.

## Module reference

- `earthaccess.save_search(results, path, limit=2000, offset=0)` — save granules or collections (default saves the first page; `limit=-1` saves all).
- `earthaccess.load_search(path, verify=False, offset=0, limit=2000)` — load offline by default (optionally a slice; `verify=True` to compare).
- `SearchResults.save(path, limit=2000, offset=0)` — method form.
- `SearchResults.load(path, verify=False, offset=0, limit=2000)` — classmethod form.
- `SearchResults.reset()` — drop streamed results and return to the initial prefetch.

See also the [Search persistence API reference](../../api/index.md#search-persistence)
and the [Granule Results API reference](../../api/granules/granules.md).
