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
- the **loaded results** themselves (the exact granules/collections you had),
- the **CMR hit count** at save time,
- a **fingerprint** of the result set.

Only the results currently loaded are saved. If you searched with `count=100`
and materialized all 100, that is exactly what gets persisted:

```python
import earthaccess

results = earthaccess.search_data(
    short_name="ATL06",
    temporal=("2024-01-01", "2024-12-31"),
    bounding_box=(-46.5, 61.0, -42.5, 63.0),
    count=100,
)
list(results)  # materialize the 100 granules you care about

results.save("atl06_2024_search.json.gz")
```

This also works for collections returned by `earthaccess.search_datasets()`.

## Load a search

```python
loaded = earthaccess.load_search("atl06_2024_search.json.gz")
```

or, equivalently:

```python
from earthaccess import SearchResults

loaded = SearchResults.load("atl06_2024_search.json.gz")
```

## Verify the search hasn't changed

By default, `load()` **re-runs the saved query against CMR** and compares the
result with what was saved. The returned object carries a comparison report in
`results.verification`:

```python
loaded = earthaccess.load_search("atl06_2024_search.json.gz")

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

If you don't want the network round-trip (e.g. offline), pass `verify=False`:

```python
loaded = earthaccess.load_search("atl06_2024_search.json.gz", verify=False)
# loaded.verification is None
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

- `earthaccess.save_search(results, path)` — save granules or collections.
- `earthaccess.load_search(path, verify=True)` — load and verify.
- `SearchResults.save(path)` — method form.
- `SearchResults.load(path, verify=True)` — classmethod form.

See also the [Search persistence API reference](../../api/index.md#search-persistence)
and the [Granule Results API reference](../../api/granules/granules.md).
