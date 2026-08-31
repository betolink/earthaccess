# Results Class tutorials

## Learning goals

`search_data()` and `search_datasets()` return **lazy result containers**
(`GranuleResults`, `CollectionResults`, both subclasses of `SearchResults`)
rather than plain lists. This section teaches you how to get the most out of
them — nothing is fetched until you ask for it.

In this section you will learn to:

- Iterate, index, and slice results: `len()`, `[0]`, `[-1]`, `[:10]`, `list()`.
- Query the CMR hit count with `total()` / `hits()` without downloading results.
- Fetch everything with `all()` or stream page-by-page with `pages()`.
- Summarize loaded results with `summary()`, map them with `plot()`, and
  convert them to STAC with `to_stac()`.

## The result container

`SearchResults` exposes:

- `total()` — the number of matches reported by CMR (`CMR-Hits`), without
  fetching all results.
- `hits()` — an alias for `total()`, for consistency with the CMR API.
- `all()` — fetch and return every result as a list.
- `pages(page_size=...)` — iterate over pages of results.
- `summary()` — aggregate metadata (total, loaded, size, cloud count, temporal
  range) for loaded results.
- `plot()` — an interactive map of the results' spatial extents (needs the
  `[widgets]` extra).
- `to_stac()` — convert loaded results to `pystac` items/collections.

Each `DataGranule` / `DataCollection` also carries convenience fields and
methods (`summary()`, `doi()`, `concept_id()`, `data_links()`, `size()`,
`__geo_interface__`, …).

## Tutorials

- [Results Class Overview](results-class.ipynb) — a hands-on walkthrough of the
  lazy container: the `repr`, `total()` vs `len()`, indexing/slicing,
  iteration, `all()`, `pages()`, `items()`, `filter()`, `summary()`, and
  `to_stac()`.

## Try it next

- [Results API reference](../../api/granules/granules.md) — `DataGranule`
  reference.
- [Collections API reference](../../api/collections/collections.md) —
  `DataCollection` reference.
- [How search works](../../user/explanation/search.md) — what the result
  containers wrap.
- [Access to selected datasets](../access-datasets/index.md) — end-to-end
  workflows on specific NASA missions that use these result containers.
