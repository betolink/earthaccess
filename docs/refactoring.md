# earthaccess refactoring: this branch vs. upstream

> **Reference point.** "Upstream" in this document means
> [`earthaccess-dev/earthaccess`](https://github.com/earthaccess-dev/earthaccess)
> on its `main` branch, **not** the fork's `origin/main`. The fork's `origin/main`
> is an older snapshot; comparing against it gives misleading results. All facts
> below were verified against `upstream/main` at the time of writing.

## The ethos we are preserving

`earthaccess` exists to lower the barrier between NASA's data systems and the
people who need the data. Search, download, stream — in a few lines of code —
regardless of whether the data lives on-premises or in the cloud.

> "Providing easy access to NASA Earthdata regardless of the data storage
> location (hosted within or outside of the cloud) is the main motivation behind
> this Python library."

This branch is a leap forward: it re-architects the package to support STAC,
virtual datasets, and a decoupled query system. But a leap forward must not cost
us **human-friendliness**. The concrete, testable meaning we use in this document:

- **Public API stability** — the names users import from `earthaccess` are a
  contract. Internals can churn; the public surface must not break without a
  deprecation path.
- **Readability** — a contributor should be able to trace a user call to the
  underlying network request without crossing six layers of indirection.

---

## 1. How this branch diverges from upstream

`nextgen-virtual` is a true fork divergence, not a fast-forward:

| Metric | Value |
|-------|-------|
| Commits ahead of `upstream/main` | **84** |
| Commits behind `upstream/main` | **17** |
| Merge base | `a8afa5d` |
| Upstream `main` is an ancestor? | No |
| Upstream version | `0.18.0` (`requires-python >= 3.12`) |
| This branch version | `1.0.0a2` |

The 17 upstream commits not yet on this branch are mostly docs/CI/README work,
with **two substantive exceptions** we must reconcile (see §3):

- **`878dc08` — "Breaking: Migrate many methods to `@property`s" (#1428)**
- **`6641f33` — "Fixes incorrect granule counts in certain cases" (#1444, pagination)**

### Narrative: the evolution

Upstream grew organically: a handful of flat modules (`results.py`, `search.py`,
`store.py`, `auth.py`) with a `CustomDict`-based data model at its core. Users
loved the simplicity; maintainers felt the growing pain of a monolith. This
branch is the planned answer: reorganize into feature-oriented packages, add
STAC as a first-class citizen, decouple query construction from authentication,
and support virtual datasets — all while keeping the same "a few lines of code"
promise.

### Concrete old → new mapping

| Upstream `main` | This branch | Note |
|-----------------|-------------|------|
| `earthaccess/results.py` (single module) | `earthaccess/search/results.py` | `DataGranule`, `DataCollection`, `SearchResults` moved into the search feature package |
| `earthaccess/search.py` | `earthaccess/search/` package | Queries split into `query/` subpackage |
| `earthaccess/store.py` | `earthaccess/store/` package | `download`, `open`, streaming, parallel access split into focused modules |
| `earthaccess/auth.py` | `earthaccess/auth/` package | `Auth`, credentials, system env separated |
| `earthaccess/formatters.py` | `earthaccess/formatting/` package | HTML + widget rendering |
| `earthaccess/utils/_search.py` | `earthaccess/search/_utils.py` | `get_results` pagination helper relocated |
| — | `earthaccess/stac/` | New: `pystac`-based `to_stac()` converters |
| `earthaccess/virtual/` (4 modules) | `earthaccess/virtual/` (6 modules) | Added `dmrpp.py`, `kerchunk.py`; icechunk/virtualizarr support |

### Where this branch is genuinely ahead

- **STAC as a first-class citizen**: `to_stac()` on `DataGranule` and
  `DataCollection`, a dedicated `earthaccess/stac/converters.py`, and an optional
  `stac` extra (`odc-stac`, `rasterio`).
- **Auth-decoupled query architecture**: `GranuleQuery`, `CollectionQuery`,
  `StacItemQuery`, geometry types (`BoundingBox`, `DateRange`, `Point`,
  `Polygon`), and a validation accumulator — queries are pure data structures
  that can be built before authenticating.
- **Lazy, cacheable results**: `SearchResults` / `GranuleResults` /
  `CollectionResults` support iteration, slicing, `pages()`, `all()`, and a
  `total()` hit count without forcing the user to load everything.
- **Virtual datasets**: kerchunk and DMRPP support plus icechunk virtual cloud
  cubes.
- **`__geo_interface__` on both result types**: upstream only exposes it on
  `DataGranule`; this branch adds it to `DataCollection` too, backed by a shared
  converter (see §3).

### Where this branch is behind upstream

- **`@property` migration (#1428)**: upstream turned `doi()`, `concept_id()`,
  `data_type()`, `version()`, `abstract()`, `landing_page()`, `size()`, and
  related accessors into read-only `@property` fields. **This branch still
  defines them as methods.** This is the single most important gap for the
  patterns in §3.
- **Test infrastructure style**: upstream uses `unittest` +
  `VCRTestCase` with full, uncompressed cassettes; this branch uses `pytest`
  with gzipped, truncated (20-item) cassettes. The truncated-cassette approach
  is more storage-friendly but requires care (see the pagination caveat in §2).

---

## 2. The main architectural differences

### 2.1 Data model: `CustomDict` remains the core

Both branches model CMR results as `dict` subclasses (`CustomDict`), exposing the
raw UMM-G/CMR fields via `self["umm"]` while adding ergonomic accessors. This
branch keeps that model but adds lazy containers around it. **Pattern to keep:**
the data model stays a dictionary; behavior is layered on top, never hidden
behind a parallel object graph.

### 2.2 Query construction is decoupled from auth

On upstream, query objects and authentication are intertwined. This branch
separates them: queries are constructed and validated first; the session is
attached only at execution time. **Pattern to keep:** pure-data query objects
with explicit `to_cmr()` / `to_stac()` conversions.

### 2.3 STAC conversion moved into a dedicated package

Upstream's STAC support is scattered. This branch centralizes it in
`earthaccess/stac/converters.py` with `pystac` as the canonical output.
**Pattern to keep:** conversion logic lives in one place and is shared by both
`DataGranule` and `DataCollection`.

### 2.4 Pagination

Upstream (post-#1444) terminates pagination when a page is empty or the limit is
reached:

```python
more_results = len(latest) > 0 and len(results) < limit
```

This branch's `get_results` in `earthaccess/search/_utils.py` uses the same
termination rule (`len(items) == 0 or len(results) >= limit`), which is the
**correct** production behavior — a short page must not end pagination (see the
regression test `test_get_paginates_past_short_first_page`, PR #1444).

> **Caveat for truncated cassettes.** Because test cassettes truncate every page
> to 20 items, the "empty page or limit" rule never fires naturally. The
> cassette must model the real end-of-results signal explicitly: the **last**
> page must not carry a `CMR-Search-After` header. This is how real CMR signals
> the end, and it is what the `test_collections_more_than_2k` cassette now does.

### 2.5 The `python-cmr` dependency and the path to replacing it

Both upstream (`pyproject.toml:44`) and this branch (`pyproject.toml:38`) declare
`python-cmr >=0.10.0` (locked at `0.13.0`). It is a **runtime hard dependency**,
but the codebase is mid-transition away from it — which is good news for the
planned swap to our own client.

**Where cmr is coupled in today.** Five import sites:

| File | What it takes from cmr |
|------|------------------------|
| `search/queries.py` | `CollectionQuery`, `GranuleQuery` — `DataCollections` / `DataGranules` **subclass** them |
| `search/_utils.py` | `get_results` pagination loop calls `query._build_url()` and `query.headers` |
| `search/results.py` | `_fetch_page` calls `self.query._build_url()`, `.headers`, `.hits()`, `._is_cloud_hosted()` |
| `search/services.py` | `ServiceQuery` for `DataServices` |
| `auth/system.py` | `CMR_OPS`, `CMR_UAT` base URLs |

**The execution contract cmr provides** (what a replacement must reproduce):

- `_build_url()` — CMR URL construction
- `headers` — request headers (format, token plumbing)
- `mode()` — set the CMR environment base URL (`CMR_OPS` / `CMR_UAT`)
- `hits()` — lightweight `CMR-Hits` count query
- `parameters(**kwargs)` — bulk parameter application
- `_is_cloud_hosted()` — cloud-hosted detection (already overridden locally)
- ~17 chainable filter methods (`concept_id`, `short_name`, `temporal`, `bounding_box`, `point`, `polygon`, `line`, `cloud_cover`, `day_night_flag`, `instrument`, `platform`, `version`, `orbit_number`, `online_only`, `downloadable`, `keyword`, `daac`)

**Why the swap should be tractable.**

1. **All 17 filter methods already exist cmr-free** in the new query system
   (`earthaccess/search/query/` — `GranuleQuery`, `CollectionQuery`,
   `StacItemQuery`, geometry types). They are pure data structures with
   `to_cmr()` conversion; the new public API never exposes cmr objects.
2. **Pagination is already earthaccess-owned.** `get_results` and
   `_fetch_page` implement the search-after loop themselves; they only borrow
   `_build_url()` + `headers` from cmr.
3. **The subclassing is thin.** `DataCollections`/`DataGranules` add auth
   sessions, umm_json format, and hit counting on top of cmr's base query
   class — they do not inherit much behavior beyond URL building and filters.

**What the swap will actually require.** Port four runtime primitives into
earthaccess so the cmr subclasses (and `_fetch_page`/`get_results`) can be
re-based onto them:

1. URL construction for collections/granules/services endpoints
2. Request header assembly (format, bearer token)
3. Environment switching (`CMR_OPS` / `CMR_UAT`)
4. Hit-count query

Because the new query objects already produce `to_cmr()` parameter dicts, the
cleanest end state is: the new query system becomes the query layer, and the
execution layer consumes `to_cmr()` output directly — removing the
`DataCollections`/`DataGranules` cmr subclassing entirely. The public API
(`search_data`, `search_datasets`, `DataGranule`, `DataCollection`) is already
agnostic to this; the change is internal.

> **Pattern to keep.** The new query objects must never import or return cmr
> types, and the execution layer should depend on a narrow, owned interface
> (URL builder + headers) rather than on cmr classes. That keeps the eventual
> swap to our own client a mechanical re-base, not a rewrite.

---

## 3. Patterns to implement (and keep) for long-term maintainability

A curated set — enough to keep the codebase human-friendly, small enough that
reviewers can actually enforce it. Each entry names the pattern, why it matters
to the ethos, and how to spot a violation.

### 3.1 Stable, thin public API surface

**Rule.** The names re-exported from `earthaccess/__init__.py` (and the public
functions in `earthaccess/api.py`) are the contract. Everything inside
subpackages is an implementation detail and may be renamed/moved freely.

**Why it matters.** This is the concrete, testable meaning of "public API
stability" from the ethos: users get a few stable verbs (`login`, `search_data`,
`search_datasets`, `download`, `open`, `open_virtual_dataset`) and stable result
types; maintainers get freedom to refactor the internals.

**How to spot a violation.** A private helper imported through the public path,
a function in `api.py` that only exists for internal use, or a public name that
changes shape (method → attribute, positional → keyword-only) without a
deprecation warning.

### 3.2 Feature-oriented packages, single responsibility per module

**Rule.** Group code by feature (`search/`, `store/`, `auth/`, `virtual/`,
`stac/`, `formatting/`), not by layer. Each module has one clear responsibility,
and modules are small enough to hold in your head at once.

**Why it matters.** A contributor answering "how does search work?" should find
one directory. It keeps the package navigable as it grows.

**How to spot a violation.** A module named `utils.py` that accumulates
unrelated helpers, a `store/` module importing from `search/` internals, or a
feature that spans five modules with no clear owner.

### 3.3 Read-only `@property` accessors (the main gap)

**Rule.** Adopt upstream's #1428 convention: computed, metadata accessors are
read-only `@property` fields — `granule.size`, `collection.doi`,
`collection.concept_id`, `collection.version`, etc. — **not** methods. This
branch still defines them as methods (`def size(self)`, `def doi(self)`, ...).

**Why it matters.** Properties read like data, which is what these are. It is
also the upstream convention, so carrying it keeps our branch aligned and makes
the eventual merge (or rebase) of #1428 trivial. `__geo_interface__` is already a
`@property` on both `DataGranule` and `DataCollection` — the rest of the
accessors should follow.

**How to spot a violation.** Any `def foo(self)` on `DataGranule` /
`DataCollection` that returns derived metadata and takes no arguments beyond
`self`. When migrating a method to a property, keep backward compatibility for
at least one release (e.g. upstream's documented 0.19.0 migration).

### 3.4 Shared, pure converters over the data model

**Rule.** Logic that converts raw UMM-G data into another representation lives
in a shared, pure function — e.g. `_geometry_to_geojson` (this branch) or the
STAC converters in `earthaccess/stac/converters.py` — and is reused by every
consumer.

**Why it matters.** It prevents drift: `DataGranule.__geo_interface__` and
`DataCollection.__geo_interface__` must produce identical GeoJSON, and the only
safe way to guarantee that is one implementation. It also isolates the tricky
edge cases (e.g. reversing ExclusiveZone rings to be clockwise) in one
reviewable place.

**How to spot a violation.** Two classes implementing the same conversion
independently, or geometry logic inlined into a caller that isn't the canonical
owner.

### 3.5 Lazy results containers

**Rule.** `search_data` / `search_datasets` return lazy containers
(`GranuleResults` / `CollectionResults`). Nothing is fetched until the user
iterates, slices, calls `list()`, `.all()`, or `.pages()`. `len()` reflects
loaded results; `total()` reflects CMR's hit count.

**Why it matters.** NASA datasets can have millions of granules. Eager loading
would punish the simple "just give me a few" use case the ethos promises.

**How to spot a violation.** A `search_*` call that issues network requests
before the user asks for results, or a container that materializes everything on
construction.

### 3.6 `__geo_interface__` on every result type

**Rule.** Every granule/collection result exposes a GeoJSON representation via
the `@property __geo_interface__`, matching the
[`__geo_interface__` specification](https://gist.github.com/sgillies/2217756).
It reads the UMM-G geometry at
`self["umm"]["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]` and maps
`GPolygons` / `BoundingRectangles` / `Points` / `Lines` to their GeoJSON
equivalents, raising `ValueError` when the geometry is missing or unrecognized.

**Why it matters.** It makes results directly usable with the Python geo
ecosystem (shapely, geopandas, folium) with zero glue code — a perfect fit for
the "a few lines of code" promise. It is also a port of the upstream contract, so
this branch must keep it faithful.

**How to spot a violation.** A result type lacking `__geo_interface__`, or a
`__geo_interface__` that diverges from the shared converter (see §3.4).

### 3.7 Type hints, google-style docstrings, strict mypy

**Rule.** Every public function/class has full type annotations and a
google-style docstring. `mypy` runs clean on the modules we touch (the existing
pre-`nextgen` errors are being tracked down and fixed incrementally).

**Why it matters.** Human-friendly also means "tooling-friendly": editors,
linters, and the docs build (`mkdocstrings`) all read these artifacts. It makes
the package approachable for newcomers contributing their first NASA-data tool.

**How to spot a violation.** A public symbol with no docstring, an
untyped parameter, or a `type: ignore` that hides a real issue rather than a
known third-party limitation.

---

## 4. Recommended next steps

1. **Reconcile the `@property` migration (#1428).** Convert the
   `DataGranule` / `DataCollection` method accessors to properties, following
   upstream's migration and deprecation notes, so this branch matches upstream's
   public shape.
2. **Reconcile the pagination fix (#1444).** The behavior already matches; verify
   the upstream commit's intent is fully covered by this branch's regression
   tests.
3. **Port the missing upstream tests/docs.** The 17 commits behind include
   README/docs/CI polish plus the two substantive fixes; cherry-pick or port the
   substantive ones so neither branch loses work.
4. **Codify the patterns as CI/review checks.** Add pre-commit checks or a
   `CONTRIBUTING` note that enforces §3.1 (public surface) and §3.3 (properties)
   so the conventions survive review pressure.
5. **Keep the truncated-cassette rule documented** (§2.4): the last page of any
   pagination cassette must omit `CMR-Search-After` so playback terminates.

---

## Related architecture documentation

This document is the high-level overview. The sibling documents under
`docs/refactoring/` go deeper on specific subsystems:

| Document | Covers |
|----------|--------|
| [Next-Gen Architecture](./refactoring/earthaccess-nextgen.md) | The complete next-generation design: query system, STAC, credentials, distributed computing |
| [STAC Module Architecture](./refactoring/stac-module-architecture.md) | STAC conversion functions, helpers, extensions, and integration with result types |
| [Architecture Comparison](./refactoring/stac-comparison.md) | GLM vs Opus branch approaches and their design tradeoffs |
| [STAC Implementation Log](./refactoring/stac-implementation-log.md) | Historical record of the phased STAC implementation |
| [Implementation Roadmap](./refactoring/nextgen-implementation.md) | The phased implementation plan tracked in `IMPLEMENTATION_TODO.md` |
| [Virtual DMRPP Groups](./refactoring/vz_dmrpp_groups.md) | DMR++ group virtualization details |

For users, the [User Guide](user/index.md) and [Search & Access API](api/index.md)
remain the entry points; none of the architecture changes affect the core
`login()` / `search_data()` / `download()` / `open()` workflow.
