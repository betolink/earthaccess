# Main Integration Plan: landing `nextgen-virtual` in upstream earthaccess

**Goal:** split `nextgen-virtual` into **six** self-contained PRs against
`earthaccess-dev/earthaccess`, each independently reviewable and mergeable, in
an order that keeps conflicts mechanical and keeps breaking changes clearly
flagged rather than diluted inside larger PRs.

> **Revision note.** This supersedes the original 9-PR draft. The 9-theme
> breakdown is still a useful reference for file scoping (each PR below lists
> which of the original themes it absorbs), but the original plan overstated
> some cross-theme dependencies and used a stale `upstream/main` SHA. See
> "What changed from the 9-PR draft" at the end of this document.

**Facts this plan relies on (re-verified this session):**
- Upstream: `earthaccess-dev/earthaccess` (remote `upstream`). Fork: `betolink/earthaccess`.
- `upstream/main` tip is `6775dd6` ("Enable D401 and D417 rulesets and fix
  violations #1460"). The merge base is `c591efc` ("Add PyOpenSci review badge
  #1463"), which **is already a direct ancestor of `nextgen-virtual`** (via
  `085d007`), so the only upstream drift is the single commit `6775dd6`.
  Every PR branch below still starts fresh from `upstream/main`, not from
  `nextgen-virtual`.
- `nextgen-virtual` tip is `128394f`. Relevant pre-existing commits:
  - `60d1593` — post-merge property-migration fixes (reconciling main's
    PR #1428 "migrate methods to `@property`s" across the whole codebase).
  - `fcfc194` — bulk `to_geopandas()` staticmethods in
    `earthaccess/search/queries.py`.
  - `4534462` — configurable CORS proxy (`earthaccess.set_proxy()`), new in
    this session; scoped to the dedicated Proxy PR below.
  - `128394f` — docs navigation reconciled onto upstream's `awesome-nav` /
    `docs/.nav.yml` layout, new in this session; scoped to the Docs/Release
    PR below.
- Upstream's backwards-compatibility doc permits breaking changes in a 1.0
  major release, provided there is a migration guide. Breaking changes are
  isolated to PR B, called out explicitly in that PR's description.
- `pyproject.toml` `[project.urls]` already points at
  `github.com/earthaccess-dev/earthaccess` on the branch (the old `nsidc`
  URLs are gone), so PR A carries that as-is rather than fixing it.
- Working tree is clean except for this plan document (which currently has
  uncommitted edits); no other uncommitted-changes step is needed before
  starting.

---

## Ground rules

1. **Branch model.** One branch per PR, named `pr/<letter>-<theme>`, created
   from `upstream/main`. Merge in the order below; after each merge, rebase
   the next dependent branch onto the new upstream main tip. The **Virtual**
   PR has no dependency on PR B/C and can be branched and reviewed in
   parallel with them (see merge-order table).
2. **Porting method — never cherry-pick history.** The commits on
   `nextgen-virtual` are interleaved and cannot be replayed cleanly. Instead,
   per PR:
   ```bash
   git fetch upstream
   git checkout -b pr/<letter>-<theme> upstream/main
   git checkout nextgen-virtual -- <paths listed below>
   # then strip out-of-scope hunks where a file belongs to multiple PRs
   ```
3. **Hot files.** `earthaccess/api.py` and `earthaccess/search/results.py`
   each contain multiple PRs' worth of code. `results.py`'s ~2,684 lines
   split cleanly along whole-method/whole-class boundaries with no
   interleaving inside method bodies (verified), so splitting is mechanical
   line-range extraction, not hunk surgery. The exact regions are listed
   under each PR below.
4. **Definition of done (every PR):** unit + integration tests green (VCR
   cassettes included where relevant), docs build (`mkdocs build --strict`),
   no undeclared public-API change, and `uv lock` regenerated where
   dependencies change.
5. **PR description template.** Each PR body states: theme(s) absorbed from
   the original 9-theme breakdown, files touched, **an explicit "Breaking
   changes" section (say "None" if truly none)**, tests added, and which
   later PRs build on it. PR B and PR C in particular must not let a real
   behavior change (see their notes) get lost among additive content.

---

## Step 0 — Local preparation (not a PR)

```bash
git fetch upstream
git log --oneline -3 upstream/main          # expect tip 6775dd6
git diff --stat upstream/main...nextgen-virtual -- earthaccess tests docs pyproject.toml | tail -1
```

`nextgen-virtual` is the source of truth for code — do not rebase it; PR
branches are built fresh from `upstream/main` and populated by copying paths
out of `nextgen-virtual`.

---

## PR A — Foundation: deps/CI + package reorg + query architecture

**Absorbs original themes:** 1 (deps & CI), 2 (package reorg), 3 (query
architecture).

**Purpose:** additive/mechanical groundwork only. No behavioral change to
existing CMR search execution. Establishes the file layout (`auth/`,
`exceptions/`, `search/`, `store/daac.py`) that every later PR's paths depend
on, plus ships a new (unused-by-default) type-safe query builder as
additional public API surface.

**Rationale for bundling 1+2+3:** all three are non-breaking; theme 3 has
**zero import dependency** on anything else in the codebase (verified: no
`earthaccess.*` imports anywhere under `earthaccess/search/query/`) and zero
file overlap with theme 2, so bundling adds no conflict risk — only a review
-mode switch (mechanical refactor vs. new API design) within one PR. Theme
1's diff is tiny (306/81 lines) and not worth a solo review cycle.

**Port from `nextgen-virtual`:**

*Infra (theme 1):*
- `pyproject.toml` (partial — see steps below)
- `.github/workflows/gh-pages.yml`
- `.readthedocs.yaml`
- `mkdocs.local.yml`
- `.codeignore`, `.codespellignore`

*Reorg (theme 2):*
- `earthaccess/auth/` (`__init__.py`, `auth.py`, `credentials.py`, `system.py`)
- `earthaccess/exceptions/__init__.py`
- `earthaccess/search/_utils.py`
- `earthaccess/search/services.py`
- `earthaccess/search/queries.py` — the CMR-executor classes `DataCollections`/
  `DataGranules` (formerly top-level `search.py`), **including**:
  - the doi/spatial-filter fixes already on the branch
  - the bulk `to_geopandas()` staticmethods added in commit `fcfc194` — these
    only call `item.__geo_interface__`, which does not exist yet in `results.py`
    at this point in the PR sequence. **Strip these two staticmethods
    (`DataCollections.to_geopandas`, `DataGranules.to_geopandas`) and the
    `_items_to_geopandas` helper out of this PR; they land in PR C instead**,
    once `__geo_interface__` exists on `DataCollection`/`DataGranule`. Keep
    everything else in `queries.py`.
- `earthaccess/search/results.py` — **only the pre-nextgen (upstream `main`)
  538-line version, moved as-is** to its new path. This is a straight file
  move (confirmed via diff: old `earthaccess/results.py` deleted, byte-for-
  -byte content re-added under `search/`), not nextgen's expanded 2,684-line
  version — that lands incrementally in PRs B/C/D.
- updated top-level `earthaccess/__init__.py` (only the reorg-driven import
  path changes; **exclude** any exports that reference symbols not yet
  introduced by this PR — cross-check against what's actually available
  after this PR's file set)

*Query architecture (theme 3):*
- `earthaccess/search/query/` (`__init__.py`, `base.py`, `collection_query.py`,
  `geometry.py`, `granule_query.py`, `stac_query.py`, `types.py`,
  `validation.py`)
- the `query=` kwarg wiring in `earthaccess/api.py` (`NewCollectionQuery`/
  `NewGranuleQuery` type hints, `query.validate()`/`query.to_kwargs()` calls)
  — **port only this slice of `api.py`**; the rest of `api.py`'s rewrite is
  PR B's. Note `StacItemQuery` is exported but has no `api.py` consumer
  (confirmed orphaned at the wiring level) — port it anyway since it has its
  own tests and is harmless additive surface; flag this in the PR
  description so reviewers know it's forward-looking, not dead code to be
  removed.
- tests: `tests/unit/test_query.py`, `tests/unit/test_geometry.py`,
  `tests/unit/test_granule_queries.py`, `tests/unit/test_collection_queries.py`,
  `tests/unit/test_auth.py`, `tests/unit/test_services.py`,
  `tests/unit/test_deprecations.py`

**Delete:** `earthaccess/auth.py`, `earthaccess/system.py`,
`earthaccess/exceptions.py`, `earthaccess/search.py`, `earthaccess/services.py`,
`earthaccess/results.py`, `earthaccess/utils/`.

**Keep untouched in this PR:** `store.py`/`daac.py` (full package lands in PR
B; this PR only needs `store/daac.py`'s straight move since `search/queries.py`
imports `find_provider`/`find_provider_by_shortname` from it — port that one
file now, empty `store/` package otherwise), `formatters.py`, `widgets.py`,
`virtual/` (Virtual PR).

**Backward-compat shims to add** (new small modules, each with a
`DeprecationWarning`):
- `earthaccess/results.py` → re-exports from `earthaccess.search.results`.
- `earthaccess/system.py` → re-exports `PROD`, `UAT`, `System` from
  `earthaccess.auth.system`.
- Verify `from earthaccess.auth import Auth` and
  `from earthaccess.search import DataGranule` keep working via package
  re-exports.

**Also include:** deprecation of `SessionWithHeaderRedirection` (factory
function replaces it) — small, self-contained auth change.

**Steps:**
1. `git checkout -b pr/A-foundation upstream/main`
2. Port files; write shims; update `__init__.py`; strip the two
   `to_geopandas` staticmethods from `queries.py` per the note above.
3. `pyproject.toml`: apply only —
   - core deps: add `pystac >= 1.8`, `tqdm >= 4.66`; drop `pqdm`; bump
     `tenacity >= 9.0`.
   - extras: restructure to `virtualizarr` (adds `icechunk >= 2.1`,
     `h5netcdf`, `virtualizarr >= 2.3.0`), new `geo`, `widgets`, `stac`
     groups; update `all`.
   - `[project.urls]` should use the `earthaccess-dev` org (already the case
     on `nextgen-virtual`, so verify rather than edit).
   - **exclude** the version change (`1.0.0a2`, hatch-vcs removal) and
     `requires-python >= 3.12` — both belong to the Docs/Release PR.
4. Regenerate the lock: `uv lock` (do not copy nextgen's `uv.lock` — it is
   entangled with later PRs' dependencies).
5. Run the **existing upstream** unit test suite (adjusted only for import
   paths) unchanged in behavior — it must pass without behavioral edits, as
   proof the refactor is behavior-preserving. Add one small test asserting
   deprecation warnings fire for shim imports.
6. Run CI + `mkdocs build`.

**Notes for maintainers:**
- `pystac` becomes a *core* dependency because later PRs' `search/results.py`
  imports it unconditionally. If maintainers prefer it optional, PR B must
  use lazy imports — flag this tradeoff now, before it is baked in.
- The `widgets`/`geo` extras carry `python_version < '3.14' and platform !=
  PyPy` markers from the branch; keep them.
- The new query builder (`GranuleQuery`/`CollectionQuery`/`BoundingBox`/
  `DateRange`/`Point`/`Polygon`) is real, public, exported API — not
  internal-only — even though it's purely additive. Say so explicitly in the
  PR body so reviewers evaluate it as new API surface, not as an invisible
  implementation detail.

**DoD:** CI green on upstream main + new deps; `python -c "import
earthaccess"` and all old-style imports work; existing test suite green with
zero behavioral test edits.

---

## Proxy — configurable CORS proxy

**Absorbs:** new feature (not part of the original 9-theme breakdown).

**Purpose:** add `earthaccess.set_proxy()` / `get_proxy()` (and the
`EARTHDATA_PROXY_URL` environment variable) so Earthdata Login, CMR, and
status requests can be routed through a CORS proxy, enabling browser-based
environments such as JupyterLite.

**Depends on:** PR A only (it needs the reorganized `earthaccess/auth/`
import paths). Independent of PR B/C and the Virtual PR, so it can be branched
and reviewed in parallel with them.

**Why a dedicated PR rather than folding into PR A:** PR A is explicitly
scoped as a mechanical reorg with **no behavioral change**. This adds public
API and changes request routing, so bundling it into A would contradict A's
stated premise and bury new behavior inside a "trust me, nothing changed" PR.

**Port from `nextgen-virtual` (commit `4534462`):**
- `earthaccess/auth/system.py` — proxy state, `set_proxy()`/`get_proxy()`,
  `route()`, and the `System.route()` convenience
- `earthaccess/auth/auth.py` — route EDL URLs; extend
  `BasicAuthResponseHook` to recognize proxied EDL URLs and reject look-alike
  hosts
- `earthaccess/search/queries.py` — route the CMR base URL for both
  authenticated and unauthenticated queries
- `earthaccess/store/core.py` — route the EDL profile and CMR collection URLs
- `earthaccess/api.py` — route `status()`; export `set_proxy`/`get_proxy`
- `earthaccess/__init__.py`, `earthaccess/auth/__init__.py` — public exports
- `.github/workflows/integration-test.yml` — pass through the PROD/UAT token
  secrets
- `.gitignore` — ignore `.wrangler/`
- tests: `tests/unit/test_system.py`, `tests/integration/test_proxy.py`,
  `tests/integration/proxy_worker/` (Cloudflare Worker + `wrangler.toml`),
  and the worker/`.env` fixtures in `tests/integration/conftest.py`
- docs: `docs/user/howto/authenticate.md`, `docs/api/auth/auth.md`,
  `docs/contributor/explanation/integration-tests.md`
- `CHANGELOG.md` entry

**Steps:**
1. `git checkout -b pr/proxy upstream/main` (rebased onto merged PR A state)
2. Port files; keep `edl_hostname` logical — it is both the `.netrc` key and
   the redirect credential guard.
3. Run the unit tests and the proxy integration suite (the latter needs Node
   for `npx wrangler`, plus EDL credentials/tokens).

**DoD:** unit tests green; proxy integration suite green; `mkdocs build
--strict`; `edl_hostname` semantics unchanged; proxy API documented with a
CHANGELOG entry.

---

## PR B — Core: SearchResults, streaming store, persistence & STAC

**Absorbs original themes:** 4 (SearchResults/store) and 5 (STAC converters).

**Purpose:** replace list-returning search with the `SearchResults` model:
lazy pagination, prefetch, `items()`/`filter()`/`all()`/`reset()`,
reproducible `save()`/`load()`, the new modular `store/` package with
parallel/streaming executors, and bidirectional CMR↔STAC conversion
(`to_stac()`/`to_dict()`, `earthaccess/stac/`).

**⚠️ This is the one PR with an actual breaking change:** `search_data()`/
`search_datasets()` return `SearchResults`/`GranuleResults`/
`CollectionResults` objects instead of plain lists. Mitigated by
iterability (`list(results)` still works) and a migration-guide excerpt
included in this PR. **State this in the first line of the PR description**,
not buried in a changelog bullet.

**Why STAC (theme 5) is bundled here rather than split out:** STAC's
coupling to this PR's classes is soft/runtime only — `stac/converters.py`
imports `DataGranule`/`DataCollection` under `TYPE_CHECKING` or via deferred
in-function imports, never at module load time — so it *could* be its own
PR. It stays bundled here to keep the plan at six PRs total, on the
judgment that ~1,600 lines of low-risk, purely-additive code does not
justify its own review cycle. **The mitigation for review-dilution is
process, not physical separation:** land STAC as a clearly separate second
commit within this PR, and require the PR description to open with the
breaking-change callout before any STAC content, so reviewers hit the
breaking change first regardless of how they skim the diff. If reviewers
push back on PR size or feel the breaking change is getting lost, splitting
STAC into its own PR is the first thing to reconsider (see "What changed
from the 9-PR draft" at the end of this document).

**Port from `nextgen-virtual`:**

*Core (theme 4):*
- `earthaccess/api.py` (the remaining slice — full query wiring + new
  return types + `prefetch`/`page_size` params — beyond what PR A already
  ported for the `query=` kwarg)
- `earthaccess/search/results.py` — nextgen's version **with two slices
  stripped out** (line numbers are current as of this writing; re-verify
  against the actual file at port time):
  - remove `__geo_interface__` on `DataCollection` (~L186–221) and
    `DataGranule` (~L865–901), and `_geometry_to_geojson` (~L751–840) →
    Display & Geo PR
  - remove `_repr_html_`/`explore`/`plot` on `DataCollection` (~L626–666),
    `DataGranule` (~L963–1000), and `SearchResults`/subclasses
    (~L2271–2280, ~L2398–2450) → Display & Geo PR
  - **keep**: `CustomDict` base (note: the per-item `to_geopandas()`
    instance method at ~L124–177 lives on `CustomDict` and depends on
    `__geo_interface__`, which is stripped in this PR — strip this method
    too, it moves to the Display & Geo PR alongside `__geo_interface__`),
    `SearchResults`/`GranuleResults`/`CollectionResults` core (pagination,
    caching, `_ensure_cached`, `_materialize`, `save`, `load`, `reset`,
    `summary`, `filter`), `GranuleFilter`, **and** `to_dict()`/`to_stac()` on
    `DataCollection` (~L445–465), `DataGranule` (~L1157–1230), and
    `SearchResults`/subclasses (~L2360–2398) — these stay in this PR as the
    STAC commit.
- `earthaccess/search/persistence.py`
- `earthaccess/store/` package (remaining files beyond `daac.py`, already
  ported in PR A): `__init__.py`, `core.py`, `access.py`, `assets.py`,
  `distributed.py`, `download.py`, `file_wrapper.py`, `filesystems.py`,
  `parallel.py`, `streaming.py`, `target.py`
- delete `earthaccess/store.py` (old monolithic module)
- tests: `test_search_results.py`, `test_results.py`, `test_streaming.py`,
  `test_persistence.py`, `test_parallel.py`, `test_store*.py` (all of them:
  `test_store.py`, `test_store_access.py`, `test_store_asset.py`,
  `test_store_asset_integration.py`, `test_store_credentials.py`,
  `test_store_download.py`, `test_store_file_wrapper.py`,
  `test_store_filesystems.py`, `test_store_integration.py`,
  `test_store_streaming.py`), `test_executor_credentials.py`,
  `test_executor_credentials_integration.py`, `test_executor_strategy.py`,
  `test_target_filesystem.py`, `test_api_query_integration.py`, `test_uat.py`,
  and the compressed VCR cassettes (`tests/unit/fixtures/**/*.yaml.gz`)
- docs: `docs/tutorials/results/` (`index.md`, `results-class.ipynb`),
  `docs/tutorials/advanced-search/` (`index.md`, `filter-bands.ipynb`),
  `docs/tutorials/access-datasets/index.md`, `docs/user/howto/reproduce-search.md`,
  `docs/api/collections/collections.md`, `docs/api/granules/granules.md`,
  `docs/api/store/store.md`
- deprecations: `hits()` alias on results

*STAC (theme 5):*
- `earthaccess/stac/` (`__init__.py`, `converters.py`)
- tests: `tests/unit/test_stac_converters.py`
- docs: `docs/api/stac/` (`converters.md`, `methods.md`, `overview.md`,
  `queries.md`), `docs/tutorials/cmr-to-stac.ipynb`, `docs/tutorials/stac/index.md`

**Steps:**
1. `git checkout -b pr/B-core upstream/main` (rebased onto merged PR A state)
2. Port files; strip the two `results.py` slices listed above (geo,
   display) — keep the STAC slice in this PR; port cassettes.
3. Two commits within this one PR:
   - Commit 1: `SearchResults`/store/persistence core (the breaking change).
   - Commit 2: STAC converters + `to_stac()`/`to_dict()` methods (additive).
4. Run unit + integration suites.

**Notes for maintainers:**
- This is the biggest diff (~7.1k lines combined) and contains most of the
  ~450k-line cassette deletion (old uncompressed cassettes replaced by
  compressed, response-truncated ones). Explain that in the PR body so
  reviewers aren't alarmed by the diff size — it's overwhelmingly test
  fixture compression, not logic.
- Add the "Results model" section of `docs/migration-guide.md` scoped to
  this change (the full migration guide lands in the Docs/Release PR).

**DoD:** all new tests green; migration note present; `list(granules)`
iteration still works; PR description leads with the breaking-change
callout, with STAC clearly delineated as a separate, additive commit.

---

## PR C — Display & Geo

**Absorbs original themes:** 6 (display/`explore()`) and 7 (geo).

**Purpose:** rich theme-aware HTML reprs, granule thumbnails, collapsible
asset lists, the interactive `explore()` map (renamed from `plot()`), plus
`__geo_interface__` and both the per-item and bulk `to_geopandas()` APIs.

**Rationale for bundling 6+7, and why NOT 6-depends-on-7 as originally
written:** re-verified this session — `to_geopandas()`/`__geo_interface__`
import only `shapely`/`geopandas`/`pandas`, never `earthaccess.formatting`.
`explore()`'s own geometry-drawing code (`_geometry_to_shapely`,
`_extract_granule_geometry` in `formatting/widgets.py`) is a fully
independent UMM→shapely implementation that does not call
`__geo_interface__` or `to_geopandas()` — the two "geo" surfaces solve a
similar sub-problem twice, in parallel, not sequentially. Theme 7's real
(new, post-branch) dependency is on PR A's `search/queries.py` — the bulk
`to_geopandas()` staticmethods added in commit `fcfc194` call
`item.__geo_interface__`, so they must land after `__geo_interface__` exists,
i.e. in this PR, using the already-merged `queries.py` from PR A as the
target file.

**⚠️ Not purely additive despite the framing:** this PR **deletes
`earthaccess/formatters.py`** (upstream's existing HTML-repr module) and
rewires `DataGranule._repr_html_`/`DataCollection._repr_html_` to new
implementations — existing notebook rendering output changes. Call this out
explicitly in the PR description; don't let it hide behind "just adds new
display methods."

**Port from `nextgen-virtual`:**

*Display (theme 6):*
- `earthaccess/formatting/` (`__init__.py`, `html.py`, `widgets.py`,
  `css/styles.css`)
- delete `earthaccess/formatters.py` and top-level `earthaccess/widgets.py`
  (add a deprecation shim for `earthaccess.widgets` if any downstream import
  exists — re-check with `grep -r "earthaccess.widgets"` at port time)
- `_repr_html_()`, `explore()`, `plot()` methods on `DataCollection`,
  `DataGranule`, `SearchResults`/subclasses in `search/results.py` (apply on
  top of PR B's merged, stripped-down `results.py`)
- tests: `tests/unit/test_formatters.py`

*Geo (theme 7):*
- `__geo_interface__` on `DataCollection`/`DataGranule`, `_geometry_to_geojson`
  helper, and the per-item `to_geopandas()` instance method on `CustomDict`,
  all in `search/results.py` (same rebase point as display, applied on top
  of PR B's `results.py`)
- geometry-drawing hunks in `explore()` (point/line/polygon support beyond
  basic bbox — these are in `formatting/widgets.py`, ported above; no
  additional file here, just noting the historical order: bbox-only
  `explore()` predates point/line/polygon geometry support, which predates
  `__geo_interface__`/`to_geopandas()` — all three landed as separate,
  non-dependent commits on the original branch)
- the bulk `to_geopandas()` staticmethods on `DataCollections`/`DataGranules`
  (currently in `earthaccess/search/queries.py` on `nextgen-virtual`,
  commit `fcfc194`) — apply this hunk to PR A's already-merged
  `search/queries.py`
- tests: `tests/unit/test_geo_interface.py`, `tests/unit/test_to_geopandas.py`

**Steps:**
1. `git checkout -b pr/C-display-geo upstream/main` (rebased onto merged PR
   B state)
2. Port files; apply the `results.py` hunks (display + geo) and the
   `queries.py` bulk-`to_geopandas` hunk.
3. Run formatter + geo tests; manual Jupyter smoke check of the HTML repr and
   lonboard map with `earthaccess[widgets]`/`earthaccess[geo]` installed.

**DoD:** display + geo tests green; `explore()` renders for granule/
collection/single-result cases; dark/light theme CSS applied; `to_geopandas()`
returns a GeoDataFrame for both single items and bulk lists (granule and
collection); PR description explicitly flags the `formatters.py` repr
replacement as a behavior change.

---

## Virtual — Virtual datasets module (independent, parallel-mergeable)

**Absorbs original theme:** 8 only.

**Purpose:** multi-parser virtual datasets: DMRPP parser, kerchunk module,
`open_virtual`/`write_virtual`, icechunk integration.

**Independence:** confirmed zero structural coupling to search/store/
formatting/stac — `virtual/*.py` only does `import earthaccess` at the
top-level package boundary (used at runtime inside function bodies for
`earthaccess.search_data()`, `earthaccess.get_fsspec_https_session()`, etc.,
never at import time) and imports its own submodules (`_credentials`,
`_parser`, `_types`, kept as-is, shared with main). **This PR can branch and
be reviewed in parallel with PR B and PR C** — it only needs PR A's reorg
(for `earthaccess.auth`/`earthaccess.store.daac` import paths) as a
prerequisite, not PR B or C.

**Port from `nextgen-virtual`:**
- `earthaccess/virtual/core.py` (new version), `earthaccess/virtual/dmrpp.py`,
  `earthaccess/virtual/kerchunk.py`, `earthaccess/virtual/__init__.py`
- keep `_parser.py`, `_credentials.py`, `_types.py` as-is (shared with main;
  only trivial diffs)
- tests: `tests/unit/test_virtual.py`, `tests/unit/test_virtual_write.py`,
  and the TEMPO/MERRA2 + Icechunk integration tests
- docs: `docs/refactoring/icechunk-vcc.md`, `docs/refactoring/vz_dmrpp_groups.md`,
  `docs/refactoring/virtualize-combine-tree.md`, `docs/tutorials/vds/index.md`,
  `docs/tutorials/icechunk_virtual.ipynb`, `docs/tutorials/virtualize_combine_tree.ipynb`
  (moved here from the original theme 9 docs list — these are virtual-module
  -specific, not general release docs, so they travel with the code they
  document rather than waiting for the final Docs/Release PR)
- pyproject: the `virtualizarr` extra changes are already in from PR A —
  nothing new here.

**Steps:**
1. `git checkout -b pr/virtual upstream/main` (rebased onto merged PR A
   state at minimum; later PR B/C merges don't affect it, so it does not
   need to be re-rebased when they land — only rebase again just before
   final merge to pick up any incidental upstream drift)
2. Port files + tests + docs.
3. Run virtual tests with `earthaccess[virtualizarr]` installed, including
   the end-to-end icechunk notebook test.

**DoD:** virtual suite green; `open_virtual_dataset`,
`open_virtual_mfdataset`, `write_virtual` all importable and tested; docs
build.

---

## Docs/Release — Version bump, breaking-change docs & release notes

**Absorbs original theme:** 9, minus the virtual-specific docs (moved to the
Virtual PR above).

**Purpose:** make it a real 1.0 alpha: version, Python floor, changelog,
full migration guide, cross-cutting architecture docs, release notes. Must
land last — it needs the final shape of every other PR to narrate correctly
(PR numbers, actual API surface, actual breaking changes).

**Port from `nextgen-virtual`:**
- `pyproject.toml`: version → `1.0.0a2` (incl. hatch-vcs dynamic-version
  removal), `requires-python >= 3.12`, classifier additions
- `CHANGELOG.md` (reconcile with actual merged PR numbers, not internal
  commit references)
- `docs/migration-guide.md` (full — supersedes the partial excerpts already
  landed in PR B/PR C's own descriptions)
- `docs/releases/1.0.0a.md`
- `docs/user/explanation/backwards-compatibility.md` updates
- `docs/user/explanation/architecture.md` (cross-cutting system diagram —
  covers query/search/STAC/streaming together, so it can only be written
  accurately once those PRs exist)
- `docs/user/explanation/search.md`, `docs/user/howto/search-services.md`
  (already-updated property-access examples from `nextgen-virtual` commit
  `60d1593` carry forward automatically since they're part of this file set)
- `docs/user/quick-start.md`
- `docs/user/tutorials/SSL.ipynb`, `docs/user/tutorials/emit-earthaccess.ipynb`
- `docs/api/index.md`, `docs/api/stac/` (`converters.md`, `methods.md`,
  `overview.md`, `queries.md`) — API reference pages for STAC; land here
  rather than the STAC-adjacent PR B since they're documentation-format
  pages, not code
- `docs/tutorials/cmr-to-stac.ipynb`, `docs/tutorials/stac/index.md`
- `docs/contributor/howto/development.md`
- docs navigation config: `mkdocs.yml`, `mkdocs.local.yml`,
  `mkdocs.gh-pages.yml`, and `docs/.nav.yml` — reconciled onto upstream's
  `awesome-nav` layout (keeping the branch-only pages), with `mkdocs.yml`
  matching upstream's `site_url` and `strict: true`
- `pyproject.toml` docs extra: `mkdocs-awesome-nav` (plus the regenerated
  `uv.lock`); `mkdocs.local.yml`/`mkdocs.gh-pages.yml` override `plugins:` and
  must re-list `awesome-nav` for `docs/.nav.yml` to apply
- `docs/refactoring.md`, `docs/refactoring/earthaccess-nextgen.md`,
  `docs/refactoring/nextgen-implementation.md`,
  `docs/refactoring/stac-comparison.md`,
  `docs/refactoring/stac-implementation-log.md`,
  `docs/refactoring/stac-module-architecture.md` (optional — internal
  planning docs; include or drop per maintainer preference, per original
  plan's note)
- `IMPLEMENTATION_TODO.md` (optional; it's a work log, not user docs)

**Steps:**
1. `git checkout -b pr/docs-release upstream/main` (rebased onto merged
   Virtual PR state — i.e., after all of A, B, C, and Virtual have merged)
2. Port files; reconcile CHANGELOG with the actual merged history.
3. Full verification: complete unit + integration suite, doctests, `mkdocs
   build --strict`, and a final tree diff (below).

**DoD / final verification:**
```bash
git diff --stat upstream/main...nextgen-virtual -- earthaccess tests docs pyproject.toml
```
The residual diff should be exactly: the backward-compat shims added in PR
A, the version/URL handling, and any deliberately excluded content.
Everything else should match `nextgen-virtual`.

---

## Merge order & conflict map

| PR | Theme(s) absorbed | Depends on | Hot-file conflicts | Breaking? |
|----|---|---|---|---|
| A | 1, 2, 3 | — | none | No |
| Proxy | new | A | `auth/system.py`, `auth/auth.py`, `api.py`, `search/queries.py`, `store/core.py` | No |
| B | 4 | A | `api.py`, `search/results.py` (core slice) | **Yes — return-type change** |
| C | 6, 7 | B (soft: A for the bulk `to_geopandas` hunk in `queries.py`) | `search/results.py` (display+geo slices), `search/queries.py` | No (but repr output changes) |
| Virtual | 8 | A only | none | No |
| Docs/Release | 9 (minus virtual docs) | A, Proxy, B, C, Virtual (all) | none | Policy (version/Python floor) |

Virtual can be branched and reviewed in parallel with the Proxy PR and with
B/C — it has zero file overlap with any of them. The Proxy PR similarly only
needs PR A and can be reviewed in parallel with B/C/Virtual. Both must still
merge before Docs/Release, since the changelog/migration guide describe them.

---

## Risk register (raise these before opening PRs)

1. **`requires-python >= 3.12`** (Docs/Release) — the single biggest
   breaking change. Open a discussion issue first; if maintainers want to
   keep 3.11, audit `pystac`/`virtualizarr`/`icechunk` minimums before
   committing.
2. **`pystac` as core dependency** (PR A) — required because PR B's results
   module imports it unconditionally. Alternative: lazy imports under the
   `stac` extra; decide in PR A's review.
3. **Breaking return type of `search_data`/`search_datasets`** (PR B) —
   mitigated by iterability + migration guide; expect maintainer scrutiny
   here. Do not let PR B's size (still ~5.5k lines even without STAC) or its
   necessarily-technical store/streaming content bury this in review.
4. **Cassette replacement diff size** (PR B) — pre-explain the ~450k-line
   deletion in the PR body; it is fixture compression, not logic.
5. **`formatters.py` repr replacement** (PR C) — existing notebook HTML
   rendering changes, not purely additive. Call this out explicitly; don't
   let it ride along unchallenged next to the genuinely inert geo additions
   in the same PR.
6. **Proxy PR integration tests need Node + `wrangler`** — the suite starts a
   Cloudflare Worker via `npx wrangler@4` (Node is present on
   `ubuntu-latest`; the worker fixture skips if `npx` is missing). It also
   expects `EDL_TOKEN`/`EDL_UAT_TOKEN` (or username/password) secrets — the
   credential-dependent tests no longer skip, so UAT will fail in CI without
   `EDL_UAT_TOKEN`.
7. **`StacItemQuery` is orphaned** (PR A) — defined, tested, exported, but no
   `api.py` function consumes it yet (no `search_stac()`). Flag as
   forward-looking API surface, not something reviewers should expect to see
   wired up in this PR.

---

## Execution checklist (per PR, copy-paste)

```bash
git fetch upstream
git checkout -b pr/<letter>-<theme> upstream/main   # or rebase after previous merge
git checkout nextgen-virtual -- <files from scope list>
# strip out-of-scope hunks per the notes above (especially results.py's
# three-way split across PR B / PR C, and queries.py's to_geopandas hunk)
uv lock                                             # only if pyproject changed
uv run pytest tests/unit                            # + integration where noted
mkdocs build --strict                               # docs-affected PRs
```

---

## What changed from the 9-PR draft

This plan consolidates the original 9 themes into six PRs (A, Proxy, B, C,
Virtual, Docs/Release), based on a re-verification of actual file-level
dependencies in the current codebase (as of `nextgen-virtual` @ `128394f`):

- **Theme 3 (query architecture) folded into PR A**, not kept separate: it
  has zero import dependency on anything (verified by grep across
  `earthaccess/search/query/`), so bundling with the reorg (theme 2) adds no
  conflict risk, only a review-mode switch within one PR.
- **Theme 1 (deps/CI) folded into PR A**: too small (306/81 lines) to
  justify a solo review cycle; shares theme 2's "no behavior change"
  character.
- **Theme 5 (STAC) bundled into PR B** as a second, clearly separated commit
  (rather than split into its own 6th PR). STAC's coupling to PR B's classes
  is soft/runtime (deferred imports), so it could stand alone, but ~1,600
  lines of low-risk additive code was judged not worth a separate review
  cycle. The mitigation for review-dilution risk is process (breaking-change
  callout first in the PR description, STAC isolated to its own commit), not
  physical separation. If reviewers find the breaking change insufficiently
  isolated in practice, splitting STAC into its own PR is the first thing to
  reconsider.
- **Theme 7 (geo)'s stated dependency on theme 6 (display) was found to be
  false** — verified `to_geopandas()`/`__geo_interface__` never import
  `earthaccess.formatting`, and `explore()`'s geometry drawing is an
  independent implementation. They're bundled into PR C anyway because they
  share a conceptual neighborhood (map/geometry surface) and both attach to
  the same post-PR-B `results.py`, not because one requires the other.
- **Theme 7 gained a new dependency** not present in the original plan: the
  bulk `to_geopandas()` staticmethods added to `nextgen-virtual` this session
  (commit `fcfc194`) live in `search/queries.py` (theme 2's file, now part of
  PR A), so PR C must apply a hunk to PR A's already-merged `queries.py` in
  addition to `results.py`.
- **Theme 8 (virtual) kept fully separate**, not folded into PR A (too large
  and behaviorally substantive to bundle with a "trust me, nothing changed"
  infra PR) nor into the final Docs/Release PR (unrelated concerns —
  reviewers would have to context-switch between deep parser review and
  version/policy sign-off in one PR). It ships as its own PR, explicitly
  parallel-mergeable with B/C.
- **Virtual-specific docs moved out of theme 9** into the Virtual PR
  (`docs/refactoring/icechunk-vcc.md`, `vz_dmrpp_groups.md`,
  `virtualize-combine-tree.md`, `docs/tutorials/vds/`,
  `icechunk_virtual.ipynb`, `virtualize_combine_tree.ipynb`) — they document
  that PR's code specifically, not the release as a whole.
- **New Proxy PR (not in either draft)** added as a standalone PR: the
  configurable CORS proxy (`earthaccess.set_proxy()` / `EARTHDATA_PROXY_URL`)
  landed on `nextgen-virtual` this session (commit `4534462`) and is public
  API + routing behavior, so it must not ride along in PR A's "no behavior
  change" reorg. It depends only on PR A.
- **Docs navigation reconciled onto upstream** (`awesome-nav` + `docs/.nav.yml`)
  this session (commit `128394f`), keeping the branch-only pages. This is why
  the Docs/Release scope now includes `mkdocs.yml`, `mkdocs.local.yml`,
  `mkdocs.gh-pages.yml`, and the `mkdocs-awesome-nav` dependency — configs
  that override `plugins:` must re-list `awesome-nav` or `docs/.nav.yml` is
  silently ignored.
- **`upstream/main` SHA corrected**: the original plan's `3e4538a` is stale.
  The merge base `c591efc` is an ancestor of `nextgen-virtual`; the current
  tip `6775dd6` is one commit ahead of it and is not yet merged.
</content>
