# Main Integration Plan: landing `nextgen-virtual` in upstream earthaccess

**Goal:** split the 139-commit `nextgen-virtual` branch (178 files, +54k/−450k lines) into nine self-contained PRs against `earthaccess-dev/earthaccess`, each independently reviewable and mergeable, in an order that keeps conflicts mechanical.

**Facts this plan relies on:**
- Upstream: `earthaccess-dev/earthaccess` (remote `upstream`). Fork: `betolink/earthaccess`.
- Local `main` (`a8afa5d`) is stale — upstream main (`3e4538a`) has ~18 more commits, including breaking #1428 ("migrate methods to `@property`s") and granule-count fix #1444. **Every PR branch starts from current `upstream/main`, not local main.**
- Upstream's backwards-compatibility doc permits breaking changes in a 1.0 major release, provided there is a migration guide. The plan uses that: breaking changes are isolated in specific PRs with docs.
- Known bug on the branch: `pyproject.toml` `[project.urls]` points to `github.com/nsidc/earthaccess`; correct org is `earthaccess-dev`. Fix during porting (PR 1).

---

## Ground rules

1. **Branch model.** One branch per PR, named `pr/N-<theme>`, created from `upstream/main`. Merge in numeric order; after each merge, rebase the next branch onto the new upstream main tip.
2. **Porting method — never cherry-pick history.** The 139 commits are interleaved and cannot be replayed cleanly. Instead, per PR:
   ```bash
   git fetch upstream
   git checkout -b pr/N-<theme> upstream/main
   git checkout nextgen-virtual -- <paths listed below>
   # then strip out-of-scope hunks where a file belongs to multiple themes
   ```
3. **Hot files.** `earthaccess/api.py`, `earthaccess/search/results.py`, and the `store/` package each contain several themes. The "Exclude" lists below define what stays out of each PR; later PRs rebase onto already-merged state, so conflicts stay localized to those files.
4. **Definition of done (every PR):** unit + integration tests green (VCR cassettes included), docs build (`mkdocs build`), no public-API change beyond what the PR description declares, and `uv lock` regenerated where dependencies change.
5. **Working-tree hygiene first.** The branch has uncommitted changes (`docs/tutorials/odc-stac-cmr.ipynb` modified, `docs/tutorials/gnssro/` untracked). Decide per file: commit to the branch or shelve; do not let them leak into PR branches.
6. **PR description template.** Each PR body states: theme, files touched, breaking changes (if any), tests added, and which later PRs build on it.

---

## Step 0 — Local preparation (not a PR)

```bash
git fetch upstream
# sanity check
git log --oneline -3 upstream/main          # expect tip 3e4538a
git diff --stat main...nextgen-virtual | tail -1
```

- Resolve the uncommitted notebook changes (commit to `nextgen-virtual` or stash).
- Optionally refresh local `main` to `upstream/main` so future diffs are against current upstream.
- Keep `nextgen-virtual` as the **source of truth for code** — do not rebase it; PR branches are built fresh from it.

---

## PR 1 — Dependencies & CI groundwork

**Purpose:** additive dependency changes and CI/docs infrastructure only. No Python behavior change.

**Port from `nextgen-virtual`:**
- `pyproject.toml` (partial — see steps)
- `.github/workflows/gh-pages.yml`
- `.readthedocs.yaml`
- `mkdocs.local.yml` (optional; local notebook-free serve config)
- `.codeignore`, `.codespellignore`

**Exclude from `pyproject.toml`:** the version change (`1.0.0a2` + removal of hatch-vcs dynamic versioning) and `requires-python >= 3.12` — both belong to PR 9. Keep main's version mechanism and Python floor for now.

**Steps:**
1. `git checkout -b pr/1-deps-ci upstream/main`
2. Port the files above; in `pyproject.toml` apply only:
   - core deps: add `pystac >= 1.8`, `tqdm >= 4.66`; drop `pqdm`; bump `tenacity >= 9.0`.
   - extras: restructure to `virtualizarr` (adds `icechunk >= 2.1`, `h5netcdf`, `virtualizarr >= 2.3.0`), new `geo`, `widgets`, `stac` groups; update `all`.
   - fix `[project.urls]` org to `earthaccess-dev`.
3. Regenerate the lock: `uv lock` (do **not** copy nextgen's `uv.lock` — it is entangled with other changes).
4. Run CI + `mkdocs build`.

**Notes for maintainers:**
- `pystac` becomes a *core* dependency because `search/results.py` imports it unconditionally. If maintainers prefer it optional, PR 4 must use lazy imports — flag this tradeoff in the PR description now, before it is baked in.
- The `widgets`/`geo` extras carry the `python_version < '3.14' and platform != PyPy` markers from the branch; keep them.

**DoD:** CI green on upstream main + new deps; no test changes needed.

---

## PR 2 — Package reorganization (refactor-only, backward-compatible)

**Purpose:** modularize `auth`, `exceptions`, and the search internals; move `results.py` into the `search/` package. **No behavior change.** Old import paths keep working via re-exports and deprecation shims.

**Port from `nextgen-virtual`:**
- `earthaccess/auth/` (`__init__.py`, `auth.py`, `credentials.py`, `system.py`)
- `earthaccess/exceptions/__init__.py`
- `earthaccess/search/_utils.py`
- `earthaccess/search/services.py`
- `earthaccess/search/queries.py` (the old `search.py` executor classes `DataCollections`/`DataGranules`, incl. the doi/spatial-filter fixes)
- `earthaccess/search/results.py` — **main's version, moved as-is** (not nextgen's new file; that lands in PR 4)
- updated top-level `earthaccess/__init__.py`

**Delete:** `earthaccess/auth.py`, `earthaccess/system.py`, `earthaccess/exceptions.py`, `earthaccess/search.py`, `earthaccess/services.py`, `earthaccess/results.py`, `earthaccess/utils/`.

**Keep untouched in this PR:** `store.py`, `daac.py`, `formatters.py`, `widgets.py`, `virtual/` (all land in later PRs).

**Backward-compat shims to add (new small modules, each with a `DeprecationWarning`):**
- `earthaccess/results.py` → re-exports from `earthaccess.search.results` (top-level module path must survive; it does not conflict with the `search/` package).
- `earthaccess/system.py` → re-exports `PROD`, `UAT`, `System` from `earthaccess.auth.system`.
- Verify `from earthaccess.auth import Auth` and `from earthaccess.search import DataGranule` keep working via package re-exports (they do, since `auth/` and `search/` re-export the old names).

**Also include:** deprecation of `SessionWithHeaderRedirection` (factory function replaces it) — small, self-contained auth change.

**Steps:**
1. `git checkout -b pr/2-reorg upstream/main`
2. Port files; write shims; update `__init__.py`.
3. Run the **existing** unit test suite unchanged — it must pass without edits (that is the proof the refactor is behavior-preserving). Add one small test asserting the deprecation warnings fire for shim imports.

**DoD:** full existing test suite green with zero test edits; `python -c "import earthaccess"` and old-style imports work.

---

## PR 3 — Query architecture (additive)

**Purpose:** new type-safe query builder classes, not yet wired into the public API. Purely additive.

**Port from `nextgen-virtual`:**
- `earthaccess/search/query/` (`__init__.py`, `base.py`, `collection_query.py`, `geometry.py`, `granule_query.py`, `stac_query.py`, `types.py`, `validation.py`)
- tests: `tests/unit/test_query.py`, `tests/unit/test_geometry.py`, `tests/unit/test_granule_queries.py`

**Exclude:** any `api.py` changes (wiring lands in PR 4).

**Steps:**
1. `git checkout -b pr/3-query upstream/main` (rebased onto merged PR 2 state)
2. Port the package + tests; export the new classes from `earthaccess.search.__init__` alongside the legacy cmr-based ones (as nextgen does).
3. Run tests.

**DoD:** new tests green; existing suite untouched and green; no public behavior change.

---

## PR 4 — SearchResults, streaming store & persistence (the core breaking PR)

**Purpose:** replace list-returning search with the `SearchResults` model: lazy pagination, prefetch, `items()`/`filter()`/`all()`/`reset()`, reproducible `save()`/`load()`, and the new modular `store/` package with parallel/streaming executors. **Breaking:** `search_data`/`search_datasets` return result objects instead of lists.

**Port from `nextgen-virtual`:**
- `earthaccess/api.py` (full — query wiring + new return types + `prefetch`/`page_size` params)
- `earthaccess/search/results.py` — nextgen's version **with slices stripped**:
  - remove `to_stac()`/`to_dict()` methods (PR 5)
  - remove `_repr_html_()`, `explore()`, `plot()` and the `earthaccess.formatting` imports (PR 6)
- `earthaccess/search/persistence.py`
- `earthaccess/store/` package: `__init__.py`, `core.py`, `file_wrapper.py`, `filesystems.py`, `daac.py`, `parallel.py`, `streaming.py`, `target.py`, `assets.py`, `distributed.py`
- delete `earthaccess/store.py`, `earthaccess/daac.py`
- tests: `test_search_results.py`, `test_streaming.py`, `test_persistence.py`, `test_parallel.py`, `test_store*.py` (all of them), `test_executor*.py`, `test_target_filesystem.py`, updated `test_results.py`, and the compressed/truncated VCR cassettes (`tests/**.yaml.gz`)
- deprecations: `hits()` alias on results

**Steps:**
1. `git checkout -b pr/4-results upstream/main` (rebased onto merged PR 3 state)
2. Port files; strip the two results.py slices listed above; port cassettes.
3. Run unit + integration suites.

**Notes for maintainers:**
- This is the biggest diff and contains most of the ~450k-line deletion (old uncompressed cassettes replaced by compressed, response-truncated ones). Explain that in the PR body so reviewers are not alarmed by the diff size.
- The breaking return type is documented here: add the "Results model" section of `docs/migration-guide.md` scoped to this change (full migration guide lands in PR 9).

**DoD:** all new tests green; migration note present; old-style `list(granules)` iteration still works because `SearchResults` is iterable.

---

## PR 5 — STAC converters (additive)

**Purpose:** bidirectional CMR↔STAC conversion and pystac-based `to_stac()` on granules/collections.

**Port from `nextgen-virtual`:**
- `earthaccess/stac/` (`__init__.py`, `converters.py`)
- `to_stac()`/`to_dict()` methods added to `DataGranule`/`DataCollection` in `search/results.py` (this is the rebase conflict point — apply on top of merged PR 4)
- tests: `tests/unit/test_stac_converters.py` + the multi-file collection asset-extraction parametrized tests

**Steps:**
1. `git checkout -b pr/5-stac upstream/main` (rebased onto merged PR 4 state)
2. Port package + methods + tests.
3. Run tests; verify `earthaccess[stac]` extra installs and converters work with `odc-stac`/`rasterio`.

**DoD:** converter tests green; `to_stac()` output validates against pystac.

---

## PR 6 — Display & `explore()` (large but isolated)

**Purpose:** rich theme-aware HTML reprs, granule thumbnails, collapsible asset lists with S3+HTTPS links, and the interactive map: `plot()` renamed to `explore()`.

**Port from `nextgen-virtual`:**
- `earthaccess/formatting/` (`__init__.py`, `html.py`, `widgets.py`, `css/styles.css`)
- delete `earthaccess/formatters.py` and top-level `earthaccess/widgets.py` (add a deprecation shim for `earthaccess.widgets` if any downstream import exists; check with `grep -r "earthaccess.widgets"`)
- `_repr_html_()`, `explore()`, `plot()` methods on result classes in `search/results.py` (second rebase conflict point)
- tests: `tests/unit/test_formatters.py`

**Steps:**
1. `git checkout -b pr/6-display upstream/main` (rebased onto merged PR 5 state)
2. Port files; apply results.py hunks.
3. Run formatter tests + a manual Jupyter smoke check of the HTML repr and lonboard map with `earthaccess[widgets]`.

**DoD:** display tests green; `explore()` renders for granule/collection/single-result cases; dark/light theme CSS applied.

---

## PR 7 — Geo additions (small, depends on PR 6)

**Purpose:** `__geo_interface__`, `to_geopandas()`, and point/line/polygon geometry drawing in `explore()`.

**Port from `nextgen-virtual`:**
- `__geo_interface__` and `to_geopandas()` methods in `search/results.py`
- geometry-drawing hunks in `explore()`
- tests: `tests/unit/test_geo_interface.py`, `tests/unit/test_to_geopandas.py`

**Steps:**
1. `git checkout -b pr/7-geo upstream/main` (rebased onto merged PR 6 state)
2. Port methods + tests; verify `earthaccess[geo]` extra (`shapely`, `geopandas`).
3. Run tests.

**DoD:** geo tests green; `to_geopandas()` returns a GeoDataFrame for granule and collection results.

---

## PR 8 — Virtual module (independent track)

**Purpose:** multi-parser virtual datasets: DMRPP parser, kerchunk module, `open_virtual`/`write_virtual`, icechunk integration. Independent of the search/results work — can be reviewed in isolation.

**Port from `nextgen-virtual`:**
- `earthaccess/virtual/core.py` (new version), `earthaccess/virtual/dmrpp.py`, `earthaccess/virtual/kerchunk.py`, `earthaccess/virtual/__init__.py`
- keep `_parser.py`, `_credentials.py`, `_types.py` as-is (shared with main)
- tests: `tests/unit/test_virtual.py`, `tests/unit/test_virtual_write.py`, and the TEMPO/MERRA2 + Icechunk integration tests
- pyproject: the `virtualizarr` extra changes are already in from PR 1 — nothing new here.

**Steps:**
1. `git checkout -b pr/8-virtual upstream/main` (rebased onto merged PR 2 state at minimum; later merges don't affect it)
2. Port files + tests.
3. Run virtual tests with `earthaccess[virtualizarr]` installed, including the end-to-end icechunk notebook test.

**DoD:** virtual suite green; `open_virtual_dataset`, `open_virtual_mfdataset`, `write_virtual` all importable and tested.

---

## PR 9 — Version bump, breaking changes & docs (final)

**Purpose:** make it a real 1.0 alpha: version, Python floor, changelog, full migration guide, release notes.

**Port from `nextgen-virtual`:**
- `pyproject.toml`: version → `1.0.0a2` (incl. the hatch-vcs dynamic-version removal), `requires-python >= 3.12`, classifier additions
- `CHANGELOG.md` (1.0.0a1/a2 sections)
- `docs/migration-guide.md` (full), `docs/releases/1.0.0a.md`, `docs/user/explanation/backwards-compatibility.md` updates, `docs/user/explanation/architecture.md`, `docs/user/howto/reproduce-search.md`, tutorial restructure (`docs/tutorials/**` new notebooks: results class, band filtering, STAC, virtualize), `docs/refactoring/**` (optional — internal planning docs; include or drop per maintainer preference)
- `IMPLEMENTATION_TODO.md` (optional; it's a work log, not user docs)

**Steps:**
1. `git checkout -b pr/9-release upstream/main` (rebased onto merged PR 8 state)
2. Port files; reconcile CHANGELOG with the actual merged history (PR numbers instead of internal commit references).
3. Full verification: complete unit + integration suite, doctests, `mkdocs build`, and a final tree diff (below).

**DoD / final verification:**
```bash
git diff --stat upstream/main...nextgen-virtual -- earthaccess tests docs pyproject.toml
```
The residual diff should be exactly: the backward-compat shims added in PR 2, the version/URL handling, and any deliberately excluded notebook changes. Everything else should match `nextgen-virtual`.

---

## Merge order & conflict map

| PR | Theme | Depends on | Hot-file conflicts |
|----|-------|-----------|--------------------|
| 1 | deps + CI | — | none |
| 2 | reorg + shims | 1 (auth extras) | none (pure moves) |
| 3 | query objects | 2 | none |
| 4 | SearchResults + store | 2, 3 | `api.py`, `search/results.py` (first pass), `store/*` |
| 5 | STAC | 1, 4 | `search/results.py` (to_stac hunks) |
| 6 | display/explore | 1, 4 | `search/results.py` (repr hunks) |
| 7 | geo | 6 | `search/results.py` (geo hunks), `explore()` |
| 8 | virtual | 1, 2 | none |
| 9 | version/docs | all | none |

PR 8 is the only one that can be developed and reviewed fully in parallel with PRs 4–7 — it touches disjoint files.

## Risk register (raise these before opening PRs)

1. **`requires-python >= 3.12`** (PR 9) — the single biggest breaking change. Open a discussion issue first; if maintainers want to keep 3.11, audit `pystac`/`virtualizarr`/`icechunk` minimums before committing.
2. **`pystac` as core dependency** (PR 1) — required because results import it unconditionally. Alternative: lazy imports under the `stac` extra; decide in PR 1's review.
3. **Breaking return type of `search_data`/`search_datasets`** (PR 4) — mitigated by iterability + migration guide; expect maintainer scrutiny here.
4. **Cassette replacement diff size** (PR 4) — pre-explain the ~450k-line deletion in the PR body.
5. **pyproject URL bug** (`nsidc` → `earthaccess-dev`) — fix in PR 1; mention it so maintainers notice the fork-specific artifact.

## Execution checklist (per PR, copy-paste)

```bash
git fetch upstream
git checkout -b pr/N-<theme> upstream/main        # or rebase after previous merge
git checkout nextgen-virtual -- <files from scope list>
# strip excluded hunks per the "Exclude" notes
uv lock                                            # only if pyproject changed
uv run pytest tests/unit                            # + integration where noted
mkdocs build                                       # docs-affected PRs
```
