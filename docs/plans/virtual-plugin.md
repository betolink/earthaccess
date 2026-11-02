# earthaccess-plugin-vds — Implementation Plan

Port `earthaccess.virtualize()` / `write_virtual()` into the plugin architecture as a
fourth plugin, **`vds`**, that virtualizes granules, creates/appends virtual stores
(icechunk default, kerchunk-parquet alternative), runs on a user-supplied Dask cluster,
and reports progress through the `Job` contract.

Status: design agreed in conversation (2026-11-02). Implementation in
progress — branches `virtual-plugin` (fork) and `vds-contract` (spec) exist;
Phase 1 (contract) and the engine slice of Phase 2 are done. Live progress and
handoff notes: `~/github/betolink/earthaccess-plugin-vds/TODO.md`. This file
remains the canonical design.

---

## 1. Constraints (non-negotiable)

- **Additive only.** No breaking changes to core, the contract, or the three existing
  plugins (harmony, pydap, sliderule).
- **`CONTRACT_VERSION` stays `1`.** A bump would unload the existing plugins. All spec
  changes are additive enum members / Literal values / output kinds.
- **Core's `earthaccess/virtual/` is untouched this phase.** `ea.virtualize`,
  `ea.write_virtual`, `ea.open_virtual` keep working unchanged. Deprecation is a
  follow-up, gated on acceptance (§8).
- **Only two store formats:** `icechunk` (default) and `kerchunk` (parquet refs).
  **No JSON output anywhere** in the new code, including the internal `load=True`
  round-trip (temp parquet refs dir instead).
- **earthaccess never serializes credentials.** Nothing containing secrets (EDL
  tokens, AWS keys, credentialed obstore stores) crosses the Dask wire. Credentials
  are resolved in the environment where the code runs — client or worker — from env
  vars / IAM roles / platform secrets (e.g. Coiled). Cluster setup is the user's
  concern; there is no `scheduler_url` option.
- Version pins are load-bearing (all verified empirically in the dev venv):
  `obstore>=0.12` (store + credential-provider pickle support), `icechunk>=2.3`
  (`S3Options`, `Storage.new_s3_object_store`, `scatter_initial_credentials`),
  `virtualizarr>=2.8` (executor *instances* in `open_virtual_mfdataset(parallel=…)`,
  `KerchunkParquetParser` from `obspec_utils.registry`).

## 2. Repos & branches

| Repo | Branch | Off | Work |
|---|---|---|---|
| `betolink/earthaccess-plugin-spec` | `vds-contract` | `main` | additive contract → merge → **tag `v0.3.0`** |
| `betolink/earthaccess-plugin-vds` | `main` (new repo) | — | ported engine + plugin |
| `betolink/earthaccess` (fork) | `virtual-plugin` | `plugin-spec` | registry row, `ea.open()` routing, pins, notebooks |

Naming: distribution `earthaccess-plugin-vds`, module `earthaccess_vds`, class
`VDSPlugin`, entry point `vds` → `ea.plugins.vds`.

Sequencing: the spec tag must exist before the fork re-pins (during dev the fork and
plugin may pin the spec branch SHA, retargeted to `@v0.3.0` at the end).

Plan home: this file (`docs/plans/virtual-plugin.md` on the fork's `virtual-plugin`
branch) is the canonical copy. A worktree (e.g.
`~/github/betolink/earthaccess@virtual-plugin`) is optional for isolated work.

## 3. User-facing design

### 3.1 Minimal calls (defaults carry the rest)

```python
import earthaccess as ea
ea.login()

results = ea.search_data(short_name="MUR-JPL-L4-GLOB-v4.1", temporal=("2013-01-02", "2013-01-11"))

# create a local icechunk store — that's all a user writes
job = ea.plugins.vds.request(results, store="mur_sst", concat_dim="time")
asset = job.results()[0]

# read it back
ds = ea.open(asset.href)

# append: SAME call — store exists, so append_dim defaults to concat_dim.
# Bring your own cluster (Client / Coiled / LocalCluster) with its secrets —
# created before request(); the active Client is used automatically.
client = Client("coiled-mycluster")
new = ea.search_data(short_name="MUR-JPL-L4-GLOB-v4.1", temporal=("2013-01-11", "2013-01-21"))
job = ea.plugins.vds.request(new, store=asset.href, concat_dim="time")
asset = job.results()[0]
ds = ea.open(asset.href)

# just a virtual dataset (nothing persisted)
vds = ea.plugins.vds.request(results, concat_dim="time").results()

# store-to-store merge: open one store, append it to another (both formats work)
job = ea.plugins.vds.request(store="cube_v2", source="cube_v1", append_dim="time")
```

Required-ish kwargs only: `concat_dim` (multi-granule) and `group` (non-root groups).
`store="s3://…"` targets resolve their credentials from the environment (client or
worker, §3.5). Everything else defaults.

### 3.2 Options (final)

```python
@dataclass
class Options:
    # --- persistence (store= triggers write/append) ---
    store: S3Store | LocalStore | str | Path | None = None
    prefix: str | None = None                    # repo prefix when store= is an S3Store
    format: Literal["icechunk", "kerchunk"] = "icechunk"
    source: str | Path | Asset | None = None     # existing store to merge INTO `store`
    append_dim: str | None = None                # defaults to concat_dim when store exists
    replace: bool = False                        # kerchunk: rewrite in place, stable href
    commit: bool = True                          # icechunk only
    # no write_credentials: s3:// targets resolve credentials from the environment
    # (§3.5); an explicit obstore store / icechunk Storage is the escape hatch

    # --- virtualize ---
    access: Literal["direct", "indirect"] = "indirect"   # ⚠ flagged decision, see §13
    parser: ParserType = "DMRPPParser"           # automatic HDFParser fallback
    group: str = "/"
    concat_dim: str | None = None
    combine: CombineType = "nested"
    preprocess: Callable | None = None
    loadable_variables: list[str] | None = None
    drop_variables: list[str] | None = None
    load: bool = False
    tree: bool = False
    reference_dir: str | None = None             # temp dir for the load=True round-trip
    combine_kwargs: dict | None = None           # escape hatch for xr combine kwargs

    # --- execution ---
    show_progress: bool | None = None            # None = auto (interactive)
```

`reference_format` is **not** public (internal round-trip is parquet; users wanting
custom refs use `load=False` + `vds.virtualize.to_kerchunk()`). `parallel` and
`scheduler_url` are **gone**: the internal executor computes with dask's default
resolution — local threads, or the user's active distributed `Client` when they have
one (cluster setup, including secrets, is the user's separate concern).

### 3.3 Output kinds, `load`, and merging

**A virtualization operation's output is an Asset.** The plugin declares
`output = "assets"` (its primary mode); the non-persisting modes override per job
exactly the way pydap does (`self.job(fn, output=…)`):

| Mode | `output` | `results()` |
|---|---|---|
| `store=` given (create / append / merge) | `"assets"` | `[Asset(href=store, access="file"\|"s3", auth="none", format="icechunk"\|"kerchunk")]` |
| `store=None`, `load=False` | `"virtual"` | ManifestArray Dataset / DataTree (for in-memory combine, preprocess chains) |
| `store=None`, `load=True` | `"xarray"` | concrete lazily-loaded Dataset (temp **parquet** refs round-trip) |

**`load` is only valid without `store`.** Persisting and reading are separate
concerns: with `store=` the job returns the Asset, and materializing is `ea.open`'s
job — `ds = ea.open(asset.href, load=True)`. Passing `load=True` together with
`store=` raises a `ValueError` saying exactly that. The internal round-trip is
parquet-only (no JSON anywhere).

**Appending never loads.** The existing store is reopened in manifest form —
`IcechunkParser` (branch/tag/snapshot pinning supported) or `KerchunkParquetParser`
— so append is manifests-in, manifests-out.

**Store-to-store merge:** `request(store=B, source=A, append_dim=…)` opens `A`
with the parser sniffed from its URI/Asset format, combines it with any granule
delta, and writes `B`. `requires_results` is effectively false when only `source`
is given (sliderule precedent). Create vs append is decided by target-store
existence; no user flag.

### 3.4 Format model

| | `format="icechunk"` (default) | `format="kerchunk"` |
|---|---|---|
| Writer | icechunk `Storage` + VCC merge-on-append | `to_kerchunk(format="dict")` → `kerchunk.df.refs_to_dataframe(refs, url, storage_options=…)` (fsspec, unconditionally) |
| Target | local dir or `s3://bucket/prefix` | refs **directory** (convention: *named* `*.parquet`, so `.endswith(".parquet")` sniffing keeps working) |
| Append | `to_icechunk(append_dim)` + commit — **O(delta)**, versioned | read old refs → combine → rewrite **new** store — **O(full)**, no history (port of `_open_virtual_via_virtualizarr`: `KerchunkParquetParser` + fsspec-loaded coords, then `xr.combine_nested([old, delta], concat_dim=…)`) |
| `href` on append | stable | changes (`mur_refs.parquet` → `mur_refs_v2.parquet`) unless `replace=True` (fsspec-rm root + rewrite) |
| Credentials | env-resolved at the write site (§3.5); explicit obstore store / `Storage` = client-side only | env-resolved (s3fs / icechunk `from_env`); worker env when a Client is active |
| Zarr metadata | v3 | v2 only (`to_kerchunk` raises for zarr_format=3 + parquet) |

### 3.5 Credential channels (resolved where the code runs; never serialized)

**Rule: earthaccess serializes URLs, config, and manifests — never secrets.** No EDL
tokens, AWS keys, or credentialed obstore stores cross the Dask wire (they *can*
pickle — verified — but by design we don't). Credentials resolve in the environment
where the code runs:

1. **Worker-side (delta virtualization, NASA reads).** Each worker lazily runs
   `earthaccess.login(strategy="environment")` (`EARTHDATA_USERNAME`/
   `EARTHDATA_PASSWORD` — exactly what Coiled secrets / platform env inject) and
   builds its own obstore registry inside the task: HTTPStore + Bearer header for
   indirect, `NasaEarthdataCredentialProvider` for direct (credentials *endpoint*
   URL from granule metadata — public info, safe to ship). The client sends only a
   serializable **registry config**: URLs, access mode, endpoints. In-region public
   buckets: anonymous direct reads need no EDL at all. Missing env → a typed error
   naming the exact variables.
2. **Write site (repo / refs target).** `s3://` targets resolve credentials from the
   environment where the write runs — worker env when a distributed `Client` is
   active (icechunk `s3_storage(from_env=True)` / s3fs env), client env otherwise.
   Manifests and ref dicts are credential-free, so shipping them to a worker is
   safe. An explicitly passed obstore store / icechunk `Storage` is the
   preconfigured writable session: honored **client-side only**, never serialized.
3. **Read-back (`ea.open`)** — client-side, existing `_authorize_vccs` flow (EDL
   Bearer injection into NASA HTTP VCCs only; refreshable NASA creds for `s3://`
   VCCs; `authorize_virtual_chunk_access` override). `load=True` lazy reads resolve
   on the client too.

Cluster setup is a separate concern: the user creates their `Client` (Coiled,
`LocalCluster`, …) with secrets attached **before** calling `request()`; dask's
default scheduler resolution routes the work there. No `scheduler_url` option
exists — passing it raises `TypeError` from Options validation.

**Remote-cluster credential distribution** is a core helper, not a plugin
option: `ea.distribute_credentials(env, client=…|cluster=…|scheduler=…)`. It
prefers Coiled's private `cluster.send_private_envs()` channel, refuses a plain
Dask cluster unless `private=False` is passed explicitly (which sends the values
through `client.run`), and returns only the keys it set — never values. The
default `EARTHDATA_TOKEN` comes from the current login. The plugin itself still
never serializes credentials; this prepares worker environments so the cluster
executor can rebuild its registry there.

### 3.6 VCC handling and the writable session (verified against icechunk 2.3.1)

The writer always runs in **write mode** with no exists/open/create branching:

```
repo    = Repository.open_or_create(storage, config=<vcc config>)
session = repo.writable_session("main")
vds.vz.to_icechunk(session.store, append_dim=…)
session.commit("vds create|append")
```

Verified behavior (local experiment, icechunk 2.3.1):

- **Create path:** the passed config — VCCs derived from the delta's refs, one per
  unique `scheme://host/` (`s3://` → region us-west-2 config; `https://` → http
  store config) — is **persisted automatically**; no explicit `save_config()`.
- **Existing repo:** the passed config is **ignored** (stored config wins). The
  append-with-new-hosts fix is therefore explicit and still required:
  `Repository.fetch_config(storage)` → merge missing prefixes → `save_config()` →
  write. (Current `_write_virtual_to_icechunk` never merges — appending granules
  from a new DAAC host would leave undeclared, unreadable containers.)
- **Single-writer rule:** icechunk warns concurrent repo *creation* is unsafe; each
  job submits exactly **one** write task (cluster or client), never per-worker
  creates. One Job = one sequential write.
- **Read authorization is mandatory:** `authorize_virtual_chunk_access=None` is
  deprecated (silently env/anonymous) and icechunk *blocks* chunk access for
  unauthorized containers. Read paths always pass an explicit mapping — the
  existing `_authorize_vccs` flow client-side (§3.5 channel 3), and
  `S3Credentials.FromEnv()` / `s3_anonymous_credentials()` / `HttpAccess`
  sentinels on workers. Keys must match the containers' `url_prefix` exactly.

## 4. Phase 0 — Spikes (in the plugin repo scratch, ~half a day)

1. obstore `Credential` field names → `icechunk.S3StaticCredentials` bridge.
2. Active-`Client` detection from the job thread (`distributed.default_client()`)
   routes virtualizarr's compute to the user's cluster, and `client.compute` yields
   futures for progress/cancel (script against a real `LocalCluster`).
3. Whether `open_virtual_mfdataset` drives a custom executor via `submit` or `map`.
4. tqdm callback ordering under `client.compute`.
5. Worker-side credential resolution: lazy `earthaccess.login(strategy="environment")`
   + per-worker obstore registry build inside the task (LocalCluster with
   `EARTHDATA_USERNAME`/`EARTHDATA_PASSWORD` exported before workers start), plus
   `s3_storage(from_env=True)` / s3fs env resolution on a worker for `s3://` targets.
6. (downgraded from spike) kerchunk engine opening a `*.parquet` refs *directory* —
   already proven by `_load_via_kerchunk`; becomes a ported regression test.

## 5. Phase 1 — Spec branch `vds-contract`

- `job.py`: `Asset.access` Literal += `"file"`; `Format` Literal += `"icechunk"`, `"kerchunk"`.
- `support.py`: `OUTPUT_TYPES["virtual"] = "xarray.Dataset (ManifestArray) or xarray.DataTree"`;
  `Capability.VIRTUALIZE` + `Capability.APPEND` with `CapabilitySpec`s (`case=False`;
  options `access`/`parser`/`concat_dim`/`combine` and `store`/`append_dim`/`format`;
  outputs `{"virtual"}` / `{"assets"}`).
- `testing.py`: `results_kind` branch for `output == "virtual"` (duck-typed
  `hasattr(value, "variables")` — suite stays dependency-free).
- `__init__.py` exports; `pyproject.toml` → `0.3.0`; `CONTRACT_VERSION` unchanged.
- Tests: new members accepted, old untouched; run the three existing plugins'
  conformance against 0.3.0 to prove non-breakage.
- Merge to `main`, tag `v0.3.0`.

## 6. Phase 2 — New repo `earthaccess-plugin-vds`

```
src/earthaccess_vds/
  __init__.py      # exports VDSPlugin
  plugin.py        # VDSPlugin(PluginBase); entry point: vds = "earthaccess_vds:VDSPlugin"
  engine.py        # virtualize + open/combine dispatch + open_refs_store + load round-trip
  parser.py        # parser resolution, DMR++ URL mapping (ObjectStoreRegistry from obspec_utils)
  credentials.py   # serializable registry *config* (URLs, access mode, endpoints — no secrets) + worker-side lazy auth and registry builder
  writer.py        # IcechunkWriter (open_or_create + writable_session + explicit VCC merge on append) + KerchunkParquetWriter (read-combine-rewrite)
  storage.py       # obstore store | URI → icechunk Storage (from_env); explicit store/Storage adapters (client-side only, never serialized)
  progress.py      # ProgressExecutor + VirtualJob
```

**Porting map** (from `earthaccess/virtual/`): `core.py::virtualize` +
`_open_virtual_*` + `_load_via_kerchunk` (parquet round-trip now) → `engine.py`;
`_parser.py` → `parser.py`; `_credentials.py::build_obstore_registry` →
`credentials.py`; `core.py::write_virtual`/`_write_virtual_to_icechunk`/
`_derive_vccs_from_vds` → `writer.py`; `core.py::_open_virtual_via_virtualizarr`
(parquet branch only; **plus `IcechunkParser` for icechunk stores** — both feed
manifest-form reopening for append and `source=` merges) → `engine.open_refs_store`. **Not ported:** `open_virtual*`
(reading stays in core behind `ea.open()` this phase), legacy `dmrpp.py`/`kerchunk.py`.

**Progress:** `ProgressExecutor` (a `concurrent.futures`-compatible Executor
instance passed as `parallel=…` to virtualizarr, used as a context manager):
collects tasks, then detects the active distributed `Client`
(`distributed.default_client()`) — if present, `client.compute` gives real futures
(tqdm callbacks via `show_progress`, dashboard link, cancellable); otherwise local
threaded compute (still per-granule counts). `VirtualJob(Job)`:
`_poll()` aggregates future states → status + `{"opened": n, "total": t,
"failed": k, "phase": "virtualize"|"combine"|"write", "dashboard": link}`;
`_cancel()` = `client.cancel(*futures)` (Job auto-derives the `cancel` capability);
local scheduler = same executor on threads (counts still per-granule).

**Validation:** empty results → ValueError; nested multi-granule without
`concat_dim` → ValueError; `tree` + `load` → ValueError (engine, ported);
existing store + no `append_dim`/`concat_dim` → clear error; `replace` only with
kerchunk; `commit` ignored (documented) for kerchunk.

**Dependencies:** `earthaccess-plugin-spec @ …@v0.3.0`; `earthaccess @
git+…/betolink/earthaccess.git@virtual-plugin` during preview (`>=2.0.0` once
published); `virtualizarr>=2.8`, `obstore>=0.12`,
`icechunk>=2.3`, `xarray`, `dask`, `distributed`. Extras: `kerchunk` =
`kerchunk`, `pandas`, `fastparquet`, `fsspec`, `s3fs`; `test`; `docs`.

**Tests (offline):** port `test_virtual.py` + `test_virtual_write.py` (already fully
mocked — FakeManifestArray etc.); new: storage adapter (S3Store `.config` /
`.credential_provider` extraction), worker-side env-auth registry builder,
**no-secrets test** (`pickle.dumps` of every task payload contains no token/key
material — search the pickled bytes), VCC merge-on-append
(mocked `Repository`), kerchunk append read-combine-rewrite (fake refs store),
`replace=True` rm+rewrite (mocked fsspec), ProgressExecutor/VirtualJob (fake
futures), Options validation (including `scheduler_url`/`write_credentials`
raising `TypeError` as removed options), Asset shape, href stability rules; spec
conformance run against `VDSPlugin` with a fake backend in CI.

## 7. Phase 3 — Fork branch `virtual-plugin`

- `earthaccess/plugins/registry.py`: `_KNOWN` += `("vds", "earthaccess-plugin-vds",
  "Virtual datasets: virtualize granules, create and append icechunk / kerchunk stores")`.
- `earthaccess/api.py::open()`: route a single icechunk-sniffed `str`/`Path`
  (`icechunk://` scheme, `.icechunk` suffix, local dir containing
  `snapshots/1CECHNKREP0F1RSTCMT0`) and `DataCollection` → `open_virtual()`,
  returning a Dataset. Lists of granules/URLs unchanged; `.json`/`.parquet` **not**
  intercepted (kerchunk refs keep going through `open_virtual` — name-suffix
  convention already matches).
- `pyproject.toml`: re-pin spec `@v0.3.0`, refresh `uv.lock`, add optional extra
  `vds = ["earthaccess-plugin-vds @ git+…"]`.

**Decision (2026-11-02): `open_virtual()` stays.** The plugin supersedes only the
**write** half (`virtualize()` + `write_virtual()`). `open_virtual()` remains the
specialized virtual reader (`access=`, `force_external=`,
`authorize_virtual_chunk_access=`, `storage_options=`, `load=False` manifest
form, `DataCollection` URIs), and `ea.open(asset)` is the generic shorthand for
the common case. No deprecation shim is planned for `open_virtual()`.
- Notebooks (§9) + `docs/tutorials/plugins/index.md` row; remove
  `docs/tutorials/icechunk_virtual.ipynb` (superseded), update
  `docs/tutorials/vds/index.md` and `docs/.nav.yml` accordingly.
- Unit tests: `_KNOWN` row; `open()` routing (tmp dir with the snapshots marker,
  mocked `open_virtual`); full existing suite green (incl. untouched
  `test_virtual*.py` — proves non-breakage).

## 8. Phase 4 — Acceptance: 20 SST granules on a 4-worker local cluster

Notebook `docs/tutorials/plugins/vds.ipynb`, mirroring the icechunk_virtual workflow:

1. Export `EARTHDATA_USERNAME`/`EARTHDATA_PASSWORD` **before** starting the cluster
   (proves the no-serialization model); `LocalCluster(n_workers=4, processes=True)`
   + `Client`; dashboard link; `ea.login()` on the client for the searches only.
2. Search batch 1 — 10 MUR SST granules (`MUR-JPL-L4-GLOB-v4.1`, 2013-01-02→01-11).
3. `request(batch1, store="mur_sst", concat_dim="time")` → Asset.
4. `ea.open(asset.href)` → correct time span; store holds manifests not data (size check).
5. Search batch 2 — 10 granules (01-11→01-21).
6. `request(batch2, store=asset.href, concat_dim="time", show_progress=True)` — no
   scheduler kwarg; the active Client is used; mid-flight `job.info()` shows counts +
   phase + dashboard; results → Asset.
7. `ea.open` again → spans both batches; equals the old `virtualize`+`write_virtual` result.
8. Kerchunk cell: `request(batch1, store="mur_sst_refs", format="kerchunk", concat_dim="time")`
   → Asset → `ea.open`; then **append** batch 2 (`format="kerchunk"`, new href) → open.
9. (Optional) throwaway job demonstrating `job.cancel()`.

**Pass criteria:** notebook runs top-to-bottom; progress observed mid-flight; both
formats create+append correctly; the distributed path works with **worker-side env
credentials only** (assert pickled task payloads contain no token/key material);
zero changes to `earthaccess/virtual/`; `ea.list_plugins(details=True)` shows four
plugins, no errors; fork test suite green; spec 0.3.0 does not unload the existing
plugins.

## 9. Notebooks

**Simple (replaces the current virtualization notebook):**
`docs/tutorials/plugins/vds.ipynb` — the §8 workflow with minimal kwargs only.
This is the acceptance artifact.

**In-depth (new):** `docs/tutorials/plugins/vds-in-depth.ipynb` — the details:
parsers (DMR++ vs HDF fallback; kerchunk refs as *inputs*), access modes and when
`direct` works (us-west-2 in-region), `group=`/`combine="by_coords"`/`tree=True`,
`format="kerchunk"` internals (parquet refs dir, fsspec, O(full) append,
`replace=True`), **bring-your-own-cluster and credentials** (active `Client`,
Coiled secrets, `EARTHDATA_*`/AWS env on workers, what earthaccess never
serializes, the missing-env error), progress/cancel/dashboard, VCC authorization
explained (the CORS analogy) + `authorize_virtual_chunk_access` override, Asset
anatomy, `ea.open()` on stores and `DataCollection` virtual collections,
`load=True` round-trip, legacy-API appendix (old calls still work — one comparison
cell; removed only in the deprecation follow-up).

## 10. Dev environment & CI

```bash
cd ~/github/betolink/earthaccess && git checkout plugin-spec && git checkout -b virtual-plugin
# spec tag after Phase 1 merge:
uv pip install --reinstall --refresh "git+https://github.com/betolink/earthaccess-plugin-spec.git@v0.3.0"
uv pip install -e "../earthaccess-plugin-vds[kerchunk,test]"
uv pip install jupyterlab matplotlib "h5netcdf>=1.7" netCDF4
source .venv/bin/activate        # never bare `uv run` (uv.lock lessons)
```

CI: plugin repo — ruff/mypy/pytest offline + spec conformance; notebook/integration
run manual or nightly (network-marked). Fork — existing suite must stay green.

## 11. Sequencing & PRs

1. PR spec `vds-contract` → `main` → tag `v0.3.0`.
2. Repo `earthaccess-plugin-vds`: initial implementation (Phases 0+2).
3. PR fork `virtual-plugin`: registry/open-routing/pins/notebooks (Phase 3).
4. Acceptance run (Phase 4); iterate.
5. Follow-ups (below) as separate PRs.

## 12. Out of scope (follow-ups, gated on acceptance)

- Deprecation shims for `ea.virtualize`/`ea.write_virtual`
  (`DeprecationWarning` → the plugin; removal next minor) + engine deletion from
  core + notebook cutover. `open_virtual()` is kept (specialized reader).
- `from_assets()` composition (harmony/pydap outputs → virtualize into a store).
- Real-S3 store integration runs (local + LocalCluster only this phase).
- `s3://` kerchunk refs writes beyond the fsspec path; `DataCollection` virtual
  collections beyond `ea.open` routing.
- PyPI publishing of the four packages; `earthaccess-dev` org migration.

## 13. Open decisions (flagged, defaults chosen)

1. **`access` default: `"indirect"`** for the plugin (works everywhere; `direct`
   requires us-west-2 in-region) — **differs from core `virtualize()`'s `"direct"`.**
   Confirm or flip.
2. ~~Kerchunk append default~~ — settled: new versioned refs dir, `replace=True` in place.
3. ~~`append_dim` defaults to `concat_dim`~~ — settled.
4. **Write location (new):** an `s3://` target with an active distributed `Client`
   submits the store write **to the cluster**, so target credentials resolve in the
   worker env (matches "obstore/fsspec find the credentials in the workers"); local
   targets and explicit obstore-store/`Storage` writes stay client-side. Confirm.

Removed options (settled this session): `scheduler_url` (cluster setup is the
user's concern — the active `Client` is used automatically; passing it raises
`TypeError`) and `write_credentials` (env-only for `s3://` targets; an explicit
obstore store / icechunk `Storage` is the preconfigured-session escape hatch,
client-side only, never serialized).
