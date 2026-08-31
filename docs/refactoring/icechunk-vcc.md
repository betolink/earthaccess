# Icechunk Virtual Chunk Container (VCC) Support

> **Status: IMPLEMENTED** — this is an architecture reference for the shipped
> VCC support. The original plan (`docs/implementation-plan-icechunk-vcc.md`)
> was fully implemented and removed; this page preserves the design decisions.

## What this is

An Icechunk repository can contain *virtual chunks* — pointers to byte ranges in
external storage (`s3://`, `http(s)://`, `file://`, …). Icechunk is secure by
default: a reader must explicitly authorize each *Virtual Chunk Container* (VCC)
via `Repository.open(..., authorize_virtual_chunk_access={...})`, and the repo
writer must declare the VCCs in the repo config.

`earthaccess` ships two workflows built on this:

- **`open_virtual()`** — opens Icechunk stores (HTTP or S3, from a
  `DataCollection` or a plain URI / local path) and **authorizes VCCs
  automatically**, so stores whose virtual chunks point at NASA buckets/URLs
  resolve without extra user setup.
- **`write_virtual()`** — creates and appends a virtual dataset (the VDS
  returned by `virtualize(..., load=False)`) to a local or `s3://` Icechunk
  store, declaring the VCCs derived from the dataset's chunk references.

Public surface:

```
earthaccess.virtualize(granules, ...)                              # unchanged: returns VDS
earthaccess.write_virtual(vds, store, ..., append_dim=..., ...)    # create/append VDS to icechunk
earthaccess.open_virtual(uri_or_collection, ...,
                         authorize_virtual_chunk_access=...)       # extended: VCC auth
```

`write_virtual` is chainable: it returns the input `vds` unchanged, mirroring
`virtualize()`. The `access` parameter was deliberately dropped from
`write_virtual` — the VCC backend is derived from the scheme of the VDS's own
chunk references (`s3://` vs `https://`), so callers do not need to restate how
the granules were virtualized.

## Credential selection rules

| `url_prefix` | collection present? | credential |
| --- | --- | --- |
| `s3://…/` | yes | `s3_credentials(get_credentials=lambda: S3StaticCredentials(collection.s3_credentials))` |
| `s3://…/` | no | `s3_from_env_credentials()` (fallback `s3_anonymous_credentials()` if `anon=True` in storage_options) |
| `http(s)://…/` | n/a | `HttpAccess` sentinel (+ `http_store(headers=Bearer)` injected into config for NASA hosts) |
| `file://…/` | n/a | `LocalFileSystemAccess` sentinel |
| `vcc://name/` | n/a | resolve `name` → `store` backend → credential as above |

`authorize_virtual_chunk_access` mirrors Icechunk's parameter. When given, it is
**merged over** the auto-detected mapping — `{**auto, **explicit}` — so explicit
keys always win. This is the escape hatch for cases auto-detection can't handle.

## Cached credentials

The EDL token exchange is a network call, so both result types cache their
temporary S3 credentials:

```python
class DataCollection(CustomDict):
    @cached_property
    def s3_credentials(self) -> dict[str, str]:
        return self.get_s3_credentials()

class DataGranule(CustomDict):
    @cached_property
    def s3_credentials(self) -> dict[str, str]:
        endpoint = self.get_s3_credentials_endpoint()
        if not endpoint:
            raise ValueError("No s3credentials endpoint for this granule.")
        return earthaccess.__auth__.get_s3_credentials(endpoint=endpoint)
```

## Key helpers in `earthaccess/virtual/core.py`

- `_icechunk_storage_for_uri(uri, *, storage_options, access, collection, for_write)` —
  builds icechunk `Storage` for a URI/collection (local, `s3://`, NASA `https://`
  with bearer headers, or `redirect_storage` fallback for non-NASA HTTPS).
- `_build_vcc_credentials(vccs, *, collection)` — maps each
  `VirtualChunkContainer.url_prefix` to an icechunk credential.
- `_inject_http_vcc_headers(config, *, token)` — rebuilds each HTTP VCC's store
  with `http_store(headers={"Authorization": "Bearer …"})`.
- `_authorize_vccs(storage, *, collection, explicit)` — `fetch_config` → merges
  explicit overrides over auto-detection.
- `_derive_vccs_from_vds(vds)` — collects `scheme://netloc/` prefixes from the
  VDS `ManifestArray` references (used by the write path).
- `_write_virtual_to_icechunk(vds, store, *, append_dim, commit, storage_options)` —
  opens/creates a writable store, writes (or appends) the VDS, commits.

## icechunk 2.1 API surface relied on

- `Repository.open(storage, config=None, authorize_virtual_chunk_access=None)`.
- `Repository.fetch_config(storage)` → `RepositoryConfig | None`, whose
  `virtual_chunk_containers` is a `dict` keyed by `url_prefix`.
- `http_store(headers=...)` / `http_storage(url, headers=...)` support static
  headers (icechunk PR #2143) — used to inject the EDL bearer token for NASA
  HTTPS VCCs.
- `HttpAccess` / `LocalFileSystemAccess` are no-arg sentinels; `None` is
  deprecated.

A `>=2.1` icechunk floor is required for HTTP VCC header support.

## Notes / constraints

- **Write credentials**: NASA S3 is read-only; writing to `s3://` requires the
  user's own bucket + write creds (env or `storage_options`). Local-store writes
  need no write creds (VCC data is still read via the EDL token).
- **`load=True` + VCCs**: only the `load=False` (VDS) path writes virtual refs;
  `write_virtual` therefore requires a VDS (`ManifestArray`-backed).
- **`s3://` VCC region**: derived from the collection when present, defaults to
  `us-west-2` otherwise. Non-NASA buckets may need an explicit region via
  `authorize_virtual_chunk_access` / `storage_options`.

## Example

See the `icechunk_virtual` tutorial notebook
([docs/tutorials/icechunk_virtual.ipynb](../tutorials/icechunk_virtual.ipynb))
for a full MUR SST create → open → append → re-open workflow.

```python
import earthaccess

earthaccess.login()
granules = earthaccess.search_data(short_name="MUR-JPL-L4-GLOB-v4.1", count=100)

# 1. Virtualize a batch, then write to a local Icechunk store
vds = earthaccess.virtualize(granules, concat_dim="time")
earthaccess.write_virtual(vds, "sst.icechunk")

# 2. Open it back up (VCCs auto-authorized)
ds = earthaccess.open_virtual("sst.icechunk")

# 3. Later: virtualize the next batch and append along time
delta = earthaccess.virtualize(new_granules, concat_dim="time")
earthaccess.write_virtual(delta, "sst.icechunk", append_dim="time")
```
