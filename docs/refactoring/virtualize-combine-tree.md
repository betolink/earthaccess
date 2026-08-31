# Virtualize: combine strategies, single files, and DataTrees

> **Status: IMPLEMENTED** — architecture reference for the shipped
> `virtualize()` capabilities. The original plan
> (`docs/implementation-plan-virtualize-combine-tree.md`) was fully implemented
> and removed; this page preserves the design decisions.

## What this covers

`earthaccess.virtualize()` turns a list of granules into a virtual xarray
Dataset (`load=False`, default) or a concrete lazy Dataset (`load=True`). The
shipped version extends that base with three capabilities:

1. **Combine strategies** — `combine="nested"` (default, requires `concat_dim`)
   or `combine="by_coords"` (aligns granules on shared coordinates, no
   `concat_dim` needed).
2. **Single-file shortcut** — `virtualize([granule])` opens the one URL directly
   via `vz.open_virtual_dataset`, forwarding `drop_variables` and
   `loadable_variables`.
3. **DataTree output** — `virtualize([granule], tree=True)` returns an
   `xr.DataTree` via `vz.open_virtual_datatree`.

## Signature (as shipped)

```python
def virtualize(
    granules: list[earthaccess.DataGranule],
    *,
    access: AccessType = "indirect",
    load: bool = False,
    group: str = "/",
    concat_dim: str | None = None,
    combine: CombineType = "nested",
    preprocess: Callable[[xr.Dataset], xr.Dataset] | None = None,
    data_vars: DataVarsType = "all",
    coords: str = "different",
    compat: CompatType = "no_conflicts",
    combine_attrs: CombineAttrsType = "drop_conflicts",
    join: JoinType = "outer",
    loadable_variables: list[str] | None = None,
    drop_variables: list[str] | None = None,
    parallel: ParallelType = "dask",
    parser: ParserType = "DMRPPParser",
    reference_dir: str | None = None,
    reference_format: ReferenceFormatType = "json",
    tree: bool = False,
    **xr_combine_kwargs: Any,
) -> xr.Dataset | xr.DataTree:
```

New parameters:

| Parameter | Type | Meaning |
| --- | --- | --- |
| `combine` | `"nested"` (default) \| `"by_coords"` | How granules are combined. `"nested"` preserves the original behavior; `"by_coords"` aligns on shared coordinates and does not require `concat_dim`. |
| `join` | `JoinType` (`"outer"` default) | Forwarded to `open_virtual_mfdataset`; applies to `combine="by_coords"`. |
| `loadable_variables` | `list[str] \| None` | Variables to load eagerly (real arrays) instead of as virtual references. Use for index coordinates you need to slice. |
| `drop_variables` | `list[str] \| None` | Variables to omit from the opened dataset. |
| `tree` | `bool` (`False`) | Return an `xr.DataTree` via `open_virtual_datatree`. Single granule only; incompatible with `load=True`. |

`CombineType` / `JoinType` are defined in `earthaccess/virtual/_types.py`.

## Dispatch & validation rules (as shipped)

1. `len(granules) == 0` → `ValueError` (unchanged).
2. `tree=True`:
   - `len(granules) != 1` → `ValueError` ("`tree=True` requires exactly one granule").
   - `load=True` → `ValueError` ("`tree=True` is only supported with `load=False`").
3. `combine="nested"` and `len(granules) > 1` and `concat_dim is None` →
   `ValueError` (unchanged).
4. `combine="by_coords"` and `concat_dim is not None` → `ValueError`
   (mirrors virtualizarr).
5. `len(granules) == 1` (and not `tree`) → short-circuit to
   `open_virtual_dataset` (single URL); `preprocess` applied manually if given.
6. Otherwise → `open_virtual_mfdataset` with `combine` / `join` /
   `loadable_variables` / `drop_variables` forwarded.

`load=True` continues to run the kerchunk round-trip (`_load_via_kerchunk`) on
the resulting **Dataset** only — never on a DataTree.

## Key helpers

- `_open_virtual_dataset_single(url, parser, registry, *, preprocess, drop_variables, loadable_variables, **kwargs)` —
  single-URL open, applies `preprocess` when given.
- `_open_virtual_datatree(url, parser, registry, *, loadable_variables, **kwargs)` —
  `vz.open_virtual_datatree` for `tree=True`.
- `_open_virtual_mfdataset(...)` — multi-file open with `combine` / `join`
  forwarding.

## Notes / constraints

- **Synthetic index**: for non-concatenatable granules, use
  `preprocess` to add a coordinate (e.g. a `time` value derived from the
  granule) together with `loadable_variables=["time"]` so the index is
  sliceable after virtualizing. See the `virtualize_combine_tree` notebook.
- **DataTree**: only single-granule, `load=False`.
- **`load=True`**: only on Datasets, via the kerchunk round-trip.

## Example

See the `virtualize_combine_tree` tutorial notebook
([docs/tutorials/virtualize_combine_tree.ipynb](../tutorials/virtualize_combine_tree.ipynb)):

```python
import earthaccess

earthaccess.login()
sst_granules = earthaccess.search_data(
    short_name="MUR-JPL-L4-GLOB-v4.1",
    temporal=("2024-01-01", "2024-01-10"),
    count=10,
)

# Combine non-concatenatable granules by their shared coordinates
vds = earthaccess.virtualize(sst_granules, combine="by_coords")

# Or align along a synthetic time index
vds = earthaccess.virtualize(
    sst_granules,
    combine="nested",
    concat_dim="time",
    preprocess=add_time_index,
    loadable_variables=["time"],
)

# Single granule -> DataTree
tree = earthaccess.virtualize([sst_granules[0]], tree=True)
```
