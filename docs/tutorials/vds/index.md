# Virtual datasets (VDS) tutorials

## Learning goals

A *virtual dataset* (VDS) is an xarray Dataset whose variables reference bytes
in remote files instead of copying them. `earthaccess` builds these with
`virtualize()`, so you can open, combine, and analyze many NASA granules as one
logical dataset without downloading them.

In this section you will learn to:

- `virtualize()` a set of granules into a single virtual dataset
  (optionally concatenating along a dimension).
- Combine non-concatenatable granules (`combine="by_coords"`) and return
  `xr.DataTree` objects (`tree=True`).
- Persist and reopen a virtual dataset with Icechunk (`write_virtual` +
  `open_virtual`), including virtual chunk container (VCC) authorization.

## Prerequisites

- `pip install "earthaccess[virtualizarr]"` (or the full dev extras).
- `pip install "earthaccess[stac]"` for the STAC-backed workflows.
- An authenticated session: `earthaccess.login()`.

## Tutorials

- [VDS in depth](../plugins/vds-in-depth.ipynb) — compose with Harmony output,
  merge stores with `source=`, inspect the manifest form, and run on a Dask
  cluster with credential delivery.
- [Virtual stores with the vds plugin](../plugins/vds.ipynb) — create a local
  Icechunk or kerchunk-parquet store from granules with one request, watch the
  job's progress, append new data along `time`, and reopen the store with
  `ea.open()`.
- [VDS Strategies](../virtualize_combine_tree.ipynb) —
  align granules by coordinates (`combine="by_coords"`), virtualize a single
  file, and return an `xr.DataTree` (`tree=True`).
- [Cloud optimized access to TEMPO data](../../user/tutorials/virtual_dataset_tutorial_with_TEMPO_Level3.ipynb) —
  open TEMPO Level 3 granules as a virtual multifile dataset and analyze them.
- [DMRPP + VirtualiZarr](../../user/tutorials/dmrpp-virtualizarr.ipynb) —
  virtualize DMR++ granule references and save a kerchunk reference file.

## Try it next

- [Icechunk virtual chunk containers](../../refactoring/icechunk-vcc.md) — the
  VCC credential model behind `write_virtual()` / `open_virtual()`.
- [API reference](../../api/index.md) — `virtualize()`, `write_virtual()`, and
  `open_virtual()` under `earthaccess.virtual`.
