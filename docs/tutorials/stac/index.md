# STAC tutorials

## Learning goals

In this section you will learn how to use `earthaccess` to work with the
[SpatioTemporal Asset Catalog (STAC)](https://stacspec.org/) representation of
NASA granules, so you can plug the data into the wider STAC ecosystem (e.g.
`odc-stac`, `pystac`, `pystac-client`).

`earthaccess` converts CMR results to STAC objects directly on the result
containers:

- `GranuleResults.to_stac()` → a list of `pystac.Item`
- `DataGranule.to_stac()` → a single `pystac.Item`
- `CollectionResults.to_stac()` / `DataCollection.to_stac()` → a `pystac.Collection`

No separate conversion step is needed — search first, then call `to_stac()` on
the results.

## Tutorials

- [CMR to STAC Semantics](../cmr-to-stac.ipynb) — a focused walkthrough
  of how CMR UMM fields map to STAC 1.0.0 Items and Collections, including
  batch conversion and the STAC → CMR round trip — no external tooling required.
- [STAC Interoperability](../odc-stac-cmr.ipynb) — search HLS granules,
  convert them to STAC items with `GranuleResults.to_stac()`, and load the
  bands with `odc.stac` into an xarray mosaic.

## Try it next

- [STAC API reference](../../api/stac/overview.md) — conversion functions and
  result methods.
- [STAC module architecture](../../refactoring/stac-module-architecture.md) —
  how the CMR → STAC conversion is implemented.
- [Search for data using filters](../../user/howto/search-granules.md) — build
  the CMR search that feeds the STAC conversion.
