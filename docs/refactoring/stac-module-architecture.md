# STAC Module Architecture

Technical deep-dive into `earthaccess.stac`: how CMR UMM ↔ STAC conversion is implemented, why it's designed this way, and how it fits into the rest of earthaccess.

For user-facing "how do I..." documentation, see the [STAC API Reference](../api/stac/overview.md) instead. This document is for contributors and anyone who wants to understand or extend the internals.

---

## Location & Files

```
earthaccess/stac/
├── __init__.py       (25 lines)  - Public API exports
└── converters.py    (863 lines)  - All conversion logic
```

```python
from earthaccess.stac import (
    umm_granule_to_stac_item,
    umm_collection_to_stac_collection,
    stac_item_to_data_granule,
    stac_collection_to_data_collection,
)
```

The module deliberately has **no dependency on pystac**. It works with plain Python dictionaries on both sides (CMR UMM dicts in, STAC-spec-shaped dicts out, and vice versa). This keeps the module lightweight, easy to test, and safe from version drift in third-party STAC libraries — consumers who want pystac objects can wrap the dict output themselves (`pystac.Item.from_dict(...)`).

---

## Public API (4 Functions)

| Function | Direction | Input | Output |
|----------|-----------|-------|--------|
| `umm_granule_to_stac_item()` | CMR → STAC | UMM granule dict | STAC Item dict |
| `umm_collection_to_stac_collection()` | CMR → STAC | UMM collection dict | STAC Collection dict |
| `stac_item_to_data_granule()` | STAC → CMR | STAC Item dict | `DataGranule` |
| `stac_collection_to_data_collection()` | STAC → CMR | STAC Collection dict | `DataCollection` |

These are pure functions: no I/O, no auth, no side effects. That's an intentional design choice (see [Design Principles](#design-principles) below) and is what makes them usable both internally (`DataGranule.to_stac()` calls `umm_granule_to_stac_item()` under the hood) and standalone for batch/offline processing.

---

## Internal Helper Functions

`converters.py` has ~18 private helpers organized by responsibility:

### Temporal extraction
- `_extract_granule_datetime(temporal_extent)` — pulls `datetime`, `start_datetime`, `end_datetime` from a granule's `TemporalExtent`
- `_extract_collection_temporal_extent(temporal_extents)` — builds STAC `extent.temporal.interval` from a collection's `TemporalExtents`

### Spatial extraction
- `_extract_granule_geometry(spatial_extent)` — converts CMR `SpatialExtent` (bounding rectangles, points, polygons) into GeoJSON `geometry` + `bbox`
- `_extract_collection_spatial_extent(spatial_extent)` — same idea at the collection level, producing `extent.spatial.bbox`

### Links & assets
- `_build_granule_links(concept_id, collection_id, provider_id)` — builds the STAC `links` array (self, collection, parent)
- `_build_collection_links(...)` — builds collection-level links (license, related URLs)
- `_build_granule_assets(related_urls)` — converts CMR `RelatedUrls` into STAC `assets`, applying the role-mapping table below
- `_build_collection_providers(data_centers)` — converts CMR `DataCenters` into STAC `providers`

### Reverse conversion utilities (STAC → CMR)
- `_stac_assets_to_related_urls(assets)` — inverse of `_build_granule_assets`
- `_stac_links_to_related_urls(links)` — inverse of link building
- `_stac_providers_to_data_centers(providers)` — inverse of provider mapping
- `_geometry_to_spatial_extent(geometry, bbox)` — inverse of geometry extraction

### Misc
- `_extract_keywords(...)` — pulls searchable keywords for STAC `keywords`

This symmetry (a `_build_x` / `_x_to_related_urls` pair for nearly every field) is what makes round-tripping (CMR → STAC → CMR) preserve data reliably — every forward mapping has a matching reverse mapping right next to it in the file.

---

## Constants & Mapping Tables

```python
STAC_VERSION = "1.0.0"
CMR_API_BASE_URL = "https://cmr.earthdata.nasa.gov"
```

### CMR URL Type → STAC Asset Role

CMR's `RelatedUrls[].Type` field is mapped to STAC's `assets[].roles`:

| CMR `Type` | STAC `roles` |
|------------|--------------|
| `GET DATA` | `["data"]` |
| `GET DATA VIA DIRECT ACCESS` | `["data"]` (cloud/S3 access) |
| `GET RELATED VISUALIZATION` | `["thumbnail"]` |
| `EXTENDED METADATA` | `["metadata"]` |
| `VIEW RELATED INFORMATION` | `["metadata"]` |
| `USE SERVICE API` | `["metadata"]` |
| ... | (9 mappings total) |

### CMR Data Center Role → STAC Provider Role

| CMR role | STAC role |
|----------|-----------|
| `ARCHIVER` | `host` |
| `PROCESSOR` | `processor` |
| `ORIGINATOR` | `producer` |

These tables are the main place to look if a conversion "loses" information — check whether the source field has an entry in the relevant mapping.

---

## Data Flow Diagram

```
                    umm_granule_to_stac_item()
CMR UMM granule  ───────────────────────────────►  STAC Item dict
   (dict)                                              (dict)
       ▲                                                  │
       │                                                  │
       └──────────────────────────────────────────────────┘
                    stac_item_to_data_granule()
                    (produces a DataGranule, not
                     a raw UMM dict — see note below)
```

```
                  umm_collection_to_stac_collection()
CMR UMM collection ─────────────────────────────►  STAC Collection dict
     (dict)                                              (dict)
       ▲                                                    │
       │                                                    │
       └────────────────────────────────────────────────────┘
                 stac_collection_to_data_collection()
```

**Note on asymmetry**: `umm_granule_to_stac_item()` takes and returns raw dicts, but `stac_item_to_data_granule()` returns a `DataGranule` object (not a raw UMM dict). This is intentional — the STAC → CMR direction exists specifically to let external STAC Items be used with earthaccess's higher-level API (`.download()`, `.open()`, `.assets()`), so returning a fully-formed `DataGranule` is more useful than an intermediate dict.

---

## Integration Points

### 1. `DataGranule.to_stac()` / `DataCollection.to_stac()`

Location: `earthaccess/search/results.py`

```python
class DataGranule:
    def to_stac(self) -> dict:
        return umm_granule_to_stac_item(self._umm_dict())
```

These methods are thin wrappers — all the real logic lives in `stac/converters.py`. This keeps `results.py` focused on result-set behavior (pagination, filtering, asset access) rather than format conversion.

### 2. `StacItemQuery`

Location: `earthaccess/search/query/stac_query.py`

A query builder that accepts STAC-native parameters (`collections`, `bbox`, `datetime`, CQL2 `query`) and translates them to CMR search parameters via `to_cmr()`, or passes them through via `to_stac()`. It does **not** use `stac/converters.py` directly — it operates on query parameters, not on result records. The two modules are conceptually related (both bridge CMR and STAC) but are independent code paths.

### 3. `api.py`

`search_data()` and `search_datasets()` accept a `query=` parameter which can be a `GranuleQuery`, `CollectionQuery`, or `StacItemQuery`. Results are always returned as `DataGranule`/`DataCollection` objects, which is where `.to_stac()` becomes available again.

### 4. External STAC ingestion

Nothing in earthaccess automatically discovers external STAC catalogs (no crawler/client for third-party STAC APIs). The `stac_item_to_data_granule()` / `stac_collection_to_data_collection()` functions are the integration seam: bring your own STAC Items (e.g., fetched via `pystac-client` or a raw HTTP request), pass the dict in, get back an object usable with the rest of earthaccess.

---

## Supported STAC Extensions

| Extension | Field | Trigger |
|-----------|-------|---------|
| [EO](https://stac-extensions.github.io/eo/v1.1.0/schema.json) | `properties["eo:cloud_cover"]` | Set when UMM `CloudCover` is present |
| [Scientific](https://stac-extensions.github.io/scientific/v1.0.0/schema.json) | `properties["sci:doi"]` | Set when UMM `DOI` is present |

Extension URIs are only added to `stac_extensions` when the corresponding field is actually populated — empty/absent source data does not produce empty extension declarations.

---

## Worked Example: Granule Round-Trip

```python
from earthaccess.stac import umm_granule_to_stac_item, stac_item_to_data_granule

umm_granule = {
    "umm": {
        "GranuleUR": "SC:ATL08_005_20231015121830_03521001_002.h5",
        "TemporalExtent": {
            "RangeDateTime": {
                "BeginningDateTime": "2023-10-15T12:18:30.000Z",
                "EndingDateTime": "2023-10-15T12:19:45.000Z",
            }
        },
        "SpatialExtent": {
            "HorizontalSpatialDomain": {
                "Geometry": {
                    "BoundingRectangles": [{
                        "WestBoundingCoordinate": -120.5,
                        "SouthBoundingCoordinate": 35.0,
                        "EastBoundingCoordinate": -118.0,
                        "NorthBoundingCoordinate": 37.5,
                    }]
                }
            }
        },
        "RelatedUrls": [
            {"URL": "https://data.example.com/ATL08.h5", "Type": "GET DATA"},
            {"URL": "s3://bucket/ATL08.h5", "Type": "GET DATA VIA DIRECT ACCESS"},
        ],
        "CloudCover": 15.5,
    },
    "meta": {
        "concept-id": "G1234567890-NSIDC_ECS",
        "native-id": "ATL08_005_20231015",
        "collection-concept-id": "C1234-NSIDC_ECS",
        "provider-id": "NSIDC_ECS",
    },
}

# CMR -> STAC
stac_item = umm_granule_to_stac_item(umm_granule)
assert stac_item["type"] == "Feature"
assert stac_item["properties"]["eo:cloud_cover"] == 15.5
assert stac_item["bbox"] == [-120.5, 35.0, -118.0, 37.5]

# STAC -> DataGranule (round trip, e.g. from an external catalog)
granule = stac_item_to_data_granule(stac_item, cloud_hosted=True)
# granule now works with the rest of the earthaccess API:
# granule.download(path="./data")
# granule.open()
```

---

## Design Principles

1. **Pure functions, dict in / dict out** (for the CMR→STAC direction) — no auth, no network calls, no hidden state. Trivial to unit test, trivial to batch.
2. **No hard pystac dependency** — avoids version coupling; consumers opt into pystac only if they want typed objects.
3. **Bidirectional by construction** — every forward mapping has an explicit inverse helper, so round-tripping is a first-class concern, not an afterthought.
4. **Extensible via STAC extensions** — new extensions (e.g., a future `proj` or `sat` extension) can be added by extending the relevant `_build_*` helper and appending to `stac_extensions` conditionally.
5. **Circular-import safe** — `converters.py` uses `TYPE_CHECKING` guards for `DataGranule`/`DataCollection` type hints so `earthaccess.stac` can be imported without pulling in `earthaccess.search` eagerly.

---

## Testing

- **File**: `tests/unit/test_stac_converters.py`
- **Coverage**: 44 tests
- **Categories**:
  - Forward conversion (UMM → STAC) for granules and collections
  - Reverse conversion (STAC → UMM/DataGranule/DataCollection)
  - Round-trip preservation (convert then convert back, compare key fields)
  - Extension handling (cloud cover present/absent, DOI present/absent)
  - Edge cases (missing optional fields, malformed geometry)

To add a new mapping (e.g., a new CMR URL type), add a test fixture with that URL type, assert on the resulting `roles`, and add the mapping to `CMR_URL_TYPE_TO_STAC_ROLE` in `converters.py`.

---

## Extending the Module

**To add a new STAC extension**:
1. Add extraction logic in a new or existing helper (e.g., `_extract_granule_projection`)
2. Set the relevant `properties["proj:*"]` field only when source data exists
3. Conditionally append the extension URI to `stac_extensions`
4. Add the reverse mapping if the field should round-trip
5. Add test fixtures and assertions to `test_stac_converters.py`

**To add a new CMR URL type mapping**:
1. Add an entry to `CMR_URL_TYPE_TO_STAC_ROLE` in `converters.py`
2. Add a test case with a `RelatedUrls` entry of that `Type`

**To support a new STAC field on import (STAC → CMR)**:
1. Add extraction in `stac_item_to_data_granule()` / `stac_collection_to_data_collection()`
2. Map it to the appropriate UMM field
3. Add round-trip test coverage

---

## Related Documentation

- [STAC API Reference (user-facing)](../api/stac/overview.md) — how to use `.to_stac()`, `StacItemQuery`, and the converters
- [STAC Implementation Log](./stac-implementation-log.md) — historical record of how this module was built (Phases 1–8)
- [Architecture Comparison](./stac-comparison.md) — design tradeoffs considered during development
- [Refactoring Overview](./../refactoring.md) — hub linking all architecture documentation
- [STAC Specification](https://stacspec.org/)
