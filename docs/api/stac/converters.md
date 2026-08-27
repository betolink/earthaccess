# STAC Conversion Functions

Complete reference for bidirectional conversion between CMR UMM and STAC formats.

## Module: `earthaccess.stac.converters`

All conversion functions are in `earthaccess.stac.converters` and available via:

```python
from earthaccess.stac import umm_granule_to_stac_item, stac_item_to_data_granule
# or
from earthaccess.stac.converters import umm_granule_to_stac_item
```

---

## CMR → STAC (Export)

### `umm_granule_to_stac_item()`

Convert a CMR UMM granule to a STAC Item dictionary.

**Signature**:
```python
def umm_granule_to_stac_item(
    umm_granule: Dict[str, Any],
    collection_id: Optional[str] = None
) -> Dict[str, Any]:
```

**Parameters**:
- `umm_granule` (dict): CMR UMM granule with keys `"umm"` and `"meta"`
- `collection_id` (str, optional): Parent collection ID. If not provided, extracted from granule metadata

**Returns**: STAC Item dictionary (valid against STAC 1.0.0 schema)

**Example**:
```python
# Using results from earthaccess.search_data()
granules = earthaccess.search_data(short_name="ATL06", count=1)
granule_dict = granules[0].umm  # Raw UMM dictionary

# Convert to STAC
stac_item = umm_granule_to_stac_item(granule_dict)

print(f"ID: {stac_item['id']}")
print(f"Cloud cover: {stac_item['properties'].get('eo:cloud_cover')}")
print(f"Assets: {list(stac_item['assets'].keys())}")
```

**What Gets Converted**:

| CMR UMM Field | STAC Field | Notes |
|---------------|-----------|-------|
| `GranuleUR` | `properties.granule_ur` | Granule unique identifier |
| `TemporalExtent` | `properties.datetime`, `start_datetime`, `end_datetime` | Temporal coverage |
| `SpatialExtent` | `geometry`, `bbox` | Geographic extent (GeoJSON) |
| `RelatedUrls` | `assets` | Data files with roles (data, metadata, thumbnail) |
| `CloudCover` | `properties.eo:cloud_cover` | Cloud cover percentage (if available) |
| `DOI` | `properties.sci:doi` | Digital Object Identifier (if available) |
| Concept ID | `properties.cmr:concept_id` | CMR metadata identifier |

**Output Example**:
```json
{
  "type": "Feature",
  "stac_version": "1.0.0",
  "stac_extensions": ["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
  "id": "ATL06_20231015_native_id",
  "geometry": {
    "type": "Polygon",
    "coordinates": [[[-120.5, 35], [-118, 35], [-118, 37.5], [-120.5, 37.5], [-120.5, 35]]]
  },
  "bbox": [-120.5, 35, -118, 37.5],
  "properties": {
    "datetime": "2023-10-15T12:18:30Z",
    "eo:cloud_cover": 15.5,
    "cmr:concept_id": "G1234567890-NSIDC"
  },
  "assets": {
    "data": {
      "href": "https://data.example.com/ATL06.h5",
      "type": "application/x-hdf5",
      "roles": ["data"]
    }
  }
}
```

---

### `umm_collection_to_stac_collection()`

Convert a CMR UMM collection to a STAC Collection dictionary.

**Signature**:
```python
def umm_collection_to_stac_collection(
    umm_collection: Dict[str, Any]
) -> Dict[str, Any]:
```

**Parameters**:
- `umm_collection` (dict): CMR UMM collection with keys `"umm"` and `"meta"`

**Returns**: STAC Collection dictionary (valid against STAC 1.0.0 schema)

**Example**:
```python
# Using results from earthaccess.search_datasets()
collections = earthaccess.search_datasets(keyword="sea level", count=1)
collection_dict = collections[0].umm  # Raw UMM dictionary

# Convert to STAC
stac_collection = umm_collection_to_stac_collection(collection_dict)

print(f"Collection: {stac_collection['id']}")
print(f"Title: {stac_collection['title']}")
print(f"Description: {stac_collection['description']}")
print(f"Temporal extent: {stac_collection['extent']['temporal']}")
```

**What Gets Converted**:

| CMR UMM Field | STAC Field | Notes |
|---------------|-----------|-------|
| `ShortName` | `id` | Collection identifier |
| `LongName` | `title` | Human-readable title |
| `Abstract` | `description` | Detailed description |
| `TemporalExtents` | `extent.temporal` | Temporal coverage |
| `SpatialExtent` | `extent.spatial` | Spatial coverage |
| `DataCenters` | `providers` | Organization information |
| `RelatedUrls` | `links` | Documentation, license, etc. |
| `DOI` | `sci:doi` | Digital Object Identifier |

**Output Example**:
```json
{
  "type": "Collection",
  "stac_version": "1.0.0",
  "id": "ATL06",
  "description": "Global Geolocated Photon Data from ATLAS",
  "links": [
    {
      "rel": "parent",
      "href": "../catalog.json"
    }
  ],
  "extent": {
    "spatial": {
      "bbox": [[-180, -90, 180, 90]]
    },
    "temporal": {
      "interval": [["2018-10-14T00:00:00Z", null]]
    }
  },
  "license": "proprietary",
  "providers": [...]
}
```

---

## STAC → CMR (Import)

### `stac_item_to_data_granule()`

Convert an external STAC Item to an earthaccess `DataGranule`.

This enables using STAC Items from other catalogs with earthaccess methods like `download()` and `open()`.

**Signature**:
```python
def stac_item_to_data_granule(
    item: Dict[str, Any],
    cloud_hosted: bool = False
) -> DataGranule:
```

**Parameters**:
- `item` (dict): STAC Item dictionary
- `cloud_hosted` (bool): Whether the granule is cloud-hosted (S3). Affects download behavior

**Returns**: `DataGranule` object compatible with earthaccess APIs

**Example**:
```python
import json
from earthaccess.stac import stac_item_to_data_granule

# Load STAC Item from external source
with open("stac_item.json") as f:
    stac_item = json.load(f)

# Convert to earthaccess DataGranule
granule = stac_item_to_data_granule(stac_item, cloud_hosted=True)

# Use with earthaccess methods
print(f"Granule ID: {granule.id()}")
print(f"Files: {granule.file_links()}")

# Download the granule
files = granule.download(path="./data")

# Or open as file-like objects
datasets = granule.open()
```

**What Gets Converted**:

| STAC Field | DataGranule Field | Notes |
|------------|-------------------|-------|
| `id` | `properties['id']` | Granule identifier |
| `properties.datetime` | `TemporalExtent` | Temporal coverage |
| `geometry` | `SpatialExtent` | Geographic extent |
| `bbox` | `SpatialExtent` | Bounding box |
| `assets` | `RelatedUrls` | Data files with roles |
| `properties.eo:cloud_cover` | `CloudCover` | Cloud cover percentage |
| `properties.sci:doi` | `DOI` | Digital Object Identifier |
| `collection` | Collection reference | Parent collection ID |

**Notes**:
- The resulting `DataGranule` is independent of CMR and works offline
- All earthaccess methods (download, open, etc.) work with converted granules
- Use `cloud_hosted=True` for S3-based assets to enable direct cloud access

---

### `stac_collection_to_data_collection()`

Convert an external STAC Collection to an earthaccess `DataCollection`.

**Signature**:
```python
def stac_collection_to_data_collection(
    collection: Dict[str, Any],
    cloud_hosted: bool = False
) -> DataCollection:
```

**Parameters**:
- `collection` (dict): STAC Collection dictionary
- `cloud_hosted` (bool): Whether granules in this collection are cloud-hosted

**Returns**: `DataCollection` object compatible with earthaccess APIs

**Example**:
```python
import json
from earthaccess.stac import stac_collection_to_data_collection

# Load STAC Collection from external source
with open("stac_collection.json") as f:
    stac_collection = json.load(f)

# Convert to earthaccess DataCollection
collection = stac_collection_to_data_collection(stac_collection)

# Use with earthaccess methods
print(f"Collection: {collection.id()}")
print(f"Title: {collection['umm']['LongName']}")
print(f"Description: {collection['umm']['Abstract']}")
```

---

## Helper Functions

earthaccess includes several helper functions for working with STAC and CMR data. These are typically used internally but can be useful for custom processing:

### `_extract_granule_datetime()`
Extract temporal information from CMR granule.

### `_extract_granule_geometry()`
Extract and validate GeoJSON geometry from CMR granule.

### `_build_granule_assets()`
Convert CMR RelatedUrls to STAC assets with proper role mapping.

### `_extract_collection_temporal_extent()`
Extract temporal bounds from CMR collection.

### `_extract_collection_spatial_extent()`
Extract spatial bounds from CMR collection.

For complete details on helper functions, see the source code in `earthaccess/stac/converters.py`.

---

## STAC Specification Compliance

All generated STAC Items and Collections comply with:
- **STAC Version**: 1.0.0
- **Supported Extensions**:
  - [EO Extension](https://stac-extensions.github.io/eo/v1.1.0/schema.json) - For cloud cover
  - [Scientific Extension](https://stac-extensions.github.io/scientific/v1.0.0/schema.json) - For DOI

---

## Error Handling

All conversion functions may raise exceptions:

| Exception | Cause |
|-----------|-------|
| `ValueError` | Invalid input structure or missing required fields |
| `KeyError` | Missing expected dictionary keys |
| `TypeError` | Input not a dictionary or wrong type |

Example:
```python
from earthaccess.stac import umm_granule_to_stac_item

try:
    stac_item = umm_granule_to_stac_item(invalid_granule)
except (ValueError, KeyError) as e:
    print(f"Conversion failed: {e}")
```

---

## Performance Considerations

- **Batch Processing**: For converting many granules, use list comprehensions or generators:
  ```python
  stac_items = [umm_granule_to_stac_item(g.umm) for g in granules]
  ```

- **Memory**: Granule UMM dictionaries can be large (100KB+). For bulk conversions, process in chunks.

- **Disk I/O**: When writing STAC Items to disk, use `json.dump()` instead of `str()` for better performance.

---

## See Also

- [STAC Module Architecture](../../refactoring/stac-module-architecture.md) - Internal design details
- [STAC Specification](https://stacspec.org/) - Official STAC spec
- [PySTAC Documentation](https://pystac.readthedocs.io/) - Python STAC library
