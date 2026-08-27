# STAC Conversion Methods on Results

Convert search results to STAC format using methods on `DataGranule` and `DataCollection`.

## `DataGranule.to_stac()`

Convert a granule to STAC Item format.

**Signature**:
```python
def to_stac(self) -> Dict[str, Any]:
    """Convert granule to STAC Item dictionary."""
```

**Returns**: Dictionary representing a STAC 1.0.0 Item

**Example**:
```python
import earthaccess

# Search for granules
granules = earthaccess.search_data(
    short_name="ATL06",
    temporal=("2023-01", "2023-02"),
    count=1
)

# Convert first granule to STAC
if granules:
    granule = granules[0]
    stac_item = granule.to_stac()
    
    print(f"ID: {stac_item['id']}")
    print(f"Geometry: {stac_item['geometry']}")
    print(f"Properties: {stac_item['properties']}")
    print(f"Assets: {stac_item['assets']}")
```

**Output Structure**:
```python
{
    "type": "Feature",
    "stac_version": "1.0.0",
    "stac_extensions": ["https://stac-extensions.github.io/eo/v1.1.0/schema.json"],
    "id": "ATL06_20231015_native_id",
    "geometry": {
        "type": "Polygon",
        "coordinates": [...]
    },
    "bbox": [-120.5, 35, -118, 37.5],
    "properties": {
        "datetime": "2023-10-15T12:18:30Z",
        "start_datetime": "2023-10-15T12:18:30Z",
        "end_datetime": "2023-10-15T12:19:45Z",
        "eo:cloud_cover": 15.5,
        "cmr:concept_id": "G1234567890-NSIDC",
        "cmr:collection_concept_id": "C1234-NSIDC"
    },
    "links": [
        {
            "rel": "collection",
            "href": "https://cmr.earthdata.nasa.gov/...",
            "type": "application/json"
        }
    ],
    "assets": {
        "data": {
            "href": "https://data.example.com/ATL06.h5",
            "type": "application/x-hdf5",
            "title": "Data file",
            "roles": ["data"]
        }
    },
    "collection": "C1234-NSIDC"
}
```

**Included Information**:
- **Temporal**: start/end datetime
- **Spatial**: geometry and bounding box
- **Assets**: all data files with roles
- **Cloud cover**: if available (EO extension)
- **DOI**: if available (Scientific extension)
- **CMR metadata**: concept IDs and provider info

**Use Cases**:
- Export granules to STAC format for external tools
- Build STAC catalogs from NASA data
- Integrate with STAC-based workflows
- Store metadata in STAC JSON format

---

## `DataCollection.to_stac()`

Convert a collection to STAC Collection format.

**Signature**:
```python
def to_stac(self) -> Dict[str, Any]:
    """Convert collection to STAC Collection dictionary."""
```

**Returns**: Dictionary representing a STAC 1.0.0 Collection

**Example**:
```python
import earthaccess

# Search for collections
collections = earthaccess.search_datasets(
    keyword="sea level",
    count=1
)

# Convert first collection to STAC
if collections:
    collection = collections[0]
    stac_collection = collection.to_stac()
    
    print(f"ID: {stac_collection['id']}")
    print(f"Title: {stac_collection['title']}")
    print(f"Description: {stac_collection['description']}")
    print(f"Spatial extent: {stac_collection['extent']['spatial']}")
    print(f"Temporal extent: {stac_collection['extent']['temporal']}")
```

**Output Structure**:
```python
{
    "type": "Collection",
    "stac_version": "1.0.0",
    "stac_extensions": [],
    "id": "ATL06",
    "description": "Global Geolocated Photon Data from ATLAS",
    "title": "ATLAS/ICESat-2 L2A Global Geolocated Photon Data",
    "license": "proprietary",
    "extent": {
        "spatial": {
            "bbox": [[-180, -90, 180, 90]]
        },
        "temporal": {
            "interval": [["2018-10-14T00:00:00Z", None]]
        }
    },
    "links": [
        {
            "rel": "license",
            "href": "https://...",
            "type": "text/html"
        }
    ],
    "providers": [
        {
            "name": "NSIDC ECS",
            "roles": ["producer"],
            "url": "https://nsidc.org/"
        }
    ],
    "summaries": {}
}
```

**Included Information**:
- **Identification**: ID, title, description
- **Extent**: spatial and temporal bounds
- **Providers**: organizations that created/host the data
- **Links**: documentation, license, related URLs
- **DOI**: if available via providers

**Use Cases**:
- Create STAC catalogs for NASA datasets
- Document dataset metadata in STAC format
- Cross-link NASA and external data catalogs
- Build discovery interfaces

---

## Batch Conversion Examples

### Convert All Search Results

```python
import earthaccess

# Search for granules
granules = earthaccess.search_data(
    short_name="ATL06",
    temporal=("2023-01", "2023-02")
)

# Convert all to STAC Items
stac_items = [granule.to_stac() for granule in granules]

print(f"Converted {len(stac_items)} granules to STAC")
```

### Export to STAC GeoJSON

```python
import json
import earthaccess

# Search and convert
granules = earthaccess.search_data(short_name="ATL06", count=10)
stac_items = [g.to_stac() for g in granules]

# Create FeatureCollection
feature_collection = {
    "type": "FeatureCollection",
    "features": stac_items
}

# Save to file
with open("stac_catalog.geojson", "w") as f:
    json.dump(feature_collection, f, indent=2)
```

### Build STAC Catalog

```python
import json
import earthaccess

# Search for collection and granules
collections = earthaccess.search_datasets(keyword="ICESAT", count=1)
granules = earthaccess.search_data(
    short_name="ATL06",
    temporal=("2023-01", "2023-02"),
    count=100
)

# Create STAC catalog structure
catalog = {
    "type": "Catalog",
    "stac_version": "1.0.0",
    "id": "nasa-icesat",
    "description": "ICESAT-2 Data Catalog",
    "links": [
        {
            "rel": "child",
            "href": "./collection.json",
            "title": "ICESAT-2 Collection"
        }
    ]
}

# Create collection
collection_stac = collections[0].to_stac()
collection_stac["links"] = [
    {"rel": "parent", "href": "./catalog.json"}
] + [
    {
        "rel": "item",
        "href": f"./items/{i:06d}.json"
    } for i in range(len(granules))
]

# Create items
items = [g.to_stac() for g in granules]

# Save all files
with open("catalog.json", "w") as f:
    json.dump(catalog, f, indent=2)

with open("collection.json", "w") as f:
    json.dump(collection_stac, f, indent=2)

# Save individual items
import os
os.makedirs("items", exist_ok=True)
for i, item in enumerate(items):
    with open(f"items/{i:06d}.json", "w") as f:
        json.dump(item, f, indent=2)
```

---

## Differences: `to_stac()` vs Direct Conversion

You can convert granules two ways:

### Method 1: Using `DataGranule.to_stac()`

```python
granule = earthaccess.search_data(short_name="ATL06", count=1)[0]
stac_item = granule.to_stac()
```

**Pros**:
- Simple, one-line conversion
- Automatic field extraction
- Uses internal UMM representation

**Cons**:
- Requires DataGranule object (not raw UMM dict)

---

### Method 2: Using Converter Functions

```python
from earthaccess.stac import umm_granule_to_stac_item

granule = earthaccess.search_data(short_name="ATL06", count=1)[0]
stac_item = umm_granule_to_stac_item(granule.umm)
```

**Pros**:
- Works with raw UMM dictionaries
- More control over conversion parameters
- Better for batch processing raw CMR responses

**Cons**:
- More verbose
- Requires knowledge of UMM structure

---

## Performance Considerations

- **Memory**: STAC Items can be 50-200KB each. For large conversions, stream to disk
- **Speed**: Conversion is fast (~1ms per item) but search is slower
- **Serialization**: Use `json.dump()` instead of `str()` for better performance

**Example: Stream Large Results**:
```python
import json
import earthaccess

granules = earthaccess.search_data(
    short_name="ATL06",
    temporal=("2023-01", "2023-02")
)

with open("stac_items.jsonl", "w") as f:
    for granule in granules:
        stac_item = granule.to_stac()
        f.write(json.dumps(stac_item) + "\n")  # JSONL format
```

---

## Troubleshooting

**Issue**: Missing assets in STAC Item
- **Cause**: Granule has no RelatedUrls in CMR
- **Solution**: Check if granule has data files in CMR metadata

**Issue**: Invalid datetime in STAC Item
- **Cause**: Granule has malformed TemporalExtent in CMR
- **Solution**: Check CMR metadata for granule

**Issue**: STAC Item doesn't validate
- **Cause**: Missing required STAC fields
- **Solution**: Use PySTAC library to validate: `Item.from_dict(stac_item).validate()`

---

## See Also

- [STAC Converter Functions](./converters.md) - Low-level conversion APIs
- [STAC Module Architecture](../../refactoring/stac-module-architecture.md) - Internal design
- [STAC Specification](https://stacspec.org/) - Official specification
- [PySTAC Library](https://pystac.readthedocs.io/) - Python STAC implementation
