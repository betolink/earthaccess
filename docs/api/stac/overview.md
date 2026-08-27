# STAC Integration Overview

earthaccess provides seamless bidirectional conversion between NASA's CMR (Common Metadata Repository) format and STAC (SpatioTemporal Asset Catalog) format, enabling interoperability with the broader geospatial ecosystem.

## What is STAC?

STAC is an open standard for describing geospatial data and spatiotemporal assets. It uses a common language for cataloging diverse datasets across different repositories and enables:

- **Discovery**: Search across multiple catalogs with consistent APIs
- **Interoperability**: Use data from any STAC-compliant catalog with earthaccess tools
- **Standardization**: Common metadata structure across NASA and non-NASA data sources

**Resources**:
- [STAC Specification](https://stacspec.org/)
- [STAC Extensions](https://stac-extensions.github.io/)
- [STAC Browser](https://stacindex.org/)

## Quick Start: Converting to STAC

### Converting Search Results to STAC Format

```python
import earthaccess

# Search for granules
granules = earthaccess.search_data(
    short_name="ATL06",
    temporal=("2023-01", "2023-02"),
    count=10
)

# Convert granules to STAC Items
for granule in granules:
    stac_item = granule.to_stac()
    print(f"STAC Item ID: {stac_item['id']}")
    print(f"Assets: {list(stac_item['assets'].keys())}")
```

### Converting Collections to STAC Format

```python
# Search for collections
collections = earthaccess.search_datasets(
    keyword="sea level",
    count=5
)

# Convert collections to STAC Collections
for collection in collections:
    stac_collection = collection.to_stac()
    print(f"Collection: {stac_collection['id']}")
    print(f"Description: {stac_collection['description']}")
```

## Using STAC Queries

earthaccess supports STAC-native query construction via `StacItemQuery`:

```python
from earthaccess import StacItemQuery

# Build STAC query with STAC-native parameters
query = (
    StacItemQuery()
    .collections("C1234567890-NSIDC")
    .bbox([-120, 35, -118, 37])
    .datetime("2023-01-01T00:00:00Z", "2023-02-01T00:00:00Z")
)

# Execute query
earthaccess.login()
results = earthaccess.search_data(query=query)
```

## Integrating External STAC Catalogs

Import STAC Items from other catalogs and use them with earthaccess:

```python
import json
from earthaccess.stac import stac_item_to_data_granule

# Load STAC Item from file or external source
with open("external_stac_item.json") as f:
    stac_item = json.load(f)

# Convert to earthaccess DataGranule
granule = stac_item_to_data_granule(stac_item, cloud_hosted=True)

# Now use it with earthaccess methods
files = granule.download(path="./data")
datasets = granule.open()
```

## Supported STAC Extensions

earthaccess STAC conversion includes support for:

| Extension | Purpose | Example |
|-----------|---------|---------|
| **EO (Electro-Optical)** | Cloud cover and sensor metadata | `eo:cloud_cover: 15.5` |
| **Scientific** | DOI and citation information | `sci:doi: "10.5067/ATLAS/ATL06.005"` |

## STAC vs CMR: Key Differences

| Aspect | STAC | CMR |
|--------|------|-----|
| **Purpose** | General geospatial asset catalog | NASA-specific metadata repository |
| **Structure** | Collections → Items (assets) | Collections → Granules (files) |
| **Discovery** | Web-based (HTTP APIs) | API-only (CMR endpoints) |
| **Extensions** | Plugin ecosystem | Fixed schema (UMM) |
| **Use Cases** | Multi-source discovery, cloud workflows | NASA data-specific queries |

## API Reference

For detailed API documentation, see:

- [Conversion Functions](./converters.md) - `umm_granule_to_stac_item()`, `stac_item_to_data_granule()`, etc.
- [Query Builders](./queries.md) - `StacItemQuery` for STAC-native queries
- [Result Methods](./methods.md) - `.to_stac()` on DataGranule and DataCollection

## See Also

- [STAC Module Architecture](../../refactoring/stac-module-architecture.md) - Complete technical guide
- [Migration Guide](../../migration-guide.md) - Upgrading to STAC support
- [Release Notes](../../releases/1.0.0a.md) - What's new in version 1.0.0a
