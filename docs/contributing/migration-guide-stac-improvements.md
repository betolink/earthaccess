# Migration Guide: STAC Improvements

This guide helps you migrate your code to use new STAC improvements in earthaccess.

## New Features Overview

### 1. Asset Filtering

Granules now support asset-level filtering for more precise data access.

### 2. STAC Conversion

`DataGranule` and `DataCollection` now have `to_stac()` and `to_umm()` methods.

### 3. External STAC Catalog Search

New `search_stac()` function for querying STAC catalogs.

---

## Migration Examples

### Example 1: Getting Data Assets

**Old way (still works):**
```python
granules = earthaccess.search_data(short_name="ATL03", count=5)
for granule in granules:
    data_links = granule.data_links()
```

**New way with asset filtering:**
```python
from earthaccess.store_components.asset import AssetFilter

granules = earthaccess.search_data(short_name="ATL03", count=5)
for granule in granules:
    # Get only data assets
    data_assets = granule.get_data_assets()

    # Or filter with custom criteria
    my_filter = AssetFilter(
        content_types=["application/netcdf"],
        min_size=1024*1024  # At least 1MB
    )
    filtered = granule.filter_assets(my_filter)
```

### Example 2: Browse and Thumbnail Access

**Old way:**
```python
granules = earthaccess.search_data(short_name="MOD09GA", count=5)
for granule in granules:
    browse_links = granule.dataviz_links()
```

**New way:**
```python
granules = earthaccess.search_data(short_name="MOD09GA", count=5)
for granule in granules:
    # Get browse images
    browse_assets = granule.get_browse_assets()

    # Get thumbnails
    thumb_assets = granule.get_thumbnail_assets()
```

### Example 3: STAC Conversion

**New feature:**

```python
# Convert granules to STAC format
granules = earthaccess.search_data(short_name="ATL06", count=3)
for granule in granules:
    stac_item = granule.to_stac()
    # stac_item is a dict with STAC Item structure
    print(stac_item['id'])
    print(stac_item['geometry'])

    # Convert back to UMM
    umm = granule.to_umm()

# Same for collections
collections = earthaccess.search_datasets(short_name="ATL06", limit=3)
for collection in collections:
    stac_collection = collection.to_stac()
    umm = collection.to_umm()
```

### Example 4: Search External STAC Catalogs

**New feature:**

```python
from earthaccess.store_components.stac_search import search_stac

# Search Microsoft Planetary Computer
results = search_stac(
    url="https://planetarycomputer.microsoft.com/api/stac/v1",
    collections=["sentinel-2-l2a"],
    bbox=[-122.5, 37.7, -122.3, 37.9],
    datetime="2023-01-01/2023-12-31",
    limit=10
)

# Iterate like CMR results
for item in results:
    print(f"Item ID: {item.id}")
    print(f"Geometry: {item.geometry}")

# Pagination
for page in results.pages(page_size=5):
    print(f"Processing page with {len(page)} items")
```

### Example 5: Advanced Asset Filtering

**New filtering capabilities:**

```python
from earthaccess.store_components.asset import (
    AssetFilter,
    get_assets_by_band,
    get_assets_by_size_range,
)

granules = earthaccess.search_data(short_name="VIIRS", count=5)

# Filter by filename pattern
filter1 = AssetFilter(filename_patterns=["*_B*.tif"])

# Filter by size
filter2 = get_assets_by_size_range(assets, 1024*1024, 100*1024*1024)

# Combine filters
combined = filter1.combine(filter2)

for granule in granules:
    filtered = granule.filter_assets(combined)
    print(f"Found {len(filtered)} matching assets")
```

### Example 6: Band-specific Filtering

**New for multi-band datasets:**

```python
from earthaccess.store_components.asset import get_assets_by_band

granules = earthaccess.search_data(short_name="HLS", count=5)

for granule in granules:
    # Get specific bands
    red_assets = get_assets_by_band(granule.get_assets(), ["B02"])
    green_assets = get_assets_by_band(granule.get_assets(), ["B03"])

    print(f"Red band: {len(red_assets)} files")
    print(f"Green band: {len(green_assets)} files")
```

---

## Backward Compatibility

All existing code continues to work without changes. New features are optional enhancements.

### What's Changed:
- **Asset objects**: New `Asset` dataclass for granule file representation
- **STAC methods**: Added `to_stac()` and `to_umm()` to `DataGranule` and `DataCollection`
- **External search**: New `search_stac()` function (requires `pystac-client`)

### What's Unchanged:
- `data_links()` - Still works, returns list of URLs
- `dataviz_links()` - Still works, returns browse images
- `search_data()` - API unchanged
- `search_datasets()` - API unchanged
- All query methods - Unchanged

---

## Quick Reference

| Feature | Old API | New API |
|---------|-----------|-----------|
| Get data URLs | `data_links()` | `get_data_assets()` |
| Get browse images | `dataviz_links()` | `get_browse_assets()` |
| Filter assets | N/A | `filter_assets(AssetFilter)` |
| Convert to STAC | N/A | `to_stac()` |
| Get original UMM | Access `['umm']` directly | `to_umm()` |
| External search | N/A | `search_stac()` |

---

## Optional Dependencies

New `search_stac()` function requires `pystac-client`:

```bash
pip install pystac-client
```

All other new features work without additional dependencies.

---

## Getting Help

For questions about migration:
- Review [Real-world Examples](examples/stac_improvements_examples.md)
- Check the [API Documentation](user-reference/)
- Open an issue on GitHub
