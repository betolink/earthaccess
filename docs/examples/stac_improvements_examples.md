"""
Real-world examples for STAC improvements in earthaccess.

This notebook demonstrates new features:
- Asset filtering
- STAC conversion methods
- External STAC catalog search
"""

# Example 1: Asset Filtering with Granule Queries

```python
import earthaccess
from earthaccess.store_components.asset import AssetFilter

# Authenticate
auth = earthaccess.login()

# Search for granules
granules = earthaccess.search_data(
    short_name="ATL03",
    bounding_box=(-122.5, 37.7, -122.3, 37.9),
    temporal=("2023-01-01", "2023-12-31"),
    count=10
)

# Get data assets only
for granule in granules:
    data_assets = granule.get_data_assets()
    print(f"Granule has {len(data_assets)} data assets")

# Or use custom filtering
data_filter = AssetFilter(
    content_types=["application/netcdf"],
    min_size=1024*1024,  # At least 1 MB
    include_roles={"data"}
)
filtered_assets = granule.filter_assets(data_filter)
```

# Example 2: Browse and Thumbnail Assets

```python
import earthaccess

granules = earthaccess.search_data(
    short_name="MOD09GA",
    count=5
)

for granule in granules:
    # Get browse images
    browse_assets = granule.get_browse_assets()
    print(f"Found {len(browse_assets)} browse images")

    # Get thumbnails
    thumb_assets = granule.get_thumbnail_assets()
    print(f"Found {len(thumb_assets)} thumbnails")
```

# Example 3: STAC Conversion

```python
import earthaccess

# Search for granules
granules = earthaccess.search_data(
    short_name="ATL06",
    count=3
)

# Convert to STAC format
for granule in granules:
    stac_item = granule.to_stac()
    print(f"STAC Item ID: {stac_item['id']}")
    print(f"STAC Geometry: {stac_item.get('geometry')}")

# Convert to UMM format
umm = granule.to_umm()
print(f"UMM keys: {list(umm.keys())}")

# Same for collections
collections = earthaccess.search_datasets(short_name="ATL06", limit=3)
for collection in collections:
    stac_collection = collection.to_stac()
    print(f"STAC Collection: {stac_collection['id']}")
```

# Example 4: External STAC Catalog Search

```python
from earthaccess.store_components.stac_search import search_stac

# Search Microsoft Planetary Computer (STAC catalog)
results = search_stac(
    url="https://planetarycomputer.microsoft.com/api/stac/v1",
    collections=["sentinel-2-l2a"],
    bbox=[-122.5, 37.7, -122.3, 37.9],
    datetime="2023-01-01/2023-12-31",
    limit=10
)

print(f"Found {len(results)} items")

# Iterate over results
for item in results:
    print(f"Item ID: {item.id}")
    print(f"Geometry: {item.geometry}")

# Get pages
for page in results.pages(page_size=5):
    print(f"Page with {len(page)} items")
```

# Example 5: Asset Filtering by Filename Pattern

```python
import earthaccess
from earthaccess.store_components.asset import AssetFilter

granules = earthaccess.search_data(
    short_name="HLS",
    count=5
)

# Filter assets by filename pattern
for granule in granules:
    filter_obj = AssetFilter(
        filename_patterns=["*_B*.tif", "*_B02*.tif"]
    )
    blue_band_assets = granule.filter_assets(filter_obj)
    print(f"Found {len(blue_band_assets)} blue band assets")
```

# Example 6: Asset Filtering by Band

```python
import earthaccess
from earthaccess.store_components.asset import get_assets_by_band

# For datasets with band information
granules = earthaccess.search_data(
    short_name="VIIRS",
    count=3
)

for granule in granules:
    # Get specific bands
    nir_assets = get_assets_by_band(granule.get_assets(), ["I01"])
    red_assets = get_assets_by_band(granule.get_assets(), ["M03"])
    print(f"NIR: {len(nir_assets)}, Red: {len(red_assets)}")
```

# Example 7: Combining Filters

```python
import earthaccess
from earthaccess.store_components.asset import AssetFilter

# Create filter with multiple criteria
filter1 = AssetFilter(content_types=["application/netcdf"])
filter2 = AssetFilter(min_size=1024*1024)

# Combine with AND logic
combined_filter = filter1.combine(filter2)

# Apply to granules
granules = earthaccess.search_data(
    short_name="ATL03",
    count=5
)

for granule in granules:
    filtered = granule.filter_assets(combined_filter)
    print(f"Granule has {len(filtered)} matching assets")
```

# Example 8: Using Enhanced Results Methods

```python
import earthaccess
from earthaccess.store_components.asset import AssetFilter

# Note: open() and download() are placeholders in base class
# Specific implementations are provided by subclasses

# Search for granules
granules = earthaccess.search_data(
    short_name="ATL03",
    count=5
)

# When subclass implements open():
# datasets = granules.open(asset_filter=AssetFilter(content_types=["application/netcdf"]))

# When subclass implements download():
# granules.download(local_path="./data", threads=8)
```
