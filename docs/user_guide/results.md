# Working with Results

## Overview

When you search for data using `earthaccess`, the results are returned as specialized objects that provide convenient methods for exploring and working with NASA Earthdata. This page covers:

- **DataCollection**: Represents a NASA dataset (collection)
- **DataGranule**: Represents an individual data file (granule)
- **SearchResults**: A lazy-loading iterator for paginated search results

All result types provide rich display in Jupyter notebooks, with HTML representations that show key metadata at a glance.

## Rich Display in Jupyter Notebooks

When working in Jupyter notebooks, `earthaccess` results automatically display rich HTML representations. Simply evaluate a result object in a cell to see a formatted view of its metadata.

```python
import earthaccess

# Search for datasets - displays rich HTML in Jupyter
datasets = earthaccess.search_datasets(short_name="ATL06", count=1)
datasets[0]  # Displays formatted collection card
```

The HTML display includes:

- **For collections**: Short name, version, abstract, DOI, temporal extent, cloud hosting status, and links
- **For granules**: Granule ID, temporal coverage, file size, cloud status, data links, and preview images
- **For SearchResults**: Summary statistics, cached count, and a collapsible table of results

## DataCollection

A `DataCollection` represents a NASA dataset. Collections contain metadata about the dataset and methods to access that metadata.

### Key Methods

| Method | Description |
|--------|-------------|
| `summary()` | Returns a dictionary with key metadata fields |
| `concept_id()` | Returns the unique CMR concept ID |
| `abstract()` | Returns the dataset description |
| `version()` | Returns the dataset version |
| `doi()` | Returns the DOI if available |
| `landing_page()` | Returns the dataset landing page URL |
| `get_data()` | Returns URLs for accessing the data |
| `s3_bucket()` | Returns S3 bucket info (cloud-hosted only) |
| `services()` | Returns available data services |
| `to_stac()` | Converts to STAC Collection format |
| `to_dict()` | Converts to a plain dictionary |
| `show_map()` | Displays interactive map of spatial extent |

### Example Usage

```python
import earthaccess

# Search for a specific dataset
datasets = earthaccess.search_datasets(
    short_name="ATL06",
    version="006"
)

collection = datasets[0]

# Get a summary
print(collection.summary())
# {'short-name': 'ATL06', 'concept-id': 'C2564625052-NSIDC_ECS', ...}

# Get the concept ID for granule searches
concept_id = collection.concept_id()

# Check if cloud-hosted
print(collection.s3_bucket())

# Get the DOI
doi = collection.doi()
print(f"DOI: {doi}")

# Convert to STAC format for interoperability
stac_collection = collection.to_stac()
```

## DataGranule

A `DataGranule` represents an individual data file. Granules contain metadata about the file and methods to access data links.

### Key Methods

| Method | Description |
|--------|-------------|
| `size()` | Returns file size in MB |
| `data_links()` | Returns URLs for downloading the data |
| `dataviz_links()` | Returns URLs for browse/preview images |
| `cloud_hosted` | Boolean indicating if cloud-hosted |
| `to_stac()` | Converts to STAC Item format |
| `to_dict()` | Converts to a plain dictionary |
| `assets()` | Returns all assets as Asset objects |
| `data_assets()` | Returns only data assets (excludes thumbnails) |
| `show_map()` | Displays interactive map of spatial extent |

### Example Usage

```python
import earthaccess

# Search for granules
results = earthaccess.search_data(
    short_name="ATL06",
    temporal=("2020-01-01", "2020-01-31"),
    count=10
)

granule = results[0]

# Check file size
print(f"Size: {granule.size()} MB")

# Get data links
for link in granule.data_links():
    print(link)

# Check if cloud-hosted
if granule.cloud_hosted:
    print("This granule is available in the cloud")

# Get browse images
for viz_link in granule.dataviz_links():
    print(f"Preview: {viz_link}")

# Convert to STAC format
stac_item = granule.to_stac()
```

## SearchResults

`SearchResults` is a lazy-loading iterator that efficiently handles large result sets by fetching pages of results on demand. This allows you to work with millions of results without loading them all into memory at once.

### Key Features

- **Lazy pagination**: Results are fetched from CMR as you iterate
- **Caching**: Previously fetched results are cached for reuse
- **Length**: Use `len()` to get total hits without fetching all results
- **Page iteration**: Use `pages()` for batch processing

### Methods

| Method | Description |
|--------|-------------|
| `__iter__()` | Iterate through results one at a time |
| `__len__()` | Get total number of matching results |
| `pages()` | Iterate through results page by page |
| `summary()` | Get aggregated statistics for cached results |
| `show_map()` | Display interactive map of spatial extents |

### Basic Iteration

```python
import earthaccess

# Search returns a SearchResults object
results = earthaccess.search_data(
    short_name="ATL06",
    temporal=("2020-01", "2020-03")
)

# Check total hits (makes one request to CMR)
print(f"Found {len(results)} granules")

# Iterate through results (fetches pages as needed)
for granule in results:
    print(granule["umm"]["GranuleUR"])
    # Process granule...
```

### Page-by-Page Iteration

For batch processing, use the `pages()` method:

```python
results = earthaccess.search_data(
    short_name="ATL06",
    temporal=("2020-01", "2020-03")
)

# Process results in batches
for page in results.pages():
    print(f"Processing batch of {len(page)} granules")
    # Each page is a list of DataGranule objects
    for granule in page:
        process_granule(granule)
```

### Summary Statistics

The `summary()` method computes aggregated statistics for cached results:

```python
results = earthaccess.search_data(
    short_name="ATL06",
    temporal=("2020-01-01", "2020-01-31"),
    count=100
)

# Fetch results first (by iterating or converting to list)
granule_list = list(results)

# Get summary statistics
summary = results.summary()
print(summary)
# {
#     'total_hits': 1234,
#     'cached_count': 100,
#     'total_size_mb': 5678.5,
#     'cloud_count': 100,
#     'temporal_range': '2020-01-01 to 2020-01-31'
# }
```

!!! note "Summary computation limits"

    For performance reasons, detailed summary statistics are only computed if the total number of hits is less than 10,000. For larger result sets, iterate through pages and compute statistics incrementally.

## Interactive Map Visualization

All result types support interactive map visualization using the `show_map()` method. This displays a GPU-accelerated map showing the spatial extent of your results.

### Installation

Map visualization requires optional dependencies:

```bash
pip install earthaccess[widgets]
```

This installs:
- `anywidget` - Widget framework
- `lonboard` - GPU-accelerated geospatial visualization
- `geopandas` - Geospatial data handling
- `shapely` - Geometric operations

### Usage

```python
import earthaccess

# Search and cache results
results = earthaccess.search_data(
    short_name="ATL06",
    bounding_box=(-50, 60, -40, 70),
    count=100
)
granules = list(results)  # Cache results

# Display map of all granule bounding boxes
results.show_map()
```

You can also visualize individual items:

```python
# Single granule
granule = results[0]
granule.show_map()

# Single collection
datasets = earthaccess.search_datasets(short_name="ATL06")
datasets[0].show_map()
```

### Customizing Map Colors

```python
# Custom colors (RGBA format)
results.show_map(
    fill_color=[255, 100, 0, 80],   # Semi-transparent orange
    line_color=[255, 100, 0, 200]   # Solid orange outline
)
```

### Performance Considerations

- By default, `show_map()` displays up to 10,000 bounding boxes
- Use the `max_items` parameter to adjust: `results.show_map(max_items=5000)`
- For very large result sets, consider filtering spatially or temporally first

### Checking Widget Availability

```python
from earthaccess.formatting import has_widget_support

if has_widget_support():
    results.show_map()
else:
    print("Install widgets extra: pip install earthaccess[widgets]")
```

## Converting Results to STAC Format

Results can be converted to STAC (SpatioTemporal Asset Catalog) format for interoperability with STAC-based tools:

```python
# Convert granule to STAC Item
stac_item = granule.to_stac()
print(stac_item["type"])  # "Feature"
print(stac_item["stac_version"])  # "1.0.0"

# Convert collection to STAC Collection
stac_collection = collection.to_stac()
print(stac_collection["type"])  # "Collection"
```

## Serialization

Results can be serialized to dictionaries for storage or transmission:

```python
# Convert to dictionary
granule_dict = granule.to_dict()
collection_dict = collection.to_dict()

# Useful for JSON serialization
import json
json_str = json.dumps(granule_dict)
```

This is particularly useful when distributing work across multiple processes or machines with tools like Dask.
