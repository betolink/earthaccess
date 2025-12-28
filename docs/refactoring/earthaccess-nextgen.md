---
title: "\n"

---

# earthaccess next-generation

Author: Luis Lopez
Date: Dec 23 / 2025

`earthaccess` has been a CMR-only client, limiting interoperability with non-NASA data providers. Ongoing STAC integration removes this constraint by aligning earthaccess with the broader STAC ecosystem and enabling more cloud-native workflows. This document outlines the expected library ergonomics after these changes get fully implemented.

The goal is not to reimplement functionality already provided by pystac-client. The primary exception is the upcoming earthaccess results object returned by `search_data()` and `search_datasets()`, which will follow pystac-client conventions, including lazy pagination and native STAC compatibility.

A key design objective is improved cloud-native behavior and horizontal scalability. Operations such as `.download(granules)` and `.open(granules)` should execute in parallel using distributed execution frameworks if they are available (e.g., Dask, Ray, Lithops).

For `download(granules, target_fs)`, the target filesystem should be abstracted beyond local storage to include cloud object stores with authenticated access. The `open()` API should support `fsspec` and `obstore`-compatible backends, with caching and I/O optimizations to improve performance in downstream consumers such as `xarray` or `geopandas`.


## Query interoperability


```python=

import earthaccess
from earthaccess.query import GranuleQuery, StacItemQuery # equivalents

# Build CMR query
query = GranuleQuery(
    short_name="HLSL30.v2.0",
    temporal=("2020-01-01", "2020-12-31"),
    bounding_box=(-180, -90, 180, -60),
    cloud_coverage=(0, 20)
)

# Or we could create a StacItemQuery
query = StacItemQuery(
    collections=["HLSL30.v2.0"], # STAC uses versioned IDs for HLS30L
    datetime="2020-01-01/2020-12-31",    # temporal range
    bbox=[-180, -90, 180, -60],          # bounding_box or intersects etc
    query={"eo:cloud_cover": {"lt": 20}} # cloud_coverage equivalent
)


# We can use geometries from files that will get simplified to < 300 points
query = GranuleQuery(
    short_name=["HLSL30.v2.0", "HLSS30.v2.0"],
    temporal=("2020-01-01", "2020-12-31"),
    polygon="myboundaries.geojson", # or shapefile, klm etc.
    cloud_coverage=(0, 20)
)
```

We have a challenge here, how can we transform/translate filter query parameters to cql2 filters? e.g. `cloud_cover` to `query={"eo:cloud_cover": {"lt": 20}}`, there are a few stac extensions that map 1 to 1 to CMR semantics and they already exist but there are some that won't. e.g. `readable_granule_name=ATL06_??_01*.nc` won't have a one to one just something similar with

```json
{
  "filter": {
    "op": "like",
    "args": [
      { "property": "assets.data.href" },
      "ATL06_%_01%.nc"
    ]
  }
}
```

Perhaps we'll need to create a mapping between CMR (https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html)
and STAC extensions https://stac-extensions.github.io/
that will guide this query conversion for the core parameters that overlap semanticall (besides the spatio temporal ones)

Additionally we can just drop those incompatible parameters with a warning when converting CMR queries to STAC queries, and vice-versa.

> Note: NASA CMR does something similar, https://github.com/nasa/cmr-stac we may want to study what got implemented here as guidance.



```python=
# Use method chaining in both flavors
query = (
    GranuleQuery()
    .short_name("ATL03")
    .temporal("2020-01-01", "2020-12-31")
    .bounding_box(-180, -90, 180, -60)
)

# Validate before authentication
validation = query.validate()

if not validation.is_valid:
    for error in validation.errors:
        print(f"{error.field}: {error.message}") # Fix all errors at once

# Get both CMR and STAC formats
cmr_params = query.to_cmr()

# transforms it to STAC semantic equivalent, cql filters should
# be used when applicable e.g. cloud_cover=(0, 20)
stac_params = query.to_stac()

```

## Query execution and results interoperability

```python=

# Authenticate and execute
auth = earthaccess.login()

# fetches the first page of results and we can paginate unless get_all=True
# imagine this search returns 100k granules
results = earthaccess.search_data(query=query)

# Or we could use the params directly, equivalent to **params
results = earthaccess.search_data(cmr_params)

# Convert to STAC (one-way) or from STAC (bidirectional!)
# this will always hit the CMR umm JSON API endpoint.
stac_items = []
for page in results.pages():
    # each item will be a pystac.Item
    stac_items.extend([granule.to_stac() for granule in page])

# This conversion will allow things like using ODC-stac
xx = odc.stac.load(
    stac_items,
    bands=["red", "green", "blue"],
)
xx.red.plot.imshow(col="time")


# Importantly: we could use pystac-client with our stac query
catalog_url = "https://cmr.earthdata.nasa.gov/stac/LPCLOUD"
client = Client.open(catalog_url)
search = client.search(stac_params)

# Or we could transform them to a geodataframe to use with geopandas or loneboard
import geopandas as gpd

for item in stac_items:
    geom = shape(item.geometry)
    geom_simplified = geom.simplify(0.01, preserve_topology=True)
    # Only keep essential properties
    geometries.append(geom_simplified)
    properties.append({
        'id': item.id[:20],  # Truncate long IDs
        'date': item.datetime.date() if item.datetime else None
    })

gdf = gpd.GeoDataFrame(properties, geometry=geometries, crs="EPSG:4326")
```

Perhaps this could be exposed as another method like`item.to_gdf(fields=["umm.some.field", "umm.another.field"])`
(geometries will get simplified to match CMR limits)


## Data Access: parallel execution, credential propagation and asset filtering.



`earthaccess.download()` and `earthaccess.open()` operate over iterables of STAC Items or search results. When a results object is passed, items are lazily paginated and partitioned into work units that can be distributed across available workers.

In a single-process context, execution is sequential. When a parallel execution engine is available (e.g., a running Dask distributed cluster), earthaccess delegates task scheduling to the executor. Each worker processes a subset of STAC Items, performing download or open operations concurrently.

Credential handling is decoupled from task execution. Authentication is resolved once on the client side and serialized into a minimal, backend-specific credential payload (e.g., AWS credentials, Earthdata tokens). This payload is broadcast to workers and used to initialize filesystem or HTTP clients lazily on first use, rather than recreating sessions per task or per thread. The recommended way however is to use safe channels to distribute environment variables to the workers via secrets so each worker knows how to write to a target cloud storage.

For object storage and remote access, earthaccess relies on fsspec (and compatible backends such as obstore) to manage connection pooling, request signing, and caching. Filesystem instances are created per worker process and reused across tasks, avoiding redundant session creation while remaining thread-safe.

This model should ensure that:

* Lazy pagination and bounded memory usage
* Efficient parallel I/O across workers
* Single authentication handshake per execution context
* Reuse of authenticated filesystem/session objects within workers

The same execution model applies to both `download()` and `open()`, with`open()` returning a fileset suitable for zero-copy or cached access by downstream libraries such as xarray and geopandas.

```python=

from earthaccess.store import AssetFilter, filter_assets

# We can download or open stac items
earthaccess.download(stac_items, "./test")

# We can filter which files/assets get downloaded or open
# filters will match against the granule name, the file name or the asset role/type in that order.
earthaccess.download(stac_items,
                     filters={
                       "include": ["B02",
                                  "B03"],
                       "exclude":["*"] # by default if not included
                     }
                     "./test")

# Or we can try to use a more formal form like:
# Define filter
asset_filter = AssetFilter(
    content_types=({"application/x-hdf5", "application/x-hdf"}),
    include_roles=({"data"}),
    exclude_roles=({"thumbnail", "metadata"}),
    include_files=({"*B02*.tif", "*B03*.tif"}),
    exclude_files=({"*B01*.tif", "*B06*.tif"}),
    max_size=1024 * 1024 * 1024,  # 1GB
)

# Apply filter to each granule
filtered_asset_urls = []
for granule in results:
    assets = granule.assets()  # Returns List[Asset]
    data_assets = filter_assets(assets, asset_filter)

    for asset in data_assets:
        filtered_asset_urls.append(asset.href)
        print(f"Downloading: {asset.href}")
        print(f"  Type: {asset.type}")
        print(f"  Size: {asset.file_size}")
        print(f"  Roles: {asset.roles}")


```

And we could also pass a filter to `download()` or `open()`

```python=

# we can open or download the results and we'll paginate
# and distribute them to the workers if we have them.
earthaccess.download(
    results,
    filter=asset_filter,
    "s3://my-bucket/test"
)

# or we can open them in parallel as well using workers, defaults to fsspec, this may not work with lithops labmda
fileset = earthaccess.open(results, filter=asset_filter)


import xarray as xr
ds = xr.open_mfdataset(fileset, engine="h5netcdf", parallel=True, **kwargs) # we'll use the running Dask cluster if there is one

# or we should be able to pass the storage options and plain URLs
storage_options = earthaccess.get_s3_credentials(results)

ds = xr.open_mfdataset(filtered_asset_urls, engine="rioxarray", parallel=True, storage_options=storage_options, **kwargs)
# Or we can leverage virtualizarr
vds = earthaccess.open_virtual_mfdataset(
    results, # will paginate to get the urls of the dmrpp files first
    group="/gt1l/land_ice_segments",   # optional (ICESat-2)
    concat_dim="time",
    load=False # if true it will load the coords dimensions required for fancy indexing
)

# then we could even persist this virtual store as Icechunk
vds.vz.to_icechunk(icechunk_store)

```
