---
title: 'earthaccess Next-Gen Implementation Plan'
---

# earthaccess Next-Gen Implementation Plan

**Author:** Based on analysis of `stac-distributed-glm` and `stac-distributed-opus` branches
**Date:** December 27, 2025

## Overview

This document outlines a phased implementation plan to merge the best components from both `stac-distributed-glm` (GLM) and `stac-distributed-opus` (Opus) branches. The goal is to achieve the vision described in `earthaccess-nextgen.md`: a cloud-native, STAC-compatible, horizontally scalable library.

### Guiding Principles

1. **User experience first**: Query building should be intuitive and auth-decoupled
2. **Ecosystem interoperability**: Bidirectional STAC conversion enables broader data access
3. **Testability**: Dependency injection and separation of concerns
4. **Backward compatibility**: Existing code should continue to work
5. **Incremental delivery**: Ship value in phases, not one massive merge

---

## Target API: Complete Examples

This section shows the complete target API as envisioned in `earthaccess-nextgen.md`. Each subsequent phase implements portions of this API.

### Query Interoperability

The new query system supports both CMR-native and STAC-native query construction with seamless conversion between formats.

```python
import earthaccess
from earthaccess.query import GranuleQuery, StacItemQuery

# Build CMR query using kwargs (new pattern)
query = GranuleQuery(
    short_name="HLSL30.v2.0",
    temporal=("2020-01-01", "2020-12-31"),
    bounding_box=(-180, -90, 180, -60),
    cloud_coverage=(0, 20)
)

# Or create a StacItemQuery directly
query = StacItemQuery(
    collections=["HLSL30.v2.0"],           # STAC uses versioned IDs
    datetime="2020-01-01/2020-12-31",      # temporal range
    bbox=[-180, -90, 180, -60],            # bounding_box equivalent
    query={"eo:cloud_cover": {"lt": 20}}   # cloud_coverage equivalent (CQL2)
)

# Support geometry from files (auto-simplified to <300 points for CMR)
query = GranuleQuery(
    short_name=["HLSL30.v2.0", "HLSS30.v2.0"],
    temporal=("2020-01-01", "2020-12-31"),
    polygon="myboundaries.geojson",  # or shapefile, kml, etc.
    cloud_coverage=(0, 20)
)
```

### Query Validation and Format Conversion

```python
# Use method chaining (alternative pattern)
query = (
    GranuleQuery()
    .short_name("ATL03")
    .temporal("2020-01-01", "2020-12-31")
    .bounding_box(-180, -90, 180, -60)
)

# Validate BEFORE authentication - see ALL errors at once
validation = query.validate()

if not validation.is_valid:
    for error in validation.errors:
        print(f"{error.field}: {error.message}")  # Fix all errors at once

# Get both CMR and STAC formats from same query
cmr_params = query.to_cmr()

# Transforms to STAC semantic equivalent
# CQL2 filters used when applicable (e.g., cloud_cover)
stac_params = query.to_stac()
```

### Query Execution and Results

```python
# Authenticate and execute
auth = earthaccess.login()

# Fetches the first page of results with lazy pagination
# Imagine this search returns 100k granules - memory stays bounded
results = earthaccess.search_data(query=query)

# Or use params directly (equivalent to **params)
results = earthaccess.search_data(cmr_params)

# Convert results to STAC Items (one-way)
# Always hits the CMR UMM JSON API endpoint
stac_items = []
for page in results.pages():
    # Each item will be a pystac.Item compatible dict
    stac_items.extend([granule.to_stac() for granule in page])
```

### Integration with STAC Ecosystem

```python
# Use with ODC-STAC for xarray integration
import odc.stac

xx = odc.stac.load(
    stac_items,
    bands=["red", "green", "blue"],
)
xx.red.plot.imshow(col="time")

# Use pystac-client with our STAC query params
from pystac_client import Client

catalog_url = "https://cmr.earthdata.nasa.gov/stac/LPCLOUD"
client = Client.open(catalog_url)
search = client.search(stac_params)
```

### GeoDataFrame Conversion

```python
import geopandas as gpd
from shapely.geometry import shape

# Transform results to a GeoDataFrame for geopandas or lonboard
geometries = []
properties = []

for item in stac_items:
    geom = shape(item.geometry)
    geom_simplified = geom.simplify(0.01, preserve_topology=True)
    geometries.append(geom_simplified)
    properties.append({
        'id': item.id[:20],  # Truncate long IDs
        'date': item.datetime.date() if item.datetime else None
    })

gdf = gpd.GeoDataFrame(properties, geometry=geometries, crs="EPSG:4326")

# Future: Could be exposed as a method
# gdf = results.to_gdf(fields=["umm.some.field", "umm.another.field"])
# (geometries auto-simplified to match CMR limits)
```

### Data Access with Asset Filtering

```python
from earthaccess.store import AssetFilter, filter_assets

# Download or open STAC items directly
earthaccess.download(stac_items, "./test")

# Filter which files/assets get downloaded or opened
# Filters match against: granule name, file name, or asset role/type (in order)
earthaccess.download(
    stac_items,
    "./test",
    filters={
        "include": ["B02", "B03"],
        "exclude": ["*"]  # Default if not included
    }
)

# Or use the formal AssetFilter class for complex filtering
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

### Parallel and Distributed Execution

```python
# Download with filter - lazy pagination distributes work to workers
earthaccess.download(
    results,
    "s3://my-bucket/test",  # Cloud target filesystem
    filter=asset_filter,
)

# Open in parallel - defaults to fsspec
# May not work with Lithops Lambda (use download instead)
fileset = earthaccess.open(results, filter=asset_filter)

# Use with xarray - leverages running Dask cluster if available
import xarray as xr

ds = xr.open_mfdataset(
    fileset,
    engine="h5netcdf",
    parallel=True,
    **kwargs
)

# Or get storage options and use plain URLs
storage_options = earthaccess.get_s3_credentials(results)

ds = xr.open_mfdataset(
    filtered_asset_urls,
    engine="rioxarray",
    parallel=True,
    storage_options=storage_options,
    **kwargs
)
```

### Virtual Datasets with VirtualiZarr

```python
# Leverage virtualizarr for cloud-native access
vds = earthaccess.open_virtual_mfdataset(
    results,  # Will paginate to get dmrpp file URLs first
    group="/gt1l/land_ice_segments",  # Optional (ICESat-2)
    concat_dim="time",
    load=False  # If True, loads coords/dims for fancy indexing
)

# Persist virtual store as Icechunk
vds.virtualizarr.to_icechunk(icechunk_store)
```

---

## Target Architecture

```
earthaccess/
├── query/                      # From Opus (auth-decoupled, dual construction)
│   ├── __init__.py
│   ├── base.py                 # QueryBase ABC with kwargs + method chaining
│   ├── types.py                # BoundingBox, DateRange, Point, Polygon
│   ├── granule_query.py        # GranuleQuery with to_cmr()/to_stac()
│   ├── collection_query.py     # CollectionQuery
│   └── validation.py           # ValidationResult accumulator pattern
│
├── stac/                       # From Opus (bidirectional conversion)
│   ├── __init__.py
│   └── converters.py           # umm_granule_to_stac_item, stac_item_to_data_granule
│
├── store/                      # Hybrid (GLM DI + Opus flexibility)
│   ├── __init__.py             # Public exports, backward compat
│   ├── asset.py                # From GLM: Asset + AssetFilter
│   ├── credentials.py          # Hybrid: S3Credentials (GLM) + from_auth/to_auth (Opus)
│   └── filesystems.py          # From GLM: FileSystemFactory
│
├── parallel.py                 # Either (nearly identical implementations)
├── streaming.py                # From Opus (dedicated module, WorkerContext)
├── target_filesystem.py        # Either (identical implementations)
├── credentials.py              # Standalone credential manager (Opus location)
├── store.py                    # Refactored with DI (GLM pattern)
├── results.py                  # Enhanced with to_stac(), assets() methods
└── api.py                      # Updated to accept query objects
```

---

## Phase 1: Query Architecture (Foundation)

**Priority:** High
**Source:** Opus
**Estimated Effort:** 1-2 weeks

### Objective

Establish the auth-decoupled query system that enables flexible query construction and validation before execution.

### Components to Port

| File | Lines | Key Features |
|------|-------|--------------|
| `query/__init__.py` | ~20 | Package exports |
| `query/base.py` | ~180 | `QueryBase` ABC with `parameters()` introspection |
| `query/types.py` | ~330 | `BoundingBox`, `DateRange`, `Point`, `Polygon` with dual output |
| `query/granule_query.py` | ~500 | `GranuleQuery` with all CMR parameters |
| `query/collection_query.py` | ~400 | `CollectionQuery` with all CMR parameters |
| `query/validation.py` | ~175 | `ValidationResult`, `ValidationError` accumulator |

### Key Design Decisions

1. **No auth at construction**: Queries are pure data structures
2. **Dual construction**: Support both kwargs and method chaining
3. **Dual output**: `to_cmr()` and `to_stac()` on all query objects
4. **Validation accumulator**: `validate()` returns `ValidationResult` with all errors

### Target API: GranuleQuery and StacItemQuery

**CMR-native query construction:**

```python
import earthaccess
from earthaccess.query import GranuleQuery, StacItemQuery

# Build CMR query using kwargs
query = GranuleQuery(
    short_name="HLSL30.v2.0",
    temporal=("2020-01-01", "2020-12-31"),
    bounding_box=(-180, -90, 180, -60),
    cloud_coverage=(0, 20)
)

# Support multiple short_names and geometry from files
query = GranuleQuery(
    short_name=["HLSL30.v2.0", "HLSS30.v2.0"],
    temporal=("2020-01-01", "2020-12-31"),
    polygon="myboundaries.geojson",  # Auto-simplified to <300 points
    cloud_coverage=(0, 20)
)
```

**STAC-native query construction:**

```python
# Create a StacItemQuery directly
query = StacItemQuery(
    collections=["HLSL30.v2.0"],           # STAC uses versioned IDs
    datetime="2020-01-01/2020-12-31",      # temporal range
    bbox=[-180, -90, 180, -60],            # bounding_box equivalent
    query={"eo:cloud_cover": {"lt": 20}}   # CQL2 filter
)
```

**Method chaining alternative:**

```python
# Use method chaining for fluent API
query = (
    GranuleQuery()
    .short_name("ATL03")
    .temporal("2020-01-01", "2020-12-31")
    .bounding_box(-180, -90, 180, -60)
)
```

### Target API: Validation

```python
# Validate BEFORE authentication - see ALL errors at once
validation = query.validate()

if not validation.is_valid:
    for error in validation.errors:
        print(f"{error.field}: {error.message}")
    # User can fix all errors at once, not one at a time
```

### Target API: Format Conversion

```python
# Get both CMR and STAC formats from same query
cmr_params = query.to_cmr()
# Returns: {"short_name": "ATL03", "temporal": "2020-01-01,2020-12-31", ...}

stac_params = query.to_stac()
# Returns: {"collections": ["ATL03"], "datetime": "2020-01-01/2020-12-31", ...}
# CQL2 filters used when applicable (e.g., cloud_cover -> eo:cloud_cover)
```

### CMR to STAC Parameter Mapping Challenge

A key challenge is transforming CMR query parameters to CQL2 filters. Some parameters map 1:1 to STAC extensions:

| CMR Parameter | STAC Equivalent |
|---------------|-----------------|
| `cloud_coverage=(0, 20)` | `query={"eo:cloud_cover": {"lt": 20}}` |
| `temporal=(start, end)` | `datetime="start/end"` |
| `bounding_box=(w,s,e,n)` | `bbox=[w,s,e,n]` |

Some parameters have no direct equivalent:

| CMR Parameter | STAC Approach |
|---------------|---------------|
| `readable_granule_name="ATL06_??_01*.nc"` | CQL2 `like` on `assets.data.href` |

**Strategy:** Create a mapping between [CMR API](https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html) and [STAC extensions](https://stac-extensions.github.io/). For incompatible parameters, drop with a warning when converting.

> **Note:** NASA CMR-STAC does similar translation: https://github.com/nasa/cmr-stac

### Integration Points

1. Update `api.py` to accept `query` parameter in `search_data()` and `search_datasets()`
2. When `query` is provided, extract CMR params via `query.to_cmr()`
3. Maintain full backward compatibility with existing kwargs

### Tests to Port

- `tests/unit/test_query.py` (~418 lines)
- `tests/unit/test_api_query_integration.py` (~262 lines)

### Acceptance Criteria

- [ ] `GranuleQuery` and `CollectionQuery` can be constructed without auth
- [ ] `StacItemQuery` can be constructed with STAC-native parameters
- [ ] Both kwargs and method chaining work
- [ ] `validate()` returns all errors, not just the first
- [ ] `to_cmr()` and `to_stac()` produce correct output
- [ ] CQL2 filters are generated for cloud_coverage and similar parameters
- [ ] Geometry files are auto-simplified to <300 points
- [ ] `search_data(query=query)` works
- [ ] Legacy `search_data(short_name=...)` still works
- [ ] All existing tests pass

---

## Phase 2: Bidirectional STAC Conversion and Results

**Priority:** High
**Source:** Opus
**Estimated Effort:** 1-2 weeks

### Objective

Enable full ecosystem interoperability by supporting conversion in both directions: CMR UMM to STAC and STAC to CMR UMM. Also implement lazy pagination for memory-efficient handling of large result sets.

### Components to Port

| File | Lines | Key Features |
|------|-------|--------------|
| `stac/__init__.py` | ~30 | Package exports |
| `stac/converters.py` | ~860 | All conversion functions and mapping tables |

### Key Functions

```python
# CMR -> STAC (one-way, both branches have this)
def umm_granule_to_stac_item(granule: Dict, collection_id: Optional[str] = None) -> Dict:
    """Convert UMM granule to STAC Item dictionary."""

def umm_collection_to_stac_collection(collection: Dict) -> Dict:
    """Convert UMM collection to STAC Collection dictionary."""

# STAC -> CMR (NEW from Opus, enables external catalog support)
def stac_item_to_data_granule(item: Dict, auth: Optional[Auth] = None) -> DataGranule:
    """Convert STAC Item to DataGranule for use with earthaccess operations."""

def stac_collection_to_data_collection(collection: Dict) -> DataCollection:
    """Convert STAC Collection to DataCollection."""
```

### Target API: Query Execution with Lazy Pagination

```python
# Authenticate and execute
auth = earthaccess.login()

# Fetches the first page of results with lazy pagination
# Imagine this search returns 100k granules - memory stays bounded
results = earthaccess.search_data(query=query)

# Or use params directly (equivalent to **params)
results = earthaccess.search_data(cmr_params)

# Paginate through results lazily
for page in results.pages():
    process_batch(page)  # Each page is a list of DataGranule

# Or iterate directly (auto-paginates)
for granule in results:
    process_granule(granule)
```

### Target API: STAC Conversion

```python
# Convert results to STAC Items (one-way)
# Always hits the CMR UMM JSON API endpoint
stac_items = []
for page in results.pages():
    # Each item will be a pystac.Item compatible dict
    stac_items.extend([granule.to_stac() for granule in page])
```

### Target API: Integration with ODC-STAC

```python
import odc.stac

# Load STAC items directly into xarray via ODC-STAC
xx = odc.stac.load(
    stac_items,
    bands=["red", "green", "blue"],
)
xx.red.plot.imshow(col="time")
```

### Target API: Integration with pystac-client

```python
from pystac_client import Client

# Use pystac-client with our STAC query params
catalog_url = "https://cmr.earthdata.nasa.gov/stac/LPCLOUD"
client = Client.open(catalog_url)
search = client.search(stac_params)  # stac_params from query.to_stac()
```

### Target API: GeoDataFrame Conversion

```python
import geopandas as gpd
from shapely.geometry import shape

# Transform results to a GeoDataFrame for geopandas or lonboard
geometries = []
properties = []

for item in stac_items:
    geom = shape(item["geometry"])
    geom_simplified = geom.simplify(0.01, preserve_topology=True)
    geometries.append(geom_simplified)
    properties.append({
        'id': item["id"][:20],  # Truncate long IDs
        'date': item["properties"].get("datetime")
    })

gdf = gpd.GeoDataFrame(properties, geometry=geometries, crs="EPSG:4326")

# Future enhancement: expose as a method
# gdf = results.to_gdf(fields=["umm.some.field", "umm.another.field"])
# (geometries auto-simplified to match CMR limits)
```

### Target API: External STAC Catalog Support

```python
from pystac_client import Client
from earthaccess.stac import stac_item_to_data_granule

# Search external STAC catalog (e.g., Element84 Earth Search)
catalog = Client.open("https://earth-search.aws.element84.com/v1")
search = catalog.search(collections=["sentinel-2-l2a"], bbox=[-122, 37, -121, 38])

# Convert STAC items to earthaccess DataGranules
granules = [stac_item_to_data_granule(item.to_dict()) for item in search.items()]

# Now use earthaccess operations on external STAC data!
earthaccess.download(granules, local_path="./data")
files = earthaccess.open(granules)
```

### Mapping Constants

Port the explicit mapping tables from Opus for maintainability:

```python
CMR_URL_TYPE_TO_STAC_ROLE = {
    "GET DATA": ["data"],
    "GET DATA VIA DIRECT ACCESS": ["data"],
    "GET RELATED VISUALIZATION": ["visual"],
    "VIEW RELATED INFORMATION": ["metadata"],
    "USE SERVICE API": ["data", "api"],
    "EXTENDED METADATA": ["metadata"],
    "DOWNLOAD SOFTWARE": ["software"],
}

CMR_URL_TYPE_TO_STAC_TYPE = {
    "GET DATA": "application/octet-stream",
    "GET RELATED VISUALIZATION": "image/png",
    # ... extensive mappings
}
```

### Tests to Port

- `tests/unit/test_stac_converters.py` (~710 lines - comprehensive!)

### Acceptance Criteria

- [ ] `umm_granule_to_stac_item()` produces valid STAC 1.0.0 Items
- [ ] `stac_item_to_data_granule()` produces functional DataGranules
- [ ] Round-trip conversion preserves essential data
- [ ] External STAC items can be used with `earthaccess.download()`
- [ ] External STAC items can be used with `earthaccess.open()`
- [ ] Mapping tables cover common CMR URL types
- [ ] Lazy pagination works with `results.pages()`
- [ ] Direct iteration works with `for granule in results`
- [ ] Memory usage is bounded for large result sets

---

## Phase 3: Credential Management and Store Refactoring

**Priority:** High
**Source:** Hybrid (GLM structure + Opus features)
**Estimated Effort:** 2-3 weeks

### Objective

Create a robust, type-safe credential system with dependency injection that supports both thread-based and distributed execution.

### Components to Create (Hybrid)

| File | Source | Key Features |
|------|--------|--------------|
| `store/credentials.py` | GLM + Opus | `S3Credentials` dataclass + `from_auth()`/`to_auth()` |
| `store/filesystems.py` | GLM | `FileSystemFactory` for consistent filesystem creation |
| `streaming.py` | Opus | `AuthContext`, `WorkerContext`, `StreamingIterator` |
| `credentials.py` | Opus | Standalone `CredentialManager` |

### S3Credentials (GLM base + Opus extensions)

```python
@dataclass(frozen=True)
class S3Credentials:
    """Immutable container for AWS temporary credentials."""
    access_key_id: str
    secret_access_key: str
    session_token: str
    expiration: datetime
    region: str = "us-west-2"

    @property
    def is_expired(self) -> bool:
        """Check if credentials are expired (5-minute buffer)."""
        buffer = timedelta(minutes=5)
        return datetime.now(timezone.utc) >= self.expiration - buffer

    def to_dict(self) -> Dict[str, str]:
        """Convert to fsspec-compatible format."""
        return {
            "key": self.access_key_id,
            "secret": self.secret_access_key,
            "token": self.session_token,
        }

    def to_boto3_dict(self) -> Dict[str, str]:
        """Convert to boto3-compatible format."""
        return {
            "aws_access_key_id": self.access_key_id,
            "aws_secret_access_key": self.secret_access_key,
            "aws_session_token": self.session_token,
        }
```

### AuthContext (Hybrid: GLM fields + Opus reconstruction)

```python
@dataclass(frozen=True)
class AuthContext:
    """Serializable authentication context for distributed workers.

    Combines GLM's comprehensive fields with Opus's Auth reconstruction.
    """
    # From GLM: S3 credentials and HTTP session data
    s3_credentials: Optional[S3Credentials] = None
    https_headers: Optional[Mapping[str, str]] = None
    https_cookies: Optional[Mapping[str, str]] = None
    provider: Optional[str] = None
    cloud_hosted: bool = True

    # From Opus: Full auth reconstruction support
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    token_expiry: Optional[datetime] = None

    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @classmethod
    def from_auth(cls, auth: Auth, provider: Optional[str] = None) -> "AuthContext":
        """Create AuthContext from Auth instance (Opus pattern)."""
        # Extract credentials
        s3_creds = None
        if provider:
            raw = auth.get_s3_credentials(provider=provider)
            s3_creds = S3Credentials(
                access_key_id=raw["accessKeyId"],
                secret_access_key=raw["secretAccessKey"],
                session_token=raw["sessionToken"],
                expiration=datetime.fromisoformat(raw["expiration"]),
            )

        # Extract session for HTTP access (GLM pattern)
        session = auth.get_session()

        return cls(
            s3_credentials=s3_creds,
            https_headers=dict(session.headers) if session else None,
            https_cookies=dict(session.cookies) if session else None,
            provider=provider,
            username=getattr(auth, "username", None),
            password=getattr(auth, "password", None),
            token=getattr(auth, "_token", {}).get("access_token"),
        )

    def to_auth(self) -> Auth:
        """Recreate Auth instance for workers (Opus pattern)."""
        from earthaccess.auth import Auth

        auth = Auth()
        auth.authenticated = True
        if self.username:
            auth.username = self.username
        if self.password:
            auth.password = self.password
        if self.token:
            setattr(auth, "_token", {"access_token": self.token})

        return auth

    def to_dict(self) -> Dict[str, Any]:
        """Serialize for shipping to workers (GLM pattern)."""
        return {
            "s3_credentials": self.s3_credentials.to_dict() if self.s3_credentials else None,
            "https_headers": dict(self.https_headers) if self.https_headers else None,
            "https_cookies": dict(self.https_cookies) if self.https_cookies else None,
            "provider": self.provider,
            "cloud_hosted": self.cloud_hosted,
        }
```

### Store Refactoring (GLM pattern)

Apply dependency injection to Store while maintaining backward compatibility:

```python
class Store:
    """Data access store with dependency injection."""

    def __init__(self, auth: Union[Auth, None] = None):
        self._auth = auth

        # Dependency injection (GLM pattern)
        if isinstance(auth, Auth):
            self.credential_manager = CredentialManager(auth)
            self.filesystem_factory = FileSystemFactory(self.credential_manager)
        else:
            self.credential_manager = None
            self.filesystem_factory = None

        # Thread-local storage for session cloning (GLM pattern)
        self.thread_locals = threading.local()
        self._current_executor_type: Optional[str] = None

    def get_s3_filesystem(self, provider: Optional[str] = None, **kwargs) -> S3FileSystem:
        """Get S3 filesystem with proper credentials."""
        if self.filesystem_factory:
            return self.filesystem_factory.get_s3_filesystem(provider, **kwargs)
        return S3FileSystem(anon=True, **kwargs)

    def _clone_session_in_local_thread(self, original_session: Session) -> Session:
        """Clone session for thread-local use (GLM pattern)."""
        if not hasattr(self.thread_locals, "local_thread_session"):
            local_session = SessionWithHeaderRedirection()
            local_session.headers.update(original_session.headers)
            local_session.cookies.update(original_session.cookies)
            self.thread_locals.local_thread_session = local_session
        return self.thread_locals.local_thread_session
```

### Tests to Port/Create

- `tests/unit/test_credentials.py` (Opus: ~485 lines)
- `tests/unit/test_store_credentials.py` (GLM: ~351 lines)
- `tests/unit/test_streaming.py` (Opus: ~400 lines)

### Acceptance Criteria

- [ ] `S3Credentials` is a frozen dataclass with expiration checking
- [ ] `AuthContext.from_auth()` captures all necessary credentials
- [ ] `AuthContext.to_auth()` reconstructs functional Auth in workers
- [ ] `AuthContext` includes HTTP headers/cookies for HTTPS fallback
- [ ] `CredentialManager` caches credentials by provider
- [ ] `FileSystemFactory` creates filesystems with correct credentials
- [ ] Store uses dependency injection for testability
- [ ] Session cloning works for thread-based executors
- [ ] All existing Store tests pass

---

## Phase 4: Asset Model and Filtering

**Priority:** Medium
**Source:** GLM
**Estimated Effort:** 1-2 weeks

### Objective

Provide a rich, type-safe model for working with granule assets (files), enabling expressive filtering for download and open operations.

### Components to Port

| File | Lines | Key Features |
|------|-------|--------------|
| `store/asset.py` | ~470 | `Asset`, `AssetFilter`, helper functions |

### Asset Class (GLM)

```python
@dataclass(frozen=True)
class Asset:
    """Immutable representation of a granule asset."""
    href: str
    title: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None  # MIME type
    roles: FrozenSet[str] = field(default_factory=frozenset)
    bands: Optional[Tuple[str, ...]] = None
    gsd: Optional[float] = None
    file_size: Optional[int] = None
    checksum: Optional[str] = None

    def is_data(self) -> bool:
        return "data" in self.roles

    def is_thumbnail(self) -> bool:
        return "thumbnail" in self.roles

    def matches_type(self, content_type: str) -> bool:
        return self.type and self.type.lower() == content_type.lower()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to STAC asset dictionary."""
        result = {"href": self.href}
        if self.title:
            result["title"] = self.title
        if self.type:
            result["type"] = self.type
        if self.roles:
            result["roles"] = list(self.roles)
        return result
```

### AssetFilter Class (GLM)

```python
@dataclass(frozen=True)
class AssetFilter:
    """Immutable, composable filter for assets."""
    content_types: Optional[FrozenSet[str]] = None
    exclude_content_types: Optional[FrozenSet[str]] = None
    include_roles: Optional[FrozenSet[str]] = None
    exclude_roles: Optional[FrozenSet[str]] = None
    include_files: Optional[FrozenSet[str]] = None  # Glob patterns
    exclude_files: Optional[FrozenSet[str]] = None  # Glob patterns
    min_size: Optional[int] = None
    max_size: Optional[int] = None

    def matches(self, asset: Asset) -> bool:
        """Check if asset matches all filter criteria."""
        # ... filtering logic
        return True

    def combine(self, other: "AssetFilter") -> "AssetFilter":
        """Combine two filters with AND logic."""
        # Merge sets and take stricter size bounds
        ...


def filter_assets(assets: Iterable[Asset], filter: AssetFilter) -> List[Asset]:
    """Apply filter to a collection of assets."""
    return [a for a in assets if filter.matches(a)]
```

### Target API: Simple Filter Dict

```python
# Download or open STAC items directly
earthaccess.download(stac_items, "./test")

# Simple filter dict - matches against granule name, file name, or asset role/type
earthaccess.download(
    stac_items,
    "./test",
    filters={
        "include": ["B02", "B03"],      # Include these bands
        "exclude": ["*"]                 # Exclude everything else by default
    }
)
```

### Target API: Formal AssetFilter Class

```python
from earthaccess.store import AssetFilter, filter_assets

# Complex filtering with AssetFilter
asset_filter = AssetFilter(
    content_types=({"application/x-hdf5", "application/x-hdf"}),
    include_roles=({"data"}),
    exclude_roles=({"thumbnail", "metadata"}),
    include_files=({"*B02*.tif", "*B03*.tif"}),
    exclude_files=({"*B01*.tif", "*B06*.tif"}),
    max_size=1024 * 1024 * 1024,  # 1GB
)

# Apply filter to each granule manually
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

### Target API: Filter in download/open

```python
# Pass filter directly to download - pagination and filtering done lazily
earthaccess.download(
    results,
    "s3://my-bucket/test",  # Cloud target filesystem
    filter=asset_filter,
)

# Same for open
fileset = earthaccess.open(results, filter=asset_filter)
```

### Integration Points

1. Add `assets()` method to `DataGranule` returning `List[Asset]`
2. Export `Asset`, `AssetFilter`, `filter_assets` from package
3. Add `filter` parameter to `download()` and `open()` functions
4. Support simple dict-based filters for common use cases

### Tests to Port

- `tests/unit/test_asset.py` (GLM: ~453 lines)

### Acceptance Criteria

- [ ] `Asset` is a frozen dataclass with role checking methods
- [ ] `AssetFilter` supports all documented filter criteria
- [ ] `AssetFilter.combine()` merges filters correctly
- [ ] `filter_assets()` applies filters correctly
- [ ] `DataGranule.assets()` returns `List[Asset]`
- [ ] `download()` and `open()` accept `filter` parameter
- [ ] Simple dict-based filters work for common use cases
- [ ] Glob patterns work for `include_files` and `exclude_files`

---

## Phase 5: Parallel Execution and Distributed Computing

**Priority:** Medium
**Source:** Either (nearly identical) + Opus streaming
**Estimated Effort:** 1 week

### Objective

Provide a unified executor abstraction supporting serial, threaded, Dask, and Lithops execution. Enable efficient parallel I/O across workers with single authentication handshake.

### Design Goals from nextgen Vision

From the `earthaccess-nextgen.md` vision:

> Operations such as `.download(granules)` and `.open(granules)` should execute in parallel using distributed execution frameworks if they are available (e.g., Dask, Ray, Lithops).

The execution model should ensure:

* **Lazy pagination and bounded memory usage**
* **Efficient parallel I/O across workers**
* **Single authentication handshake per execution context**
* **Reuse of authenticated filesystem/session objects within workers**

### Components

| File | Source | Key Features |
|------|--------|--------------|
| `parallel.py` | Either | Executor ABC, get_executor factory |
| `streaming.py` | Opus | StreamingIterator, WorkerContext, StreamingExecutor |

### Executor Types

```python
class Executor(ABC):
    """Abstract base for all executors."""

    @abstractmethod
    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """Submit a single task."""

    @abstractmethod
    def map(self, fn: Callable, *iterables, **kwargs) -> Iterator:
        """Map function over iterables."""


class SerialExecutor(Executor):
    """Synchronous execution for debugging."""

class ThreadPoolExecutorWrapper(Executor):
    """Thread pool with progress bar support."""

class DaskDelayedExecutor(Executor):
    """Dask-based distributed execution."""

class LithopsEagerFunctionExecutor(Executor):
    """Serverless execution via Lithops."""


def get_executor(
    parallel: Union[str, Executor, bool, None] = True,
    max_workers: int = None,
    show_progress: bool = True,
) -> Executor:
    """Factory function for executor selection."""
```

### Target API: Parallel open() with xarray

```python
# Open in parallel - defaults to fsspec
# Uses running Dask cluster if available
fileset = earthaccess.open(results, filter=asset_filter)

# Use with xarray - leverages parallel I/O
import xarray as xr

ds = xr.open_mfdataset(
    fileset,
    engine="h5netcdf",
    parallel=True,  # Uses Dask cluster if available
    **kwargs
)
```

### Target API: Direct URL Access with Credentials

```python
# Or get storage options and use plain URLs
storage_options = earthaccess.get_s3_credentials(results)

ds = xr.open_mfdataset(
    filtered_asset_urls,
    engine="rioxarray",
    parallel=True,
    storage_options=storage_options,
    **kwargs
)
```

### Credential Propagation Model

From the nextgen vision:

> Credential handling is decoupled from task execution. Authentication is resolved once on the client side and serialized into a minimal, backend-specific credential payload (e.g., AWS credentials, Earthdata tokens). This payload is broadcast to workers and used to initialize filesystem or HTTP clients lazily on first use.

```python
class Store:
    def open(
        self,
        granules: Sequence[DataGranule],
        parallel: Union[str, Executor, bool, None] = True,
        **kwargs,
    ) -> List[EarthAccessFile]:
        """Open granule files with parallel execution support."""
        executor = get_executor(parallel)
        self._set_executor_type(parallel)

        if self._use_session_cloning():
            # Thread-based: clone session (GLM pattern)
            # Avoids N authentication requests for N files
            session = self._auth.get_session()
            return list(executor.map(
                lambda g: self._open_with_cloned_session(g, session),
                granules,
            ))
        else:
            # Distributed: ship auth context (Opus pattern)
            # Workers can reconstruct full Auth if needed
            context = AuthContext.from_auth(self._auth)
            return list(executor.map(
                lambda g: self._open_with_context(g, context),
                granules,
            ))
```

### StreamingExecutor with Backpressure

```python
class StreamingExecutor:
    """Iterator-based executor with backpressure support."""

    def __init__(
        self,
        executor: Optional[Executor] = None,
        max_workers: int = 4,
        prefetch: int = 2,
        show_progress: bool = True,
    ):
        self.executor = executor or ThreadPoolExecutor(max_workers=max_workers)
        self.prefetch = prefetch
        self.worker_context = WorkerContext()

    def map(
        self,
        func: Callable[[T], R],
        items: Iterable[T],
        auth_context: Optional[AuthContext] = None,
    ) -> Iterator[R]:
        """Map function over items with streaming and backpressure.

        Items are lazily paginated and partitioned into work units
        that can be distributed across available workers.
        """
        def worker_func(item: T) -> R:
            if auth_context:
                auth = auth_context.to_auth()
                self.worker_context.set_auth(auth)
            return func(item)

        safe_items = StreamingIterator(items)
        # ... backpressure logic with prefetch buffer
```

### Tests to Port

- `tests/unit/test_parallel.py` (~180 lines)
- `tests/unit/test_executor_strategy.py` (~125 lines)
- `tests/unit/test_streaming.py` (~400 lines)

### Acceptance Criteria

- [ ] `get_executor()` returns correct executor for each parallel option
- [ ] SerialExecutor works for debugging
- [ ] ThreadPoolExecutorWrapper shows progress
- [ ] DaskDelayedExecutor integrates with Dask clusters
- [ ] LithopsEagerFunctionExecutor works with Lithops
- [ ] StreamingExecutor handles backpressure correctly
- [ ] Auth context is properly shipped to distributed workers
- [ ] Session cloning works for thread-based executors (avoids N auth requests)
- [ ] `earthaccess.get_s3_credentials()` returns usable storage_options

---

## Phase 6: Target Filesystem Abstraction

**Priority:** Low
**Source:** Either (identical implementations)
**Estimated Effort:** 0.5 weeks

### Objective

Abstract the target filesystem for downloads beyond local storage to include cloud object stores.

### Component

| File | Lines | Key Features |
|------|-------|--------------|
| `target_filesystem.py` | ~240 | `TargetFileSystem` with S3, GCS, Azure support |

### API

```python
class TargetFileSystem:
    """Abstraction for download target locations."""

    def __init__(self, path: str, storage_options: Optional[Dict] = None):
        self.path = path
        self.storage_options = storage_options or {}
        self._fs = self._create_filesystem()

    def _create_filesystem(self) -> AbstractFileSystem:
        """Create appropriate filesystem based on path."""
        if self.path.startswith("s3://"):
            return S3FileSystem(**self.storage_options)
        elif self.path.startswith("gs://"):
            return GCSFileSystem(**self.storage_options)
        elif self.path.startswith("az://"):
            return AzureBlobFileSystem(**self.storage_options)
        else:
            return LocalFileSystem()

    def write(self, source_path: str, content: bytes) -> str:
        """Write content to target filesystem."""
        target_path = self._get_target_path(source_path)
        with self._fs.open(target_path, "wb") as f:
            f.write(content)
        return target_path
```

### Usage

```python
# Download to S3
earthaccess.download(
    granules,
    "s3://my-bucket/data/",
    storage_options={"key": "...", "secret": "..."}
)

# Download to GCS
earthaccess.download(
    granules,
    "gs://my-bucket/data/",
    storage_options={"token": "..."}
)
```

### Tests to Port

- `tests/unit/test_target_filesystem.py` (~345 lines)

### Acceptance Criteria

- [ ] Downloads work to local filesystem
- [ ] Downloads work to S3 with credentials
- [ ] Downloads work to GCS with credentials
- [ ] Storage options are properly passed through

---

## Phase 7: Results Enhancement

**Priority:** Medium
**Source:** Both
**Estimated Effort:** 1 week

### Objective

Enhance DataGranule and DataCollection with STAC conversion and asset access methods.

### Enhancements to DataGranule

```python
class DataGranule:
    """Represents a granule from CMR."""

    def to_stac(self) -> Dict[str, Any]:
        """Convert to STAC Item dictionary."""
        from earthaccess.stac import umm_granule_to_stac_item
        return umm_granule_to_stac_item(self._granule, self.collection_id())

    def assets(self) -> List[Asset]:
        """Get all assets as Asset objects."""
        from earthaccess.store import Asset
        assets = []
        for url_info in self._granule.get("umm", {}).get("RelatedUrls", []):
            asset = Asset(
                href=url_info.get("URL"),
                type=url_info.get("MimeType"),
                roles=self._url_type_to_roles(url_info.get("Type")),
                file_size=url_info.get("FileSize"),
            )
            assets.append(asset)
        return assets

    def data_assets(self) -> List[Asset]:
        """Get only data assets."""
        return [a for a in self.assets() if a.is_data()]
```

### Enhancements to DataCollection

```python
class DataCollection:
    """Represents a collection from CMR."""

    def to_stac(self) -> Dict[str, Any]:
        """Convert to STAC Collection dictionary."""
        from earthaccess.stac import umm_collection_to_stac_collection
        return umm_collection_to_stac_collection(self._collection)
```

### Lazy Pagination (from nextgen vision)

```python
class SearchResults:
    """Lazy paginated search results."""

    def __init__(self, query: QueryBase, auth: Auth):
        self._query = query
        self._auth = auth
        self._current_page = 0
        self._page_size = 2000
        self._total_hits: Optional[int] = None

    def __iter__(self) -> Iterator[DataGranule]:
        """Iterate through all results with lazy pagination."""
        for page in self.pages():
            yield from page

    def pages(self) -> Iterator[List[DataGranule]]:
        """Iterate through result pages."""
        while True:
            page = self._fetch_page(self._current_page)
            if not page:
                break
            yield page
            self._current_page += 1

    def __len__(self) -> int:
        """Get total number of results (fetches first page if needed)."""
        if self._total_hits is None:
            self._fetch_page(0)
        return self._total_hits
```

### Acceptance Criteria

- [ ] `DataGranule.to_stac()` produces valid STAC Items
- [ ] `DataGranule.assets()` returns `List[Asset]`
- [ ] `DataCollection.to_stac()` produces valid STAC Collections
- [ ] Lazy pagination works with large result sets
- [ ] Memory usage is bounded for large searches

---

## Phase 8: VirtualiZarr Integration

**Priority:** Low
**Source:** Vision (earthaccess-nextgen.md)
**Estimated Effort:** 1-2 weeks

### Objective

Enable cloud-native virtual dataset access using VirtualiZarr, allowing users to create virtual Zarr stores from DMR++ metadata without downloading full data files.

### Target API: Virtual Datasets

```python
# Leverage virtualizarr for cloud-native access
vds = earthaccess.open_virtual_mfdataset(
    results,  # Will paginate to get dmrpp file URLs first
    group="/gt1l/land_ice_segments",  # Optional (ICESat-2)
    concat_dim="time",
    load=False  # If True, loads coords/dims for fancy indexing
)

# Work with virtual dataset
print(vds)  # Shows structure without loading data

# Subset and load only what's needed
subset = vds.sel(time="2020-06")
data = subset.compute()  # Only now downloads actual data
```

### Target API: Persist to Icechunk

```python
# Persist virtual store as Icechunk for fast repeated access
vds.virtualizarr.to_icechunk(icechunk_store)

# Later: load from Icechunk (no CMR query needed)
import icechunk
store = icechunk.IcechunkStore.open(icechunk_store)
ds = xr.open_zarr(store)
```

### Implementation Notes

This phase builds on existing `dmrpp_zarr.py` functionality:

```python
def open_virtual_mfdataset(
    granules: Union[Sequence[DataGranule], SearchResults],
    group: Optional[str] = None,
    concat_dim: str = "time",
    load: bool = False,
    parallel: Union[str, Executor, bool, None] = True,
    **kwargs,
) -> xr.Dataset:
    """Open multiple granules as a virtual multi-file dataset.

    Uses DMR++ metadata to create a virtual Zarr store without
    downloading full data files.

    Args:
        granules: DataGranules or SearchResults (will paginate lazily)
        group: HDF5 group path (for hierarchical datasets like ICESat-2)
        concat_dim: Dimension to concatenate along
        load: If True, load coordinate/dimension data for indexing
        parallel: Parallel execution strategy

    Returns:
        xarray.Dataset backed by virtual Zarr store
    """
    # 1. Get DMR++ URLs from granules (paginate if SearchResults)
    dmrpp_urls = _get_dmrpp_urls(granules)

    # 2. Parse DMR++ to virtual Zarr references (parallel)
    executor = get_executor(parallel)
    references = list(executor.map(_parse_dmrpp, dmrpp_urls))

    # 3. Combine into multi-file virtual dataset
    vds = virtualizarr.open_virtual_mfdataset(
        references,
        concat_dim=concat_dim,
        **kwargs,
    )

    # 4. Optionally load coordinates for fancy indexing
    if load:
        vds = vds.load_coords()

    return vds
```

### Integration Points

1. Expose `open_virtual_mfdataset()` at package level
2. Integrate with existing `dmrpp_zarr.py` code
3. Support both DataGranules and SearchResults as input
4. Use parallel executor for DMR++ parsing

### Acceptance Criteria

- [ ] `open_virtual_mfdataset()` works with DataGranules
- [ ] `open_virtual_mfdataset()` works with SearchResults (lazy pagination)
- [ ] `group` parameter works for hierarchical datasets
- [ ] `load=True` loads coordinate data for indexing
- [ ] Virtual dataset can be persisted to Icechunk
- [ ] Parallel DMR++ parsing uses configured executor

---

## Implementation Timeline

```
Week 1-2:   Phase 1 - Query Architecture (GranuleQuery, StacItemQuery, validation)
Week 3-4:   Phase 2 - Bidirectional STAC Conversion (converters, lazy pagination)
Week 5-7:   Phase 3 - Credential Management & Store Refactoring (DI, auth context)
Week 8-9:   Phase 4 - Asset Model and Filtering (Asset, AssetFilter, filter parameter)
Week 10:    Phase 5 - Parallel Execution Framework (executors, streaming)
Week 10.5:  Phase 6 - Target Filesystem Abstraction (S3, GCS, Azure targets)
Week 11:    Phase 7 - Results Enhancement (to_stac, assets, lazy pagination)
Week 12-13: Phase 8 - VirtualiZarr Integration (open_virtual_mfdataset, Icechunk)
Week 14:    Integration testing, documentation, release prep
```

**Total estimated time:** 12-14 weeks

---

## Risk Mitigation

### Merge Conflicts

- **Strategy:** Work on isolated packages first (`query/`, `stac/`)
- **Mitigation:** Create feature branches for each phase, merge incrementally

### Backward Compatibility

- **Strategy:** All new features are additive; existing API unchanged
- **Mitigation:** Comprehensive test suite, deprecation warnings for future changes

### Performance Regression

- **Strategy:** Benchmark critical paths before and after each phase
- **Mitigation:** Profile `search_data()`, `download()`, `open()` with large datasets

### Testing Coverage

- **Strategy:** Port all tests from both branches
- **Mitigation:** Require passing tests before merging each phase

---

## Success Metrics

1. **Query flexibility:** Users can build queries without authentication
2. **STAC interoperability:** External STAC catalogs work with earthaccess
3. **Asset filtering:** Users can filter downloads by role, type, size
4. **Distributed execution:** Dask and Lithops work correctly with auth
5. **Backward compatibility:** All existing notebooks and scripts work unchanged
6. **Test coverage:** >90% coverage on new code
7. **Performance:** No regression in common operations

---

## Conclusion

This implementation plan combines the best of both branches to achieve the vision in `earthaccess-nextgen.md`:

| Capability | Source | Phase |
|------------|--------|-------|
| Auth-decoupled queries | Opus | 1 |
| StacItemQuery (STAC-native queries) | Vision | 1 |
| Validation accumulator | Opus | 1 |
| Dual output (CMR/STAC) | Opus | 1 |
| Geometry file support (auto-simplify) | Vision | 1 |
| Bidirectional STAC | Opus | 2 |
| Lazy pagination | Vision | 2 |
| ODC-STAC integration | Vision | 2 |
| GeoDataFrame conversion | Vision | 2 |
| S3Credentials dataclass | GLM | 3 |
| Auth reconstruction | Opus | 3 |
| FileSystemFactory | GLM | 3 |
| Session cloning | GLM | 3 |
| Asset/AssetFilter | GLM | 4 |
| Simple filter dict | Vision | 4 |
| Parallel executors | Both | 5 |
| Dask/Lithops support | Both | 5 |
| xarray parallel open | Vision | 5 |
| get_s3_credentials() | Vision | 5 |
| Target filesystem (S3, GCS) | Both | 6 |
| DataGranule.to_stac() | Both | 7 |
| DataGranule.assets() | GLM | 7 |
| open_virtual_mfdataset() | Vision | 8 |
| Icechunk persistence | Vision | 8 |

### Key API Endpoints After Implementation

```python
# Query building (Phase 1)
from earthaccess.query import GranuleQuery, StacItemQuery

# STAC conversion (Phase 2)
from earthaccess.stac import stac_item_to_data_granule

# Asset filtering (Phase 4)
from earthaccess.store import AssetFilter, filter_assets

# High-level API
results = earthaccess.search_data(query=query)
files = earthaccess.download(results, "./data", filter=asset_filter)
fileset = earthaccess.open(results, filter=asset_filter)
storage_options = earthaccess.get_s3_credentials(results)
vds = earthaccess.open_virtual_mfdataset(results, concat_dim="time")
```

The result will be an earthaccess library that achieves the vision outlined in `earthaccess-nextgen.md`: cloud-native, STAC-compatible, horizontally scalable, and interoperable with the broader geospatial Python ecosystem.
