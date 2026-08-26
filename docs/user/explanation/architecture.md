# Architecture

This document describes the architecture of earthaccess's query, STAC conversion, and streaming components.

## Overview

The earthaccess library provides a unified interface for searching, accessing, and processing NASA Earthdata. The architecture follows SOLID principles with clear separation of concerns.

```mermaid
graph TB
    subgraph "User Interface"
        API[earthaccess API]
        QO[Query Objects]
    end

    subgraph "Query Layer"
        GQ[GranuleQuery]
        CQ[CollectionQuery]
        QB[QueryBase]
    end

    subgraph "Search Layer"
        DG[DataGranules]
        DC[DataCollections]
        CMR[NASA CMR API]
    end

    subgraph "Results Layer"
        DGR[DataGranule Results]
        DCR[DataCollection Results]
    end

    subgraph "STAC Layer"
        SC[STAC Converters]
        SI[STAC Items]
        SCO[STAC Collections]
    end

    subgraph "Processing Layer"
        SE[StreamingExecutor]
        AC[AuthContext]
        CM[CredentialManager]
    end

    subgraph "Access Layer"
        ST[Store]
        S3[S3 Direct Access]
        HTTP[HTTPS Access]
    end

    API --> QO
    QO --> GQ
    QO --> CQ
    GQ --> QB
    CQ --> QB

    GQ -->|to_cmr| DG
    CQ -->|to_cmr| DC
    DG --> CMR
    DC --> CMR

    CMR --> DGR
    CMR --> DCR

    DGR -->|to_stac| SC
    DCR -->|to_stac| SC
    SC --> SI
    SC --> SCO

    DGR --> SE
    SE --> AC
    AC --> CM
    CM --> ST
    ST --> S3
    ST --> HTTP
```

## Component Architecture

### Query Layer

The query layer provides a fluent API for building search queries that can be converted to either CMR or STAC format.

```mermaid
classDiagram
    class QueryBase {
        <<abstract>>
        +_params: Dict
        +_temporal_ranges: List[DateRange]
        +_spatial: SpatialType
        +parameters(**kwargs) Self
        +validate() ValidationResult
        +to_cmr() Dict
        +to_stac() Dict
        +copy() Self
    }

    class GranuleQuery {
        +short_name(name) Self
        +temporal(from, to) Self
        +bounding_box(w, s, e, n) Self
        +polygon(coords) Self
        +cloud_cover(min, max) Self
        +granule_name(name) Self
    }

    class CollectionQuery {
        +keyword(text) Self
        +daac(name) Self
        +cloud_hosted(bool) Self
        +has_granules(bool) Self
    }

    class BoundingBox {
        +west: float
        +south: float
        +east: float
        +north: float
        +to_cmr() str
        +to_stac() List
    }

    class DateRange {
        +start: datetime
        +end: datetime
        +exclude_boundary: bool
        +to_cmr() str
        +to_stac() str
    }

    QueryBase <|-- GranuleQuery
    QueryBase <|-- CollectionQuery
    GranuleQuery --> BoundingBox
    GranuleQuery --> DateRange
    CollectionQuery --> BoundingBox
    CollectionQuery --> DateRange
```

### STAC Conversion Layer

The STAC layer provides bidirectional conversion between NASA CMR UMM format and STAC format.

```mermaid
flowchart LR
    subgraph "CMR Format"
        UG[UMM Granule]
        UC[UMM Collection]
    end

    subgraph "earthaccess Results"
        DG[DataGranule]
        DC[DataCollection]
    end

    subgraph "STAC Format"
        SI[STAC Item]
        SC[STAC Collection]
    end

    UG -->|"DataGranule()"| DG
    UC -->|"DataCollection()"| DC

    DG -->|"to_stac()"| SI
    DC -->|"to_stac()"| SC

    SI -->|"stac_item_to_data_granule()"| DG
    SC -->|"stac_collection_to_data_collection()"| DC
```

### Streaming Execution Layer

The streaming layer enables lazy, memory-efficient processing of large result sets.

```mermaid
sequenceDiagram
    participant User
    participant StreamingExecutor
    participant ThreadPool
    participant AuthContext
    participant CredentialManager
    participant Worker

    User->>StreamingExecutor: map(func, granules)
    StreamingExecutor->>AuthContext: Create context
    AuthContext->>CredentialManager: Get credentials
    CredentialManager-->>AuthContext: S3Credentials

    loop For each granule
        StreamingExecutor->>ThreadPool: Submit task
        ThreadPool->>Worker: Execute with context
        Worker->>AuthContext: Activate in thread
        Worker-->>StreamingExecutor: Result (via queue)
        StreamingExecutor-->>User: Yield result (lazy)
    end
```

### Credential Management

The credential manager provides thread-safe, cached access to S3 credentials.

```mermaid
flowchart TB
    subgraph "CredentialManager"
        CC[CredentialCache]
        PI[Provider Inference]
        PM[Provider Mapping]
    end

    subgraph "Credential Sources"
        AUTH[Auth.get_s3_credentials]
        DAAC[DAAC Endpoints]
    end

    subgraph "Usage"
        S3FS[s3fs]
        BOTO[boto3]
        FSSPEC[fsspec]
    end

    URL[S3 URL] --> PI
    PI --> PM
    PM --> CC
    CC -->|cache miss| AUTH
    AUTH --> DAAC
    DAAC -->|credentials| CC
    CC -->|S3Credentials| S3FS
    CC -->|to_boto3_dict| BOTO
    CC -->|to_dict| FSSPEC
```

## Use Cases

### Use Case 1: Search and Download with Query Objects

```python
from earthaccess import GranuleQuery, search_data, download, login

# Authenticate
login()

# Build a query
query = (
    GranuleQuery()
    .short_name("ATL06")
    .temporal("2020-01-01", "2020-03-31")
    .bounding_box(-50, 60, -40, 70)
    .cloud_cover(0, 20)
)

# Validate before searching
validation = query.validate()
if not validation.is_valid:
    print(f"Query errors: {validation.errors}")
else:
    # Search and download
    granules = search_data(query=query, count=10)
    files = download(granules, "./data")
```

### Use Case 2: Convert Results to STAC for Interoperability

```python
import earthaccess
from pystac import ItemCollection

# Search for granules
granules = earthaccess.search_data(
    short_name="MUR-JPL-L4-GLOB-v4.1",
    temporal=("2024-01-01", "2024-01-07"),
    count=7
)

# Convert to STAC Items
stac_items = [g.to_stac() for g in granules]

# Create a STAC ItemCollection for use with other tools
item_collection = ItemCollection(items=stac_items)

# Save as GeoJSON
with open("granules.json", "w") as f:
    f.write(item_collection.to_dict())
```

### Use Case 3: Stream Processing Large Datasets

```python
from earthaccess import search_data, login
from earthaccess.streaming import StreamingExecutor, AuthContext
import xarray as xr

login()

# Search for many granules
granules = search_data(short_name="ATL06", count=1000)

# Create auth context for distributed processing
auth_context = AuthContext.from_earthaccess()

# Define processing function
def process_granule(granule):
    # This runs in a worker thread with credentials
    url = granule.data_links()[0]
    ds = xr.open_dataset(url)
    return ds.attrs.get("title", "Unknown")

# Stream results with backpressure
with StreamingExecutor(max_workers=4, auth_context=auth_context) as executor:
    for result in executor.map(process_granule, granules):
        print(result)
```

### Use Case 4: Credential Management for Direct S3 Access

```python
from earthaccess import login
from earthaccess.credentials import CredentialManager
import s3fs

login()

# Create credential manager
cred_manager = CredentialManager()

# Get credentials for a specific S3 URL
url = "s3://nsidc-cumulus-prod-protected/ATLAS/ATL06/..."
creds = cred_manager.get_credentials_for_url(url)

# Use with s3fs
fs = s3fs.S3FileSystem(**creds.to_dict())
with fs.open(url) as f:
    data = f.read(1024)
```

### Use Case 5: Building STAC-Compatible Search Queries

```python
from earthaccess import GranuleQuery
from pystac_client import Client

# Build query using earthaccess
query = (
    GranuleQuery()
    .short_name("HLSL30")
    .temporal("2023-06-01", "2023-06-30")
    .bounding_box(-122.5, 37.5, -122.0, 38.0)
)

# Get STAC parameters
stac_params = query.to_stac()
# {'collections': ['HLSL30'],
#  'datetime': '2023-06-01T00:00:00Z/2023-06-30T23:59:59Z',
#  'bbox': [-122.5, 37.5, -122.0, 38.0]}

# Use with a STAC API client
catalog = Client.open("https://cmr.earthdata.nasa.gov/stac")
search = catalog.search(**stac_params)
items = list(search.items())
```

## Data Flow

### Search Flow

```mermaid
flowchart LR
    A[User Query] --> B{Query Type?}
    B -->|kwargs| C[Direct to CMR]
    B -->|GranuleQuery| D[Validate]
    D --> E[to_cmr()]
    E --> C
    C --> F[DataGranules/DataCollections]
    F --> G[CMR API Request]
    G --> H[UMM JSON Response]
    H --> I[DataGranule/DataCollection Objects]
    I --> J[Return to User]
```

### Access Flow

```mermaid
flowchart TB
    A[DataGranule] --> B{Access Method?}
    B -->|download| C[Store.get]
    B -->|open| D[Store.open]

    C --> E{In Cloud?}
    D --> E

    E -->|Yes| F[S3 Direct Access]
    E -->|No| G[HTTPS Download]

    F --> H[CredentialManager]
    H --> I[Get/Cache Credentials]
    I --> J[S3FileSystem]

    G --> K[Authenticated Session]
    K --> L[Download Files]

    J --> M[Return File Handles]
    L --> N[Return File Paths]
```

## Extension Points

The architecture is designed to be extensible:

1. **New Query Types**: Extend `QueryBase` to create new query types
2. **Custom Converters**: Add new STAC conversion functions in `stac/converters.py`
3. **Processing Backends**: `StreamingExecutor` can be extended for Dask/Ray
4. **Credential Providers**: `CredentialManager` supports custom provider mappings
