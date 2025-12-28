---
title: 'Architecture Comparison: `stac-distributed-glm` vs `stac-distributed-opus`'

---

# Architecture Comparison: `stac-distributed-glm` vs `stac-distributed-opus`

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [High-Level Overview](#high-level-overview)
3. [Directory Structure Comparison](#directory-structure-comparison)
4. [Query Architecture](#query-architecture)
5. [STAC Conversion Strategy](#stac-conversion-strategy)
6. [Credential Management](#credential-management)
7. [Distributed Computing](#distributed-computing)
8. [Store Architecture](#store-architecture)
9. [Asset Handling](#asset-handling)
10. [Type Safety and Validation](#type-safety-and-validation)
11. [Testing Strategy](#testing-strategy)
12. [Backward Compatibility](#backward-compatibility)
13. [Use Case Scenarios](#use-case-scenarios)
14. [Summary: Pros and Cons](#summary-pros-and-cons)
15. [Recommendations](#recommendations)

---

## Executive Summary

Both branches represent significant architectural refactoring efforts aimed at modernizing earthaccess with STAC integration, distributed computing support, and SOLID-compliant design. While they share similar goals, they take notably different approaches in several key areas.

### At a Glance

| Aspect | stac-distributed-glm | stac-distributed-opus |
|--------|---------------------|----------------------|
| **Primary Focus** | Store-centric refactoring | Query-centric refactoring |
| **Philosophy** | "Rebuild the data access layer" | "Enhance the query and conversion layer" |
| **New Modules** | `store_components/`, `main_store.py` | `query/`, `stac/`, `streaming.py` |
| **Lines Added** | +18,888 | +17,771 |
| **Lines Removed** | -1,038 | -318 |
| **Files Modified** | 50 files | 40 files |
| **New Test Files** | ~12 test files | ~8 test files |
| **Breaking Changes** | Internal (shim for compat) | Minimal |

### Key Differentiators

| Capability | GLM Approach | Opus Approach | Winner |
|------------|-------------|---------------|--------|
| Query building | Auth-coupled, method chaining | Auth-decoupled, kwargs + chaining | **Opus** |
| STAC conversion | One-way (CMR → STAC) | Bidirectional (CMR ↔ STAC) | **Opus** |
| Store architecture | Complete DI refactoring | Incremental modification | **GLM** |
| Asset handling | Rich Asset/AssetFilter classes | Defers to pystac | **GLM** |
| Credential management | Type-safe S3Credentials | Dict-based with Auth reconstruction | **GLM** |
| Validation | Exception-based (fail-fast) | Accumulator pattern (collect all) | **Opus** |
| Merge risk | Higher (more invasive) | Lower (less invasive) | **Opus** |

---

## High-Level Overview

### Design Philosophy

#### stac-distributed-glm: "Component-Based Store Refactoring"

This branch takes the position that the `Store` class is the heart of earthaccess and needs a complete architectural overhaul. The approach:

1. **Decompose** the monolithic Store into focused components (`store_components/`)
2. **Inject dependencies** (CredentialManager, FileSystemFactory) rather than creating them inline
3. **Model domain concepts** explicitly (Asset, AssetFilter, S3Credentials)
4. **Maintain backward compatibility** through a shim package (`store/__init__.py`)

The result is a more testable, maintainable codebase where each component has a single responsibility.

#### stac-distributed-opus: "Query-First STAC Integration"

This branch takes the position that the query interface is the primary user touchpoint and should be enhanced first. The approach:

1. **Create a dedicated query package** with rich type classes
2. **Enable bidirectional STAC conversion** for ecosystem interoperability
3. **Decouple queries from authentication** for flexibility
4. **Minimize changes to existing Store** to reduce risk

The result is a more flexible query API that works seamlessly with both CMR and STAC backends.

### Change Impact Visualization

```
                    stac-distributed-glm                    stac-distributed-opus
                    =====================                   =====================

    ┌─────────────────────────────────────┐     ┌─────────────────────────────────────┐
    │           earthaccess/              │     │           earthaccess/              │
    │  ┌─────────────────────────────┐    │     │                                     │
    │  │     store_components/       │◄───┼─────┼── NEW PACKAGE                       │
    │  │  • credentials.py           │    │     │                                     │
    │  │  • filesystems.py           │    │     │  ┌─────────────────────────────┐    │
    │  │  • asset.py                 │    │     │  │         query/              │◄───┼── NEW PACKAGE
    │  │  • cloud_transfer.py        │    │     │  │  • base.py                  │    │
    │  │  • query.py                 │    │     │  │  • types.py                 │    │
    │  │  • geometry.py              │    │     │  │  • granule_query.py         │    │
    │  │  • results.py               │    │     │  │  • collection_query.py      │    │
    │  │  • stac_search.py           │    │     │  │  • validation.py            │    │
    │  └─────────────────────────────┘    │     │  └─────────────────────────────┘    │
    │                                     │     │                                     │
    │  ┌─────────────────────────────┐    │     │  ┌─────────────────────────────┐    │
    │  │     main_store.py           │◄───┼─────┼── │         stac/               │◄───┼── NEW PACKAGE
    │  │  (replaces store.py)        │    │     │  │  • converters.py            │    │
    │  └─────────────────────────────┘    │     │  └─────────────────────────────┘    │
    │                                     │     │                                     │
    │  ┌─────────────────────────────┐    │     │  ┌─────────────────────────────┐    │
    │  │     parallel.py             │◄───┼─────┼── │     streaming.py            │◄───┼── NEW FILE
    │  └─────────────────────────────┘    │     │  └─────────────────────────────┘    │
    │                                     │     │                                     │
    │  ┌─────────────────────────────┐    │     │  ┌─────────────────────────────┐    │
    │  │  target_filesystem.py       │◄───┼─────┼── │     credentials.py          │◄───┼── NEW FILE
    │  └─────────────────────────────┘    │     │  └─────────────────────────────┘    │
    │                                     │     │                                     │
    │  ┌─────────────────────────────┐    │     │  ┌─────────────────────────────┐    │
    │  │     store/__init__.py       │◄───┼─────┼── │     store.py                │    │
    │  │  (backward compat shim)     │    │     │  │  (modified in-place)        │    │
    │  └─────────────────────────────┘    │     │  └─────────────────────────────┘    │
    └─────────────────────────────────────┘     └─────────────────────────────────────┘
```

---

## Directory Structure Comparison

### stac-distributed-glm Structure

```
earthaccess/
├── store_components/           # NEW: Core modular components (8 files, ~3,200 lines)
│   ├── __init__.py            # Public exports
│   ├── asset.py               # Asset + AssetFilter (469 lines)
│   ├── cloud_transfer.py      # Cloud-to-cloud transfers (500 lines)
│   ├── credentials.py         # CredentialManager, S3Credentials, AuthContext (309 lines)
│   ├── filesystems.py         # FileSystemFactory (178 lines)
│   ├── geometry.py            # Geometry loading/simplification (242 lines)
│   ├── query.py               # GranuleQuery, CollectionQuery (824 lines)
│   ├── results.py             # ResultsBase, LazyResultsBase, StreamingExecutor (387 lines)
│   └── stac_search.py         # External STAC catalog search (233 lines)
├── store/                     # NEW: Backward compatibility shim
│   └── __init__.py            # Re-exports from store_components + main_store
├── main_store.py              # RENAMED: Refactored Store class (1,226 lines)
├── parallel.py                # NEW: Executor framework (596 lines)
├── target_filesystem.py       # NEW: Target filesystem abstraction (241 lines)
├── results.py                 # MODIFIED: Enhanced with STAC methods (+319 lines)
├── api.py                     # MODIFIED: Updated to use new components
└── __init__.py                # MODIFIED: New exports added
```

**Total new code:** ~6,500+ lines across 12 new files

### stac-distributed-opus Structure

```
earthaccess/
├── query/                     # NEW: Dedicated query package (5 files, ~1,600 lines)
│   ├── __init__.py            # Package exports
│   ├── base.py                # QueryBase abstract class (181 lines)
│   ├── collection_query.py    # CollectionQuery (392 lines)
│   ├── granule_query.py       # GranuleQuery (503 lines)
│   ├── types.py               # BoundingBox, DateRange, Point, Polygon (327 lines)
│   └── validation.py          # ValidationResult, validators (175 lines)
├── stac/                      # NEW: STAC conversion package (2 files, ~890 lines)
│   ├── __init__.py            # Package exports
│   └── converters.py          # Bidirectional UMM ↔ STAC conversion (863 lines)
├── credentials.py             # NEW: CredentialManager standalone (555 lines)
├── streaming.py               # NEW: AuthContext, WorkerContext, StreamingExecutor (587 lines)
├── parallel.py                # NEW: Executor framework (615 lines)
├── target_filesystem.py       # NEW: Target filesystem abstraction (241 lines)
├── results.py                 # MODIFIED: Enhanced with to_stac(), to_dict() (+456 lines)
├── store.py                   # MODIFIED: Added credential integration (+644 lines)
├── api.py                     # MODIFIED: Accepts query objects
└── __init__.py                # MODIFIED: New exports added
```

**Total new code:** ~5,500+ lines across 10 new files

### Structural Comparison Table

| Aspect | GLM | Opus | Analysis |
|--------|-----|------|----------|
| **New packages** | 2 (`store_components/`, `store/`) | 2 (`query/`, `stac/`) | Equal in count, different focus |
| **New standalone modules** | 3 (`main_store.py`, `parallel.py`, `target_filesystem.py`) | 4 (`credentials.py`, `streaming.py`, `parallel.py`, `target_filesystem.py`) | Opus has more standalone files |
| **Organization style** | Hierarchical (everything under `store_components/`) | Flat (multiple top-level packages) | GLM is more centralized |
| **Component coupling** | Tighter (components reference each other) | Looser (packages more independent) | Opus is more modular |
| **Discovery** | Easier (one place to look) | Harder (multiple locations) | GLM is easier to navigate |

### Why This Matters

**GLM's centralized approach** means:
- ✅ All store-related code is in one place
- ✅ Easier to understand dependencies between components
- ❌ Changes to one component may ripple to others
- ❌ Harder to adopt components individually

**Opus's distributed approach** means:
- ✅ Packages are more independent and reusable
- ✅ Easier to adopt individual features incrementally
- ❌ Need to look in multiple places to understand the system
- ❌ Risk of code duplication between packages

---

## Query Architecture

### Overview

The query architecture determines how users construct search queries. This is a critical user-facing API that affects usability and flexibility.

### stac-distributed-glm Query Design

**Location:** `store_components/query.py` (824 lines)

```python
class BaseQuery:
    """Base class for all query types."""

    def __init__(
        self,
        auth: Auth,                                    # Required at construction
        backend: Literal["cmr", "stac"] = "cmr",       # Backend choice upfront
    ) -> None:
        self.auth = auth
        self.backend = backend
        self._params: Dict[str, Any] = {}

class GranuleQuery(BaseQuery):
    """Query builder for granule searches."""

    def short_name(self, *names: str) -> "GranuleQuery":
        """Add short name filter."""
        self._params["short_name"] = list(names)
        return self

    def bounding_box(
        self,
        west: float,
        south: float,
        east: float,
        north: float,
    ) -> "GranuleQuery":
        """Add bounding box filter."""
        bbox = self._validate_bbox((west, south, east, north))
        self._params["bounding_box"] = bbox
        return self
```

**Characteristics:**

| Feature | Implementation | Implication |
|---------|---------------|-------------|
| Auth requirement | Required at `__init__` | Cannot create query without authentication |
| Backend selection | Constructor parameter | Must decide CMR vs STAC upfront |
| Parameter style | Method chaining only | No kwargs alternative |
| Validation | Raises `QueryValidationError` | Fails on first invalid parameter |
| Type coercion | Inline in methods | Validation logic mixed with query building |

**Example usage:**

```python
import earthaccess

auth = earthaccess.login()

# Must provide auth at construction
query = GranuleQuery(auth, backend="cmr")
query.short_name("ATL03").temporal("2020-01", "2020-12").bounding_box(-180, -90, 180, 90)

results = query.execute()
```

### stac-distributed-opus Query Design

**Location:** `query/` package (5 files, ~1,600 lines)

```python
# query/base.py
class QueryBase(ABC):
    """Abstract base class for query objects."""

    def __init__(self, **kwargs: Any) -> None:    # No auth required!
        self._params: Dict[str, Any] = {}
        self._temporal_ranges: List[DateRange] = []
        self._spatial: Optional[SpatialType] = None

        # Apply any named parameters via introspection
        if kwargs:
            self.parameters(**kwargs)

    def parameters(self, **kwargs: Any) -> Self:
        """Apply query parameters as keyword arguments."""
        methods = dict(getmembers(self, predicate=ismethod))
        for key, val in kwargs.items():
            if key not in methods:
                raise ValueError(f"Unknown parameter: {key}")
            if isinstance(val, tuple):
                methods[key](*val)
            else:
                methods[key](val)
        return self

# query/types.py
@dataclass(frozen=True)
class BoundingBox:
    """Immutable bounding box with dual output formats."""
    west: float
    south: float
    east: float
    north: float

    def to_cmr(self) -> str:
        """Convert to CMR format: 'west,south,east,north'"""
        return f"{self.west},{self.south},{self.east},{self.north}"

    def to_stac(self) -> List[float]:
        """Convert to STAC format: [west, south, east, north]"""
        return [self.west, self.south, self.east, self.north]

    @classmethod
    def from_coords(cls, coords: Sequence[float]) -> "BoundingBox":
        """Factory method from coordinate sequence."""
        if len(coords) != 4:
            raise ValueError(f"Expected 4 coordinates, got {len(coords)}")
        return cls(west=coords[0], south=coords[1], east=coords[2], north=coords[3])

# query/validation.py
@dataclass
class ValidationResult:
    """Accumulates validation errors instead of failing fast."""
    errors: List[ValidationError] = field(default_factory=list)

    def add_error(self, field: str, message: str, value: Any = None) -> None:
        self.errors.append(ValidationError(field, message, value))

    def merge(self, other: "ValidationResult") -> None:
        self.errors.extend(other.errors)

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0
```

**Characteristics:**

| Feature | Implementation | Implication |
|---------|---------------|-------------|
| Auth requirement | Not required | Queries are portable, reusable objects |
| Backend selection | At execution time | Same query works with CMR or STAC |
| Parameter style | Method chaining + kwargs | More Pythonic, flexible construction |
| Validation | `ValidationResult` accumulator | Collects all errors, user-friendly |
| Type coercion | Dedicated type classes | Clean separation of concerns |

**Example usage:**

```python
import earthaccess
from earthaccess.query import GranuleQuery, BoundingBox, DateRange

# Construct query without auth - it's just a data structure
query = GranuleQuery(
    short_name="ATL03",
    temporal=("2020-01", "2020-12"),
    bounding_box=(-180, -90, 180, 90),
)

# Or use method chaining
query = GranuleQuery().short_name("ATL03").temporal("2020-01", "2020-12")

# Validate before execution
validation = query.validate()
if not validation.is_valid:
    for error in validation.errors:
        print(f"Error in {error.field}: {error.message}")

# Execute with auth when ready
auth = earthaccess.login()
results = earthaccess.search_data(query=query)  # Auth used internally

# Same query can emit different formats
cmr_params = query.to_cmr()    # {"short_name": "ATL03", "temporal": "2020-01-01,2020-12-31"}
stac_params = query.to_stac()  # {"collections": ["ATL03"], "datetime": "2020-01-01/2020-12-31"}
```

### Query Architecture Comparison

| Aspect | stac-distributed-glm | stac-distributed-opus |
|--------|---------------------|----------------------|
| **Auth coupling** | Tightly coupled (required) | Decoupled (not required) |
| **Query portability** | Limited (tied to auth) | High (just a data structure) |
| **Construction flexibility** | Method chaining only | Method chaining + kwargs |
| **Dual format output** | Via backend parameter | Via `to_cmr()` / `to_stac()` methods |
| **Type classes** | None (inline tuples) | `BoundingBox`, `DateRange`, `Point`, `Polygon` |
| **Validation style** | Exception (fail-fast) | Accumulator (collect all) |
| **Introspection** | No | Yes (dynamic parameter application) |
| **Lines of code** | 824 (single file) | 1,600 (5 files) |

### Why Opus's Query Design is Superior

1. **Separation of concerns**: Queries are pure data structures, authentication is orthogonal
2. **Reusability**: Queries can be saved, shared, serialized without auth context
3. **Testing**: Queries can be tested without mocking authentication
4. **Flexibility**: Same query works with both CMR and STAC backends
5. **User experience**: `ValidationResult` shows all problems at once
6. **Type safety**: Dedicated type classes prevent invalid states

---

## STAC Conversion Strategy

### Overview

STAC (SpatioTemporal Asset Catalog) is becoming the standard for geospatial data catalogs. Both branches add STAC support, but with very different approaches.

### stac-distributed-glm STAC Approach

**Location:** Inline methods on result classes (`results.py`)

```python
# In earthaccess/results.py
class DataGranule:
    """Represents a granule from CMR."""

    def to_stac(self) -> Dict[str, Any]:
        """Convert this granule to a STAC Item dictionary."""
        # Inline conversion logic (~50 lines)
        item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": self.concept_id(),
            "geometry": self._extract_geometry(),
            "bbox": self._extract_bbox(),
            "properties": {
                "datetime": self._extract_datetime(),
                "start_datetime": self.start_date(),
                "end_datetime": self.end_date(),
            },
            "assets": self._convert_assets_to_stac(),
            "links": self._convert_links_to_stac(),
        }
        return item
```

**Characteristics:**

| Feature | Implementation | Implication |
|---------|---------------|-------------|
| **Direction** | One-way only (CMR → STAC) | Cannot ingest external STAC data |
| **Location** | Methods on result classes | Mixed responsibilities |
| **Format** | Returns Python dict | Manual STAC compliance |
| **Testing** | Requires DataGranule instances | Higher test complexity |
| **Extensibility** | Modify result classes | Tight coupling |

### stac-distributed-opus STAC Approach

**Location:** Dedicated `stac/` package with pure functions (`stac/converters.py`, 863 lines)

```python
# In earthaccess/stac/converters.py

# Mapping constants for URL types
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


def umm_granule_to_stac_item(
    granule: Dict[str, Any],
    collection_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Convert a UMM granule to a STAC Item.

    Args:
        granule: Raw UMM granule dictionary from CMR
        collection_id: Optional STAC collection ID

    Returns:
        STAC Item dictionary conforming to STAC 1.0.0 spec
    """
    umm = granule.get("umm", granule)
    meta = granule.get("meta", {})

    # Extract geometry with fallback chain
    geometry = _extract_geometry(umm)
    bbox = _geometry_to_bbox(geometry)

    # Build properties from temporal extent
    properties = _build_properties(umm, meta)

    # Convert assets with role mapping
    assets = _convert_related_urls_to_assets(umm.get("RelatedUrls", []))

    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "stac_extensions": [],
        "id": meta.get("concept-id", umm.get("GranuleUR", "")),
        "collection": collection_id or meta.get("collection-concept-id"),
        "geometry": geometry,
        "bbox": bbox,
        "properties": properties,
        "assets": assets,
        "links": _build_links(meta),
    }


def stac_item_to_data_granule(
    item: Dict[str, Any],
    auth: Optional[Any] = None,
) -> "DataGranule":
    """Convert a STAC Item to a DataGranule.

    This enables earthaccess to work with external STAC catalogs.

    Args:
        item: STAC Item dictionary
        auth: Optional Auth instance for data access

    Returns:
        DataGranule instance compatible with earthaccess operations
    """
    # Reconstruct UMM-like structure from STAC
    umm = _stac_properties_to_umm(item["properties"])
    umm["RelatedUrls"] = _stac_assets_to_related_urls(item.get("assets", {}))
    umm["SpatialExtent"] = _stac_geometry_to_spatial_extent(item.get("geometry"))

    meta = {
        "concept-id": item["id"],
        "collection-concept-id": item.get("collection"),
        "provider-id": _infer_provider(item),
    }

    return DataGranule({"umm": umm, "meta": meta}, auth=auth)


def umm_collection_to_stac_collection(collection: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a UMM collection to a STAC Collection."""
    # ... ~200 lines of collection conversion


def stac_collection_to_data_collection(
    collection: Dict[str, Any]
) -> "DataCollection":
    """Convert a STAC Collection to a DataCollection."""
    # ... reverse conversion
```

**Characteristics:**

| Feature | Implementation | Implication |
|---------|---------------|-------------|
| **Direction** | Bidirectional (CMR ↔ STAC) | Full ecosystem interoperability |
| **Location** | Dedicated `stac/` package | Clean separation of concerns |
| **Format** | Pure functions on dicts | Easy to test and compose |
| **Testing** | Works with raw dicts | Simple, fast tests |
| **Extensibility** | Add new converter functions | Loose coupling |
| **Mappings** | Explicit constant tables | Auditable, maintainable |

### STAC Conversion Comparison

| Aspect | stac-distributed-glm | stac-distributed-opus |
|--------|---------------------|----------------------|
| **Conversion direction** | One-way (CMR → STAC) | Bidirectional |
| **External STAC support** | No | Yes |
| **Architecture** | Methods on classes | Pure functions |
| **Mapping constants** | Inline | Explicit tables |
| **Lines of code** | ~50 | ~864 |
| **Test coverage** | Basic | Comprehensive (710 lines of tests) |

### Why Bidirectional Conversion Matters

**Use Case: Working with Element84 Earth Search**

```python
# With Opus's bidirectional conversion:
from pystac_client import Client
from earthaccess.stac import stac_item_to_data_granule

# Search external STAC catalog
catalog = Client.open("https://earth-search.aws.element84.com/v1")
search = catalog.search(collections=["sentinel-2-l2a"], bbox=[-122, 37, -121, 38])

# Convert to earthaccess DataGranules
granules = [stac_item_to_data_granule(item.to_dict()) for item in search.items()]

# Now use earthaccess operations!
earthaccess.download(granules, local_path="./data")
```

**With GLM, this is not possible** - you can only convert earthaccess results TO STAC, not FROM STAC.

---

## Credential Management

### Overview

Credential management is critical for:
1. Authenticating with NASA Earthdata
2. Accessing cloud-hosted data via S3
3. Distributing credentials to parallel workers

### stac-distributed-glm Credential Design

**Location:** `store_components/credentials.py` (309 lines)

```python
@dataclass(frozen=True)
class S3Credentials:
    """Immutable container for AWS temporary credentials.

    These credentials are obtained from NASA's S3 credential endpoint
    and typically expire after 1 hour.
    """
    access_key_id: str
    secret_access_key: str
    session_token: str
    expiration: datetime
    region: str = "us-west-2"

    @property
    def is_expired(self) -> bool:
        """Check if credentials are expired or about to expire.

        Uses a 5-minute buffer to ensure credentials don't expire
        during a long-running operation.
        """
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


@dataclass
class AuthContext:
    """Serializable authentication context for distributed workers.

    Contains everything a worker needs to access data without
    re-authenticating with Earthdata Login.
    """
    s3_credentials: Optional[S3Credentials] = None
    https_headers: Optional[Mapping[str, str]] = None
    https_cookies: Optional[Mapping[str, str]] = None
    provider: Optional[str] = None
    cloud_hosted: bool = True
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        """Serialize for shipping to workers."""
        return {
            "s3_credentials": self.s3_credentials.to_dict() if self.s3_credentials else None,
            "https_headers": dict(self.https_headers) if self.https_headers else None,
            "https_cookies": dict(self.https_cookies) if self.https_cookies else None,
            "provider": self.provider,
            "cloud_hosted": self.cloud_hosted,
        }


class CredentialManager:
    """Manages credential lifecycle with caching and expiration handling.

    Responsibilities:
    - Fetch credentials when needed
    - Cache valid credentials by provider
    - Refresh expired credentials automatically
    - Infer provider from S3 bucket names
    """

    # Provider inference from bucket names
    BUCKET_PROVIDER_MAP = {
        "podaac": "POCLOUD",
        "nsidc-cumulus": "NSIDC_CPRD",
        "lp-prod": "LPCLOUD",
        "gesdisc-cumulus": "GES_DISC",
        "ornl-cumulus": "ORNL_CLOUD",
        "asf-cumulus": "ASF",
        "ghrc-cumulus": "GHRC_DAAC",
    }

    def __init__(self, auth: Optional[Auth]) -> None:
        self.auth = auth
        self._credential_cache: Dict[str, S3Credentials] = {}

    def get_credentials(self, provider: Optional[str] = None) -> S3Credentials:
        """Get credentials for a provider, using cache if valid."""
        if provider and provider in self._credential_cache:
            creds = self._credential_cache[provider]
            if not creds.is_expired:
                return creds

        # Fetch fresh credentials
        raw_creds = self.auth.get_s3_credentials(provider=provider)
        creds = S3Credentials(
            access_key_id=raw_creds["accessKeyId"],
            secret_access_key=raw_creds["secretAccessKey"],
            session_token=raw_creds["sessionToken"],
            expiration=datetime.fromisoformat(raw_creds["expiration"]),
        )

        if provider:
            self._credential_cache[provider] = creds

        return creds

    def get_auth_context(
        self,
        provider: Optional[str] = None,
        cloud_hosted: bool = True,
    ) -> AuthContext:
        """Create an AuthContext for distributed workers."""
        s3_creds = self.get_credentials(provider) if cloud_hosted else None

        # Include HTTP headers/cookies for HTTPS access
        session = self.auth.get_session() if self.auth else None

        return AuthContext(
            s3_credentials=s3_creds,
            https_headers=dict(session.headers) if session else None,
            https_cookies=dict(session.cookies) if session else None,
            provider=provider,
            cloud_hosted=cloud_hosted,
        )
```

**Integration with Store and FileSystemFactory:**

```python
# In main_store.py
class Store:
    def __init__(self, auth: Union[Auth, None] = None):
        self._auth = auth
        # Dependency injection
        self.credential_manager = CredentialManager(auth) if isinstance(auth, Auth) else None
        self.filesystem_factory = FileSystemFactory(self.credential_manager)


# In filesystems.py
class FileSystemFactory:
    def __init__(self, credential_manager: Optional[CredentialManager]) -> None:
        self.credential_manager = credential_manager
        self._fs_cache: Dict[str, AbstractFileSystem] = {}

    def get_s3_filesystem(self, provider: Optional[str] = None) -> S3FileSystem:
        """Create S3 filesystem with proper credentials."""
        if self.credential_manager:
            creds = self.credential_manager.get_credentials(provider)
            return S3FileSystem(**creds.to_dict())
        return S3FileSystem(anon=True)
```

### stac-distributed-opus Credential Design

**Location:** `streaming.py` (AuthContext) + `credentials.py` (CredentialManager)

```python
# In streaming.py
@dataclass(frozen=True)
class AuthContext:
    """Serializable authentication context for shipping credentials to workers.

    Includes full login credentials to allow workers to re-authenticate
    if needed (e.g., for long-running distributed jobs).
    """
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    s3_credentials: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    token_expiry: Optional[datetime] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @classmethod
    def from_auth(cls, auth: Any) -> "AuthContext":
        """Create an AuthContext from an earthaccess Auth instance.

        Extracts all credentials needed for workers to operate independently.
        """
        from earthaccess.auth import Auth

        if not isinstance(auth, Auth):
            raise TypeError(f"Expected Auth instance, got {type(auth)}")

        username = getattr(auth, "username", None)
        password = getattr(auth, "password", None)
        token = None

        token_data = getattr(auth, "_token", None)
        if token_data and isinstance(token_data, dict):
            token = token_data.get("access_token")

        return cls(
            username=username,
            password=password,
            token=token,
            s3_credentials={},
            token_expiry=None,
        )

    def to_auth(self) -> Any:
        """Recreate an Auth instance from this context.

        Allows workers to get a functional Auth object without
        going through the full login flow.
        """
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

    def is_expired(self) -> bool:
        """Check if the token has expired."""
        if self.token_expiry is None:
            return False
        return datetime.now(timezone.utc) > self.token_expiry


# In credentials.py
class CredentialManager:
    """Manages S3 credentials with caching and automatic refresh."""

    BUCKET_PROVIDER_MAP = {
        # Same mapping as GLM
    }

    def __init__(self, auth: Auth) -> None:
        self.auth = auth
        self._cache: Dict[str, Tuple[Dict[str, str], datetime]] = {}
        self._cache_ttl = timedelta(minutes=55)  # Refresh 5 min before expiry

    def get_credentials(self, provider: Optional[str] = None) -> Dict[str, str]:
        """Get S3 credentials, using cache if still valid."""
        cache_key = provider or "default"

        if cache_key in self._cache:
            creds, expiry = self._cache[cache_key]
            if datetime.now(timezone.utc) < expiry:
                return creds

        # Fetch and cache
        creds = self.auth.get_s3_credentials(provider=provider)
        self._cache[cache_key] = (creds, datetime.now(timezone.utc) + self._cache_ttl)

        return creds
```

### Credential Management Comparison

| Aspect | stac-distributed-glm | stac-distributed-opus |
|--------|---------------------|----------------------|
| **S3Credentials type** | Frozen dataclass | Raw dict |
| **Type safety** | High (explicit fields) | Low (string keys) |
| **Expiration checking** | `is_expired` property | Manual TTL tracking |
| **Output formats** | `to_dict()`, `to_boto3_dict()` | Single dict format |
| **AuthContext scope** | S3 + HTTP headers/cookies | Username/password + token |
| **Auth reconstruction** | Not supported | `from_auth()` / `to_auth()` |
| **Integration** | Deep (Store, FileSystemFactory) | Standalone |
| **HTTP access support** | Yes (headers/cookies included) | No (S3 only) |

### Credential Design Trade-offs

**GLM Advantages:**
- Type-safe `S3Credentials` prevents key typos
- HTTP headers/cookies included for HTTPS fallback
- Tight integration with Store/FileSystemFactory
- Clear expiration semantics with buffer

**Opus Advantages:**
- `from_auth()` / `to_auth()` enables full Auth reconstruction
- Workers can re-authenticate if credentials expire
- Storing username/password is more robust for long jobs
- More flexible dict-based approach

### Recommendation

**Combine the best of both:**
- Use GLM's `S3Credentials` frozen dataclass for type safety
- Add Opus's `from_auth()` / `to_auth()` methods for worker flexibility
- Include HTTP headers/cookies from GLM for HTTPS access

---

## Distributed Computing

### Overview

Distributed computing enables parallel processing of large datasets across multiple workers. Both branches add this capability with similar executor abstractions.

### Executor Architecture (Both Branches)

Both branches implement nearly identical executor patterns:

```python
# parallel.py (both branches)

class SerialExecutor(Executor):
    """Synchronous execution for debugging."""

    def submit(self, fn, *args, **kwargs) -> Future:
        future = Future()
        try:
            result = fn(*args, **kwargs)
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)
        return future


class ThreadPoolExecutorWrapper(Executor):
    """Thread pool with progress bar support."""

    def __init__(self, max_workers=None, show_progress=True):
        self._max_workers = max_workers
        self._show_progress = show_progress
        self._executor = None


class DaskDelayedExecutor(Executor):
    """Dask-based distributed execution."""

    def map(self, fn, *iterables, **kwargs):
        import dask
        delayed_results = [dask.delayed(fn)(item) for item in zip(*iterables)]
        return dask.compute(*delayed_results)


class LithopsEagerFunctionExecutor(Executor):
    """Serverless execution via Lithops."""

    def map(self, fn, *iterables, **kwargs):
        from lithops import FunctionExecutor
        with FunctionExecutor() as executor:
            futures = executor.map(fn, list(zip(*iterables)))
            return executor.get_result(futures)


def get_executor(
    parallel: Union[str, Executor, bool, None] = True,
    max_workers: int = None,
    show_progress: bool = True,
) -> Executor:
    """Factory function for executor selection."""
    if parallel is False or parallel == "serial":
        return SerialExecutor()
    elif parallel is True or parallel is None or parallel == "threads":
        return ThreadPoolExecutorWrapper(max_workers, show_progress)
    elif parallel == "dask":
        return DaskDelayedExecutor()
    elif parallel == "lithops":
        return LithopsEagerFunctionExecutor()
    elif isinstance(parallel, Executor):
        return parallel
    else:
        raise ValueError(f"Unknown parallel option: {parallel}")
```

### Executor Selection Guide

| `parallel` Value | Executor | Use Case |
|-----------------|----------|----------|
| `True`, `None`, `"threads"` | `ThreadPoolExecutorWrapper` | Default, local machine parallelism |
| `False`, `"serial"` | `SerialExecutor` | Debugging, step-through execution |
| `"dask"` | `DaskDelayedExecutor` | Cluster computing, large datasets |
| `"lithops"` | `LithopsEagerFunctionExecutor` | Serverless, cloud functions |
| `Executor` instance | Pass-through | Custom executor |

### Streaming Architecture Differences

#### stac-distributed-glm: Integrated Streaming

```python
# In store_components/results.py

class StreamingExecutor:
    """Producer-consumer pattern for concurrent streaming."""

    def __init__(self, max_workers: int = 4, prefetch_pages: int = 2):
        self.max_workers = max_workers
        self.prefetch_pages = prefetch_pages

    def map(
        self,
        func: Callable[[T], R],
        results: ResultsBase[T],
    ) -> Generator[R, None, None]:
        """Stream-process results with backpressure."""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit initial batch
            futures = deque()
            pages = results.pages()

            for _ in range(self.prefetch_pages):
                try:
                    page = next(pages)
                    futures.extend(executor.submit(func, item) for item in page)
                except StopIteration:
                    break

            # Yield results as they complete, prefetch more
            while futures:
                future = futures.popleft()
                yield future.result()

                # Maintain prefetch buffer
                if len(futures) < self.prefetch_pages * 10:
                    try:
                        page = next(pages)
                        futures.extend(executor.submit(func, item) for item in page)
                    except StopIteration:
                        pass
```

#### stac-distributed-opus: Dedicated Streaming Module

```python
# In streaming.py

class StreamingIterator(Generic[T]):
    """Thread-safe iterator wrapper for streaming results."""

    def __init__(self, iterable: Iterable[T]):
        self._iterator = iter(iterable)
        self._lock = threading.Lock()
        self._exhausted = False

    def __next__(self) -> T:
        with self._lock:
            if self._exhausted:
                raise StopIteration
            try:
                return next(self._iterator)
            except StopIteration:
                self._exhausted = True
                raise


class WorkerContext:
    """Thread-local state management for workers."""

    def __init__(self):
        self._local = threading.local()

    def get_auth(self) -> Optional[Any]:
        return getattr(self._local, "auth", None)

    def set_auth(self, auth: Any) -> None:
        self._local.auth = auth

    def get_session(self) -> Optional[Any]:
        return getattr(self._local, "session", None)

    def set_session(self, session: Any) -> None:
        self._local.session = session


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
        self.show_progress = show_progress
        self.worker_context = WorkerContext()

    def map(
        self,
        func: Callable[[T], R],
        items: Iterable[T],
        auth_context: Optional[AuthContext] = None,
    ) -> Iterator[R]:
        """Map function over items with streaming and backpressure."""

        # Wrap function to include auth context
        def worker_func(item: T) -> R:
            if auth_context:
                auth = auth_context.to_auth()
                self.worker_context.set_auth(auth)
            return func(item)

        # Create thread-safe iterator
        safe_items = StreamingIterator(items)

        # Submit with backpressure
        pending: queue.Queue[Future] = queue.Queue(maxsize=self.prefetch)

        # ... implementation details
```

### Distributed Computing Comparison

| Aspect | stac-distributed-glm | stac-distributed-opus |
|--------|---------------------|----------------------|
| **Executor types** | Identical (4 types) | Identical (4 types) |
| **Streaming location** | `store_components/results.py` | `streaming.py` (dedicated) |
| **Worker context** | Implicit in Store | Explicit `WorkerContext` class |
| **Auth shipping** | `AuthContext.to_dict()` | `AuthContext.to_auth()` |
| **Thread-safe iterator** | No explicit class | `StreamingIterator` class |
| **Session handling** | Clone in Store | Via `WorkerContext` |

### Session Cloning Strategy (GLM Only)

GLM includes sophisticated session cloning for thread-based executors:

```python
# In main_store.py

class Store:
    def __init__(self, auth):
        self.thread_locals = threading.local()
        self._current_executor_type: Optional[str] = None

    def _clone_session_in_local_thread(self, original_session: Session) -> Session:
        """Clone session for thread-local use.

        This avoids N authentication requests for N files.
        Instead, we authenticate once and clone the session
        (headers, cookies) to each thread.
        """
        if not hasattr(self.thread_locals, "local_thread_session"):
            local_session = SessionWithHeaderRedirection()
            local_session.headers.update(original_session.headers)
            local_session.cookies.update(original_session.cookies)
            self.thread_locals.local_thread_session = local_session

        return self.thread_locals.local_thread_session

    def _use_session_cloning(self) -> bool:
        """Determine if session cloning is appropriate.

        Clone for thread-based executors (shared process, shared auth).
        Use AuthContext for distributed executors (separate processes).
        """
        return self._current_executor_type in ["threads", "serial"]
```

---

## Store Architecture

### Overview

The Store class is the central component for data access in earthaccess. It handles downloading, streaming, and filesystem operations.

### stac-distributed-glm Store Architecture

**Complete refactoring with dependency injection:**

```python
# In main_store.py (1,226 lines)

class Store:
    """Data access store with dependency injection.

    This class orchestrates data access operations using injected
    components for credential management, filesystem creation, and
    cloud transfers.
    """

    def __init__(self, auth: Union[Auth, None] = None):
        self._auth = auth

        # Dependency injection - components are created once and shared
        if isinstance(auth, Auth):
            self.credential_manager = CredentialManager(auth)
            self.filesystem_factory = FileSystemFactory(self.credential_manager)
        else:
            self.credential_manager = None
            self.filesystem_factory = None

        # Thread-local storage for session cloning
        self.thread_locals = threading.local()
        self._current_executor_type: Optional[str] = None

    @property
    def authenticated(self) -> bool:
        """Check if store has valid authentication."""
        return (
            self._auth is not None
            and isinstance(self._auth, Auth)
            and self._auth.authenticated
        )

    def get_s3_filesystem(
        self,
        provider: Optional[str] = None,
        **kwargs,
    ) -> S3FileSystem:
        """Get S3 filesystem with proper credentials.

        Uses FileSystemFactory for consistent credential handling.
        """
        if self.filesystem_factory:
            return self.filesystem_factory.get_s3_filesystem(provider, **kwargs)

        # Fallback for unauthenticated access
        return S3FileSystem(anon=True, **kwargs)

    def open(
        self,
        granules: Sequence[DataGranule],
        parallel: Union[str, Executor, bool, None] = True,
        **kwargs,
    ) -> List[EarthAccessFile]:
        """Open granule files for streaming access.

        Returns fsspec-compatible file handles that can be passed
        directly to xarray, rasterio, etc.
        """
        executor = get_executor(parallel)
        self._set_executor_type(parallel)

        if self._use_session_cloning():
            # Thread-based: clone session
            session = self._auth.get_session()
            return list(executor.map(
                lambda g: self._open_granule_with_cloned_session(g, session),
                granules,
            ))
        else:
            # Distributed: ship auth context
            context = self.credential_manager.get_auth_context()
            return list(executor.map(
                lambda g: self._open_granule_with_context(g, context),
                granules,
            ))
```

**FileSystemFactory component:**

```python
# In store_components/filesystems.py (178 lines)

class FileSystemFactory:
    """Factory for creating authenticated filesystems.

    Single Responsibility: Create filesystem instances with proper credentials.
    Uses CredentialManager for credential lifecycle.
    """

    def __init__(self, credential_manager: Optional[CredentialManager]) -> None:
        self.credential_manager = credential_manager
        self._fs_cache: Dict[str, AbstractFileSystem] = {}

    def get_s3_filesystem(
        self,
        provider: Optional[str] = None,
        credentials: Optional[S3Credentials] = None,
        **kwargs,
    ) -> S3FileSystem:
        """Create S3 filesystem with credentials."""
        if credentials is None and self.credential_manager:
            credentials = self.credential_manager.get_credentials(provider)

        if credentials:
            return S3FileSystem(
                **credentials.to_dict(),
                **kwargs,
            )

        return S3FileSystem(anon=True, **kwargs)

    def get_https_filesystem(
        self,
        session: Optional[Session] = None,
        **kwargs,
    ) -> HTTPFileSystem:
        """Create HTTPS filesystem with session."""
        return HTTPFileSystem(
            client_kwargs={"session": session} if session else {},
            **kwargs,
        )

    def get_filesystem_for_url(
        self,
        url: str,
        provider: Optional[str] = None,
        **kwargs,
    ) -> AbstractFileSystem:
        """Auto-detect filesystem type from URL."""
        if url.startswith("s3://"):
            return self.get_s3_filesystem(provider, **kwargs)
        elif url.startswith(("http://", "https://")):
            return self.get_https_filesystem(**kwargs)
        else:
            return LocalFileSystem()
```

### stac-distributed-opus Store Architecture

**Incremental modification of existing store.py:**

```python
# In store.py (modified, +644 lines)

class Store:
    """Data access store with credential manager integration."""

    def __init__(self, auth: Union[Auth, None] = None):
        self._auth = auth

        # Added: Credential manager for S3 access
        if isinstance(auth, Auth):
            from .credentials import CredentialManager
            self._credential_manager = CredentialManager(auth)
        else:
            self._credential_manager = None

        # Existing attributes preserved
        self.running_in_aws = self._check_if_running_in_aws()

    # Most existing methods unchanged

    def get_s3_credentials(self, provider: Optional[str] = None) -> Dict[str, str]:
        """Get S3 credentials using credential manager."""
        if self._credential_manager:
            return self._credential_manager.get_credentials(provider)

        # Fallback to direct auth call
        if self._auth:
            return self._auth.get_s3_credentials(provider=provider)

        raise ValueError("No authentication available")
```

### Store Architecture Comparison

| Aspect | stac-distributed-glm | stac-distributed-opus |
|--------|---------------------|----------------------|
| **File location** | `main_store.py` (new) | `store.py` (modified) |
| **Refactoring scope** | Complete rewrite | Incremental changes |
| **Lines of code** | 1,226 | +644 (additions) |
| **Dependency injection** | Full (CredentialManager, FileSystemFactory) | Partial (CredentialManager only) |
| **FileSystemFactory** | Yes | No |
| **Session cloning** | Yes (explicit strategy) | No |
| **Backward compatibility** | Via `store/__init__.py` shim | In-place |
| **Testability** | High (inject mocks) | Medium |

### Why GLM's Store Architecture is Superior

1. **Testability**: Inject mock CredentialManager and FileSystemFactory
2. **Separation of concerns**: Each component has one job
3. **Flexibility**: Swap components without changing Store
4. **Session strategy**: Explicit handling for different executor types
5. **Caching**: FileSystemFactory maintains filesystem cache

**However**, GLM's approach is more invasive and has higher merge risk.

---

## Asset Handling

### Overview

Assets are the individual files within a granule (e.g., data files, thumbnails, metadata). Proper asset handling is important for:
- Filtering which files to download
- Understanding file roles and types
- Selecting specific bands or resolutions

### stac-distributed-glm Asset Model

**Location:** `store_components/asset.py` (469 lines)

```python
@dataclass(frozen=True)
class Asset:
    """Immutable representation of a granule asset.

    Assets are individual files within a granule, such as data files,
    thumbnails, or metadata. This class provides a type-safe,
    STAC-compatible representation.

    Attributes:
        href: URL or path to the asset
        title: Human-readable title
        description: Longer description
        type: MIME type (e.g., "application/x-hdf5")
        roles: STAC roles (e.g., {"data"}, {"thumbnail"})
        bands: Band names for multi-band assets
        gsd: Ground sample distance in meters
        file_size: File size in bytes
        checksum: File checksum for verification
    """
    href: str
    title: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    roles: FrozenSet[str] = field(default_factory=frozenset)
    bands: Optional[Tuple[str, ...]] = None
    gsd: Optional[float] = None
    file_size: Optional[int] = None
    checksum: Optional[str] = None

    def has_role(self, role: str) -> bool:
        """Check if asset has a specific role."""
        return role in self.roles

    def is_data(self) -> bool:
        """Check if this is a data asset."""
        return "data" in self.roles or self.has_role("data")

    def is_thumbnail(self) -> bool:
        """Check if this is a thumbnail."""
        return "thumbnail" in self.roles

    def is_metadata(self) -> bool:
        """Check if this is a metadata file."""
        return "metadata" in self.roles

    def is_browse(self) -> bool:
        """Check if this is a browse image."""
        return "overview" in self.roles or "visual" in self.roles

    def matches_type(self, content_type: str) -> bool:
        """Check if asset matches a content type."""
        if self.type is None:
            return False
        return self.type.lower() == content_type.lower()

    def matches_types(self, content_types: Iterable[str]) -> bool:
        """Check if asset matches any of the content types."""
        return any(self.matches_type(t) for t in content_types)

    def with_role(self, role: str) -> "Asset":
        """Return new Asset with additional role (immutable update)."""
        return replace(self, roles=self.roles | {role})

    def to_dict(self) -> Dict[str, Any]:
        """Convert to STAC asset dictionary."""
        result = {"href": self.href}
        if self.title:
            result["title"] = self.title
        if self.description:
            result["description"] = self.description
        if self.type:
            result["type"] = self.type
        if self.roles:
            result["roles"] = list(self.roles)
        # ... additional fields
        return result


@dataclass(frozen=True)
class AssetFilter:
    """Immutable, composable filter for assets.

    Filters can be combined using the `combine()` method for AND logic.
    Each filter criterion is optional; only specified criteria are applied.

    Example:
        # Filter for HDF5 data files under 1GB
        filter = AssetFilter(
            content_types={"application/x-hdf5", "application/x-hdf"},
            include_roles={"data"},
            max_size=1024 * 1024 * 1024,  # 1GB
        )

        # Apply to assets
        data_files = filter_assets(granule.assets(), filter)
    """
    content_types: Optional[FrozenSet[str]] = None
    exclude_content_types: Optional[FrozenSet[str]] = None
    include_roles: Optional[FrozenSet[str]] = None
    exclude_roles: Optional[FrozenSet[str]] = None
    bands: Optional[FrozenSet[str]] = None
    exclude_bands: Optional[FrozenSet[str]] = None
    min_size: Optional[int] = None
    max_size: Optional[int] = None
    checksums: Optional[FrozenSet[str]] = None
    exclude_missing_checksum: bool = False
    filename_patterns: Optional[Tuple[str, ...]] = None
    exclude_filename_patterns: Optional[Tuple[str, ...]] = None

    def matches(self, asset: Asset) -> bool:
        """Check if an asset matches all filter criteria."""
        # Content type filter
        if self.content_types:
            if not asset.matches_types(self.content_types):
                return False

        if self.exclude_content_types:
            if asset.matches_types(self.exclude_content_types):
                return False

        # Role filter
        if self.include_roles:
            if not any(asset.has_role(r) for r in self.include_roles):
                return False

        if self.exclude_roles:
            if any(asset.has_role(r) for r in self.exclude_roles):
                return False

        # Size filter
        if self.min_size is not None:
            if asset.file_size is None or asset.file_size < self.min_size:
                return False

        if self.max_size is not None:
            if asset.file_size is None or asset.file_size > self.max_size:
                return False

        # ... additional filters

        return True

    def combine(self, other: "AssetFilter") -> "AssetFilter":
        """Combine two filters with AND logic."""
        return AssetFilter(
            content_types=self._merge_sets(self.content_types, other.content_types),
            include_roles=self._merge_sets(self.include_roles, other.include_roles),
            min_size=max(self.min_size or 0, other.min_size or 0) or None,
            max_size=min(
                self.max_size or float("inf"),
                other.max_size or float("inf"),
            ) or None,
            # ... merge other fields
        )


# Helper functions
def filter_assets(assets: Iterable[Asset], filter: AssetFilter) -> List[Asset]:
    """Apply filter to a collection of assets."""
    return [a for a in assets if filter.matches(a)]


def get_data_assets(assets: Iterable[Asset]) -> List[Asset]:
    """Get only data assets."""
    return [a for a in assets if a.is_data()]


def get_assets_by_size_range(
    assets: Iterable[Asset],
    min_size: Optional[int] = None,
    max_size: Optional[int] = None,
) -> List[Asset]:
    """Filter assets by file size."""
    filter = AssetFilter(min_size=min_size, max_size=max_size)
    return filter_assets(assets, filter)
```

### stac-distributed-opus Asset Handling

**No dedicated Asset model** - relies on pystac and raw dictionaries:

```python
# Working with assets in Opus requires manual dict access
for granule in results:
    stac_item = granule.to_stac()
    for name, asset in stac_item.get("assets", {}).items():
        if "data" in asset.get("roles", []):
            print(f"Data asset: {asset['href']}")
```

### Asset Handling Comparison

| Aspect | stac-distributed-glm | stac-distributed-opus |
|--------|---------------------|----------------------|
| **Asset class** | Rich frozen dataclass | None (raw dicts) |
| **Type safety** | High | Low |
| **Role queries** | `is_data()`, `is_thumbnail()`, etc. | Manual dict access |
| **Filtering** | `AssetFilter` with `combine()` | Manual iteration |
| **Immutability** | Yes (frozen) | N/A |
| **STAC compatibility** | `to_dict()` method | Native dicts |
| **Lines of code** | 469 | 0 |

### Why GLM's Asset Model is Valuable

**Use Case: Download only HDF5 data files under 500MB**

```python
# With GLM's Asset model:
from earthaccess.store_components import AssetFilter, filter_assets

filter = AssetFilter(
    content_types=frozenset({"application/x-hdf5"}),
    include_roles=frozenset({"data"}),
    max_size=500 * 1024 * 1024,  # 500MB
)

for granule in results:
    assets = granule.assets()  # Returns List[Asset]
    data_files = filter_assets(assets, filter)
    for asset in data_files:
        download(asset.href)

# With Opus (or without Asset model):
for granule in results:
    stac = granule.to_stac()
    for name, asset in stac.get("assets", {}).items():
        roles = asset.get("roles", [])
        mime = asset.get("type", "")
        size = asset.get("file:size")  # Might be None

        if "data" in roles and "hdf5" in mime.lower():
            if size is None or size <= 500 * 1024 * 1024:
                download(asset["href"])
```

The GLM approach is more readable, type-safe, and reusable.

---

## Type Safety and Validation

### Overview

Type safety and validation are crucial for:
- Catching errors early (at query construction, not execution)
- Providing helpful error messages
- Enabling IDE autocompletion

### stac-distributed-glm Type Safety

```python
# Frozen dataclasses for immutability
@dataclass(frozen=True)
class Asset:
    href: str
    roles: FrozenSet[str] = field(default_factory=frozenset)
    # ...

@dataclass(frozen=True)
class S3Credentials:
    access_key_id: str
    secret_access_key: str
    session_token: str
    expiration: datetime
    # ...

# Exception-based validation (fail-fast)
class QueryValidationError(ValueError):
    """Raised when query parameters are invalid."""

    def __init__(
        self,
        message: str,
        parameter: Optional[str] = None,
        value: Any = None,
    ):
        super().__init__(message)
        self.parameter = parameter
        self.value = value

# Usage
def _validate_bbox(self, bbox: BBoxLike) -> Tuple[float, float, float, float]:
    if len(bbox) != 4:
        raise QueryValidationError(
            f"Bounding box must have 4 values, got {len(bbox)}",
            parameter="bounding_box",
            value=bbox,
        )
    # ... more validation
```

### stac-distributed-opus Type Safety

```python
# Dedicated type classes with dual output
@dataclass(frozen=True)
class BoundingBox:
    """Immutable bounding box with CMR and STAC output."""
    west: float
    south: float
    east: float
    north: float

    def __post_init__(self):
        # Validation in __post_init__
        if not -180 <= self.west <= 180:
            raise ValueError(f"Invalid west: {self.west}")
        # ... more validation

    def to_cmr(self) -> str:
        return f"{self.west},{self.south},{self.east},{self.north}"

    def to_stac(self) -> List[float]:
        return [self.west, self.south, self.east, self.north]


# Accumulator-based validation (collect all errors)
@dataclass
class ValidationResult:
    """Collects all validation errors instead of failing on first."""
    errors: List[ValidationError] = field(default_factory=list)

    def add_error(self, field: str, message: str, value: Any = None) -> None:
        self.errors.append(ValidationError(field, message, value))

    def merge(self, other: "ValidationResult") -> None:
        self.errors.extend(other.errors)

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    def raise_if_invalid(self) -> None:
        if not self.is_valid:
            messages = [f"{e.field}: {e.message}" for e in self.errors]
            raise ValueError("Validation failed:\n" + "\n".join(messages))


@dataclass
class ValidationError:
    """Single validation error with context."""
    field: str
    message: str
    value: Any = None


# Usage
class GranuleQuery(QueryBase):
    def validate(self) -> ValidationResult:
        result = ValidationResult()

        if "short_name" not in self._params and "concept_id" not in self._params:
            result.add_error(
                "query",
                "Either short_name or concept_id is required",
            )

        if self._spatial:
            spatial_result = self._spatial.validate()
            result.merge(spatial_result)

        return result
```

### Type Safety Comparison

| Aspect | stac-distributed-glm | stac-distributed-opus |
|--------|---------------------|----------------------|
| **Frozen dataclasses** | Asset, S3Credentials | BoundingBox, DateRange, Point, Polygon |
| **Validation style** | Exception (fail-fast) | Accumulator (collect all) |
| **Error context** | Parameter + value | Field + message + value |
| **User experience** | Stops on first error | Shows all errors |
| **Type classes** | Domain objects (Asset) | Query parameters (BoundingBox) |
| **Dual output** | No | Yes (`to_cmr()`, `to_stac()`) |

### Validation Style Comparison

**Exception-based (GLM):**
```python
# User sees first error only
try:
    query.bounding_box(200, -90, 180, 90)  # Invalid west
    query.temporal("invalid", "also-invalid")
    query.execute()
except QueryValidationError as e:
    print(e)  # "West must be between -180 and 180"
    # User fixes, runs again, gets next error...
```

**Accumulator-based (Opus):**
```python
# User sees all errors at once
query = GranuleQuery(
    bounding_box=(200, -90, 180, 90),  # Invalid west
    temporal=("invalid", "also-invalid"),  # Invalid dates
)

result = query.validate()
if not result.is_valid:
    for error in result.errors:
        print(f"{error.field}: {error.message}")
    # Output:
    # bounding_box: West must be between -180 and 180
    # temporal: Invalid date format 'invalid'
```

The accumulator pattern is more user-friendly.

---

## Testing Strategy

### stac-distributed-glm Testing

```
tests/unit/
├── test_asset.py              # 453 lines - Asset and AssetFilter
├── test_basic_query.py        # 125 lines - Basic query building
├── test_cloud_transfer.py     # 227 lines - Cloud-to-cloud transfers
├── test_executor_strategy.py  # 125 lines - Executor selection
├── test_flexible_inputs.py    # 245 lines - Input normalization
├── test_geometry.py           # 343 lines - Geometry loading
├── test_parallel.py           # 180 lines - Parallel execution
├── test_results_base.py       # 157 lines - ResultsBase class
├── test_results_enhanced.py   # 274 lines - Enhanced result methods
├── test_stac_search.py        # 145 lines - External STAC search
├── test_store_credentials.py  # 351 lines - Credential management
└── test_target_filesystem.py  # 345 lines - Target filesystem
```

**Total: ~2,970 lines of new tests across 12 files**

### stac-distributed-opus Testing

```
tests/unit/
├── test_api_query_integration.py  # 262 lines - API + query integration
├── test_credentials.py            # 485 lines - Credential management
├── test_executor_strategy.py      # 124 lines - Executor selection
├── test_parallel.py               # 182 lines - Parallel execution
├── test_query.py                  # 418 lines - Query building
├── test_stac_converters.py        # 710 lines - STAC conversion (!)
├── test_streaming.py              # 400 lines - Streaming execution
└── test_target_filesystem.py      # 345 lines - Target filesystem
```

**Total: ~2,926 lines of new tests across 8 files**

### Testing Comparison

| Aspect | stac-distributed-glm | stac-distributed-opus |
|--------|---------------------|----------------------|
| **Total test lines** | ~2,970 | ~2,926 |
| **Number of test files** | 12 | 8 |
| **STAC converter tests** | Basic (~50 lines) | Comprehensive (710 lines) |
| **Asset tests** | 453 lines | None |
| **Query tests** | 125 lines | 418 lines |
| **Credential tests** | 351 lines | 485 lines |

**Key Observations:**
- GLM has broader coverage (more files)
- Opus has deeper STAC testing (3x more lines)
- GLM tests Asset model extensively
- Opus tests query building more thoroughly

---

## Backward Compatibility

### stac-distributed-glm Compatibility Strategy

```python
# earthaccess/store/__init__.py (backward compat shim)
"""Backward compatibility layer.

This module re-exports components from their new locations to maintain
compatibility with existing code that imports from `earthaccess.store`.
"""

# Re-export Store class from new location
from ..main_store import Store

# Re-export components from store_components
from ..store_components import (
    Asset,
    AssetFilter,
    AuthContext,
    CredentialManager,
    FileSystemFactory,
    S3Credentials,
    # ...
)

# Deprecated aliases
from ..store_components.credentials import (
    CredentialManager as _CredentialManager,
)

import warnings

def get_credential_manager(*args, **kwargs):
    """Deprecated: Use CredentialManager directly."""
    warnings.warn(
        "get_credential_manager is deprecated, use CredentialManager instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return _CredentialManager(*args, **kwargs)
```

### stac-distributed-opus Compatibility Strategy

```python
# earthaccess/__init__.py (new exports alongside existing)

# Existing exports preserved
from .auth import Auth
from .search import DataGranules, DataCollections
from .store import Store

# New exports added (non-breaking)
from .query import (
    GranuleQuery,
    CollectionQuery,
    BoundingBox,
    DateRange,
    Point,
    Polygon,
)
from .stac import (
    umm_granule_to_stac_item,
    stac_item_to_data_granule,
)
from .streaming import AuthContext
from .credentials import CredentialManager
```

### Compatibility Comparison

| Aspect | stac-distributed-glm | stac-distributed-opus |
|--------|---------------------|----------------------|
| **Import paths** | New paths, shim for old | Original paths preserved |
| **Store location** | `main_store.py` (new) | `store.py` (same) |
| **Breaking changes** | Internal (shim hides) | None |
| **Deprecation warnings** | Yes | No |
| **Migration required** | Eventually | No |
| **Merge risk** | Higher | Lower |

---

## Use Case Scenarios

### Use Case 1: Search for ICESat-2 Data Over Antarctica

**Scenario:** A researcher wants to find ICESat-2 ATL03 granules over Antarctica from 2020.

#### stac-distributed-glm Approach

```python
import earthaccess
from earthaccess.store_components import GranuleQuery

# Must authenticate first
auth = earthaccess.login()

# Build query (requires auth)
query = GranuleQuery(auth, backend="cmr")
query.short_name("ATL03")
query.temporal("2020-01-01", "2020-12-31")
query.bounding_box(-180, -90, 180, -60)  # Antarctica

# Execute
try:
    results = query.execute()
except QueryValidationError as e:
    print(f"Error: {e}")  # Only first error shown

# Convert to STAC
for granule in results:
    stac_item = granule.to_stac()  # One-way conversion
```

**Characteristics:**
- ✅ Simple, linear flow
- ❌ Must authenticate before building query
- ❌ Only sees first validation error
- ❌ One-way STAC conversion

#### stac-distributed-opus Approach

```python
import earthaccess
from earthaccess.query import GranuleQuery, BoundingBox, DateRange

# Build query FIRST (no auth needed)
query = GranuleQuery(
    short_name="ATL03",
    temporal=("2020-01-01", "2020-12-31"),
    bounding_box=(-180, -90, 180, -60),
)

# Or use method chaining
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
        print(f"{error.field}: {error.message}")
    # Fix all errors at once

# Authenticate and execute
auth = earthaccess.login()
results = earthaccess.search_data(query=query)

# Get both CMR and STAC formats
cmr_params = query.to_cmr()
stac_params = query.to_stac()

# Convert to STAC (one-way) or from STAC (bidirectional!)
for granule in results:
    stac_item = granule.to_stac()
```

**Characteristics:**
- ✅ Build query without authentication
- ✅ See all validation errors at once
- ✅ Multiple construction styles
- ✅ Dual output formats

#### Winner: **Opus** (more flexible query building)

---

### Use Case 2: Download Data with Asset Filtering

**Scenario:** A user wants to download only HDF5 data files (not thumbnails or metadata) under 1GB.

#### stac-distributed-glm Approach

```python
import earthaccess
from earthaccess.store_components import AssetFilter, filter_assets

# Search for granules
results = earthaccess.search_data(short_name="ATL03", count=10)

# Define filter
asset_filter = AssetFilter(
    content_types=frozenset({"application/x-hdf5", "application/x-hdf"}),
    include_roles=frozenset({"data"}),
    exclude_roles=frozenset({"thumbnail", "metadata"}),
    max_size=1024 * 1024 * 1024,  # 1GB
)

# Apply filter to each granule
for granule in results:
    assets = granule.assets()  # Returns List[Asset]
    data_assets = filter_assets(assets, asset_filter)

    for asset in data_assets:
        print(f"Downloading: {asset.href}")
        print(f"  Type: {asset.type}")
        print(f"  Size: {asset.file_size}")
        print(f"  Roles: {asset.roles}")

# Or use helper functions
from earthaccess.store_components import get_data_assets

for granule in results:
    data_assets = get_data_assets(granule.assets())
```

**Characteristics:**
- ✅ Rich, type-safe Asset model
- ✅ Composable filters
- ✅ Helper functions for common cases
- ✅ Clear, readable code

#### stac-distributed-opus Approach

```python
import earthaccess

# Search for granules
results = earthaccess.search_data(short_name="ATL03", count=10)

# Manual filtering (no Asset class)
for granule in results:
    stac_item = granule.to_stac()

    for name, asset in stac_item.get("assets", {}).items():
        roles = asset.get("roles", [])
        mime_type = asset.get("type", "")
        size = asset.get("file:size")  # STAC extension, might be None

        # Manual filtering logic
        is_data = "data" in roles
        is_hdf5 = "hdf5" in mime_type.lower() or "hdf" in mime_type.lower()
        not_thumbnail = "thumbnail" not in roles
        not_metadata = "metadata" not in roles
        size_ok = size is None or size <= 1024 * 1024 * 1024

        if is_data and is_hdf5 and not_thumbnail and not_metadata and size_ok:
            print(f"Downloading: {asset['href']}")
            print(f"  Type: {mime_type}")
            print(f"  Size: {size}")
            print(f"  Roles: {roles}")
```

**Characteristics:**
- ❌ No type-safe Asset model
- ❌ Manual dict access
- ❌ Verbose filtering logic
- ❌ No composable filters

#### Winner: **GLM** (rich Asset model makes filtering easy and type-safe)

---

### Use Case 3: Ingest Data from External STAC Catalog

**Scenario:** A user wants to find Sentinel-2 data from Element84's Earth Search catalog and process it with earthaccess.

#### stac-distributed-glm Approach

```python
import earthaccess
from pystac_client import Client

# Search external STAC catalog
catalog = Client.open("https://earth-search.aws.element84.com/v1")
search = catalog.search(
    collections=["sentinel-2-l2a"],
    bbox=[-122.5, 37.7, -122.3, 37.9],
    datetime="2023-01-01/2023-12-31",
)

# Get STAC items
items = list(search.items())

# ❌ Cannot convert STAC items to DataGranules!
# GLM only supports one-way conversion (CMR → STAC)

# Must work with STAC items directly
for item in items:
    # Manual access to assets
    for name, asset in item.assets.items():
        if "data" in asset.roles:
            # Cannot use earthaccess.download() on STAC items
            # Must use pystac or fsspec directly
            import fsspec
            fs = fsspec.filesystem("https")
            fs.download(asset.href, f"./data/{name}")
```

**Characteristics:**
- ❌ Cannot convert external STAC to DataGranules
- ❌ Cannot use earthaccess download/open on STAC items
- ❌ Must use separate tools for external STAC

#### stac-distributed-opus Approach

```python
import earthaccess
from earthaccess.stac import stac_item_to_data_granule
from pystac_client import Client

# Search external STAC catalog
catalog = Client.open("https://earth-search.aws.element84.com/v1")
search = catalog.search(
    collections=["sentinel-2-l2a"],
    bbox=[-122.5, 37.7, -122.3, 37.9],
    datetime="2023-01-01/2023-12-31",
)

# Convert STAC items to DataGranules!
granules = []
for item in search.items():
    # Bidirectional conversion
    granule = stac_item_to_data_granule(item.to_dict())
    granules.append(granule)

# Now use earthaccess operations!
# ✅ Works with earthaccess download
local_files = earthaccess.download(granules, local_path="./data")

# ✅ Works with earthaccess open
file_handles = earthaccess.open(granules)

# ✅ Works with xarray
import xarray as xr
ds = xr.open_mfdataset(file_handles, engine="h5netcdf")
```

**Characteristics:**
- ✅ Convert external STAC to DataGranules
- ✅ Use earthaccess download/open on converted items
- ✅ Full ecosystem interoperability

#### Winner: **Opus** (bidirectional STAC conversion enables external catalog integration)

---

### Use Case 4: Distributed Processing with Dask

**Scenario:** A user wants to process 1000 granules in parallel using a Dask cluster.

#### stac-distributed-glm Approach

```python
import earthaccess
from earthaccess.parallel import get_executor
from earthaccess.store_components import AuthContext

# Login and search
auth = earthaccess.login()
results = earthaccess.search_data(short_name="ATL03", count=1000)

# Get auth context for workers
store = earthaccess.get_requests_https_session()  # Gets Store
context = store.credential_manager.get_auth_context(provider="NSIDC_CPRD")

# Serialize context for Dask workers
context_dict = context.to_dict()
# Contains: s3_credentials, https_headers, https_cookies

# Define processing function
def process_granule(granule, auth_context):
    # Reconstruct credentials in worker
    import s3fs
    s3 = s3fs.S3FileSystem(**auth_context["s3_credentials"])

    # Access data
    url = granule.data_links()[0]
    with s3.open(url) as f:
        # Process file...
        return f.read(1024)  # Example

# Use Dask executor
executor = get_executor(parallel="dask", max_workers=10)

# Process in parallel
results = list(executor.map(
    lambda g: process_granule(g, context_dict),
    results,
))
```

**Characteristics:**
- ✅ Session cloning for thread executors
- ✅ AuthContext for distributed executors
- ❌ Cannot reconstruct full Auth in workers

#### stac-distributed-opus Approach

```python
import earthaccess
from earthaccess.parallel import get_executor
from earthaccess.streaming import AuthContext

# Login and search
auth = earthaccess.login()
results = earthaccess.search_data(short_name="ATL03", count=1000)

# Create auth context with full credentials
context = AuthContext.from_auth(auth)

# Define processing function
def process_granule(granule, auth_context):
    # Reconstruct FULL Auth in worker!
    auth = auth_context.to_auth()

    # Can re-authenticate if needed
    if auth_context.is_expired():
        auth.refresh()

    # Use reconstructed auth
    store = earthaccess.Store(auth)
    files = store.open([granule])

    # Process file...
    with files[0] as f:
        return f.read(1024)

# Use Dask executor
executor = get_executor(parallel="dask", max_workers=10)

# Process in parallel
results = list(executor.map(
    lambda g: process_granule(g, context),
    results,
))
```

**Characteristics:**
- ✅ Full Auth reconstruction in workers via `to_auth()`
- ✅ Workers can re-authenticate if credentials expire
- ✅ Username/password available for long-running jobs

#### Winner: **Opus** (full Auth reconstruction is more robust for distributed computing)

---

### Use Case Summary Table

| Use Case | GLM Winner | Opus Winner | Notes |
|----------|-----------|-------------|-------|
| Query building | | ✅ | Auth-decoupled, validation accumulator |
| Asset filtering | ✅ | | Rich Asset/AssetFilter model |
| STAC export | ✅ | ✅ | Both support CMR → STAC |
| STAC import | | ✅ | Only Opus has STAC → CMR |
| External catalogs | | ✅ | Bidirectional conversion required |
| Distributed processing | | ✅ | Full Auth reconstruction |
| Thread-based parallel | ✅ | | Session cloning strategy |
| Credential management | ✅ | | Type-safe S3Credentials |

---

## Summary: Pros and Cons

### stac-distributed-glm

#### Pros

| Advantage | Description | Impact |
|-----------|-------------|--------|
| **Clean Store architecture** | Full refactoring with dependency injection | High testability, maintainability |
| **Rich Asset model** | Type-safe, immutable Asset and AssetFilter | Easy filtering, no dict-key typos |
| **Type-safe credentials** | S3Credentials frozen dataclass | Prevents credential mishandling |
| **Session cloning** | Explicit strategy for thread-based executors | Efficient thread-local sessions |
| **FileSystemFactory** | Centralized filesystem creation | Consistent credential handling |
| **Broader test coverage** | 12 new test files | More edge cases covered |
| **HTTP access support** | AuthContext includes headers/cookies | Works with HTTPS fallback |

#### Cons

| Disadvantage | Description | Impact |
|--------------|-------------|--------|
| **Auth-coupled queries** | Queries require Auth at construction | Less flexible query building |
| **One-way STAC** | Only CMR → STAC conversion | Cannot use external STAC catalogs |
| **Exception validation** | Fails on first error | User must fix iteratively |
| **More invasive** | Complete Store rewrite | Higher merge conflict risk |
| **No Auth reconstruction** | Cannot recreate Auth in workers | Limited distributed support |

### stac-distributed-opus

#### Pros

| Advantage | Description | Impact |
|-----------|-------------|--------|
| **Decoupled queries** | No Auth required for construction | Portable, testable queries |
| **Dual construction** | Method chaining + kwargs | More Pythonic, flexible |
| **Bidirectional STAC** | Full CMR ↔ STAC conversion | External catalog support |
| **Validation accumulator** | Collects all errors | Better user experience |
| **Dedicated type classes** | BoundingBox, DateRange, etc. | Reusable, dual-output |
| **Auth reconstruction** | `from_auth()` / `to_auth()` | Robust distributed support |
| **Less invasive** | Incremental changes | Lower merge risk |

#### Cons

| Disadvantage | Description | Impact |
|--------------|-------------|--------|
| **No Asset model** | Relies on raw dicts | Verbose filtering, typo-prone |
| **Dict-based credentials** | S3 creds as raw dict | Less type safety |
| **Fragmented structure** | Multiple packages | Harder to navigate |
| **No FileSystemFactory** | Inline filesystem creation | Less consistent |
| **Limited Store refactoring** | Incremental only | Missed DI opportunity |
| **No HTTP in AuthContext** | S3 credentials only | No HTTPS fallback support |

---

## Recommendations

### For a Merged Architecture

The ideal architecture would combine the strengths of both branches:

```
earthaccess/
├── query/                     # From Opus
│   ├── base.py               # Auth-decoupled QueryBase
│   ├── types.py              # BoundingBox, DateRange with to_cmr()/to_stac()
│   ├── granule_query.py      # Supports kwargs + method chaining
│   ├── collection_query.py
│   └── validation.py         # ValidationResult accumulator
│
├── stac/                      # From Opus
│   └── converters.py         # Bidirectional UMM ↔ STAC conversion
│
├── store_components/          # From GLM
│   ├── asset.py              # Rich Asset + AssetFilter
│   ├── credentials.py        # S3Credentials + CredentialManager
│   │                         # (add from_auth/to_auth from Opus)
│   ├── filesystems.py        # FileSystemFactory
│   └── cloud_transfer.py     # CloudTransfer
│
├── main_store.py              # From GLM (with DI)
├── parallel.py                # Either (nearly identical)
├── streaming.py               # From Opus (dedicated module)
└── target_filesystem.py       # Either (identical)
```

### Priority Matrix for Feature Adoption

| Feature | Priority | Source | Rationale |
|---------|----------|--------|-----------|
| Auth-decoupled queries | **High** | Opus | Core usability improvement |
| Bidirectional STAC | **High** | Opus | Essential for ecosystem |
| ValidationResult pattern | **High** | Opus | Better user experience |
| Store with DI | **High** | GLM | Testability, maintainability |
| Asset/AssetFilter | **Medium** | GLM | Valuable but not critical |
| Type classes (BoundingBox) | **Medium** | Opus | Nice-to-have type safety |
| S3Credentials dataclass | **Medium** | GLM | Type safety for credentials |
| Auth reconstruction | **Medium** | Opus | Important for distributed |
| FileSystemFactory | **Low** | GLM | Internal implementation detail |
| CloudTransfer | **Low** | GLM | Advanced feature |

### Migration Path

1. **Phase 1: Adopt Opus query architecture**
   - Port `query/` package from Opus
   - Update API to accept query objects
   - Maintain backward compatibility with kwargs

2. **Phase 2: Add bidirectional STAC**
   - Port `stac/` package from Opus
   - Enable external STAC catalog support

3. **Phase 3: Refactor Store with GLM's DI**
   - Introduce CredentialManager and FileSystemFactory
   - Add session cloning from GLM
   - Include Auth reconstruction from Opus

4. **Phase 4: Add Asset model from GLM**
   - Port Asset and AssetFilter
   - Integrate with DataGranule

---

## Conclusion

Both branches represent significant, well-designed improvements to earthaccess. They are **complementary rather than competing** - each focused on different aspects of the library:

- **stac-distributed-glm** excels at Store architecture, asset handling, and credential management
- **stac-distributed-opus** excels at query building, STAC conversion, and distributed computing flexibility

A thoughtful merge combining the best of both would yield the strongest result:
- Opus's query architecture for usability
- Opus's STAC converters for ecosystem interoperability
- GLM's Store architecture for testability
- GLM's Asset model for rich filtering

The key insight is that these branches complement each other, and a hybrid approach would capture the benefits of both while minimizing their individual weaknesses.
