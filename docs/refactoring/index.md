# Architecture & Refactoring Documentation

Overview of earthaccess architecture, design decisions, and implementation roadmaps for the next-generation platform.

## Quick Navigation

| Topic | Document | Purpose |
|-------|----------|---------|
| **STAC Integration** | [STAC Module Architecture](./stac-module-architecture.md) | Complete guide to STAC conversion, APIs, and workflows |
| **Implementation History** | [STAC Implementation Log](./stac-implementation-log.md) | Historical record of 7-phase STAC implementation (2024-12-22) |
| **Branch Comparison** | [Architecture Comparison](./stac-comparison.md) | Comparison of GLM vs Opus branch approaches and design tradeoffs |
| **Future Vision** | [Next-Gen Architecture](./earthaccess-nextgen.md) | Complete next-generation design: Query system, STAC, credentials, distributed computing |

---

## By Topic

### STAC Format Conversion

**Goal**: Enable seamless interoperability between NASA's CMR (Common Metadata Repository) and STAC (SpatioTemporal Asset Catalog) formats.

**Key Documents**:
- [STAC Module Architecture](./stac-module-architecture.md) - **START HERE**
  - 4 core conversion functions
  - 18 helper utilities
  - Real-world examples with NASA data
  - Integration with DataGranule and DataCollection
  - Supported STAC extensions

**Quick Facts**:
- ✅ Bidirectional conversion (CMR → STAC and STAC → CMR)
- ✅ Supports STAC 1.0.0 specification
- ✅ 44 unit tests covering all conversions
- ✅ Production-ready since Phase 2 (2024-12-22)

### Query System & Authentication

**Goal**: Decouple query construction from authentication for maximum flexibility.

**Key Features**:
- Build queries without authenticating first
- Support both CMR and STAC-native query interfaces
- Validate queries before execution
- Comprehensive error accumulation (not fail-fast)

**Key Documents**:
- [Next-Gen Architecture](./earthaccess-nextgen.md) - Phase 1: Query Architecture
- Implementation Log shows phases 1-2 details

**Related Classes**:
- `GranuleQuery` - CMR-native granule queries
- `CollectionQuery` - CMR-native collection queries
- `StacItemQuery` - STAC-native queries
- `BoundingBox`, `DateRange`, `Point`, `Polygon` - Geometry types

### Credential Management

**Goal**: Type-safe credential handling with support for distributed execution.

**Key Features**:
- Frozen dataclasses for immutability
- Thread-safe credential caching
- Credential serialization for distributed workers
- Support for S3, HTTP, and URS credentials

**Key Documents**:
- [Next-Gen Architecture](./earthaccess-nextgen.md) - Phase 3: Credential Management
- Implementation Log shows Phase 5 details

**Related Classes**:
- `S3Credentials` - AWS temporary credentials
- `HTTPHeaders` - HTTP authentication
- `AuthContext` - Serializable credential bundle
- `CredentialManager` - Thread-safe credential caching

### Asset Filtering

**Goal**: Rich, type-safe model for working with granule assets (files).

**Key Features**:
- Pattern matching with glob patterns
- Role-based filtering (data, metadata, thumbnail)
- Size-based filtering
- Composable filters with `combine()`

**Key Documents**:
- [Next-Gen Architecture](./earthaccess-nextgen.md) - Phase 4: Asset Model
- Implementation Log shows Phase 4 details

**Related Classes**:
- `Asset` - Frozen dataclass representing a granule file
- `AssetFilter` - Composable filter criteria
- `DataGranule.assets()` - List all assets in a granule
- `DataGranule.data_assets()` - List only data-role assets

### Distributed Computing

**Goal**: Support parallel execution across local threads, Dask clusters, and serverless platforms.

**Key Features**:
- 4 executor backends (Serial, ThreadPool, Dask, Lithops)
- Transparent credential distribution to workers
- Memory-bounded execution with backpressure
- Progress tracking with optional progress bars

**Key Documents**:
- [Next-Gen Architecture](./earthaccess-nextgen.md) - Phase 5: Parallel Execution
- Implementation Log shows Phase 5 details

**Related Functions**:
- `get_executor(backend, max_workers=N)` - Factory function
- `execute_with_credentials(executor, operation, items, auth_context)` - Helper for distributed work

### Architecture Comparison

**Goal**: Understand different architectural approaches and their tradeoffs.

**Key Questions Answered**:
- Query building: Auth-decoupled vs Auth-coupled?
- STAC conversion: One-way vs Bidirectional?
- Store architecture: Complete refactor vs Incremental?
- Asset handling: Rich model vs Deferred to pystac?

**Key Documents**:
- [Architecture Comparison](./stac-comparison.md) - Detailed pros/cons analysis

---

## Implementation Timeline

All phases from Phase 1-8 are **complete** as of December 28, 2025:

| Phase | Name | Status | Date | Tests | Key Docs |
|-------|------|--------|------|-------|----------|
| 1 | Query Architecture | ✅ | 2025-12-27 | 65 | [Implementation Log](./stac-implementation-log.md) |
| 2 | STAC Conversion | ✅ | 2025-12-27 | 44 | [STAC Architecture](./stac-module-architecture.md) |
| 3 | Credentials & Store | ✅ | 2025-12-28 | 108 | Implementation Log |
| 4 | Asset Model | ✅ | 2025-12-28 | 73 | Implementation Log |
| 5 | Parallel Execution | ✅ | 2025-12-28 | 41 | Implementation Log |
| 6 | Target Filesystem | ✅ | 2025-12-28 | 24 | Implementation Log |
| 7 | Results Enhancement | ✅ | 2025-12-28 | 24 | Implementation Log |
| 8 | VirtualiZarr Integration | ✅ | 2025-12-28 | 10 | Implementation Log |
| **TOTAL** | **All Phases** | **✅ 100%** | **Complete** | **635 tests** | **All production-ready** |

---

## For Different Audiences

### I'm a User
- Read the [User Guide](../user/index.md)
- STAC features are documented in [API Reference: STAC](../api/stac/overview.md)
- No need to understand the internal architecture

### I'm a Contributor
- Start with [Next-Gen Architecture](./earthaccess-nextgen.md) to understand the big picture
- Read [STAC Module Architecture](./stac-module-architecture.md) to understand STAC integration
- Review [Architecture Comparison](./stac-comparison.md) to understand design tradeoffs
- Check [Implementation Log](./stac-implementation-log.md) for historical context

### I'm Evaluating Architectural Approaches
- Start with [Architecture Comparison](./stac-comparison.md)
- Dive into [Next-Gen Architecture](./earthaccess-nextgen.md) for complete design details
- Review [Implementation Log](./stac-implementation-log.md) for what was actually implemented

---

## Key Concepts

### STAC (SpatioTemporal Asset Catalog)
A standard for describing geospatial data and spatiotemporal assets, enabling cataloging and discovery across diverse repositories. earthaccess now supports full bidirectional conversion between CMR and STAC formats.

**Resources**:
- [STAC Specification](https://stacspec.org/)
- [STAC Browser](https://stacindex.org/)

### CMR (Common Metadata Repository)
NASA's metadata repository for Earth science data. CMR uses UMM (Unified Metadata Model) to describe collections and granules.

**Resources**:
- [CMR API Documentation](https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html)
- [UMM Specification](https://github.com/nasa/Common-Metadata-Repository/tree/main/umm-spec)

### Query Pattern
Auth-decoupled query construction: build queries without authentication, validate them, and execute later.

```python
# Build without auth
query = earthaccess.GranuleQuery().short_name("ATL06").temporal("2023-01", "2023-02")

# Validate
query.validate()

# Execute with auth
earthaccess.login()
results = earthaccess.search_data(query=query)
```

### Credential Distribution
For distributed execution: capture credentials once during authentication, serialize them, ship them to workers, and reconstruct credentials in worker processes.

```python
# Main process
auth_context = AuthContext.from_auth(earthaccess.__auth__)

# Worker processes receive auth_context and can access S3, HTTP, URS
def download_in_worker(granule, context):
    return granule.download(context=context, path="/data")
```

---

## Design Principles

The earthaccess next-generation architecture follows these core principles:

1. **SOLID Principles**
   - Single Responsibility: Each module has one reason to change
   - Open/Closed: Open for extension, closed for modification
   - Liskov Substitution: Subtypes are substitutable
   - Interface Segregation: Small, focused interfaces
   - Dependency Inversion: Depend on abstractions, not implementations

2. **Type Safety**
   - Comprehensive type hints for IDE support
   - Frozen dataclasses for immutability
   - Explicit error types for better error handling

3. **Backward Compatibility**
   - All existing APIs continue to work
   - New features are opt-in
   - Deprecation path for any breaking changes

4. **Testability**
   - Pure functions where possible
   - Dependency injection for mockability
   - Comprehensive test coverage (>90%)

---

## Related Documentation

- **User Guide**: [Main Documentation](../user/index.md)
- **API Reference**: [Search and Access API](../api/index.md)
- **Governance**: [Architecture Decision Records](../governance/decisions/)

---

## Last Updated

- **Index**: 2025-12-28
- **STAC Architecture**: 2025-12-28
- **Implementation Log**: 2024-12-22 (archived from root `STAC_IMPLEMENTATION_TODO.md`, since removed)
- **Architecture Comparison**: [Existing]
- **Next-Gen Architecture**: [Existing]
