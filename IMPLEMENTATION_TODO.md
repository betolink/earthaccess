# Earthaccess Next-Gen Implementation TODO

**Branch:** `nextgen`
**Status:** Phase 1 In Progress
**Last Updated:** 2025-12-27

## Executive Summary

This document tracks the incremental implementation of the earthaccess next-generation vision as outlined in `docs/refactoring/nextgen-implementation.md` and `docs/refactoring/earthaccess-nextgen.md`.

The implementation is divided into 8 phases spanning ~12-14 weeks, combining the best components from `stac-distributed-glm` and `stac-distributed-opus` branches.

**Total Acceptance Criteria:** 63 across all phases
**Estimated Effort:** 12-14 weeks

---

## Phase 1: Query Architecture (Foundation)

**Priority:** High
**Status:** In Progress
**Estimated Effort:** 1-2 weeks
**Source:** Opus branch
**Started:** 2025-12-27

### Objective

Establish the auth-decoupled query system that enables flexible query construction and validation before execution.

### Components to Port

| File | Lines | Status | Notes |
|------|-------|--------|-------|
| `query/__init__.py` | ~33 | ✅ Completed | Package exports including StacItemQuery |
| `query/base.py` | ~182 | ✅ Completed | `QueryBase` ABC with `parameters()` introspection |
| `query/types.py` | ~327 | ✅ Completed | `BoundingBox`, `DateRange`, `Point`, `Polygon` |
| `query/granule_query.py` | ~503 | ✅ Completed | `GranuleQuery` with all CMR parameters |
| `query/collection_query.py` | ~392 | ✅ Completed | `CollectionQuery` with all CMR parameters |
| `query/validation.py` | ~175 | ✅ Completed | `ValidationResult`, `ValidationError` accumulator |
| `query/stac_query.py` | ~218 | ✅ Completed | NEW: `StacItemQuery` with STAC-native parameters |

### Key Design Decisions

- [x] No auth at construction: Queries are pure data structures
- [x] Dual construction: Support both kwargs and method chaining
- [x] Dual output: `to_cmr()` and `to_stac()` on all query objects
- [x] Validation accumulator: `validate()` returns `ValidationResult` with all errors
- [x] CMR to STAC parameter mapping: Create mapping table for common parameters

### Acceptance Criteria

- [x] `GranuleQuery` and `CollectionQuery` can be constructed without auth
- [x] `StacItemQuery` can be constructed with STAC-native parameters
- [x] Both kwargs and method chaining work
- [x] `validate()` returns all errors, not just the first
- [x] `to_cmr()` and `to_stac()` produce correct output
- [x] CQL2 filters are generated for cloud_coverage and similar parameters
- [ ] Geometry files are auto-simplified to <300 points
- [ ] `search_data(query=query)` works
- [ ] Legacy `search_data(short_name=...)` still works
- [x] All existing tests pass (50 tests in test_query.py)
- [x] Tests ported: `tests/unit/test_query.py` (~418 lines)
- [x] Tests ported: `tests/unit/test_api_query_integration.py` (~262 lines)

### Implementation Subtasks

- [x] Copy `query/` directory structure from Opus branch
- [x] Port `query/base.py` with `QueryBase` ABC
- [x] Port `query/types.py` with all geometric types
- [x] Port `query/granule_query.py` with CMR parameter coverage
- [x] Port `query/collection_query.py`
- [x] Port `query/validation.py`
- [x] Create `StacItemQuery` class (NEW from vision)
- [x] Implement CMR to STAC parameter mapping in StacItemQuery
- [x] Port all query tests (50 tests passing)
- [ ] Update `api.py` to accept `query` parameter in `search_data()`
- [ ] Update `api.py` to accept `query` parameter in `search_datasets()`
- [ ] Test backward compatibility with existing kwargs
- [ ] Document new query API in docstrings

---

## Phase 2: Bidirectional STAC Conversion and Results

**Priority:** High
**Status:** Not Started
**Estimated Effort:** 1-2 weeks
**Source:** Opus branch

### Objective

Enable full ecosystem interoperability by supporting conversion in both directions: CMR UMM to STAC and STAC to CMR UMM. Also implement lazy pagination for memory-efficient handling of large result sets.

### Components to Port

| File | Lines | Status | Notes |
|------|-------|--------|-------|
| `stac/__init__.py` | ~30 | Not Started | Package exports |
| `stac/converters.py` | ~860 | Not Started | All conversion functions and mapping tables |

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
- [ ] Tests ported: `tests/unit/test_stac_converters.py` (~710 lines)

### Implementation Subtasks

- [ ] Copy `stac/` directory structure from Opus branch
- [ ] Port all conversion functions from converters.py
- [ ] Port all mapping tables (CMR URL types, metadata, etc)
- [ ] Implement `stac_item_to_data_granule()` for external catalog support
- [ ] Implement `stac_collection_to_data_collection()`
- [ ] Port all STAC conversion tests
- [ ] Implement lazy pagination in results object
- [ ] Add `pages()` method to SearchResults
- [ ] Add `__iter__()` for direct iteration
- [ ] Test memory bounds with large result sets
- [ ] Document STAC integration in docstrings

---

## Phase 3: Credential Management and Store Refactoring

**Priority:** High
**Status:** Not Started
**Estimated Effort:** 2-3 weeks
**Source:** Hybrid (GLM structure + Opus features)

### Objective

Create a robust, type-safe credential system with dependency injection that supports both thread-based and distributed execution.

### Components to Create

| File | Source | Status | Key Features |
|------|--------|--------|--------------|
| `store/credentials.py` | GLM + Opus | Not Started | `S3Credentials` dataclass + `from_auth()`/`to_auth()` |
| `store/filesystems.py` | GLM | Not Started | `FileSystemFactory` for consistent filesystem creation |
| `streaming.py` | Opus | Not Started | `AuthContext`, `WorkerContext`, `StreamingIterator` |
| `credentials.py` | Opus | Not Started | Standalone `CredentialManager` |

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
- [ ] Tests ported: `tests/unit/test_credentials.py` (~485 lines)
- [ ] Tests ported: `tests/unit/test_store_credentials.py` (~351 lines)
- [ ] Tests ported: `tests/unit/test_streaming.py` (~400 lines)

### Implementation Subtasks

- [ ] Create `store/credentials.py` with `S3Credentials` dataclass
- [ ] Implement `from_auth()` class method
- [ ] Implement `to_auth()` method for worker reconstruction
- [ ] Create `AuthContext` dataclass with serialization support
- [ ] Create `store/filesystems.py` with `FileSystemFactory`
- [ ] Create `CredentialManager` class
- [ ] Implement session cloning for thread-based execution
- [ ] Port credential-related tests
- [ ] Refactor Store class with dependency injection
- [ ] Update Store to use FileSystemFactory
- [ ] Test end-to-end credential flow

---

## Phase 4: Asset Model and Filtering

**Priority:** Medium
**Status:** Not Started
**Estimated Effort:** 1-2 weeks
**Source:** GLM branch

### Objective

Provide a rich, type-safe model for working with granule assets (files), enabling expressive filtering for download and open operations.

### Components to Port

| File | Lines | Status | Key Features |
|------|-------|--------|--------------|
| `store/asset.py` | ~470 | Not Started | `Asset`, `AssetFilter`, helper functions |

### Acceptance Criteria

- [ ] `Asset` is a frozen dataclass with role checking methods
- [ ] `AssetFilter` supports all documented filter criteria
- [ ] `AssetFilter.combine()` merges filters correctly
- [ ] `filter_assets()` applies filters correctly
- [ ] `DataGranule.assets()` returns `List[Asset]`
- [ ] `download()` and `open()` accept `filter` parameter
- [ ] Simple dict-based filters work for common use cases
- [ ] Glob patterns work for `include_files` and `exclude_files`
- [ ] Tests ported: `tests/unit/test_asset.py` (~453 lines)

### Implementation Subtasks

- [ ] Create `store/asset.py` with `Asset` frozen dataclass
- [ ] Implement `Asset` helper methods (`is_data()`, `is_thumbnail()`, etc)
- [ ] Create `AssetFilter` frozen dataclass
- [ ] Implement `AssetFilter.matches()` logic
- [ ] Implement `AssetFilter.combine()` for filter merging
- [ ] Implement `filter_assets()` function
- [ ] Add `assets()` method to `DataGranule`
- [ ] Add `filter` parameter to `download()` function
- [ ] Add `filter` parameter to `open()` function
- [ ] Implement simple dict-based filters for common use cases
- [ ] Port asset-related tests
- [ ] Support glob patterns for file filtering

---

## Phase 5: Parallel Execution and Distributed Computing

**Priority:** Medium
**Status:** Not Started
**Estimated Effort:** 1 week
**Source:** Either (nearly identical) + Opus streaming

### Objective

Provide a unified executor abstraction supporting serial, threaded, Dask, and Lithops execution. Enable efficient parallel I/O across workers with single authentication handshake.

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
- [ ] Tests ported: `tests/unit/test_parallel.py` (~180 lines)
- [ ] Tests ported: `tests/unit/test_executor_strategy.py` (~125 lines)

### Implementation Subtasks

- [ ] Review `parallel.py` implementations from both branches
- [ ] Implement `Executor` ABC with `submit()` and `map()`
- [ ] Implement `SerialExecutor` for debugging
- [ ] Implement `ThreadPoolExecutorWrapper` with progress support
- [ ] Implement `DaskDelayedExecutor` for Dask integration
- [ ] Implement `LithopsEagerFunctionExecutor` for serverless
- [ ] Create `get_executor()` factory function
- [ ] Implement `StreamingExecutor` with backpressure
- [ ] Integrate auth context shipping to workers
- [ ] Update `Store.download()` to use executors
- [ ] Update `Store.open()` to use executors
- [ ] Port parallel execution tests

---

## Phase 6: Target Filesystem Abstraction

**Priority:** Low
**Status:** Not Started
**Estimated Effort:** 0.5 weeks
**Source:** Either (identical implementations)

### Objective

Abstract the target filesystem for downloads beyond local storage to include cloud object stores.

### Acceptance Criteria

- [ ] Downloads work to local filesystem
- [ ] Downloads work to S3 with credentials
- [ ] Downloads work to GCS with credentials
- [ ] Storage options are properly passed through
- [ ] Tests ported: `tests/unit/test_target_filesystem.py` (~345 lines)

### Implementation Subtasks

- [ ] Review `target_filesystem.py` from both branches
- [ ] Implement `TargetFileSystem` abstraction
- [ ] Support local filesystem as default
- [ ] Support S3 filesystem (s3://)
- [ ] Support GCS filesystem (gs://)
- [ ] Support Azure Blob Storage (az://)
- [ ] Update `download()` to use TargetFileSystem
- [ ] Port target filesystem tests

---

## Phase 7: Results Enhancement

**Priority:** Medium
**Status:** Not Started
**Estimated Effort:** 1 week
**Source:** Both branches

### Objective

Enhance DataGranule and DataCollection with STAC conversion and asset access methods.

### Acceptance Criteria

- [ ] `DataGranule.to_stac()` produces valid STAC Items
- [ ] `DataGranule.assets()` returns `List[Asset]`
- [ ] `DataCollection.to_stac()` produces valid STAC Collections
- [ ] Lazy pagination works with large result sets
- [ ] Memory usage is bounded for large searches
- [ ] `results.pages()` works correctly
- [ ] `for granule in results` works correctly

### Implementation Subtasks

- [ ] Add `to_stac()` method to `DataGranule`
- [ ] Add `assets()` method to `DataGranule`
- [ ] Add `data_assets()` helper to `DataGranule`
- [ ] Add `to_stac()` method to `DataCollection`
- [ ] Enhance `SearchResults` with lazy pagination
- [ ] Implement `pages()` method on SearchResults
- [ ] Implement `__iter__()` on SearchResults
- [ ] Implement bounded memory usage for large result sets
- [ ] Test memory usage with >100k result sets
- [ ] Update docstrings with new methods

---

## Phase 8: VirtualiZarr Integration

**Priority:** Low
**Status:** Not Started
**Estimated Effort:** 1-2 weeks
**Source:** Vision (earthaccess-nextgen.md)

### Objective

Enable cloud-native virtual dataset access using VirtualiZarr, allowing users to create virtual Zarr stores from DMR++ metadata without downloading full data files.

### Acceptance Criteria

- [ ] `open_virtual_mfdataset()` works with DataGranules
- [ ] `open_virtual_mfdataset()` works with SearchResults (lazy pagination)
- [ ] `group` parameter works for hierarchical datasets
- [ ] `load=True` loads coordinate data for indexing
- [ ] Virtual dataset can be persisted to Icechunk
- [ ] Parallel DMR++ parsing uses configured executor

### Implementation Subtasks

- [ ] Review existing `dmrpp_zarr.py` functionality
- [ ] Implement `open_virtual_mfdataset()` function
- [ ] Support DataGranules as input
- [ ] Support SearchResults with lazy pagination
- [ ] Implement `group` parameter for HDF5 groups
- [ ] Implement `load` parameter for coordinate loading
- [ ] Integrate with VirtualiZarr library
- [ ] Support Icechunk persistence
- [ ] Use parallel executor for DMR++ parsing
- [ ] Test with ICESat-2 and other hierarchical datasets

---

## Cross-Phase Tasks

### Testing Infrastructure

- [ ] Review test structure across both branches
- [ ] Consolidate test fixtures
- [ ] Set up continuous integration for nextgen branch
- [ ] Create integration test suite for full workflow
- [ ] Establish performance benchmarks

### Documentation

- [ ] Update main README with new query API
- [ ] Create migration guide from old to new API
- [ ] Document all new classes and functions
- [ ] Add examples for common use cases
- [ ] Document STAC integration points
- [ ] Document parallel execution options
- [ ] Create tutorial notebooks for new features

### Backward Compatibility

- [ ] Ensure all existing tests pass
- [ ] Test existing notebooks work unchanged
- [ ] Document any deprecations
- [ ] Provide deprecation warnings (if needed)
- [ ] Maintain existing API surface

### Performance

- [ ] Benchmark `search_data()` performance
- [ ] Benchmark `download()` performance
- [ ] Benchmark `open()` performance
- [ ] Compare against baseline (main branch)
- [ ] Profile memory usage for large datasets
- [ ] Optimize hotspots if needed

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

## Branch Status

| Phase | Status | Start Date | End Date | Notes |
|-------|--------|------------|----------|-------|
| 1 | Not Started | - | - | Query Architecture |
| 2 | Not Started | - | - | STAC Conversion |
| 3 | Not Started | - | - | Credentials & Store |
| 4 | Not Started | - | - | Asset Model |
| 5 | Not Started | - | - | Parallel Execution |
| 6 | Not Started | - | - | Target Filesystem |
| 7 | Not Started | - | - | Results Enhancement |
| 8 | Not Started | - | - | VirtualiZarr |

---

## Notes

- All phases depend on previous phases being stable
- Code reviews required at end of each phase
- Integration tests after every phase
- Update this file after each completed component
- Reference the implementation plan in `docs/refactoring/nextgen-implementation.md`

---

## Last Updated

2025-12-27 - Initial planning and branch creation
