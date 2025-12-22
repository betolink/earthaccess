# STAC Improvements Implementation Todo List

**Started**: 2024-12-22
**Last Updated**: 2024-12-22

## Implementation Approach

Following SOLID principles, TDD, and Pythonic code:
- **S**ingle Responsibility: Each class has one job
- **O**pen/Closed: Open for extension, closed for modification
- **L**iskov Substitution: Subtypes are substitutable
- **I**nterface Segregation: Small, focused interfaces
- **D**ependency Inversion: Depend on abstractions

## Phase 1: Core Query Architecture (Group A) ✅

### 1.1 Create Query Base Classes
- [x] Create `earthaccess/query/__init__.py` package
- [x] Create `earthaccess/query/base.py` with `QueryBase` abstract class
- [x] Create `earthaccess/query/validation.py` with `ValidationError` dataclass and validators
- [x] Create `earthaccess/query/types.py` for type definitions

### 1.2 Implement GranuleQuery
- [x] Create `earthaccess/query/granule_query.py`
- [x] Implement method chaining API
- [x] Implement named parameter construction
- [x] Implement `to_stac()` conversion
- [x] Implement `to_cmr()` conversion
- [x] Add parameter validation

### 1.3 Implement CollectionQuery
- [x] Create `earthaccess/query/collection_query.py`
- [x] Implement method chaining API
- [x] Implement `to_stac()` conversion
- [x] Implement `to_cmr()` conversion

### 1.4 Tests for Query Architecture
- [x] Create `tests/unit/test_query.py` with 50 tests

---

## Phase 2: Results Classes Enhancement (Group B) ✅

### 2.1 Create Results Base Classes
- [ ] Create `ResultsBase` abstract class with lazy pagination (deferred)
- [ ] Implement CMR `search-after` token pagination (deferred)
- [ ] Add `pages()` method for batch iteration (deferred)
- [ ] Add `get_all()` for backwards compatibility (deferred)

### 2.2 Enhance DataGranule
- [x] Add `to_stac()` method to convert to pystac Item
- [x] Add `to_dict()` method for serialization (needed for distributed)
- [ ] Add asset filtering methods (deferred)

### 2.3 Enhance DataCollection
- [x] Add `to_stac()` method to convert to pystac Collection
- [x] Add `to_dict()` method for serialization

### 2.4 Tests
- [x] Extend `tests/unit/test_results.py` with STAC conversion tests (6 new tests)

---

## Phase 3: STAC Conversion (Group D) ✅

### 3.1 UMM to STAC Converter
- [x] Create `earthaccess/stac/__init__.py` package
- [x] Create `earthaccess/stac/converters.py`
- [x] Implement `umm_granule_to_stac_item()` function
- [x] Implement `umm_collection_to_stac_collection()` function

### 3.2 STAC to UMM Converter (for external STAC)
- [x] Implement `stac_item_to_data_granule()` function
- [x] Implement `stac_collection_to_data_collection()` function

### 3.3 Tests
- [x] Create `tests/unit/test_stac_converters.py` (44 tests)

---

## Phase 4: Streaming Execution (Group B.3) ✅

### 4.1 Auth Context
- [x] Create `AuthContext` dataclass for credential shipping
- [x] Create `WorkerContext` for thread-local state

### 4.2 StreamingExecutor
- [x] Create `StreamingExecutor` class for lazy iterables
- [x] Implement producer-consumer pattern with backpressure
- [x] Add progress bar support

### 4.3 GranuleResults Methods
- [ ] Add `open()` method with streaming support (deferred to API integration)
- [ ] Add `download()` method with streaming support (deferred to API integration)
- [ ] Add `process()` method for map-style processing (deferred to API integration)

### 4.4 Tests
- [x] Create `tests/unit/test_streaming.py` (25 tests)

---

## Phase 5: Credential Management (Group 0.2) ✅

### 5.1 Credential Manager
- [x] Create `CredentialManager` class
- [x] Implement S3 credential caching with expiration
- [x] Add URL-to-provider inference (bucket prefix mapping)

### 5.2 Tests
- [x] Create `tests/unit/test_credentials.py` (32 tests)

---

## Phase 6: API Integration ✅

### 6.1 Update search_data()
- [x] Accept `GranuleQuery` objects via `query` parameter
- [x] Maintain backward compatibility with kwargs
- [x] Validate query before execution
- [x] Raise error if both query and kwargs provided

### 6.2 Update search_datasets()
- [x] Accept `CollectionQuery` objects via `query` parameter
- [x] Maintain backward compatibility with kwargs
- [x] Validate query before execution
- [x] Raise error if both query and kwargs provided

### 6.3 Update __init__.py exports
- [x] Export `GranuleQuery`, `CollectionQuery`, `BoundingBox`, `DateRange`, `Point`, `Polygon`

### 6.4 Tests
- [x] Create `tests/unit/test_api_query_integration.py` with 15 tests

### 6.5 Open/Download streaming (deferred)
- [ ] Add `streaming` parameter to open() and download()
- [ ] Add `auto_batch` parameter

---

## Phase 7: Documentation (Group I) 🔲

### 7.1 User Documentation
- [ ] Update `docs/user_guide/search.md` with new query API
- [ ] Add parallel execution guide
- [ ] Add STAC conversion examples
- [ ] Add migration guide

### 7.2 API Documentation
- [ ] Update docstrings for new classes
- [ ] Generate API reference

---

## Implementation Log

### 2024-12-22 - Phase 6 Complete
- Updated `earthaccess/api.py`:
  - `search_data()` now accepts `query` parameter (GranuleQuery object)
  - `search_datasets()` now accepts `query` parameter (CollectionQuery object)
  - Both validate query before execution and raise ValueError if invalid
  - Both raise ValueError if both query and kwargs are provided
  - Full backward compatibility with existing kwargs API
- Updated `earthaccess/__init__.py`:
  - Export new query classes: `GranuleQuery`, `CollectionQuery`
  - Export type classes: `BoundingBox`, `DateRange`, `Point`, `Polygon`
- Created `tests/unit/test_api_query_integration.py` with 15 tests:
  - Tests for search_data() with GranuleQuery
  - Tests for search_datasets() with CollectionQuery
  - Tests for STAC output
  - Tests for validation
- Total unit tests: 287 (all passing)

### 2024-12-22 - Session Start
- Analyzed STAC improvements plan
- Reviewed existing codebase structure
- Created implementation todo list
- Identified key SOLID principles to apply

### 2024-12-22 - Phase 5 Complete
- Created `earthaccess/credentials.py` module with:
  - `S3Credentials` class - Container for AWS temporary credentials
  - `CredentialCache` class - Thread-safe credential caching
  - `CredentialManager` class - Centralized credential lifecycle management
- Features:
  - Automatic credential expiration handling
  - Bucket-to-provider inference from URL patterns
  - Provider-to-endpoint mapping
  - Thread-safe caching with RLock
  - Force refresh support
- Created `tests/unit/test_credentials.py` with 32 tests

### 2024-12-22 - Phase 4 Complete
- Created `earthaccess/streaming.py` module with:
  - `AuthContext` dataclass - Serializable credential container for distributed workers
  - `WorkerContext` class - Thread-local state management
  - `StreamingIterator` - Lazy iterator with backpressure support
  - `StreamingExecutor` - Iterator-based parallel executor
  - `stream_process()` convenience function
- Features:
  - Thread-local authentication context
  - Backpressure with bounded queues
  - Ordered and unordered result modes
  - Progress bar support via tqdm
- Created `tests/unit/test_streaming.py` with 25 tests

### 2024-12-22 - Phase 3 Complete
- Created `earthaccess/stac/` package with:
  - `__init__.py` - Package exports
  - `converters.py` - Standalone conversion functions
- Implemented UMM to STAC converters:
  - `umm_granule_to_stac_item()` - Convert CMR granule to STAC Item
  - `umm_collection_to_stac_collection()` - Convert CMR collection to STAC Collection
- Implemented STAC to UMM converters (for external STAC sources):
  - `stac_item_to_data_granule()` - Convert STAC Item to DataGranule
  - `stac_collection_to_data_collection()` - Convert STAC Collection to DataCollection
- Created `tests/unit/test_stac_converters.py` with 44 tests

### 2024-12-22 - Phase 2 Complete
- Enhanced `earthaccess/results.py` with STAC conversion:
  - `DataGranule.to_stac()` - Convert CMR UMM granule to STAC Item format
  - `DataGranule.to_dict()` - Return plain dictionary for serialization
  - `DataCollection.to_stac()` - Convert CMR UMM collection to STAC Collection
  - `DataCollection.to_dict()` - Return plain dictionary for serialization
- Added helper methods for temporal, spatial, links, and assets extraction
- Extended `tests/unit/test_results.py` with 6 new tests (total 21 tests)
- Commit: `dc9b277`

### 2024-12-22 - Phase 1 Complete
- Created `earthaccess/query/` package with:
  - `__init__.py` - Package exports
  - `types.py` - Type definitions (BoundingBox, Point, Polygon, DateRange)
  - `validation.py` - ValidationError, ValidationResult, validators
  - `base.py` - QueryBase abstract class
  - `granule_query.py` - GranuleQuery class
  - `collection_query.py` - CollectionQuery class
- Created `tests/unit/test_query.py` with 50 tests (all passing)
- Features implemented:
  - Method chaining API
  - Named parameter construction
  - `to_cmr()` conversion
  - `to_stac()` conversion
  - Query validation
  - Query copying/equality

### What Worked
- Immutable dataclasses for types (BoundingBox, Point, Polygon, DateRange)
- Separating types from query classes (SRP)
- Using Polygon class instead of raw list for type safety
- Comprehensive test coverage from the start

### What Didn't Work
- Initially used `list[tuple[float, float]]` for polygon which caused type narrowing issues
- Fixed by creating proper Polygon dataclass

---

## Commits
(To be updated with each commit)

| Date | Commit | Description |
|------|--------|-------------|
| 2024-12-22 | ed8b1e3 | Add query package with GranuleQuery and CollectionQuery classes |
| 2024-12-22 | dc9b277 | Add to_stac() and to_dict() methods to DataGranule and DataCollection |
| 2024-12-22 | 7b14b51 | Add STAC converters module with bidirectional conversion |
| 2024-12-22 | c989800 | Add streaming execution module with AuthContext and StreamingExecutor |
| 2024-12-22 | 41ce123 | Add CredentialManager for S3 credential caching |
| 2024-12-22 | cdce7de | Add API integration for new query objects |

---

## Notes

### Key Design Decisions
1. **Query objects vs kwargs**: New query objects provide validation and STAC conversion while maintaining backward compatibility
2. **Streaming execution**: Uses producer-consumer pattern to handle large result sets without memory blowup
3. **Credential shipping**: For distributed backends, fetch credentials once and ship to workers
4. **fsspec everywhere**: Use fsspec for all I/O to provide unified interface

### Dependencies to Consider
- `pystac` for STAC model classes
- `pystac-client` compatibility for `to_stac()` output
- `tqdm` for progress bars (already optional)
