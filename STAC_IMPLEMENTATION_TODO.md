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

## Phase 2: Results Classes Enhancement (Group B) 🔲

### 2.1 Create Results Base Classes
- [ ] Create `ResultsBase` abstract class with lazy pagination
- [ ] Implement CMR `search-after` token pagination
- [ ] Add `pages()` method for batch iteration
- [ ] Add `get_all()` for backwards compatibility

### 2.2 Enhance DataGranule
- [ ] Add `to_stac()` method to convert to pystac Item
- [ ] Add `to_dict()` method for serialization (needed for distributed)
- [ ] Add asset filtering methods

### 2.3 Enhance DataCollection
- [ ] Add `to_stac()` method to convert to pystac Collection

### 2.4 Tests
- [ ] Extend `tests/unit/test_results.py` with STAC conversion tests

---

## Phase 3: STAC Conversion (Group D) 🔲

### 3.1 UMM to STAC Converter
- [ ] Create `earthaccess/stac/__init__.py` package
- [ ] Create `earthaccess/stac/converters.py`
- [ ] Implement `umm_granule_to_stac_item()` function
- [ ] Implement `umm_collection_to_stac_collection()` function

### 3.2 STAC to UMM Converter (for external STAC)
- [ ] Implement `stac_item_to_data_granule()` function
- [ ] Implement `stac_collection_to_data_collection()` function

### 3.3 Tests
- [ ] Create `tests/unit/test_stac_converters.py`

---

## Phase 4: Streaming Execution (Group B.3) 🔲

### 4.1 Auth Context
- [ ] Create `AuthContext` dataclass for credential shipping
- [ ] Create `WorkerContext` for thread-local state

### 4.2 StreamingExecutor
- [ ] Create `StreamingExecutor` class for lazy iterables
- [ ] Implement producer-consumer pattern with backpressure
- [ ] Add progress bar support

### 4.3 GranuleResults Methods
- [ ] Add `open()` method with streaming support
- [ ] Add `download()` method with streaming support
- [ ] Add `process()` method for map-style processing

### 4.4 Tests
- [ ] Create `tests/unit/test_streaming.py`

---

## Phase 5: Credential Management (Group 0.2) 🔲

### 5.1 Credential Manager
- [ ] Create `CredentialManager` class
- [ ] Implement S3 credential caching with expiration
- [ ] Add URL-to-provider inference (bucket prefix mapping)

### 5.2 Tests
- [ ] Create `tests/unit/test_credentials.py`

---

## Phase 6: API Integration 🔲

### 6.1 Update search_data()
- [ ] Accept `GranuleQuery` objects
- [ ] Maintain backward compatibility with kwargs

### 6.2 Update search_datasets()
- [ ] Accept `CollectionQuery` objects
- [ ] Maintain backward compatibility

### 6.3 Update open() and download()
- [ ] Add `streaming` parameter
- [ ] Add `auto_batch` parameter

### 6.4 Tests
- [ ] Update `tests/unit/test_api.py`

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

### 2024-12-22 - Session Start
- Analyzed STAC improvements plan
- Reviewed existing codebase structure
- Created implementation todo list
- Identified key SOLID principles to apply

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
| | | |

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
