# Earthaccess Codebase Assessment

**Date:** 2025-12-22
**Scope:** High-level assessment of implementation status, redundancies, and simplification opportunities

---

## Executive Summary

The earthaccess codebase has made significant progress on modernization through STAC improvements and store refactoring. However, there is **critical redundancy** between two parallel directory structures (`store/` and `store_components/`) and incomplete integration of new components into the main Store class.

**Key Issues:**
- ✅ Store refactoring infrastructure complete (executor system, target filesystem abstraction)
- ✅ STAC improvements partially implemented (asset filtering, search_stac)
- ❌ Duplicate credential/filesystem code exists in two directories
- ❌ New components not integrated into Store class
- ❌ 14+ mypy type errors remain

---

## 1. Implementation Status

### 1.1 Completed Features ✅

**STAC Improvements** (from CHANGELOG.md):
- `Asset` dataclass for granule asset representation
- `AssetFilter` with method chaining for filtering by content type, roles, bands, size
- `get_data_assets()`, `get_thumbnail_assets()`, `get_browse_assets()` on `DataGranule`
- `filter_assets()` method on `DataGranule`
- `to_stac()` and `to_umm()` methods on `DataGranule` and `DataCollection`
- `search_stac()` function for external STAC catalogs
- Flexible input handling for numpy arrays in bbox, point, polygon
- Geometry simplification for CMR 300-point limit
- `coordinates()` method accepting GeoJSON geometry dicts
- Enhanced provider inference for S3 buckets

**Store Refactoring Infrastructure** (from stac-improvements.md):
- `parallel.py`: Pluggable executor system (SerialExecutor, ThreadPoolExecutorWrapper, DaskDelayedExecutor, LithopsEagerFunctionExecutor)
- `target_filesystem.py`: TargetFilesystem ABC with LocalFilesystem and FsspecFilesystem
- Session strategy selection via `_use_session_cloning()` in Store
- pqdm dependency removed

**Store Components** (store_components/):
- `credentials.py`: S3Credentials dataclass, CredentialManager, AuthContext, infer_provider_from_url
- `filesystems.py`: FileSystemFactory for creating authenticated filesystems
- `asset.py`: Asset filtering and helper functions
- `cloud_transfer.py`: CloudTransfer class for S3↔S3 and cloud transfers
- `query.py`: GranuleQuery and CollectionQuery classes
- `geometry.py`: Geometry loading with shapely
- `results.py`: ResultsBase, LazyResultsBase, ConcreteResultsBase, StreamingExecutor

**Tests:** 256 passing tests, 2 skipped

### 1.2 Missing Features ❌

**Store Integration** (from store-refactoring-plan.md):
- Store class does NOT use CredentialManager
- Store class does NOT use FileSystemFactory
- CloudTransfer not integrated into Store.download() for cloud-to-cloud transfers
- URL-to-provider inference exists but not integrated

**STAC Implementation Plan** (from stac-improvements.md):
- Groups A-J (Query Architecture, Results Classes, Asset Access, etc.) listed as pending
- However, Groups C, D, E, F, G, I appear implemented based on CHANGELOG

**Type Safety:**
- 14+ mypy errors in `stac_search.py` alone
- No ruff errors reported (0), but plans reference fixing 141 errors

---

## 2. Critical Redundancies

### 2.1 Duplicate Directory Structure

**Issue:** Two parallel directories contain identical code:

| File | Location A | Location B | Status |
|------|-----------|-----------|--------|
| `credentials.py` | `earthaccess/store/credentials.py` (300 lines) | `earthaccess/store_components/credentials.py` (300 lines) | **IDENTICAL** (MD5: a61de2055f3...) |
| `filesystems.py` | `earthaccess/store/filesystems.py` (162 lines) | `earthaccess/store_components/filesystems.py` (162 lines) | **IDENTICAL** |
| `__init__.py` | `earthaccess/store/__init__.py` | `earthaccess/store_components/__init__.py` | **Different** |

**Impact:**
- 462 lines of duplicate code (462 + 462 = 924 lines total)
- Maintenance burden - fixes must be applied to both locations
- Risk of divergence
- Confusing import structure

### 2.2 Confusing Import Paths

```python
# From earthaccess/store/__init__.py
from ..main_store import EarthAccessFile, Store, _open_files
from .credentials import AuthContext, CredentialManager, infer_provider_from_url
from .filesystems import FileSystemFactory

# From earthaccess/store_components/__init__.py
from .credentials import AuthContext, CredentialManager, infer_provider_from_url
from .filesystems import FileSystemFactory
from .asset import Asset, AssetFilter
from .cloud_transfer import CloudTransfer
from .query import CollectionQuery, GranuleQuery
```

**Analysis:**
- `store/` seems to be the public package (imports from `main_store.py`)
- `store_components/` seems to be the internal implementation location
- Both expose CredentialManager and FileSystemFactory
- This creates two import paths for the same classes

### 2.3 Split Store Implementation

The Store class is located in `main_store.py` (1195 lines) and does NOT use the new components in `store_components/`.

**Expected per store-refactoring-plan.md:**
```python
class Store:
    def __init__(self, auth=None, threads=8, ...):
        self.auth = auth
        self.credential_manager = CredentialManager(auth)  # NOT DONE
        self.filesystem_factory = FileSystemFactory(...)    # NOT DONE
```

**Actual:**
- Direct credential creation inline in Store methods
- Direct filesystem creation using s3fs directly
- New components exist but are not used

---

## 3. Simplification Recommendations

### 3.1 Eliminate Directory Redundancy

**Action:** Remove `earthaccess/store/` and consolidate everything into `earthaccess/store_components/`.

**Steps:**
1. Delete `earthaccess/store/` directory entirely
2. Update `earthaccess/main_store.py` imports to use `store_components`
3. Update `earthaccess/__init__.py` to expose from `store_components`
4. Add deprecation aliases if needed for backward compatibility

**Impact:** Removes 462 lines of duplicate code, simplifies import structure

### 3.2 Integrate New Components into Store

**Per store-refactoring-plan.md Phase 4:**

| Step | Status |
|------|--------|
| Add `CredentialManager` to `Store.__init__()` | ❌ Not done |
| Refactor `_get_credentials()` to use `CredentialManager.get_credentials()` | ❌ Not done |
| Refactor `_get_filesystem()` to use `FileSystemFactory.get_filesystem()` | ❌ Not done |
| Add `CloudTransfer` integration for cloud-to-cloud downloads | ❌ Not done |

**Action:** Complete Store refactoring as planned to eliminate inline credential and filesystem creation code.

### 3.3 Resolve Type Errors

**Current mypy errors in `stac_search.py`:**
- Missing return type annotations
- `pystac_client` import-not-found (need stub or TYPE_CHECKING pattern)
- Type mismatches (bbox, datetime, intersects, limit parameters)

**Action:** Apply fixes from legacy-code-modernization-plan.md Phase 2 and 3.

### 3.4 Simplify Results Classes

**Current situation:**
- `earthaccess/results.py`: CustomDict, DataCollection, DataGranule (legacy CMR-specific)
- `earthaccess/store_components/results.py`: ResultsBase, LazyResultsBase, ConcreteResultsBase (generic backend-agnostic)

**Recommendation:**
- Keep DataCollection/DataGranule as CMR result models
- Use ResultsBase inheritance pattern where appropriate
- Avoid parallel results implementations

### 3.5 Clarify Query Architecture

**Current:**
- `earthaccess/search.py`: DataGranuleQuery, DataCollectionQuery (legacy CMR)
- `earthaccess/store_components/query.py`: GranuleQuery, CollectionQuery (new backend-agnostic)

**Recommendation:**
- Migrate to new query classes in store_components/
- Deprecate old query classes or alias them
- Consolidate to single query architecture

---

## 4. Code Cleanup Opportunities

### 4.1 Type Safety (from legacy-code-modernization-plan.md)

**High priority:**
- Fix TYPE_CHECKING imports in `cloud_transfer.py`, `stac_search.py`
- Fix Auth/Store union type issues (~60+ issues)
- Fix method signature type issues (bbox, temporal, coordinates)
- Add specific exceptions instead of bare except

### 4.2 Modern Python Patterns

**Recommendations:**
- Use f-strings instead of .format() (30+ instances in query.py, cloud_transfer.py)
- Use match/case instead of if/elif chains (Python 3.10+)
- Use `from __future__ import annotations` for self-referencing types

### 4.3 Remove Dead Code

**Potential candidates for removal:**
- Duplicate `store/` directory (entire)
- Old query classes if new ones are adopted
- Bare except clauses (convert to specific exceptions)
- Unused imports and variables

---

## 5. Priority Actions

### High Priority (blocking clean architecture)

1. **Eliminate `store/` directory** - Remove duplicate code, consolidate to `store_components/`
2. **Integrate CredentialManager into Store** - Complete store-refactoring-plan.md Phase 4
3. **Integrate FileSystemFactory into Store** - Complete store-refactoring-plan.md Phase 4
4. **Fix TYPE_CHECKING imports** - Apply legacy-code-modernization-plan.md Phase 2

### Medium Priority

5. **Fix mypy type errors** - Address remaining 14+ errors
6. **Deprecate old query/results classes** - Migrate to new architecture
7. **Add CloudTransfer integration** - Enable cloud-to-cloud downloads via Store.download()
8. **Apply modern Python patterns** - f-strings, match/case, specific exceptions

### Low Priority

9. **Update documentation** - Reflect new architecture and import paths
10. **Add obstore backend** (optional per stac-improvements.md)
11. **Complete STAC Groups A-J** if still relevant

---

## 6. Architecture Diagram (Proposed)

```
earthaccess/
├── __init__.py                 # Public API exports
├── main_store.py              # Store class using new components
├── auth.py                    # Auth class
├── api.py                     # Public functions (login, search, etc.)
├── results.py                 # DataGranule, DataCollection (CMR models)
├── search.py                  # Search functions (uses query.py)
├── parallel.py                # Executor system ✅
├── target_filesystem.py       # Target abstraction ✅
└── store_components/          # Core components
    ├── __init__.py
    ├── credentials.py          # S3Credentials, CredentialManager
    ├── filesystems.py          # FileSystemFactory
    ├── query.py               # GranuleQuery, CollectionQuery
    ├── asset.py               # Asset, AssetFilter
    ├── cloud_transfer.py       # CloudTransfer
    ├── geometry.py            # Geometry utilities
    └── results.py            # ResultsBase (generic)
```

**Eliminated:** `earthaccess/store/` directory (redundant)

---

## 7. Success Metrics

**Before:**
- 462 lines of duplicate code in `store/`
- Store class using inline credential/filesystem creation
- 14+ mypy type errors
- Two import paths for CredentialManager/FileSystemFactory

**After:**
- 0 lines of duplicate code
- Store using CredentialManager and FileSystemFactory
- 0 mypy type errors
- Single import path from `earthaccess.store_components`

---

## 8. Risks and Mitigation

### Risk: Breaking changes from removing `store/` directory

**Mitigation:**
- Add deprecation aliases in `__init__.py`
- Update all internal imports
- Run full test suite after changes
- Document breaking changes in CHANGELOG

### Risk: Store refactoring introduces bugs

**Mitigation:**
- Incremental refactoring per store-refactoring-plan.md Phase 4
- Run tests after each step
- Maintain backward compatibility for public API

### Risk: Type fixes break existing code

**Mitigation:**
- Only fix internal type annotations
- Keep public API signatures unchanged
- Add deprecation warnings before changing behavior

---

## 9. Summary

The earthaccess project has made excellent progress on modernization but is hampered by:
1. **Critical redundancy** - 462 lines duplicated between `store/` and `store_components/`
2. **Incomplete integration** - New components created but not used by Store
3. **Type safety gaps** - 14+ mypy errors remain

**Path forward:**
1. Remove `store/` directory
2. Integrate CredentialManager and FileSystemFactory into Store
3. Fix type errors
4. Consolidate query/results architecture

This will reduce code by ~500 lines, simplify the import structure, and complete the store refactoring initiative.
