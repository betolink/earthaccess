# Legacy Code Modernization Plan

## Overview

This plan simplifies legacy code and fixes all standing ruff/type issues while maintaining backward compatibility.

## Goals

1. **Fix all 141 ruff errors**
2. **Fix all mypy type errors**
3. **Simplify complex code**
4. **Modernize patterns** (type hints, f-strings, match/case)
5. **Maintain backward compatibility**
6. **Pass all pre-commit checks**

## Phase 1: Issue Classification

### 1.1 Ruff Issues Breakdown

| Category | Count | Files |
|-----------|--------|--------|
| Import issues | 1 | `cloud_transfer.py` |
| Type imports (TYPE_CHECKING) | 1 | `stac_search.py` |
| Type mismatches (Auth/Store) | 60+ | `results.py`, `query.py`, test files |
| Type mismatches (method signatures) | 30+ | `query.py`, `geometry.py`, `test_flexible_inputs.py` |
| Unused variables/imports | 2 | ✅ Fixed (`test_store_credentials.py`, `test_stac_search.py`) |
| Optional parameter handling | 4 | `asset.py`, `test_asset.py` |
| Other logic errors | 20+ | `cloud_transfer.py`, `query.py` |

### 1.2 MyPy Issues Breakdown

| Category | Count | Files |
|-----------|--------|--------|
| TYPE_CHECKING missing | 1 | `stac_search.py` |
| Type annotation needed | 20+ | `asset.py`, `query.py`, test files |
| Import resolution | 2 | `cloud_transfer.py` |
| Type variable issues | 1 | `results.py` |

### 1.3 Simplification Opportunities

| Area | Simplification | Impact |
|-------|----------------|---------|
| Credential creation | Use CredentialManager | 15 lines reduced |
| Filesystem creation | Use FileSystemFactory | 20 lines reduced |
| Bare except | Add specific exceptions | 5 fixes |
| Tuple/List conversions | Remove redundant conversions | 10 lines reduced |
| String formatting | Use f-strings | 30 instances |
| Dict access safety | Use .get() with defaults | 20 instances |

## Phase 2: Type Import Fixes (Priority 1)

### 2.1 Fix cloud_transfer.py TYPE_CHECKING

**Current:**
```python
from typing import Any, Dict, List, Optional, Tuple, Union

import fsspec
import s3fs

from ..auth import Auth
from ..parallel import get_executor
from ..target_filesystem import TargetLocation


class CloudTransfer:
    def __init__(
        self,
        auth: Auth,
        credential_manager: Optional["CredentialManager"] = None,
    ) -> None:
```

**Fixed:**
```python
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import fsspec
import s3fs

from ..auth import Auth
from ..parallel import get_executor
from ..target_filesystem import TargetLocation

if TYPE_CHECKING:
    from .credentials import CredentialManager
    from ..results import DataGranule
    from .credentials import AuthContext


class CloudTransfer:
    def __init__(
        self,
        auth: Auth,
        credential_manager: Optional["CredentialManager"] = None,
    ) -> None:
```

### 2.2 Fix stac_search.py TYPE_CHECKING

**Current:**
```python
try:
    import pystac_client
except ImportError:
    raise ImportError(...)
```

**Fixed:**
```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pystac_client import Client
else:
    Client = None  # type: ignore[assignment]


def search_stac(...):
    if Client is None:
        raise ImportError(
            "pystac-client is required for STAC catalog searches. "
            "Install it with: pip install pystac-client"
        )
    client = Client.open(url)
```

## Phase 3: Type Mismatch Fixes (Priority 2)

### 3.1 Fix Auth/Store Union Issues

**Problem:** Many places accept `Auth | Store` but only work with `Auth`

**Solution Pattern:**
```python
# OLD
def __init__(self, auth: Union[Auth, Store] = None, ...):
    self.auth = auth

# NEW
def __init__(self, auth: Optional[Auth] = None, ...):
    # If Store passed, extract Auth
    if hasattr(auth, "auth"):
        auth = auth.auth  # type: ignore[attr-defined]
    self.auth = auth
```

**Files to fix:**
- `earthaccess/results.py` (DataCollection.query())
- `earthaccess/results.py` (DataGranule.query())
- `tests/unit/test_geometry.py` (7 instances)
- `tests/unit/test_flexible_inputs.py` (14 instances)

### 3.2 Fix Method Signature Type Issues

#### 3.2.1 Fix bbox/point parameter validation

**Current in `query.py`:**
```python
def _validate_bbox(self, bbox: BBoxLike | float) -> BBoxLike:
    if isinstance(bbox, (float, int)):
        bbox = (float(bbox), -90.0, 180.0, 90.0)
    if hasattr(bbox, "tolist"):
        bbox = tuple(bbox.tolist())
    return bbox
```

**Simplified:**
```python
def _validate_bbox(self, bbox: BBoxLike | float) -> Tuple[float, float, float, float]:
    if isinstance(bbox, (float, int)):
        return (float(bbox), -90.0, 180.0, 90.0)
    if isinstance(bbox, np.ndarray):
        bbox = tuple(bbox)  # type: ignore[misc]
    return tuple(bbox)
```

#### 3.2.2 Fix temporal validation

**Current:**
```python
def temporal(self, start_or_temporal, end=None) -> "GranuleQuery":
    ...
```

**Simplified:**
```python
from collections.abc import Sequence
from typing import overload, Union

@overload
def temporal(self, start_or_temporal: str, end: str) -> "GranuleQuery": ...

@overload
def temporal(self, start_or_temporal: Tuple[str | None, str | None]) -> "GranuleQuery": ...

@overload
def temporal(self, start_or_temporal: Sequence[str | None]) -> "GranuleQuery": ...

def temporal(self, start_or_temporal: TemporalLike, end=None) -> "GranuleQuery":
    ...
```

#### 3.2.3 Fix coordinates method

**Current:**
```python
def coordinates(self, coords: List[Tuple[float, float]] | List[List[float]] | List[DataPoint] | str) -> "GranuleQuery":
    ...
```

**Simplified:**
```python
def coordinates(self, coords: CoordinatesLike) -> "GranuleQuery":
    """Add coordinate filter.

    Args:
        coords: Coordinate data as list of tuples, list of lists,
                DataPoint object, or GeoJSON geometry dict
    """
    if isinstance(coords, dict):
        # Handle GeoJSON
        geom = load_geometry(coords)
        coords = geom_to_coords(geom)
    # ... rest of logic
```

### 3.3 Fix Geometry Type Handling

**Current in `geometry.py`:**
```python
def load_geometry(geometry: GeometryLike) -> shapely.geometry.base.BaseGeometry:
    if isinstance(geometry, str):
        return shapely.from_wkt(geometry)
    elif isinstance(geometry, Path):
        return shapely.from_wkt(geometry.read_text())
    elif isinstance(geometry, dict):
        return shapely.geometry.shape(geometry)
    elif has_shapely_geometry(geometry):
        return geometry
```

**Simplified:**
```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import shapely.geometry as geom_module
    BaseGeometry = geom_module.base.BaseGeometry
else:
    BaseGeometry = object  # type: ignore[assignment]


def load_geometry(geometry: GeometryLike) -> BaseGeometry:
    if isinstance(geometry, str):
        return shapely.from_wkt(geometry)
    elif isinstance(geometry, Path):
        return shapely.from_wkt(geometry.read_text())
    elif isinstance(geometry, dict):
        return shapely.geometry.shape(geometry)
    elif isinstance(geometry, BaseGeometry):
        return geometry
    raise ValueError(f"Invalid geometry type: {type(geometry)}")
```

## Phase 4: Simplification (Priority 3)

### 4.1 Replace Bare Except with Specific Exceptions

**Files:**
- `cloud_transfer.py`

**Current:**
```python
try:
    urls.extend(granule.data_links(access="direct"))
except:
    urls.extend(granule.data_links(access="onprem"))
```

**Fixed:**
```python
try:
    urls.extend(granule.data_links(access="direct"))
except (AttributeError, KeyError):
    # Fallback to on-prem links
    urls.extend(granule.data_links(access="onprem"))
```

### 4.2 Use f-strings Instead of .format()

**Pattern:**
```python
# OLD
raise ValueError(f"Invalid target type: {type(target)}")

# NEW
raise ValueError(f"Invalid target type: {type(target)}")
```

**Files with many .format() calls:**
- `query.py`: ~15 instances
- `cloud_transfer.py`: ~10 instances

### 4.3 Simplify Conditional Logic

**Example in `cloud_transfer.py`:**
```python
# OLD
if strategy == "s3_server_copy":
    return self._s3_server_copy(...)
elif strategy == "download_upload":
    return self._download_upload(...)
else:
    return self._generic_transfer(...)

# NEW - Use match (Python 3.10+)
match strategy:
    case "s3_server_copy":
        return self._s3_server_copy(...)
    case "download_upload":
        return self._download_upload(...)
    case _:
        return self._generic_transfer(...)
```

### 4.4 Remove Redundant Conversions

**Example:**
```python
# OLD
if hasattr(bbox, "tolist"):
    bbox = tuple(bbox.tolist())
elif isinstance(bbox, np.ndarray):
    bbox = tuple(bbox)
else:
    bbox = tuple(bbox)

# NEW
if isinstance(bbox, np.ndarray):
    bbox = tuple(bbox)
else:
    bbox = tuple(bbox)
```

## Phase 5: Optional Parameter Handling (Priority 4)

### 5.1 Fix AssetFilter Optional Parameters

**Current in `asset.py`:**
```python
def role_filter(
    self,
    include_roles: Set[str] = None,
    exclude_roles: Set[str] = None,
) -> "AssetFilter":
    return self.copy(include_roles=include_roles, exclude_roles=exclude_roles)
```

**Fixed:**
```python
def role_filter(
    self,
    include_roles: Optional[Set[str]] = None,
    exclude_roles: Optional[Set[str]] = None,
) -> "AssetFilter":
    # Only pass non-None values
    kwargs: Dict[str, Any] = {}
    if include_roles is not None:
        kwargs["include_roles"] = include_roles
    if exclude_roles is not None:
        kwargs["exclude_roles"] = exclude_roles
    return self.copy(**kwargs)
```

### 5.2 Fix Test Optional Parameters

**Current in `test_asset.py`:**
```python
def test_combine_filters(self):
    filter1 = AssetFilter(content_types=["application/netcdf"])
    filter2 = AssetFilter(include_roles={"data"})
    combined = filter1.combine(filter2)
```

**Fixed:**
```python
def test_combine_filters(self):
    filter1 = AssetFilter(content_types=["application/netcdf"])
    filter2 = AssetFilter(include_roles={"data"})
    combined = filter1.combine(filter2)
    # Assert set() for optional params
    assert isinstance(combined.include_roles, set)
```

## Phase 6: Cloud Transfer Bug Fixes (Priority 5)

### 6.1 Fix Undefined Variable

**File:** `cloud_transfer.py`

**Current:**
```python
def _download_upload(self, granules, target_url, ...):
    ...
    with target_s3fs.open(
        target_bucket + "/" + target_key, "wb"
    ) as dst:
```

**Fixed:**
```python
def _download_upload(self, granules, target_url, ...):
    ...
    # Parse target to extract bucket
    parsed = urllib.parse.urlparse(target_url)
    target_bucket = parsed.netloc
    with target_s3fs.open(
        target_bucket + "/" + target_key, "wb"
    ) as dst:
```

### 6.2 Fix fsspec.exists Usage

**Current:**
```python
if not fsspec.exists(dst_path):
    ...
```

**Fixed:**
```python
if not fs.exists(dst_path):
    ...
```

### 6.3 Fix Type Mismatch for copy()

**Current:**
```python
fs.copy(
    src_path,
    dst_path,
    recursive=False,  # Wrong type - should be bool
)
```

**Fixed:**
```python
fs.copy(src_path, dst_path, recursive=False)
```

### 6.4 Fix Unused Variable

**Current:**
```python
source_s3fs = self._get_s3_fs_from_context(source_fs)
target_s3fs = self._get_s3_fs_from_context(target_fs)
```

**Fixed:**
```python
source_s3fs = self._get_s3_fs_from_context(source_fs)
# target_s3fs not needed in _s3_server_copy
```

## Phase 7: Documentation and Tests

### 7.1 Add Type Stubs

Create stubs for external libraries:
```bash
stubs/
  pystac_client.pyi  # For optional pystac_client
```

### 7.2 Update mypy overrides

**Current in `pyproject.toml`:**
```toml
[[tool.mypy.overrides]]
module = ["fsspec.*", "h5netcdf.*", "dask.*", "kerchunk.*", "s3fs", "tinynetrc.*", "vcr.unittest"]
ignore_missing_imports = true
```

**Add:**
```toml
[[tool.mypy.overrides]]
module = ["pystac_client"]
ignore_missing_imports = true
```

### 7.3 Add Type Self Annotations

**Pattern:**
```python
from __future__ import annotations

class Store:
    def __init__(self, auth: Optional[Auth] = None) -> None:
        self.auth: Optional[Auth] = auth
```

## Phase 8: Incremental Execution Order

### Week 1: Type Infrastructure
1. Add TYPE_CHECKING to all files
2. Create stubs for pystac_client
3. Fix import resolution issues

**Files:** `cloud_transfer.py`, `stac_search.py`, `query.py`

### Week 2: Core Type Fixes
1. Fix Auth/Store union issues
2. Fix method signature issues
3. Fix Optional parameter handling

**Files:** `results.py`, `query.py`, `asset.py`, `geometry.py`

### Week 3: Simplification
1. Replace bare except with specific exceptions
2. Use f-strings instead of .format()
3. Simplify conditional logic
4. Remove redundant conversions

**Files:** `query.py`, `cloud_transfer.py`

### Week 4: Cloud Transfer Fixes
1. Fix undefined variables
2. Fix fsspec usage
3. Fix type mismatches
4. Remove unused variables

**Files:** `cloud_transfer.py`

### Week 5: Test Fixes
1. Fix test type mismatches
2. Fix test optional parameters
3. Add type self annotations

**Files:** `test_flexible_inputs.py`, `test_geometry.py`, `test_cloud_transfer.py`, `test_asset.py`, `test_store_credentials.py`, `test_basic_query.py`

### Week 6: Verification
1. Run ruff with --fix
2. Run mypy
3. Run pytest
4. Run pre-commit
5. Fix remaining issues

## Phase 9: Testing Strategy

### 9.1 Test Commands

```bash
# Type checking
mypy earthaccess/store_components/
mypy earthaccess/results.py

# Ruff checking
ruff check earthaccess/ tests/ --fix
ruff check earthaccess/ --fix

# Unit tests
python -m pytest tests/unit/ -x -v  # Stop on first failure

# Full test suite
python -m pytest tests/ -v
```

### 9.2 Pre-commit Workflow

```bash
# Run all pre-commit checks
pre-commit run --all-files --hook-stage commit

# Run specific hooks
pre-commit run ruff --all-files
pre-commit run ruff-format --all-files
pre-commit run mypy --all-files
```

## Phase 10: Success Metrics

### 10.1 Before Metrics

```bash
# Count issues
ruff check earthaccess/ tests/ --statistics
mypy earthaccess/ | grep -c "error"
python -m pytest tests/unit/ --tb=no -q  # Count failures
```

### 10.2 After Metrics

Target goals:
- **0** ruff errors
- **0** ruff warnings
- **0** mypy errors
- **All** tests passing (256+)
- **All** pre-commit hooks passing

## Phase 11: Risk Mitigation

### 11.1 Backward Compatibility

**Strategy:**
1. Keep public API unchanged
2. Only change private methods
3. Add deprecation warnings before removing
4. Add integration tests for public APIs

### 11.2 Branching Strategy

```bash
# Create feature branches for each phase
git checkout -b fix/type-infrastructure
git checkout -b fix/core-types
git checkout -b fix/simplification
git checkout -b fix/cloud-transfer
git checkout -b fix/tests
```

### 11.3 Code Review Checkpoints

After each week:
1. Create pull request
2. Get code review
3. Merge to main
4. Update this plan with progress

## Phase 12: Documentation Updates

### 12.1 Update Type Hints

Document changes in:
- `docs/user-reference/typing.md` (new file)
- Update existing API docs with new type hints

### 12.2 Update Contributing Guide

Update `docs/contributing/development.md`:
- Document TYPE_CHECKING pattern
- Document type annotation style
- Document modern Python patterns

### 12.3 Update Migration Guide

Update `docs/contributing/migration-guide-stac-improvements.md`:
- Note internal changes
- Assure backward compatibility
- Document any breaking internal changes

## Estimated Timeline

| Phase | Duration | Completion |
|--------|-----------|------------|
| Phase 1: Classification | 4 hours | Week 1 |
| Phase 2: Type Imports | 4 hours | Week 1 |
| Phase 3: Core Type Fixes | 12 hours | Week 2 |
| Phase 4: Simplification | 8 hours | Week 3 |
| Phase 5: Optional Params | 4 hours | Week 3 |
| Phase 6: Cloud Transfer Fixes | 6 hours | Week 4 |
| Phase 7: Docs & Tests | 6 hours | Week 5 |
| Phase 8: Incremental Exec | Ongoing | Weeks 1-6 |
| Phase 9: Testing | 4 hours | Week 6 |
| Phase 10: Success Metrics | 2 hours | Week 6 |
| Phase 11: Risk Mitigation | Ongoing | All weeks |
| Phase 12: Documentation | 4 hours | Week 6 |
| **Total** | **~58 hours** | **6 weeks** |

## Success Criteria

1. ✅ All 141 ruff errors fixed
2. ✅ All mypy errors fixed
3. ✅ All unit tests passing (256+)
4. ✅ Pre-commit checks pass
5. ✅ Code simplified where possible
6. ✅ Modern Python patterns used
7. ✅ Type safety improved
8. ✅ No breaking API changes
9. ✅ Documentation updated
10. ✅ Migration guide updated

## Notes

### Modern Python Patterns

Use these patterns throughout the codebase:

1. **Type hints with PEP 695**
   ```python
   from __future__ import annotations

   class MyClass:
       attr: str
   ```

2. **TYPE_CHECKING for imports**
   ```python
   from typing import TYPE_CHECKING

   if TYPE_CHECKING:
       from module import Class
   else:
       Class = Any  # type: ignore[assignment]
   ```

3. **f-strings instead of .format()**
   ```python
   # OLD: f"{var}"
   # NEW: f"{var}"
   ```

4. **match/case instead of if/elif chains**
   ```python
   match value:
       case "a": ...
       case "b": ...
       case _: ...
   ```

5. **Specific exceptions instead of bare except**
   ```python
   try:
       ...
   except (ValueError, TypeError) as e:
       ...
   ```

### Files NOT to Modify

- `store_components/asset.py` - Already minimal issues
- `store_components/stac_search.py` - Already minimal issues
- `store_components/credentials.py` - Already working
- `store_components/filesystems.py` - Already working
- `store_components/geometry.py` - Minor type fixes only
- New test files - Already passing

### Files TO Modify

**Priority 1 (Core):**
- `earthaccess/results.py` - ~10 type issues
- `earthaccess/store.py` - Refactoring separate, not in this plan
- `earthaccess/query.py` - ~50 issues
- `earthaccess/store_components/cloud_transfer.py` - ~20 issues

**Priority 2 (Support):**
- `earthaccess/store_components/geometry.py` - ~10 type issues
- `earthaccess/store_components/asset.py` - ~4 optional param issues

**Priority 3 (Tests):**
- `tests/unit/test_flexible_inputs.py` - ~20 type issues
- `tests/unit/test_geometry.py` - ~7 type issues
- `tests/unit/test_cloud_transfer.py` - ~10 type issues
- `tests/unit/test_asset.py` - ~2 type issues
- `tests/unit/test_basic_query.py` - ~5 type issues
- `tests/unit/test_store_credentials.py` - ~1 type issue
