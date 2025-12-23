# Store Refactoring & Legacy Code Cleanup Plan

## Overview

This plan refactors `earthaccess/store.py` to use the new store components (CredentialManager, FileSystemFactory, CloudTransfer) and cleans up legacy code issues.

## Goals

1. **Integrate new store components** into Store class
2. **Fix all ruff/type errors** in legacy code
3. **Maintain backward compatibility** - no breaking changes to public API
4. **Follow SOLID principles** - Single Responsibility, Dependency Inversion

## Phase 1: Current State Analysis

### Existing Store Issues

Current `earthaccess/store.py` problems:
- Direct S3Credentials creation (no CredentialManager usage)
- Direct filesystem creation (no FileSystemFactory usage)
- No CloudTransfer usage (manual download/upload)
- Ruff errors (~30)
- Type errors with Store/Auth ambiguity

### New Components Available

Ready to use:
- ✅ `CredentialManager` - `earthaccess/store_components/credentials.py`
- ✅ `FileSystemFactory` - `earthaccess/store_components/filesystems.py`
- ✅ `CloudTransfer` - `earthaccess/store_components/cloud_transfer.py`

## Phase 2: Refactor Strategy

### 2.1 Update Store.__init__()

**Current pattern:**
```python
class Store:
    def __init__(self, auth=None, threads=8, ...):
        self.auth = auth or earthaccess.__auth__
        self.threads = threads
        # Direct credential creation in methods
```

**New pattern:**
```python
from .store_components.credentials import CredentialManager

class Store:
    def __init__(self, auth=None, threads=8, ...):
        auth = auth or earthaccess.__auth__
        self.auth = auth
        self.credential_manager = CredentialManager(auth)
        self.threads = threads
        self._logger = logging.getLogger(__name__)
```

### 2.2 Replace S3Credentials calls

**Current pattern:**
```python
creds = S3Credentials(
    access_key_id=auth.s3_credentials["accessKeyId"],
    secret_access_key=auth.s3_credentials["secretAccessKey"],
    session_token=auth.s3_credentials["sessionToken"],
    expiration=auth.s3_credentials["expiration"],
    region="us-west-2",
)
```

**New pattern:**
```python
creds = self.credential_manager.get_credentials(provider)
```

### 2.3 Replace filesystem creation

**Current pattern:**
```python
fs = s3fs.S3FileSystem(
    key=creds.access_key_id,
    secret=creds.secret_access_key,
    token=creds.session_token,
    client_kwargs={"region_name": creds.region},
)
```

**New pattern:**
```python
from .store_components.filesystems import FileSystemFactory

factory = FileSystemFactory(self.credential_manager)
fs = factory.get_filesystem(url)
```

### 2.4 Add CloudTransfer for cloud-to-cloud

**New capability:**
```python
from .store_components.cloud_transfer import CloudTransfer

def transfer_to_cloud(self, granules, target_url, ...):
    transfer = CloudTransfer(self.auth, self.credential_manager)
    return transfer.transfer(granules, target_url, ...)
```

## Phase 3: Ruff/Type Fixes Priority Order

### Priority 1: Type Imports (TYPE_CHECKING)

**Files:**
- `cloud_transfer.py` - Add TYPE_CHECKING imports for CredentialManager, DataGranule, AuthContext
- `query.py` - Fix module import issues
- `asset.py` - Fix None handling for optional params

### Priority 2: Type Mismatches in Auth/Store

**Files with `Auth | Store` issues:**
- `store.py` - Type hints accepting Auth | Store
- `results.py` - DataGranule/Collection query methods
- `test_flexible_inputs.py` - Query test mocks

### Priority 3: Method Signature Issues

**Files:**
- `query.py` - bbox, point, temporal parameter validation
- `geometry.py` - shapely type handling
- `asset.py` - Optional parameter handling

### Priority 4: Unused Variables/Imports

**Files:**
- `test_store_credentials.py` - Fixed ✅
- `test_stac_search.py` - Fixed ✅
- Any other unused variables found

## Phase 4: Incremental Refactoring Steps

### Step 1: Add CredentialManager to Store
```python
# store.py
def __init__(self, auth=None, threads=8, ...):
    auth = auth or earthaccess.__auth__
    self.auth = auth
    self.credential_manager = CredentialManager(auth)  # NEW
    self.threads = threads
```

### Step 2: Refactor _get_credentials() method
```python
def _get_credentials(self, provider: str):
    # OLD: return S3Credentials(...)
    # NEW: return self.credential_manager.get_credentials(provider)
```

### Step 3: Refactor _get_filesystem() method
```python
def _get_filesystem(self, url: str):
    # OLD: return s3fs.S3FileSystem(...)
    # NEW:
    factory = FileSystemFactory(self.credential_manager)
    return factory.get_filesystem(url)
```

### Step 4: Add CloudTransfer support
```python
def transfer(self, granules, target_url, **kwargs):
    transfer = CloudTransfer(self.auth, self.credential_manager)
    return transfer.transfer(granules, target_url, **kwargs)
```

### Step 5: Run tests, fix issues incrementally
- `python -m pytest tests/unit/test_store.py -x`
- Fix each issue
- Re-run until all pass

## Phase 5: Documentation Updates

### Update API docs
- Document Store changes in `docs/user-reference/store/`
- Update examples using new patterns
- Document CredentialManager usage

### Update migration guide
- Note that internal Store implementation changed
- Public API remains compatible
- New `Store.transfer()` method for cloud-to-cloud

## Testing Strategy

### Test Coverage
1. **Unit tests**: All existing Store tests must pass
2. **Integration tests**: Test with real NASA data
3. **Backward compatibility**: Verify existing code still works

### Test Commands
```bash
# Run Store tests
python -m pytest tests/unit/ -k "store" -v

# Run authentication tests
python -m pytest tests/unit/test_auth.py -v

# Run full unit suite
python -m pytest tests/unit/ -v
```

### Pre-commit Check
```bash
# Run all pre-commit checks
pre-commit run --all-files

# Run ruff specifically
ruff check earthaccess/ tests/ --fix
```

## Success Criteria

1. ✅ All ruff errors fixed (0 remaining)
2. ✅ All mypy errors fixed (0 remaining)
3. ✅ All unit tests passing (256+)
4. ✅ Pre-commit checks pass
5. ✅ CredentialManager used in Store
6. ✅ FileSystemFactory used in Store
7. ✅ CloudTransfer integrated
8. ✅ No breaking API changes

## Estimated Timeline

- **Phase 1**: 1 hour (analysis)
- **Phase 2**: 2 hours (planning)
- **Phase 3**: 4 hours (ruff/type fixes)
- **Phase 4**: 6 hours (incremental refactoring)
- **Phase 5**: 2 hours (documentation)
- **Total**: ~15 hours of focused work

## Risk Mitigation

### Backward Compatibility
- Keep public Store API unchanged
- Add new methods, don't modify existing signatures
- Add deprecation warnings before removing old patterns

### Testing
- Run full test suite after each step
- Use feature branches for each phase
- Keep tests green during refactoring

## Notes

### Files NOT to Modify
- `store_components/` - New components are stable
- New test files created - Already passing
- `CHANGELOG.md` - Already updated

### Files TO Modify
- `earthaccess/store.py` - Main refactoring target
- `earthaccess/results.py` - Type fixes
- `earthaccess/store_components/cloud_transfer.py` - Add TYPE_CHECKING
- `earthaccess/store_components/query.py` - Fix imports
- `earthaccess/store_components/geometry.py` - Type fixes
- `earthaccess/store_components/asset.py` - Optional param fixes
- Test files for legacy code - Type fixes
