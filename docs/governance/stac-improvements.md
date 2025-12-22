# STAC Improvements Implementation Plan

- Status: **In Progress**
- Date: 2024-12-22
- Last Updated: 2024-12-22
- Related: [STAC-PLAN.md](./STAC-PLAN.md)

## Implementation Status

### ✅ Completed (via `maxparallelism` branch merge)

- **Pluggable Executor System** (`earthaccess/parallel.py`)
  - `get_executor()` factory following `concurrent.futures.Executor` ABC
  - `SerialExecutor` for debugging
  - `ThreadPoolExecutorWrapper` with optional tqdm progress
  - `DaskDelayedExecutor` for Dask workflows
  - `LithopsEagerFunctionExecutor` for serverless execution
  - Helper functions: `execute_with_executor()`, `submit_all_and_wait()`

- **Target Filesystem Abstraction** (`earthaccess/target_filesystem.py`)
  - `TargetFilesystem` ABC with `LocalFilesystem` and `FsspecFilesystem` implementations
  - `TargetLocation` unified interface for local and cloud storage targets
  - Auto-detection of backend based on path (s3://, gs://, az://, local)

- **Store Integration** (`earthaccess/store.py`)
  - All `open()` and `download()` methods accept `parallel` parameter
  - Session strategy selection via `_use_session_cloning()`
  - Executor type tracking via `_set_executor_type()`
  - pqdm dependency removed - uses `ThreadPoolExecutor` by default

### 🔲 Remaining Work

- Groups A-H (Query Architecture, Results Classes, Asset Access, STAC Conversion, etc.)
- `CredentialManager` class (credentials still inline in Store)
- `FileSystemFactory` class (FS creation still inline in Store)
- URL-to-provider inference
- Cloud-to-cloud streaming (`download()` to GCS/Azure)
- Streaming execution pattern for lazy results
- obstore backend (optional)

### 🔲 Documentation Improvements Needed

- **User documentation for parallel execution**: Document the `parallel` parameter on `open()` and `download()` methods
- **Executor selection guide**: When to use ThreadPool vs Dask vs Lithops
- **TargetLocation documentation**: How to download to cloud storage (S3, GCS, Azure)
- **Real-world examples page**: Add comprehensive tutorial page with realistic use cases (see Group I below)
- **Migration guide**: Document changes from previous versions

### 🔲 VirtualiZarr Refactoring Needed

- **Align `open_virtual_dataset` with `open_virtual_mfdataset`**: Single granule opener should support all parameters from mfdataset:
  - `load` parameter (materialize coordinates or not)
  - `reference_dir` and `reference_format` parameters
  - Consistent behavior for coordinate handling
- **Update virtualizarr dependency**: Currently pinned to `>=2.1.2`, latest is `2.2.1`
- **Dependency consolidation**: Consider moving `virtualizarr` and related packages to core dependencies to avoid user confusion

### 🔲 Dependency Review

Current optional dependencies that may cause user confusion:
- `virtualizarr` - users expect `open_virtual_dataset` to work out of the box
- `kerchunk` - needed for reference file generation
- `xarray` - used extensively in examples but optional

Recommendation: Evaluate adding these as core dependencies or improve error messages.

---

## Overview

This document outlines a **robust, grouped-by-functionality** implementation plan for STAC interoperability improvements in earthaccess. The design prioritizes:

1. **Non-breaking changes** - Existing API remains unchanged
2. **Backend abstraction** - Support for CMR and external STAC backends
3. **Method chaining AND named parameters** - Flexible query construction
4. **pystac-client compatibility** - `query.to_stac()` produces params compatible with `ItemSearch`/`CollectionSearch`
5. **Validation** - Query parameters validated against CMR/STAC API specs

---

## Table of Contents

0. [Group 0: Store Refactoring (Pre-requisite)](#group-0-store-refactoring-pre-requisite) ✅ **~70% Complete**
   - [0.1 Current Issues](#01-current-issues) ✅ Addressed
   - [0.2 Implemented Architecture](#02-implemented-architecture) ✅ Done
   - [0.3 Benefits Achieved](#03-benefits-achieved) ✅ Done
   - [0.4 Handling URLs Without Granules](#04-handling-urls-without-granules) 🔲 Pending
   - [0.5 Implementation Tasks](#05-implementation-tasks) ✅ Mostly Done
   - [0.6 Migration Path](#06-migration-path) ✅ Done
   - [0.7 Cloud-to-Cloud Streaming](#07-cloud-to-cloud-streaming) 🔲 Pending
   - [0.8 fsspec High-Level API Usage](#08-fsspec-high-level-api-usage) ✅ Done
   - [0.9 obstore as Alternative Backend](#09-obstore-as-alternative-backend) 🔲 Optional
1. [Group A: Query Architecture](#group-a-query-architecture) 🔲 Pending
2. [Group B: Results Classes](#group-b-results-classes) 🔲 Pending
3. [Group C: Asset Access & Filtering](#group-c-asset-access--filtering) 🔲 Pending
4. [Group D: STAC Conversion](#group-d-stac-conversion) 🔲 Pending
5. [Group E: External STAC Catalogs](#group-e-external-stac-catalogs) 🔲 Pending
6. [Group F: Flexible Input Types](#group-f-flexible-input-types) 🔲 Pending
7. [Group G: Geometry Handling](#group-g-geometry-handling) 🔲 Pending
8. [Group H: Query Widget (Optional)](#group-h-query-widget-optional) 🔲 Optional
9. [Group I: Documentation & Examples](#group-i-documentation--examples) 🔲 Pending
10. [Group J: VirtualiZarr Improvements](#group-j-virtualizarr-improvements) 🔲 Pending
11. [Implementation Phases](#implementation-phases)
12. [Migration Guide](#migration-guide)

---

## Group 0: Store Refactoring (Pre-requisite)

> **Status**: ✅ ~70% Complete via `maxparallelism` branch
>
> The core executor infrastructure is implemented. Remaining work includes `CredentialManager`, `FileSystemFactory`, URL-to-provider inference, and cloud-to-cloud streaming.

### 0.1 Issues Addressed ✅

The following issues from the original `Store` class have been addressed:

#### 0.1.1 SOLID Principle Improvements

| Principle | Before | After |
|-----------|--------|-------|
| **Single Responsibility** | `Store` handled everything | Executor logic extracted to `parallel.py`, target filesystem to `target_filesystem.py` |
| **Open/Closed** | Adding backends required modifying Store | New executors can be added without modifying existing code |
| **Dependency Inversion** | Hard dependency on `pqdm` | Generic `Executor` interface - any compliant executor works |

#### 0.1.2 Problems Solved

1. ✅ **pqdm coupling removed**: Now uses pluggable `get_executor()` factory
   ```python
   # Before: tightly coupled to pqdm
   from pqdm.threads import pqdm
   results = pqdm(data_links, _download, **pqdm_kwargs)

   # After: pluggable executor
   from earthaccess.parallel import get_executor
   executor = get_executor(parallel, max_workers=max_workers)
   results = list(executor.map(process_fn, items))
   ```

2. ✅ **Parallel execution unified**: All methods use `get_executor()` consistently

3. ✅ **Session strategy selection**: `_use_session_cloning()` determines when to clone sessions vs ship credentials

4. 🔲 **Credential complexity**: Still requires `provider` parameter for raw S3 URLs (URL-to-provider inference pending)

5. 🔲 **Mixed concerns**: `CredentialManager` and `FileSystemFactory` not yet extracted

### 0.2 Implemented Architecture ✅

The `maxparallelism` branch implemented a cleaner architecture than originally planned:

#### 0.2.1 Actual File Structure

```
earthaccess/
├── parallel.py            # ✅ Pluggable executor system (NEW)
├── target_filesystem.py   # ✅ Target filesystem abstraction (NEW)
├── store.py               # Modified to use new components
├── api.py                 # Modified to accept parallel parameter
└── ...
```

#### 0.2.2 Parallel Executor System ✅

The implementation follows `concurrent.futures.Executor` ABC, which is cleaner than the originally planned approach:

```python
# earthaccess/parallel.py (IMPLEMENTED)

from concurrent.futures import Executor, Future, ThreadPoolExecutor

class SerialExecutor(Executor):
    """Sequential executor for debugging."""
    def submit(self, fn, /, *args, **kwargs) -> Future: ...
    def map(self, fn, *iterables, timeout=None, chunksize=1) -> Iterator: ...

class ThreadPoolExecutorWrapper(Executor):
    """Thread pool with optional tqdm progress bars."""
    def __init__(self, max_workers=None, show_progress=True): ...
    def map_with_progress(self, fn, *iterables, desc="Processing"): ...

class DaskDelayedExecutor(Executor):
    """Dask delayed computation executor."""
    ...

class LithopsEagerFunctionExecutor(Executor):
    """Lithops serverless executor."""
    ...

def get_executor(
    parallel: Union[str, Executor, bool, None] = True,
    max_workers: Union[int, None] = None,
    show_progress: bool = True,
) -> Executor:
    """Factory function for getting executors.

    Args:
        parallel:
            - True or "threads": ThreadPoolExecutor (default)
            - False or "serial": SerialExecutor
            - "dask": DaskDelayedExecutor
            - "lithops": LithopsEagerFunctionExecutor
            - Executor instance: use directly
    """
```

**Key Design Decisions:**
- Uses `concurrent.futures.Executor` ABC (not a custom protocol)
- `ThreadPoolExecutor` is the default (not pqdm)
- Progress bars via tqdm are optional
- Any custom `Executor` subclass can be passed directly

#### 0.2.3 Target Filesystem Abstraction ✅

```python
# earthaccess/target_filesystem.py (IMPLEMENTED)

class TargetFilesystem(ABC):
    """Abstract base for target filesystems."""
    def open(self, path: str, mode: str = "rb") -> IO: ...
    def exists(self, path: str) -> bool: ...
    def mkdir(self, path: str, exist_ok: bool = True): ...
    def join(self, *paths: str) -> str: ...
    def basename(self, path: str) -> str: ...

class LocalFilesystem(TargetFilesystem):
    """Local POSIX filesystem."""
    def __init__(self, base_path: str): ...

class FsspecFilesystem(TargetFilesystem):
    """Fsspec-based filesystem for cloud storage (S3, GCS, Azure)."""
    def __init__(self, base_path: str, storage_options: Dict = None): ...

class TargetLocation:
    """Unified target location that auto-detects backend."""
    def __init__(self, path: Union[str, Path], backend: str = "auto"): ...
    def get_filesystem(self) -> TargetFilesystem: ...
    def is_local(self) -> bool: ...
    def is_cloud(self) -> bool: ...
```

**Key Design Decisions:**
- `TargetLocation` auto-detects backend from path prefix (s3://, gs://, etc.)
- Uses fsspec for all cloud storage (not boto3/SDK directly)
- Clean separation between local and cloud filesystem implementations

#### 0.2.4 Session Strategy Selection ✅

The branch implements smart session handling based on executor type:

```python
# In earthaccess/store.py (IMPLEMENTED)

def _set_executor_type(self, parallel: Union[str, bool, None]) -> None:
    """Track current executor type for session strategy selection."""
    if parallel is None or parallel is True:
        self._current_executor_type = "threads"
    elif parallel == "dask":
        self._current_executor_type = "dask"
    elif parallel == "lithops":
        self._current_executor_type = "lithops"
    # ...

def _use_session_cloning(self) -> bool:
    """Determine if session cloning is appropriate for current executor."""
    # Only clone sessions for in-process executors
    return self._current_executor_type in ["threads", "threadpool", "serial"]
```

**Key insight:** For distributed executors (Dask distributed, Lithops, Ray), session cloning doesn't work. Instead, credentials must be shipped to workers.

### 0.2.5 Remaining Components 🔲

The following planned components are **not yet implemented**:

| Component | Status | Notes |
|-----------|--------|-------|
| `CredentialManager` class | 🔲 Pending | Credentials still inline in `Store._s3_credentials` |
| `FileSystemFactory` class | 🔲 Pending | FS creation still inline in `Store.get_s3fs_session()` |
| URL-to-provider inference | 🔲 Pending | Still requires `provider` parameter for raw S3 URLs |
| Cloud-to-cloud streaming | 🔲 Pending | `TargetLocation` provides foundation, but `download()` not yet updated |

### 0.3 Benefits Achieved ✅

| Aspect | Before | After |
|--------|--------|-------|
| **Parallelization** | Hard-coded pqdm | Pluggable executors (threads, dask, lithops, custom) |
| **Testing** | Difficult to mock pqdm | Easy to use `SerialExecutor` |
| **Progress bars** | Always pqdm-style | Optional tqdm via `show_progress` param |
| **Distributed** | Not supported | Dask and Lithops executors built-in |
| **Target filesystems** | Local only | Local, S3, GCS, Azure via `TargetLocation` |
| **Session handling** | Always clone | Smart selection via `_use_session_cloning()` |
| **API** | Changed methods | Added `parallel` param to all methods |

### 0.3.1 Usage Examples

```python
# Default: ThreadPoolExecutor with tqdm progress
earthaccess.download(granules, local_path)

# Serial execution for debugging
earthaccess.download(granules, local_path, parallel=False)

# Dask for distributed workflows
earthaccess.download(granules, local_path, parallel="dask")

# Lithops for serverless
earthaccess.download(granules, local_path, parallel="lithops")

# Custom max workers
earthaccess.download(granules, local_path, max_workers=16)

# Custom executor
from concurrent.futures import ProcessPoolExecutor
earthaccess.download(granules, local_path, parallel=ProcessPoolExecutor(4))
```

### 0.4 Handling URLs Without Granules

The core problem: when users pass S3 URLs directly, we don't know which provider's credentials to use.

#### Proposed Solutions

**Option 1: Require provider parameter** (Current behavior, improved error message)
```python
# Clear error when provider missing
earthaccess.open(["s3://bucket/key"])
# ValueError: S3 URL requires provider or credentials_endpoint.
# Available providers: earthaccess.list_cloud_providers()
# Example: earthaccess.open(urls, provider="POCLOUD")
```

**Option 2: URL-to-provider mapping** (New feature)
```python
# earthaccess/store/provider_mapping.py

# Known S3 bucket prefixes to provider mapping
BUCKET_PROVIDER_MAP = {
    "podaac-": "POCLOUD",
    "nsidc-cumulus-": "NSIDC_CPRD",
    "lp-prod-": "LPCLOUD",
    "gesdisc-cumulus-": "GES_DISC",
    "ghrc-cumulus-": "GHRC_DAAC",
    "ornldaac-cumulus-": "ORNL_CLOUD",
    "asf-cumulus-": "ASF",
}

def infer_provider_from_url(url: str) -> Optional[str]:
    """Attempt to infer provider from S3 URL bucket name."""
    if not url.startswith("s3://"):
        return None

    bucket = url.split("/")[2]
    for prefix, provider in BUCKET_PROVIDER_MAP.items():
        if bucket.startswith(prefix):
            return provider
    return None
```

**Option 3: Provider from previous context** (Convenience)
```python
# If all URLs are from the same search, reuse the provider
granules = earthaccess.search_data(short_name="ATL03")
urls = [g.data_links()[0] for g in granules]

# Store remembers the last-used provider
earthaccess.open(urls)  # Uses NSIDC_CPRD from the search context
```

### 0.5 Distributed Authentication Strategy

The Store refactoring enables the **credential shipping pattern** for distributed execution:

1. **Centralized fetching**: `CredentialManager` fetches credentials once per provider
2. **Serializable format**: Credentials returned as simple dict (JSON-serializable)
3. **Worker reconstruction**: Remote workers recreate filesystems from credential dict
4. **No session cloning**: S3 tokens are self-contained, avoiding session complexity
5. **Server protection**: Prevents thundering herd on NASA's authentication servers

This approach works across all distributed backends (Ray, Lithops, Dask, HPC) and eliminates the need for complex session management in distributed contexts.

### 0.5 Implementation Tasks

> **Status**: Phase 1 and parts of Phases 3, 5, 7 are complete via the `maxparallelism` branch.

#### Phase 1: Core Abstractions ✅ COMPLETE

| Task | Priority | Test File | Status |
|------|----------|-----------|--------|
| ~~Create `earthaccess/store/` package structure~~ | High | - | ✅ Not needed - files in `earthaccess/` |
| Define `Executor` protocol (use `concurrent.futures.Executor`) | High | `test_parallel.py` | ✅ **Done** |
| Implement `SerialExecutor` | High | `test_parallel.py` | ✅ **Done** |
| ~~Implement `PqdmExecutor` (default)~~ | ~~High~~ | - | ❌ Removed - using `ThreadPoolExecutorWrapper` |
| Implement `ThreadPoolExecutorWrapper` | High | `test_parallel.py` | ✅ **Done** |
| Implement `DaskDelayedExecutor` | High | `test_parallel.py` | ✅ **Done** |
| Implement `LithopsEagerFunctionExecutor` | High | `test_parallel.py` | ✅ **Done** |
| Implement `get_executor()` factory | High | `test_parallel.py` | ✅ **Done** |
| Helper functions `execute_with_executor()`, `submit_all_and_wait()` | Medium | `test_parallel.py` | ✅ **Done** |

#### Phase 2: Credential Management 🔲 PENDING

| Task | Priority | Test File | Status |
|------|----------|-----------|--------|
| Implement `S3Credentials` dataclass | High | `test_credentials.py` | 🔲 Pending |
| Implement `CredentialManager` with caching | High | `test_credentials.py` | 🔲 Pending |
| Add expiration/refresh logic | High | `test_credentials.py` | 🔲 Pending |
| Add URL-to-provider inference | Medium | `test_credentials.py` | 🔲 Pending |

#### Phase 3: Target Filesystem ✅ COMPLETE (renamed from FileSystem Factory)

| Task | Priority | Test File | Status |
|------|----------|-----------|--------|
| Implement `TargetFilesystem` ABC | High | `test_target_filesystem.py` | ✅ **Done** |
| Implement `LocalFilesystem` | High | `test_target_filesystem.py` | ✅ **Done** |
| Implement `FsspecFilesystem` | High | `test_target_filesystem.py` | ✅ **Done** |
| Implement `TargetLocation` with auto-detection | High | `test_target_filesystem.py` | ✅ **Done** |
| Add S3 support (`s3://`) | High | `test_target_filesystem.py` | ✅ **Done** |
| Add GCS support (`gs://`) | Medium | `test_target_filesystem.py` | ✅ **Done** |
| Add Azure Blob support (`az://`) | Medium | `test_target_filesystem.py` | ✅ **Done** |

#### Phase 4: Streaming Execution 🔲 PENDING

| Task | Priority | Test File | Status |
|------|----------|-----------|--------|
| Implement `stream_parallel()` function | High | `test_streaming.py` | 🔲 Pending |
| Implement `StreamingExecutor` class | High | `test_streaming.py` | 🔲 Pending |
| Implement `DaskStreamingExecutor` | Medium | `test_streaming.py` | 🔲 Pending |
| Add `open()` method to `GranuleResults` | High | `test_results.py` | 🔲 Pending |
| Add `download()` method to `GranuleResults` | High | `test_results.py` | 🔲 Pending |

#### Phase 5: Distributed Backends ✅ PARTIAL

| Task | Priority | Test File | Status |
|------|----------|-----------|--------|
| Implement `DaskDelayedExecutor` | High | `test_parallel.py` | ✅ **Done** |
| Implement `LithopsEagerFunctionExecutor` | High | `test_parallel.py` | ✅ **Done** |
| Add `_use_session_cloning()` strategy | High | `test_executor_strategy.py` | ✅ **Done** |
| Add `_set_executor_type()` tracking | High | `test_executor_strategy.py` | ✅ **Done** |
| Implement `RayExecutor` | Medium | `test_parallel.py` | 🔲 Pending |
| Add `DataGranule.to_dict()` method | High | `test_results.py` | 🔲 Pending |
| Add clear error for `open()` + distributed | High | `test_api.py` | 🔲 Pending |

#### Phase 6: Cloud-to-Cloud Transfer 🔲 PENDING

| Task | Priority | Test File | Status |
|------|----------|-----------|--------|
| Implement `CloudTransfer` class | High | `test_transfer.py` | 🔲 Pending |
| Implement `stream_file()` with fsspec | High | `test_transfer.py` | 🔲 Pending |
| Implement `transfer_batch()` | High | `test_transfer.py` | 🔲 Pending |
| Update `Store.download()` for cloud targets | High | `test_store.py` | 🔲 Pending |

#### Phase 7: Store Integration ✅ COMPLETE

| Task | Priority | Test File | Status |
|------|----------|-----------|--------|
| Add `parallel` parameter to all `open()`/`download()` methods | High | `test_store.py` | ✅ **Done** |
| Integrate `get_executor()` in Store | High | `test_store.py` | ✅ **Done** |
| Maintain 100% backwards compatibility | High | `test_store.py` | ✅ **Done** |
| Remove pqdm dependency | High | `pyproject.toml` | ✅ **Done** |
| Add `streaming` parameter to `open()` | High | `test_api.py` | 🔲 Pending |
| Add `auto_batch` parameter to `open()` | High | `test_api.py` | 🔲 Pending |
| Implement `open_mfdataset_batched()` | High | `test_xarray.py` | 🔲 Pending |

#### Phase 8: Integration & Documentation 🔲 PENDING

| Task | Priority | Test File | Status |
|------|----------|-----------|--------|
| Integration tests with distributed session | High | `test_distributed_session.py` | ✅ **Done** |
| Integration tests with Dask | High | `test_distributed.py` | ✅ **Done** |
| Integration tests for cloud-to-cloud | Medium | `test_transfer_integration.py` | 🔲 Pending |
| Add xarray integration examples | High | - | 🔲 Pending |
| Document distributed processing patterns | High | - | 🔲 Pending |
| Update API documentation | Medium | - | 🔲 Pending |

#### Optional Enhancements (Future)

| Task | Priority | Test File | Status |
|------|----------|-----------|--------|
| Implement `ObstoreTransfer` class | Low | `test_obstore.py` | 🔲 Optional |
| Add async transfer support | Low | `test_obstore.py` | 🔲 Optional |
| Implement `open_mfdataset_distributed()` | Medium | `test_xarray.py` | 🔲 Optional |

### 0.6 Migration Path ✅ VERIFIED

The refactoring maintains 100% backwards compatibility:

```python
# All existing code continues to work unchanged:
store = earthaccess.get_store()
files = store.open(granules)
store.get(granules, local_path)

# New optional features:
store = earthaccess.get_store(parallel_backend="dask")
files = store.open(granules, parallel_backend="lithops")
```

### 0.7 Cloud-to-Cloud Streaming

Enable streaming data directly between cloud providers without local intermediary storage.

#### 0.7.1 Use Case

Users running in GCP or Azure need to transfer NASA data from AWS S3 to their cloud storage:

```python
# Current: Requires downloading to local disk first
earthaccess.download(granules, local_path="/tmp")
# Then manually upload to GCS...

# New: Direct cloud-to-cloud streaming
earthaccess.download(
    granules,
    target="gs://my-bucket/nasa-data/",  # GCS target
)

# Or with explicit source URLs
earthaccess.download(
    source=["s3://podaac-bucket/file1.nc", "s3://podaac-bucket/file2.nc"],
    target="gs://my-bucket/output/",
    provider="POCLOUD",  # For S3 credentials
)

# Azure Blob Storage
earthaccess.download(
    granules,
    target="az://container/path/",
)
```

#### 0.7.2 Design Principles

1. **Use fsspec for all I/O** - No boto3, no cloud-specific SDKs
2. **Streaming, not buffering** - Stream chunks directly between filesystems
3. **Parallel transfers** - Use the pluggable executor system
4. **Configurable chunk size** - For memory/performance tuning
5. **Progress reporting** - Unified progress across all transfers

#### 0.7.3 Implementation

```python
# earthaccess/store/transfer.py

"""
Cloud-to-cloud transfer using fsspec.

Key insight: fsspec provides a unified interface across all cloud providers.
We use fsspec.open() for reading and writing, streaming chunks between them.
"""

from pathlib import Path
from typing import Any, BinaryIO, Callable, Dict, Iterator, List, Optional, Union
from urllib.parse import urlparse
import logging

import fsspec

from .credentials import CredentialManager
from .parallel import get_executor, ParallelBackend

logger = logging.getLogger(__name__)

# Default chunk size for streaming (8 MB)
DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024


class CloudTransfer:
    """Handles cloud-to-cloud and cloud-to-local transfers using fsspec."""

    def __init__(
        self,
        credential_manager: CredentialManager,
        default_chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        self.credentials = credential_manager
        self.chunk_size = default_chunk_size
        self._fs_cache: Dict[str, fsspec.AbstractFileSystem] = {}

    def get_filesystem(
        self,
        url: str,
        *,
        provider: Optional[str] = None,
        storage_options: Optional[Dict[str, Any]] = None,
    ) -> fsspec.AbstractFileSystem:
        """Get fsspec filesystem for a URL.

        Args:
            url: URL or path (s3://, gs://, az://, /local/path)
            provider: NASA provider for S3 credentials
            storage_options: Additional fsspec options

        Returns:
            Configured fsspec filesystem
        """
        parsed = urlparse(url)
        protocol = parsed.scheme or "file"

        # Build cache key
        cache_key = (protocol, provider, frozenset((storage_options or {}).items()))

        if cache_key in self._fs_cache:
            return self._fs_cache[cache_key]

        options = dict(storage_options or {})

        if protocol == "s3":
            # NASA S3 - get credentials from EDL
            if provider:
                creds = self.credentials.get_credentials(provider=provider)
                options.update(creds.to_dict())
            # If no provider, assume credentials in storage_options or environment

        elif protocol == "gs":
            # Google Cloud Storage - uses default credentials or storage_options
            pass

        elif protocol in ("az", "abfs"):
            # Azure Blob Storage - uses storage_options or environment
            pass

        elif protocol in ("file", ""):
            # Local filesystem
            pass

        fs = fsspec.filesystem(protocol, **options)
        self._fs_cache[cache_key] = fs
        return fs

    def stream_file(
        self,
        source_url: str,
        target_url: str,
        *,
        source_fs: Optional[fsspec.AbstractFileSystem] = None,
        target_fs: Optional[fsspec.AbstractFileSystem] = None,
        chunk_size: Optional[int] = None,
        progress_callback: Optional[Callable[[int], None]] = None,
    ) -> str:
        """Stream a single file from source to target.

        Uses fsspec's high-level open() for both read and write,
        streaming chunks to minimize memory usage.

        Args:
            source_url: Source URL (s3://, gs://, https://, local path)
            target_url: Target URL or path
            source_fs: Pre-configured source filesystem (optional)
            target_fs: Pre-configured target filesystem (optional)
            chunk_size: Bytes per chunk (default: 8MB)
            progress_callback: Called with bytes transferred

        Returns:
            Target path/URL where file was written
        """
        chunk_size = chunk_size or self.chunk_size

        # Get filesystems if not provided
        if source_fs is None:
            source_fs = self.get_filesystem(source_url)
        if target_fs is None:
            target_fs = self.get_filesystem(target_url)

        # Extract paths from URLs
        source_path = self._url_to_path(source_url)
        target_path = self._url_to_path(target_url)

        # If target is a directory, append source filename
        if target_fs.isdir(target_path) or target_path.endswith("/"):
            source_name = source_path.rsplit("/", 1)[-1]
            target_path = f"{target_path.rstrip('/')}/{source_name}"

        # Ensure target directory exists
        target_dir = target_path.rsplit("/", 1)[0]
        target_fs.makedirs(target_dir, exist_ok=True)

        # Stream chunks using fsspec.open()
        bytes_transferred = 0
        with source_fs.open(source_path, "rb") as src:
            with target_fs.open(target_path, "wb") as dst:
                while True:
                    chunk = src.read(chunk_size)
                    if not chunk:
                        break
                    dst.write(chunk)
                    bytes_transferred += len(chunk)
                    if progress_callback:
                        progress_callback(len(chunk))

        logger.debug(f"Transferred {bytes_transferred} bytes: {source_url} -> {target_url}")
        return target_path

    def transfer_batch(
        self,
        sources: List[str],
        target: str,
        *,
        provider: Optional[str] = None,
        source_options: Optional[Dict[str, Any]] = None,
        target_options: Optional[Dict[str, Any]] = None,
        parallel: ParallelBackend = "pqdm",
        max_workers: int = 8,
        chunk_size: Optional[int] = None,
        show_progress: bool = True,
    ) -> List[str]:
        """Transfer multiple files from source(s) to target.

        Args:
            sources: List of source URLs
            target: Target directory URL (s3://, gs://, az://, local)
            provider: NASA provider for S3 source credentials
            source_options: fsspec options for source filesystem
            target_options: fsspec options for target filesystem
            parallel: Parallelization backend
            max_workers: Number of parallel transfers
            chunk_size: Bytes per chunk
            show_progress: Show transfer progress

        Returns:
            List of target paths where files were written

        Examples:
            # S3 to local
            transfer.transfer_batch(
                ["s3://podaac/file1.nc", "s3://podaac/file2.nc"],
                "/local/data/",
                provider="POCLOUD",
            )

            # S3 to GCS (cloud-to-cloud)
            transfer.transfer_batch(
                ["s3://podaac/file1.nc"],
                "gs://my-bucket/data/",
                provider="POCLOUD",
            )

            # S3 to Azure
            transfer.transfer_batch(
                ["s3://podaac/file1.nc"],
                "az://container/data/",
                provider="POCLOUD",
                target_options={"account_name": "myaccount"},
            )
        """
        if not sources:
            return []

        # Pre-configure filesystems for efficiency (avoid repeated auth)
        source_fs = self.get_filesystem(
            sources[0],
            provider=provider,
            storage_options=source_options,
        )
        target_fs = self.get_filesystem(
            target,
            storage_options=target_options,
        )

        chunk_size = chunk_size or self.chunk_size

        def transfer_one(source_url: str) -> str:
            return self.stream_file(
                source_url,
                target,
                source_fs=source_fs,
                target_fs=target_fs,
                chunk_size=chunk_size,
            )

        executor = get_executor(parallel, n_jobs=max_workers, show_progress=show_progress)
        results = list(executor.map(transfer_one, sources))

        return results

    @staticmethod
    def _url_to_path(url: str) -> str:
        """Extract path from URL, handling various schemes."""
        parsed = urlparse(url)
        if parsed.scheme in ("s3", "gs", "az", "abfs"):
            # Cloud URLs: bucket/key
            return f"{parsed.netloc}{parsed.path}"
        elif parsed.scheme in ("http", "https"):
            return url  # Keep full URL for HTTP
        else:
            # Local path
            return parsed.path or url


def download_to_cloud(
    granules: List["DataGranule"],
    target: str,
    *,
    target_options: Optional[Dict[str, Any]] = None,
    parallel: ParallelBackend = "pqdm",
    max_workers: int = 8,
    **kwargs,
) -> List[str]:
    """Download granules to a cloud storage destination.

    Convenience function for cloud-to-cloud transfers.

    Args:
        granules: List of DataGranule objects
        target: Target URL (gs://, az://, s3://, or local path)
        target_options: fsspec options for target storage
        parallel: Parallelization backend
        max_workers: Parallel transfer count

    Returns:
        List of target paths
    """
    import earthaccess

    auth = earthaccess.get_auth()
    credential_manager = CredentialManager(auth)
    transfer = CloudTransfer(credential_manager)

    # Get source URLs from granules
    source_urls = []
    provider = None
    for g in granules:
        urls = g.data_links()
        if urls:
            source_urls.append(urls[0])
            if provider is None:
                provider = g["meta"]["provider-id"]

    return transfer.transfer_batch(
        source_urls,
        target,
        provider=provider,
        target_options=target_options,
        parallel=parallel,
        max_workers=max_workers,
    )
```

#### 0.7.4 Updated Store.download() Signature

```python
# In earthaccess/store/__init__.py

def download(
    self,
    granules: Union[List[str], List[DataGranule]],
    target: Optional[Union[str, Path]] = None,
    *,
    provider: Optional[str] = None,
    target_options: Optional[Dict[str, Any]] = None,
    parallel: ParallelBackend = "pqdm",
    max_workers: int = 8,
    show_progress: bool = True,
    chunk_size: Optional[int] = None,
) -> List[str]:
    """Download granules to local or cloud storage.

    Supports downloading to:
    - Local filesystem (default, current behavior)
    - AWS S3 (s3://bucket/prefix/)
    - Google Cloud Storage (gs://bucket/prefix/)
    - Azure Blob Storage (az://container/prefix/)

    Args:
        granules: DataGranule objects or URLs to download
        target: Target path or URL. Defaults to current directory.
            - Local: "/path/to/dir" or Path object
            - S3: "s3://bucket/prefix/"
            - GCS: "gs://bucket/prefix/"
            - Azure: "az://container/prefix/"
        provider: Provider for source S3 credentials (required for S3 URLs)
        target_options: fsspec options for target filesystem
            - GCS: {"project": "my-project"}
            - Azure: {"account_name": "myaccount", "account_key": "..."}
            - S3: {"key": "...", "secret": "..."} for non-NASA S3
        parallel: Parallelization backend
        max_workers: Number of parallel downloads
        show_progress: Show progress bar
        chunk_size: Streaming chunk size in bytes (default: 8MB)

    Returns:
        List of paths/URLs where files were written

    Examples:
        # Current behavior (local download)
        paths = store.download(granules, "/local/data")

        # Download to Google Cloud Storage
        paths = store.download(
            granules,
            "gs://my-bucket/nasa-data/",
        )

        # Download to Azure with credentials
        paths = store.download(
            granules,
            "az://container/data/",
            target_options={"account_name": "x", "account_key": "y"},
        )

        # Cloud-to-cloud with Dask parallelization
        paths = store.download(
            granules,
            "gs://my-bucket/data/",
            parallel="dask",
            max_workers=32,
        )
    """
    target = target or Path.cwd()

    # Detect if target is cloud storage
    target_str = str(target)
    is_cloud_target = any(
        target_str.startswith(prefix)
        for prefix in ("s3://", "gs://", "az://", "abfs://")
    )

    if is_cloud_target:
        # Use cloud transfer
        return self._cloud_transfer.transfer_batch(
            sources=self._extract_urls(granules),
            target=target_str,
            provider=provider or self._infer_provider(granules),
            target_options=target_options,
            parallel=parallel,
            max_workers=max_workers,
            chunk_size=chunk_size,
            show_progress=show_progress,
        )
    else:
        # Local download (existing behavior)
        return self._download_local(
            granules,
            Path(target),
            provider=provider,
            parallel=parallel,
            max_workers=max_workers,
            show_progress=show_progress,
        )
```

#### 0.7.5 fsspec Protocol Support

| Protocol | URL Pattern | Notes |
|----------|-------------|-------|
| Local | `/path/to/file` | Default |
| AWS S3 | `s3://bucket/key` | NASA data sources, also target |
| Google Cloud | `gs://bucket/key` | Requires `gcsfs` |
| Azure Blob | `az://container/path` | Requires `adlfs` |
| Azure Data Lake | `abfs://container/path` | Requires `adlfs` |
| HTTP/HTTPS | `https://server/path` | Read-only source |

### 0.8 fsspec High-Level API Usage

#### 0.8.1 Design Philosophy

**Use fsspec's high-level APIs exclusively** - avoid boto3, google-cloud-storage, azure-storage-blob SDKs directly. This provides:

1. **Unified interface** across all storage backends
2. **Automatic protocol detection** from URL schemes
3. **Built-in caching** and connection pooling
4. **Lazy loading** of backend-specific dependencies

#### 0.8.2 Current vs. Proposed Approach

```python
# AVOID: Direct boto3 usage
import boto3
s3 = boto3.client("s3", **credentials)
s3.download_file(bucket, key, local_path)

# PREFER: fsspec high-level API
import fsspec
fs = fsspec.filesystem("s3", **credentials)
fs.get(f"s3://{bucket}/{key}", local_path)

# BEST: Use fsspec.open() for streaming
with fsspec.open(f"s3://{bucket}/{key}", "rb", **credentials) as f:
    data = f.read()
```

#### 0.8.3 fsspec Pattern Reference

```python
# earthaccess/store/fsspec_patterns.py

"""
Reference patterns for fsspec usage in earthaccess.

All cloud I/O should use these patterns instead of direct SDK calls.
"""

import fsspec
from typing import Any, Dict, List, Optional, Iterator
from contextlib import contextmanager


# =============================================================================
# FILESYSTEM CREATION
# =============================================================================

def get_authenticated_fs(
    protocol: str,
    credentials: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> fsspec.AbstractFileSystem:
    """Create authenticated filesystem using fsspec.

    Args:
        protocol: "s3", "gs", "az", "https", "file"
        credentials: Protocol-specific credentials
        **kwargs: Additional fsspec options

    Returns:
        Configured filesystem instance
    """
    options = dict(credentials or {})
    options.update(kwargs)
    return fsspec.filesystem(protocol, **options)


# =============================================================================
# FILE OPERATIONS
# =============================================================================

def read_file(url: str, **storage_options) -> bytes:
    """Read entire file contents.

    Prefer streaming with open_file() for large files.
    """
    with fsspec.open(url, "rb", **storage_options) as f:
        return f.read()


@contextmanager
def open_file(url: str, mode: str = "rb", **storage_options):
    """Open file as context manager for streaming.

    This is the preferred method for large files.

    Example:
        with open_file("s3://bucket/key", key=..., secret=...) as f:
            for chunk in iter(lambda: f.read(8192), b""):
                process(chunk)
    """
    with fsspec.open(url, mode, **storage_options) as f:
        yield f


def copy_file(
    source: str,
    target: str,
    source_options: Optional[Dict[str, Any]] = None,
    target_options: Optional[Dict[str, Any]] = None,
    chunk_size: int = 8 * 1024 * 1024,
) -> None:
    """Copy file between any two storage locations.

    Streams chunks to minimize memory usage.
    Works across different protocols (S3 -> GCS, etc.)
    """
    with fsspec.open(source, "rb", **(source_options or {})) as src:
        with fsspec.open(target, "wb", **(target_options or {})) as dst:
            while True:
                chunk = src.read(chunk_size)
                if not chunk:
                    break
                dst.write(chunk)


def list_files(path: str, **storage_options) -> List[str]:
    """List files at path (works for any protocol)."""
    fs = fsspec.filesystem(path.split("://")[0] if "://" in path else "file", **storage_options)
    return fs.ls(path)


def file_exists(url: str, **storage_options) -> bool:
    """Check if file exists."""
    fs, path = fsspec.core.url_to_fs(url, **storage_options)
    return fs.exists(path)


def file_size(url: str, **storage_options) -> int:
    """Get file size in bytes."""
    fs, path = fsspec.core.url_to_fs(url, **storage_options)
    return fs.size(path)


# =============================================================================
# BATCH OPERATIONS
# =============================================================================

def download_files(
    urls: List[str],
    local_dir: str,
    **storage_options,
) -> List[str]:
    """Download multiple files to local directory.

    Uses fsspec.get() for efficient batch downloads.
    """
    if not urls:
        return []

    # All URLs should have same protocol
    protocol = urls[0].split("://")[0]
    fs = fsspec.filesystem(protocol, **storage_options)

    local_paths = []
    for url in urls:
        _, path = fsspec.core.url_to_fs(url)
        filename = path.rsplit("/", 1)[-1]
        local_path = f"{local_dir}/{filename}"
        fs.get(path, local_path)
        local_paths.append(local_path)

    return local_paths


def upload_files(
    local_paths: List[str],
    target_prefix: str,
    **storage_options,
) -> List[str]:
    """Upload local files to cloud storage.

    Uses fsspec.put() for efficient batch uploads.
    """
    protocol = target_prefix.split("://")[0]
    fs = fsspec.filesystem(protocol, **storage_options)

    target_urls = []
    for local_path in local_paths:
        filename = local_path.rsplit("/", 1)[-1]
        target = f"{target_prefix.rstrip('/')}/{filename}"
        _, target_path = fsspec.core.url_to_fs(target)
        fs.put(local_path, target_path)
        target_urls.append(target)

    return target_urls


# =============================================================================
# CACHING
# =============================================================================

def open_cached(
    url: str,
    cache_dir: str = "/tmp/earthaccess_cache",
    **storage_options,
):
    """Open file with local caching.

    Files are cached locally and reused on subsequent access.
    """
    return fsspec.open(
        f"filecache::{url}",
        mode="rb",
        filecache={"cache_storage": cache_dir},
        **storage_options,
    )


# =============================================================================
# URL UTILITIES
# =============================================================================

def url_to_fs_and_path(url: str, **storage_options):
    """Parse URL into filesystem and path components.

    Returns:
        (filesystem, path) tuple
    """
    return fsspec.core.url_to_fs(url, **storage_options)


def normalize_url(url: str) -> str:
    """Normalize URL to canonical form."""
    from urllib.parse import urlparse, urlunparse

    parsed = urlparse(url)

    # Ensure path doesn't have double slashes
    path = "/".join(p for p in parsed.path.split("/") if p)
    if parsed.path.startswith("/"):
        path = "/" + path

    return urlunparse(parsed._replace(path=path))
```

#### 0.8.4 Migration from boto3

| boto3 Pattern | fsspec Equivalent |
|---------------|-------------------|
| `s3.download_file(bucket, key, path)` | `fs.get(f"{bucket}/{key}", path)` |
| `s3.upload_file(path, bucket, key)` | `fs.put(path, f"{bucket}/{key}")` |
| `s3.list_objects_v2(Bucket=bucket)` | `fs.ls(bucket)` |
| `obj.get()['Body'].read()` | `fs.cat(path)` or `fs.open(path).read()` |
| `s3.head_object(...)` | `fs.info(path)` |
| `s3.delete_object(...)` | `fs.rm(path)` |

### 0.9 obstore as Alternative Backend

#### 0.9.1 What is obstore?

[obstore](https://github.com/developmentseed/obstore) is a Python binding to Rust's `object_store` crate, providing high-performance cloud storage access. Key advantages:

- **Performance**: 2-3x faster than boto3/s3fs for many operations
- **Memory efficiency**: Rust-based, lower memory overhead
- **Async native**: Built for async I/O
- **Multi-cloud**: S3, GCS, Azure with unified API

#### 0.9.2 When to Use obstore vs fsspec

| Use Case | Recommended | Reason |
|----------|-------------|--------|
| General data access | fsspec | Broader ecosystem, more backends |
| High-throughput transfers | obstore | Raw performance |
| Streaming analytics | fsspec | Better xarray/pandas integration |
| Serverless (Lambda/Functions) | obstore | Smaller binary, faster cold start |
| Multi-cloud workflows | Both work | obstore slightly faster |

#### 0.9.3 obstore Integration

```python
# earthaccess/store/obstore_backend.py

"""
obstore backend for high-performance cloud transfers.

Optional dependency: pip install earthaccess[obstore]
"""

from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

# Check for obstore availability
try:
    import obstore
    from obstore.store import S3Store, GCSStore, AzureStore
    HAS_OBSTORE = True
except ImportError:
    HAS_OBSTORE = False
    obstore = None


def check_obstore_available() -> bool:
    """Check if obstore is installed."""
    if not HAS_OBSTORE:
        logger.warning(
            "obstore not available. Install with: pip install earthaccess[obstore]"
        )
    return HAS_OBSTORE


class ObstoreTransfer:
    """High-performance cloud transfer using obstore.

    This is an optional backend for users who need maximum throughput.
    Falls back to fsspec if obstore is not installed.
    """

    def __init__(self, credential_manager: "CredentialManager"):
        if not HAS_OBSTORE:
            raise ImportError(
                "obstore is required for ObstoreTransfer. "
                "Install with: pip install obstore"
            )
        self.credentials = credential_manager
        self._stores: Dict[str, Any] = {}

    def get_store(
        self,
        url: str,
        *,
        provider: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
    ):
        """Get obstore Store for URL.

        Args:
            url: Cloud URL (s3://, gs://, az://)
            provider: NASA provider for S3 credentials
            options: Store-specific options

        Returns:
            Configured obstore Store
        """
        from urllib.parse import urlparse
        parsed = urlparse(url)
        protocol = parsed.scheme
        bucket = parsed.netloc

        cache_key = (protocol, bucket, provider)
        if cache_key in self._stores:
            return self._stores[cache_key]

        opts = dict(options or {})

        if protocol == "s3":
            if provider:
                creds = self.credentials.get_credentials(provider=provider)
                opts.update({
                    "aws_access_key_id": creds.access_key_id,
                    "aws_secret_access_key": creds.secret_access_key,
                    "aws_session_token": creds.session_token,
                    "region": "us-west-2",
                })
            store = S3Store.from_url(f"s3://{bucket}", **opts)

        elif protocol == "gs":
            store = GCSStore.from_url(f"gs://{bucket}", **opts)

        elif protocol in ("az", "abfs"):
            store = AzureStore.from_url(url, **opts)

        else:
            raise ValueError(f"Unsupported protocol for obstore: {protocol}")

        self._stores[cache_key] = store
        return store

    async def copy_file_async(
        self,
        source_url: str,
        target_url: str,
        *,
        source_provider: Optional[str] = None,
        source_options: Optional[Dict[str, Any]] = None,
        target_options: Optional[Dict[str, Any]] = None,
        chunk_size: int = 8 * 1024 * 1024,
    ) -> str:
        """Copy file between cloud locations asynchronously.

        Uses obstore's async API for high throughput.
        """
        from urllib.parse import urlparse

        source_store = self.get_store(source_url, provider=source_provider, options=source_options)
        target_store = self.get_store(target_url, options=target_options)

        source_path = urlparse(source_url).path.lstrip("/")
        target_parsed = urlparse(target_url)
        target_path = target_parsed.path.lstrip("/")

        # If target is directory, append source filename
        if target_path.endswith("/") or not target_path:
            source_name = source_path.rsplit("/", 1)[-1]
            target_path = f"{target_path.rstrip('/')}/{source_name}"

        # Stream using obstore's get_range and put
        # obstore handles chunking internally for large files
        data = await source_store.get_async(source_path)
        content = await data.bytes_async()

        await target_store.put_async(target_path, content)

        return f"{target_parsed.scheme}://{target_parsed.netloc}/{target_path}"

    def copy_file(
        self,
        source_url: str,
        target_url: str,
        **kwargs,
    ) -> str:
        """Synchronous wrapper for copy_file_async."""
        import asyncio
        return asyncio.run(self.copy_file_async(source_url, target_url, **kwargs))

    async def transfer_batch_async(
        self,
        sources: List[str],
        target: str,
        *,
        provider: Optional[str] = None,
        source_options: Optional[Dict[str, Any]] = None,
        target_options: Optional[Dict[str, Any]] = None,
        concurrency: int = 8,
    ) -> List[str]:
        """Transfer multiple files concurrently using async I/O.

        obstore's async API enables high concurrency without threads.
        """
        import asyncio

        semaphore = asyncio.Semaphore(concurrency)

        async def transfer_one(source: str) -> str:
            async with semaphore:
                return await self.copy_file_async(
                    source,
                    target,
                    source_provider=provider,
                    source_options=source_options,
                    target_options=target_options,
                )

        tasks = [transfer_one(s) for s in sources]
        return await asyncio.gather(*tasks)

    def transfer_batch(
        self,
        sources: List[str],
        target: str,
        **kwargs,
    ) -> List[str]:
        """Synchronous wrapper for transfer_batch_async."""
        import asyncio
        return asyncio.run(self.transfer_batch_async(sources, target, **kwargs))


def get_transfer_backend(
    backend: str = "auto",
    credential_manager: Optional["CredentialManager"] = None,
) -> "CloudTransfer":
    """Get appropriate transfer backend.

    Args:
        backend: "fsspec", "obstore", or "auto"
        credential_manager: Credential manager instance

    Returns:
        CloudTransfer or ObstoreTransfer instance
    """
    if backend == "obstore":
        if not HAS_OBSTORE:
            raise ImportError("obstore requested but not installed")
        return ObstoreTransfer(credential_manager)

    elif backend == "fsspec":
        from .transfer import CloudTransfer
        return CloudTransfer(credential_manager)

    elif backend == "auto":
        # Prefer obstore if available and user is doing batch transfers
        if HAS_OBSTORE:
            return ObstoreTransfer(credential_manager)
        else:
            from .transfer import CloudTransfer
            return CloudTransfer(credential_manager)

    else:
        raise ValueError(f"Unknown backend: {backend}. Use 'fsspec', 'obstore', or 'auto'")
```

#### 0.9.4 API Surface

```python
# User-facing API additions

def download(
    granules,
    target,
    *,
    transfer_backend: Literal["fsspec", "obstore", "auto"] = "auto",
    **kwargs,
) -> List[str]:
    """
    ...existing docstring...

    Args:
        ...
        transfer_backend: Which backend for cloud transfers
            - "fsspec": Use fsspec (default, most compatible)
            - "obstore": Use obstore (fastest, requires install)
            - "auto": Use obstore if available, else fsspec
    """
```

---

## Group A: Query Architecture

### A.1 Design Goals

- **Dedicated query classes**: `GranuleQuery` and `CollectionQuery` as first-class objects
- **Backend parameter**: Specify `backend="cmr"` or `backend="stac"` at construction
- **Dual construction**: Support both method chaining and named parameters
- **Validation**: Validate parameters based on the selected backend's API
- **`to_stac()` method**: Convert query to pystac-client compatible parameters
- **`to_cmr()` method**: Convert query to CMR API parameters

### A.2 Query Class Design

```python
# earthaccess/query.py

from dataclasses import dataclass, field
from datetime import datetime, date
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
import re

class Backend(Enum):
    """Supported query backends."""
    CMR = "cmr"
    STAC = "stac"


@dataclass
class ValidationError:
    """Validation error for query parameters."""
    parameter: str
    message: str
    value: Any = None


class GranuleQuery:
    """Query for granules/items from CMR or STAC catalogs.

    Supports both method chaining and named parameter construction.
    The backend determines which API the query will be executed against.

    Examples:
        # Method chaining style
        query = (
            earthaccess.GranuleQuery(backend="cmr")
            .collections(["C1234567890-PROVIDER"])
            .temporal("2023-01-01", "2023-12-31")
            .bounding_box(-180, -90, 180, 90)
            .cloud_hosted(True)
        )
        results = earthaccess.search_data(query)

        # Named parameters style
        query = earthaccess.GranuleQuery(
            backend="cmr",
            collections=["C1234567890-PROVIDER"],
            temporal=("2023-01-01", "2023-12-31"),
            bounding_box=(-180, -90, 180, 90),
            cloud_hosted=True,
        )
        results = earthaccess.search_data(query)

        # Get pystac-client compatible parameters
        stac_params = query.to_stac()
        # Use with pystac-client
        from pystac_client import Client
        catalog = Client.open("https://cmr.earthdata.nasa.gov/stac")
        item_search = catalog.search(**stac_params)
    """

    def __init__(
        self,
        backend: str = "cmr",
        *,
        # Collection/dataset identification
        collections: Optional[List[str]] = None,
        short_name: Optional[str] = None,
        concept_id: Optional[Union[str, List[str]]] = None,
        doi: Optional[str] = None,
        # Spatial filters
        bounding_box: Optional[Tuple[float, float, float, float]] = None,
        polygon: Optional[Sequence[Tuple[float, float]]] = None,
        point: Optional[Tuple[float, float]] = None,
        intersects: Optional[Dict[str, Any]] = None,  # GeoJSON
        # Temporal filters
        temporal: Optional[Tuple[Optional[str], Optional[str]]] = None,
        # Granule filters
        granule_name: Optional[Union[str, List[str]]] = None,
        granule_ur: Optional[Union[str, List[str]]] = None,
        day_night_flag: Optional[str] = None,
        cloud_cover: Optional[Tuple[Optional[float], Optional[float]]] = None,
        # CMR-specific
        provider: Optional[str] = None,
        cloud_hosted: Optional[bool] = None,
        downloadable: Optional[bool] = None,
        online_only: Optional[bool] = None,
        # Pagination
        limit: Optional[int] = None,
    ):
        """Initialize a GranuleQuery.

        Args:
            backend: Query backend - "cmr" or "stac"
            collections: List of collection IDs (concept IDs or short names)
            short_name: Collection short name (e.g., "ATL03")
            concept_id: Collection or granule concept ID(s)
            doi: Dataset DOI
            bounding_box: (west, south, east, north) in WGS84
            polygon: List of (lon, lat) tuples forming a closed polygon
            point: (lon, lat) point
            intersects: GeoJSON geometry for spatial intersection
            temporal: (start, end) date strings or None for open-ended
            granule_name: Granule name pattern (supports wildcards)
            granule_ur: Granule UR (unique reference)
            day_night_flag: "day", "night", or "unspecified"
            cloud_cover: (min, max) cloud cover percentage
            provider: Data provider (e.g., "POCLOUD", "NSIDC_CPRD")
            cloud_hosted: Filter for cloud-hosted data only
            downloadable: Filter for downloadable granules
            online_only: Filter for online-only granules
            limit: Maximum number of results
        """
        self._backend = Backend(backend.lower())
        self._params: Dict[str, Any] = {}
        self._errors: List[ValidationError] = []

        # Apply named parameters
        if collections:
            self.collections(collections)
        if short_name:
            self.short_name(short_name)
        if concept_id:
            self.concept_id(concept_id)
        if doi:
            self.doi(doi)
        if bounding_box:
            self.bounding_box(*bounding_box)
        if polygon:
            self.polygon(polygon)
        if point:
            self.point(*point)
        if intersects:
            self.intersects(intersects)
        if temporal:
            self.temporal(*temporal)
        if granule_name:
            self.granule_name(granule_name)
        if granule_ur:
            self.granule_ur(granule_ur)
        if day_night_flag:
            self.day_night_flag(day_night_flag)
        if cloud_cover:
            self.cloud_cover(*cloud_cover)
        if provider:
            self.provider(provider)
        if cloud_hosted is not None:
            self.cloud_hosted(cloud_hosted)
        if downloadable is not None:
            self.downloadable(downloadable)
        if online_only is not None:
            self.online_only(online_only)
        if limit:
            self.limit(limit)

    @property
    def backend(self) -> Backend:
        """Return the query backend."""
        return self._backend

    # =========================================================================
    # COLLECTION/DATASET IDENTIFICATION
    # =========================================================================

    def collections(self, collection_ids: List[str]) -> "GranuleQuery":
        """Filter by collection IDs.

        Accepts concept IDs (C*) or short names. For CMR, concept IDs are
        preferred for unambiguous matching.

        Args:
            collection_ids: List of collection identifiers

        Returns:
            self for chaining
        """
        self._validate_collections(collection_ids)
        self._params["collections"] = collection_ids
        return self

    def short_name(self, name: str) -> "GranuleQuery":
        """Filter by collection short name.

        Args:
            name: Collection short name (e.g., "ATL03", "HLSS30")

        Returns:
            self for chaining
        """
        self._validate_short_name(name)
        self._params["short_name"] = name
        return self

    def concept_id(self, ids: Union[str, List[str]]) -> "GranuleQuery":
        """Filter by concept ID(s).

        Args:
            ids: Single concept ID or list of concept IDs

        Returns:
            self for chaining
        """
        if isinstance(ids, str):
            ids = [ids]
        self._validate_concept_ids(ids)
        self._params["concept_id"] = ids
        return self

    def doi(self, doi: str) -> "GranuleQuery":
        """Filter by dataset DOI.

        Args:
            doi: DOI string (e.g., "10.5067/AQR50-3Q7CS")

        Returns:
            self for chaining
        """
        self._validate_doi(doi)
        self._params["doi"] = doi
        return self

    # =========================================================================
    # SPATIAL FILTERS
    # =========================================================================

    def bounding_box(
        self,
        west: float,
        south: float,
        east: float,
        north: float,
    ) -> "GranuleQuery":
        """Filter by bounding box.

        Args:
            west: Western longitude (-180 to 180)
            south: Southern latitude (-90 to 90)
            east: Eastern longitude (-180 to 180)
            north: Northern latitude (-90 to 90)

        Returns:
            self for chaining
        """
        bbox = (float(west), float(south), float(east), float(north))
        self._validate_bbox(bbox)
        self._params["bounding_box"] = bbox
        return self

    def polygon(self, coordinates: Sequence[Tuple[float, float]]) -> "GranuleQuery":
        """Filter by polygon intersection.

        Args:
            coordinates: List of (lon, lat) tuples forming a closed polygon.
                        The ring should be closed (first == last point).

        Returns:
            self for chaining
        """
        self._validate_polygon(coordinates)
        self._params["polygon"] = list(coordinates)
        return self

    def point(self, lon: float, lat: float) -> "GranuleQuery":
        """Filter by point intersection.

        Args:
            lon: Longitude (-180 to 180)
            lat: Latitude (-90 to 90)

        Returns:
            self for chaining
        """
        self._validate_point(lon, lat)
        self._params["point"] = (float(lon), float(lat))
        return self

    def intersects(self, geometry: Dict[str, Any]) -> "GranuleQuery":
        """Filter by GeoJSON geometry intersection.

        Args:
            geometry: GeoJSON geometry object (Point, Polygon, MultiPolygon, etc.)

        Returns:
            self for chaining
        """
        self._validate_geojson(geometry)
        self._params["intersects"] = geometry
        return self

    # =========================================================================
    # TEMPORAL FILTERS
    # =========================================================================

    def temporal(
        self,
        start: Optional[Union[str, date, datetime]] = None,
        end: Optional[Union[str, date, datetime]] = None,
    ) -> "GranuleQuery":
        """Filter by temporal range.

        Args:
            start: Start of temporal range (ISO format string or datetime)
            end: End of temporal range (ISO format string or datetime)

        Returns:
            self for chaining

        Examples:
            query.temporal("2023-01-01", "2023-12-31")
            query.temporal("2023-01-01", None)  # Open-ended
            query.temporal(None, "2023-12-31")  # Open start
        """
        start_dt = self._parse_datetime(start) if start else None
        end_dt = self._parse_datetime(end) if end else None
        self._validate_temporal(start_dt, end_dt)
        self._params["temporal"] = (start_dt, end_dt)
        return self

    # =========================================================================
    # GRANULE FILTERS
    # =========================================================================

    def granule_name(self, name: Union[str, List[str]]) -> "GranuleQuery":
        """Filter by granule name pattern.

        Supports wildcards (* and ?) for pattern matching.

        Args:
            name: Granule name or pattern (e.g., "ATL03_*.h5")

        Returns:
            self for chaining
        """
        if isinstance(name, str):
            name = [name]
        self._params["granule_name"] = name
        return self

    def granule_ur(self, ur: Union[str, List[str]]) -> "GranuleQuery":
        """Filter by granule UR (unique reference).

        Args:
            ur: Granule UR or list of URs

        Returns:
            self for chaining
        """
        if isinstance(ur, str):
            ur = [ur]
        self._params["granule_ur"] = ur
        return self

    def day_night_flag(self, flag: str) -> "GranuleQuery":
        """Filter by day/night acquisition flag.

        Args:
            flag: "day", "night", or "unspecified"

        Returns:
            self for chaining
        """
        self._validate_day_night_flag(flag)
        self._params["day_night_flag"] = flag.upper()
        return self

    def cloud_cover(
        self,
        min_cover: Optional[float] = None,
        max_cover: Optional[float] = None,
    ) -> "GranuleQuery":
        """Filter by cloud cover percentage.

        Args:
            min_cover: Minimum cloud cover (0-100)
            max_cover: Maximum cloud cover (0-100)

        Returns:
            self for chaining
        """
        self._validate_cloud_cover(min_cover, max_cover)
        self._params["cloud_cover"] = (min_cover, max_cover)
        return self

    # =========================================================================
    # CMR-SPECIFIC FILTERS
    # =========================================================================

    def provider(self, provider: str) -> "GranuleQuery":
        """Filter by data provider.

        Args:
            provider: Provider code (e.g., "POCLOUD", "NSIDC_CPRD", "LPCLOUD")

        Returns:
            self for chaining
        """
        self._params["provider"] = provider.upper()
        return self

    def cloud_hosted(self, hosted: bool = True) -> "GranuleQuery":
        """Filter for cloud-hosted data only.

        Args:
            hosted: If True, return only cloud-hosted data

        Returns:
            self for chaining
        """
        self._params["cloud_hosted"] = hosted
        return self

    def downloadable(self, downloadable: bool = True) -> "GranuleQuery":
        """Filter for downloadable granules.

        Args:
            downloadable: If True, return only downloadable granules

        Returns:
            self for chaining
        """
        self._params["downloadable"] = downloadable
        return self

    def online_only(self, online: bool = True) -> "GranuleQuery":
        """Filter for online-only granules.

        Args:
            online: If True, return only online granules

        Returns:
            self for chaining
        """
        self._params["online_only"] = online
        return self

    # =========================================================================
    # PAGINATION
    # =========================================================================

    def limit(self, count: int) -> "GranuleQuery":
        """Set maximum number of results.

        Args:
            count: Maximum results to return

        Returns:
            self for chaining
        """
        if count < 1:
            self._errors.append(ValidationError("limit", "must be >= 1", count))
        self._params["limit"] = count
        return self

    # =========================================================================
    # CONVERSION METHODS
    # =========================================================================

    def to_stac(self) -> Dict[str, Any]:
        """Convert query to pystac-client compatible parameters.

        Returns a dictionary that can be passed directly to pystac-client's
        `Client.search()` or `ItemSearch` constructor.

        Returns:
            Dictionary of STAC API parameters

        Example:
            >>> query = GranuleQuery(backend="cmr", collections=["ATL03"])
            >>> stac_params = query.to_stac()
            >>>
            >>> from pystac_client import Client
            >>> catalog = Client.open("https://cmr.earthdata.nasa.gov/stac")
            >>> search = catalog.search(**stac_params)
            >>> for item in search.items():
            ...     print(item.id)
        """
        params: Dict[str, Any] = {}

        # Collections
        if "collections" in self._params:
            params["collections"] = self._params["collections"]
        elif "short_name" in self._params:
            params["collections"] = [self._params["short_name"]]
        elif "concept_id" in self._params:
            params["collections"] = self._params["concept_id"]

        # Spatial - prefer intersects (GeoJSON), then bbox
        if "intersects" in self._params:
            params["intersects"] = self._params["intersects"]
        elif "polygon" in self._params:
            coords = self._params["polygon"]
            # Ensure ring is closed
            if coords[0] != coords[-1]:
                coords = coords + [coords[0]]
            params["intersects"] = {
                "type": "Polygon",
                "coordinates": [[[lon, lat] for lon, lat in coords]]
            }
        elif "point" in self._params:
            lon, lat = self._params["point"]
            params["intersects"] = {
                "type": "Point",
                "coordinates": [lon, lat]
            }
        elif "bounding_box" in self._params:
            params["bbox"] = list(self._params["bounding_box"])

        # Temporal - convert to RFC 3339 interval
        if "temporal" in self._params:
            start, end = self._params["temporal"]
            start_str = start.isoformat() + "Z" if start else ".."
            end_str = end.isoformat() + "Z" if end else ".."
            params["datetime"] = f"{start_str}/{end_str}"

        # Item IDs (if granule concept IDs specified)
        if "concept_id" in self._params and all(
            id.startswith("G") for id in self._params["concept_id"]
        ):
            params["ids"] = self._params["concept_id"]

        # STAC Query Extension (for filters like cloud_cover)
        query_ext: Dict[str, Any] = {}

        if "cloud_cover" in self._params:
            min_cc, max_cc = self._params["cloud_cover"]
            cc_filter = {}
            if min_cc is not None:
                cc_filter["gte"] = min_cc
            if max_cc is not None:
                cc_filter["lte"] = max_cc
            if cc_filter:
                query_ext["eo:cloud_cover"] = cc_filter

        if query_ext:
            params["query"] = query_ext

        # Limit
        if "limit" in self._params:
            params["max_items"] = self._params["limit"]

        return params

    def to_cmr(self) -> Dict[str, Any]:
        """Convert query to CMR API parameters.

        Returns a dictionary suitable for CMR's granule search API.

        Returns:
            Dictionary of CMR API parameters
        """
        params: Dict[str, Any] = {}

        # Collection identification
        if "short_name" in self._params:
            params["short_name"] = self._params["short_name"]
        if "concept_id" in self._params:
            params["concept_id"] = self._params["concept_id"]
        if "doi" in self._params:
            params["doi"] = self._params["doi"]
        if "collections" in self._params:
            # Determine if concept IDs or short names
            colls = self._params["collections"]
            if colls and colls[0].startswith("C"):
                params["concept_id"] = colls
            else:
                # Treat as short names
                if len(colls) == 1:
                    params["short_name"] = colls[0]
                else:
                    params["short_name"] = colls

        # Spatial
        if "bounding_box" in self._params:
            w, s, e, n = self._params["bounding_box"]
            params["bounding_box"] = f"{w},{s},{e},{n}"
        if "polygon" in self._params:
            coords = self._params["polygon"]
            flat = ",".join(f"{lon},{lat}" for lon, lat in coords)
            params["polygon"] = flat
        if "point" in self._params:
            lon, lat = self._params["point"]
            params["point"] = f"{lon},{lat}"
        if "intersects" in self._params:
            geom = self._params["intersects"]
            if geom["type"] == "Point":
                coords = geom["coordinates"]
                params["point"] = f"{coords[0]},{coords[1]}"
            elif geom["type"] == "Polygon":
                coords = geom["coordinates"][0]
                flat = ",".join(f"{c[0]},{c[1]}" for c in coords)
                params["polygon"] = flat

        # Temporal
        if "temporal" in self._params:
            start, end = self._params["temporal"]
            start_str = start.isoformat() if start else ""
            end_str = end.isoformat() if end else ""
            params["temporal"] = f"{start_str},{end_str}"

        # Granule filters
        if "granule_name" in self._params:
            params["readable_granule_name"] = self._params["granule_name"]
            params["options[readable_granule_name][pattern]"] = "true"
        if "granule_ur" in self._params:
            params["granule_ur"] = self._params["granule_ur"]
        if "day_night_flag" in self._params:
            params["day_night_flag"] = self._params["day_night_flag"]
        if "cloud_cover" in self._params:
            min_cc, max_cc = self._params["cloud_cover"]
            min_str = str(min_cc) if min_cc is not None else ""
            max_str = str(max_cc) if max_cc is not None else ""
            params["cloud_cover"] = f"{min_str},{max_str}"

        # CMR-specific
        if "provider" in self._params:
            params["provider"] = self._params["provider"]
        if "cloud_hosted" in self._params:
            params["cloud_hosted"] = str(self._params["cloud_hosted"]).lower()
        if "downloadable" in self._params:
            params["downloadable"] = str(self._params["downloadable"]).lower()
        if "online_only" in self._params:
            params["online_only"] = str(self._params["online_only"]).lower()

        # Pagination
        if "limit" in self._params:
            params["page_size"] = min(self._params["limit"], 2000)

        return params

    # =========================================================================
    # VALIDATION
    # =========================================================================

    def validate(self) -> List[ValidationError]:
        """Validate all query parameters.

        Returns:
            List of validation errors (empty if valid)
        """
        errors = list(self._errors)

        # Backend-specific validation
        if self._backend == Backend.CMR:
            errors.extend(self._validate_cmr_params())
        else:
            errors.extend(self._validate_stac_params())

        return errors

    def is_valid(self) -> bool:
        """Check if query is valid."""
        return len(self.validate()) == 0

    def _validate_collections(self, collection_ids: List[str]) -> None:
        if not collection_ids:
            self._errors.append(ValidationError(
                "collections", "must not be empty"
            ))

    def _validate_short_name(self, name: str) -> None:
        if not name or not name.strip():
            self._errors.append(ValidationError(
                "short_name", "must not be empty", name
            ))

    def _validate_concept_ids(self, ids: List[str]) -> None:
        for id in ids:
            if not re.match(r'^[CGS]\d+-[A-Z0-9_]+$', id):
                self._errors.append(ValidationError(
                    "concept_id",
                    f"invalid format: {id}. Expected pattern: C/G/S + digits + - + provider",
                    id
                ))

    def _validate_doi(self, doi: str) -> None:
        if not re.match(r'^10\.\d+/', doi):
            self._errors.append(ValidationError(
                "doi", f"invalid DOI format: {doi}", doi
            ))

    def _validate_bbox(self, bbox: Tuple[float, float, float, float]) -> None:
        west, south, east, north = bbox
        if not (-180 <= west <= 180):
            self._errors.append(ValidationError("bounding_box", "west must be -180 to 180", west))
        if not (-90 <= south <= 90):
            self._errors.append(ValidationError("bounding_box", "south must be -90 to 90", south))
        if not (-180 <= east <= 180):
            self._errors.append(ValidationError("bounding_box", "east must be -180 to 180", east))
        if not (-90 <= north <= 90):
            self._errors.append(ValidationError("bounding_box", "north must be -90 to 90", north))
        if south > north:
            self._errors.append(ValidationError("bounding_box", "south must be <= north"))

    def _validate_polygon(self, coords: Sequence[Tuple[float, float]]) -> None:
        if len(coords) < 3:
            self._errors.append(ValidationError(
                "polygon", "must have at least 3 points", coords
            ))
        for lon, lat in coords:
            if not (-180 <= lon <= 180):
                self._errors.append(ValidationError("polygon", f"invalid longitude: {lon}"))
            if not (-90 <= lat <= 90):
                self._errors.append(ValidationError("polygon", f"invalid latitude: {lat}"))

    def _validate_point(self, lon: float, lat: float) -> None:
        if not (-180 <= lon <= 180):
            self._errors.append(ValidationError("point", f"invalid longitude: {lon}"))
        if not (-90 <= lat <= 90):
            self._errors.append(ValidationError("point", f"invalid latitude: {lat}"))

    def _validate_geojson(self, geom: Dict[str, Any]) -> None:
        if "type" not in geom:
            self._errors.append(ValidationError("intersects", "missing 'type' field"))
        elif geom["type"] not in ("Point", "Polygon", "MultiPolygon", "LineString"):
            self._errors.append(ValidationError(
                "intersects", f"unsupported geometry type: {geom['type']}"
            ))
        if "coordinates" not in geom:
            self._errors.append(ValidationError("intersects", "missing 'coordinates' field"))

    def _validate_temporal(self, start: Optional[datetime], end: Optional[datetime]) -> None:
        if start and end and start > end:
            self._errors.append(ValidationError(
                "temporal", "start must be <= end"
            ))

    def _validate_day_night_flag(self, flag: str) -> None:
        valid = {"day", "night", "unspecified"}
        if flag.lower() not in valid:
            self._errors.append(ValidationError(
                "day_night_flag", f"must be one of {valid}", flag
            ))

    def _validate_cloud_cover(self, min_cc: Optional[float], max_cc: Optional[float]) -> None:
        if min_cc is not None and not (0 <= min_cc <= 100):
            self._errors.append(ValidationError("cloud_cover", "min must be 0-100", min_cc))
        if max_cc is not None and not (0 <= max_cc <= 100):
            self._errors.append(ValidationError("cloud_cover", "max must be 0-100", max_cc))
        if min_cc is not None and max_cc is not None and min_cc > max_cc:
            self._errors.append(ValidationError("cloud_cover", "min must be <= max"))

    def _validate_cmr_params(self) -> List[ValidationError]:
        """CMR-specific validation."""
        errors = []
        # Add CMR-specific validation rules
        return errors

    def _validate_stac_params(self) -> List[ValidationError]:
        """STAC-specific validation."""
        errors = []
        # STAC doesn't support some CMR-specific params
        unsupported = ["granule_name", "granule_ur", "downloadable", "online_only"]
        for param in unsupported:
            if param in self._params:
                errors.append(ValidationError(
                    param, f"not supported by STAC backend", self._params[param]
                ))
        return errors

    @staticmethod
    def _parse_datetime(dt: Union[str, date, datetime]) -> datetime:
        """Parse various datetime formats to datetime object."""
        if isinstance(dt, datetime):
            return dt
        if isinstance(dt, date):
            return datetime(dt.year, dt.month, dt.day, 23, 59, 59)
        # Parse string - try common formats
        for fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"]:
            try:
                return datetime.strptime(dt, fmt)
            except ValueError:
                continue
        # Try ISO format
        return datetime.fromisoformat(dt.replace("Z", "+00:00").replace("+00:00", ""))

    def __repr__(self) -> str:
        return f"GranuleQuery(backend={self._backend.value!r}, params={self._params!r})"


class CollectionQuery:
    """Query for collections/datasets from CMR or STAC catalogs.

    Similar interface to GranuleQuery but for collection-level searches.

    Examples:
        # Method chaining
        query = (
            earthaccess.CollectionQuery(backend="cmr")
            .keyword("temperature ocean")
            .provider("POCLOUD")
            .cloud_hosted(True)
        )
        results = earthaccess.search_datasets(query)

        # Named parameters
        query = earthaccess.CollectionQuery(
            backend="cmr",
            keyword="temperature ocean",
            provider="POCLOUD",
            cloud_hosted=True,
        )
    """

    def __init__(
        self,
        backend: str = "cmr",
        *,
        keyword: Optional[str] = None,
        short_name: Optional[str] = None,
        concept_id: Optional[Union[str, List[str]]] = None,
        doi: Optional[str] = None,
        bounding_box: Optional[Tuple[float, float, float, float]] = None,
        temporal: Optional[Tuple[Optional[str], Optional[str]]] = None,
        provider: Optional[str] = None,
        cloud_hosted: Optional[bool] = None,
        has_granules: Optional[bool] = None,
        limit: Optional[int] = None,
    ):
        """Initialize a CollectionQuery.

        Args:
            backend: Query backend - "cmr" or "stac"
            keyword: Full-text search keyword(s)
            short_name: Collection short name
            concept_id: Collection concept ID(s)
            doi: Dataset DOI
            bounding_box: (west, south, east, north) spatial filter
            temporal: (start, end) temporal filter
            provider: Data provider
            cloud_hosted: Filter for cloud-hosted collections
            has_granules: Filter for collections with/without granules
            limit: Maximum results
        """
        self._backend = Backend(backend.lower())
        self._params: Dict[str, Any] = {}
        self._errors: List[ValidationError] = []

        if keyword:
            self.keyword(keyword)
        if short_name:
            self.short_name(short_name)
        if concept_id:
            self.concept_id(concept_id)
        if doi:
            self.doi(doi)
        if bounding_box:
            self.bounding_box(*bounding_box)
        if temporal:
            self.temporal(*temporal)
        if provider:
            self.provider(provider)
        if cloud_hosted is not None:
            self.cloud_hosted(cloud_hosted)
        if has_granules is not None:
            self.has_granules(has_granules)
        if limit:
            self.limit(limit)

    def keyword(self, text: str) -> "CollectionQuery":
        """Full-text search across collection metadata.

        Args:
            text: Search text

        Returns:
            self for chaining
        """
        self._params["keyword"] = text
        return self

    def short_name(self, name: str) -> "CollectionQuery":
        """Filter by collection short name."""
        self._params["short_name"] = name
        return self

    def concept_id(self, ids: Union[str, List[str]]) -> "CollectionQuery":
        """Filter by concept ID(s)."""
        if isinstance(ids, str):
            ids = [ids]
        self._params["concept_id"] = ids
        return self

    def doi(self, doi: str) -> "CollectionQuery":
        """Filter by DOI."""
        self._params["doi"] = doi
        return self

    def bounding_box(self, west: float, south: float, east: float, north: float) -> "CollectionQuery":
        """Filter by bounding box."""
        self._params["bounding_box"] = (float(west), float(south), float(east), float(north))
        return self

    def temporal(self, start: Optional[str] = None, end: Optional[str] = None) -> "CollectionQuery":
        """Filter by temporal extent."""
        self._params["temporal"] = (
            GranuleQuery._parse_datetime(start) if start else None,
            GranuleQuery._parse_datetime(end) if end else None,
        )
        return self

    def provider(self, provider: str) -> "CollectionQuery":
        """Filter by data provider."""
        self._params["provider"] = provider.upper()
        return self

    def cloud_hosted(self, hosted: bool = True) -> "CollectionQuery":
        """Filter for cloud-hosted collections."""
        self._params["cloud_hosted"] = hosted
        return self

    def has_granules(self, has_granules: bool = True) -> "CollectionQuery":
        """Filter for collections with/without granules."""
        self._params["has_granules"] = has_granules
        return self

    def limit(self, count: int) -> "CollectionQuery":
        """Set maximum number of results."""
        self._params["limit"] = count
        return self

    def to_stac(self) -> Dict[str, Any]:
        """Convert to pystac-client CollectionSearch compatible parameters."""
        params: Dict[str, Any] = {}

        if "keyword" in self._params:
            params["q"] = self._params["keyword"]
        if "bounding_box" in self._params:
            params["bbox"] = list(self._params["bounding_box"])
        if "temporal" in self._params:
            start, end = self._params["temporal"]
            start_str = start.isoformat() + "Z" if start else ".."
            end_str = end.isoformat() + "Z" if end else ".."
            params["datetime"] = f"{start_str}/{end_str}"
        if "limit" in self._params:
            params["limit"] = self._params["limit"]

        return params

    def to_cmr(self) -> Dict[str, Any]:
        """Convert to CMR collection search parameters."""
        params: Dict[str, Any] = {}

        if "keyword" in self._params:
            params["keyword"] = self._params["keyword"]
        if "short_name" in self._params:
            params["short_name"] = self._params["short_name"]
        if "concept_id" in self._params:
            params["concept_id"] = self._params["concept_id"]
        if "doi" in self._params:
            params["doi"] = self._params["doi"]
        if "bounding_box" in self._params:
            w, s, e, n = self._params["bounding_box"]
            params["bounding_box"] = f"{w},{s},{e},{n}"
        if "temporal" in self._params:
            start, end = self._params["temporal"]
            start_str = start.isoformat() if start else ""
            end_str = end.isoformat() if end else ""
            params["temporal"] = f"{start_str},{end_str}"
        if "provider" in self._params:
            params["provider"] = self._params["provider"]
        if "cloud_hosted" in self._params:
            params["cloud_hosted"] = str(self._params["cloud_hosted"]).lower()
        if "has_granules" in self._params:
            params["has_granules"] = str(self._params["has_granules"]).lower()
        if "limit" in self._params:
            params["page_size"] = min(self._params["limit"], 2000)

        return params

    def validate(self) -> List[ValidationError]:
        """Validate query parameters."""
        return list(self._errors)

    def is_valid(self) -> bool:
        """Check if query is valid."""
        return len(self.validate()) == 0

    def __repr__(self) -> str:
        return f"CollectionQuery(backend={self._backend.value!r}, params={self._params!r})"
```

### A.3 Integration with `search_data()` and `search_datasets()`

```python
# earthaccess/api.py (additions)

from typing import Union, overload

@overload
def search_data(query: GranuleQuery) -> List[DataGranule]: ...

@overload
def search_data(
    *,
    short_name: str = ...,
    temporal: Tuple[str, str] = ...,
    # ... other params
) -> List[DataGranule]: ...

def search_data(
    query: Optional[GranuleQuery] = None,
    **kwargs
) -> List[DataGranule]:
    """Search for granules using a query object or keyword parameters.

    Accepts either a GranuleQuery object or keyword parameters (current behavior).

    Args:
        query: A GranuleQuery object (new API)
        **kwargs: Keyword parameters (existing API, unchanged)

    Returns:
        List of DataGranule objects

    Examples:
        # New: Query object
        query = earthaccess.GranuleQuery(
            backend="cmr",
            collections=["ATL03"],
            temporal=("2023-01-01", "2023-12-31"),
        )
        granules = earthaccess.search_data(query)

        # Existing: Keyword parameters (unchanged)
        granules = earthaccess.search_data(
            short_name="ATL03",
            temporal=("2023-01-01", "2023-12-31"),
        )
    """
    if query is not None:
        # New query-based API
        if not query.is_valid():
            errors = query.validate()
            raise ValueError(f"Invalid query: {errors}")

        # Build internal query object
        cmr_params = query.to_cmr()
        # Execute using existing machinery
        return _execute_granule_search(cmr_params)
    else:
        # Existing keyword parameter API (unchanged)
        return _existing_search_data_impl(**kwargs)
```

### A.4 Parameter Mapping Reference

| Python Parameter | CMR API | STAC API | Notes |
|-----------------|---------|----------|-------|
| `collections` | `concept_id` or `short_name` | `collections` | Auto-detected |
| `short_name` | `short_name` | `collections[0]` | Single name |
| `concept_id` | `concept_id` | `collections` or `ids` | C* for collections, G* for items |
| `doi` | `doi` | - | CMR only |
| `bounding_box` | `bounding_box` | `bbox` | Format differs |
| `polygon` | `polygon` | `intersects` | Converted to GeoJSON |
| `point` | `point` | `intersects` | Converted to GeoJSON |
| `intersects` | `polygon` | `intersects` | Native GeoJSON |
| `temporal` | `temporal` | `datetime` | Format differs |
| `granule_name` | `readable_granule_name[pattern]` | - | CMR only, wildcards |
| `day_night_flag` | `day_night_flag` | - | CMR only |
| `cloud_cover` | `cloud_cover` | `query.eo:cloud_cover` | STAC Query Ext |
| `provider` | `provider` | - | CMR only |
| `cloud_hosted` | `cloud_hosted` | - | CMR only |
| `limit` | `page_size` | `max_items` | - |

---

## Group B: Results Classes

### B.1 Design Goals

- **Lazy pagination** using CMR's `cmr-search-after` token
- **Unified interface** across CMR and STAC results
- **`.get_all()`** for backwards-compatible eager fetching
- **`.to_stac()`** and **`.to_umm()`** for bidirectional conversion

### B.2 Results Class Hierarchy

```python
# earthaccess/results.py

from abc import ABC, abstractmethod
from typing import Iterator, List, Generic, TypeVar, Dict, Any

T = TypeVar("T")

class ResultsBase(ABC, Generic[T]):
    """Abstract base for lazy, paginated search results."""

    @abstractmethod
    def __iter__(self) -> Iterator[T]:
        """Iterate over individual results."""
        ...

    @abstractmethod
    def pages(self, page_size: int = 2000) -> Iterator[List[T]]:
        """Iterate over pages of results."""
        ...

    def get_all(self) -> List[T]:
        """Eagerly fetch all results as a list.

        Provides backwards compatibility with current earthaccess API.
        """
        return list(self)

    @abstractmethod
    def matched(self) -> int:
        """Return total number of matching results."""
        ...

    def __len__(self) -> int:
        """Return matched count."""
        return self.matched()


class GranuleResults(ResultsBase["DataGranule"]):
    """Lazy, paginated granule search results.

    Provides iteration patterns similar to pystac-client's ItemSearch.
    """

    def items(self) -> Iterator["DataGranule"]:
        """Iterate over individual granules (STAC-style naming)."""
        return iter(self)

    def granules(self) -> Iterator["DataGranule"]:
        """Alias for items() - earthaccess-style naming."""
        return self.items()

    def to_stac(self) -> "pystac.ItemCollection":
        """Convert all granules to a pystac ItemCollection."""
        import pystac
        items = [g.to_stac() for g in self]
        return pystac.ItemCollection(items)

    def to_umm(self) -> List[Dict[str, Any]]:
        """Return raw UMM-G dictionaries."""
        return [g.to_umm() for g in self]


class CollectionResults(ResultsBase["DataCollection"]):
    """Lazy, paginated collection search results."""

    def collections(self) -> Iterator["DataCollection"]:
        """Iterate over individual collections."""
        return iter(self)

    def to_stac(self) -> List["pystac.Collection"]:
        """Convert all results to pystac Collection objects."""
        return [c.to_stac() for c in self]

    def to_umm(self) -> List[Dict[str, Any]]:
        """Return raw UMM-C dictionaries."""
        return [c.to_umm() for c in self]
```

See [STAC-PLAN.md](./STAC-PLAN.md) for full implementation details.

### B.3 Lazy Pagination vs Parallel Execution

#### The Problem

CMR pagination is inherently **sequential** (each page requires the `search-after` token from the previous response), but `open()` and `download()` use **parallel execution** which benefits from knowing all URLs upfront.

```python
# Lazy results - pages fetched on demand, sequentially
results = earthaccess.search_data(short_name="ATL03", count=100000)
# results.matched() = 100,000 but only page 1 (2000 items) is fetched

# Parallel open wants all URLs upfront to distribute work
earthaccess.open(results)  # Problem: need to materialize all 100K URLs first?
```

**The tension**:
- Lazy pagination: Memory-efficient, fast first-result time
- Parallel execution: Needs work items upfront to distribute to workers
- CMR limitation: Cannot fetch page N without first fetching pages 1..N-1

#### Solution: Streaming Producer-Consumer Pattern

Instead of materializing all results upfront, use a **streaming architecture** where pagination and parallel execution happen concurrently:

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  CMR Paginator  │────▶│   Work Queue     │────▶│  Worker Pool    │
│  (Producer)     │     │   (Buffer)       │     │  (Consumers)    │
└─────────────────┘     └──────────────────┘     └─────────────────┘
         │                       │                        │
   Fetches pages            Bounded queue           Opens/downloads
   sequentially             (backpressure)          in parallel
```

#### B.3.1 Implementation: Chunked Streaming with Auth Context

```python
# earthaccess/store/streaming.py

"""
Streaming parallel execution for lazy results.

Key insight: We don't need ALL URLs upfront - we just need enough to keep
workers busy. Fetch pages in chunks, process in parallel, repeat.

Auth Strategy (per Issue #913):
- For S3: Fetch credentials ONCE, share immutable dict across all workers
- For HTTPS: Clone session per-thread to share cookies, avoid per-file auth
- Thread-local storage ensures each thread has its own session/filesystem
"""

import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from queue import Queue
from threading import Thread
from typing import Any, Callable, Dict, Iterable, List, Optional, TypeVar, Union
from urllib.parse import urlparse

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class AuthContext:
    """Immutable authentication context shared across all workers.

    Created once before parallel execution begins. Contains credentials
    and session state that can be safely shared or cloned by workers.

    For S3 (cloud-hosted):
        - s3_credentials: Dict with access_key, secret_key, token
        - Immutable, thread-safe to share directly

    For HTTPS (on-prem):
        - session_template: requests.Session with auth cookies
        - Each thread clones this to get its own session (per Issue #913)
    """
    s3_credentials: Optional[Dict[str, str]] = None
    session_template: Optional["requests.Session"] = None
    provider: Optional[str] = None
    cloud_hosted: bool = True

    @classmethod
    def for_streaming(
        cls,
        provider: Optional[str] = None,
        cloud_hosted: bool = True,
    ) -> "AuthContext":
        """Create auth context for streaming execution.

        Fetches credentials/session ONCE, to be shared by all workers.
        """
        import earthaccess

        if cloud_hosted:
            # S3: Fetch credentials once
            # These are immutable dicts, safe to share across threads
            creds = earthaccess.get_s3_credentials(provider=provider)
            return cls(
                s3_credentials=creds,
                provider=provider,
                cloud_hosted=True,
            )
        else:
            # HTTPS: Get authenticated session template
            # Workers will clone this per-thread (not per-file!)
            auth = earthaccess.get_auth()
            session = auth.get_session()
            return cls(
                session_template=session,
                provider=provider,
                cloud_hosted=False,
            )


class WorkerContext:
    """Per-thread context for workers, providing filesystem and session access.

    Each thread gets its own WorkerContext via thread-local storage.
    This ensures:
    - S3: Each thread has its own s3fs.S3FileSystem instance
    - HTTPS: Each thread has its own cloned session (not per-file!)
    - Target FS: Each thread creates its own target filesystem

    This pattern addresses Issue #913 by reusing sessions within threads,
    reducing EDL auth requests from N (per-file) to T (per-thread).

    Supports multiple filesystem backends:
    - fsspec/s3fs: Default, with configurable caching (blockcache, filecache, none)
    - obstore: Alternative high-performance S3 backend
    """

    def __init__(
        self,
        auth_context: AuthContext,
        fs_backend: str = "fsspec",
        fs_options: Optional[Dict[str, Any]] = None,
    ):
        self._auth_context = auth_context
        self._fs_backend = fs_backend
        self._fs_options = fs_options or {}
        self._source_fs: Optional[Any] = None  # Lazy-initialized
        self._session: Optional[Any] = None  # Lazy-initialized
        self._target_filesystems: Dict[str, Any] = {}  # Cache by target path
        self._obstore: Optional[Any] = None  # Lazy-initialized obstore

    def get_filesystem(self, url: str) -> "fsspec.AbstractFileSystem":
        """Get filesystem for the given URL.

        Creates filesystem once per thread, reuses for all files in that thread.

        For fsspec, supports cache configuration via fs_options:
        - cache_type: "blockcache", "filecache", "readahead", "none", "all" (default)
        - block_size: Block size for blockcache (default 8MB)
        - cache_storage: Directory for filecache
        """
        if self._source_fs is not None:
            return self._source_fs

        parsed = urlparse(url)

        if self._fs_backend == "obstore":
            # Use obstore wrapped in fsspec compatibility layer
            self._source_fs = self._create_obstore_fs()
        elif parsed.scheme == "s3" or self._auth_context.cloud_hosted:
            # S3 filesystem with shared credentials and optional caching
            self._source_fs = self._create_s3fs_with_cache()
        elif parsed.scheme in ("http", "https"):
            # HTTP filesystem with cloned session
            import fsspec
            session = self._get_cloned_session()
            self._source_fs = fsspec.filesystem(
                "https",
                client_kwargs={"session": session},
            )
        else:
            # Local filesystem
            import fsspec
            self._source_fs = fsspec.filesystem("file")

        return self._source_fs

    def _create_s3fs_with_cache(self) -> "fsspec.AbstractFileSystem":
        """Create S3 filesystem with configurable caching.

        Cache options (via fs_options):
        - cache_type: Type of cache to use
            - "none": No caching, direct reads
            - "blockcache": Cache blocks in memory (good for random access)
            - "filecache": Cache whole files to disk
            - "readahead": Read-ahead buffer (good for sequential reads)
            - "all": Cache entire file in memory (default fsspec behavior)
        - block_size: Size of blocks for blockcache (default 8MB)
        - cache_storage: Directory for filecache (default /tmp)
        """
        import s3fs
        import fsspec

        # Base S3 filesystem with credentials
        base_fs = s3fs.S3FileSystem(**self._auth_context.s3_credentials)

        cache_type = self._fs_options.get("cache_type", "all")

        if cache_type == "none":
            # No caching - direct access
            return base_fs
        elif cache_type == "blockcache":
            # Block-based caching (good for random access patterns)
            block_size = self._fs_options.get("block_size", 8 * 1024 * 1024)
            return fsspec.filesystem(
                "blockcache",
                target_protocol="s3",
                target_options=self._auth_context.s3_credentials,
                block_size=block_size,
            )
        elif cache_type == "filecache":
            # File-based caching (caches entire files to disk)
            cache_storage = self._fs_options.get("cache_storage", "/tmp/earthaccess_cache")
            return fsspec.filesystem(
                "filecache",
                target_protocol="s3",
                target_options=self._auth_context.s3_credentials,
                cache_storage=cache_storage,
                same_names=True,
            )
        elif cache_type == "readahead":
            # Read-ahead buffer (good for sequential access)
            return fsspec.filesystem(
                "simplecache",
                target_protocol="s3",
                target_options=self._auth_context.s3_credentials,
            )
        else:
            # Default: let s3fs use its default caching
            return base_fs

    def _create_obstore_fs(self) -> "fsspec.AbstractFileSystem":
        """Create obstore-backed filesystem.

        Obstore (https://github.com/developmentseed/obstore) provides a
        high-performance alternative to s3fs for S3 access. It can be
        significantly faster for some workloads.

        Requires: pip install obstore
        """
        try:
            import obstore as obs
            from obstore.fsspec import ObstoreFsspecStore
        except ImportError:
            raise ImportError(
                "obstore is required for fs_backend='obstore'. "
                "Install with: pip install obstore"
            )

        creds = self._auth_context.s3_credentials

        # Create obstore S3 store with credentials
        store = obs.store.S3Store.from_env(
            access_key_id=creds.get("key"),
            secret_access_key=creds.get("secret"),
            session_token=creds.get("token"),
            region=creds.get("region", "us-west-2"),
            **self._fs_options,
        )

        # Wrap in fsspec-compatible interface
        return ObstoreFsspecStore(store)

    def open_file(self, url: str, mode: str = "rb") -> Any:
        """Open a file, using the configured filesystem backend.

        This is the preferred method for opening files as it handles
        all backend-specific details internally.

        Args:
            url: URL to open (s3://, https://, or local path)
            mode: File mode (default "rb")

        Returns:
            File-like object (context manager)
        """
        fs = self.get_filesystem(url)
        return fs.open(url, mode)

    def _get_cloned_session(self) -> "requests.Session":
        """Clone the session template for this thread.

        Called once per thread, not once per file. This is the key
        optimization from PR #909 that addresses Issue #913.
        """
        if self._session is not None:
            return self._session

        import requests

        template = self._auth_context.session_template
        self._session = requests.Session()
        self._session.headers.update(template.headers)
        self._session.cookies.update(template.cookies)
        self._session.auth = template.auth

        return self._session

    def get_target_filesystem(
        self,
        target: str,
        target_options: Optional[Dict[str, Any]] = None,
    ) -> "fsspec.AbstractFileSystem":
        """Get or create target filesystem for downloads.

        Each worker creates its own target filesystem instance.
        Cached per target path within the thread.
        """
        cache_key = target
        if cache_key in self._target_filesystems:
            return self._target_filesystems[cache_key]

        import fsspec

        parsed = urlparse(target)
        options = target_options or {}

        if parsed.scheme == "s3":
            fs = fsspec.filesystem("s3", **options)
        elif parsed.scheme == "gs":
            fs = fsspec.filesystem("gcs", **options)
        elif parsed.scheme in ("az", "abfs"):
            fs = fsspec.filesystem("az", **options)
        else:
            fs = fsspec.filesystem("file")

        self._target_filesystems[cache_key] = fs
        return fs

    def stream_to_target(
        self,
        source_url: str,
        target_base: str,
        source_fs: "fsspec.AbstractFileSystem",
        target_fs: "fsspec.AbstractFileSystem",
        chunk_size: int = 1024 * 1024,
        **kwargs,
    ) -> str:
        """Stream file from source to target filesystem.

        Worker handles the entire transfer, writing directly to target.
        """
        from urllib.parse import urlparse
        import posixpath

        # Determine target path
        filename = source_url.split("/")[-1]
        parsed_target = urlparse(target_base)

        if parsed_target.scheme in ("s3", "gs", "az", "abfs"):
            target_path = f"{target_base.rstrip('/')}/{filename}"
        else:
            target_path = posixpath.join(target_base, filename)

        # Check if exists
        if target_fs.exists(target_path):
            return target_path

        # Stream copy
        with source_fs.open(source_url, "rb") as src:
            with target_fs.open(target_path, "wb") as dst:
                while True:
                    chunk = src.read(chunk_size)
                    if not chunk:
                        break
                    dst.write(chunk)

        return target_path


class StreamingExecutor:
    """Executor optimized for lazy, paginated inputs with proper auth handling.

    Unlike standard executors that need all items upfront, this
    streams items through a bounded buffer, enabling parallel
    processing of lazy iterables without memory blowup.

    Auth handling (per Issue #913):
    - AuthContext is created ONCE before execution
    - Each thread gets its own WorkerContext via thread-local storage
    - Sessions/credentials are shared efficiently, not recreated per-file

    Filesystem backends:
    - fsspec (default): Supports configurable caching (blockcache, filecache, etc.)
    - obstore: High-performance alternative for S3 access
    """

    def __init__(
        self,
        max_workers: int = 8,
        prefetch_pages: int = 2,
        page_size: int = 2000,
        auth_context: Optional[AuthContext] = None,
        fs_backend: str = "fsspec",
        fs_options: Optional[Dict[str, Any]] = None,
    ):
        self.max_workers = max_workers
        self.prefetch_pages = prefetch_pages
        self.page_size = page_size
        self.auth_context = auth_context
        self.fs_backend = fs_backend
        self.fs_options = fs_options or {}
        self._thread_local = threading.local()

    def _get_worker_context(self) -> WorkerContext:
        """Get or create WorkerContext for current thread."""
        if not hasattr(self._thread_local, "worker_context"):
            self._thread_local.worker_context = WorkerContext(
                self.auth_context,
                fs_backend=self.fs_backend,
                fs_options=self.fs_options,
            )
        return self._thread_local.worker_context

    def map(
        self,
        fn: Callable[[T, WorkerContext], R],
        items: Iterable[T],
        show_progress: bool = True,
        **kwargs,
    ) -> Iterator[R]:
        """Apply function to lazy iterable with streaming parallelism.

        The function receives both the item AND a WorkerContext, which
        provides thread-local filesystem and session access.
        """
        max_queue_size = self.prefetch_pages * self.page_size
        work_queue: Queue[Optional[T]] = Queue(maxsize=max_queue_size)

        # Producer: fetches pages and enqueues items
        def producer():
            try:
                for item in items:
                    work_queue.put(item)  # Blocks if queue full (backpressure)
            finally:
                # Signal end of items
                for _ in range(self.max_workers):
                    work_queue.put(None)

        producer_thread = Thread(target=producer, daemon=True)
        producer_thread.start()

        # Wrapper that provides WorkerContext to the function
        def process_with_context(item: T) -> R:
            worker_context = self._get_worker_context()
            return fn(item, worker_context)

        # Consumer: workers process items from queue
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            pending = []
            done_count = 0
            workers_done = 0

            while workers_done < self.max_workers or pending:
                # Submit work while queue has items and we have capacity
                while len(pending) < self.max_workers * 2:
                    try:
                        item = work_queue.get_nowait()
                    except:
                        break

                    if item is None:
                        workers_done += 1
                        continue

                    future = executor.submit(process_with_context, item)
                    pending.append(future)

                if not pending:
                    continue

                # Yield completed results
                done = [f for f in pending if f.done()]
                for future in done:
                    pending.remove(future)
                    done_count += 1
                    yield future.result()

    def map_batched(
        self,
        fn: Callable[[List[T], WorkerContext], List[R]],
        items: Iterable[T],
        batch_size: int = 100,
        **kwargs,
    ) -> Iterator[R]:
        """Apply function to batches from lazy iterable.

        Useful when the function benefits from batching (e.g.,
        fsspec can open multiple files more efficiently in batch).
        """
        def batch_iterator():
            batch = []
            for item in items:
                batch.append(item)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

        for result_batch in self.map(fn, batch_iterator(), **kwargs):
            yield from result_batch
```

#### B.3.2 Integration with GranuleResults

```python
# In earthaccess/results.py

class GranuleResults(ResultsBase["DataGranule"]):
    """Lazy, paginated granule search results."""

    def open(
        self,
        *,
        max_workers: int = 8,
        prefetch_pages: int = 2,
        show_progress: bool = True,
        provider: Optional[str] = None,
        fs_backend: Literal["fsspec", "obstore"] = "fsspec",
        fs_options: Optional[Dict[str, Any]] = None,
        **fsspec_kwargs,
    ) -> Iterator[fsspec.spec.AbstractBufferedFile]:
        """Open granules with streaming parallelism.

        Fetches pages lazily while opening files in parallel.
        Memory-efficient for large result sets.

        Args:
            max_workers: Parallel file openers
            prefetch_pages: Pages to buffer ahead
            show_progress: Show progress bar
            provider: Data provider (auto-detected if not specified)
            fs_backend: Filesystem backend
                - "fsspec": Use fsspec/s3fs with configurable caching (default)
                - "obstore": Use obstore for faster S3 access
            fs_options: Options for filesystem backend. For fsspec:
                - cache_type: "blockcache", "filecache", "readahead", "none", "all"
                - block_size: Block size for blockcache (default 8MB)
                - cache_storage: Directory for filecache
            **fsspec_kwargs: Additional options passed to fs.open()

        Yields:
            Open file handles as they become ready

        Example:
            results = earthaccess.search_data(short_name="ATL03", count=50000)

            # Streaming: never holds all 50K URLs in memory
            for fh in results.open(max_workers=16):
                data = xr.open_dataset(fh)
                process(data)
                fh.close()

            # With blockcache for random access patterns
            for fh in results.open(
                fs_options={"cache_type": "blockcache", "block_size": 16*1024*1024}
            ):
                data = xr.open_dataset(fh)
                process(data)
                fh.close()
        """
        from .store.streaming import StreamingExecutor

        # Prepare auth context once, share across workers
        auth_context = AuthContext.for_streaming(
            provider=provider,
            cloud_hosted=self._cloud_hosted,
        )

        executor = StreamingExecutor(
            max_workers=max_workers,
            prefetch_pages=prefetch_pages,
            auth_context=auth_context,
            fs_backend=fs_backend,
            fs_options=fs_options,
        )

        def open_one(granule: DataGranule, worker_context: WorkerContext):
            url = granule.data_links()[0]  # TODO: asset filtering
            return worker_context.open_file(url, **fsspec_kwargs)

        yield from executor.map(open_one, self, show_progress=show_progress)

    def download(
        self,
        target: str,
        *,
        max_workers: int = 8,
        prefetch_pages: int = 2,
        show_progress: bool = True,
        provider: Optional[str] = None,
        target_options: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Iterator[str]:
        """Download granules with streaming parallelism.

        Fetches pages lazily while downloading in parallel.
        Each worker writes directly to the target filesystem.

        Args:
            target: Target path (local, s3://, gs://, az://)
            max_workers: Parallel downloaders
            prefetch_pages: Pages to buffer ahead
            show_progress: Show progress bar
            provider: Data provider (auto-detected if not specified)
            target_options: fsspec options for target filesystem (credentials, etc.)

        Yields:
            Target paths as downloads complete
        """
        from .store.streaming import StreamingExecutor

        # Prepare auth context once, share across workers
        auth_context = AuthContext.for_streaming(
            provider=provider,
            cloud_hosted=self._cloud_hosted,
        )

        executor = StreamingExecutor(
            max_workers=max_workers,
            prefetch_pages=prefetch_pages,
            auth_context=auth_context,
        )

        def download_one(granule: DataGranule, worker_context: WorkerContext) -> str:
            url = granule.data_links()[0]
            # Worker creates its own target filesystem
            target_fs = worker_context.get_target_filesystem(target, target_options)
            source_fs = worker_context.get_filesystem(url)
            return worker_context.stream_to_target(
                url, target, source_fs, target_fs, **kwargs
            )

        yield from executor.map(download_one, self, show_progress=show_progress)

    def process(
        self,
        fn: Callable[["xr.Dataset"], T],
        *,
        max_workers: int = 8,
        prefetch_pages: int = 2,
        show_progress: bool = True,
        provider: Optional[str] = None,
        fs_backend: Literal["fsspec", "obstore"] = "fsspec",
        fs_options: Optional[Dict[str, Any]] = None,
        xarray_engine: str = "h5netcdf",
        xarray_options: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Iterator[T]:
        """Process granules with a function, streaming results.

        Fetches pages lazily while processing in parallel.
        Each worker opens, processes, and closes files independently.

        Args:
            fn: Function that takes an xarray Dataset and returns a result.
                The result must be serializable (numpy arrays, dicts, primitives).
            max_workers: Parallel workers
            prefetch_pages: Pages to buffer ahead
            show_progress: Show progress bar
            provider: Data provider (auto-detected if not specified)
            fs_backend: Filesystem backend to use:
                - "fsspec": Use fsspec/s3fs (default, supports caching)
                - "obstore": Use obstore for potentially faster S3 access
            fs_options: Options passed to the filesystem backend. For fsspec, this
                includes cache configuration:
                - {"cache_type": "blockcache", "block_size": 8*1024*1024}
                - {"cache_type": "filecache", "cache_storage": "/tmp/earthaccess_cache"}
                - {"cache_type": "none"} to disable caching
                For obstore, options are passed directly to the store constructor.
            xarray_engine: Engine for xr.open_dataset() (default: "h5netcdf")
            xarray_options: Additional options passed to xr.open_dataset()

        Yields:
            Results as they complete

        Example:
            results = earthaccess.search_data(short_name="MUR-JPL-L4-GLOB", count=365)

            def get_mean(ds):
                return float(ds["analysed_sst"].mean())

            # Default: uses fsspec with default caching
            for mean_sst in results.process(get_mean, max_workers=8):
                print(mean_sst)

            # With custom cache settings
            for mean_sst in results.process(
                get_mean,
                fs_options={
                    "cache_type": "blockcache",
                    "block_size": 16 * 1024 * 1024,  # 16MB blocks
                },
            ):
                print(mean_sst)

            # Using obstore for faster S3 access
            for mean_sst in results.process(
                get_mean,
                fs_backend="obstore",
            ):
                print(mean_sst)

            # Disable caching entirely
            for mean_sst in results.process(
                get_mean,
                fs_options={"cache_type": "none"},
            ):
                print(mean_sst)
        """
        from .store.streaming import StreamingExecutor

        # Prepare auth context once, share across workers
        auth_context = AuthContext.for_streaming(
            provider=provider,
            cloud_hosted=self._cloud_hosted,
        )

        executor = StreamingExecutor(
            max_workers=max_workers,
            prefetch_pages=prefetch_pages,
            auth_context=auth_context,
            fs_backend=fs_backend,
            fs_options=fs_options,
        )

        xr_opts = xarray_options or {}

        def process_one(granule: DataGranule, worker_context: WorkerContext) -> T:
            import xarray as xr
            url = granule.data_links()[0]

            with worker_context.open_file(url) as fh:
                ds = xr.open_dataset(fh, engine=xarray_engine, **xr_opts)
                result = fn(ds)
                ds.close()
            return result

        yield from executor.map(process_one, self, show_progress=show_progress)
```

#### B.3.2.1 Aligned Method Signatures

All methods on `GranuleResults` follow a consistent pattern with aligned parameters:

| Parameter | `open()` | `download()` | `process()` | Description |
|-----------|----------|--------------|-------------|-------------|
| `max_workers` | ✅ | ✅ | ✅ | Number of parallel workers |
| `prefetch_pages` | ✅ | ✅ | ✅ | Pages to buffer ahead for backpressure |
| `show_progress` | ✅ | ✅ | ✅ | Show progress bar |
| `provider` | ✅ | ✅ | ✅ | Data provider (auto-detected if not specified) |
| `fs_backend` | ✅ | - | ✅ | Filesystem backend: "fsspec" or "obstore" |
| `fs_options` | ✅ | - | ✅ | Filesystem options (cache settings, etc.) |
| `target` | - | ✅ | - | Target path for downloads |
| `target_options` | - | ✅ | - | fsspec options for target filesystem |
| `xarray_engine` | - | - | ✅ | Engine for xr.open_dataset() |
| `xarray_options` | - | - | ✅ | Options passed to xr.open_dataset() |
| `fn` | - | - | ✅ | Processing function |

**Key Design Principles:**

1. **Auth handled once**: `AuthContext` is created before parallel execution begins
2. **Thread-local workers**: Each thread gets its own `WorkerContext` with cloned sessions (per Issue #913)
3. **Workers write directly**: For downloads, each worker writes directly to target filesystem
4. **Configurable filesystem**: Support for fsspec (with caching) and obstore backends
5. **Lazy by default**: All methods return iterators, not lists

**Filesystem Cache Options (for `fs_options`):**

| `cache_type` | Description | Best For |
|--------------|-------------|----------|
| `"all"` | Cache entire file in memory (default) | Small files, repeated access |
| `"blockcache"` | Cache blocks in memory | Random access patterns, large files |
| `"filecache"` | Cache whole files to disk | Very large files, limited memory |
| `"readahead"` | Read-ahead buffer | Sequential access patterns |
| `"none"` | No caching, direct access | Streaming, one-pass reads |

#### B.3.3 Eager vs Streaming API

Provide both patterns for flexibility:

```python
# earthaccess/api.py

def open(
    granules: Union[List[DataGranule], GranuleResults],
    *,
    streaming: bool = False,  # NEW: opt-in to streaming
    max_workers: int = 8,
    **kwargs,
) -> Union[List[AbstractBufferedFile], Iterator[AbstractBufferedFile]]:
    """Open granules for reading.

    Args:
        granules: Granules to open (list or lazy results)
        streaming: If True, return iterator (memory-efficient for large sets).
                   If False, return list (current behavior, backwards compatible).
        max_workers: Parallel workers

    Returns:
        List of file handles (streaming=False) or iterator (streaming=True)

    Examples:
        # Current behavior (eager, returns list)
        files = earthaccess.open(granules)

        # New streaming mode (lazy, memory-efficient)
        for fh in earthaccess.open(results, streaming=True):
            process(fh)
    """
    if isinstance(granules, GranuleResults):
        if streaming:
            # Streaming mode: return iterator directly
            return granules.open(max_workers=max_workers, **kwargs)
        else:
            # Eager mode: materialize all first, then parallel open
            # This is backwards compatible but uses more memory
            granule_list = granules.get_all()
            return _open_list(granule_list, max_workers=max_workers, **kwargs)
    else:
        # Already a list - use current parallel approach
        return _open_list(granules, max_workers=max_workers, **kwargs)
```

#### B.3.4 Memory Characteristics

| Mode | Memory Usage | First Result | Use Case |
|------|--------------|--------------|----------|
| **Eager** (`streaming=False`) | O(N) - all URLs in memory | After all pages fetched | Small result sets, need random access |
| **Streaming** (`streaming=True`) | O(prefetch_pages × page_size) | After first page | Large result sets, sequential processing |

Example memory comparison for 100,000 granules:

| Mode | Approximate Memory |
|------|-------------------|
| Eager | ~500 MB (all DataGranule objects) |
| Streaming (2 pages) | ~20 MB (4,000 granules buffered) |

#### B.3.5 Distributed Execution Considerations

For Dask/Lithops/Ray, the streaming pattern needs adaptation:

```python
class DaskStreamingExecutor:
    """Streaming executor for Dask distributed.

    Strategy: Submit pages as Dask bags, compute incrementally.
    """

    def map(
        self,
        fn: Callable[[T], R],
        items: Iterable[T],
        page_size: int = 2000,
    ) -> Iterator[R]:
        import dask.bag as db

        # Process page by page to avoid memory bloat
        if hasattr(items, 'pages'):
            # GranuleResults has .pages() method
            for page in items.pages(page_size=page_size):
                bag = db.from_sequence(page, npartitions=self.max_workers)
                results = bag.map(fn).compute()
                yield from results
        else:
            # Fall back to chunking
            chunk = []
            for item in items:
                chunk.append(item)
                if len(chunk) >= page_size:
                    bag = db.from_sequence(chunk, npartitions=self.max_workers)
                    yield from bag.map(fn).compute()
                    chunk = []
            if chunk:
                bag = db.from_sequence(chunk, npartitions=self.max_workers)
                yield from bag.map(fn).compute()
```

#### B.3.6 Implementation Tasks

| Task | Priority | Complexity |
|------|----------|------------|
| Implement `stream_parallel()` function | High | Medium |
| Implement `StreamingExecutor` class | High | Medium |
| Add `open()` method to `GranuleResults` | High | Low |
| Add `download()` method to `GranuleResults` | High | Low |
| Add `streaming` parameter to `earthaccess.open()` | High | Low |
| Add `streaming` parameter to `earthaccess.download()` | High | Low |
| Implement `DaskStreamingExecutor` | Medium | Medium |
| Add progress bar for streaming mode | Medium | Low |
| Add memory usage warnings for eager mode with large N | Low | Low |

#### B.3.7 xarray Integration Patterns

The primary downstream use case for `earthaccess.open()` is xarray. Understanding xarray's requirements helps us design the right abstractions.

##### The `open_mfdataset()` Challenge

```python
# What users want:
results = earthaccess.search_data(short_name="ATL03", count=10000)
ds = xr.open_mfdataset(earthaccess.open(results))

# Problem: open_mfdataset() is fundamentally EAGER
# It needs ALL file handles upfront to:
# 1. Inspect dimensions/coordinates across files
# 2. Build combined dask graph
```

**Key insight**: `open_mfdataset()` doesn't actually *read* all data upfront - it builds a lazy dask graph. The memory concern is the **file handles and metadata**, not the data itself.

##### Memory Analysis: What Actually Matters?

| Component | Memory per file | 10K files |
|-----------|-----------------|-----------|
| `DataGranule` object | ~5 KB | 50 MB |
| fsspec file handle | ~2 KB | 20 MB |
| xarray metadata (coords, attrs) | ~10-50 KB | 100-500 MB |
| **Total metadata overhead** | | **170-570 MB** |
| Actual data (if loaded) | MBs-GBs each | TBs |

The metadata overhead for 10K files is manageable on most systems. The streaming pattern is most valuable when:
1. You have **100K+ granules**
2. You're **not using open_mfdataset** (processing files individually)
3. You're in a **memory-constrained environment** (serverless, small VMs)

##### Pattern 1: Direct `open_mfdataset()` (Recommended for <10K files)

For typical use cases, eager opening + `open_mfdataset()` is fine:

```python
# Simple, clean, works well for most cases
results = earthaccess.search_data(short_name="MUR-JPL-L4-GLOB", count=365)
files = earthaccess.open(results)  # Eager: opens all 365 files
ds = xr.open_mfdataset(files, engine="h5netcdf", parallel=True)

# xarray handles the parallelism internally via dask
# Data is lazy - only metadata loaded upfront
```

##### Pattern 2: Batched `open_mfdataset()` (For large result sets)

For very large result sets, process in batches:

```python
# earthaccess/xarray.py

def open_mfdataset_batched(
    results: GranuleResults,
    batch_size: int = 500,
    concat_dim: str = "time",
    **xr_kwargs,
) -> xr.Dataset:
    """Open large result sets in batches, then combine.

    More memory-efficient than opening all files at once for very
    large (10K+) result sets.

    Args:
        results: Lazy granule results
        batch_size: Files per batch (tune based on available memory)
        concat_dim: Dimension to concatenate along
        **xr_kwargs: Passed to xr.open_mfdataset()

    Returns:
        Combined xarray Dataset (lazy)
    """
    datasets = []

    for page in results.pages(page_size=batch_size):
        # Open this batch
        files = earthaccess.open(page)

        # Create dataset for this batch
        ds_batch = xr.open_mfdataset(
            files,
            concat_dim=concat_dim,
            combine="nested",
            **xr_kwargs,
        )
        datasets.append(ds_batch)

    # Combine all batches (still lazy)
    return xr.concat(datasets, dim=concat_dim)


# Usage:
results = earthaccess.search_data(short_name="ATL03", count=50000)
ds = earthaccess.open_mfdataset_batched(results, batch_size=1000)
```

##### Pattern 3: Streaming Individual Files (For sequential processing)

When you don't need `open_mfdataset()` - e.g., processing files independently:

```python
results = earthaccess.search_data(short_name="ATL03", count=100000)

# Streaming: memory-efficient, processes one at a time
for fh in earthaccess.open(results, streaming=True):
    ds = xr.open_dataset(fh, engine="h5netcdf")
    result = compute_something(ds)
    save_result(result)
    ds.close()
    fh.close()  # Release memory
```

##### Pattern 4: Parallel Processing with Dask (Best of both worlds)

Use dask for both pagination AND processing:

```python
# earthaccess/xarray.py

def open_mfdataset_distributed(
    results: GranuleResults,
    client: "dask.distributed.Client",
    preprocess: Optional[Callable] = None,
    **xr_kwargs,
) -> xr.Dataset:
    """Open files using Dask distributed for maximum parallelism.

    Distributes both file opening AND xarray operations across workers.

    Args:
        results: Lazy granule results
        client: Dask distributed client
        preprocess: Optional function to apply to each dataset
        **xr_kwargs: Passed to xr.open_dataset()
    """
    import dask
    from dask import delayed

    @delayed
    def open_one(granule):
        url = granule.data_links()[0]
        fs = earthaccess.get_fsspec_session()
        fh = fs.open(url)
        ds = xr.open_dataset(fh, **xr_kwargs)
        if preprocess:
            ds = preprocess(ds)
        return ds

    # Build lazy graph for all files
    # Note: This iterates through results (fetches all pages)
    # but doesn't actually open files until compute()
    delayed_datasets = [open_one(g) for g in results]

    # Combine lazily
    @delayed
    def combine(datasets):
        return xr.concat(datasets, dim="time")

    return combine(delayed_datasets)


# Usage with Dask distributed:
from dask.distributed import Client

client = Client(n_workers=8)
results = earthaccess.search_data(short_name="ATL03", count=10000)

# Files opened in parallel across workers
ds = earthaccess.open_mfdataset_distributed(results, client)
result = ds.mean(dim="time").compute()  # Distributed computation
```

##### Pattern 5: VirtualiZarr / Kerchunk (Zero-copy reference)

For cloud-optimized workflows, avoid opening files entirely:

```python
# Using kerchunk/VirtualiZarr to create reference files
results = earthaccess.search_data(short_name="MUR-JPL-L4-GLOB", count=365)

# Generate kerchunk references (reads only metadata)
refs = earthaccess.create_kerchunk_refs(results)

# Open via reference - no file handles needed
ds = xr.open_dataset(
    "reference://",
    engine="zarr",
    backend_kwargs={"consolidated": False, "storage_options": {"fo": refs}},
)
```

##### Recommendation Matrix

| Scenario | Files | Approach | Why |
|----------|-------|----------|-----|
| Typical analysis | <1K | `open()` + `open_mfdataset()` | Simple, efficient enough |
| Large time series | 1K-10K | `open()` + `open_mfdataset(parallel=True)` | xarray parallelizes internally |
| Very large | 10K-100K | `open_mfdataset_batched()` | Bounded memory |
| Huge / distributed | 100K+ | `open_mfdataset_distributed()` | Dask handles scale |
| Cloud-optimized | Any | VirtualiZarr / Kerchunk | Best performance |
| Sequential processing | Any | `open(streaming=True)` | Memory efficient |

##### Default Behavior: Smart Batching

To hide complexity, `earthaccess.open()` can automatically choose the right strategy:

```python
def open(
    granules: Union[List[DataGranule], GranuleResults],
    *,
    auto_batch: bool = True,  # NEW
    batch_threshold: int = 5000,  # Files above this trigger batching
    batch_size: int = 500,
    **kwargs,
) -> List[AbstractBufferedFile]:
    """Open granules with automatic batching for large result sets.

    When auto_batch=True (default):
    - <5000 files: Opens all at once (simple, fast)
    - >=5000 files: Opens in batches to manage memory

    Users don't need to think about it - just works.
    """
    if isinstance(granules, GranuleResults):
        total = granules.matched()

        if auto_batch and total >= batch_threshold:
            # Large result set: batch automatically
            logger.info(
                f"Opening {total} files in batches of {batch_size} "
                f"to manage memory"
            )
            return _open_batched(granules, batch_size, **kwargs)
        else:
            # Small enough: open all at once
            return _open_eager(granules.get_all(), **kwargs)
    else:
        return _open_eager(granules, **kwargs)
```

##### Implementation Tasks (xarray Integration)

| Task | Priority | Complexity |
|------|----------|------------|
| Implement `open_mfdataset_batched()` | High | Medium |
| Add `auto_batch` parameter to `open()` | High | Low |
| Implement `open_mfdataset_distributed()` | Medium | Medium |
| Add xarray integration docs/examples | High | Low |
| Benchmark memory usage for various file counts | Medium | Medium |

#### B.3.8 Distributed Execution: Lithops, Ray, and File Handles

##### The Authentication and Serialization Problem

**Two fundamental limitations** for distributed execution:

1. **File handles cannot be serialized**:
```python
# This will FAIL with Lithops/Ray:
@ray.remote
def open_file(granule):
    fh = fs.open(granule.data_links()[0])
    return fh  # ❌ Cannot pickle/serialize open file handle

# The file handle lives in the worker's memory
# It cannot be transmitted back to the driver process
```

2. **Authentication must be coordinated to avoid server saturation**:

This is a critical issue that has already impacted NASA's services. See [Issue #913](https://github.com/nsidc/earthaccess/issues/913) where earthaccess was creating new sessions for each download, overwhelming EDL servers.

```python
# This will OVERWHELM NASA's EDL servers:
@ray.remote
def worker_that_auths(granule):
    # ❌ Each worker hits EDL independently!
    earthaccess.login()  # 100 workers = 100 EDL requests
    return process(granule)

# Better: Pre-fetch and ship credentials
credentials = earthaccess.get_s3_credentials(provider="POCLOUD")  # ✅ ONE request

@ray.remote
def worker_with_shipped_creds(granule, credentials):
    fs = s3fs.S3FileSystem(**credentials)  # ✅ No new auth request
    return process(granule, fs)
```

**For HTTPS downloads** (on-prem data), the current implementation uses thread-local session cloning ([PR #909](https://github.com/nsidc/earthaccess/pull/909)) to reuse authentication cookies within each thread, reducing EDL requests from N (one per file) to T (one per thread).

**For S3 cloud access** in distributed environments, we use credential shipping: fetch S3 credentials once and pass them to all workers.

This means `earthaccess.open()` **cannot return file handles** when using distributed executors, AND **authentication must be coordinated** to prevent saturating NASA's servers.

##### Credential Shipping Pattern

The solution is simple: **fetch credentials once, ship to all workers**:

```python
# ✅ RECOMMENDED: One fetch, share with all workers
def process_batch_distributed(granules):
    # Step 1: Fetch credentials ONCE
    provider = granules[0]["meta"]["provider-id"]
    credentials = earthaccess.get_s3_credentials(provider=provider)

    # Step 2: Convert granules to serializable format
    granule_dicts = [g.to_dict() for g in granules]

    # Step 3: Distribute with credentials
    futures = [process_granule.remote(g, credentials) for g in granule_dicts]
    results = ray.get(futures)
    return results

# Each worker receives same credentials
@ray.remote
def process_granule(granule_dict, credentials):
    # Worker reconstructs filesystem locally
    fs = s3fs.S3FileSystem(**credentials)

    # Open, process, close entirely on worker
    url = granule_dict["s3_links"][0]
    with fs.open(url) as fh:
        ds = xr.open_dataset(fh)
        result = ds.mean().values
        ds.close()

    return result
```

**Key advantages of credential shipping:**
- **Single EDL request** per provider (prevents server saturation)
- **Works with all backends** (Ray, Lithops, Dask, HPC)
- **Credentials are just JSON** - fully serializable
- **No session cloning needed** - S3 tokens are self-contained
- **~55 minute usable TTL** - sufficient for most batches

##### Handling Long-Running Jobs

For jobs exceeding the ~55 minute credential window:

```python
def process_long_job(granules, batch_size=50):
    """Process in batches to refresh credentials as needed."""
    results = []

    for i in range(0, len(granules), batch_size):
        batch = granules[i:i+batch_size]

        # Fresh credentials for each batch
        provider = batch[0]["meta"]["provider-id"]
        credentials = earthaccess.get_s3_credentials(provider=provider)

        # Process batch
        batch_results = process_batch_distributed(batch)
        results.extend(batch_results)

        # Brief pause between batches (optional, be good citizen)
        if i + batch_size < len(granules):
            time.sleep(5)

    return results
```

**Benefits of batching:**
- Automatic credential refresh
- Natural checkpoint points
- Limits concurrent EDL requests
- Works with any job duration

##### What Works vs What Doesn't

| Operation | ThreadPool | Dask Local | Dask Distributed | Lithops | Ray |
|-----------|------------|------------|------------------|---------|-----|
| `open()` → return file handles | ✅ | ✅ | ❌ | ❌ | ❌ |
| `download()` → return paths | ✅ | ✅ | ✅ | ✅ | ✅ |
| Process data on workers | ✅ | ✅ | ✅ | ✅ | ✅ |
| Return processed results | ✅ | ✅ | ✅ | ✅ | ✅ |

##### Pattern: Do Everything on Workers

For Lithops/Ray, the pattern must be: **open, process, and close on the worker**:

```python
# ✅ CORRECT: Process entirely on worker, return serializable result
@ray.remote
def process_granule(granule_dict, credentials):
    """Runs entirely on remote worker."""
    import xarray as xr
    import s3fs

    # Reconstruct filesystem on worker
    fs = s3fs.S3FileSystem(**credentials)

    # Open, process, close - all on worker
    url = granule_dict["data_links"][0]
    with fs.open(url) as fh:
        ds = xr.open_dataset(fh, engine="h5netcdf")
        result = ds.mean(dim="time").values  # Compute to numpy array
        ds.close()

    return result  # ✅ numpy array IS serializable


# Usage
results = earthaccess.search_data(short_name="ATL03", count=1000)
credentials = earthaccess.get_s3_credentials(provider="NSIDC_CPRD")

# Convert granules to serializable dicts
granule_dicts = [g.to_dict() for g in results]

# Distribute processing
futures = [process_granule.remote(g, credentials) for g in granule_dicts]
processed_results = ray.get(futures)
```

##### Handling HTTPS (On-Prem) Downloads in Distributed Mode

For on-prem data accessed via HTTPS, we need to ship session state (cookies/headers) to workers. This extends the pattern from [PR #909](https://github.com/nsidc/earthaccess/pull/909):

```python
# ✅ HTTPS downloads in distributed mode: Ship session state
def get_distributed_https_session(auth) -> dict:
    """Extract serializable session state for distributed workers."""
    session = auth.get_session()
    return {
        "headers": dict(session.headers),
        "cookies": requests.utils.dict_from_cookiejar(session.cookies),
        "auth": (auth.username, auth.password) if auth.username else None,
    }

@ray.remote
def download_granule_https(granule_dict, session_state):
    """Download via HTTPS on remote worker."""
    import requests

    # Reconstruct session from shipped state
    session = requests.Session()
    session.headers.update(session_state["headers"])
    session.cookies.update(session_state["cookies"])
    if session_state["auth"]:
        session.auth = session_state["auth"]

    url = granule_dict["data_links"][0]
    response = session.get(url, stream=True)
    # ... download logic
    return local_path

# Usage
session_state = get_distributed_https_session(earthaccess.auth)
futures = [download_granule_https.remote(g, session_state) for g in granule_dicts]
```

**Key difference from S3:**
- S3: Ship credentials dict → reconstruct `s3fs.S3FileSystem`
- HTTPS: Ship session state (cookies/headers) → reconstruct `requests.Session`

Both patterns avoid repeated authentication requests to EDL.

##### Lithops-Specific Implementation

```python
# earthaccess/store/lithops_backend.py

"""
Lithops backend for serverless distributed processing.

Key insight: With serverless, we can't return file handles.
Instead, we provide a map function that processes data on workers.
"""

from typing import Any, Callable, Dict, List, TypeVar

T = TypeVar("T")
R = TypeVar("R")


class LithopsProcessor:
    """Process granules using Lithops serverless functions.

    Unlike local execution, Lithops workers are stateless and remote.
    File handles cannot be returned - all processing must happen on workers.
    """

    def __init__(
        self,
        config: Dict[str, Any] = None,
        **lithops_kwargs,
    ):
        import lithops
        self.executor = lithops.FunctionExecutor(config=config, **lithops_kwargs)

    def map(
        self,
        process_fn: Callable[[Dict, Dict], R],
        granules: List["DataGranule"],
        credentials: Dict[str, str],
        **kwargs,
    ) -> List[R]:
        """Apply function to granules on serverless workers.

        The process_fn receives:
        - granule_dict: Serializable dict representation of granule
        - credentials: S3 credentials dict

        And must return a serializable result.

        Args:
            process_fn: Function(granule_dict, credentials) -> result
            granules: List of DataGranule objects
            credentials: S3 credentials from earthaccess.get_s3_credentials()

        Returns:
            List of results from each worker

        Example:
            def compute_mean(granule_dict, creds):
                import xarray as xr
                import s3fs

                fs = s3fs.S3FileSystem(**creds)
                url = granule_dict["meta"]["concept-id"]  # or data_links

                with fs.open(url) as fh:
                    ds = xr.open_dataset(fh)
                    return float(ds["temperature"].mean())

            processor = LithopsProcessor()
            means = processor.map(compute_mean, granules, credentials)
        """
        # Convert to serializable format
        granule_dicts = [self._granule_to_dict(g) for g in granules]

        # Create wrapper that unpacks arguments
        def worker(granule_dict):
            return process_fn(granule_dict, credentials)

        # Execute on Lithops
        futures = self.executor.map(worker, granule_dicts)
        results = self.executor.get_result(futures)

        return results

    def map_reduce(
        self,
        map_fn: Callable[[Dict, Dict], R],
        reduce_fn: Callable[[List[R]], Any],
        granules: List["DataGranule"],
        credentials: Dict[str, str],
    ) -> Any:
        """Map-reduce pattern for aggregating results.

        Example:
            def extract_values(g, creds):
                # ... open and extract data
                return values_array

            def combine(arrays):
                return np.concatenate(arrays).mean()

            result = processor.map_reduce(extract_values, combine, granules, creds)
        """
        mapped = self.map(map_fn, granules, credentials)
        return reduce_fn(mapped)

    @staticmethod
    def _granule_to_dict(granule: "DataGranule") -> Dict[str, Any]:
        """Convert DataGranule to serializable dict."""
        return {
            "meta": dict(granule.get("meta", {})),
            "umm": dict(granule.get("umm", {})),
            "data_links": granule.data_links(),
            "s3_links": granule.data_links(access="direct"),
        }


class RayProcessor:
    """Process granules using Ray distributed computing."""

    def __init__(self, **ray_kwargs):
        import ray
        if not ray.is_initialized():
            ray.init(**ray_kwargs)
        self._ray = ray

    def map(
        self,
        process_fn: Callable[[Dict, Dict], R],
        granules: List["DataGranule"],
        credentials: Dict[str, str],
        num_cpus: int = 1,
    ) -> List[R]:
        """Apply function to granules on Ray workers.

        Similar interface to LithopsProcessor.map()
        """
        # Put credentials in object store (shared across workers)
        creds_ref = self._ray.put(credentials)

        @self._ray.remote(num_cpus=num_cpus)
        def worker(granule_dict, creds_ref):
            creds = self._ray.get(creds_ref)
            return process_fn(granule_dict, creds)

        granule_dicts = [self._granule_to_dict(g) for g in granules]
        futures = [worker.remote(g, creds_ref) for g in granule_dicts]

        return self._ray.get(futures)

    @staticmethod
    def _granule_to_dict(granule):
        return LithopsProcessor._granule_to_dict(granule)
```

##### High-Level API: `earthaccess.process()`

Since `open()` can't work with distributed backends, provide a dedicated function:

```python
# earthaccess/api.py

def process(
    granules: Union[List[DataGranule], GranuleResults],
    fn: Callable[[xr.Dataset], T],
    *,
    backend: Literal["local", "dask", "lithops", "ray"] = "local",
    streaming: bool = True,
    max_workers: int = 8,
    prefetch_pages: int = 2,
    provider: Optional[str] = None,
    fs_backend: Literal["fsspec", "obstore"] = "fsspec",
    fs_options: Optional[Dict[str, Any]] = None,
    xarray_engine: str = "h5netcdf",
    xarray_options: Optional[Dict[str, Any]] = None,
    credentials: Optional[Dict] = None,
    **backend_kwargs,
) -> Union[Iterator[T], List[T]]:
    """Process granules with a function, optionally distributed.

    For local backend with streaming=True (default), this uses lazy pagination
    with the producer-consumer pattern for memory-efficient processing.

    For distributed backends (dask, lithops, ray), results are materialized
    upfront as these backends need to distribute work across nodes.

    Args:
        granules: Granules to process (list or lazy GranuleResults)
        fn: Function that takes an xarray Dataset and returns a result.
            The result must be serializable (numpy arrays, dicts, primitives).
        backend: Execution backend
            - "local": StreamingExecutor with ThreadPoolExecutor (default)
            - "dask": Dask distributed (materializes all granules first)
            - "lithops": Lithops serverless (materializes all granules first)
            - "ray": Ray distributed (materializes all granules first)
        streaming: If True and backend="local", return iterator (memory-efficient).
            If False, return list. Ignored for distributed backends.
        max_workers: Parallel workers (for local backend)
        prefetch_pages: Pages to buffer ahead (for local streaming)
        provider: Data provider (auto-detected if not specified)
        fs_backend: Filesystem backend
            - "fsspec": Use fsspec/s3fs with configurable caching (default)
            - "obstore": Use obstore for faster S3 access
        fs_options: Options for filesystem backend. For fsspec:
            - cache_type: "blockcache", "filecache", "readahead", "none", "all"
            - block_size: Block size for blockcache (default 8MB)
            - cache_storage: Directory for filecache
        xarray_engine: Engine for xr.open_dataset() (default: "h5netcdf")
        xarray_options: Additional options passed to xr.open_dataset()
        credentials: S3 credentials (auto-fetched if not provided)
        **backend_kwargs: Backend-specific options

    Returns:
        Iterator[T] if streaming=True and backend="local", else List[T]

    Examples:
        # Streaming local processing (default, memory-efficient)
        for result in earthaccess.process(results, get_mean):
            print(result)

        # Eager local processing (returns list)
        results = earthaccess.process(granules, get_mean, streaming=False)

        # With custom cache settings
        for result in earthaccess.process(
            results,
            get_mean,
            fs_options={"cache_type": "blockcache", "block_size": 16*1024*1024},
        ):
            print(result)

        # Using obstore for faster S3 access
        for result in earthaccess.process(results, get_mean, fs_backend="obstore"):
            print(result)

        # Distributed with Ray (materializes granules first)
        results = earthaccess.process(
            granules,
            get_mean,
            backend="ray",
            num_cpus=2,
        )

        # Serverless with Lithops
        results = earthaccess.process(
            granules,
            get_mean,
            backend="lithops",
            config={"lithops": {"backend": "aws_lambda"}},
        )
    """
    if backend == "local":
        # Use streaming executor for memory-efficient processing
        if isinstance(granules, GranuleResults):
            # Use the GranuleResults.process() method which handles streaming
            iterator = granules.process(
                fn,
                max_workers=max_workers,
                prefetch_pages=prefetch_pages,
                provider=provider,
                fs_backend=fs_backend,
                fs_options=fs_options,
                xarray_engine=xarray_engine,
                xarray_options=xarray_options,
            )
            if streaming:
                return iterator
            else:
                return list(iterator)
        else:
            # List of granules - wrap in simple iterator
            return _process_local_list(
                granules, fn,
                max_workers=max_workers,
                fs_backend=fs_backend,
                fs_options=fs_options,
                xarray_engine=xarray_engine,
                xarray_options=xarray_options,
                **backend_kwargs,
            )

    # Distributed backends: must materialize granules first
    if isinstance(granules, GranuleResults):
        logger.info(
            f"Materializing {granules.matched()} granules for distributed backend '{backend}'. "
            f"For memory-efficient processing, use backend='local' with streaming=True."
        )
        granules = granules.get_all()

    # Auto-fetch credentials if needed (fetches ONCE, ships to all workers)
    if credentials is None:
        auth_context = get_distributed_credentials(granules)
        credentials = auth_context["credentials"]
        logger.info(f"Fetched credentials for {auth_context['provider']} - shipping to {len(granules)} workers")

    # Wrap function to handle file opening with specified options
    wrapped_fn = _wrap_fn(fn, fs_backend, fs_options, xarray_engine, xarray_options)

    if backend == "dask":
        return _process_dask(granules, wrapped_fn, credentials, **backend_kwargs)
    elif backend == "lithops":
        processor = LithopsProcessor(**backend_kwargs)
        return processor.map(wrapped_fn, granules, credentials)
    elif backend == "ray":
        processor = RayProcessor(**backend_kwargs)
        return processor.map(wrapped_fn, granules, credentials)
    else:
        raise ValueError(f"Unknown backend: {backend}")


def _process_local_list(
    granules: List[DataGranule],
    fn: Callable[[xr.Dataset], T],
    max_workers: int = 8,
    fs_backend: str = "fsspec",
    fs_options: Optional[Dict[str, Any]] = None,
    xarray_engine: str = "h5netcdf",
    xarray_options: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> List[T]:
    """Process a list of granules locally with thread pool."""
    from .store.streaming import AuthContext, WorkerContext, StreamingExecutor

    # Detect provider from first granule
    provider = granules[0]["meta"]["provider-id"] if granules else None
    cloud_hosted = granules[0].cloud_hosted if granules else True

    auth_context = AuthContext.for_streaming(
        provider=provider,
        cloud_hosted=cloud_hosted,
    )

    executor = StreamingExecutor(
        max_workers=max_workers,
        auth_context=auth_context,
        fs_backend=fs_backend,
        fs_options=fs_options,
    )

    xr_opts = xarray_options or {}

    def process_one(granule: DataGranule, worker_context: WorkerContext) -> T:
        import xarray as xr
        url = granule.data_links()[0]
        with worker_context.open_file(url) as fh:
            ds = xr.open_dataset(fh, engine=xarray_engine, **xr_opts)
            result = fn(ds)
            ds.close()
        return result

    return list(executor.map(process_one, granules))


def get_distributed_credentials(granules) -> Dict[str, Any]:
    """Get credentials optimized for distributed execution.

    Fetches credentials ONCE per provider and returns in a format
    suitable for shipping to distributed workers.

    Args:
        granules: List of granules to process

    Returns:
        Dictionary with credentials and provider info

    Example:
        >>> creds = get_distributed_credentials(granules)
        >>> # Ship to workers
        >>> results = processor.map(process_fn, granules, creds["credentials"])
    """
    if not granules:
        raise ValueError("No granules provided")

    # Extract provider from first granule (assume all same provider)
    provider = granules[0]["meta"]["provider-id"]

    # Fetch credentials ONCE
    credentials = earthaccess.get_s3_credentials(provider=provider)

    return {
        "credentials": credentials,
        "provider": provider,
        "fetched_at": datetime.now().isoformat(),
        "expires_at": (datetime.now() + timedelta(hours=1)).isoformat(),
    }


def _wrap_fn(
    fn: Callable,
    fs_backend: str = "fsspec",
    fs_options: Optional[Dict[str, Any]] = None,
    xarray_engine: str = "h5netcdf",
    xarray_options: Optional[Dict[str, Any]] = None,
):
    """Wrap user function to handle file opening with configurable backend."""
    xr_opts = xarray_options or {}

    def wrapped(granule_dict, credentials):
        import xarray as xr

        url = granule_dict["s3_links"][0]

        if fs_backend == "obstore":
            try:
                import obstore as obs
                from obstore.fsspec import ObstoreFsspecStore
            except ImportError:
                raise ImportError(
                    "obstore required for fs_backend='obstore'. "
                    "Install with: pip install obstore"
                )
            store = obs.store.S3Store.from_env(
                access_key_id=credentials.get("key"),
                secret_access_key=credentials.get("secret"),
                session_token=credentials.get("token"),
                region=credentials.get("region", "us-west-2"),
                **(fs_options or {}),
            )
            fs = ObstoreFsspecStore(store)
        else:
            # fsspec with optional caching
            import s3fs
            import fsspec

            cache_type = (fs_options or {}).get("cache_type", "all")

            if cache_type == "none":
                fs = s3fs.S3FileSystem(**credentials)
            elif cache_type == "blockcache":
                block_size = (fs_options or {}).get("block_size", 8 * 1024 * 1024)
                fs = fsspec.filesystem(
                    "blockcache",
                    target_protocol="s3",
                    target_options=credentials,
                    block_size=block_size,
                )
            elif cache_type == "filecache":
                cache_storage = (fs_options or {}).get("cache_storage", "/tmp/earthaccess_cache")
                fs = fsspec.filesystem(
                    "filecache",
                    target_protocol="s3",
                    target_options=credentials,
                    cache_storage=cache_storage,
                    same_names=True,
                )
            else:
                fs = s3fs.S3FileSystem(**credentials)

        with fs.open(url) as fh:
            ds = xr.open_dataset(fh, engine=xarray_engine, **xr_opts)
            result = fn(ds)
            ds.close()

        return result

    return wrapped
```

##### When to Use What

| Goal | Backend | Function |
|------|---------|----------|
| Get file handles for local use | `local`/`threads` | `earthaccess.open()` |
| Download to local disk | Any | `earthaccess.download()` |
| Download to cloud storage | Any | `earthaccess.download(target="gs://...")` |
| Process and return results | Any | `earthaccess.process()` |
| Build xarray Dataset | `local`/`dask` | `earthaccess.open()` + `xr.open_mfdataset()` |

##### Authentication Strategy Summary

**For distributed execution, earthaccess uses credential shipping:**

1. **Fetch ONCE**: `earthaccess.get_s3_credentials()` called once per provider
2. **Ship to workers**: Credentials passed as serializable dict to all workers
3. **Reconstruct locally**: Each worker creates `s3fs.S3FileSystem(**credentials)`
4. **No session cloning**: S3 tokens are self-contained, no session state needed
5. **TTL management**: ~55 minute usable window, batch long jobs as needed

**For HTTPS downloads (on-prem), earthaccess uses thread-local session cloning:**

This pattern was implemented in [PR #909](https://github.com/nsidc/earthaccess/pull/909) to address [Issue #913](https://github.com/nsidc/earthaccess/issues/913):

1. **Clone session per thread**: `_clone_session_in_local_thread()` copies cookies/headers
2. **Reuse within thread**: All downloads in a thread share the same authenticated session
3. **Reduces EDL requests**: From N (one per file) to T (one per thread)

**This approach:**
- ✅ Prevents EDL server saturation (addresses real production issue #913)
- ✅ Works with all distributed backends (Ray, Lithops, Dask, HPC)
- ✅ Simplifies worker code (no auth complexity)
- ✅ Handles long-running jobs (via batching)
- ✅ Leverages existing thread-local session pattern for HTTPS

**Alternative approaches considered and rejected:**
- Per-worker auth: Risks server overload/bans (proven in issue #913)
- Shared credential stores: Over-engineering for most use cases
- Full session serialization: Complex and unnecessary for S3 tokens

##### Error Handling: Clear Messages

```python
def open(granules, *, parallel: ParallelBackend = "pqdm", **kwargs):
    """..."""
    if parallel in ("lithops", "ray"):
        raise ValueError(
            f"Cannot use parallel='{parallel}' with open() because file handles "
            f"cannot be serialized to remote workers.\n\n"
            f"Options:\n"
            f"  1. Use earthaccess.process() to run computations on workers\n"
            f"  2. Use earthaccess.download() to download files first\n"
            f"  3. Use parallel='threads' or 'dask' for local parallel opening"
        )
    # ... rest of implementation
```

##### Implementation Tasks (Distributed Execution)

| Task | Priority | Complexity |
|------|----------|------------|
| Implement `AuthContext` class | High | Low |
| Implement `WorkerContext` class with fs_backend support | High | Medium |
| Implement `StreamingExecutor` with auth_context | High | Medium |
| Implement `GranuleResults.process()` method | High | Medium |
| Implement `earthaccess.process()` function with streaming | High | Medium |
| Add obstore filesystem backend support | Medium | Medium |
| Add fsspec cache configuration (blockcache, filecache, etc.) | High | Low |
| Implement `LithopsProcessor` class | Medium | Medium |
| Implement `RayProcessor` class | Medium | Medium |
| Implement `get_distributed_credentials()` for S3 | High | Low |
| Implement `get_distributed_https_session()` for HTTPS | High | Low |
| Add credential/session TTL documentation and warnings | High | Low |
| Add clear error message for open() + distributed | High | Low |
| Add `DataGranule.to_dict()` method | High | Low |
| Document credential/session shipping patterns | High | Low |
| Document batch processing for long jobs | Medium | Low |
| Reference issue #913 in auth documentation | Medium | Low |

#### B.3.9 Real-World Examples

##### Example 1: Sea Surface Temperature (SST) Time Series

**Use case**: Compute monthly mean SST for a region from MUR-JPL-L4-GLOB dataset.

```python
import earthaccess
import numpy as np

# Authenticate
earthaccess.login()

# Search for 1 year of daily SST data (365 granules)
results = earthaccess.search_data(
    short_name="MUR-JPL-L4-GLOB-v4.1",
    temporal=("2023-01-01", "2023-12-31"),
    bounding_box=(-125, 35, -120, 40),  # California coast
)
print(f"Found {len(results)} granules")

# Define processing function
def extract_regional_sst(ds):
    """Extract mean SST for the region."""
    # Select variable and compute regional mean
    sst = ds["analysed_sst"].sel(
        lat=slice(35, 40),
        lon=slice(-125, -120),
    )
    return {
        "time": str(ds.time.values[0]),
        "mean_sst": float(sst.mean().values),
        "std_sst": float(sst.std().values),
        "min_sst": float(sst.min().values),
        "max_sst": float(sst.max().values),
    }

# Process locally (good for <1000 granules)
daily_stats = earthaccess.process(results, extract_regional_sst)

# Convert to DataFrame for analysis
import pandas as pd
df = pd.DataFrame(daily_stats)
df["time"] = pd.to_datetime(df["time"])
df = df.set_index("time")

# Compute monthly means
monthly = df.resample("M").mean()
print(monthly)
```

**Distributed version (Lithops on AWS Lambda)**:

```python
# Same search and function...

# Process with Lithops - 365 files across ~50 Lambda functions
daily_stats = earthaccess.process(
    results,
    extract_regional_sst,
    backend="lithops",
    config={
        "lithops": {
            "backend": "aws_lambda",
            "storage": "aws_s3",
        },
        "aws_lambda": {
            "execution_role": "arn:aws:iam::...",
            "runtime": "python3.11",
        },
    },
)
# Completes in ~30 seconds vs ~10 minutes locally
```

##### Example 2: ICESat-2 ATL03 Photon Analysis

**Use case**: Extract photon counts and elevation statistics along a glacier.

```python
import earthaccess
import numpy as np

earthaccess.login()

# Search for ATL03 granules over Greenland
results = earthaccess.search_data(
    short_name="ATL03",
    temporal=("2023-06-01", "2023-08-31"),
    bounding_box=(-50, 68, -45, 72),  # Western Greenland
)
print(f"Found {len(results)} granules")


def analyze_photons(ds):
    """Analyze photon data from ATL03 granule.

    Note: ATL03 is HDF5 with complex structure.
    This example uses a simplified approach.
    """
    stats = {}

    # ATL03 has multiple ground tracks (gt1l, gt1r, etc.)
    for gt in ["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"]:
        try:
            heights = ds[f"{gt}/heights/h_ph"][:]

            # Filter for valid elevations (ice surface)
            valid = heights[(heights > 0) & (heights < 4000)]

            if len(valid) > 0:
                stats[gt] = {
                    "n_photons": len(valid),
                    "mean_elevation": float(np.mean(valid)),
                    "std_elevation": float(np.std(valid)),
                }
        except KeyError:
            continue

    return {
        "granule_id": ds.attrs.get("granule_id", "unknown"),
        "tracks": stats,
    }


# Process with Ray on a cluster
import ray
ray.init(address="auto")  # Connect to existing cluster

photon_stats = earthaccess.process(
    results,
    analyze_photons,
    backend="ray",
    engine="h5py",  # Use h5py for HDF5 files
)

# Aggregate results
total_photons = sum(
    sum(t["n_photons"] for t in g["tracks"].values())
    for g in photon_stats
)
print(f"Total photons analyzed: {total_photons:,}")
```

##### Example 3: VirtualiZarr with DMRPP

**Use case**: Create virtual Zarr dataset from multiple granules without downloading.

```python
import earthaccess
from virtualizarr import open_virtual_dataset

earthaccess.login()

# Search for TEMPO atmospheric data
results = earthaccess.search_data(
    short_name="TEMPO_NO2_L2",
    temporal=("2024-01-15", "2024-01-15"),
    count=100,
)


def create_virtual_refs(granule_dict, credentials):
    """Create VirtualiZarr references from DMRPP metadata.

    DMRPP files contain byte-range information that allows
    creating virtual Zarr stores without reading the full file.
    """
    from virtualizarr import open_virtual_dataset
    import s3fs

    fs = s3fs.S3FileSystem(**credentials)

    # Get DMRPP URL (metadata sidecar file)
    data_url = granule_dict["s3_links"][0]
    dmrpp_url = data_url + ".dmrpp"

    # Open as virtual dataset - only reads metadata
    vds = open_virtual_dataset(
        dmrpp_url,
        filetype="dmrpp",
        indexes={},
    )

    # Return serializable references
    return vds.virtualize.to_kerchunk()


# Process to get references (fast - only metadata)
refs_list = earthaccess.process(
    results,
    create_virtual_refs,
    backend="threads",  # Local is fine - just metadata
    raw=True,  # Don't wrap with xr.open_dataset
)

# Combine references
from kerchunk.combine import MultiZarrToZarr

combined = MultiZarrToZarr(
    refs_list,
    concat_dims=["time"],
).translate()

# Open combined virtual dataset
import xarray as xr

ds = xr.open_dataset(
    "reference://",
    engine="zarr",
    backend_kwargs={
        "consolidated": False,
        "storage_options": {
            "fo": combined,
            "remote_protocol": "s3",
            "remote_options": earthaccess.get_s3_credentials(provider="GES_DISC"),
        },
    },
)

print(ds)
# <xarray.Dataset>
# Dimensions:  (time: 100, x: 2048, y: 1024)
# Data is lazy - reads only when accessed
```

##### Example 4: Multi-Dataset Fusion

**Use case**: Combine SST and ocean color data for chlorophyll-SST correlation.

```python
import earthaccess
import numpy as np

earthaccess.login()

# Search for SST
sst_results = earthaccess.search_data(
    short_name="MUR-JPL-L4-GLOB-v4.1",
    temporal=("2023-06-01", "2023-06-30"),
)

# Search for ocean color (MODIS Aqua)
oc_results = earthaccess.search_data(
    short_name="MODIS_A-JPL-L2P-v2019.0",
    temporal=("2023-06-01", "2023-06-30"),
)


def extract_sst_values(ds):
    """Extract SST for specific coordinates."""
    lats = [35.0, 36.0, 37.0]
    lons = [-122.0, -121.0, -120.0]

    values = []
    for lat, lon in zip(lats, lons):
        val = ds["analysed_sst"].sel(lat=lat, lon=lon, method="nearest")
        values.append(float(val.values))

    return {
        "time": str(ds.time.values[0]),
        "sst_values": values,
    }


def extract_chlor_values(ds):
    """Extract chlorophyll for specific coordinates."""
    lats = [35.0, 36.0, 37.0]
    lons = [-122.0, -121.0, -120.0]

    values = []
    for lat, lon in zip(lats, lons):
        try:
            val = ds["chlor_a"].sel(lat=lat, lon=lon, method="nearest")
            values.append(float(val.values))
        except:
            values.append(np.nan)

    return {
        "time": str(ds.time.values[0]),
        "chlor_values": values,
    }


# Process both datasets in parallel using Dask
from dask.distributed import Client
client = Client(n_workers=4)

sst_data = earthaccess.process(sst_results, extract_sst_values, backend="dask")
chlor_data = earthaccess.process(oc_results, extract_chlor_values, backend="dask")

# Merge and analyze
import pandas as pd

sst_df = pd.DataFrame(sst_data).set_index("time")
chlor_df = pd.DataFrame(chlor_data).set_index("time")

# Compute correlation
merged = sst_df.join(chlor_df, how="inner")
# ... analysis continues
```

#### B.3.10 Implementation Plan: TDD & SOLID Principles

##### Design Principles Applied

| Principle | Application |
|-----------|-------------|
| **S**ingle Responsibility | Each class does one thing: `Executor` executes, `CredentialManager` manages credentials, `FileSystemFactory` creates filesystems |
| **O**pen/Closed | New executors added by implementing `Executor` ABC, not modifying existing code |
| **L**iskov Substitution | Any `Executor` subclass can replace another without breaking code |
| **I**nterface Segregation | Small, focused protocols: `Executor`, `CredentialProvider`, `FileSystemProvider` |
| **D**ependency Inversion | High-level `Store` depends on abstract `Executor`, not concrete `PqdmExecutor` |

##### Phase 1: Core Abstractions (Week 1)

**Goal**: Define interfaces and base classes with 100% test coverage.

```python
# tests/unit/test_executor_protocol.py

"""Test that executor implementations conform to the protocol."""

import pytest
from concurrent.futures import Executor, Future
from typing import Protocol, runtime_checkable

from earthaccess.store.parallel import (
    SerialExecutor,
    PqdmExecutor,
    get_executor,
)


class TestExecutorProtocol:
    """All executors must implement concurrent.futures.Executor interface."""

    @pytest.fixture(params=["serial", "pqdm", "threads"])
    def executor(self, request):
        """Parametrized fixture for all executor types."""
        return get_executor(request.param)

    def test_is_executor_subclass(self, executor):
        """Executor must be a subclass of concurrent.futures.Executor."""
        assert isinstance(executor, Executor)

    def test_submit_returns_future(self, executor):
        """submit() must return a Future."""
        future = executor.submit(lambda x: x * 2, 21)
        assert isinstance(future, Future)
        assert future.result() == 42

    def test_map_returns_iterator(self, executor):
        """map() must return an iterator of results."""
        results = executor.map(lambda x: x ** 2, [1, 2, 3])
        assert list(results) == [1, 4, 9]

    def test_context_manager(self, executor):
        """Executor should work as context manager."""
        with executor:
            result = list(executor.map(str.upper, ["a", "b"]))
        assert result == ["A", "B"]

    def test_handles_exceptions(self, executor):
        """Exceptions in tasks should be captured."""
        def failing_fn(x):
            if x == 2:
                raise ValueError("test error")
            return x

        # Submit individual task
        future = executor.submit(failing_fn, 2)
        with pytest.raises(ValueError, match="test error"):
            future.result()


class TestSerialExecutor:
    """SerialExecutor-specific tests."""

    def test_executes_immediately(self):
        """Serial executor should execute synchronously."""
        call_order = []

        def track(x):
            call_order.append(x)
            return x

        executor = SerialExecutor()
        future = executor.submit(track, 1)

        # Should already be executed
        assert call_order == [1]
        assert future.done()


class TestGetExecutor:
    """Test executor factory function."""

    def test_default_is_pqdm(self):
        """Default executor should be PqdmExecutor."""
        executor = get_executor()
        assert isinstance(executor, PqdmExecutor)

    def test_false_returns_serial(self):
        """parallel=False should return SerialExecutor."""
        executor = get_executor(False)
        assert isinstance(executor, SerialExecutor)

    def test_invalid_backend_raises(self):
        """Invalid backend name should raise ValueError."""
        with pytest.raises(ValueError, match="Unrecognized"):
            get_executor("invalid_backend")

    def test_accepts_executor_class(self):
        """Should accept custom Executor subclass."""
        from concurrent.futures import ThreadPoolExecutor
        executor = get_executor(ThreadPoolExecutor, max_workers=2)
        assert isinstance(executor, ThreadPoolExecutor)
```

**Implementation**:

```python
# earthaccess/store/parallel.py

"""
Parallel execution backends following concurrent.futures.Executor API.

This module provides a unified interface for parallel execution,
allowing users to choose between different backends while maintaining
a consistent API.
"""

from abc import ABC
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from typing import Any, Callable, Iterable, Iterator, Literal, TypeVar

__all__ = ["get_executor", "SerialExecutor", "PqdmExecutor"]

T = TypeVar("T")
R = TypeVar("R")

ParallelBackend = Literal["pqdm", "threads", "dask", "lithops", "ray", False]


def get_executor(
    parallel: ParallelBackend | type[Executor] = "pqdm",
    **kwargs: Any,
) -> Executor:
    """Factory function to get an executor instance.

    Args:
        parallel: Backend name or Executor subclass
        **kwargs: Backend-specific options

    Returns:
        Configured Executor instance

    Raises:
        ValueError: If backend is not recognized
    """
    if parallel is False:
        return SerialExecutor()

    if parallel == "pqdm":
        return PqdmExecutor(**kwargs)

    if parallel == "threads":
        return ThreadPoolExecutor(**kwargs)

    if parallel == "dask":
        return DaskDelayedExecutor(**kwargs)

    if parallel == "lithops":
        return LithopsExecutor(**kwargs)

    if parallel == "ray":
        return RayExecutor(**kwargs)

    if isinstance(parallel, type) and issubclass(parallel, Executor):
        return parallel(**kwargs)

    raise ValueError(
        f"Unrecognized parallel backend: {parallel!r}. "
        f"Use 'pqdm', 'threads', 'dask', 'lithops', 'ray', False, "
        f"or a concrete Executor subclass."
    )


class SerialExecutor(Executor):
    """Execute tasks sequentially for debugging and testing.

    Implements the concurrent.futures.Executor interface but
    runs everything synchronously in the calling thread.
    """

    def submit(
        self, fn: Callable[..., T], /, *args: Any, **kwargs: Any
    ) -> Future[T]:
        """Execute function immediately, return completed Future."""
        future: Future[T] = Future()
        try:
            result = fn(*args, **kwargs)
            future.set_result(result)
        except Exception as exc:
            future.set_exception(exc)
        return future

    def map(
        self,
        fn: Callable[..., T],
        *iterables: Iterable[Any],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[T]:
        """Apply function to iterables sequentially."""
        return map(fn, *iterables)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        """No-op for serial executor."""
        pass


class PqdmExecutor(Executor):
    """Execute tasks using pqdm with progress bars.

    This is the default executor, providing backwards compatibility
    with earthaccess's current behavior.
    """

    def __init__(
        self,
        n_jobs: int = 8,
        exception_behaviour: Literal["immediate", "deferred"] = "immediate",
        show_progress: bool = True,
        desc: str = "",
        **pqdm_kwargs: Any,
    ):
        self.n_jobs = n_jobs
        self.exception_behaviour = exception_behaviour
        self.show_progress = show_progress
        self.desc = desc
        self.pqdm_kwargs = pqdm_kwargs

    def submit(
        self, fn: Callable[..., T], /, *args: Any, **kwargs: Any
    ) -> Future[T]:
        """Submit single task. Executes immediately for single items."""
        future: Future[T] = Future()
        try:
            result = fn(*args, **kwargs)
            future.set_result(result)
        except Exception as exc:
            future.set_exception(exc)
        return future

    def map(
        self,
        fn: Callable[..., T],
        *iterables: Iterable[Any],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[T]:
        """Apply function using pqdm for parallel execution."""
        from pqdm.threads import pqdm

        # Combine iterables
        if len(iterables) == 1:
            items = list(iterables[0])
        else:
            items = list(zip(*iterables))

        if not items:
            return iter([])

        results = pqdm(
            items,
            fn,
            n_jobs=self.n_jobs,
            exception_behaviour=self.exception_behaviour,
            disable=not self.show_progress,
            desc=self.desc,
            **self.pqdm_kwargs,
        )

        return iter(results)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        """No-op - pqdm doesn't require explicit shutdown."""
        pass
```

##### Phase 2: Credential Management (Week 1-2)

**Tests first**:

```python
# tests/unit/test_credentials.py

"""Test credential management."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from earthaccess.store.credentials import (
    S3Credentials,
    CredentialManager,
    CredentialProvider,
)


class TestS3Credentials:
    """Test S3Credentials dataclass."""

    def test_is_expired_when_past_expiration(self):
        """Credentials should be expired when past expiration time."""
        creds = S3Credentials(
            access_key_id="test",
            secret_access_key="test",
            session_token="test",
            expiration=datetime.now() - timedelta(minutes=5),
        )
        assert creds.is_expired is True

    def test_is_not_expired_when_valid(self):
        """Credentials should not be expired when within validity period."""
        creds = S3Credentials(
            access_key_id="test",
            secret_access_key="test",
            session_token="test",
            expiration=datetime.now() + timedelta(minutes=30),
        )
        assert creds.is_expired is False

    def test_to_dict_returns_fsspec_format(self):
        """to_dict() should return fsspec-compatible credential dict."""
        creds = S3Credentials(
            access_key_id="AKID",
            secret_access_key="SECRET",
            session_token="TOKEN",
            expiration=datetime.now() + timedelta(hours=1),
        )
        result = creds.to_dict()

        assert result == {
            "key": "AKID",
            "secret": "SECRET",
            "token": "TOKEN",
        }


class TestCredentialManager:
    """Test CredentialManager caching and refresh."""

    @pytest.fixture
    def mock_auth(self):
        """Mock earthaccess Auth object."""
        auth = Mock()
        auth.get_s3_credentials.return_value = {
            "accessKeyId": "AKID",
            "secretAccessKey": "SECRET",
            "sessionToken": "TOKEN",
        }
        return auth

    def test_caches_credentials(self, mock_auth):
        """Credentials should be cached and reused."""
        manager = CredentialManager(mock_auth)

        # First call fetches
        creds1 = manager.get_credentials(provider="POCLOUD")
        # Second call uses cache
        creds2 = manager.get_credentials(provider="POCLOUD")

        assert mock_auth.get_s3_credentials.call_count == 1
        assert creds1 is creds2

    def test_refreshes_expired_credentials(self, mock_auth):
        """Expired credentials should be refreshed."""
        manager = CredentialManager(mock_auth)

        # Get initial credentials
        creds1 = manager.get_credentials(provider="POCLOUD")

        # Manually expire them
        creds1.expiration = datetime.now() - timedelta(minutes=5)

        # Next call should fetch fresh credentials
        creds2 = manager.get_credentials(provider="POCLOUD")

        assert mock_auth.get_s3_credentials.call_count == 2
        assert creds1 is not creds2

    def test_different_providers_cached_separately(self, mock_auth):
        """Different providers should have separate cache entries."""
        manager = CredentialManager(mock_auth)

        manager.get_credentials(provider="POCLOUD")
        manager.get_credentials(provider="NSIDC_CPRD")

        assert mock_auth.get_s3_credentials.call_count == 2

    def test_requires_at_least_one_identifier(self, mock_auth):
        """Should raise if no identifier provided."""
        manager = CredentialManager(mock_auth)

        with pytest.raises(ValueError, match="At least one of"):
            manager.get_credentials()
```

##### Phase 3: FileSystem Factory (Week 2)

**Tests first**:

```python
# tests/unit/test_filesystems.py

"""Test filesystem factory."""

import pytest
from unittest.mock import Mock, patch

from earthaccess.store.filesystems import FileSystemFactory


class TestFileSystemFactory:
    """Test FileSystemFactory."""

    @pytest.fixture
    def factory(self):
        """Create factory with mocked dependencies."""
        auth = Mock()
        credential_manager = Mock()
        credential_manager.get_credentials.return_value = Mock(
            to_dict=lambda: {"key": "x", "secret": "y", "token": "z"}
        )
        return FileSystemFactory(auth, credential_manager)

    def test_get_s3_filesystem_uses_credentials(self, factory):
        """S3 filesystem should use credentials from manager."""
        with patch("s3fs.S3FileSystem") as mock_s3fs:
            fs = factory.get_s3_filesystem(provider="POCLOUD")

            mock_s3fs.assert_called_once_with(key="x", secret="y", token="z")

    def test_get_filesystem_for_s3_url(self, factory):
        """Should return S3 filesystem for s3:// URLs."""
        with patch("s3fs.S3FileSystem"):
            fs = factory.get_filesystem_for_url(
                "s3://bucket/key",
                provider="POCLOUD",
            )

            factory._credential_manager.get_credentials.assert_called()

    def test_get_filesystem_for_https_url(self, factory):
        """Should return HTTPS filesystem for https:// URLs."""
        with patch("fsspec.filesystem") as mock_fsspec:
            fs = factory.get_filesystem_for_url("https://example.com/file")

            mock_fsspec.assert_called()

    def test_s3_url_without_provider_raises(self, factory):
        """S3 URL without provider should raise helpful error."""
        with pytest.raises(ValueError, match="requires provider"):
            factory.get_filesystem_for_url("s3://bucket/key")
```

##### Phase 4: Streaming Executor (Week 2-3)

**Tests first**:

```python
# tests/unit/test_streaming.py

"""Test streaming parallel execution."""

import pytest
import time
from concurrent.futures import Future
from typing import Iterator

from earthaccess.store.streaming import (
    stream_parallel,
    StreamingExecutor,
)


class TestStreamParallel:
    """Test stream_parallel function."""

    def test_processes_all_items(self):
        """All items should be processed."""
        items = list(range(10))
        results = list(stream_parallel(items, lambda x: x * 2, max_workers=2))

        assert sorted(results) == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

    def test_handles_lazy_iterator(self):
        """Should work with lazy iterators."""
        def lazy_items():
            for i in range(5):
                yield i

        results = list(stream_parallel(lazy_items(), lambda x: x ** 2))
        assert sorted(results) == [0, 1, 4, 9, 16]

    def test_respects_max_workers(self):
        """Should not exceed max_workers concurrent executions."""
        active = []
        max_active = 0

        def track_concurrency(x):
            nonlocal max_active
            active.append(x)
            max_active = max(max_active, len(active))
            time.sleep(0.1)
            active.remove(x)
            return x

        list(stream_parallel(range(20), track_concurrency, max_workers=4))

        assert max_active <= 4

    def test_propagates_exceptions(self):
        """Exceptions should be propagated."""
        def failing(x):
            if x == 5:
                raise ValueError("test")
            return x

        with pytest.raises(ValueError, match="test"):
            list(stream_parallel(range(10), failing))

    def test_yields_results_as_completed(self):
        """Results should be yielded as they complete."""
        def slow_then_fast(x):
            time.sleep(0.1 if x == 0 else 0.01)
            return x

        results = list(stream_parallel([0, 1, 2, 3], slow_then_fast, max_workers=4))

        # Item 0 is slow, others fast - so 0 likely comes later
        # Just verify all results present
        assert sorted(results) == [0, 1, 2, 3]


class TestStreamingExecutor:
    """Test StreamingExecutor class."""

    def test_map_returns_iterator(self):
        """map() should return an iterator."""
        executor = StreamingExecutor(max_workers=2)
        result = executor.map(lambda x: x, [1, 2, 3])

        assert hasattr(result, "__iter__")
        assert list(result) == [1, 2, 3]

    def test_works_with_granule_results_mock(self):
        """Should work with objects that have pages() method."""
        # Mock GranuleResults-like object
        class MockResults:
            def __init__(self, items):
                self._items = items

            def __iter__(self):
                return iter(self._items)

            def pages(self, page_size=10):
                for i in range(0, len(self._items), page_size):
                    yield self._items[i:i+page_size]

        results = MockResults(list(range(25)))
        executor = StreamingExecutor(max_workers=4, prefetch_pages=1)

        output = list(executor.map(lambda x: x * 2, results))
        assert sorted(output) == [x * 2 for x in range(25)]
```

##### Phase 5: Process Function (Week 3)

**Tests first**:

```python
# tests/unit/test_process.py

"""Test earthaccess.process() function."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import numpy as np

from earthaccess.api import process


class TestProcess:
    """Test process() function."""

    @pytest.fixture
    def mock_granules(self):
        """Create mock granules."""
        granules = []
        for i in range(3):
            g = Mock()
            g.__getitem__ = Mock(return_value={"provider-id": "POCLOUD"})
            g.data_links.return_value = [f"s3://bucket/file{i}.nc"]
            g.to_dict.return_value = {
                "meta": {"provider-id": "POCLOUD"},
                "s3_links": [f"s3://bucket/file{i}.nc"],
            }
            granules.append(g)
        return granules

    @patch("earthaccess.api.get_s3_credentials")
    @patch("earthaccess.api._process_local")
    def test_local_backend_default(self, mock_local, mock_creds, mock_granules):
        """Default backend should be local."""
        mock_creds.return_value = {"key": "x"}
        mock_local.return_value = [1, 2, 3]

        result = process(mock_granules, lambda ds: ds.mean())

        mock_local.assert_called_once()
        assert result == [1, 2, 3]

    @patch("earthaccess.api.get_s3_credentials")
    def test_auto_fetches_credentials(self, mock_creds, mock_granules):
        """Should auto-fetch credentials if not provided."""
        mock_creds.return_value = {"key": "x"}

        with patch("earthaccess.api._process_local"):
            process(mock_granules, lambda ds: 1)

        mock_creds.assert_called_once_with(provider="POCLOUD")

    def test_accepts_explicit_credentials(self, mock_granules):
        """Should use provided credentials."""
        creds = {"key": "explicit"}

        with patch("earthaccess.api._process_local") as mock_local:
            process(mock_granules, lambda ds: 1, credentials=creds)

            # Verify credentials passed through
            call_kwargs = mock_local.call_args
            assert call_kwargs is not None

    @patch("earthaccess.api.LithopsProcessor")
    @patch("earthaccess.api.get_s3_credentials")
    def test_lithops_backend(self, mock_creds, mock_lithops, mock_granules):
        """Should use LithopsProcessor for lithops backend."""
        mock_creds.return_value = {"key": "x"}
        mock_processor = Mock()
        mock_processor.map.return_value = [1, 2, 3]
        mock_lithops.return_value = mock_processor

        result = process(mock_granules, lambda ds: 1, backend="lithops")

        mock_lithops.assert_called_once()
        assert result == [1, 2, 3]

    @patch("earthaccess.api.RayProcessor")
    @patch("earthaccess.api.get_s3_credentials")
    def test_ray_backend(self, mock_creds, mock_ray, mock_granules):
        """Should use RayProcessor for ray backend."""
        mock_creds.return_value = {"key": "x"}
        mock_processor = Mock()
        mock_processor.map.return_value = [1, 2, 3]
        mock_ray.return_value = mock_processor

        result = process(mock_granules, lambda ds: 1, backend="ray")

        mock_ray.assert_called_once()


class TestProcessIntegration:
    """Integration tests for process() - require mocked xarray."""

    @patch("xarray.open_dataset")
    @patch("s3fs.S3FileSystem")
    @patch("earthaccess.api.get_s3_credentials")
    def test_end_to_end_local(self, mock_creds, mock_s3fs, mock_xr, mock_granules):
        """End-to-end test with local backend."""
        # Setup mocks
        mock_creds.return_value = {"key": "x", "secret": "y", "token": "z"}

        mock_fs = Mock()
        mock_fs.open.return_value.__enter__ = Mock(return_value=Mock())
        mock_fs.open.return_value.__exit__ = Mock(return_value=False)
        mock_s3fs.return_value = mock_fs

        mock_ds = Mock()
        mock_ds.mean.return_value = 42.0
        mock_ds.close = Mock()
        mock_xr.return_value = mock_ds

        # Create mock granules
        granules = []
        for i in range(2):
            g = Mock()
            g.__getitem__ = Mock(return_value={"provider-id": "POCLOUD"})
            g.data_links.return_value = [f"s3://bucket/file{i}.nc"]
            granules.append(g)

        # Run process
        results = process(granules, lambda ds: ds.mean(), backend="local")

        assert results == [42.0, 42.0]
```

##### Phase 6: Integration Tests (Week 3-4)

```python
# tests/integration/test_process_integration.py

"""Integration tests for process() with real data."""

import pytest
import numpy as np

import earthaccess


@pytest.fixture(scope="module")
def auth():
    """Authenticate for integration tests."""
    return earthaccess.login()


@pytest.mark.integration
@pytest.mark.vcr  # Record/replay HTTP interactions
class TestProcessSST:
    """Test process() with real SST data."""

    def test_extract_sst_statistics(self, auth):
        """Should extract SST statistics from MUR data."""
        results = earthaccess.search_data(
            short_name="MUR-JPL-L4-GLOB-v4.1",
            temporal=("2023-01-01", "2023-01-03"),
            count=3,
        )

        def get_stats(ds):
            sst = ds["analysed_sst"]
            return {
                "mean": float(sst.mean()),
                "std": float(sst.std()),
            }

        stats = earthaccess.process(results, get_stats)

        assert len(stats) == 3
        assert all("mean" in s and "std" in s for s in stats)
        assert all(isinstance(s["mean"], float) for s in stats)


@pytest.mark.integration
class TestProcessATL03:
    """Test process() with ICESat-2 ATL03 data."""

    def test_count_photons(self, auth):
        """Should count photons from ATL03 granules."""
        results = earthaccess.search_data(
            short_name="ATL03",
            temporal=("2023-06-01", "2023-06-01"),
            bounding_box=(-50, 68, -45, 70),
            count=2,
        )

        if len(results) == 0:
            pytest.skip("No ATL03 granules found for test region")

        def count_photons(ds):
            total = 0
            for gt in ["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"]:
                try:
                    heights = ds[f"{gt}/heights/h_ph"]
                    total += len(heights)
                except KeyError:
                    pass
            return total

        counts = earthaccess.process(
            results,
            count_photons,
            engine="h5py",
        )

        assert len(counts) == len(results)
        assert all(isinstance(c, int) for c in counts)
```

##### Implementation Checklist

| Phase | Week | Deliverables | Tests |
|-------|------|--------------|-------|
| **1. Core Abstractions** | 1 | `Executor` protocol, `SerialExecutor`, `PqdmExecutor`, `get_executor()` | `test_executor_protocol.py` |
| **2. Credentials** | 1-2 | `S3Credentials`, `CredentialManager` | `test_credentials.py` |
| **3. Filesystems** | 2 | `FileSystemFactory` | `test_filesystems.py` |
| **4. Streaming** | 2-3 | `stream_parallel()`, `StreamingExecutor` | `test_streaming.py` |
| **5. Process API** | 3 | `earthaccess.process()`, `LithopsProcessor`, `RayProcessor` | `test_process.py` |
| **6. Integration** | 3-4 | End-to-end tests with real data | `test_process_integration.py` |
| **7. Documentation** | 4 | Examples, migration guide | - |

##### Code Organization

```
earthaccess/
├── store/
│   ├── __init__.py           # Public API: Store class
│   ├── parallel.py           # Executor implementations
│   ├── credentials.py        # S3Credentials, CredentialManager
│   ├── filesystems.py        # FileSystemFactory
│   ├── streaming.py          # StreamingExecutor, stream_parallel
│   ├── transfer.py           # CloudTransfer for downloads
│   └── processors/
│       ├── __init__.py
│       ├── base.py           # Processor protocol
│       ├── local.py          # Local processor
│       ├── lithops.py        # LithopsProcessor
│       └── ray.py            # RayProcessor
├── api.py                    # process(), open(), download()
└── ...

tests/
├── unit/
│   ├── store/
│   │   ├── test_parallel.py
│   │   ├── test_credentials.py
│   │   ├── test_filesystems.py
│   │   ├── test_streaming.py
│   │   └── test_processors.py
│   └── test_process.py
└── integration/
    └── test_process_integration.py
```

##### SOLID Principles Summary

| Principle | Implementation |
|-----------|----------------|
| **Single Responsibility** | `CredentialManager` only manages credentials; `FileSystemFactory` only creates filesystems; `Executor` only executes tasks |
| **Open/Closed** | Add new backends (`LithopsExecutor`, `RayExecutor`) by implementing `Executor` ABC - no modification to existing code |
| **Liskov Substitution** | Any `Executor` can replace any other in `process()`, `open()`, `download()` without breaking functionality |
| **Interface Segregation** | Small focused interfaces: `Executor.submit()`, `Executor.map()` - not bloated with unrelated methods |
| **Dependency Inversion** | `Store` depends on abstract `Executor`, not concrete `PqdmExecutor`; `process()` accepts any `Executor` subclass |

##### Pythonic Design Patterns

| Pattern | Usage |
|---------|-------|
| **Factory Function** | `get_executor("dask")` returns configured executor |
| **Protocol/ABC** | `Executor` from `concurrent.futures` as the interface |
| **Context Manager** | `with executor:` for proper resource cleanup |
| **Iterator** | `stream_parallel()` yields results as they complete |
| **Dataclass** | `S3Credentials` for immutable credential storage |
| **Dependency Injection** | `Store(auth, credential_manager, filesystem_factory)` |

---

## Group C: Asset Access & Filtering

### C.1 Design Goals

- **STAC-style asset access** via `.assets` property
- **Flexible filtering** by name pattern, roles, media type
- **Integration with `open()` and `download()`**
- **Migration path** from `.data_links()`

### C.2 Asset Model

```python
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class Asset:
    """STAC-like asset representation."""
    href: str
    media_type: Optional[str] = None
    roles: List[str] = None
    title: Optional[str] = None
    description: Optional[str] = None

    def __post_init__(self):
        if self.roles is None:
            self.roles = []


@dataclass
class AssetFilter:
    """Filter specification for granule assets."""
    name_pattern: Optional[str] = None      # Glob pattern
    name_regex: Optional[Pattern] = None    # Regex pattern
    roles: Optional[List[str]] = None       # Include roles
    exclude_roles: Optional[List[str]] = None  # Exclude roles
    media_types: Optional[List[str]] = None
    cloud_optimized: Optional[bool] = None

    @classmethod
    def bands(cls, *band_names: str) -> "AssetFilter":
        """Factory for filtering specific bands."""
        pattern = "|".join(re.escape(b) for b in band_names)
        return cls(name_regex=re.compile(pattern))
```

### C.3 Enhanced open() and download()

```python
def open(
    granules: List[DataGranule],
    *,
    asset_filter: Optional[AssetFilter] = None,
    name_pattern: Optional[str] = None,
    name_contains: Optional[str] = None,
    roles: Optional[List[str]] = None,
    # ... existing params
) -> List[AbstractFileSystem]:
    """Open granule assets with optional filtering.

    Examples:
        # Open only RGB bands
        files = earthaccess.open(granules, name_pattern="*B0[234].tif")

        # Open using filter object
        filter = AssetFilter.bands("B02", "B03", "B04")
        files = earthaccess.open(granules, asset_filter=filter)
    """
    ...
```

See [STAC-PLAN.md](./STAC-PLAN.md) for full implementation details.

---

## Group D: STAC Conversion

### D.1 DataGranule Methods

```python
class DataGranule(CustomDict):

    def to_stac(self) -> "pystac.Item":
        """Convert to a pystac.Item."""
        ...

    def to_umm(self) -> Dict[str, Any]:
        """Return raw UMM-G dictionary."""
        return self["umm"]

    @classmethod
    def from_stac_item(cls, item: "pystac.Item") -> "DataGranule":
        """Create from a pystac.Item."""
        ...
```

### D.2 DataCollection Methods

```python
class DataCollection(CustomDict):

    def to_stac(self) -> "pystac.Collection":
        """Convert to a pystac.Collection."""
        ...

    def to_umm(self) -> Dict[str, Any]:
        """Return raw UMM-C dictionary."""
        return self["umm"]
```

See [STAC-PLAN.md](./STAC-PLAN.md) for conversion details.

---

## Group E: External STAC Catalogs

### E.1 search_stac() Function

```python
def search_stac(
    catalog_url: str,
    *,
    collections: Optional[List[str]] = None,
    bbox: Optional[List[float]] = None,
    datetime: Optional[str] = None,
    intersects: Optional[Dict] = None,
    max_items: Optional[int] = None,
) -> GranuleResults:
    """Search an external STAC catalog.

    Returns GranuleResults (via STACItemResults subclass) that can be used
    with earthaccess workflows.

    Example:
        results = earthaccess.search_stac(
            "https://earth-search.aws.element84.com/v1",
            collections=["sentinel-2-l2a"],
            bbox=[-122.5, 37.5, -122.0, 38.0],
            datetime="2023-01-01/2023-06-30",
        )
        for granule in results:
            files = earthaccess.open([granule])
    """
    from pystac_client import Client

    client = Client.open(catalog_url)
    item_search = client.search(
        collections=collections,
        bbox=bbox,
        datetime=datetime,
        intersects=intersects,
        max_items=max_items,
    )

    return STACItemResults(item_search, max_results=max_items)
```

### E.2 Using Query.to_stac() with pystac-client

```python
# Build query with earthaccess
query = earthaccess.GranuleQuery(
    backend="cmr",
    collections=["ATL03"],
    temporal=("2023-01-01", "2023-12-31"),
    bounding_box=(-180, -90, 180, 90),
)

# Convert to STAC parameters
stac_params = query.to_stac()
# Result:
# {
#     "collections": ["ATL03"],
#     "datetime": "2023-01-01T00:00:00Z/2023-12-31T23:59:59Z",
#     "bbox": [-180, -90, 180, 90],
# }

# Use with pystac-client
from pystac_client import Client

catalog = Client.open("https://cmr.earthdata.nasa.gov/stac")
search = catalog.search(**stac_params)

for item in search.items():
    print(item.id)
```

---

## Group F: Flexible Input Types

### F.1 Design Goals

Accept **any iterable** (list, tuple, generator, numpy array) for spatial and temporal parameters. Users should never encounter "this needs to be a tuple" errors.

### F.2 Type Definitions

```python
# earthaccess/query.py

from typing import Iterable, Union, Sequence
from collections.abc import Iterable as IterableABC
import numpy as np

# Flexible type aliases
BBoxLike = Union[
    Tuple[float, float, float, float],  # Tuple
    List[float],                         # List
    Sequence[float],                     # Any sequence
    np.ndarray,                          # NumPy array
]

PointLike = Union[
    Tuple[float, float],
    List[float],
    Sequence[float],
    np.ndarray,
]

TemporalLike = Union[
    Tuple[Optional[str], Optional[str]],
    List[Optional[str]],
    Sequence[Optional[str]],
    str,  # Single datetime or interval string "2023-01-01/2023-12-31"
]

CoordinatesLike = Union[
    Sequence[Tuple[float, float]],       # List of tuples
    Sequence[Sequence[float]],           # List of lists
    np.ndarray,                          # 2D NumPy array (N, 2)
]
```

### F.3 Updated Method Signatures

```python
class GranuleQuery:

    def bounding_box(
        self,
        west_or_bbox: Union[float, BBoxLike],
        south: Optional[float] = None,
        east: Optional[float] = None,
        north: Optional[float] = None,
    ) -> "GranuleQuery":
        """Filter by bounding box.

        Accepts either four separate values or any iterable of 4 floats.

        Args:
            west_or_bbox: Western longitude OR an iterable of [west, south, east, north]
            south: Southern latitude (if west_or_bbox is a single value)
            east: Eastern longitude (if west_or_bbox is a single value)
            north: Northern latitude (if west_or_bbox is a single value)

        Returns:
            self for chaining

        Examples:
            # All of these work:
            query.bounding_box(-180, -90, 180, 90)           # Separate values
            query.bounding_box([-180, -90, 180, 90])         # List
            query.bounding_box((-180, -90, 180, 90))         # Tuple
            query.bounding_box(np.array([-180, -90, 180, 90]))  # NumPy
            query.bounding_box(gdf.total_bounds)             # GeoPandas bounds
        """
        bbox = self._normalize_bbox(west_or_bbox, south, east, north)
        self._validate_bbox(bbox)
        self._params["bounding_box"] = bbox
        return self

    def _normalize_bbox(
        self,
        west_or_bbox: Union[float, BBoxLike],
        south: Optional[float],
        east: Optional[float],
        north: Optional[float],
    ) -> Tuple[float, float, float, float]:
        """Normalize various bbox inputs to a tuple."""
        if south is None and east is None and north is None:
            # Single iterable argument
            coords = list(west_or_bbox)
            if len(coords) != 4:
                raise ValueError(f"bounding_box must have 4 values, got {len(coords)}")
            return tuple(float(c) for c in coords)
        else:
            # Four separate arguments
            return (float(west_or_bbox), float(south), float(east), float(north))

    def point(
        self,
        lon_or_point: Union[float, PointLike],
        lat: Optional[float] = None,
    ) -> "GranuleQuery":
        """Filter by point intersection.

        Accepts either two separate values or any iterable of 2 floats.

        Args:
            lon_or_point: Longitude OR an iterable of [lon, lat]
            lat: Latitude (if lon_or_point is a single value)

        Returns:
            self for chaining

        Examples:
            query.point(-122.4, 37.8)        # Separate values
            query.point([-122.4, 37.8])      # List
            query.point((-122.4, 37.8))      # Tuple
            query.point(shapely_point)       # Shapely Point (extracts coords)
        """
        point = self._normalize_point(lon_or_point, lat)
        self._validate_point(*point)
        self._params["point"] = point
        return self

    def _normalize_point(
        self,
        lon_or_point: Union[float, PointLike],
        lat: Optional[float],
    ) -> Tuple[float, float]:
        """Normalize various point inputs to a tuple."""
        if lat is None:
            # Check for shapely Point
            if hasattr(lon_or_point, '__geo_interface__'):
                geom = lon_or_point.__geo_interface__
                if geom['type'] == 'Point':
                    return tuple(geom['coordinates'][:2])
            # Single iterable
            coords = list(lon_or_point)
            if len(coords) != 2:
                raise ValueError(f"point must have 2 values, got {len(coords)}")
            return (float(coords[0]), float(coords[1]))
        else:
            return (float(lon_or_point), float(lat))

    def temporal(
        self,
        start_or_range: Union[str, date, datetime, TemporalLike, None] = None,
        end: Optional[Union[str, date, datetime]] = None,
    ) -> "GranuleQuery":
        """Filter by temporal range.

        Accepts various input formats for maximum flexibility.

        Args:
            start_or_range: Start datetime, or an iterable of [start, end], or interval string
            end: End datetime (if start_or_range is a single value)

        Returns:
            self for chaining

        Examples:
            # All of these work:
            query.temporal("2023-01-01", "2023-12-31")           # Separate strings
            query.temporal(["2023-01-01", "2023-12-31"])         # List
            query.temporal(("2023-01-01", "2023-12-31"))         # Tuple
            query.temporal("2023-01-01/2023-12-31")              # Interval string
            query.temporal(datetime(2023, 1, 1), datetime(2023, 12, 31))  # datetime objects
            query.temporal("2023-01-01", None)                   # Open-ended
            query.temporal(None, "2023-12-31")                   # Open start
        """
        start_dt, end_dt = self._normalize_temporal(start_or_range, end)
        self._validate_temporal(start_dt, end_dt)
        self._params["temporal"] = (start_dt, end_dt)
        return self

    def _normalize_temporal(
        self,
        start_or_range: Union[str, date, datetime, TemporalLike, None],
        end: Optional[Union[str, date, datetime]],
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Normalize various temporal inputs."""
        if start_or_range is None and end is None:
            return (None, None)

        # Check for interval string "start/end"
        if isinstance(start_or_range, str) and "/" in start_or_range and end is None:
            parts = start_or_range.split("/")
            start_str = parts[0] if parts[0] != ".." else None
            end_str = parts[1] if parts[1] != ".." else None
            return (
                self._parse_datetime(start_str) if start_str else None,
                self._parse_datetime(end_str) if end_str else None,
            )

        # Check for iterable [start, end]
        if end is None and hasattr(start_or_range, '__iter__') and not isinstance(start_or_range, str):
            items = list(start_or_range)
            if len(items) == 2:
                return (
                    self._parse_datetime(items[0]) if items[0] else None,
                    self._parse_datetime(items[1]) if items[1] else None,
                )

        # Two separate values
        return (
            self._parse_datetime(start_or_range) if start_or_range else None,
            self._parse_datetime(end) if end else None,
        )

    def polygon(
        self,
        coordinates: CoordinatesLike,
    ) -> "GranuleQuery":
        """Filter by polygon intersection.

        Accepts various coordinate formats.

        Args:
            coordinates: Polygon coordinates as any of:
                - List of (lon, lat) tuples
                - List of [lon, lat] lists
                - 2D NumPy array of shape (N, 2)
                - Shapely Polygon (extracts exterior coordinates)

        Returns:
            self for chaining

        Examples:
            # List of tuples
            query.polygon([(-122, 37), (-122, 38), (-121, 38), (-121, 37), (-122, 37)])

            # List of lists
            query.polygon([[-122, 37], [-122, 38], [-121, 38], [-121, 37], [-122, 37]])

            # NumPy array
            coords = np.array([[-122, 37], [-122, 38], [-121, 38], [-121, 37], [-122, 37]])
            query.polygon(coords)
        """
        coords = self._normalize_polygon_coords(coordinates)
        self._validate_polygon(coords)
        self._params["polygon"] = coords
        return self

    def _normalize_polygon_coords(
        self,
        coordinates: CoordinatesLike,
    ) -> List[Tuple[float, float]]:
        """Normalize various polygon coordinate inputs."""
        # Handle shapely geometry
        if hasattr(coordinates, '__geo_interface__'):
            geom = coordinates.__geo_interface__
            if geom['type'] == 'Polygon':
                return [(float(c[0]), float(c[1])) for c in geom['coordinates'][0]]
            raise ValueError(f"Expected Polygon, got {geom['type']}")

        # Handle numpy array
        if hasattr(coordinates, 'tolist'):
            coordinates = coordinates.tolist()

        # Convert to list of tuples
        result = []
        for coord in coordinates:
            if hasattr(coord, 'tolist'):
                coord = coord.tolist()
            result.append((float(coord[0]), float(coord[1])))

        return result

    def cloud_cover(
        self,
        min_or_range: Union[float, Sequence[Optional[float]], None] = None,
        max_cover: Optional[float] = None,
    ) -> "GranuleQuery":
        """Filter by cloud cover percentage.

        Args:
            min_or_range: Minimum cloud cover OR [min, max] iterable
            max_cover: Maximum cloud cover (if min_or_range is a single value)

        Returns:
            self for chaining

        Examples:
            query.cloud_cover(0, 20)         # Separate values
            query.cloud_cover([0, 20])       # List
            query.cloud_cover((0, 20))       # Tuple
            query.cloud_cover(max_cover=20)  # Max only
        """
        if min_or_range is None and max_cover is None:
            return self

        if max_cover is None and hasattr(min_or_range, '__iter__'):
            items = list(min_or_range)
            min_cc = items[0]
            max_cc = items[1] if len(items) > 1 else None
        else:
            min_cc = min_or_range
            max_cc = max_cover

        self._validate_cloud_cover(min_cc, max_cc)
        self._params["cloud_cover"] = (min_cc, max_cc)
        return self
```

### F.4 Constructor Updates

```python
def __init__(
    self,
    backend: str = "cmr",
    *,
    # Spatial filters - now accept any iterable
    bounding_box: Optional[BBoxLike] = None,
    polygon: Optional[CoordinatesLike] = None,
    point: Optional[PointLike] = None,
    intersects: Optional[Dict[str, Any]] = None,
    # Temporal - now accepts list, tuple, or interval string
    temporal: Optional[TemporalLike] = None,
    # Cloud cover - now accepts [min, max] iterable
    cloud_cover: Optional[Union[Sequence[Optional[float]], Tuple[Optional[float], Optional[float]]]] = None,
    # ... other params unchanged
):
    # Apply with flexible parsing
    if bounding_box is not None:
        self.bounding_box(bounding_box)  # Handles list, tuple, array
    if temporal is not None:
        self.temporal(temporal)          # Handles list, tuple, interval string
    if cloud_cover is not None:
        self.cloud_cover(cloud_cover)    # Handles list, tuple
    # ...
```

---

## Group G: Geometry Handling

### G.1 Design Goals

- **Load geometries from multiple sources**: GeoJSON files, shapely objects, GeoDataFrame
- **Auto-simplify** complex geometries to ≤300 points for CMR compliance
- **Support common GIS workflows**

### G.2 Geometry Input Sources

```python
# earthaccess/geometry.py

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import json
import warnings

# CMR limit for polygon points
CMR_MAX_POLYGON_POINTS = 300

# Type for geometry-like objects
GeometryLike = Union[
    Dict[str, Any],           # GeoJSON dict
    str,                      # File path or WKT string
    Path,                     # File path
    "shapely.geometry.base.BaseGeometry",  # Shapely geometry
    "geopandas.GeoDataFrame", # GeoDataFrame (uses unary_union)
    "geopandas.GeoSeries",    # GeoSeries (uses unary_union)
]


def load_geometry(
    geometry: GeometryLike,
    *,
    simplify: bool = True,
    max_points: int = CMR_MAX_POLYGON_POINTS,
) -> Dict[str, Any]:
    """Load geometry from various sources and prepare for CMR.

    Accepts GeoJSON dicts, file paths, shapely geometries, or GeoDataFrames.
    Automatically simplifies complex geometries to comply with CMR's 300-point limit.

    Args:
        geometry: Geometry input (see examples below)
        simplify: If True, simplify geometries exceeding max_points
        max_points: Maximum points allowed (default: 300 for CMR)

    Returns:
        GeoJSON geometry dict ready for CMR queries

    Raises:
        ValueError: If geometry cannot be loaded or is invalid

    Examples:
        # GeoJSON dict
        geom = load_geometry({"type": "Polygon", "coordinates": [...]})

        # GeoJSON file
        geom = load_geometry("path/to/region.geojson")
        geom = load_geometry(Path("path/to/region.geojson"))

        # Shapely geometry
        from shapely.geometry import box
        geom = load_geometry(box(-122, 37, -121, 38))

        # GeoDataFrame (uses unary_union of all geometries)
        import geopandas as gpd
        gdf = gpd.read_file("countries.shp")
        geom = load_geometry(gdf[gdf.name == "France"])

        # WKT string
        geom = load_geometry("POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))")
    """
    # Convert to GeoJSON dict
    geojson = _to_geojson(geometry)

    # Validate geometry type
    geom_type = geojson.get("type")
    if geom_type not in ("Point", "Polygon", "MultiPolygon", "LineString"):
        raise ValueError(f"Unsupported geometry type: {geom_type}")

    # Count points and simplify if needed
    if geom_type in ("Polygon", "MultiPolygon") and simplify:
        point_count = _count_polygon_points(geojson)
        if point_count > max_points:
            geojson = _simplify_geometry(geojson, max_points)
            new_count = _count_polygon_points(geojson)
            warnings.warn(
                f"Geometry simplified from {point_count} to {new_count} points "
                f"to comply with CMR's {max_points}-point limit."
            )

    return geojson


def _to_geojson(geometry: GeometryLike) -> Dict[str, Any]:
    """Convert various geometry inputs to GeoJSON dict."""

    # Already a dict (GeoJSON)
    if isinstance(geometry, dict):
        # Handle FeatureCollection
        if geometry.get("type") == "FeatureCollection":
            features = geometry.get("features", [])
            if len(features) == 1:
                return features[0].get("geometry", features[0])
            # Multiple features - union them
            return _union_features(features)
        # Handle Feature
        if geometry.get("type") == "Feature":
            return geometry.get("geometry", geometry)
        return geometry

    # File path (string or Path)
    if isinstance(geometry, (str, Path)):
        path = Path(geometry)

        # Check if it's a file
        if path.exists() and path.is_file():
            with open(path) as f:
                data = json.load(f)
            return _to_geojson(data)  # Recurse to handle Feature/FeatureCollection

        # Check if it's WKT
        if isinstance(geometry, str) and geometry.strip().upper().startswith(
            ("POINT", "POLYGON", "MULTIPOLYGON", "LINESTRING")
        ):
            return _wkt_to_geojson(geometry)

        raise ValueError(f"File not found or invalid WKT: {geometry}")

    # Shapely geometry (has __geo_interface__)
    if hasattr(geometry, "__geo_interface__"):
        return geometry.__geo_interface__

    # GeoDataFrame or GeoSeries
    if hasattr(geometry, "unary_union"):
        # This handles both GeoDataFrame and GeoSeries
        union_geom = geometry.unary_union
        return union_geom.__geo_interface__

    raise ValueError(f"Cannot convert {type(geometry)} to GeoJSON")


def _wkt_to_geojson(wkt: str) -> Dict[str, Any]:
    """Convert WKT string to GeoJSON."""
    try:
        from shapely import wkt as shapely_wkt
        geom = shapely_wkt.loads(wkt)
        return geom.__geo_interface__
    except ImportError:
        raise ImportError("shapely is required for WKT parsing: pip install shapely")


def _union_features(features: List[Dict]) -> Dict[str, Any]:
    """Union multiple GeoJSON features into a single geometry."""
    try:
        from shapely.geometry import shape
        from shapely.ops import unary_union

        geometries = [shape(f.get("geometry", f)) for f in features]
        union = unary_union(geometries)
        return union.__geo_interface__
    except ImportError:
        # Fallback: return first feature's geometry
        warnings.warn(
            "shapely not installed; using first feature only. "
            "Install shapely for multi-feature support: pip install shapely"
        )
        return features[0].get("geometry", features[0])


def _count_polygon_points(geojson: Dict[str, Any]) -> int:
    """Count total points in a polygon or multipolygon."""
    geom_type = geojson.get("type")
    coords = geojson.get("coordinates", [])

    if geom_type == "Polygon":
        # Sum points in all rings
        return sum(len(ring) for ring in coords)
    elif geom_type == "MultiPolygon":
        # Sum points in all polygons' rings
        return sum(
            sum(len(ring) for ring in polygon)
            for polygon in coords
        )
    return 0


def _simplify_geometry(
    geojson: Dict[str, Any],
    max_points: int,
) -> Dict[str, Any]:
    """Simplify geometry to have at most max_points vertices.

    Uses Douglas-Peucker algorithm via shapely with iterative tolerance
    adjustment to meet the point limit.
    """
    try:
        from shapely.geometry import shape, mapping
    except ImportError:
        raise ImportError(
            "shapely is required for geometry simplification: pip install shapely"
        )

    geom = shape(geojson)

    # Calculate initial tolerance based on geometry extent
    bounds = geom.bounds  # (minx, miny, maxx, maxy)
    extent = max(bounds[2] - bounds[0], bounds[3] - bounds[1])

    # Start with a small tolerance and increase until under limit
    tolerance = extent / 10000
    max_iterations = 20

    for i in range(max_iterations):
        simplified = geom.simplify(tolerance, preserve_topology=True)

        # Check point count
        simplified_geojson = mapping(simplified)
        point_count = _count_polygon_points(simplified_geojson)

        if point_count <= max_points:
            return simplified_geojson

        # Increase tolerance
        tolerance *= 1.5

    # Final fallback: use convex hull if still too complex
    warnings.warn(
        f"Could not simplify to {max_points} points; using convex hull instead."
    )
    return mapping(geom.convex_hull)
```

### G.3 Integration with GranuleQuery

```python
class GranuleQuery:

    def intersects(
        self,
        geometry: GeometryLike,
        *,
        simplify: bool = True,
        max_points: int = CMR_MAX_POLYGON_POINTS,
    ) -> "GranuleQuery":
        """Filter by geometry intersection.

        Accepts GeoJSON dicts, file paths, shapely geometries, or GeoDataFrames.
        Complex geometries are automatically simplified to comply with CMR's
        300-point limit.

        Args:
            geometry: Geometry input - can be:
                - GeoJSON dict (Point, Polygon, MultiPolygon)
                - Path to GeoJSON file (str or Path)
                - Shapely geometry object
                - GeoDataFrame (uses unary_union of all geometries)
                - WKT string
            simplify: If True, automatically simplify geometries exceeding max_points
            max_points: Maximum points allowed (default: 300 for CMR compliance)

        Returns:
            self for chaining

        Examples:
            # GeoJSON dict
            query.intersects({"type": "Polygon", "coordinates": [...]})

            # GeoJSON file
            query.intersects("study_area.geojson")

            # Shapely geometry
            from shapely.geometry import box
            query.intersects(box(-122, 37, -121, 38))

            # GeoDataFrame (union of all features)
            gdf = gpd.read_file("countries.shp")
            france = gdf[gdf.name == "France"]
            query.intersects(france)  # Auto-simplifies if > 300 points

            # Complex geometry with auto-simplification
            complex_boundary = gpd.read_file("detailed_coastline.geojson")
            query.intersects(complex_boundary)  # Simplified automatically with warning

            # Disable auto-simplification (may fail for complex geometries)
            query.intersects(geometry, simplify=False)
        """
        geojson = load_geometry(geometry, simplify=simplify, max_points=max_points)
        self._validate_geojson(geojson)
        self._params["intersects"] = geojson
        return self
```

### G.4 Convenience Methods

```python
class GranuleQuery:

    @classmethod
    def from_geojson_file(
        cls,
        path: Union[str, Path],
        backend: str = "cmr",
        **kwargs,
    ) -> "GranuleQuery":
        """Create a query with spatial filter from a GeoJSON file.

        Args:
            path: Path to GeoJSON file
            backend: Query backend ("cmr" or "stac")
            **kwargs: Additional query parameters

        Returns:
            GranuleQuery with intersects filter set

        Example:
            query = GranuleQuery.from_geojson_file(
                "study_area.geojson",
                collections=["ATL03"],
                temporal=("2023-01-01", "2023-12-31"),
            )
        """
        query = cls(backend=backend, **kwargs)
        query.intersects(path)
        return query

    @classmethod
    def from_geodataframe(
        cls,
        gdf: "geopandas.GeoDataFrame",
        backend: str = "cmr",
        **kwargs,
    ) -> "GranuleQuery":
        """Create a query with spatial filter from a GeoDataFrame.

        Uses the unary_union of all geometries in the GeoDataFrame.
        Complex geometries are automatically simplified.

        Args:
            gdf: GeoDataFrame with geometry column
            backend: Query backend ("cmr" or "stac")
            **kwargs: Additional query parameters

        Returns:
            GranuleQuery with intersects filter set

        Example:
            import geopandas as gpd

            # Load study areas
            gdf = gpd.read_file("study_areas.shp")

            # Filter for specific area
            roi = gdf[gdf.name == "Amazon Basin"]

            # Create query
            query = GranuleQuery.from_geodataframe(
                roi,
                collections=["GEDI_L4A"],
                temporal=("2023-01-01", "2023-12-31"),
            )
        """
        query = cls(backend=backend, **kwargs)
        query.intersects(gdf)
        return query
```

### G.5 CMR Polygon Point Limit Reference

From CMR documentation:
- **Maximum 300 points** for polygon queries
- Points beyond this limit cause query failures
- Complex coastlines, national boundaries, and detailed ROIs often exceed this limit

Our implementation:
- **Auto-simplifies** using Douglas-Peucker algorithm
- **Preserves topology** to avoid self-intersections
- **Falls back to convex hull** if simplification isn't sufficient
- **Warns users** when simplification occurs

---

## Group H: Query Widget (Optional)

### H.1 Design Goals

- **Visual query builder** using ipyleaflet for interactive map-based selection
- **Spatio-temporal controls** in a unified interface
- **Export to GranuleQuery** for use in scripts

> **Note**: This is an optional enhancement, not a core requirement. The widget will be in a separate module that requires additional dependencies.

### H.2 Widget Architecture

```python
# earthaccess/widgets/query_builder.py

from typing import Optional, Tuple, List
from datetime import date, datetime

# These imports are optional - widget only works in Jupyter
try:
    import ipyleaflet
    import ipywidgets as widgets
    from IPython.display import display
    HAS_WIDGETS = True
except ImportError:
    HAS_WIDGETS = False


class QueryBuilderWidget:
    """Interactive widget for building earthaccess queries.

    Provides a map interface for spatial selection and date pickers for
    temporal filtering. The widget state can be exported to a GranuleQuery
    or CollectionQuery.

    Requires:
        pip install ipyleaflet ipywidgets

    Example:
        from earthaccess.widgets import QueryBuilderWidget

        # Create and display widget
        builder = QueryBuilderWidget()
        builder.show()

        # After interacting with the widget...
        query = builder.to_query()
        results = earthaccess.search_data(query)
    """

    def __init__(
        self,
        center: Tuple[float, float] = (0, 0),
        zoom: int = 2,
        collections: Optional[List[str]] = None,
    ):
        """Initialize the query builder widget.

        Args:
            center: Initial map center (lat, lon)
            zoom: Initial zoom level
            collections: Pre-selected collection IDs
        """
        if not HAS_WIDGETS:
            raise ImportError(
                "QueryBuilderWidget requires ipyleaflet and ipywidgets. "
                "Install with: pip install ipyleaflet ipywidgets"
            )

        self._collections = collections or []
        self._bbox: Optional[Tuple[float, float, float, float]] = None
        self._polygon: Optional[List[Tuple[float, float]]] = None
        self._temporal: Optional[Tuple[Optional[datetime], Optional[datetime]]] = None

        # Create map
        self._map = ipyleaflet.Map(
            center=center,
            zoom=zoom,
            layout=widgets.Layout(height="400px"),
        )

        # Add drawing controls
        self._draw_control = ipyleaflet.DrawControl(
            rectangle={"shapeOptions": {"color": "#3388ff"}},
            polygon={"shapeOptions": {"color": "#3388ff"}},
            polyline={},
            circle={},
            marker={},
            circlemarker={},
        )
        self._draw_control.on_draw(self._handle_draw)
        self._map.add_control(self._draw_control)

        # Create date pickers
        self._start_date = widgets.DatePicker(
            description="Start:",
            value=None,
        )
        self._end_date = widgets.DatePicker(
            description="End:",
            value=None,
        )

        # Collection input
        self._collection_input = widgets.Text(
            description="Collection:",
            placeholder="e.g., ATL03, HLSS30.002",
            value=", ".join(self._collections) if self._collections else "",
        )

        # Cloud cover slider
        self._cloud_cover = widgets.IntRangeSlider(
            value=[0, 100],
            min=0,
            max=100,
            step=1,
            description="Cloud %:",
        )

        # Provider dropdown
        self._provider = widgets.Dropdown(
            description="Provider:",
            options=["Any", "NSIDC_CPRD", "POCLOUD", "LPCLOUD", "GES_DISC", "ASF"],
            value="Any",
        )

        # Cloud hosted checkbox
        self._cloud_hosted = widgets.Checkbox(
            description="Cloud hosted only",
            value=False,
        )

        # Status output
        self._status = widgets.HTML(value="Draw a rectangle or polygon on the map to set spatial filter.")

        # Export button
        self._export_btn = widgets.Button(
            description="Export Query",
            button_style="primary",
        )
        self._export_btn.on_click(self._on_export)

        # Clear button
        self._clear_btn = widgets.Button(
            description="Clear",
            button_style="warning",
        )
        self._clear_btn.on_click(self._on_clear)

        # Layout
        self._controls = widgets.VBox([
            widgets.HBox([self._start_date, self._end_date]),
            self._collection_input,
            widgets.HBox([self._cloud_cover, self._cloud_hosted]),
            self._provider,
            widgets.HBox([self._export_btn, self._clear_btn]),
            self._status,
        ])

    def _handle_draw(self, target, action, geo_json):
        """Handle draw events from the map."""
        if action == "created":
            geom_type = geo_json["geometry"]["type"]
            coords = geo_json["geometry"]["coordinates"]

            if geom_type == "Polygon":
                # Extract coordinates
                ring = coords[0]
                self._polygon = [(c[0], c[1]) for c in ring]

                # Calculate bounding box
                lons = [c[0] for c in ring]
                lats = [c[1] for c in ring]
                self._bbox = (min(lons), min(lats), max(lons), max(lats))

                self._status.value = (
                    f"<b>Polygon:</b> {len(self._polygon)} points<br>"
                    f"<b>Bbox:</b> {self._bbox}"
                )

    def _on_clear(self, btn):
        """Clear all selections."""
        self._bbox = None
        self._polygon = None
        self._draw_control.clear()
        self._status.value = "Cleared. Draw a new shape on the map."

    def _on_export(self, btn):
        """Export current state to query."""
        query = self.to_query()
        self._status.value = f"<pre>{repr(query)}</pre>"

    def show(self):
        """Display the widget."""
        display(widgets.VBox([self._map, self._controls]))

    def to_query(self, query_type: str = "granule") -> "GranuleQuery":
        """Export widget state to a GranuleQuery or CollectionQuery.

        Args:
            query_type: "granule" or "collection"

        Returns:
            GranuleQuery or CollectionQuery with widget state
        """
        from earthaccess import GranuleQuery, CollectionQuery

        # Parse collections
        collections = None
        if self._collection_input.value:
            collections = [c.strip() for c in self._collection_input.value.split(",") if c.strip()]

        # Parse temporal
        start = self._start_date.value
        end = self._end_date.value
        temporal = None
        if start or end:
            temporal = (start, end)

        # Parse cloud cover
        cloud_cover = None
        cc_min, cc_max = self._cloud_cover.value
        if cc_min > 0 or cc_max < 100:
            cloud_cover = (cc_min, cc_max)

        # Parse provider
        provider = None
        if self._provider.value != "Any":
            provider = self._provider.value

        # Build query
        if query_type == "granule":
            query = GranuleQuery(backend="cmr")

            if collections:
                query.collections(collections)
            if self._polygon:
                query.polygon(self._polygon)
            elif self._bbox:
                query.bounding_box(self._bbox)
            if temporal:
                query.temporal(*temporal)
            if cloud_cover:
                query.cloud_cover(*cloud_cover)
            if provider:
                query.provider(provider)
            if self._cloud_hosted.value:
                query.cloud_hosted(True)

            return query
        else:
            query = CollectionQuery(backend="cmr")

            if collections:
                query.short_name(collections[0])
            if self._bbox:
                query.bounding_box(self._bbox)
            if temporal:
                query.temporal(*temporal)
            if provider:
                query.provider(provider)
            if self._cloud_hosted.value:
                query.cloud_hosted(True)

            return query

    @property
    def bbox(self) -> Optional[Tuple[float, float, float, float]]:
        """Get the current bounding box selection."""
        return self._bbox

    @property
    def polygon(self) -> Optional[List[Tuple[float, float]]]:
        """Get the current polygon selection."""
        return self._polygon

    @property
    def temporal(self) -> Optional[Tuple[Optional[date], Optional[date]]]:
        """Get the current temporal selection."""
        start = self._start_date.value
        end = self._end_date.value
        if start or end:
            return (start, end)
        return None
```

### H.3 Usage Example

```python
# In a Jupyter notebook
from earthaccess.widgets import QueryBuilderWidget

# Create and display the widget
builder = QueryBuilderWidget(
    center=(37.7749, -122.4194),  # San Francisco
    zoom=8,
    collections=["HLSS30.002"],
)
builder.show()

# [User interacts with the map and controls...]

# Export to a query
query = builder.to_query()
print(query)
# GranuleQuery(backend='cmr', params={'collections': ['HLSS30.002'],
#              'bounding_box': (-122.5, 37.5, -122.0, 38.0),
#              'temporal': (datetime(2023, 1, 1), datetime(2023, 12, 31))})

# Use in search
results = earthaccess.search_data(query)
```

### H.4 Installation

The widget is optional and requires additional dependencies:

```bash
# Core earthaccess (no widget)
pip install earthaccess

# With widget support
pip install earthaccess[widgets]
# or
pip install earthaccess ipyleaflet ipywidgets
```

### H.5 Future Enhancements

- **Load existing geometry**: Import GeoJSON files into the map
- **Preview results**: Show granule footprints on the map
- **Time slider**: Animate through temporal range
- **Collection search**: Search collections by keyword within the widget
- **Save/load state**: Persist widget state for reproducibility

---

## Group I: Documentation & Examples

> **Status**: 🔲 Not Started
>
> Documentation needs significant improvement to help users understand the new parallel execution features and provide real-world usage examples.

### I.1 Documentation Improvements

The following documentation updates are needed:

1. **Parallel Execution Guide**
   - Document the `parallel` parameter on `open()` and `download()` methods
   - Explain when to use each executor type (ThreadPool, Dask, Lithops)
   - Performance considerations and benchmarks
   - Examples with Dask distributed clusters

2. **TargetLocation Documentation**
   - How to download directly to cloud storage (S3, GCS, Azure)
   - Storage options configuration
   - Cross-cloud data movement patterns

3. **Migration Guide**
   - Changes from pqdm-based parallel execution
   - New parameter names and behaviors
   - Deprecation notices

### I.2 Real-World Examples Page

Create a comprehensive tutorial/examples page showcasing realistic use cases:

#### I.2.1 HLS (Harmonized Landsat Sentinel-2) Example

```python
import earthaccess
import xarray as xr

# Authenticate
earthaccess.login()

# Search for HLS granules over an area of interest
granules = earthaccess.search_data(
    short_name="HLSS30",
    bounding_box=(-122.5, 37.5, -122.0, 38.0),  # San Francisco Bay Area
    temporal=("2024-06-01", "2024-06-30"),
    cloud_cover=(0, 20),
    count=10,
)

print(f"Found {len(granules)} granules")

# Open specific bands using virtualizarr
vds = earthaccess.open_virtual_mfdataset(
    granules,
    access="indirect",
    concat_dim="time",
    coords="minimal",
    compat="override",
)

# Select only the bands we need (RGB + NIR)
bands = ["B02", "B03", "B04", "B08"]
ds = vds[bands]

# Calculate NDVI
ds["NDVI"] = (ds["B08"] - ds["B04"]) / (ds["B08"] + ds["B04"])

# Plot a time series
ds["NDVI"].mean(dim=["x", "y"]).plot()
```

#### I.2.2 MUR SST (Sea Surface Temperature) Example

```python
import earthaccess
import matplotlib.pyplot as plt

# Search for MUR SST data
granules = earthaccess.search_data(
    short_name="MUR-JPL-L4-GLOB-v4.1",
    temporal=("2024-01-01", "2024-01-07"),
    count=7,
)

# Open as virtual dataset for efficient access
vds = earthaccess.open_virtual_mfdataset(
    granules,
    access="indirect",
    load=True,
    concat_dim="time",
    coords="minimal",
    compat="override",
    combine_attrs="drop_conflicts",
)

# Select a region (Gulf of Mexico)
gulf_sst = vds["analysed_sst"].sel(
    lat=slice(18, 31),
    lon=slice(-98, -80),
)

# Plot SST anomaly
gulf_sst.mean(dim="time").plot(cmap="RdBu_r", vmin=290, vmax=305)
plt.title("Mean SST - Gulf of Mexico (Jan 2024)")
```

#### I.2.3 ICESat-2 ATL08 (Land/Vegetation Height) Example

```python
import earthaccess
from pathlib import Path

# Search for ATL08 data over a forest region
granules = earthaccess.search_data(
    short_name="ATL08",
    bounding_box=(-122.5, 37.5, -122.0, 38.0),
    temporal=("2023-01-01", "2023-12-31"),
    count=20,
)

# Download with parallel execution
files = earthaccess.download(
    granules,
    path="./atl08_data",
    parallel="threads",  # Use thread pool
    max_workers=4,
)

print(f"Downloaded {len(files)} files to ./atl08_data")

# Process with h5py
import h5py
for f in files[:3]:
    with h5py.File(f, "r") as h5:
        # Access vegetation height data
        heights = h5["/gt1l/land_segments/canopy/h_canopy"][:]
        print(f"{f.name}: {len(heights)} canopy height measurements")
```

#### I.2.4 Parallel Download to Cloud Storage Example

```python
import earthaccess

# Download directly to S3 bucket
granules = earthaccess.search_data(
    short_name="GEDI_L2A",
    temporal=("2024-01"),
    count=50,
)

# Download to S3 with Dask distributed
files = earthaccess.download(
    granules,
    path="s3://my-bucket/gedi-data/",
    parallel="dask",
    max_workers=16,
)
```

#### I.2.5 EMIT Hyperspectral Example

```python
import earthaccess

# Search for EMIT hyperspectral data
granules = earthaccess.search_data(
    short_name="EMITL2ARFL",
    bounding_box=(-118.5, 34.0, -118.0, 34.5),  # Los Angeles
    temporal=("2024-01", "2024-06"),
    count=5,
)

# Open with virtualizarr
vds = earthaccess.open_virtual_mfdataset(
    granules,
    access="indirect",
    load=True,
)

# Access reflectance data
print(f"Dimensions: {vds.dims}")
print(f"Variables: {list(vds.data_vars)}")
```

### I.3 Implementation Tasks

| Task | Priority | Status |
|------|----------|--------|
| Create parallel execution user guide | High | 🔲 Pending |
| Create TargetLocation documentation | High | 🔲 Pending |
| Create real-world examples page | High | 🔲 Pending |
| Add HLS tutorial example | High | 🔲 Pending |
| Add MUR SST tutorial example | High | 🔲 Pending |
| Add ICESat-2 ATL08 example | Medium | 🔲 Pending |
| Add cloud-to-cloud download example | Medium | 🔲 Pending |
| Add EMIT hyperspectral example | Medium | 🔲 Pending |
| Update API reference documentation | High | 🔲 Pending |
| Create migration guide from previous versions | Medium | 🔲 Pending |

---

## Group J: VirtualiZarr Improvements

> **Status**: 🔲 Not Started
>
> The virtualizarr integration needs refactoring to provide consistent API between single and multi-file openers, update to the latest virtualizarr version, and review dependency management.

### J.1 Current Issues

#### J.1.1 API Inconsistency

The `open_virtual_dataset` (single granule) and `open_virtual_mfdataset` (multiple granules) have inconsistent APIs:

**`open_virtual_mfdataset` parameters:**
- `granules`, `group`, `access` ✅
- `preprocess` ✅
- `parallel` ✅
- `load` ✅ - Controls coordinate materialization
- `reference_dir` ✅
- `reference_format` ✅
- `**xr_combine_nested_kwargs` ✅

**`open_virtual_dataset` parameters:**
- `granule`, `group`, `access` ✅
- `preprocess` ❌ Missing
- `parallel` ❌ Missing (hardcoded to False)
- `load` ❌ Missing
- `reference_dir` ❌ Missing
- `reference_format` ❌ Missing

This means users cannot:
- Choose whether to materialize coordinates for single granules
- Use the same workflow for single vs. multiple granules

#### J.1.2 Outdated virtualizarr Version

- **Current pinned version**: `>=2.1.2`
- **Latest available version**: `2.2.1`
- **Update needed**: Review changelog for breaking changes and update minimum version

#### J.1.3 Dependency Confusion

Users frequently encounter import errors because virtualizarr and related packages are optional:

```python
>>> import earthaccess
>>> vds = earthaccess.open_virtual_dataset(granules[0])
ImportError: `earthaccess.open_virtual_dataset` requires `pip install earthaccess[virtualizarr]`
```

**Options to consider:**
1. Move virtualizarr to core dependencies
2. Improve error messages with clear installation instructions
3. Add optional import guards with helpful warnings
4. Document clearly in README which features require extras

### J.2 Proposed Changes

#### J.2.1 Align `open_virtual_dataset` with `open_virtual_mfdataset`

```python
def open_virtual_dataset(
    granule: earthaccess.DataGranule,
    group: str | None = None,
    access: str = "indirect",
    load: bool = True,  # NEW: Control coordinate materialization
    reference_dir: str | None = None,  # NEW
    reference_format: Literal["json", "parquet"] = "json",  # NEW
) -> xr.Dataset:
    """Open a granule as a single virtual xarray Dataset.

    Parameters:
        granule: The granule to open
        group: Path to the netCDF4 group to open
        access: "direct" (S3) or "indirect" (HTTPS)
        load: If True, materialize coordinates for lazy indexing.
              If False, return pure virtual dataset with ManifestArrays.
        reference_dir: Directory to store kerchunk references (if load=True)
        reference_format: Reference format - "json" or "parquet"

    Returns:
        xarray.Dataset
    """
    return open_virtual_mfdataset(
        granules=[granule],
        group=group,
        access=access,
        parallel=False,
        preprocess=None,
        load=load,
        reference_dir=reference_dir,
        reference_format=reference_format,
    )
```

#### J.2.2 Update virtualizarr Version

Update `pyproject.toml`:

```toml
[project.optional-dependencies]
virtualizarr = [
    "numpy >=1.26.4",
    "zarr >=3.1.1",
    "virtualizarr >=2.2.0",  # Updated from 2.1.2
    # ... rest unchanged
]
```

#### J.2.3 Dependency Consolidation Options

**Option A: Move to core dependencies**

Pros:
- Simpler user experience
- No import errors
- Features work out of the box

Cons:
- Larger install size
- May conflict with user environments
- Not all users need these features

**Option B: Keep optional with better UX**

```python
# In earthaccess/__init__.py
def open_virtual_dataset(*args, **kwargs):
    try:
        from .dmrpp_zarr import open_virtual_dataset as _open_vds
        return _open_vds(*args, **kwargs)
    except ImportError:
        raise ImportError(
            "Virtual dataset support requires additional dependencies.\n"
            "Install with: pip install 'earthaccess[virtualizarr]'\n"
            "Or: conda install -c conda-forge earthaccess virtualizarr"
        )
```

**Recommended: Option B** - Keep optional but improve error messages and documentation.

### J.3 Implementation Tasks

| Task | Priority | Status |
|------|----------|--------|
| Add `load`, `reference_dir`, `reference_format` to `open_virtual_dataset` | High | 🔲 Pending |
| Update virtualizarr minimum version to 2.2.0 | Medium | 🔲 Pending |
| Review virtualizarr 2.2.x changelog for breaking changes | Medium | 🔲 Pending |
| Improve import error messages for optional deps | High | 🔲 Pending |
| Add virtualizarr installation to quick-start docs | High | 🔲 Pending |
| Consider xarray as core dependency | Medium | 🔲 Pending |
| Add integration tests for virtualizarr features | Medium | 🔲 Pending |
| Document coord materialization behavior | High | 🔲 Pending |

### Phase 0: Store Refactoring (Week 0-2) - Pre-requisite

> **Status: PARTIALLY COMPLETE** - Core parallel execution system implemented via `maxparallelism` branch.

| Task | Priority | Status | Notes |
|------|----------|--------|-------|
| Create `earthaccess/store/` package structure | High | ⏸️ Deferred | Implemented in `parallel.py` and `target_filesystem.py` instead |
| Implement `ParallelExecutor` abstract base class | High | ✅ Done | `Executor` ABC in `earthaccess/parallel.py` |
| Implement `ThreadPoolParallelExecutor` | High | ✅ Done | `ThreadPoolExecutorWrapper` in `parallel.py` |
| Implement `SequentialExecutor` (for testing) | High | ✅ Done | `SerialExecutor` in `parallel.py` |
| Implement `DaskParallelExecutor` | Medium | ✅ Done | `DaskDelayedExecutor` in `parallel.py` |
| Implement `LithopsParallelExecutor` | Low | ✅ Done | `LithopsEagerFunctionExecutor` in `parallel.py` |
| Extract `CredentialManager` class | High | 🔲 Pending | Credentials still managed inline in Store |
| Extract `FileSystemFactory` class | High | 🔲 Pending | Not yet extracted |
| Implement URL-to-provider inference | Medium | 🔲 Pending | Not yet implemented |
| Refactor `Store` class as facade | High | ⏸️ Partial | Added parallel integration, not full refactor |
| Migrate `pqdm` calls to `ParallelExecutor` | High | ✅ Done | pqdm removed, uses `get_executor()` |
| Implement `TargetFilesystem` ABC | High | ✅ Done | `earthaccess/target_filesystem.py` |
| Implement `LocalFilesystem` | High | ✅ Done | `target_filesystem.py` |
| Implement `FsspecFilesystem` | High | ✅ Done | `target_filesystem.py` |
| Add `parallel` parameter to Store methods | High | ✅ Done | All download/open methods support it |
| Update all tests for new architecture | High | ✅ Done | Tests in `test_parallel.py`, `test_target_filesystem.py` |
| Ensure 100% backwards compatibility | High | ✅ Done | All existing APIs unchanged |
| Update documentation | Medium | 🔲 Pending | Needs user documentation |

### Phase 1: Query Classes (Week 2-4)

> **Status: NOT STARTED** - Depends on Phase 0 remaining items.

| Task | Priority | Status | Notes |
|------|----------|--------|-------|
| Create `GranuleQuery` class with validation | High | 🔲 Pending | See Group A in plan |
| Create `CollectionQuery` class | High | 🔲 Pending | See Group A in plan |
| Implement `to_stac()` conversion | High | 🔲 Pending | |
| Implement `to_cmr()` conversion | High | 🔲 Pending | |
| Add parameter validation | High | 🔲 Pending | |
| Integrate with `search_data()` | High | 🔲 Pending | |
| Integrate with `search_datasets()` | High | 🔲 Pending | |
| Add unit tests | High | 🔲 Pending | |

### Phase 2: Results Classes (Week 4-5)

> **Status: NOT STARTED** - Depends on Phase 1.

| Task | Priority | Status | Notes |
|------|----------|--------|-------|
| Create `GranuleResults` with lazy pagination | High | 🔲 Pending | See Group B in plan |
| Create `CollectionResults` | High | 🔲 Pending | |
| Implement `cmr-search-after` pagination | High | 🔲 Pending | |
| Add `get_all()` method | High | 🔲 Pending | |
| Add `to_stac()` on results | Medium | 🔲 Pending | |
| Add `to_umm()` on results | Medium | 🔲 Pending | |
| Add tests | High | 🔲 Pending | |

### Phase 3: Asset Access (Week 5-6)

> **Status: NOT STARTED** - Depends on Phase 2.

| Task | Priority | Status | Notes |
|------|----------|--------|-------|
| Create `Asset` dataclass | High | 🔲 Pending | See Group C in plan |
| Add `.assets` property to `DataGranule` | High | 🔲 Pending | |
| Add `.get_assets()` method | High | 🔲 Pending | |
| Create `AssetFilter` dataclass | Medium | 🔲 Pending | |
| Update `open()` with filtering | Medium | 🔲 Pending | |
| Update `download()` with filtering | Medium | 🔲 Pending | |
| Add tests | High | 🔲 Pending | |

### Phase 4: STAC Conversion (Week 6-7)

> **Status: NOT STARTED** - Depends on Phase 3.

| Task | Priority | Status | Notes |
|------|----------|--------|-------|
| Implement `DataGranule.to_stac()` | High | 🔲 Pending | See Group D in plan |
| Implement `DataGranule.from_stac_item()` | High | 🔲 Pending | |
| Implement `DataCollection.to_stac()` | Medium | 🔲 Pending | |
| Add geometry conversion utilities | Medium | 🔲 Pending | |
| Add tests | High | 🔲 Pending | |

### Phase 5: External STAC (Week 7-8)

> **Status: NOT STARTED** - Depends on Phase 4.

| Task | Priority | Status | Notes |
|------|----------|--------|-------|
| Implement `search_stac()` function | Medium | 🔲 Pending | See Group E in plan |
| Create `STACItemResults` class | Medium | 🔲 Pending | |
| Add integration tests | Medium | 🔲 Pending | |
| Documentation | High | 🔲 Pending | |

### Phase 6: Flexible Inputs & Geometry (Week 8-9)

> **Status: NOT STARTED** - Can be done in parallel with other phases.

| Task | Priority | Status | Notes |
|------|----------|--------|-------|
| Update methods to accept any iterable (list/tuple/array) | High | 🔲 Pending | See Group G in plan |
| Add `_normalize_*` helper methods | High | 🔲 Pending | |
| Implement `load_geometry()` function | High | 🔲 Pending | |
| Add GeoJSON file loading | High | 🔲 Pending | |
| Add shapely geometry support | High | 🔲 Pending | |
| Add GeoDataFrame support | Medium | 🔲 Pending | |
| Implement geometry simplification (≤300 points) | High | 🔲 Pending | |
| Add WKT parsing | Low | 🔲 Pending | |
| Add unit tests for flexible inputs | High | 🔲 Pending | |
| Add tests for geometry simplification | High | 🔲 Pending | |

### Phase 7: Query Widget (Optional, Week 9-10)

> **Status: NOT STARTED** - Optional feature, low priority.

| Task | Priority | Status | Notes |
|------|----------|--------|-------|
| Create `QueryBuilderWidget` class | Low | 🔲 Pending | See Group H in plan |
| Implement ipyleaflet map integration | Low | 🔲 Pending | |
| Add drawing controls (rectangle, polygon) | Low | 🔲 Pending | |
| Add date pickers | Low | 🔲 Pending | |
| Implement `to_query()` export | Low | 🔲 Pending | |
| Add to optional dependencies in pyproject.toml | Low | 🔲 Pending | |
| Documentation | Low | 🔲 Pending | |

---

## Migration Guide

### Existing Code (Unchanged)

```python
# This continues to work exactly as before
granules = earthaccess.search_data(
    short_name="ATL03",
    temporal=("2023-01-01", "2023-12-31"),
    count=100,
)

for g in granules:
    urls = g.data_links()  # Still works
```

### New Query-Based API

```python
# Option 1: Query object with method chaining
query = (
    earthaccess.GranuleQuery(backend="cmr")
    .collections(["ATL03"])
    .temporal("2023-01-01", "2023-12-31")
    .cloud_hosted(True)
)
results = earthaccess.search_data(query)

# Option 2: Query object with named parameters
query = earthaccess.GranuleQuery(
    backend="cmr",
    collections=["ATL03"],
    temporal=("2023-01-01", "2023-12-31"),
    cloud_hosted=True,
)
results = earthaccess.search_data(query)

# Get pystac-client compatible params
stac_params = query.to_stac()
```

### New Asset Access

```python
# STAC-style asset access (recommended)
for g in granules:
    # Direct access
    data_url = g.assets["data"].href

    # Filtered access
    s3_assets = g.get_assets(roles=["cloud-optimized"])

# Filtered open()
files = earthaccess.open(granules, name_pattern="*B0[234].tif")
```

### Lazy Pagination

```python
# New: Lazy iteration
query = earthaccess.GranuleQuery(backend="cmr", collections=["ATL03"])
results = query.execute()  # Returns GranuleResults

# Iterate lazily (fetches pages on-demand)
for granule in results:
    process(granule)

# Or get all at once (backwards compatible)
all_granules = results.get_all()
```

### Flexible Input Types (New)

```python
# No more "must be a tuple" errors!

# Bounding box - all of these work:
query.bounding_box(-180, -90, 180, 90)              # Separate values
query.bounding_box([-180, -90, 180, 90])            # List
query.bounding_box((-180, -90, 180, 90))            # Tuple
query.bounding_box(np.array([-180, -90, 180, 90]))  # NumPy array
query.bounding_box(gdf.total_bounds)                # GeoPandas bounds

# Temporal - all of these work:
query.temporal("2023-01-01", "2023-12-31")          # Separate strings
query.temporal(["2023-01-01", "2023-12-31"])        # List
query.temporal(("2023-01-01", "2023-12-31"))        # Tuple
query.temporal("2023-01-01/2023-12-31")             # Interval string

# Cloud cover - all of these work:
query.cloud_cover(0, 20)                            # Separate values
query.cloud_cover([0, 20])                          # List
query.cloud_cover((0, 20))                          # Tuple
```

### Geometry Loading (New)

```python
# Load from GeoJSON file
query.intersects("study_area.geojson")

# Load from shapely geometry
from shapely.geometry import box
query.intersects(box(-122, 37, -121, 38))

# Load from GeoDataFrame (auto-unions features)
import geopandas as gpd
gdf = gpd.read_file("countries.shp")
france = gdf[gdf.name == "France"]
query.intersects(france)

# Complex geometries auto-simplified to ≤300 points for CMR compliance
# (You'll see a warning when this happens)
```

### Interactive Query Builder (Optional)

```python
# In Jupyter notebook
from earthaccess.widgets import QueryBuilderWidget

builder = QueryBuilderWidget()
builder.show()  # Interactive map with drawing tools

# After drawing on the map...
query = builder.to_query()
results = earthaccess.search_data(query)
```

---

## Summary

This implementation plan provides:

**Pre-requisite: Store Refactoring (Phase 0)**

*Completed:*
- ✅ **Pluggable `Executor` system** - Abstract base class with multiple implementations (Serial, ThreadPool, Dask, Lithops)
- ✅ **`TargetFilesystem` abstraction** - ABC with `LocalFilesystem` and `FsspecFilesystem` implementations
- ✅ **`parallel` parameter** - All Store methods support custom executors
- ✅ **pqdm removal** - Replaced with `get_executor()` factory function
- ✅ **Backwards compatibility** - All existing APIs unchanged

*Remaining:*
- 🔲 **`CredentialManager`** - Dedicated class for S3 credential caching and refresh
- 🔲 **`FileSystemFactory`** - Centralized filesystem creation with authentication
- 🔲 **URL-to-provider inference** - Automatic provider detection from S3 bucket names

**STAC Improvements (Phases 1-7) - NOT YET STARTED**
1. **`GranuleQuery` and `CollectionQuery`** classes with `backend` parameter
2. **Method chaining AND named parameter** support
3. **`query.to_stac()`** for pystac-client compatibility
4. **`query.to_cmr()`** for CMR API compatibility
5. **Validation** based on backend API specs
6. **Lazy pagination** via `GranuleResults`/`CollectionResults`
7. **STAC-style asset access** on `DataGranule`
8. **Asset filtering** in `open()` and `download()`
9. **Bidirectional conversion** via `.to_stac()` and `.to_umm()`
10. **External STAC catalog support** via `search_stac()`
11. **Flexible input types** - lists, tuples, arrays, any iterable
12. **Geometry loading** from GeoJSON files, shapely, GeoDataFrame
13. **Auto-simplification** of complex geometries (≤300 points for CMR)
14. **Optional query widget** for interactive spatio-temporal selection
15. **100% backwards compatible** - existing API unchanged
