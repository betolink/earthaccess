# Migration Guide: v0.x → v1.0.0

This guide helps you upgrade from earthaccess **v0.x** (current stable) to **v1.0.0** (major release).

**Target audience:** All earthaccess users upgrading to v1.0.0.

---

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Breaking Changes](#breaking-changes)
4. [New Features](#new-features)
5. [Import Path Changes](#import-path-changes)
6. [API Changes](#api-changes)
7. [Behavioral Changes](#behavioral-changes)
8. [Deprecations](#deprecations)
9. [Step-by-Step Migration](#step-by-step-migration)
10. [Testing Your Migration](#testing-your-migration)
11. [Rollback Plan](#rollback-plan)
12. [Getting Help](#getting-help)

---

## Overview

earthaccess v1.0.0 represents a major milestone with significant improvements:

- **Modular package structure** - Better organized code
- **Type-safe query builders** - Better developer experience
- **Rich display features** - Enhanced Jupyter notebook integration
- **STAC interoperability** - Bidirectional CMR/STAC conversion
- **Performance improvements** - Lazy pagination, parallel processing
- **Enhanced authentication** - Better session management and credentials

**Key Philosophy:** Maintain backward compatibility while introducing opt-in improvements.

---

## Quick Start

### Installation

```bash
# Upgrade to v1.0.0
pip install --upgrade earthaccess

# Or with optional extras
pip install --upgrade earthaccess[widgets,virtualizarr]
```

### Minimal Changes Required

For most users, only **one change** is required:

```python
import earthaccess

# ✅ Add this line (was previously automatic)
earthaccess.login()

# Your existing code should work as-is
granules = earthaccess.search_data(short_name="ATL06", count=10)
files = earthaccess.open(granules)
```

---

## Breaking Changes

### 1. Explicit Login Required

**What changed:** Automatic login behavior has been removed.

**Before (v0.x):**
```python
import earthaccess

# Login happened automatically if credentials were available
granules = earthaccess.search_data(short_name="ATL06")
```

**After (v1.0.0):**
```python
import earthaccess

# ✅ Explicit login required
earthaccess.login()

granules = earthaccess.search_data(short_name="ATL06")
```

**Why:** Explicit is better than implicit. Auto-login led to confusing errors when credentials were missing or incorrect.

**Migration:** Add `earthaccess.login()` at the beginning of your script/notebook.

---

### 2. SearchResults `len()` Behavior

**What changed:** `len(results)` now returns the count of loaded results, not total CMR hits.

**Before (v0.x):**
```python
results = earthaccess.search_data(short_name="ATL06", count=10)
len(results)  # Returned total CMR hits (e.g., 50000)
```

**After (v1.0.0):**
```python
results = earthaccess.search_data(short_name="ATL06", count=10)
len(results)       # Returns loaded count (10)
results.total()    # Returns total CMR hits (50000)
```

**Why:** Makes `SearchResults` behave like a standard Python sequence. More intuitive when iterating.

**Migration:** Replace `len(results)` with `results.total()` when you need total CMR hits.

---

### 3. Import Path Changes

**What changed:** Internal package structure reorganized into logical sub-packages.

| Old Import (v0.x) | New Import (v1.0.0) |
|-------------------|-------------------|
| `from earthaccess.query import GranuleQuery` | `from earthaccess.search import GranuleQuery` |
| `from earthaccess.daac import DAACS` | `from earthaccess.store.daac import DAACS` |
| `from earthaccess.results import DataGranule` | `from earthaccess.search import DataGranule` |

**Recommended:** Use top-level imports instead:
```python
from earthaccess import GranuleQuery, BoundingBox, DataGranule
```

**Why:** Better code organization and clearer API boundaries.

**Migration:** Update imports or use top-level exports (recommended).

---

### 4. SessionWithHeaderRedirection Deprecated

**What changed:** `SessionWithHeaderRedirection` is now deprecated and will be removed in v2.0.0.

**Before (v0.x):**
```python
from earthaccess.auth import SessionWithHeaderRedirection
session = SessionWithHeaderRedirection("urs.earthdata.nasa.gov", ("user", "pass"))
```

**After (v1.0.0):**
```python
# ✅ Option 1: Use Auth.get_session() (recommended)
import earthaccess
auth = earthaccess.login()
session = auth.get_session()

# ✅ Option 2: Use _create_earthdata_session() for low-level use
from earthaccess.auth import _create_earthdata_session
session = _create_earthdata_session("urs.earthdata.nasa.gov", ("user", "pass"))
```

**Why:** The factory function approach is cleaner and uses response hooks instead of subclassing.

**Migration:** Use `auth.get_session()` for authenticated sessions, or `_create_earthdata_session()` for low-level session creation.

---

## New Features

### Query Builders

Build type-safe queries with method chaining and validation:

```python
from earthaccess import GranuleQuery

query = (
    GranuleQuery()
    .short_name("ATL06")
    .temporal("2023-01-01", "2023-06-30")
    .bounding_box(-180, -90, 180, 90)
    .cloud_hosted(True)
)

# Validate before execution
result = query.validate()
if not result.is_valid:
    print(result.errors)

# Execute query
granules = earthaccess.search_data(query=query)
```

**Features:**
- Type checking and validation
- Convert to STAC format: `query.to_stac()`
- Load geometries from files: `.polygon(file="boundary.geojson")`

---

### Rich HTML Display

In Jupyter notebooks, results now display as formatted HTML:

```python
granules = earthaccess.search_data(short_name="ATL06", count=10)
granules  # Displays as interactive HTML table with pagination
```

Features:
- Interactive pagination (First/Previous/Next/Last)
- "Total in CMR" vs "Loaded" counts
- Enhanced granule/collection cards
- Auth status display

---

### Interactive Maps

Visualize spatial extent with interactive maps (requires `[widgets]` extra):

```bash
pip install earthaccess[widgets]
```

```python
# Show map for search results
granules = earthaccess.search_data(short_name="ATL06", count=100)
granules.show_map()

# Show single granule
granules[0].show_map()

# Show collection extent
collections = earthaccess.search_datasets(short_name="ATL06")
collections[0].show_map()
```

---

### STAC Conversion

Convert between CMR and STAC formats:

```python
# DataGranule to STAC Item
granule = granules[0]
stac_item = granule.to_stac()

# DataCollection to STAC Collection
collection = collections[0]
stac_collection = collection.to_stac()

# Use the stac module directly
from earthaccess.stac import umm_granule_to_stac_item
```

---

### Lazy Pagination

Efficiently iterate over large result sets:

```python
from earthaccess import SearchResults, GranuleQuery

query = GranuleQuery().short_name("ATL06").temporal("2023-01", "2023-12")
results = SearchResults(query, limit=10000)

# Lazy iteration (pystac-client compatible)
for granule in results.items():
    process(granule)

# Page-by-page iteration
for page in results.pages(page_size=100):
    batch_process(page)

# Get all at once (use with caution for large result sets)
all_granules = results.all()
```

---

### Enhanced Filtering

Filter results by various criteria:

```python
# Filter by size (in MB)
large_granules = results.filter(min_size=1000)

# Filter cloud-hosted only
cloud_granules = results.filter(cloud_hosted=True)

# Custom predicate
def is_valid(granule):
    return "ATL06" in granule["producer_granule_id"]

filtered = results.filter(predicate=is_valid)
```

---

### Virtual Datasets

Enhanced virtual dataset support with multiple parsers:

```python
from earthaccess import open_virtual_dataset

# Automatic parser selection
vds = open_virtual_dataset(granule)

# Specify parser explicitly
vds = open_virtual_dataset(granule, parser="dmrpp")  # or "hdf5", "netcdf3"

# Multi-file datasets
from earthaccess import open_virtual_mfdataset
vds = open_virtual_mfdataset(granules)
```

---

### Cloud Storage Downloads

Download directly to cloud storage:

```python
from earthaccess import TargetLocation

# Download to S3
target = TargetLocation(
    "s3://my-bucket/earthdata/",
    storage_options={"profile": "my-aws-profile"}
)
files = earthaccess.download(granules, target)

# Download to GCS
target = TargetLocation("gs://my-bucket/earthdata/")
files = earthaccess.download(granules, target)
```

---

## Import Path Changes

### Complete Import Mapping

| Category | Old Import (v0.x) | New Import (v1.0.0) |
|----------|-------------------|-------------------|
| **Query Classes** | `from earthaccess.query import GranuleQuery` | `from earthaccess.search import GranuleQuery` |
| | `from earthaccess.query import BoundingBox` | `from earthaccess.search import BoundingBox` |
| **Results** | `from earthaccess.results import DataGranule` | `from earthaccess.search import DataGranule` |
| | `from earthaccess.results import DataCollection` | `from earthaccess.search import DataCollection` |
| **DAAC Info** | `from earthaccess.daac import DAACS` | `from earthaccess.store.daac import DAACS` |
| **Store** | `from earthaccess.store import Store` | `from earthaccess.store import Store` (unchanged) |

### Recommended Approach

Use top-level imports whenever possible:

```python
# ✅ Recommended
from earthaccess import (
    GranuleQuery,
    CollectionQuery,
    BoundingBox,
    DataGranule,
    SearchResults,
)

# ⚠️ Still works, but not recommended
from earthaccess.search import GranuleQuery
from earthaccess.search.query import BoundingBox
```

---

## API Changes

### New Methods on SearchResults

| Method | Description |
|--------|-------------|
| `total()` | Get total CMR hits (different from `len()`) |
| `all()` | Fetch and return all results as a list |
| `filter()` | Filter results by criteria |
| `items()` | Iterate through results (pystac-client compatible) |
| `pages(page_size)` | Iterate by pages with configurable size |
| `summary()` | Get aggregated statistics |

### New Methods on DataGranule/DataCollection

| Method | Description |
|--------|-------------|
| `to_stac()` | Convert to STAC format |
| `to_dict()` | Convert to dictionary |
| `show_map()` | Display interactive map (requires `[widgets]`) |

### New Classes

| Class | Module | Description |
|-------|--------|-------------|
| `GranuleQuery` | `earthaccess.search` | Query builder for granule searches |
| `CollectionQuery` | `earthaccess.search` | Query builder for collection searches |
| `SearchResults` | `earthaccess.search` | Lazy pagination wrapper |
| `Asset` | `earthaccess.store` | Immutable granule file representation |
| `AssetFilter` | `earthaccess.store` | Filter for selecting assets |
| `TargetLocation` | `earthaccess.store` | Cloud storage download target |

---

## Behavioral Changes

### 1. SearchResults Return Type

**Before (v0.x):**
```python
results = earthaccess.search_data(short_name="ATL06")
type(results)  # <class 'list'>
```

**After (v1.0.0):**
```python
results = earthaccess.search_data(short_name="ATL06")
type(results)  # <class 'earthaccess.search.results.GranuleResults'>

# Still acts like a list
results[0]        # Works
len(results)      # Works (returns loaded count)
for g in results: # Works
```

**Why:** Enables lazy pagination and rich display features.

**Migration:** Code treating results as a list should continue to work. If you need an actual list: `results.all()`

---

### 2. Prefetching Behavior

**New in v1.0.0:** SearchResults automatically prefetches the first 20 results for immediate access.

```python
results = earthaccess.search_data(short_name="ATL06", count=1000)

# First 20 are already loaded
results[0]   # Instant access
results[19]  # Instant access
results[20]  # Triggers next page fetch
```

**Why:** Better user experience - immediate access to first results while maintaining lazy loading for large datasets.

---

### 3. User Agent String

**New in v1.0.0:** All HTTP requests include `User-Agent: earthaccess v1.0.0`

**Why:** Helps NASA track earthaccess usage for funding and support.

---

## Deprecations

### SessionWithHeaderRedirection

**Status:** Deprecated in v1.0.0, will be removed in v2.0.0

**Replacement:**
```python
# Old (deprecated)
from earthaccess.auth import SessionWithHeaderRedirection
session = SessionWithHeaderRedirection(edl_hostname, auth)

# New
import earthaccess
auth = earthaccess.login()
session = auth.get_session()
```

**Deprecation Warning:** You'll see a warning when using `SessionWithHeaderRedirection`:
```
DeprecationWarning: SessionWithHeaderRedirection is deprecated.
Use auth.get_session() to get an authenticated session,
or _create_earthdata_session() for low-level session creation.
```

---

## Step-by-Step Migration

### Step 1: Update Installation

```bash
# Upgrade to v1.0.0
pip install --upgrade earthaccess

# Or with extras
pip install --upgrade earthaccess[widgets,virtualizarr]
```

### Step 2: Add Explicit Login

Add `earthaccess.login()` to your code:

```python
import earthaccess

# ✅ Add this at the start
earthaccess.login()

# Rest of your code...
```

### Step 3: Update Imports (If Needed)

If you use internal imports, update them:

```python
# Before
from earthaccess.query import GranuleQuery, BoundingBox
from earthaccess.daac import DAACS

# After - Option 1: Top-level (recommended)
from earthaccess import GranuleQuery, BoundingBox
from earthaccess.store.daac import DAACS

# After - Option 2: Module imports
from earthaccess.search import GranuleQuery, BoundingBox
from earthaccess.store.daac import DAACS
```

### Step 4: Update len() Usage (If Needed)

If you rely on `len(results)` to get total CMR hits:

```python
# Before
results = earthaccess.search_data(short_name="ATL06", count=10)
total = len(results)  # Was total CMR hits

# After
results = earthaccess.search_data(short_name="ATL06", count=10)
total = results.total()  # Total CMR hits
loaded = len(results)    # Loaded count (10)
```

### Step 5: Test Your Code

Run your existing tests/scripts and verify they work correctly.

### Step 6: Adopt New Features (Optional)

Take advantage of new features:

```python
# Use query builders
from earthaccess import GranuleQuery
query = GranuleQuery().short_name("ATL06").temporal("2023-01", "2023-12")
granules = earthaccess.search_data(query=query)

# Use STAC conversion
stac_item = granules[0].to_stac()

# Use interactive maps (if widgets installed)
granules.show_map()

# Use filtering
large_granules = granules.filter(min_size=1000)
```

---

## Testing Your Migration

### 1. Run Existing Tests

```bash
pytest tests/
```

### 2. Test Common Operations

```python
import earthaccess

# Login
earthaccess.login()

# Search
granules = earthaccess.search_data(short_name="ATL06", count=10)
assert len(granules) == 10
assert granules.total() > 10

# Open
files = earthaccess.open(granules)
assert len(files) == len(granules)

# Download (if not in cloud)
paths = earthaccess.download(granules[:2], ".")
assert len(paths) == 2
```

### 3. Check Deprecation Warnings

```python
import warnings
warnings.filterwarnings("error", category=DeprecationWarning)

# Your code here - will raise errors on deprecated usage
```

---

## Rollback Plan

If you encounter issues, you can rollback to the last stable v0.x release:

```bash
# Uninstall v1.0.0
pip uninstall earthaccess

# Reinstall last stable v0.x version
pip install earthaccess==0.15.1
```

Or pin your dependencies:

```toml
# pyproject.toml
dependencies = [
    "earthaccess>=0.15,<1.0"
]
```

```txt
# requirements.txt
earthaccess>=0.15,<1.0
```

---

## Getting Help

### Report Issues

Found a bug or unexpected behavior?

1. Check [existing issues](https://github.com/nsidc/earthaccess/issues)
2. [Open a new issue](https://github.com/nsidc/earthaccess/issues/new) with:
   - Code snippet showing the problem
   - Expected vs actual behavior
   - earthaccess version: `earthaccess.__version__`

### Ask Questions

- [GitHub Discussions](https://github.com/nsidc/earthaccess/discussions)
- Tag migration questions with `[migration]`

### Contribute

Help improve this guide:

1. Fork the repository
2. Edit `docs/migration-guide.md`
3. Submit a pull request

---

## Summary

**Most users only need to:**

1. Upgrade: `pip install --upgrade earthaccess`
2. Add `earthaccess.login()` at the start
3. Replace `len(results)` with `results.total()` if needed

**Everything else is backward compatible or opt-in.**

---

## Version Comparison

| Feature | v0.x | v1.0.0 |
|---------|------|--------|
| Auto-login | ✅ Yes | ❌ No (explicit required) |
| Query builders | ❌ No | ✅ Yes |
| Rich HTML display | ❌ No | ✅ Yes |
| Interactive maps | ❌ No | ✅ Yes (with [widgets]) |
| STAC conversion | ❌ No | ✅ Yes |
| Lazy pagination | ❌ No | ✅ Yes |
| Cloud downloads | ❌ No | ✅ Yes |
| Virtual datasets | ✅ Basic | ✅ Enhanced (multi-parser) |
| `len(results)` | Total CMR hits | Loaded count |
| SearchResults type | `list` | `GranuleResults`/`CollectionResults` |

---

## Next Steps

1. **Read the release notes:** [releases/1.0.0a.md](releases/1.0.0a.md)
2. **Review the CHANGELOG:** [CHANGELOG.md](../CHANGELOG.md)
3. **Check the API docs:** [User Reference](user-reference/api/api.md)
4. **Try the tutorials:** [Tutorials](tutorials/)

---

**Last updated:** 2026-01-06
**Version:** 1.0.0a2
