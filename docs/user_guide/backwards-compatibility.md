# Our commitments to backwards compatibility

We care deeply about minimizing negative impacts of changes to earthaccess, but we also care deeply about making earthaccess the most valuable it can be to our users. These are sometimes in conflict, and this documentation helps us make decisions that balance these needs in a way that's best for our users.

## Our versioning scheme

We use [Semantic Versioning (SemVer)](https://semver.org/) to tell users what to expect when upgrading. We recommend following the link to learn more, but here are the important ways this affects you:

    1. There are 3 version parts: `MAJOR.MINOR.PATCH` (e.g. `1.2.3` is major version `1`, minor release `2`, patch release `3`).
    2. When the major version changes, anything can break! Always visit this documentation when doing a major upgrade.
    3. When the minor version changes, new features should be available, but nothing should break. Visit the documentation to learn about new features. If you discover any breaking changes, please [open a bug report](https://github.com/nsidc/earthaccess/issues/new)!
    4. When the patch version changes, only bugfixes should be included. Visit the CHANGELOG to learn more about the fixes.

## Our commitments

### Versioning

We will follow SemVer. All version changes will consider [this](https://example.com/our-public-api) to be the public [API](user-reference/api/api) documentation for the purposes of deciding whether a change is breaking or non-breaking.

### Release Communication

1. We will announce releases on the following channel: [Github Releases](https://github.com/nsidc/earthaccess/releases)
2. Release announcements will include a prominent notification of breaking changes, including migration instructions.

### CHANGELOG

1. We will update the CHANGELOG for every release following [Common Changelog](https://common-changelog.org/) style.
2. The CHANGELOG will include prominent notification of breaking changes, including a link to migration guide.

### Fixing Backwards Incompatible Changes

1. We will plan to fix any backwards incompatible changes in non-major releases _or_ re-release with a major version bump.

2. We cannot guarantee a timeline under which maintainers will be able to complete this work alongside their other priorities.

3. Our maintenance team will always welcome outside contributions towards this goal.

4. Please use a [GitHub Issue](https://github.com/nsidc/earthaccess/issues) to communicate about this work.

## Our Python and dependency support policy

Our project follows the [SPEC0](https://scientific-python.org/specs/spec-0000/) dependency version deprecation policy, which outlines the guidelines for upgrading dependencies of _earthaccess_.

## Migration guides

### 1.0.0a2

* `SessionWithHeaderRedirection` is now deprecated. Use `auth.get_session()` to get an
  authenticated session, or `_create_earthdata_session()` for low-level session creation:

  ```python
  import earthaccess

  # Recommended approach
  auth = earthaccess.login()
  session = auth.get_session()

  # Or for low-level session creation
  from earthaccess.auth import _create_earthdata_session
  session = _create_earthdata_session("urs.earthdata.nasa.gov", ("user", "pass"))
  ```

* See the comprehensive [Migration Guide](../migration-guide.md) for upgrading from
  v0.x to v1.0.0.

### 1.0.0a1

* `earthaccess.login()` must now be called explicitly before accessing data.
  Automatic login behavior has been removed.

* `len(results)` on `SearchResults` now returns the count of loaded/cached results,
  not the total CMR hits. Use `results.total()` to get total matches in CMR.

* Package structure has been reorganized. If you use internal imports, update them:
  - `from earthaccess.query import GranuleQuery` → `from earthaccess.search import GranuleQuery`
  - `from earthaccess.daac import DAACS` → `from earthaccess.store.daac import DAACS`
  - Or use top-level imports: `from earthaccess import GranuleQuery`

### 0.14.0

* `earthaccess.login()` will now raise an exception if Earthdata Login rejects credentials.
  If you want to ignore errors, which we do not recommend, use a `try` block:

  ```python
  try:
      earthaccess.login()
  except Exception:
      pass
  ```
