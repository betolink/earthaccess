# Advanced Search tutorials

## Learning goals

Beyond the basic `search_data()` / `search_datasets()` keyword calls, the CMR
search API supports spatial, temporal, and metadata filters that let you
narrow millions of granules down to exactly what you need.

In this section you will learn to:

- Combine filters: `bounding_box`, `temporal`, `cloud_cover`, `day_night_flag`,
  `orbit_number`, `version`, `point`, `polygon`, `line`.
- Search with class-based queries (`earthaccess.search.GranuleQuery` /
  `CollectionQuery`) for programmatic, reusable search objects.
- Handle **restricted** (access-controlled) datasets with an authenticated
  session.

## Prerequisites

- An authenticated session for restricted data: `earthaccess.login()`.
- The [search API reference](../../api/index.md) for the full parameter list.

## Tutorials

- [Search for data using filters](../../user/howto/search-granules.md) — search
  granules within a dataset using spatial and temporal filters.
- [Accessing datasets under an Access Control List (ACL)](../../user/tutorials/restricted-datasets.ipynb) —
  anonymous vs authenticated queries, and how restricted granules behave.
- [earthaccess and NASA EDL](../../user/howto/edl.ipynb) — authenticated HTTP /
  S3 sessions for searches and access.

## Try it next

- [Search services](../../user/howto/search-services.md) — discover subsetting /
  transformation services on a dataset.
- [How search works](../../user/explanation/search.md) — collections vs
  granules, and the search parameters shared between them.
