"""STAC conversion utilities for earthaccess.

This module provides functions to convert between CMR UMM format and STAC format,
enabling interoperability with STAC-based tools and catalogs.

Converters:
    - umm_granule_to_stac_item: Convert CMR UMM granule to STAC Item
    - umm_collection_to_stac_collection: Convert CMR UMM collection to STAC Collection
    - stac_item_to_data_granule: Convert STAC Item to DataGranule
    - stac_collection_to_data_collection: Convert STAC Collection to DataCollection
"""

from .converters import (
    stac_collection_to_data_collection,
    stac_item_to_data_granule,
    umm_collection_to_stac_collection,
    umm_granule_to_stac_item,
)

__all__ = [
    "umm_granule_to_stac_item",
    "umm_collection_to_stac_collection",
    "stac_item_to_data_granule",
    "stac_collection_to_data_collection",
]
