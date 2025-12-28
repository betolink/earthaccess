"""Virtual dataset utilities for cloud-native access.

This package provides tools for working with virtual datasets using
VirtualiZarr parsers (DMR++, HDF5, NetCDF3) and Kerchunk references.
"""

from earthaccess.virtual.dmrpp import (
    SUPPORTED_PARSERS,
    get_granule_credentials_endpoint_and_region,
    open_virtual_dataset,
    open_virtual_mfdataset,
)
from earthaccess.virtual.kerchunk import consolidate_metadata

__all__ = [
    "open_virtual_dataset",
    "open_virtual_mfdataset",
    "get_granule_credentials_endpoint_and_region",
    "consolidate_metadata",
    "SUPPORTED_PARSERS",
]
