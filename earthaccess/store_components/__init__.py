"""Store package for earthaccess data access operations."""

from .asset import (
    Asset,
    AssetFilter,
    filter_assets,
    get_assets_by_band,
    get_assets_by_size_range,
    get_browse_assets,
    get_data_assets,
    get_thumbnail_assets,
)
from .cloud_transfer import CloudTransfer
from .credentials import AuthContext, CredentialManager, infer_provider_from_url
from .filesystems import FileSystemFactory
from .geometry import CMR_MAX_POLYGON_POINTS, load_geometry
from .query import CollectionQuery, GranuleQuery
from .results import (
    ConcreteResultsBase,
    LazyResultsBase,
    ResultsBase,
    StreamingExecutor,
)

__all__ = [
    "Asset",
    "AssetFilter",
    "filter_assets",
    "get_data_assets",
    "get_thumbnail_assets",
    "get_browse_assets",
    "get_assets_by_band",
    "get_assets_by_size_range",
    "CloudTransfer",
    "CredentialManager",
    "AuthContext",
    "infer_provider_from_url",
    "FileSystemFactory",
    "GranuleQuery",
    "CollectionQuery",
    "ResultsBase",
    "LazyResultsBase",
    "ConcreteResultsBase",
    "StreamingExecutor",
    "load_geometry",
    "CMR_MAX_POLYGON_POINTS",
]
