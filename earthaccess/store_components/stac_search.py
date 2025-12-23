"""External STAC catalog search functionality.

Provides search_stac() and STACItemResults for querying
external STAC catalogs via pystac-client.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def search_stac(
    url: str,
    collections: Optional[List[str]] = None,
    bbox: Optional[List[float]] = None,
    datetime: Optional[str] = None,
    intersects: Optional[Dict[str, Any]] = None,
    limit: int = 100,
    **kwargs: Any,
):
    """Search external STAC catalogs.

    Args:
        url: STAC API endpoint URL
        collections: Optional list of collection IDs to filter
        bbox: Optional bounding box [west, south, east, north]
        datetime: Optional temporal filter (ISO 8601 format)
        intersects: Optional GeoJSON geometry for spatial filter
        limit: Maximum number of items to return (default: 100)
        **kwargs: Additional search parameters passed to STAC API

    Returns:
        STACItemResults object with search results

    Raises:
        ImportError: If pystac-client is not installed
        ValueError: If URL is invalid

    Examples:
        Search by bounding box:
        >>> results = search_stac(
        ...     "https://earth-search.aws.element84.com/v1",
        ...     collections=["sentinel-2-l2a"],
        ...     bbox=[-122.5, 37.7, -122.3, 37.9],
        ...     limit=10
        ... )

        Search with temporal filter:
        >>> results = search_stac(
        ...     "https://earth-search.aws.element84.com/v1",
        ...     datetime="2023-01-01/2023-12-31"
        ... )

        Search with geometry:
        >>> geojson = {
        ...     "type": "Polygon",
        ...     "coordinates": [[
        ...         [-122.5, 37.7], [-122.3, 37.7],
        ...         [-122.3, 37.9], [-122.5, 37.9],
        ...         [-122.5, 37.7]
        ...     ]]
        ... }
        >>> results = search_stac(
        ...     "https://earth-search.aws.element84.com/v1",
        ...     intersects=geojson
        ... )
    """
    try:
        import pystac_client
    except ImportError:
        raise ImportError(
            "pystac-client is required for STAC catalog searches. "
            "Install it with: pip install pystac-client"
        )

    # Validate URL
    if not url or not isinstance(url, str):
        raise ValueError(f"Invalid STAC URL: {url}")

    try:
        # Open STAC catalog
        client = pystac_client.Client.open(url)

        # Build search parameters
        search_params = {}
        if collections:
            search_params["collections"] = collections
        if bbox:
            search_params["bbox"] = bbox
        if datetime:
            search_params["datetime"] = datetime
        if intersects:
            search_params["intersects"] = intersects
        search_params["limit"] = limit

        # Add any additional parameters
        search_params.update(kwargs)

        # Execute search
        logger.debug(f"Searching STAC catalog: {url}")
        logger.debug(f"Search parameters: {search_params}")

        search = client.search(**search_params)

        return STACItemResults(
            search=search,
            url=url,
            search_params=search_params,
        )

    except Exception as e:
        logger.error(f"Error searching STAC catalog: {e}")
        raise


class STACItemResults:
    """Results from external STAC catalog search.

    Provides lazy iteration over STAC items with CMR-like
    interface for compatibility.
    """

    def __init__(self, search: Any, url: str, search_params: Dict[str, Any]):
        """Initialize STAC results.

        Args:
            search: pystac_client search object
            url: STAC API endpoint URL
            search_params: Original search parameters
        """
        self._search = search
        self._url = url
        self._search_params = search_params
        self._items_cache: Optional[List[Any]] = None
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def __len__(self) -> int:
        """Return number of matched items."""
        return self._search.matched()

    def __repr__(self) -> str:
        """String representation."""
        return f"<STACItemResults from {self._url} with {len(self)} items>"

    def __iter__(self):
        """Iterate over all items."""
        return iter(self.items())

    def __getitem__(self, index):
        """Get item by index."""
        items = self.items()
        return items[index]

    def matched(self) -> int:
        """Return number of matched items."""
        return self._search.matched()

    def count(self) -> int:
        """Alias for matched()."""
        return self.matched()

    def items(self, limit: Optional[int] = None) -> List[Any]:
        """Get all items as list.

        Args:
            limit: Optional limit on number of items returned

        Returns:
            List of pystac Item objects
        """
        if self._items_cache is None:
            self._items_cache = list(self._search.items())

        if limit is not None:
            return self._items_cache[:limit]
        return self._items_cache

    def get_all(self):
        """Get all items as list (alias for items())."""
        return self.items()

    def pages(self, page_size: int = 100):
        """Iterate over pages of results.

        Args:
            page_size: Number of items per page

        Yields:
            Lists of items for each page
        """
        all_items = self.items()

        for i in range(0, len(all_items), page_size):
            yield all_items[i : i + page_size]

    def first(self):
        """Get first item or None if empty."""
        items = self.items()
        return items[0] if len(items) > 0 else None

    def last(self):
        """Get last item or None if empty."""
        items = self.items()
        return items[-1] if len(items) > 0 else None

    def preview(self, limit: int = 10):
        """Get preview of first N items."""
        return self.items(limit=limit)

    def to_dict(self) -> Dict[str, Any]:
        """Convert results to dictionary.

        Returns:
            Dictionary with metadata and items
        """
        return {
            "url": self._url,
            "search_params": self._search_params,
            "matched": self.matched(),
            "items": [self._item_to_dict(item) for item in self.items()],
        }

    def _item_to_dict(self, item: Any) -> Dict[str, Any]:
        """Convert pystac Item to dictionary.

        Args:
            item: pystac Item object

        Returns:
            Dictionary representation
        """
        return {
            "id": item.id,
            "geometry": item.geometry,
            "bbox": item.bbox,
            "properties": item.properties,
            "assets": {
                key: {
                    "href": asset.href,
                    "type": asset.media_type,
                    "roles": asset.roles,
                }
                for key, asset in item.assets.items()
            },
            "collection": item.collection_id,
            "datetime": item.datetime.isoformat() if item.datetime else None,
        }
