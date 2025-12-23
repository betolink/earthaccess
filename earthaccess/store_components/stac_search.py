"""External STAC catalog search functionality.

Provides search_stac() and STACItemResults for querying
external STAC catalogs via pystac-client.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Generator, Iterator, List, Optional

if TYPE_CHECKING:
    try:
        from pystac_client import Client as PystacClient, ItemSearch as PystacItemSearch
    except ImportError:
        PystacClient: Any  # type: ignore
        PystacItemSearch: Any  # type: ignore
else:
    PystacClient = Any  # type: ignore
    PystacItemSearch = Any  # type: ignore

logger = logging.getLogger(__name__)


def search_stac(
    url: str,
    collections: Optional[List[str]] = None,
    bbox: Optional[List[float]] = None,
    datetime: Optional[str] = None,
    intersects: Optional[Dict[str, Any]] = None,
    limit: int = 100,
    **kwargs: Any,
) -> "STACItemResults":
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
    if PystacClient is None:
        raise ImportError(
            "pystac-client is required for STAC catalog searches. "
            "Install it with: pip install pystac-client"
        )

    import pystac_client

    # Validate URL
    if not url or not isinstance(url, str):
        raise ValueError(f"Invalid STAC URL: {url}")

    try:
        # Open STAC catalog
        client = pystac_client.Client.open(url)

        # Build search parameters
        search_params: Dict[str, Any] = {}
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
        logger.exception(f"Error searching STAC catalog: {e}")
        raise


class STACItemResults:
    """Wrapper for STAC catalog search results.

    Provides CMR-like interface for external STAC catalog results.
    """

    def __init__(
        self,
        search: "PystacItemSearch",
        url: str,
        search_params: Dict[str, Any],
    ) -> None:
        """Initialize STACItemResults.

        Args:
            search: pystac_client ItemSearch object
            url: STAC catalog URL
            search_params: Search parameters used
        """
        self._search = search
        self.url = url
        self.search_params = search_params

        if hasattr(self._search, "items"):
            self._items = list(self._search.items())
        else:
            self._items = []

    def __len__(self) -> int:
        """Return number of items."""
        if hasattr(self._search, "matched"):
            return self._search.matched() or 0
        return 0

    def __repr__(self) -> str:
        """String representation."""
        return f"<STACItemResults from {self.url} with {len(self)} items>"

    def matched(self) -> int:
        """Get total matched items."""
        if hasattr(self._search, "matched"):
            return self._search.matched()
        return 0

    def count(self) -> int:
        """Get count of items (alias for len)."""
        return len(self)

    def __iter__(self) -> Iterator[Any]:
        """Iterate over items."""
        return iter(self._items)

    def __getitem__(self, index: int) -> Any:
        """Get item by index."""
        return self._items[index]

    def get_all(self) -> List[Any]:
        """Get all items as list."""
        return self._items

    def pages(self, page_size: int = 100) -> Generator[List[Any], None, None]:
        """Iterate over pages of results.

        Args:
            page_size: Number of items per page

        Yields:
            Lists of items for each page
        """
        all_items = self.get_all()
        for i in range(0, len(all_items), page_size):
            yield all_items[i : i + page_size]

    def first(self) -> Any:
        """Get first item."""
        items = self.get_all()
        return items[0] if items else None

    def last(self) -> Any:
        """Get last item."""
        items = self.get_all()
        return items[-1] if items else None

    def preview(self, limit: int = 3) -> List[Any]:
        """Preview first n items."""
        return self.get_all()[:limit]

    def items(self, limit: Optional[int] = None) -> List[Any]:
        """Get items with optional limit.

        Args:
            limit: Maximum number of items to return

        Returns:
            List of items
        """
        items = self.get_all()
        return items[:limit] if limit else items

    def to_dict(self) -> Dict[str, Any]:
        """Convert results to dictionary format."""
        return {
            "url": self.url,
            "search_params": self.search_params,
            "count": len(self),
            "items": self.get_all(),
        }
