"""Type stubs for pystac-client.

This is a minimal stub file to provide type checking support
for the optional pystac-client dependency.
"""

from typing import Any, Dict, Iterator, Optional

class Client:
    """STAC API client."""

    @staticmethod
    def open(url: str, **kwargs: Any) -> "Client":
        """Open a STAC catalog by URL."""
        ...

    def search(self, **kwargs: Any) -> "ItemSearch":
        """Search catalog."""
        ...

class ItemSearch:
    """STAC item search results."""

    def matched(self) -> int:
        """Get total matched items."""
        ...

    def items(self) -> Iterator["Item"]:
        """Iterate over search results."""
        ...

    def __getitem__(self, index: int) -> "Item":
        """Get item by index."""
        ...

class Item:
    """STAC item."""

    id: str
    geometry: Dict[str, Any]
    properties: Dict[str, Any]
    assets: Dict[str, Dict[str, Any]]
