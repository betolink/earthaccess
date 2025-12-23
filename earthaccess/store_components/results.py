"""Results classes for earthaccess with lazy pagination and streaming.

Provides base classes for different result types with common
interfaces and lazy evaluation capabilities.
"""

import logging
from abc import abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    TypeVar,
)

if TYPE_CHECKING:
    from ..store_components.asset import AssetFilter

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class ResultsBase:
    """Base class for all result types with common functionality.

    Provides lazy evaluation capabilities and method chaining
    for search results across different backends.
    """

    def __init__(self, items: Optional[List[T]] = None, **kwargs: Any) -> None:
        """Initialize results.

        Args:
            items: Optional initial list of items
            **kwargs: Additional parameters for subclasses
        """
        self._items = items or []
        self._lazy_params: Dict[str, Any] = kwargs
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def pages(self, page_size: int = 100):
        """Iterator over pages of results.

        Args:
            page_size: Number of items per page

        Yields:
            Lists of items for each page
        """
        for i in range(0, len(self._items), page_size):
            yield list(self._items[i : i + page_size])

    def __iter__(self):
        """Iterate over all items."""
        return iter(self._items)

    def __len__(self) -> int:
        """Return number of items."""
        return len(self._items)

    def __repr__(self) -> str:
        """String representation."""
        return f"<{self.__class__.__name__} with {len(self._items)} items>"

    def __getitem__(self, index):
        """Get item by index or slice."""
        return self._items[index]

    def get_all(self):
        """Get all items as list (eager evaluation).

        Returns:
            List of all items
        """
        return list(self._items)

    def matched(self) -> int:
        """Return number of matched items."""
        return len(self._items)

    def count(self) -> int:
        """Alias for matched() for compatibility."""
        return self.matched()

    def first(self):
        """Get first item or None if empty."""
        return self._items[0] if len(self._items) > 0 else None

    def last(self):
        """Get last item or None if empty."""
        return self._items[-1] if len(self._items) > 0 else None

    def preview(self, limit: int = 10):
        """Get preview of first N items."""
        return self._items[:limit]

    def filter(self, predicate):
        """Filter items by predicate.

        Args:
            predicate: Function that returns bool for each item

        Returns:
            New ResultsBase with filtered items
        """
        filtered_items = [item for item in self._items if predicate(item)]
        return ConcreteResultsBase(filtered_items)

    def map(self, func):
        """Apply function to each item.

        Args:
            func: Function to apply to each item

        Returns:
            New ResultsBase with mapped items
        """
        mapped_items = [func(item) for item in self._items]
        return ConcreteResultsBase(mapped_items)

    def open(
        self,
        asset_filter: Optional["AssetFilter"] = None,
        **kwargs: Any,
    ):
        """Open data files from results with optional asset filtering.

        This is a placeholder for subclasses to implement. Subclasses should
        override this method to provide actual file opening functionality
        based on their specific data types.

        Args:
            asset_filter: Optional AssetFilter to select specific assets
            **kwargs: Additional parameters passed to the file opening implementation

        Returns:
            Depends on subclass implementation (e.g., xarray datasets, file handles)

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.open() must be implemented by subclass"
        )

    def download(
        self,
        asset_filter: Optional["AssetFilter"] = None,
        local_path: Optional[str] = None,
        threads: int = 8,
        **kwargs: Any,
    ):
        """Download data files from results with optional asset filtering.

        This is a placeholder for subclasses to implement. Subclasses should
        override this method to provide actual download functionality based on
        their specific data types.

        Args:
            asset_filter: Optional AssetFilter to select specific assets
            local_path: Optional local directory path for downloads
            threads: Number of concurrent download threads
            **kwargs: Additional parameters passed to the download implementation

        Returns:
            Depends on subclass implementation (e.g., list of downloaded paths)

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.download() must be implemented by subclass"
        )

    def process(
        self,
        func: Callable,
        asset_filter: Optional["AssetFilter"] = None,
        **kwargs: Any,
    ):
        """Process items with optional asset filtering.

        This is a placeholder for subclasses to implement. Subclasses should
        override this method to provide custom processing based on their
        specific data types and asset structures.

        Args:
            func: Processing function to apply to each item/asset
            asset_filter: Optional AssetFilter to select specific assets
            **kwargs: Additional parameters for processing

        Returns:
            Depends on subclass implementation

        Raises:
            NotImplementedError: If not implemented by subclass
        """
        raise NotImplementedError(
            f"{self.__class__.__name__}.process() must be implemented by subclass"
        )


class ConcreteResultsBase(ResultsBase):
    """Concrete implementation of ResultsBase for testing and simple use."""

    def pages(self, page_size: int = 100):
        """Iterator over pages of results.

        Args:
            page_size: Number of items per page

        Yields:
            Lists of items for each page
        """
        for i in range(0, len(self._items), page_size):
            yield list(self._items[i : i + page_size])


class LazyResultsBase(ResultsBase):
    """Base class for lazy-evaluated results.

    Inherits from ResultsBase but adds lazy evaluation
    capabilities for large result sets.
    """

    def __init__(self, items: Optional[List[T]] = None, **kwargs: Any) -> None:
        """Initialize lazy results."""
        super().__init__(items, **kwargs)
        self._fetched_all: bool = False

    def get_all(self):
        """Get all items, fetching if needed.

        Override to implement lazy fetching logic.
        """
        if not self._fetched_all:
            self._logger.debug(f"Fetching all items for {self.__class__.__name__}")
            self._items = self._fetch_all()
            self._fetched_all = True

        return super().get_all()

    def _fetch_all(self):
        """Fetch all items from backend.

        Must be implemented by subclasses for specific
        data fetching logic.
        """
        raise NotImplementedError("Subclasses must implement _fetch_all()")

    def pages(self, page_size: int = 100):
        """Iterator over pages with lazy evaluation."""
        if not self._fetched_all:
            # Don't have total count yet, need to fetch progressively
            return self._lazy_pages(page_size)
        else:
            # Have all items, can paginate directly
            return super().pages(page_size)

    @abstractmethod
    def _lazy_pages(self, page_size: int):
        """Lazy page fetching logic.

        Must be implemented by subclasses.
        """
        pass


class StreamingExecutor:
    """Executor for streaming operations with producer-consumer pattern.

    Handles concurrent processing of lazy result streams
    with configurable parallelism and backpressure handling.
    """

    def __init__(
        self,
        max_workers: int = 8,
        prefetch_pages: int = 2,
        show_progress: bool = True,
    ) -> None:
        """Initialize streaming executor.

        Args:
            max_workers: Maximum number of concurrent workers
            prefetch_pages: Number of pages to buffer ahead
            show_progress: Whether to show progress
        """
        self.max_workers = max_workers
        self.prefetch_pages = prefetch_pages
        self.show_progress = show_progress
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def map(
        self,
        func: Callable,
        results,
    ):
        """Apply function to items from results with streaming.

        Args:
            func: Function that takes an item and returns a result
            results: LazyResultsBase to process

        Yields:
            Results from func applied to each item
        """
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from queue import Queue

        def process_page(page_items: List[T]) -> List[R]:
            """Process a single page of items."""
            return [func(item) for item in page_items]

        # Thread-safe queues for producer-consumer pattern
        page_queue = Queue(maxsize=self.prefetch_pages)
        result_queue = Queue()

        def producer():
            """Producer thread fetches pages."""
            try:
                for page in results.pages():
                    page_queue.put(page)
                page_queue.put(None)  # Signal end
            except Exception as e:
                self._logger.error(f"Producer error: {e}")
                page_queue.put(None)

        def consumer():
            """Consumer thread processes pages."""
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []

                while True:
                    page = page_queue.get()
                    if page is None:
                        break

                    future = executor.submit(process_page, page)
                    futures.append(future)

                # Collect results from completed futures
                for future in as_completed(futures):
                    for result in future.result():
                        result_queue.put(result)

                result_queue.put(None)  # Signal end

        # Start producer and consumer threads
        producer_thread = threading.Thread(target=producer, daemon=True)
        consumer_thread = threading.Thread(target=consumer, daemon=True)

        producer_thread.start()
        consumer_thread.start()

        try:
            # Yield results as they become available
            while True:
                result = result_queue.get()
                if result is None:
                    break
                yield result
        finally:
            producer_thread.join(timeout=1)
            consumer_thread.join(timeout=1)

    def process_and_collect(
        self,
        func: Callable,
        results,
    ):
        """Process all items and collect results (eager version).

        Args:
            func: Function to apply to each item
            results: ResultsBase to process

        Returns:
            List of all results
        """
        return list(self.map(func, results))
