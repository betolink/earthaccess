"""Distributed execution context for granule operations.

This module provides serializable context objects for worker processes in
distributed execution environments (Dask, Ray, multiprocessing). These classes
ensure that all necessary authentication and credential information is properly
serialized and reconstructed in worker processes.

Key components:
- DistributedWorkerContext: Pickleable context for distributed workers
- DistributedStreamingIterator: Iterator that yields chunks with worker context
- process_granule_in_worker: Helper for processing granules in workers
"""

from __future__ import annotations

from dataclasses import dataclass
from pickle import dumps, loads
from typing import Any, List, Optional

from earthaccess.auth.credentials import AuthContext


@dataclass(frozen=True)
class DistributedWorkerContext:
    """Serializable context for worker process execution.

    This class bundles all credential and configuration information needed
    by a worker process to access cloud-native data. It's designed to be
    pickleable for use with multiprocessing, Dask, Ray, and other
    distributed execution frameworks.

    Attributes:
        auth_context: AuthContext with all available credentials (S3, HTTPS, provider)
        config: Optional dictionary for additional worker configuration
    """

    auth_context: AuthContext
    config: Optional[dict] = None

    def __post_init__(self) -> None:
        """Validate that auth_context is properly initialized."""
        if not isinstance(self.auth_context, AuthContext):
            raise TypeError(
                f"auth_context must be AuthContext instance, got {type(self.auth_context)}"
            )

    def to_bytes(self) -> bytes:
        """Serialize the worker context to bytes for transmission.

        Returns:
            Pickled bytes representation of the context

        Example:
            >>> context = DistributedWorkerContext(auth_context=auth_ctx)
            >>> serialized = context.to_bytes()
            >>> # Send to worker and reconstruct
            >>> context2 = DistributedWorkerContext.from_bytes(serialized)
        """
        return dumps(self)

    @classmethod
    def from_bytes(cls, data: bytes) -> DistributedWorkerContext:
        """Deserialize a worker context from bytes.

        Parameters:
            data: Pickled bytes representation

        Returns:
            Reconstructed DistributedWorkerContext instance

        Raises:
            ValueError: If bytes cannot be unpickled or don't contain valid context
        """
        try:
            context = loads(data)
            if not isinstance(context, cls):
                raise ValueError(
                    f"Deserialized object is not DistributedWorkerContext, got {type(context)}"
                )
            return context
        except Exception as e:
            raise ValueError(
                f"Failed to deserialize DistributedWorkerContext: {e}"
            ) from e

    def reconstruct_auth(self) -> Any:
        """Reconstruct Auth object in worker process.

        This is called in worker processes to restore the Auth object
        from the serialized AuthContext.

        Returns:
            Reconstructed Auth instance

        Raises:
            ValueError: If AuthContext is invalid or credentials are expired
        """
        if not self.auth_context.is_valid():
            raise ValueError(
                "AuthContext contains expired credentials. "
                "Please refresh credentials in the parent process."
            )
        return self.auth_context.to_auth()


class DistributedStreamingIterator:
    """Iterator for parallel streaming operations on multiple granules.

    This class enables chunking granules for parallel processing while
    maintaining credential context for each worker. It yields tuples of
    (granule, worker_context) that can be processed in parallel.

    This iterator is NOT itself serializable - it's used in the parent
    process to distribute work. The individual items it yields ARE
    serializable.
    """

    def __init__(
        self,
        granules: List[Any],
        auth_context: AuthContext,
        chunk_size: int = 1,
        config: Optional[dict] = None,
    ) -> None:
        """Initialize the streaming iterator.

        Parameters:
            granules: List of DataGranule objects to stream
            auth_context: AuthContext with credentials for all workers
            chunk_size: Number of granules per chunk (default: 1)
            config: Optional configuration dictionary for workers

        Raises:
            ValueError: If granules is empty or chunk_size < 1
        """
        if not granules:
            raise ValueError("granules list cannot be empty")
        if chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")

        self.granules = granules
        self.auth_context = auth_context
        self.chunk_size = chunk_size
        self.config = config
        self._index = 0

    def __iter__(self) -> DistributedStreamingIterator:
        """Return iterator object (self)."""
        self._index = 0
        return self

    def __next__(self) -> tuple[list[Any], DistributedWorkerContext]:
        """Get next chunk of granules with worker context.

        Returns:
            Tuple of (granule_chunk, worker_context) ready for parallel execution

        Raises:
            StopIteration: When all granules have been yielded
        """
        if self._index >= len(self.granules):
            raise StopIteration

        # Get the next chunk
        chunk_end = min(self._index + self.chunk_size, len(self.granules))
        granule_chunk = self.granules[self._index : chunk_end]
        self._index = chunk_end

        # Create worker context with current credentials and config
        worker_context = DistributedWorkerContext(
            auth_context=self.auth_context,
            config=self.config,
        )

        return granule_chunk, worker_context

    def __len__(self) -> int:
        """Return total number of chunks.

        Returns:
            Number of chunks that will be yielded
        """
        import math

        return math.ceil(len(self.granules) / self.chunk_size)

    def chunks(self) -> List[tuple[list[Any], DistributedWorkerContext]]:
        """Eagerly evaluate all chunks into a list.

        This is useful for frameworks that need all work upfront
        (like Dask's compute or Ray's put).

        Returns:
            List of (granule_chunk, worker_context) tuples

        Example:
            >>> iterator = DistributedStreamingIterator(granules, auth_context)
            >>> all_chunks = iterator.chunks()
            >>> results = dask_client.compute(*[process(chunk, ctx) for chunk, ctx in all_chunks])
        """
        return list(self)

    def with_config(self, config: dict) -> DistributedStreamingIterator:
        """Create new iterator with additional worker configuration.

        The configuration is passed through DistributedWorkerContext to workers.

        Parameters:
            config: Configuration dictionary for workers

        Returns:
            New DistributedStreamingIterator with same granules/chunk_size but with config
        """
        return DistributedStreamingIterator(
            self.granules,
            self.auth_context,
            self.chunk_size,
            config=config,
        )


def process_granule_in_worker(
    granule: Any,
    worker_context: DistributedWorkerContext,
    operation: Any,
) -> Any:
    """Process a granule in a worker process with proper credential setup.

    This is a helper function for parallel operations that ensures
    credentials are properly reconstructed from the worker context.

    Parameters:
        granule: DataGranule object to process
        worker_context: DistributedWorkerContext with serialized credentials
        operation: Callable that performs the actual operation

    Returns:
        Result of the operation

    Example:
        >>> def open_file(granule, auth):
        ...     store = Store(auth)
        ...     return store.open_files([granule])[0]
        >>> result = process_granule_in_worker(granule, ctx, open_file)
    """
    # Reconstruct auth in worker
    auth = worker_context.reconstruct_auth()

    # Call the operation with reconstructed auth
    # The operation should accept (granule, auth) or similar
    return operation(granule, auth)


# Backward compatibility aliases
WorkerContext = DistributedWorkerContext
StreamingIterator = DistributedStreamingIterator

__all__ = [
    "DistributedWorkerContext",
    "DistributedStreamingIterator",
    "process_granule_in_worker",
    # Backward compatibility
    "WorkerContext",
    "StreamingIterator",
]
