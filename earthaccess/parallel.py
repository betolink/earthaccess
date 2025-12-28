"""Generic parallel execution utilities for earthaccess.

This module provides a unified interface for different parallel execution backends,
following the concurrent.futures.Executor API pattern.
"""

import warnings
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    TypeVar,
    Union,
)

__all__ = [
    "SerialExecutor",
    "ThreadPoolExecutorWrapper",
    "DaskDelayedExecutor",
    "LithopsEagerFunctionExecutor",
    "get_executor",
    "execute_with_executor",
    "submit_all_and_wait",
    "execute_with_credentials",
]

T = TypeVar("T")


class SerialExecutor(Executor):
    """A custom Executor that runs tasks sequentially, mimicking the
    concurrent.futures.Executor interface. Useful as a default and for debugging.
    """

    def __init__(self) -> None:
        # Track submitted futures to maintain interface compatibility
        self._futures: list[Future] = []

    def submit(self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> Future[T]:
        """Submit a callable to be executed.

        Unlike parallel executors, this runs the task immediately and sequentially.

        Parameters
        ----------
        fn
            The callable to execute
        *args
            Positional arguments for the callable
        **kwargs
            Keyword arguments for the callable

        Returns:
        -------
        A Future representing the result of the execution
        """
        # Create a future to maintain interface compatibility
        future: Future = Future()

        try:
            # Execute the function immediately
            result = fn(*args, **kwargs)

            # Set the result of the future
            future.set_result(result)
        except Exception as e:
            # If an exception occurs, set it on the future
            future.set_exception(e)

        # Keep track of futures for potential cleanup
        self._futures.append(future)

        return future

    def map(
        self,
        fn: Callable[..., T],
        *iterables: Iterable[Any],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[T]:
        """Execute a function over an iterable sequentially.

        Parameters
        ----------
        fn
            Function to apply to each item
        *iterables
            Iterables to process
        timeout
            Optional timeout (ignored in serial execution)

        Returns:
        -------
        Generator of results
        """
        return map(fn, *iterables)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        """Shutdown the executor.

        Parameters
        ----------
        wait
            Whether to wait for pending futures (always True for serial executor)
        """
        # In a serial executor, shutdown is a no-op
        pass


class ThreadPoolExecutorWrapper(Executor):
    """A wrapper around ThreadPoolExecutor that provides a consistent interface
    and handles common configuration patterns with optional progress bars.
    """

    def __init__(
        self,
        max_workers: Union[int, None] = None,
        show_progress: bool = True,
        **kwargs: Any,
    ) -> None:
        """Initialize thread pool executor."""
        self._max_workers = max_workers
        self._show_progress = show_progress
        self._executor_kwargs = kwargs
        self._executor: Optional[ThreadPoolExecutor] = None

    def submit(self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> Future[T]:
        """Submit a task to thread pool."""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=self._max_workers, **self._executor_kwargs
            )
        assert self._executor is not None
        return self._executor.submit(fn, *args, **kwargs)

    def map(
        self,
        fn: Callable[..., T],
        *iterables: Iterable[Any],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[T]:
        """Map a function over iterables using thread pool."""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=self._max_workers, **self._executor_kwargs
            )
        assert self._executor is not None
        return self._executor.map(fn, *iterables, timeout=timeout, chunksize=chunksize)

    def map_with_progress(
        self,
        fn: Callable[..., T],
        *iterables: Iterable[Any],
        desc: str = "Processing",
        **kwargs: Any,
    ) -> List[T]:
        """Map a function over iterables with optional progress bar."""
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=self._max_workers, **self._executor_kwargs
            )
        assert self._executor is not None

        if self._show_progress:
            try:
                from tqdm import tqdm  # type: ignore[import-untyped]

                # Get the first iterable to determine length
                if iterables:
                    first_iterable = iterables[0]
                    try:
                        total = len(first_iterable)  # type: ignore[arg-type]
                    except TypeError:
                        total = None
                else:
                    total = None

                return list(
                    tqdm(
                        self._executor.map(fn, *iterables, **kwargs),  # type: ignore[attr-defined]
                        total=total,
                        desc=desc,
                    )
                )
            except ImportError:
                # Fallback without progress if tqdm not available
                pass

        return list(self._executor.map(fn, *iterables, **kwargs))  # type: ignore[attr-defined]

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        """Shutdown thread pool executor."""
        if self._executor is not None:
            self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)
        self._executor = None


def get_executor(
    parallel: Union[str, Executor, bool, None] = True,
    max_workers: Union[int, None] = None,
    show_progress: bool = True,
    **kwargs: Any,
) -> Executor:
    """Get an executor that follows the concurrent.futures.Executor ABC API.

    Parameters
    ----------
    parallel : str, Executor, bool, or None
        Parallel execution backend. Options:
        - True or "threads": Use ThreadPoolExecutor
        - False or "serial": Use SerialExecutor
        - Executor instance: Use the provided executor
        - "dask": Use DaskDelayedExecutor (if available)
        - "lithops": Use LithopsEagerFunctionExecutor (if available)
        - None: Use default (ThreadPoolExecutor)
    max_workers : int, optional
        Maximum number of worker threads/processes
    show_progress : bool, optional
        Whether to show progress bars (default: True)
    **kwargs
        Additional arguments passed to the executor

    Returns:
    -------
    Executor
        An executor instance following the concurrent.futures.Executor API

    Examples:
    --------
    >>> executor = get_executor("threads", max_workers=4)
    >>> executor = get_executor("serial")
    >>> executor = get_executor(False)
    >>> executor = get_executor("dask")
    >>> executor = get_executor("lithops")
    """
    if parallel is None or parallel is True:
        # Default to thread pool
        return ThreadPoolExecutorWrapper(
            max_workers=max_workers, show_progress=show_progress, **kwargs
        )
    elif parallel is False:
        # Serial execution
        return SerialExecutor()
    elif isinstance(parallel, str):
        parallel = parallel.lower()
        if parallel in ("threads", "thread", "threadpool"):
            return ThreadPoolExecutorWrapper(
                max_workers=max_workers, show_progress=show_progress, **kwargs
            )
        elif parallel in ("serial", "none", "disabled"):
            return SerialExecutor()
        elif parallel == "dask":
            try:
                return DaskDelayedExecutor(
                    max_workers=max_workers, show_progress=show_progress, **kwargs
                )
            except ImportError:
                warnings.warn(
                    "Dask is not installed. Falling back to ThreadPoolExecutor.",
                    ImportWarning,
                )
                return ThreadPoolExecutorWrapper(
                    max_workers=max_workers, show_progress=show_progress, **kwargs
                )
        elif parallel == "lithops":
            try:
                return LithopsEagerFunctionExecutor(**kwargs)
            except ImportError:
                warnings.warn(
                    "Lithops is not installed. Falling back to ThreadPoolExecutor.",
                    ImportWarning,
                )
                return ThreadPoolExecutorWrapper(
                    max_workers=max_workers, show_progress=show_progress, **kwargs
                )
        else:
            raise ValueError(
                f"Unrecognized parallel backend: {parallel}. "
                "Valid options are: 'threads', 'serial', 'dask', 'lithops', or an Executor instance."
            )
    elif isinstance(parallel, Executor):
        # Use the provided executor
        return parallel
    else:
        raise ValueError(
            f"Invalid parallel argument: {parallel}. "
            "Must be a string, Executor instance, or boolean."
        )


class DaskDelayedExecutor(Executor):
    """An Executor that uses [dask.delayed][dask.delayed.delayed] for parallel computation.

    This executor mimics the concurrent.futures.Executor interface but uses Dask's delayed computation model.
    """

    def __init__(
        self,
        max_workers: Union[int, None] = None,
        show_progress: bool = True,
        **kwargs: Any,
    ) -> None:
        """Initialize the Dask Delayed Executor."""
        try:
            import dask  # type: ignore[import-untyped]
        except ImportError:
            raise ImportError("Dask is required for DaskDelayedExecutor")

        self._max_workers = max_workers
        self._show_progress = show_progress
        self._kwargs = kwargs
        self._dask = dask

    def submit(self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> Future[T]:
        """Submit a task to be computed with [dask.delayed][dask.delayed.delayed].

        Parameters
        ----------
        fn
            The callable to execute
        *args
            Positional arguments for the callable
        **kwargs
            Keyword arguments for the callable

        Returns:
        -------
        A Future representing the result of the execution
        """
        # Create a delayed computation
        delayed_task = self._dask.delayed(fn)(*args, **kwargs)

        # Create a concurrent.futures Future to maintain interface compatibility
        future: Future = Future()

        try:
            # Compute the result
            result = delayed_task.compute()

            # Set the result on the future
            future.set_result(result)
        except Exception as e:
            # Set any exception on the future
            future.set_exception(e)

        return future

    def map(
        self,
        fn: Callable[..., T],
        *iterables: Iterable[Any],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[T]:
        """Apply a function to an iterable using [dask.delayed][dask.delayed.delayed].

        Parameters
        ----------
        fn
            Function to apply to each item
        *iterables
            Iterables to process
        timeout
            Optional timeout (ignored in serial execution)

        Returns:
        -------
        Generator of results
        """
        if timeout is not None:
            warnings.warn("Timeout parameter is not directly supported by Dask delayed")

        # Create delayed computations for each item
        delayed_tasks = [self._dask.delayed(fn)(*items) for items in zip(*iterables)]

        # Compute all tasks
        return iter(self._dask.compute(*delayed_tasks))

    def map_with_progress(
        self,
        fn: Callable[..., T],
        *iterables: Iterable[Any],
        desc: str = "Processing",
        **kwargs: Any,
    ) -> List[T]:
        """Map a function over iterables with optional progress bar."""
        # Create delayed computations for each item
        delayed_tasks = [self._dask.delayed(fn)(*items) for items in zip(*iterables)]

        if self._show_progress:
            try:
                from dask.diagnostics import ProgressBar  # type: ignore[import]

                # Use Dask's built-in progress bar
                with ProgressBar():
                    results = self._dask.compute(*delayed_tasks)
                return list(results)
            except ImportError:
                # Fallback without progress if dask.diagnostics not available
                pass

        # Compute without progress bar
        results = self._dask.compute(*delayed_tasks)
        return list(results)

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        """Shutdown the executor.

        Parameters
        ----------
        wait
            Whether to wait for pending futures (always True for serial executor))
        """
        # For Dask.delayed, shutdown is essentially a no-op
        pass


class LithopsEagerFunctionExecutor(Executor):
    """Lithops-based function executor which follows the [concurrent.futures.Executor][] API.

    Only required because lithops doesn't follow the [concurrent.futures.Executor][] API, see https://github.com/lithops-cloud/lithops/issues/1427.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the Lithops executor."""
        try:
            import lithops  # type: ignore[import-not-found, import-untyped]
        except ImportError:
            raise ImportError("Lithops is required for LithopsEagerFunctionExecutor")

        # Create Lithops client with optional configuration
        self.lithops_client = lithops.FunctionExecutor(**kwargs)

    def submit(self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> Future[T]:
        """Submit a task to be computed using lithops.

        Parameters
        ----------
        fn
            The callable to execute
        *args
            Positional arguments for the callable
        **kwargs
            Keyword arguments for the callable

        Returns:
        -------
        A concurrent.futures.Future representing the result of the execution
        """
        # Create a concurrent.futures Future to maintain interface compatibility
        future: Future = Future()

        try:
            # Submit to Lithops
            lithops_future = self.lithops_client.call_async(fn, *args, **kwargs)

            # Add a callback to set the result or exception
            def _on_done(lithops_result: Any) -> None:
                try:
                    result = lithops_result.result()
                    future.set_result(result)
                except Exception as e:
                    future.set_exception(e)

            # Register the callback
            lithops_future.add_done_callback(_on_done)
        except Exception as e:
            # If submission fails, set exception immediately
            future.set_exception(e)

        return future

    def map(
        self,
        fn: Callable[..., T],
        *iterables: Iterable[Any],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[T]:
        """Apply a function to an iterable using lithops.

        Only needed because [lithops.executors.FunctionExecutor.map][lithops.executors.FunctionExecutor.map] returns futures, unlike [concurrent.futures.Executor.map][].

        Parameters
        ----------
        fn
            Function to apply to each item
        *iterables
            Iterables to process
        timeout
            Optional timeout (ignored in serial execution)

        Returns:
        -------
        Generator of results
        """
        futures = self.lithops_client.map(fn, *iterables)
        results = self.lithops_client.get_result(futures)

        return results

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        """Shutdown the executor.

        Parameters
        ----------
        wait
            Whether to wait for pending futures.
        """
        # Should this call lithops .clean() method?
        pass


def execute_with_executor(
    executor: Executor,
    func: Callable[..., T],
    iterable: Iterable[Any],
    *args: Any,
    **kwargs: Any,
) -> list[T]:
    """Execute a function over an iterable using the provided executor.

    This is a convenience function that handles both map and submit patterns
    and returns a list of results.

    Parameters
    ----------
    executor : Executor
        The executor to use for parallel execution
    func : Callable
        Function to apply to each item
    iterable : Iterable[Any]
        Iterable of items to process
    *args
        Additional positional arguments to pass to func
    **kwargs
        Additional keyword arguments to pass to func

    Returns:
    -------
    list[T]
        List of results from applying func to each item
    """

    # Prepare arguments for each item
    def wrapped_func(item: Any) -> T:
        return func(item, *args, **kwargs)

    # Use map for better performance with large iterables
    results = list(executor.map(wrapped_func, iterable))
    return results


def submit_all_and_wait(
    executor: Executor,
    func: Callable[..., T],
    iterable: Iterable[Any],
    *args: Any,
    **kwargs: Any,
) -> list[T]:
    """Submit all tasks to executor and wait for all to complete.

    This is useful when you need more control over the submission process
    or when working with futures directly.

    Parameters
    ----------
    executor : Executor
        The executor to use for parallel execution
    func : Callable
        Function to apply to each item
    iterable : Iterable[Any]
        Iterable of items to process
    *args
        Additional positional arguments to pass to func
    **kwargs
        Additional keyword arguments to pass to func

    Returns:
    -------
    list[T]
        List of results from applying func to each item
    """
    futures = []
    for item in iterable:
        future = executor.submit(func, item, *args, **kwargs)
        futures.append(future)

    # Wait for all futures to complete and get results
    results = [future.result() for future in futures]
    return results


def execute_with_credentials(
    executor: Executor,
    func: Callable[..., T],
    iterable: Iterable[Any],
    credentials_context: Any,
    *args: Any,
    **kwargs: Any,
) -> list[T]:
    """Execute a function over an iterable with credential context distribution.

    This function wraps the provided function to include a credentials context
    that can be used by worker processes for cloud and on-premises data access.
    The credentials are serialized and passed to each worker.

    Parameters
    ----------
    executor : Executor
        The executor to use for parallel execution (Serial, ThreadPool, Dask, Lithops)
    func : Callable
        Function to apply to each item. Should accept (item, credentials_context)
    iterable : Iterable[Any]
        Iterable of items to process
    credentials_context : AuthContext
        The credentials context to distribute to workers. Should be an AuthContext
        from earthaccess.credentials_store.credentials
    *args
        Additional positional arguments to pass to func
    **kwargs
        Additional keyword arguments to pass to func

    Returns:
    -------
    list[T]
        List of results from applying func to each item with credentials

    Examples:
    --------
    >>> from earthaccess.parallel import get_executor, execute_with_credentials
    >>> from earthaccess.credentials_store.credentials import AuthContext
    >>>
    >>> # Create auth context from authenticated auth
    >>> auth_context = AuthContext.from_auth(earthaccess.__auth__)
    >>>
    >>> # Define operation that uses credentials
    >>> def download_granule(granule, auth_context):
    ...     return granule.download(auth_context=auth_context, path="/data")
    >>>
    >>> # Execute in parallel with credentials distributed to workers
    >>> executor = get_executor("threads", max_workers=4)
    >>> results = execute_with_credentials(
    ...     executor, download_granule, granules, auth_context
    ... )
    """

    # Wrap function to include credentials context for each item
    def func_with_credentials(item: Any) -> T:
        return func(item, credentials_context, *args, **kwargs)

    # Use map for better performance with large iterables
    results = list(executor.map(func_with_credentials, iterable))
    return results
