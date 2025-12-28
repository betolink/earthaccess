"""Download operations for earthaccess store.

This module provides functions for downloading granules from NASA Earthdata,
supporting both HTTP and S3 access patterns. It follows SOLID principles
with single responsibility for download operations.
"""

import logging
from pathlib import Path
from typing import List, Optional, Union

import fsspec
import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .parallel import get_executor
from .target import TargetLocation

logger = logging.getLogger(__name__)

# Default chunk size for streaming downloads (1MB)
DEFAULT_CHUNK_SIZE = 1024 * 1024

__all__ = [
    "download_file",
    "download_cloud_file",
    "download_granules",
    "clone_session",
    "DEFAULT_CHUNK_SIZE",
]


def clone_session(
    original_session: requests.Session,
) -> requests.Session:
    """Clone a session for use in a worker thread.

    Creates a new session instance that replicates the headers, cookies,
    and authentication settings from the original session.

    Parameters:
        original_session: The session to clone.

    Returns:
        A new session with the same configuration.
    """
    cloned = requests.Session()
    cloned.headers.update(original_session.headers)
    cloned.cookies.update(original_session.cookies)
    cloned.auth = original_session.auth
    # Copy response hooks from original session
    cloned.hooks["response"] = list(original_session.hooks.get("response", []))
    return cloned


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(Exception),
)
def download_file(
    url: str,
    directory: Union[Path, TargetLocation],
    session: requests.Session,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> Path:
    """Download a single file using HTTP.

    Downloads a file from the given URL to the target directory.
    Supports both local paths and cloud storage via TargetLocation.

    Parameters:
        url: The URL to download from.
        directory: Target directory (Path or TargetLocation).
        session: Authenticated requests session.
        chunk_size: Size of chunks for streaming download.

    Returns:
        Path to the downloaded file.
    """
    # Handle OpenDAP URLs by stripping .html suffix
    if "opendap" in url and url.endswith(".html"):
        url = url.replace(".html", "")

    local_filename = url.split("/")[-1]

    # Handle TargetLocation (cloud storage)
    if isinstance(directory, TargetLocation):
        filesystem = directory.get_filesystem()
        target_path = filesystem.join(local_filename)

        if filesystem.exists(target_path):
            logger.info(f"File {local_filename} already downloaded")
            return Path(target_path)

        with session.get(url, stream=True, allow_redirects=True) as response:
            response.raise_for_status()
            with filesystem.open(target_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    f.write(chunk)

        return Path(target_path)

    # Handle local Path
    path = directory / Path(local_filename)
    if path.exists():
        logger.info(f"File {local_filename} already downloaded")
        return path

    with session.get(url, stream=True, allow_redirects=True) as response:
        response.raise_for_status()
        with open(path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                f.write(chunk)

    logger.info(f"Downloaded: {local_filename}")
    return path


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type(Exception),
)
def download_cloud_file(
    s3_fs: fsspec.AbstractFileSystem,
    file: str,
    path: Union[Path, TargetLocation],
) -> Path:
    """Download a file from S3 to local or cloud storage.

    Uses the provided S3 filesystem to download the file.
    Supports both local paths and cloud storage via TargetLocation.

    Parameters:
        s3_fs: Authenticated S3 filesystem.
        file: S3 URL (s3://bucket/key).
        path: Target directory (Path or TargetLocation).

    Returns:
        Path to the downloaded file.
    """
    file_name = Path(file).name

    # Handle TargetLocation
    if isinstance(path, TargetLocation):
        filesystem = path.get_filesystem()
        target_path = filesystem.join(file_name)

        if filesystem.exists(target_path):
            return Path(target_path)

        # Download to temporary location, then copy to target
        import tempfile

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir) / file_name
            s3_fs.get([file], str(temp_path), recursive=False)
            logger.info(f"Downloading: {file_name}")

            # Copy to target filesystem
            with open(temp_path, "rb") as src_file:
                with filesystem.open(target_path, "wb") as dst_file:
                    dst_file.write(src_file.read())

        return Path(target_path)

    # Handle local Path
    file_path = path / file_name
    if file_path.exists():
        return file_path

    s3_fs.get([file], str(path), recursive=False)
    logger.info(f"Downloaded: {file_name}")
    return file_path


def download_granules(
    urls: List[str],
    directory: Union[Path, TargetLocation],
    session: requests.Session,
    *,
    max_workers: Optional[int] = None,
    show_progress: bool = True,
    parallel: Union[str, bool, None] = None,
) -> List[Path]:
    """Download a list of URLs to the target directory.

    Uses parallel execution for efficient batch downloads.
    Creates the target directory if it doesn't exist.

    Parameters:
        urls: List of URLs to download.
        directory: Target directory (Path or TargetLocation).
        session: Authenticated requests session.
        max_workers: Maximum number of worker threads.
        show_progress: Whether to show a progress bar.
        parallel: Parallel execution strategy.

    Returns:
        List of paths to downloaded files.

    Raises:
        ValueError: If urls is None or empty.
    """
    if urls is None:
        raise ValueError("URLs list cannot be None")

    if not urls:
        return []

    # Create target directory
    if isinstance(directory, TargetLocation):
        filesystem = directory.get_filesystem()
        filesystem.mkdir("", exist_ok=True)
    else:
        directory.mkdir(parents=True, exist_ok=True)

    def _download(url: str) -> Path:
        return download_file(url, directory, session)

    # Get executor and execute
    executor = get_executor(
        parallel, max_workers=max_workers, show_progress=show_progress
    )
    try:
        results = list(executor.map(_download, urls))
        return [r for r in results if r is not None]
    finally:
        executor.shutdown(wait=True)


def download_cloud_granules(
    files: List[str],
    path: Union[Path, TargetLocation],
    s3_fs: fsspec.AbstractFileSystem,
    *,
    max_workers: Optional[int] = None,
    show_progress: bool = True,
    parallel: Union[str, bool, None] = None,
) -> List[Path]:
    """Download a list of S3 files to the target directory.

    Uses parallel execution for efficient batch downloads from S3.
    Creates the target directory if it doesn't exist.

    Parameters:
        files: List of S3 URLs to download.
        path: Target directory (Path or TargetLocation).
        s3_fs: Authenticated S3 filesystem.
        max_workers: Maximum number of worker threads.
        show_progress: Whether to show a progress bar.
        parallel: Parallel execution strategy.

    Returns:
        List of paths to downloaded files.
    """
    if not files:
        return []

    # Create target directory
    if isinstance(path, TargetLocation):
        filesystem = path.get_filesystem()
        filesystem.mkdir("", exist_ok=True)
    else:
        path.mkdir(parents=True, exist_ok=True)

    def _download(file: str) -> Optional[Path]:
        return download_cloud_file(s3_fs, file, path)

    # Get executor and execute
    executor = get_executor(
        parallel, max_workers=max_workers, show_progress=show_progress
    )
    try:
        results = list(executor.map(_download, files))
        return [r for r in results if r is not None]
    finally:
        executor.shutdown(wait=True)
