"""Cloud-to-cloud transfer functionality for earthaccess.

Provides optimized transfers between cloud providers using
server-side copy operations where possible.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import fsspec
import s3fs

from ..auth import Auth
from ..parallel import get_executor
from ..target_filesystem import TargetLocation

if TYPE_CHECKING:
    from ..results import DataGranule
    from .credentials import AuthContext, CredentialManager


class CloudTransfer:
    """Handles cloud-to-cloud data transfers.

    Single Responsibility: Optimize cloud data movement
    - Uses server-side copy when possible (S3-to-S3)
    - Falls back to download+upload when necessary
    - Provides progress tracking and error handling
    """

    def __init__(
        self,
        auth: Auth,
        credential_manager: Optional["CredentialManager"] = None,
    ) -> None:
        """Initialize cloud transfer manager.

        Args:
            auth: Auth instance for authentication
            credential_manager: Optional CredentialManager for credential caching
        """
        self.auth = auth
        self.credential_manager = credential_manager
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def transfer(
        self,
        granules: List[DataGranule],
        target: Union[str, TargetLocation],
        *,
        parallel: Union[str, bool, None] = True,
        max_workers: int = 8,
        show_progress: bool = True,
        preserve_structure: bool = True,
        overwrite: bool = False,
        **transfer_kwargs: Any,
    ) -> List[str]:
        """Transfer granules to cloud storage target.

        Args:
            granules: List of granules to transfer
            target: Target location (URL string or TargetLocation)
            parallel: Parallel execution strategy
            max_workers: Maximum number of concurrent transfers
            show_progress: Show progress bar
            preserve_structure: Maintain directory structure
            overwrite: Overwrite existing files
            **transfer_kwargs: Additional transfer options

        Returns:
            List of transferred file URLs

        Raises:
            ValueError: If target is invalid
            TransferError: If transfer fails
        """
        # Parse target
        target_url = self._parse_target(target)

        # Determine transfer strategy
        strategy = self._determine_strategy(granules, target_url)

        self._logger.info(
            f"Transferring {len(granules)} granules using {strategy} strategy"
        )

        if strategy == "s3_to_s3":
            return self._s3_server_copy(
                granules, target_url, parallel, max_workers, show_progress, overwrite
            )
        elif strategy == "https_to_s3":
            return self._download_upload(
                granules, target_url, parallel, max_workers, show_progress, overwrite
            )
        else:
            return self._generic_transfer(
                granules, target_url, parallel, max_workers, show_progress, overwrite
            )

    def _parse_target(self, target: Union[str, TargetLocation]) -> str:
        """Parse target to URL string."""
        if isinstance(target, str):
            return target
        elif hasattr(target, "path"):
            return target.path
        else:
            raise ValueError(f"Invalid target type: {type(target)}")

    def _determine_strategy(self, granules: List[DataGranule], target_url: str) -> str:
        """Determine optimal transfer strategy."""
        # Check if source is S3
        source_urls = []
        for granule in granules:
            source_urls.extend(granule.data_links(access="direct"))

        all_s3 = all(url.startswith("s3://") for url in source_urls)
        target_s3 = target_url.startswith("s3://")

        if all_s3 and target_s3:
            return "s3_to_s3"  # Optimal: server-side copy
        elif any(url.startswith("http") for url in source_urls) and target_s3:
            return "https_to_s3"  # Download from HTTPS, upload to S3
        else:
            return "generic"  # Fallback

    def _s3_server_copy(
        self,
        granules: List[DataGranule],
        target_url: str,
        parallel: Union[str, bool, None],
        max_workers: int,
        show_progress: bool,
        overwrite: bool,
    ) -> List[str]:
        """Perform S3-to-S3 server-side copies."""
        from .credentials import infer_provider_from_url

        # Get source and target credentials
        source_provider = self._infer_provider_from_granules(granules)
        target_provider = infer_provider_from_url(target_url)

        if not self.credential_manager:
            raise ValueError("CredentialManager required for S3 transfers")

        # Get filesystems
        source_fs = self.credential_manager.get_auth_context(
            source_provider, cloud_hosted=True
        )
        target_fs = self.credential_manager.get_auth_context(
            target_provider, cloud_hosted=True
        )

        # Use proper S3 filesystems
        source_s3fs = self._get_s3_fs_from_context(source_fs)
        target_s3fs = self._get_s3_fs_from_context(target_fs)

        # Parse target bucket once
        tgt_bucket, tgt_prefix = self._parse_s3_url(target_url)

        transferred = []

        def copy_file(source_url: str) -> str:
            """Copy single file using S3 server-side copy."""
            try:
                # Parse S3 paths
                src_bucket, src_key = self._parse_s3_url(source_url)
                # Create target key preserving structure
                target_key = self._create_target_key(source_url, src_key)

                # Build full target path
                if tgt_prefix:
                    full_target_path = f"{tgt_prefix}/{target_key}"
                else:
                    full_target_path = target_key

                # Check if target exists and overwrite is False
                if not overwrite and target_s3fs.exists(
                    f"{tgt_bucket}/{full_target_path}"
                ):
                    self._logger.warning(
                        f"Target exists, skipping: {tgt_bucket}/{full_target_path}"
                    )
                    return f"s3://{tgt_bucket}/{full_target_path}"

                # Perform server-side copy
                source_s3fs.copy(
                    f"{src_bucket}/{src_key}", f"{tgt_bucket}/{full_target_path}"
                )

                transferred.append(f"s3://{tgt_bucket}/{full_target_path}")
                self._logger.debug(
                    f"Copied {source_url} -> s3://{tgt_bucket}/{full_target_path}"
                )
                return f"s3://{tgt_bucket}/{full_target_path}"

            except Exception as e:
                self._logger.error(f"Failed to copy {source_url}: {e}")
                raise

        # Execute parallel copies
        urls = []
        for granule in granules:
            urls.extend(granule.data_links(access="direct"))

        executor = get_executor(
            parallel, max_workers=max_workers, show_progress=show_progress
        )

        try:
            list(executor.map(copy_file, urls))
        finally:
            executor.shutdown()

        return transferred

    def _download_upload(
        self,
        granules: List[DataGranule],
        target_url: str,
        parallel: Union[str, bool, None],
        max_workers: int,
        show_progress: bool,
        overwrite: bool,
    ) -> List[str]:
        """Download from HTTPS and upload to S3."""
        # Get target credentials
        from .credentials import infer_provider_from_url

        target_provider = infer_provider_from_url(target_url)

        if not self.credential_manager:
            raise ValueError("CredentialManager required for HTTPS transfers")

        target_context = self.credential_manager.get_auth_context(
            target_provider, cloud_hosted=True
        )
        target_s3fs = self._get_s3_fs_from_context(target_context)

        # Parse target bucket once
        tgt_bucket, tgt_prefix = self._parse_s3_url(target_url)

        transferred = []

        def download_and_upload(source_url: str) -> str:
            """Download from HTTPS and upload to S3."""
            try:
                # Use fsspec for download
                src_file = fsspec.open(source_url, "rb")
                with src_file as src:
                    # Create target key - extract filename from URL
                    source_key = source_url.split("/")[-1]
                    target_key = self._create_target_key(source_url, source_key)

                    # Build full target path
                    if tgt_prefix:
                        full_target_path = f"{tgt_prefix}/{target_key}"
                    else:
                        full_target_path = target_key

                    # Check if target exists and overwrite is False
                    if not overwrite and target_s3fs.exists(
                        f"{tgt_bucket}/{full_target_path}"
                    ):
                        self._logger.warning(
                            f"Target exists, skipping: {tgt_bucket}/{full_target_path}"
                        )
                        return f"s3://{tgt_bucket}/{full_target_path}"

                    # Upload to S3
                    with target_s3fs.open(
                        f"{tgt_bucket}/{full_target_path}", "wb"
                    ) as dst:
                        # Copy in chunks to handle large files
                        while True:
                            chunk = src.read(8 * 1024 * 1024)  # 8MB chunks
                            if not chunk:
                                break
                            dst.write(chunk)

                    transferred.append(f"s3://{tgt_bucket}/{full_target_path}")
                    self._logger.debug(
                        f"Uploaded {source_url} -> s3://{tgt_bucket}/{full_target_path}"
                    )
                    return f"s3://{tgt_bucket}/{full_target_path}"

            except Exception as e:
                self._logger.error(f"Failed to transfer {source_url}: {e}")
                raise

        # Get all source URLs (mix of HTTPS and S3)
        urls = []
        for granule in granules:
            # Use direct access if available, otherwise HTTP
            try:
                urls.extend(granule.data_links(access="direct"))
            except Exception:
                urls.extend(granule.data_links(access="onprem"))

        executor = get_executor(
            parallel, max_workers=max_workers, show_progress=show_progress
        )

        try:
            list(executor.map(download_and_upload, urls))
        finally:
            executor.shutdown()

        return transferred

    def _generic_transfer(
        self,
        granules: List[DataGranule],
        target_url: str,
        parallel: Union[str, bool, None],
        max_workers: int,
        show_progress: bool,
        overwrite: bool,
    ) -> List[str]:
        """Generic transfer using fsspec."""
        transferred = []

        def transfer_file(source_url: str) -> str:
            """Transfer single file using fsspec."""
            try:
                # Use fsspec for generic transfer
                with fsspec.open(source_url, "rb") as src:
                    # Create target key - extract filename from URL
                    source_key = source_url.split("/")[-1]
                    target_key = self._create_target_key(source_url, source_key)
                    full_target = target_url + "/" + target_key

                    # Check if target exists
                    if not overwrite and fsspec.exists(full_target):
                        self._logger.warning(f"Target exists, skipping: {full_target}")
                        return full_target

                    with fsspec.open(full_target, "wb") as dst:
                        # Copy in chunks
                        while True:
                            chunk = src.read(8 * 1024 * 1024)
                            if not chunk:
                                break
                            dst.write(chunk)

                    transferred.append(full_target)
                    self._logger.debug(f"Transferred {source_url} -> {full_target}")
                    return full_target

            except Exception as e:
                self._logger.error(f"Failed to transfer {source_url}: {e}")
                raise

        urls = []
        for granule in granules:
            urls.extend(granule.data_links())

        executor = get_executor(
            parallel, max_workers=max_workers, show_progress=show_progress
        )

        try:
            list(executor.map(transfer_file, urls))
        finally:
            executor.shutdown()

        return transferred

    def _infer_provider_from_granules(
        self, granules: List[DataGranule]
    ) -> Optional[str]:
        """Infer provider from granules."""
        from .credentials import infer_provider_from_url

        providers = set()
        for granule in granules:
            for url in granule.data_links(access="direct"):
                provider = infer_provider_from_url(url)
                if provider:
                    providers.add(provider)

        return list(providers)[0] if providers else None

    def _get_s3_fs_from_context(self, context: "AuthContext") -> s3fs.S3FileSystem:
        """Get S3 filesystem from AuthContext."""
        if not context.s3_credentials:
            raise ValueError("S3 credentials not found in context")

        return s3fs.S3FileSystem(**context.s3_credentials.to_dict())

    def _parse_s3_url(self, url: str) -> Tuple[str, str]:
        """Parse S3 URL into bucket and key.

        Args:
            url: S3 URL (e.g., "s3://bucket/path/to/file" or "s3://bucket")

        Returns:
            Tuple of (bucket, key) where key may be empty string
        """
        if not url.startswith("s3://"):
            raise ValueError(f"Not an S3 URL: {url}")

        # Remove s3:// prefix
        path = url[5:]

        # Split into bucket and key (key may be empty)
        if "/" in path:
            bucket, key = path.split("/", 1)
        else:
            bucket = path
            key = ""

        return bucket, key

    def _create_target_key(self, source_url: str, source_key: str) -> str:
        """Create target key preserving directory structure.

        Args:
            source_url: Full source URL
            source_key: Source key (already parsed without bucket)

        Returns:
            Target key path
        """
        # source_key is already just the key part without the bucket
        # Return it as-is to preserve the directory structure
        return source_key

    def get_transfer_estimate(
        self,
        granules: List[DataGranule],
        target_url: str,
        strategy: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get transfer time and size estimates.

        Args:
            granules: Granules to transfer
            target_url: Target URL
            strategy: Transfer strategy (auto-detected if None)

        Returns:
            Dictionary with estimates:
            - strategy: Transfer strategy that will be used
            - file_count: Number of files
            - estimated_size: Total size in bytes (if available)
            - estimated_time: Rough time estimate in seconds
        """
        if not strategy:
            strategy = self._determine_strategy(granules, target_url)

        file_count = sum(len(g.data_links()) for g in granules)

        # Size estimation (if available from metadata)
        estimated_size: Union[int, float] = 0
        for granule in granules:
            try:
                # Try to get size from metadata
                size = granule.get("meta", {}).get("granule-size", 0)
                if isinstance(size, (int, float)):
                    estimated_size += size
            except (KeyError, TypeError):
                pass

        # Rough time estimates based on strategy
        if strategy == "s3_to_s3":
            # Server-side copy: very fast, network-limited
            bytes_per_second = 50 * 1024 * 1024  # 50 MB/s
        elif strategy == "https_to_s3":
            # Download + upload: slower
            bytes_per_second = 10 * 1024 * 1024  # 10 MB/s
        else:
            # Generic transfer: moderate speed
            bytes_per_second = 20 * 1024 * 1024  # 20 MB/s

        estimated_time = (
            estimated_size / bytes_per_second if estimated_size > 0 else None
        )

        return {
            "strategy": strategy,
            "file_count": file_count,
            "estimated_size": estimated_size,
            "estimated_time": estimated_time,
        }


class TransferError(Exception):
    """Raised when a transfer operation fails."""

    def __init__(
        self,
        message: str,
        source_url: str,
        target_url: str,
        cause: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.source_url = source_url
        self.target_url = target_url
        self.cause = cause
