"""Remote‑cloud storage helpers (S3, GCS, Azure)."""

from __future__ import annotations

import io
from typing import BinaryIO

# NOTE: The real implementation will still rely on fsspec or the
# respective cloud SDKs.  Here we only expose a thin wrapper that can be
# swapped out in tests.


def open_remote(url: str, mode: str = "rb") -> BinaryIO:
    """Open a remote object via fsspec."""
    import fsspec

    fs = fsspec.filesystem(url.split("://")[0])
    return fs.open(url, mode=mode)
