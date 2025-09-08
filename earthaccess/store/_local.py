"""Local‑filesystem storage helpers."""

from __future__ import annotations

import pathlib
from typing import BinaryIO, Union


def open_local(path: Union[str, pathlib.Path], mode: str = "rb") -> BinaryIO:
    """Open a local file, ensuring the parent directory exists for write modes."""
    p = pathlib.Path(path)
    if "w" in mode or "a" in mode or "+" in mode:
        p.parent.mkdir(parents=True, exist_ok=True)
    return p.open(mode)
