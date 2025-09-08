import json
from typing import Any


def to_json(obj: Any, *, indent: int = 2) -> str:
    """Serialize *obj* to a pretty‑printed JSON string."""
    return json.dumps(obj, default=str, indent=indent)
