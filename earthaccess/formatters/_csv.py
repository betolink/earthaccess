import csv
import io
from typing import Iterable, Mapping, Sequence


def to_csv(rows: Sequence[Mapping[str, Any]]) -> str:
    """Convert a list of dict‑like rows to CSV text."""
    if not rows:
        return ""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()
