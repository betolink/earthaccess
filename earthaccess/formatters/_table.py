from tabulate import tabulate
from typing import Sequence, Mapping, Any


def to_table(
    rows: Sequence[Mapping[str, Any]], *, headers: Sequence[str] | None = None
) -> str:
    """Render rows as an ASCII table using ``tabulate``."""
    return tabulate(rows, headers=headers, tablefmt="github")
