"""Entry-point discovery and the earthaccess plugin registry.

Discovery reads distribution metadata only: it never imports a plugin module
(and therefore never a backend) until that plugin is actually requested.
"""

from __future__ import annotations

import logging
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from importlib import metadata
from typing import Any

from earthaccess_plugin_spec import (
    CONTRACT_VERSION,
    PluginBase,
    PluginContext,
    PluginLoadError,
    PluginNotFoundError,
    default_context,
)

__all__ = ["PluginInfo", "PluginList", "PluginRegistry", "default_registry"]

logger = logging.getLogger("earthaccess.plugins")

# Known plugins that are not installed still show up, so users can see the
# install hint. (name -> (distribution, summary))
_KNOWN: dict[str, tuple[str, str]] = {
    "harmony": ("earthaccess-harmony", "Subset and transform data with NASA Harmony"),
    "pydap": ("earthaccess-pydap", "Lazy OPeNDAP access through PyDAP"),
    "sliderule": (
        "earthaccess-sliderule",
        "On-demand ICESat-2 and GEDI processing with SlideRule",
    ),
}
_RESERVED = {"get", "available"}


@dataclass
class PluginInfo:
    """One row of :meth:`PluginRegistry.list_plugins`."""

    name: str
    installed: bool
    loaded: bool
    version: str | None = None
    summary: str = ""
    contract_version: int | None = None
    needs: frozenset | None = None
    auth: str | None = None
    output: str | None = None
    error: str | None = None


class PluginList(list):
    """A list of :class:`PluginInfo` with a table repr."""

    _COLUMNS = ("name", "installed", "version", "summary")

    def _render(self) -> str:
        header = tuple(self._COLUMNS)
        cells = [
            (
                row.name,
                "yes" if row.installed else "no",
                row.version or "-",
                row.error or row.summary,
            )
            for row in self
        ]
        widths = [len(col) for col in header]
        for row in cells:
            for i, cell in enumerate(row):
                widths[i] = max(widths[i], len(str(cell)))
        lines = [
            "  ".join(str(c).ljust(widths[i]) for i, c in enumerate(header)).rstrip(),
            "  ".join("-" * widths[i] for i in range(len(header))),
        ]
        for row in cells:
            lines.append(
                "  ".join(str(c).ljust(widths[i]) for i, c in enumerate(row)).rstrip()
            )
        return "\n".join(lines)

    def __repr__(self) -> str:
        return self._render()

    def _repr_html_(self) -> str:
        head = "".join(f"<th>{col}</th>" for col in self._COLUMNS)
        rows = "".join(
            "<tr>"
            + f"<td>{row.name}</td>"
            + f"<td>{'yes' if row.installed else 'no'}</td>"
            + f"<td>{row.version or ''}</td>"
            + f"<td>{row.error or row.summary}</td>"
            + "</tr>"
            for row in self
        )
        return f"<table><tr>{head}</tr>{rows}</table>"


def _dist_version(entry_point: Any) -> str | None:
    dist = getattr(entry_point, "dist", None)
    return getattr(dist, "version", None) if dist is not None else None


def _dist_summary(entry_point: Any) -> str:
    dist = getattr(entry_point, "dist", None)
    meta = getattr(dist, "metadata", None)
    if meta is None:
        return ""
    return meta.get("Summary") or ""


class PluginRegistry:
    """Discovers plugins through entry points and caches loaded instances."""

    def __init__(
        self,
        group: str = "earthaccess.plugins",
        context_factory: Callable[[], PluginContext] = default_context,
    ) -> None:
        self._group = group
        self._context_factory = context_factory
        self._entry_points: dict[str, Any] | None = None
        self._instances: dict[str, PluginBase] = {}

    @property
    def entry_points(self) -> dict[str, Any]:
        if self._entry_points is None:
            found = sorted(
                metadata.entry_points(group=self._group),
                key=lambda ep: ep.dist.name if ep.dist else "",
            )
            self._entry_points = {}
            for entry_point in found:
                if entry_point.name in _RESERVED:
                    warnings.warn(
                        f"ignoring plugin {entry_point.name!r}: reserved name",
                        stacklevel=2,
                    )
                elif entry_point.name in self._entry_points:
                    warnings.warn(
                        f"plugin {entry_point.name!r} is provided twice; using the first",
                        stacklevel=2,
                    )
                else:
                    self._entry_points[entry_point.name] = entry_point
        return self._entry_points

    def names(self) -> list[str]:
        return sorted(set(self.entry_points) | set(_KNOWN))

    def _load_class(self, name: str) -> type[PluginBase]:
        entry_point = self.entry_points.get(name)
        if entry_point is None:
            known = _KNOWN.get(name)
            hint = f" (pip install {known[0]})" if known else ""
            raise PluginNotFoundError(
                f"plugin {name!r} is not installed{hint}; known: {self.names()}"
            )
        try:
            cls = entry_point.load()
        except Exception as exc:
            raise PluginLoadError(f"failed to load plugin {name!r}: {exc}") from exc
        got = getattr(cls, "contract_version", None)
        if got != CONTRACT_VERSION:
            raise PluginLoadError(
                f"plugin {name!r} targets contract {got}; core supports {CONTRACT_VERSION}"
            )
        return cls

    def __getitem__(self, name: str) -> PluginBase:
        if name not in self._instances:
            self._instances[name] = self._load_class(name)(self._context_factory())
        return self._instances[name]

    def get(self, name: str) -> PluginBase:
        """Return a loaded plugin instance by name."""
        return self[name]

    def available(self) -> list[str]:
        """Return the names of known and installed plugins."""
        return self.names()

    def list_plugins(self, details: bool = False) -> PluginList:
        """Return a table-friendly list of plugins, known and installed."""
        rows = PluginList()
        for name in self.names():
            entry_point = self.entry_points.get(name)
            summary = _KNOWN.get(name, (None, ""))[1] or _dist_summary(entry_point)
            row = PluginInfo(
                name=name,
                installed=entry_point is not None,
                loaded=name in self._instances,
                version=_dist_version(entry_point),
                summary=summary,
            )
            if details and entry_point is not None:
                try:
                    cls = self._load_class(name)
                    row.contract_version = cls.contract_version
                    row.needs = cls.needs
                    row.auth = cls.auth
                    row.output = cls.output
                except PluginLoadError as exc:
                    row.error = str(exc)
            rows.append(row)
        return rows

    def table(self) -> str:
        return repr(self.list_plugins())

    def table_html(self) -> str:
        return self.list_plugins()._repr_html_()


default_registry = PluginRegistry()
