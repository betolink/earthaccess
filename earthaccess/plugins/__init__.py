"""earthaccess plugins.

``ea.plugins`` is a namespace: attribute access loads the named plugin
(``ea.plugins.harmony``), ``get``/``available`` are programmatic lookups, and
``repr(ea.plugins)`` prints the plugin table. The contract classes are
re-exported here so plugin authors can ``from earthaccess.plugins import
PluginBase``.
"""

from __future__ import annotations

import sys
import types

# Re-exported contract. Plugin authors subclass PluginBase and (for a remote
# backend) Job; everyone raises the typed errors below.
from earthaccess_plugin_spec import (
    CONTRACT_VERSION,
    Asset,
    AuthHandle,
    Capabilities,
    Capability,
    EarthaccessPluginError,
    GranuleResultSet,
    Job,
    JobCanceledError,
    JobError,
    JobFailedError,
    JobPausedError,
    JobStatus,
    MissingResultsError,
    NotSupportedError,
    PluginBase,
    PluginContext,
    PluginLoadError,
    PluginNotFoundError,
    ResultSet,
    UnsupportedCollectionError,
    UnsupportedOutputError,
    accepting_plugins,
    as_result_set,
    default_context,
    list_services,
)

from .registry import PluginInfo, PluginList, PluginRegistry, default_registry

__all__ = [
    "CONTRACT_VERSION",
    "Asset",
    "AuthHandle",
    "Capabilities",
    "Capability",
    "EarthaccessPluginError",
    "GranuleResultSet",
    "Job",
    "JobCanceledError",
    "JobError",
    "JobFailedError",
    "JobPausedError",
    "JobStatus",
    "MissingResultsError",
    "NotSupportedError",
    "PluginBase",
    "PluginContext",
    "PluginInfo",
    "PluginList",
    "PluginLoadError",
    "PluginNotFoundError",
    "PluginRegistry",
    "ResultSet",
    "UnsupportedCollectionError",
    "UnsupportedOutputError",
    "accepting_plugins",
    "as_result_set",
    "available",
    "default_context",
    "get",
    "list_plugins",
    "list_services",
]


def get(name: str) -> PluginBase:
    """Return a loaded plugin instance by name."""
    return default_registry[name]


def available() -> list[str]:
    """Return the names of known and installed plugins."""
    return default_registry.names()


def list_plugins(details: bool = False) -> PluginList:
    """Return a table-friendly list of known and installed plugins."""
    return default_registry.list_plugins(details=details)


class _PluginsModule(types.ModuleType):
    """Module whose attribute access falls through to the plugin registry."""

    def __getattr__(self, name: str) -> PluginBase:
        # Runs only when normal lookup fails. Dunders and privates stay normal
        # so the import machinery keeps working.
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            return default_registry[name]
        except PluginNotFoundError as exc:
            raise AttributeError(str(exc)) from exc

    def __dir__(self) -> list[str]:
        return sorted(set(super().__dir__()) | set(self.names()))

    def names(self) -> list[str]:
        return default_registry.names()

    def __repr__(self) -> str:
        return default_registry.table()

    def _repr_html_(self) -> str:
        return default_registry.table_html()


sys.modules[__name__].__class__ = _PluginsModule
