"""Testing helpers for earthaccess plugins.

Re-exports the plugin conformance suite so plugin authors can write:

    from earthaccess.testing import run_conformance
"""

from __future__ import annotations

from earthaccess_plugin_spec.testing import (
    FakeAuth,
    Fixtures,
    fake_context,
    run_conformance,
    sample_result_set,
)

__all__ = [
    "FakeAuth",
    "Fixtures",
    "fake_context",
    "run_conformance",
    "sample_result_set",
]
