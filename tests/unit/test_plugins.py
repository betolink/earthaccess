"""Tests for the plugin registry, the ``ea.plugins`` namespace and dispatch."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import earthaccess
import pytest
from earthaccess import plugins as plugins_mod
from earthaccess.plugins import (
    Asset,
    Job,
    PluginBase,
    PluginLoadError,
    PluginNotFoundError,
    PluginRegistry,
    UnsupportedOutputError,
    registry as registry_mod,
)

# ---------------------------------------------------------------------------
# Real plugin classes and fake entry points
# ---------------------------------------------------------------------------


@dataclass
class _Options:
    value: int = 0


class FakePlugin(PluginBase):
    name = "fake"
    auth = "none"
    output = "dataframe"
    Options = _Options

    def _request(self, results: Any, options: _Options) -> Job:
        return self.job(lambda: options.value)


class WrongContractPlugin(FakePlugin):
    name = "wrong"
    contract_version = 99


def make_entry_point(
    name: str,
    cls: type = FakePlugin,
    *,
    version: str = "1.2.3",
    summary: str = "",
    load_error: Exception | None = None,
) -> SimpleNamespace:
    """A stand-in for importlib.metadata.EntryPoint."""
    calls = {"load": 0}

    def load() -> type:
        calls["load"] += 1
        if load_error is not None:
            raise load_error
        return cls

    dist = SimpleNamespace(
        name=f"{name}-dist",
        version=version,
        metadata={"Summary": summary} if summary else {},
    )
    return SimpleNamespace(name=name, dist=dist, load=load, calls=calls)


@pytest.fixture
def entry_point_source(monkeypatch: pytest.MonkeyPatch) -> list[SimpleNamespace]:
    """Patch entry-point discovery to serve a mutable list of fake entry points."""
    points: list[SimpleNamespace] = []
    monkeypatch.setattr(registry_mod.metadata, "entry_points", lambda *, group: points)
    return points


def make_registry(points: list[SimpleNamespace]) -> PluginRegistry:
    return PluginRegistry()


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def test_names_merge_installed_and_known(entry_point_source):
    entry_point_source.append(make_entry_point("fake"))
    registry = make_registry(entry_point_source)

    names = registry.names()

    assert names == sorted(names)
    assert {"fake", "harmony", "pydap", "sliderule"} <= set(names)


def test_list_plugins_reports_installed_and_missing(entry_point_source):
    entry_point_source.append(
        make_entry_point("fake", version="9.9.9", summary="Installed")
    )
    registry = make_registry(entry_point_source)

    rows = {row.name: row for row in registry.list_plugins()}

    assert rows["fake"].installed is True
    assert rows["fake"].version == "9.9.9"
    assert rows["fake"].summary == "Installed"
    # Known but not installed: still listed, summary from the built-in table.
    assert rows["harmony"].installed is False
    assert rows["harmony"].summary.startswith("Subset and transform")


def test_list_plugins_does_not_import_plugins(entry_point_source):
    point = make_entry_point("fake")
    entry_point_source.append(point)

    make_registry(entry_point_source).list_plugins()

    assert point.calls["load"] == 0


def test_list_plugins_details_loads_contract_metadata(entry_point_source):
    point = make_entry_point("fake")
    entry_point_source.append(point)

    row = make_registry(entry_point_source).list_plugins(details=True)[0]

    assert point.calls["load"] == 1
    assert row.contract_version == 1
    assert row.auth == "none"
    assert row.output == "dataframe"
    assert row.needs == frozenset()


def test_list_plugins_details_reports_load_error(entry_point_source):
    entry_point_source.append(
        make_entry_point("broken", load_error=ImportError("backend missing"))
    )

    row = make_registry(entry_point_source).list_plugins(details=True)[0]

    assert row.installed is True
    assert "backend missing" in (row.error or "")


def test_entry_points_are_discovered_once(entry_point_source, monkeypatch):
    calls = {"n": 0}

    def counting(*, group):
        calls["n"] += 1
        return entry_point_source

    monkeypatch.setattr(registry_mod.metadata, "entry_points", counting)
    registry = make_registry(entry_point_source)

    registry.names()
    registry.names()
    registry.list_plugins()

    assert calls["n"] == 1


def test_reserved_and_duplicate_names(entry_point_source):
    entry_point_source.extend(
        [
            make_entry_point("get"),
            make_entry_point("dup", cls=FakePlugin),
            make_entry_point("dup", cls=WrongContractPlugin),
        ]
    )
    registry = make_registry(entry_point_source)

    with pytest.warns(UserWarning, match="reserved"):
        with pytest.warns(UserWarning, match="provided twice"):
            names = registry.names()

    assert "get" not in names
    # First distribution wins.
    assert isinstance(registry["dup"], FakePlugin)


# ---------------------------------------------------------------------------
# Loading and errors
# ---------------------------------------------------------------------------


def test_getitem_loads_and_caches(entry_point_source):
    point = make_entry_point("fake")
    entry_point_source.append(point)
    registry = make_registry(entry_point_source)

    first = registry["fake"]
    second = registry["fake"]

    assert isinstance(first, FakePlugin)
    assert first is second
    assert point.calls["load"] == 1


def test_contract_mismatch_is_rejected(entry_point_source):
    entry_point_source.append(make_entry_point("wrong", cls=WrongContractPlugin))
    registry = make_registry(entry_point_source)

    with pytest.raises(PluginLoadError, match="contract 99"):
        registry["wrong"]


def test_load_failure_is_wrapped(entry_point_source):
    entry_point_source.append(
        make_entry_point("broken", load_error=ImportError("nope"))
    )
    registry = make_registry(entry_point_source)

    with pytest.raises(PluginLoadError) as excinfo:
        registry["broken"]

    assert isinstance(excinfo.value.__cause__, ImportError)


def test_missing_plugin_names_the_install(entry_point_source):
    registry = make_registry(entry_point_source)

    with pytest.raises(PluginNotFoundError, match=r"pip install earthaccess-harmony"):
        registry["harmony"]


# ---------------------------------------------------------------------------
# Namespace
# ---------------------------------------------------------------------------


@pytest.fixture
def isolated_registry(entry_point_source, monkeypatch):
    entry_point_source.append(make_entry_point("fake"))
    fresh = make_registry(entry_point_source)
    monkeypatch.setattr(plugins_mod, "default_registry", fresh)
    return fresh


def test_namespace_get_and_attribute(isolated_registry):
    assert isinstance(plugins_mod.get("fake"), FakePlugin)
    assert isinstance(plugins_mod.fake, FakePlugin)
    assert "fake" in plugins_mod.available()


def test_namespace_unknown_attribute_raises(isolated_registry):
    with pytest.raises(AttributeError, match="not installed"):
        _ = plugins_mod.does_not_exist


def test_namespace_dir_and_repr(isolated_registry):
    assert "fake" in dir(plugins_mod)
    table = repr(plugins_mod)
    assert "fake" in table
    assert {"name", "installed", "summary"} <= set(table.split())


def test_top_level_surface():
    assert "plugins" in earthaccess.__all__
    assert "list_plugins" in earthaccess.__all__
    assert callable(earthaccess.list_plugins)
    assert callable(earthaccess.testing.run_conformance)


# ---------------------------------------------------------------------------
# open() / download() dispatch
# ---------------------------------------------------------------------------


class FakeStore:
    def __init__(self) -> None:
        self.opened: Any = None
        self.got: Any = None

    def open(self, granules: Any, **kwargs: Any) -> list[str]:
        self.opened = granules
        return [f"file:{href}" for href in granules]

    def get(self, granules: Any, path: Any, provider: Any, threads: int, **kwargs: Any):
        self.got = granules
        return list(granules)


def assets_job(*hrefs: str) -> Job:
    return Job(
        lambda: [Asset(href=href, access="https") for href in hrefs],
        plugin="fake",
        output="assets",
    )


@pytest.fixture
def fake_store(monkeypatch: pytest.MonkeyPatch) -> FakeStore:
    store = FakeStore()
    monkeypatch.setattr(earthaccess, "_store", store)
    return store


def test_open_assets_job_reaches_store(fake_store):
    opened = earthaccess.open(assets_job("https://e.org/a.nc", "https://e.org/b.nc"))

    assert fake_store.opened == ["https://e.org/a.nc", "https://e.org/b.nc"]
    assert opened == ["file:https://e.org/a.nc", "file:https://e.org/b.nc"]


def test_open_non_asset_job_returns_object_without_store(monkeypatch):
    monkeypatch.setattr(earthaccess, "_store", None)
    dataset = object()
    job = Job(lambda: dataset, plugin="fake", output="xarray")

    assert earthaccess.open(job) is dataset


def test_download_assets_job_reaches_store(fake_store):
    earthaccess.download(assets_job("https://e.org/a.nc"), None)

    assert fake_store.got == ["https://e.org/a.nc"]


def test_download_non_asset_job_raises(monkeypatch):
    monkeypatch.setattr(earthaccess, "_store", None)
    job = Job(lambda: object(), plugin="fake", output="xarray")

    with pytest.raises(UnsupportedOutputError, match="xarray"):
        earthaccess.download(job, None)


def test_open_opendap_assets_returns_urls(fake_store):
    job = Job(
        lambda: [Asset(href="dap4://e.org/a?dap4.ce=/lat", access="opendap")],
        plugin="pydap",
        output="assets",
    )

    assert earthaccess.open(job) == ["dap4://e.org/a?dap4.ce=/lat"]
    assert fake_store.opened is None  # the store is not used for OPeNDAP


def test_download_opendap_assets_uses_pydap(monkeypatch, tmp_path):
    import sys
    import types

    calls: dict = {}

    def to_netcdf(urls, session=None, output_path=None, **kwargs):
        calls["urls"] = urls
        calls["output_path"] = output_path
        (tmp_path / "out.nc").write_text("x")

    fake_client = types.ModuleType("pydap.client")
    fake_client.to_netcdf = to_netcdf
    fake_pydap = types.ModuleType("pydap")
    fake_pydap.client = fake_client
    monkeypatch.setitem(sys.modules, "pydap", fake_pydap)
    monkeypatch.setitem(sys.modules, "pydap.client", fake_client)
    monkeypatch.setattr(
        earthaccess.api, "get_requests_https_session", lambda: "session"
    )

    job = Job(
        lambda: [Asset(href="dap4://e.org/a?dap4.ce=/lat", access="opendap")],
        plugin="pydap",
        output="assets",
    )
    paths = earthaccess.download(job, tmp_path)

    assert calls["urls"] == ["https://e.org/a?dap4.ce=/lat"]
    assert calls["output_path"] == str(tmp_path)
    assert paths == [tmp_path / "out.nc"]


def test_download_opendap_assets_rejects_cloud_target():
    from earthaccess.store.target import TargetLocation

    job = Job(
        lambda: [Asset(href="dap4://e.org/a", access="opendap")],
        plugin="pydap",
        output="assets",
    )

    with pytest.raises(UnsupportedOutputError, match="local path"):
        earthaccess.download(job, TargetLocation("s3://bucket/data"))
