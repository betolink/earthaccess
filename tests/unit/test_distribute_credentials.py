"""Top-level tests for ``earthaccess.distribute_credentials``.

No Dask install is required: the function talks to whatever client/cluster
objects it is given, and the tests use small fakes that record what they see.
"""

from __future__ import annotations

import os
import sys
import types
from typing import Any

import earthaccess
import pytest
from earthaccess.auth.distribute import distribute_credentials
from earthaccess.exceptions import CredentialDistributionError


class FakeClient:
    """Stand-in for distributed.Client: records run() and reports workers."""

    def __init__(self, cluster: Any = None, workers: int = 2) -> None:
        self.cluster = cluster
        self.scheduler = types.SimpleNamespace(address="tcp://scheduler:8786")
        self._workers = workers
        self.runs: list[tuple[Any, tuple[Any, ...]]] = []

    def scheduler_info(self) -> dict[str, Any]:
        return {"workers": {f"w{i}": {} for i in range(self._workers)}}

    def run(self, func: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
        result = func(*args, **kwargs)
        self.runs.append((func, args))
        return {f"w{i}": result for i in range(self._workers)}


class FakeCoiledCluster:
    """Stand-in for a Coiled cluster: the only object with a private channel."""

    def __init__(self) -> None:
        self.sent: dict[str, str] | None = None

    def send_private_envs(self, env: dict[str, str]) -> None:
        self.sent = dict(env)


@pytest.fixture
def logged_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        earthaccess,
        "__auth__",
        types.SimpleNamespace(token={"access_token": "edl-token"}),
        raising=False,
    )


def test_coiled_cluster_uses_send_private_envs(logged_in: None) -> None:
    cluster = FakeCoiledCluster()

    result = distribute_credentials({"AWS_ACCESS_KEY_ID": "AKIA"}, cluster=cluster)

    assert cluster.sent == {
        "AWS_ACCESS_KEY_ID": "AKIA",
        "EARTHDATA_TOKEN": "edl-token",
    }
    assert result["transport"] == "coiled"
    assert result["keys"] == ["AWS_ACCESS_KEY_ID", "EARTHDATA_TOKEN"]
    # The result is safe to print: keys only, never values.
    assert "AKIA" not in repr(result)
    assert "edl-token" not in repr(result)


def test_client_whose_cluster_is_coiled_uses_the_private_channel() -> None:
    cluster = FakeCoiledCluster()
    client = FakeClient(cluster=cluster)

    result = distribute_credentials({"A": "1"}, client=client, token="tok")

    assert cluster.sent == {"A": "1", "EARTHDATA_TOKEN": "tok"}
    assert result["transport"] == "coiled"
    assert client.runs == []


def test_plain_dask_requires_an_explicit_opt_in() -> None:
    client = FakeClient()

    with pytest.raises(CredentialDistributionError, match="send_private_envs"):
        distribute_credentials({"A": "1"}, client=client, token="tok")

    assert client.runs == []


def test_private_false_sends_env_through_client_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("EARTHDATA_TOKEN", raising=False)
    client = FakeClient()

    result = distribute_credentials(
        {"AWS_ACCESS_KEY_ID": "AKIA"}, client=client, token="tok", private=False
    )

    assert len(client.runs) == 1
    assert os.environ["AWS_ACCESS_KEY_ID"] == "AKIA"
    assert os.environ["EARTHDATA_TOKEN"] == "tok"
    assert result["transport"] == "dask"
    assert result["workers"] == 2
    assert "AKIA" not in repr(result)


def test_scheduler_string_builds_a_client(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()
    fake_distributed = types.ModuleType("distributed")
    fake_distributed.Client = lambda address: client  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "distributed", fake_distributed)

    result = distribute_credentials(
        {"A": "1"}, scheduler="tcp://host:8786", token="tok", private=False
    )

    assert result["transport"] == "dask"
    assert len(client.runs) == 1


def test_dashboard_url_is_rejected() -> None:
    with pytest.raises(ValueError, match="dashboard"):
        distribute_credentials(
            {"A": "1"},
            scheduler="http://host:8787",
            token="tok",
            private=False,
        )


def test_non_string_values_are_rejected() -> None:
    with pytest.raises(TypeError, match="strings"):
        distribute_credentials({"A": 1}, client=FakeClient(), token="tok")  # type: ignore[dict-item]


def test_nothing_to_distribute_is_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(earthaccess, "__auth__", None, raising=False)

    with pytest.raises(ValueError, match="nothing to distribute"):
        distribute_credentials(client=FakeClient())
