"""Deliver credentials to the workers of a Dask cluster.

Earthaccess never puts credentials in plugin payloads or task graphs. When the
compute runs on a remote cluster, the worker environment must instead carry the
credentials (see the ``environment`` login strategy). This module is the
explicit, opt-in way to set them:

```python
import earthaccess as ea

ea.login()
ea.distribute_credentials(
    {"AWS_ACCESS_KEY_ID": ..., "AWS_SECRET_ACCESS_KEY": ..., "AWS_SESSION_TOKEN": ...},
    cluster=cluster,          # a Coiled cluster, or a distributed Client
)
```

Delivery is **private by default**: the only channel that does not put the
values into the scheduler's task stream is Coiled's
``cluster.send_private_envs``. Without it, the function refuses to proceed
unless the caller passes ``private=False``, which sends the values through
``client.run`` (they are then serialized to the scheduler and every worker).
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from typing import Any

from earthaccess.exceptions import CredentialDistributionError

__all__ = ["distribute_credentials"]


def distribute_credentials(
    env: Mapping[str, str] | None = None,
    *,
    token: str | None = None,
    scheduler: str | None = None,
    client: Any = None,
    cluster: Any = None,
    private: bool | None = None,
) -> dict[str, Any]:
    """Set environment variables on the workers of a Dask cluster.

    Parameters:
        env: Extra variables to set on every worker, for example AWS
            credentials with write access to the target bucket
            (``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``,
            ``AWS_SESSION_TOKEN``).
        token: EDL token to expose as ``EARTHDATA_TOKEN``. Defaults to the
            current login token, if any.
        scheduler: Scheduler address (``tcp://host:8786``, ``inproc://``); a
            ``distributed.Client`` is created for it.
        client: An existing ``distributed.Client``. Its cluster is used when it
            supports a private channel.
        cluster: An existing cluster (Coiled, ``LocalCluster``, ...). A client
            is created when one is needed for delivery.
        private: ``None`` (default) uses a private channel when one exists and
            otherwise raises; ``True`` requires a private channel;
            ``False`` explicitly allows sending the values through
            ``client.run``.

    Returns:
        A JSON-friendly summary with ``transport`` (``"coiled"`` or
        ``"dask"``), the variable ``keys`` (never values), the scheduler
        address, and the worker count.

    Raises:
        CredentialDistributionError: No private channel is available and
            ``private`` is not ``False``.
        TypeError: A key or value in ``env`` is not a string.
        ValueError: Nothing to distribute, no client/scheduler was supplied,
            or a dashboard URL was passed instead of a scheduler address.
    """
    payload = _payload(env, token)

    private_channel = _private_channel(client=client, cluster=cluster)
    if private is not False and private_channel is not None:
        private_channel.send_private_envs(payload)
        return _summary("coiled", payload, client=client, cluster=cluster)

    if private is None:
        msg = (
            "no private credential channel is available for this cluster. "
            "Use a Coiled cluster (cluster.send_private_envs) or pass "
            "private=False to send the values through the scheduler "
            "(client.run), where they are visible to the scheduler and workers."
        )
        raise CredentialDistributionError(msg)
    if private is True:
        msg = (
            "private=True requires a cluster with send_private_envs "
            "(for example a Coiled cluster)"
        )
        raise CredentialDistributionError(msg)

    resolved = _resolve_client(client=client, cluster=cluster, scheduler=scheduler)
    resolved.run(_apply_env, payload)
    return _summary("dask", payload, client=resolved, cluster=cluster)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _apply_env(env: dict[str, str]) -> dict[str, bool]:
    """Worker-side: set the variables. Returns set/not-set, never values."""
    os.environ.update(env)
    return {key: True for key in env}


def _payload(
    env: Mapping[str, str] | None,
    token: str | None,
) -> dict[str, str]:
    payload: dict[str, str] = {}
    for key, value in (env or {}).items():
        if not isinstance(key, str) or not isinstance(value, str):
            msg = (
                "distribute_credentials env keys and values must be strings; "
                f"got {type(value).__name__} for {key!r}"
            )
            raise TypeError(msg)
        payload[key] = value

    resolved_token = token if token is not None else _current_token()
    if resolved_token and "EARTHDATA_TOKEN" not in payload:
        payload["EARTHDATA_TOKEN"] = resolved_token

    if not payload:
        msg = (
            "nothing to distribute: pass env=..., token=..., or log in first "
            "(the login token is sent as EARTHDATA_TOKEN)"
        )
        raise ValueError(msg)
    return payload


def _current_token() -> str | None:
    import earthaccess

    auth = getattr(earthaccess, "__auth__", None)
    token = getattr(auth, "token", None)
    if isinstance(token, dict):
        return token.get("access_token")
    return None


def _private_channel(*, client: Any, cluster: Any) -> Any | None:
    """The cluster object offering ``send_private_envs``, if any."""
    for candidate in (cluster, getattr(client, "cluster", None)):
        if callable(getattr(candidate, "send_private_envs", None)):
            return candidate
    return None


def _resolve_client(
    *,
    client: Any,
    cluster: Any,
    scheduler: str | None,
) -> Any:
    if client is not None:
        if cluster is not None:
            msg = "pass client= or cluster=, not both"
            raise ValueError(msg)
        return client
    if cluster is not None:
        return _distributed().Client(cluster)
    if scheduler is not None:
        if scheduler.startswith(("http://", "https://")):
            msg = (
                f"{scheduler!r} looks like a dashboard URL; pass the scheduler "
                "address instead (for example tcp://host:8786)"
            )
            raise ValueError(msg)
        return _distributed().Client(scheduler)
    try:
        return _distributed().default_client()
    except Exception as exc:
        msg = (
            "no Dask client found: pass client=, cluster=, or scheduler=, or "
            "create a Client first"
        )
        raise ValueError(msg) from exc


def _distributed() -> Any:
    try:
        import distributed
    except ImportError as exc:  # pragma: no cover - distributed is optional
        msg = "distribute_credentials requires `pip install distributed`"
        raise ImportError(msg) from exc
    return distributed


def _summary(
    transport: str,
    payload: Mapping[str, str],
    *,
    client: Any,
    cluster: Any,
) -> dict[str, Any]:
    workers = _worker_count(client=client, cluster=cluster)
    scheduler = getattr(getattr(client, "scheduler", None), "address", None)
    if scheduler is None:
        scheduler = getattr(cluster, "scheduler_address", None)
    return {
        "transport": transport,
        "keys": sorted(payload),
        "scheduler": scheduler,
        "workers": workers,
    }


def _worker_count(*, client: Any, cluster: Any) -> int | None:
    info: Callable[[], Any] | None = getattr(client, "scheduler_info", None)
    if info is not None:
        try:
            return len(info().get("workers", {}))
        except Exception:  # noqa: BLE001 - summary must not fail a delivery
            return None
    count = getattr(cluster, "n_workers", None)
    return int(count) if isinstance(count, int) else None
