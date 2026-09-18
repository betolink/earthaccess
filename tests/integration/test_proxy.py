"""Integration tests for routing Earthdata requests through a CORS proxy.

These tests start the Cloudflare Worker in ``proxy_worker/`` via ``wrangler``
and point :func:`earthaccess.set_proxy` at it.

Environment variables (a token takes precedence over username/password):
    EARTHDATA_TOKEN or EARTHDATA_USERNAME + EARTHDATA_PASSWORD:
        Credentials for the production (PROD) Earthdata Login.
    EARTHDATA_UAT_TOKEN or EARTHDATA_UAT_USERNAME + EARTHDATA_UAT_PASSWORD:
        Credentials for the UAT Earthdata Login.
"""

import logging
import os
from pathlib import Path

import earthaccess
import pytest
import requests

logger = logging.getLogger(__name__)

PROD_CREDENTIALS = {
    "token": "EARTHDATA_TOKEN",
    "username": "EARTHDATA_USERNAME",
    "password": "EARTHDATA_PASSWORD",
}
UAT_CREDENTIALS = {
    "token": "EARTHDATA_UAT_TOKEN",
    "username": "EARTHDATA_UAT_USERNAME",
    "password": "EARTHDATA_UAT_PASSWORD",
}

SYSTEM_PARAMS = [
    pytest.param(earthaccess.PROD, PROD_CREDENTIALS, "ATL06", id="PROD"),
    # UAT's ATL06 granules point at an internal, non-routable NSIDC host
    # (f5eil01.edn.ecs.nasa.gov), so use a small cloud-hosted UAT collection.
    pytest.param(
        earthaccess.UAT,
        UAT_CREDENTIALS,
        "REYNOLDS_NCDC_L4_SST_HIST_RECON_MONTHLY_V2",
        id="UAT",
    ),
]


@pytest.fixture
def proxy(proxy_worker_url, monkeypatch):
    """Configure earthaccess to use the local worker, resetting it afterwards."""
    # Use a fresh Auth/Store per test so switching systems re-authenticates.
    auth = earthaccess.Auth()
    monkeypatch.setattr(earthaccess, "_auth", auth)
    monkeypatch.setattr(earthaccess, "__auth__", auth)
    monkeypatch.setattr(earthaccess, "_store", None)

    earthaccess.set_proxy(proxy_worker_url)
    try:
        yield earthaccess.get_proxy()
    finally:
        earthaccess.set_proxy(None)


def _configure_credentials(
    credential_env: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    token = os.environ.get(credential_env["token"])
    username = os.environ.get(credential_env["username"])
    password = os.environ.get(credential_env["password"])

    if token:
        monkeypatch.setenv("EARTHDATA_TOKEN", token)
    elif username and password:
        monkeypatch.delenv("EARTHDATA_TOKEN", raising=False)
        monkeypatch.setenv("EARTHDATA_USERNAME", username)
        monkeypatch.setenv("EARTHDATA_PASSWORD", password)
    else:
        pytest.fail(
            f"Configure either {credential_env['token']} or "
            f"{credential_env['username']}/{credential_env['password']}"
        )


def test_proxy_worker_forwards_and_adds_cors(proxy_worker_url):
    """The worker forwards an allowed target and adds CORS headers."""
    url = f"{proxy_worker_url}/https://status.earthdata.nasa.gov/api/v1/statuses"
    response = requests.get(url, timeout=30)

    assert response.status_code == 200
    assert response.headers.get("Access-Control-Allow-Origin") == "*"
    assert "statuses" in response.json()


def test_proxy_worker_rejects_disallowed_host(proxy_worker_url):
    """The worker refuses to proxy hosts outside its allowlist."""
    response = requests.get(f"{proxy_worker_url}/https://example.com/", timeout=10)
    assert response.status_code == 403


def test_unauthenticated_search_through_proxy(proxy):
    """CMR search is routed through the proxy even without logging in."""
    granules = list(earthaccess.search_data(short_name="ATL06", count=1))
    assert len(granules) == 1


@pytest.mark.parametrize(("system", "credential_env", "short_name"), SYSTEM_PARAMS)
def test_login_search_download_through_proxy(
    system, credential_env, short_name, proxy, monkeypatch, tmp_path
):
    """login(), search(), download(), and open() work with the proxy enabled."""
    _configure_credentials(credential_env, monkeypatch)
    assert earthaccess.get_proxy() == proxy

    auth = earthaccess.login(strategy="environment", system=system)
    assert auth.authenticated
    assert auth.system.edl_hostname == system.edl_hostname

    # Hitting CMR requires the bearer token obtained via the proxied EDL login.
    assert auth.token is not None
    assert auth.token["access_token"]

    granules = list(earthaccess.search_data(short_name=short_name, count=1))
    assert len(granules) == 1

    files = earthaccess.download(granules, str(tmp_path))
    assert files
    path = Path(files[0])
    assert path.exists()
    assert path.stat().st_size > 0

    handles = earthaccess.open(granules)
    assert handles
    assert not isinstance(handles[0], Exception)
    assert handles[0].read(16)
