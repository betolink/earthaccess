import contextlib
import json
import os
import pathlib
import shutil
import socket
import subprocess
import tempfile
import time

# Load local secrets (e.g. EARTHDATA_TOKEN) from a gitignored .env, if present.
# Real environment variables take precedence over values from .env.
with contextlib.suppress(ImportError):
    from dotenv import load_dotenv

    load_dotenv(pathlib.Path(__file__).parents[2] / ".env")

import earthaccess
import pytest

# =============================================================================
# VCR Configuration for pytest-recording
# =============================================================================

REDACTED_STRING = "REDACTED"


def redact_key_values(keys_to_redact):
    """Create a response filter that redacts specified keys."""

    def redact(payload):
        for key in keys_to_redact:
            if key in payload:
                payload[key] = REDACTED_STRING
        return payload

    def before_record_response(response):
        body = response["body"]["string"].decode("utf8")

        with contextlib.suppress(json.JSONDecodeError):
            payload = json.loads(body)
            redacted_payload = (
                list(map(redact, payload))
                if isinstance(payload, list)
                else redact(payload)
            )
            response["body"]["string"] = json.dumps(redacted_payload).encode()

        return response

    return before_record_response


@pytest.fixture(scope="module")
def vcr_config():
    """VCR configuration for pytest-recording in integration tests."""
    return {
        "decode_compressed_response": True,
        "filter_headers": [
            "Accept-Encoding",
            "Authorization",
            "Cookie",
            "Set-Cookie",
            "User-Agent",
        ],
        "filter_post_data_parameters": ["access_token"],
        "before_record_response": redact_key_values(
            ["access_token", "uid", "first_name", "last_name", "email_address"]
        ),
    }


@pytest.fixture(scope="module")
def vcr_cassette_dir(request):
    """Return the cassette directory for the current test module."""
    module_name = pathlib.Path(request.fspath).stem
    return str(
        pathlib.Path(__file__).parent / "fixtures" / "vcr_cassettes" / module_name
    )


# =============================================================================
# Auth Fixtures
# =============================================================================


@pytest.fixture
def mock_missing_netrc(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch):
    netrc_path = tmp_path / ".netrc"
    monkeypatch.setenv("NETRC", str(netrc_path))
    monkeypatch.delenv("EARTHDATA_USERNAME")
    monkeypatch.delenv("EARTHDATA_PASSWORD")
    # Currently, due to there being only a single, global, module-level auth
    # value, tests using different auth strategies interfere with each other,
    # so here we are monkeypatching a new, unauthenticated Auth object.
    auth = earthaccess.Auth()
    monkeypatch.setattr(earthaccess, "_auth", auth)
    monkeypatch.setattr(earthaccess, "__auth__", auth)


@pytest.fixture  # pyright: ignore[reportCallIssue]
def mock_netrc(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch):
    netrc = tmp_path / ".netrc"
    monkeypatch.setenv("NETRC", str(netrc))

    username = os.environ["EARTHDATA_USERNAME"]
    password = os.environ["EARTHDATA_PASSWORD"]

    netrc.write_text(
        f"machine urs.earthdata.nasa.gov login {username} password {password}\n",
    )
    netrc.chmod(0o600)


# =============================================================================
# Proxy Worker Fixtures
# =============================================================================


PROXY_WORKER_DIR = pathlib.Path(__file__).parent / "proxy_worker"


def _find_free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_port(host: str, port: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


@pytest.fixture(scope="session")  # pyright: ignore[reportCallIssue]
def proxy_worker_url():
    """Run the local Cloudflare Worker proxy via `wrangler dev`.

    Skips if Node.js (`npx`) is unavailable or if the worker fails to start
    (e.g., no network access to download `wrangler`).
    """
    npx = shutil.which("npx")
    if npx is None:
        pytest.skip("Node.js/npx is required to run the proxy worker")

    port = _find_free_port()
    log = tempfile.NamedTemporaryFile(
        mode="w+", prefix="wrangler-", suffix=".log", delete=False
    )
    env = {**os.environ, "WRANGLER_SEND_METRICS": "false", "CI": "1"}
    process = subprocess.Popen(  # noqa: S603
        [npx, "--yes", "wrangler@4", "dev", "--port", str(port), "--ip", "127.0.0.1"],
        cwd=PROXY_WORKER_DIR,
        stdout=log,
        stderr=subprocess.STDOUT,
        env=env,
    )

    try:
        if not _wait_for_port("127.0.0.1", port, timeout=180):
            process.terminate()
            log.flush()
            log.seek(0)
            pytest.skip(f"`wrangler dev` failed to start:\n{log.read()[-2000:]}")
        yield f"http://127.0.0.1:{port}"
    finally:
        with contextlib.suppress(Exception):
            process.terminate()
            process.wait(timeout=10)
        if process.poll() is None:  # pragma: no cover - defensive
            with contextlib.suppress(Exception):
                process.kill()
        log.close()
