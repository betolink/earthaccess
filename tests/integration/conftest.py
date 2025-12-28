import contextlib
import json
import os
import pathlib

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
        f"machine urs.earthdata.nasa.gov login {username} password {password}\n"
    )
    netrc.chmod(0o600)
