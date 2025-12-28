"""Pytest configuration and shared fixtures for unit tests."""

import contextlib
import json
import os.path
from pathlib import Path

import pytest

# =============================================================================
# VCR Configuration for pytest-recording
# =============================================================================

REDACTED_STRING = "REDACTED"


def redact_login_request(request):
    """Redact sensitive user information from login requests."""
    if "/api/users/" in request.path and "/api/users/tokens" not in request.path:
        _, user_name = os.path.split(request.path)
        request.uri = request.uri.replace(user_name, REDACTED_STRING)
    return request


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
    """VCR configuration for pytest-recording.

    This fixture configures VCR to:
    - Store cassettes in module-specific subdirectories
    - Redact sensitive authentication data
    - Match requests by method, scheme, host, path, query, and headers
    - Decode compressed responses for easier inspection
    """
    return {
        "decode_compressed_response": True,
        # Match on headers to test search-after functionality
        "match_on": [
            "method",
            "scheme",
            "host",
            "port",
            "path",
            "query",
            "headers",
        ],
        "filter_headers": [
            "Accept-Encoding",
            "Authorization",
            "Cookie",
            "Set-Cookie",
            "User-Agent",
        ],
        "filter_query_parameters": [
            ("client_id", REDACTED_STRING),
        ],
        "before_record_response": redact_key_values(
            [
                "access_token",
                "uid",
                "first_name",
                "last_name",
                "email_address",
                "nams_auid",
            ]
        ),
        "before_record_request": redact_login_request,
    }


@pytest.fixture(scope="module")
def vcr_cassette_dir(request):
    """Return the cassette directory for the current test module.

    Cassettes are organized by test module name:
    - tests/unit/test_results.py -> tests/unit/fixtures/vcr_cassettes/test_results/
    - tests/unit/test_services.py -> tests/unit/fixtures/vcr_cassettes/test_services/
    """
    # Get the test module name without .py extension
    module_name = Path(request.fspath).stem
    return str(Path(__file__).parent / "fixtures" / "vcr_cassettes" / module_name)


# =============================================================================
# Fixture Utilities
# =============================================================================


@pytest.fixture
def fixtures_dir():
    """Return the path to the fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def granule_fixtures_dir(fixtures_dir):
    """Return the path to the granule fixtures directory."""
    return fixtures_dir / "granules"


@pytest.fixture
def collection_fixtures_dir(fixtures_dir):
    """Return the path to the collection fixtures directory."""
    return fixtures_dir / "collections"
