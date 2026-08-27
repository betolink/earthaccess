"""Pytest configuration and shared fixtures for unit tests."""

import contextlib
import gzip
import json
import os.path
from pathlib import Path

import pytest
from vcr.cassette import CassetteNotFoundError
from vcr.serialize import deserialize, serialize

# =============================================================================
# VCR Configuration for pytest-recording
# =============================================================================

REDACTED_STRING = "REDACTED"
MAX_ITEMS_PER_RESPONSE = 20  # Truncate CMR responses to reduce cassette size


class CompressedPersister:
    """VCR persister that transparently handles .yaml.gz compressed cassettes.

    This persister automatically compresses cassettes using gzip when saving
    and decompresses when loading. Files are stored with .yaml.gz extension.

    For backward compatibility, it falls back to reading .yaml files if
    the .yaml.gz version doesn't exist.

    Benefits:
    - ~13x size reduction for typical CMR response cassettes
    - Transparent to test code - no changes needed in tests
    - Works with existing VCR/pytest-recording infrastructure
    """

    @classmethod
    def load_cassette(cls, cassette_path, serializer):
        """Load cassette from .yaml.gz compressed file, falling back to .yaml.

        Args:
            cassette_path: Path to cassette (without .gz extension)
            serializer: VCR serializer to use for deserialization

        Returns:
            Deserialized cassette data

        Raises:
            CassetteNotFoundError: If neither compressed nor uncompressed cassette exists
        """
        cassette_path = Path(cassette_path)
        gz_path = cassette_path.with_suffix(".yaml.gz")

        # Try compressed first
        if gz_path.is_file():
            with gzip.open(gz_path, "rt", encoding="utf-8") as f:
                data = f.read()
            return deserialize(data, serializer)

        # Fall back to uncompressed .yaml
        yaml_path = cassette_path.with_suffix(".yaml")
        if yaml_path.is_file():
            with open(yaml_path, "r", encoding="utf-8") as f:
                data = f.read()
            return deserialize(data, serializer)

        raise CassetteNotFoundError(f"Cassette not found: {gz_path} or {yaml_path}")

    @staticmethod
    def save_cassette(cassette_path, cassette_dict, serializer):
        """Save cassette as .yaml.gz compressed file.

        Args:
            cassette_path: Path to cassette (without .gz extension)
            cassette_dict: Cassette data to serialize
            serializer: VCR serializer to use for serialization
        """
        cassette_path = Path(cassette_path)
        gz_path = cassette_path.with_suffix(".yaml.gz")

        # Ensure parent directory exists
        gz_path.parent.mkdir(parents=True, exist_ok=True)

        data = serialize(cassette_dict, serializer)
        with gzip.open(gz_path, "wt", encoding="utf-8") as f:
            f.write(data)


def truncate_response_items(response):
    """Truncate large CMR response bodies to reduce cassette size.

    CMR responses contain 'items' arrays that can have thousands of entries.
    For testing pagination and parsing, we only need a small sample.

    Args:
        response: VCR response dict with 'body' containing 'string'

    Returns:
        Modified response with truncated items array
    """
    body = response["body"]["string"]

    # Handle bytes vs string
    if isinstance(body, bytes):
        try:
            body = body.decode("utf-8")
        except UnicodeDecodeError:
            return response

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return response

    # Truncate 'items' array if present (CMR response format)
    if isinstance(payload, dict) and "items" in payload:
        if len(payload["items"]) > MAX_ITEMS_PER_RESPONSE:
            payload["items"] = payload["items"][:MAX_ITEMS_PER_RESPONSE]
            response["body"]["string"] = json.dumps(payload).encode("utf-8")

    return response


def chain_filters(*filters):
    """Chain multiple VCR response filters together.

    Args:
        *filters: Response filter functions to chain

    Returns:
        Combined filter function that applies all filters in order
    """

    def combined(response):
        for f in filters:
            response = f(response)
        return response

    return combined


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
        body = response["body"]["string"]

        # Handle bytes
        if isinstance(body, bytes):
            body = body.decode("utf8")

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


def pytest_recording_configure(config, vcr):
    """Register custom compressed persister with VCR.

    This hook is called by pytest-recording to allow VCR customization.
    We use it to register our gzip-compressed cassette persister.
    """
    vcr.register_persister(CompressedPersister)


@pytest.fixture(scope="module")
def vcr_config():
    """VCR configuration for pytest-recording.

    This fixture configures VCR to:
    - Store cassettes as compressed .yaml.gz files
    - Truncate response items to reduce cassette size
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
        "before_record_response": chain_filters(
            truncate_response_items,
            redact_key_values(
                [
                    "access_token",
                    "uid",
                    "first_name",
                    "last_name",
                    "email_address",
                    "nams_auid",
                ]
            ),
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
