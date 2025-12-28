"""Test fixture utilities for loading granule and collection data.

This module provides helper functions for loading test fixtures from the
organized fixture directories:

- `granules/` - UMM granule JSON fixtures
- `collections/` - UMM collection JSON fixtures (future)
- `vcr_cassettes/` - VCR HTTP recordings for workflow tests

Usage:
    from tests.unit.fixtures import load_granule_fixture

    def test_stac_conversion():
        fixture = load_granule_fixture("HLSL30_umm")
        granule = DataGranule(fixture, cloud_hosted=True)
        stac = granule.to_stac()
        assert "B02" in stac["assets"]
"""

import json
from pathlib import Path
from typing import Optional

FIXTURES_DIR = Path(__file__).parent


def load_granule_fixture(name: str) -> dict:
    """Load a UMM granule fixture by name.

    Args:
        name: Fixture name, with or without file extension.
              Examples: "HLSL30_umm", "HLSL30_umm.json", "atl03"

    Returns:
        The parsed JSON fixture as a dictionary.

    Raises:
        FileNotFoundError: If the fixture doesn't exist.

    Examples:
        >>> fixture = load_granule_fixture("HLSL30_umm")
        >>> fixture["meta"]["concept-id"]
        'G2926016408-LPCLOUD'
    """
    path = _resolve_fixture_path(FIXTURES_DIR / "granules", name)
    with open(path) as f:
        return json.load(f)


def load_collection_fixture(name: str) -> dict:
    """Load a UMM collection fixture by name.

    Args:
        name: Fixture name, with or without file extension.

    Returns:
        The parsed JSON fixture as a dictionary.

    Raises:
        FileNotFoundError: If the fixture doesn't exist.
    """
    path = _resolve_fixture_path(FIXTURES_DIR / "collections", name)
    with open(path) as f:
        return json.load(f)


def _resolve_fixture_path(directory: Path, name: str) -> Path:
    """Resolve a fixture name to a full path.

    Tries multiple naming patterns:
    1. Exact name with .json extension
    2. Name without extension + .json
    3. Name + _umm.json suffix
    """
    # If name already has .json extension
    if name.endswith(".json"):
        path = directory / name
        if path.exists():
            return path
        raise FileNotFoundError(f"Fixture not found: {path}")

    # Try exact name + .json
    path = directory / f"{name}.json"
    if path.exists():
        return path

    # Try name without _umm suffix + _umm.json
    if not name.endswith("_umm"):
        umm_path = directory / f"{name}_umm.json"
        if umm_path.exists():
            return umm_path

    raise FileNotFoundError(
        f"Fixture not found: tried {path} and variations in {directory}"
    )


def list_granule_fixtures() -> list[str]:
    """List all available granule fixture names."""
    granules_dir = FIXTURES_DIR / "granules"
    if not granules_dir.exists():
        return []
    return [p.stem for p in granules_dir.glob("*.json")]


def list_collection_fixtures() -> list[str]:
    """List all available collection fixture names."""
    collections_dir = FIXTURES_DIR / "collections"
    if not collections_dir.exists():
        return []
    return [p.stem for p in collections_dir.glob("*.json")]


def get_fixtures_dir() -> Path:
    """Return the fixtures directory path."""
    return FIXTURES_DIR


def get_vcr_cassettes_dir(module_name: Optional[str] = None) -> Path:
    """Return the VCR cassettes directory path.

    Args:
        module_name: Optional test module name for module-specific cassettes.
                    Example: "test_results"

    Returns:
        Path to the cassettes directory.
    """
    cassettes_dir = FIXTURES_DIR / "vcr_cassettes"
    if module_name:
        return cassettes_dir / module_name
    return cassettes_dir
