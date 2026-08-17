"""Unit tests for earthaccess.write_virtual (icechunk create/append).

All icechunk and virtualizarr I/O is mocked so the suite has no external
dependencies.  The tests are behavioural: they assert which store the dataset
is written to, that containers are configured on first write, and that appends
open the existing store.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class FakeManifestArray:
    """Stand-in for virtualizarr's ManifestArray used by _derive_vccs_from_vds."""

    def __init__(self, paths):
        self.manifest = {p: [] for p in paths}


def _vds_with_refs(*paths):
    vds = MagicMock()
    vds.vz = MagicMock()
    vds.variables = {
        "sst": MagicMock(data=FakeManifestArray(list(paths))),
        "time": MagicMock(data=FakeManifestArray(list(paths))),
    }
    return vds


# ---------------------------------------------------------------------------
# _derive_vccs_from_vds
# ---------------------------------------------------------------------------


def test_derive_vccs_from_vds_collects_prefixes():
    import icechunk
    from earthaccess.virtual.core import _derive_vccs_from_vds

    vds = _vds_with_refs(
        "https://archive.podaac.earthdata.nasa.gov/data/x.nc",
        "s3://podaac-bucket/data/y.nc",
        "https://archive.podaac.earthdata.nasa.gov/data/t.nc",
    )

    with patch("virtualizarr.manifests.ManifestArray", FakeManifestArray):
        vccs = _derive_vccs_from_vds(vds)

    assert set(vccs) == {
        "https://archive.podaac.earthdata.nasa.gov/",
        "s3://podaac-bucket/",
    }
    assert isinstance(vccs["s3://podaac-bucket/"], icechunk.ObjectStoreConfig.S3)
    assert isinstance(
        vccs["https://archive.podaac.earthdata.nasa.gov/"],
        icechunk.ObjectStoreConfig.Http,
    )


def test_derive_vccs_from_vds_raises_without_manifest_arrays():
    from earthaccess.virtual.core import _derive_vccs_from_vds

    vds = MagicMock()
    vds.variables = {"sst": MagicMock(data=object())}

    with (
        patch("virtualizarr.manifests.ManifestArray", FakeManifestArray),
        pytest.raises(ValueError, match="ManifestArray"),
    ):
        _derive_vccs_from_vds(vds)


# ---------------------------------------------------------------------------
# write_virtual
# ---------------------------------------------------------------------------


def test_write_virtual_creates_local_store_with_containers():
    from earthaccess.virtual.core import write_virtual

    vds = _vds_with_refs("https://archive.podaac.earthdata.nasa.gov/data/x.nc")
    session = MagicMock()
    session.store = MagicMock()
    repo_obj = MagicMock()
    repo_obj.writable_session.return_value = session
    repo_create = MagicMock(return_value=repo_obj)

    with (
        patch(
            "earthaccess.virtual.core._derive_vccs_from_vds",
            return_value={"https://archive.podaac.earthdata.nasa.gov/": "STORE"},
        ),
        patch("icechunk.local_filesystem_storage", return_value="LOCAL") as mock_local,
        patch("icechunk.Repository.exists", return_value=False),
        patch("icechunk.Repository.create", repo_create),
        patch("icechunk.RepositoryConfig.default") as mock_config_default,
        patch("icechunk.VirtualChunkContainer") as mock_vc,
    ):
        result = write_virtual(vds, "/tmp/sst.icechunk")

    mock_local.assert_called_once_with("/tmp/sst.icechunk")
    repo_create.assert_called_once()
    config = mock_config_default.return_value
    mock_vc.assert_called_once_with(
        "https://archive.podaac.earthdata.nasa.gov/",
        "STORE",
    )
    config.set_virtual_chunk_container.assert_called_once_with(
        mock_vc.return_value,
    )
    repo_obj.save_config.assert_called_once()
    vds.vz.to_icechunk.assert_called_once_with(session.store, append_dim=None)
    session.commit.assert_called_once()
    assert result is vds


def test_write_virtual_appends_to_existing_store():
    from earthaccess.virtual.core import write_virtual

    vds = _vds_with_refs("https://archive.podaac.earthdata.nasa.gov/data/x.nc")
    session = MagicMock()
    session.store = MagicMock()
    repo_obj = MagicMock()
    repo_obj.writable_session.return_value = session

    with (
        patch("earthaccess.virtual.core._derive_vccs_from_vds", return_value={}),
        patch("icechunk.local_filesystem_storage", return_value="LOCAL"),
        patch("icechunk.Repository.exists", return_value=True),
        patch("icechunk.Repository.open", return_value=repo_obj) as mock_open,
    ):
        result = write_virtual(vds, "/tmp/sst.icechunk", append_dim="time")

    mock_open.assert_called_once()
    vds.vz.to_icechunk.assert_called_once_with(session.store, append_dim="time")
    session.commit.assert_called_once()
    assert result is vds


def test_write_virtual_s3_uses_storage_options():
    from earthaccess.virtual.core import write_virtual

    vds = _vds_with_refs("s3://podaac-bucket/data/x.nc")

    with (
        patch("earthaccess.virtual.core._derive_vccs_from_vds", return_value={}),
        patch("icechunk.s3_storage") as mock_s3,
        patch("icechunk.Repository.exists", return_value=True),
        patch("icechunk.Repository.open"),
    ):
        write_virtual(
            vds,
            "s3://mybucket/store",
            storage_options={"region": "us-east-1", "from_env": True},
        )

    mock_s3.assert_called_once_with(
        bucket="mybucket",
        prefix="store",
        region="us-east-1",
        from_env=True,
    )
