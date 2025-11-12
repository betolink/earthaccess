"""
Tests for TargetLocation and target filesystem functionality.
"""

import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from earthaccess.target_filesystem import (
    TargetLocation,
    LocalFilesystem,
    FsspecFilesystem,
    TargetFilesystem,
)


class TestTargetLocation:
    """Test TargetLocation class functionality."""

    def test_local_path_detection(self):
        """Test that local paths are detected correctly."""
        loc = TargetLocation("/tmp/test")
        assert loc.path == "/tmp/test"
        assert loc.backend == "auto"
        assert isinstance(loc.get_filesystem(), LocalFilesystem)

    def test_path_object_detection(self):
        """Test that Path objects are handled correctly."""
        path = Path("/tmp/test")
        loc = TargetLocation(path)
        assert loc.path == str(path)
        assert isinstance(loc.get_filesystem(), LocalFilesystem)

    def test_cloud_detection_s3(self):
        """Test that S3 paths are detected correctly."""
        loc = TargetLocation("s3://my-bucket/path")
        assert loc.path == "s3://my-bucket/path"
        assert isinstance(loc.get_filesystem(), FsspecFilesystem)

    @patch("fsspec.filesystem")
    def test_cloud_detection_gs(self, mock_fs):
        """Test that Google Cloud Storage paths are detected correctly."""
        mock_fs_instance = MagicMock()
        mock_fs.return_value = mock_fs_instance

        loc = TargetLocation("gs://my-bucket/path")
        assert loc.path == "gs://my-bucket/path"
        assert isinstance(loc.get_filesystem(), FsspecFilesystem)

    @patch("fsspec.filesystem")
    def test_cloud_detection_azure(self, mock_fs):
        """Test that Azure paths are detected correctly."""
        mock_fs_instance = MagicMock()
        mock_fs.return_value = mock_fs_instance

        loc = TargetLocation("az://my-container/path")
        assert loc.path == "az://my-container/path"
        assert isinstance(loc.get_filesystem(), FsspecFilesystem)

    def test_explicit_backend_local(self):
        """Test explicit backend selection."""
        loc = TargetLocation("/tmp/test", backend="local")
        assert isinstance(loc.get_filesystem(), LocalFilesystem)

    def test_explicit_backend_fsspec(self):
        """Test explicit fsspec backend selection."""
        loc = TargetLocation("s3://bucket/path", backend="fsspec")
        assert isinstance(loc.get_filesystem(), FsspecFilesystem)

    def test_storage_options(self):
        """Test storage options are passed correctly."""
        storage_opts = {"key": "test_key", "secret": "test_secret"}
        loc = TargetLocation("s3://bucket/path", storage_options=storage_opts)
        fs = loc.get_filesystem()
        assert fs.storage_options == storage_opts

    def test_repr(self):
        """Test string representation."""
        loc = TargetLocation("/tmp/test")
        assert "TargetLocation" in repr(loc)
        assert "/tmp/test" in repr(loc)
        assert "auto" in repr(loc)


class TestLocalFilesystem:
    """Test LocalFilesystem class functionality."""

    def test_filesystem_operations(self):
        """Test basic filesystem operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fs = LocalFilesystem(tmpdir)

            # Test exists on empty directory
            assert fs.exists("") is True
            assert fs.exists("nonexistent") is False

            # Test mkdir
            fs.mkdir("test_dir")
            assert fs.exists("test_dir") is True

            # Test basename
            assert fs.basename("path/to/file.txt") == "file.txt"
            assert fs.basename("/absolute/path/file.txt") == "file.txt"

            # Test join
            assert fs.join("dir", "file.txt") == str(Path("dir", "file.txt"))
            assert fs.join("dir", "subdir", "file.txt") == str(
                Path("dir", "subdir", "file.txt")
            )

            # Test file write/read
            test_content = b"Hello, World!"
            with fs.open("test_file.txt", "wb") as f:
                f.write(test_content)

            assert fs.exists("test_file.txt") is True

            with fs.open("test_file.txt", "rb") as f:
                read_content = f.read()
                assert read_content == test_content

            # Test text mode
            text_content = "Hello, World!"
            with fs.open("test_text.txt", "w") as f:
                f.write(text_content)

            with fs.open("test_text.txt", "r") as f:
                read_text = f.read()
                assert read_text == text_content

    def test_nested_directory_creation(self):
        """Test nested directory creation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fs = LocalFilesystem(tmpdir)
            fs.mkdir("nested/deep/path")
            assert fs.exists("nested/deep/path") is True

    def test_exist_ok_parameter(self):
        """Test exist_ok parameter in mkdir."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fs = LocalFilesystem(tmpdir)
            fs.mkdir("test_dir")
            # Should not raise an exception
            fs.mkdir("test_dir", exist_ok=True)


class TestFsspecFilesystem:
    """Test FsspecFilesystem class functionality."""

    @patch("fsspec.filesystem")
    def test_filesystem_creation(self, mock_fs):
        """Test fsspec filesystem creation."""
        mock_fs_instance = MagicMock()
        mock_fs.return_value = mock_fs_instance

        fs = FsspecFilesystem("s3://bucket/path", {"key": "test"})

        assert fs.protocol == "s3"
        assert fs.storage_options == {"key": "test"}
        mock_fs.assert_called_once_with("s3", key="test")

    @patch("fsspec.filesystem")
    def test_full_path_construction(self, mock_fs):
        """Test full path construction."""
        mock_fs_instance = MagicMock()
        mock_fs.return_value = mock_fs_instance

        fs = FsspecFilesystem("s3://bucket/path")

        # Test various path combinations
        assert fs._get_full_path("") == "s3://bucket/path"
        assert fs._get_full_path("file.txt") == "s3://bucket/path/file.txt"
        assert fs._get_full_path("/file.txt") == "s3://bucket/path/file.txt"
        assert fs._get_full_path("dir/file.txt") == "s3://bucket/path/dir/file.txt"

    @patch("fsspec.filesystem")
    def test_full_path_construction_trailing_slash(self, mock_fs):
        """Test full path construction with trailing slash."""
        mock_fs_instance = MagicMock()
        mock_fs.return_value = mock_fs_instance

        fs = FsspecFilesystem("s3://bucket/path/")

        # The implementation strips trailing slashes, so we expect that behavior
        assert fs._get_full_path("") == "s3://bucket/path"
        assert fs._get_full_path("file.txt") == "s3://bucket/path/file.txt"

    @patch("fsspec.filesystem")
    def test_open_with_directory_creation(self, mock_fs):
        """Test that open creates parent directories."""
        mock_fs_instance = MagicMock()
        mock_fs.return_value = mock_fs_instance
        mock_file = MagicMock()
        mock_fs_instance.open.return_value = mock_file

        fs = FsspecFilesystem("s3://bucket/path")

        # Test opening a file in a subdirectory
        result = fs.open("subdir/file.txt", "w")

        # Should create parent directory
        mock_fs_instance.makedirs.assert_called_once_with(
            "s3://bucket/path/subdir", exist_ok=True
        )
        # Should open the file
        mock_fs_instance.open.assert_called_once_with(
            "s3://bucket/path/subdir/file.txt", "w"
        )
        assert result == mock_file

    @patch("fsspec.filesystem")
    def test_exists(self, mock_fs):
        """Test exists method."""
        mock_fs_instance = MagicMock()
        mock_fs.return_value = mock_fs_instance
        mock_fs_instance.exists.return_value = True

        fs = FsspecFilesystem("s3://bucket/path")

        result = fs.exists("file.txt")

        mock_fs_instance.exists.assert_called_once_with("s3://bucket/path/file.txt")
        assert result is True

    @patch("fsspec.filesystem")
    def test_mkdir(self, mock_fs):
        """Test mkdir method."""
        mock_fs_instance = MagicMock()
        mock_fs.return_value = mock_fs_instance

        fs = FsspecFilesystem("s3://bucket/path")

        fs.mkdir("subdir", exist_ok=False)

        mock_fs_instance.makedirs.assert_called_once_with(
            "s3://bucket/path/subdir", exist_ok=False
        )

    @patch("fsspec.filesystem")
    def test_basename(self, mock_fs):
        """Test basename method."""
        mock_fs_instance = MagicMock()
        mock_fs.return_value = mock_fs_instance

        fs = FsspecFilesystem("s3://bucket/path")

        result = fs.basename("path/to/file.txt")
        assert result == "file.txt"

    @patch("fsspec.filesystem")
    def test_join(self, mock_fs):
        """Test join method."""
        mock_fs_instance = MagicMock()
        mock_fs.return_value = mock_fs_instance

        fs = FsspecFilesystem("s3://bucket/path")

        result = fs.join("dir", "file.txt")
        assert result == str(Path("dir", "file.txt"))


class TestTargetFilesystemInterface:
    """Test that TargetFilesystem implementations follow the interface."""

    def test_local_filesystem_interface(self):
        """Test that LocalFilesystem implements the interface correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fs = LocalFilesystem(tmpdir)
            assert isinstance(fs, TargetFilesystem)

            # Test that all required methods exist and are callable
            assert callable(fs.open)
            assert callable(fs.exists)
            assert callable(fs.mkdir)
            assert callable(fs.basename)
            assert callable(fs.join)

    @patch("fsspec.filesystem")
    def test_fsspec_filesystem_interface(self, mock_fs):
        """Test that FsspecFilesystem implements the interface correctly."""
        mock_fs_instance = MagicMock()
        mock_fs.return_value = mock_fs_instance

        fs = FsspecFilesystem("s3://bucket/path")
        assert isinstance(fs, TargetFilesystem)

        # Test that all required methods exist and are callable
        assert callable(fs.open)
        assert callable(fs.exists)
        assert callable(fs.mkdir)
        assert callable(fs.basename)
        assert callable(fs.join)


class TestIntegration:
    """Integration tests for TargetLocation with real filesystem operations."""

    def test_complete_local_workflow(self):
        """Test a complete workflow with local filesystem."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create TargetLocation
            target = TargetLocation(tmpdir)
            fs = target.get_filesystem()

            # Create directory structure
            fs.mkdir("data/subdir")
            assert fs.exists("data/subdir")

            # Write files
            with fs.open("data/file1.txt", "w") as f:
                f.write("Content 1")

            with fs.open("data/subdir/file2.txt", "w") as f:
                f.write("Content 2")

            # Verify files exist
            assert fs.exists("data/file1.txt")
            assert fs.exists("data/subdir/file2.txt")

            # Read files back
            with fs.open("data/file1.txt", "r") as f:
                assert f.read() == "Content 1"

            with fs.open("data/subdir/file2.txt", "r") as f:
                assert f.read() == "Content 2"

            # Test utility methods
            assert fs.basename("data/subdir/file2.txt") == "file2.txt"
            assert fs.join("data", "subdir", "file3.txt") == str(
                Path("data", "subdir", "file3.txt")
            )

    def test_path_conversion_compatibility(self):
        """Test that TargetLocation works with various path types."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # String path
            loc1 = TargetLocation(tmpdir)
            assert isinstance(loc1.get_filesystem(), LocalFilesystem)

            # Path object
            loc2 = TargetLocation(Path(tmpdir))
            assert isinstance(loc2.get_filesystem(), LocalFilesystem)

            # Both should point to the same location
            assert loc1.path == loc2.path
