"""Unit tests for cloud transfer functionality."""

import pytest
from unittest.mock import Mock, patch

from earthaccess.store_components.cloud_transfer import CloudTransfer, TransferError


class TestCloudTransfer:
    """Test CloudTransfer class functionality."""

    @pytest.fixture
    def mock_auth(self):
        """Mock Auth instance."""
        auth = Mock()
        auth.authenticated = True
        auth.system.edl_hostname = "urs.earthdata.nasa.gov"
        return auth

    @pytest.fixture
    def mock_credential_manager(self):
        """Mock CredentialManager."""
        manager = Mock()
        manager.get_auth_context.return_value = Mock(
            s3_credentials=Mock(
                to_dict=lambda: {"key": "x", "secret": "y", "token": "z"}
            )
        )
        return manager

    def test_parse_target_string(self, mock_auth, mock_credential_manager):
        """Test parsing string targets."""
        transfer = CloudTransfer(mock_auth, mock_credential_manager)

        result = transfer._parse_target("s3://bucket/key")
        assert result == "s3://bucket/key"

        result = transfer._parse_target("https://example.com/file")
        assert result == "https://example.com/file"

    def test_parse_target_location(self, mock_auth, mock_credential_manager):
        """Test parsing TargetLocation objects."""
        transfer = CloudTransfer(mock_auth, mock_credential_manager)

        location = Mock()
        location.url = "s3://bucket/key"

        result = transfer._parse_target(location)
        assert result == "s3://bucket/key"

    def test_invalid_target_raises_error(self, mock_auth, mock_credential_manager):
        """Test that invalid target raises ValueError."""
        transfer = CloudTransfer(mock_auth, mock_credential_manager)

        with pytest.raises(ValueError, match="Invalid target type"):
            transfer._parse_target(123)

    def test_determine_s3_to_s3_strategy(self, mock_auth, mock_credential_manager):
        """Test strategy determination for S3-to-S3."""
        transfer = CloudTransfer(mock_auth, mock_credential_manager)

        # Mock granules with S3 links
        granule1 = Mock()
        granule1.data_links.return_value = ["s3://source/file1.nc"]
        granule2 = Mock()
        granule2.data_links.return_value = ["s3://source/file2.nc"]
        granules = [granule1, granule2]

        strategy = transfer._determine_strategy(granules, "s3://target/")
        assert strategy == "s3_to_s3"

    def test_determine_https_to_s3_strategy(self, mock_auth, mock_credential_manager):
        """Test strategy determination for HTTPS-to-S3."""
        transfer = CloudTransfer(mock_auth, mock_credential_manager)

        # Mock granules with HTTPS links
        granule1 = Mock()
        granule1.data_links.return_value = ["https://source/file1.nc"]
        granules = [granule1]

        strategy = transfer._determine_strategy(granules, "s3://target/")
        assert strategy == "https_to_s3"

    def test_determine_generic_strategy(self, mock_auth, mock_credential_manager):
        """Test fallback to generic strategy."""
        transfer = CloudTransfer(mock_auth, mock_credential_manager)

        # Mock granules with mixed or unsupported protocols
        granule1 = Mock()
        granule1.data_links.return_value = ["file://local/file1.nc"]
        granules = [granule1]

        strategy = transfer._determine_strategy(granules, "file://local/target/")
        assert strategy == "generic"

    def test_parse_s3_url(self, mock_auth, mock_credential_manager):
        """Test S3 URL parsing."""
        transfer = CloudTransfer(mock_auth, mock_credential_manager)

        bucket, key = transfer._parse_s3_url("s3://my-bucket/path/to/file.nc")
        assert bucket == "my-bucket"
        assert key == "path/to/file.nc"

    def test_parse_s3_url_raises_error(self, mock_auth, mock_credential_manager):
        """Test that non-S3 URLs raise error."""
        transfer = CloudTransfer(mock_auth, mock_credential_manager)

        with pytest.raises(ValueError, match="Not an S3 URL"):
            transfer._parse_s3_url("https://example.com/file")

    def test_create_target_key_with_path(self, mock_auth, mock_credential_manager):
        """Test target key creation with subdirectory."""
        transfer = CloudTransfer(mock_auth, mock_credential_manager)

        source_key = "path/to/file.nc"
        target_url = "s3://bucket/target/"

        result = transfer._create_target_key(
            "s3://source/" + source_key, target_url, source_key
        )
        assert result == "path/to/file.nc"  # Preserves path structure

    def test_create_target_key_filename_only(self, mock_auth, mock_credential_manager):
        """Test target key creation with filename only."""
        transfer = CloudTransfer(mock_auth, mock_credential_manager)

        source_key = "file.nc"
        target_url = "s3://bucket/"

        result = transfer._create_target_key(
            "s3://source/" + source_key, target_url, source_key
        )
        assert result == "file.nc"  # Just filename

    @patch("earthaccess.store_components.cloud_transfer.infer_provider_from_url")
    def test_infer_provider_from_granules(
        self, mock_infer, mock_auth, mock_credential_manager
    ):
        """Test provider inference from granules."""
        transfer = CloudTransfer(mock_auth, mock_credential_manager)

        # Setup mock return
        mock_infer.side_effect = lambda url: "POCLOUD" if "podaac" in url else None

        # Mock granules
        granule1 = Mock()
        granule1.data_links.return_value = ["s3://podaac/file1.nc"]
        granules = [granule1]

        provider = transfer._infer_provider_from_granules(granules)
        assert provider == "POCLOUD"
        mock_infer.assert_called_with("s3://podaac/file1.nc")

    def test_get_transfer_estimate_s3_strategy(
        self, mock_auth, mock_credential_manager
    ):
        """Test transfer estimates for S3-to-S3."""
        transfer = CloudTransfer(mock_auth, mock_credential_manager)

        granule1 = Mock()
        granule1.data_links.return_value = ["s3://source/file.nc"]
        granule1.get.return_value = {"meta": {"granule-size": 1000000000}}  # 1GB
        granules = [granule1]

        estimate = transfer.get_transfer_estimate(granules, "s3://target/", "s3_to_s3")

        assert estimate["strategy"] == "s3_to_s3"
        assert estimate["file_count"] == 1
        assert estimate["estimated_size"] == 1000000000
        assert estimate["estimated_time"] == pytest.approx(
            20, rel=0.1
        )  # ~20 seconds at 50MB/s

    def test_get_transfer_estimate_auto_strategy(
        self, mock_auth, mock_credential_manager
    ):
        """Test transfer estimates with auto strategy detection."""
        transfer = CloudTransfer(mock_auth, mock_credential_manager)

        granule1 = Mock()
        granule1.data_links.return_value = ["https://source/file.nc"]
        granules = [granule1]

        # Should auto-detect as https_to_s3
        estimate = transfer.get_transfer_estimate(granules, "s3://target/")

        assert estimate["strategy"] == "https_to_s3"
        assert estimate["file_count"] == 1

    def test_transfer_error_attributes(self):
        """Test TransferError attributes."""
        error = TransferError("Test message", "s3://source/file", "s3://target/file")

        assert str(error) == "Test message"
        assert error.source_url == "s3://source/file"
        assert error.target_url == "s3://target/file"

    def test_init_without_credential_manager(self, mock_auth):
        """Test initialization without CredentialManager."""
        transfer = CloudTransfer(mock_auth)

        assert transfer.auth == mock_auth
        assert transfer.credential_manager is None

    def test_init_with_credential_manager(self, mock_auth, mock_credential_manager):
        """Test initialization with CredentialManager."""
        transfer = CloudTransfer(mock_auth, mock_credential_manager)

        assert transfer.auth == mock_auth
        assert transfer.credential_manager == mock_credential_manager
