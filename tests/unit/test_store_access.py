"""TDD Tests for store/access.py module.

Tests the S3 access probing and access strategy utilities that help
determine whether to use S3 or HTTPS for data access.
"""

from unittest.mock import Mock


class TestProbeS3Access:
    """Test the probe_s3_access function."""

    def test_probe_success_returns_direct(self) -> None:
        """Test that successful S3 probe returns 'direct' access."""
        from earthaccess.store.access import probe_s3_access

        mock_fs = Mock()
        mock_file = Mock()
        mock_file.read.return_value = b"test data"
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock(return_value=False)
        mock_fs.open.return_value = mock_file

        result = probe_s3_access(
            s3_fs=mock_fs,
            s3_url="s3://bucket/path/file.nc",
        )

        assert result is True
        mock_fs.open.assert_called_once()

    def test_probe_failure_returns_false(self) -> None:
        """Test that failed S3 probe returns False."""
        from earthaccess.store.access import probe_s3_access

        mock_fs = Mock()
        mock_fs.open.side_effect = PermissionError("Access denied")

        result = probe_s3_access(
            s3_fs=mock_fs,
            s3_url="s3://bucket/path/file.nc",
        )

        assert result is False

    def test_probe_with_empty_url_returns_false(self) -> None:
        """Test that empty URL returns False."""
        from earthaccess.store.access import probe_s3_access

        mock_fs = Mock()

        result = probe_s3_access(
            s3_fs=mock_fs,
            s3_url="",
        )

        assert result is False
        mock_fs.open.assert_not_called()

    def test_probe_reads_small_chunk(self) -> None:
        """Test that probe only reads a small amount of data."""
        from earthaccess.store.access import probe_s3_access

        mock_fs = Mock()
        mock_file = Mock()
        mock_file.read.return_value = b"x" * 10
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock(return_value=False)
        mock_fs.open.return_value = mock_file

        probe_s3_access(s3_fs=mock_fs, s3_url="s3://bucket/file.nc")

        # Should read a small chunk (10 bytes by default)
        mock_file.read.assert_called_once_with(10)


class TestDetermineAccessMethod:
    """Test the determine_access_method function."""

    def test_cloud_hosted_with_s3_access(self) -> None:
        """Test that cloud-hosted granule with S3 access returns 'direct'."""
        from earthaccess.store.access import AccessMethod, determine_access_method

        mock_granule = Mock()
        mock_granule.cloud_hosted = True
        mock_granule.data_links.return_value = ["s3://bucket/file.nc"]
        mock_granule.__getitem__ = Mock(
            side_effect=lambda k: {"meta": {"provider-id": "POCLOUD"}}[k]
        )

        mock_s3_fs = Mock()
        mock_file = Mock()
        mock_file.read.return_value = b"test"
        mock_file.__enter__ = Mock(return_value=mock_file)
        mock_file.__exit__ = Mock(return_value=False)
        mock_s3_fs.open.return_value = mock_file

        result = determine_access_method(
            granule=mock_granule,
            s3_fs=mock_s3_fs,
        )

        assert result == AccessMethod.DIRECT

    def test_cloud_hosted_without_s3_access(self) -> None:
        """Test that cloud-hosted granule without S3 access returns 'external'."""
        from earthaccess.store.access import AccessMethod, determine_access_method

        mock_granule = Mock()
        mock_granule.cloud_hosted = True
        mock_granule.data_links.return_value = ["s3://bucket/file.nc"]

        mock_s3_fs = Mock()
        mock_s3_fs.open.side_effect = PermissionError("No access")

        result = determine_access_method(
            granule=mock_granule,
            s3_fs=mock_s3_fs,
        )

        assert result == AccessMethod.EXTERNAL

    def test_on_prem_granule_returns_external(self) -> None:
        """Test that on-prem granule returns 'external'."""
        from earthaccess.store.access import AccessMethod, determine_access_method

        mock_granule = Mock()
        mock_granule.cloud_hosted = False

        result = determine_access_method(
            granule=mock_granule,
            s3_fs=None,
        )

        assert result == AccessMethod.EXTERNAL

    def test_no_s3_links_returns_external(self) -> None:
        """Test that no S3 links returns 'external'."""
        from earthaccess.store.access import AccessMethod, determine_access_method

        mock_granule = Mock()
        mock_granule.cloud_hosted = True
        mock_granule.data_links.return_value = []  # No S3 links

        mock_s3_fs = Mock()

        result = determine_access_method(
            granule=mock_granule,
            s3_fs=mock_s3_fs,
        )

        assert result == AccessMethod.EXTERNAL


class TestAccessMethod:
    """Test the AccessMethod enum."""

    def test_access_method_values(self) -> None:
        """Test that AccessMethod has expected values."""
        from earthaccess.store.access import AccessMethod

        assert AccessMethod.DIRECT.value == "direct"
        assert AccessMethod.EXTERNAL.value == "external"

    def test_access_method_to_string(self) -> None:
        """Test converting AccessMethod to string for data_links()."""
        from earthaccess.store.access import AccessMethod

        assert str(AccessMethod.DIRECT.value) == "direct"
        assert str(AccessMethod.EXTERNAL.value) == "external"


class TestExtractS3CredentialsEndpoint:
    """Test the extract_s3_credentials_endpoint function."""

    def test_extract_from_related_urls(self) -> None:
        """Test extracting S3 credentials endpoint from RelatedUrls."""
        from earthaccess.store.access import extract_s3_credentials_endpoint

        related_urls = [
            {"URL": "https://data.example.com/file.nc", "Type": "GET DATA"},
            {
                "URL": "https://data.example.com/s3credentials",
                "Type": "VIEW RELATED INFORMATION",
                "Subtype": "ALGORITHM THEORETICAL BASIS DOCUMENT (ATBD)",
            },
            {
                "URL": "https://archive.example.com/s3credentials",
                "Type": "USE SERVICE API",
                "Subtype": "S3 CREDENTIALS",
            },
        ]

        result = extract_s3_credentials_endpoint(related_urls)

        assert result == "https://archive.example.com/s3credentials"

    def test_extract_returns_none_when_not_found(self) -> None:
        """Test that None is returned when no S3 credentials endpoint found."""
        from earthaccess.store.access import extract_s3_credentials_endpoint

        related_urls = [
            {"URL": "https://data.example.com/file.nc", "Type": "GET DATA"},
        ]

        result = extract_s3_credentials_endpoint(related_urls)

        assert result is None

    def test_extract_handles_empty_list(self) -> None:
        """Test that empty list returns None."""
        from earthaccess.store.access import extract_s3_credentials_endpoint

        result = extract_s3_credentials_endpoint([])

        assert result is None


class TestGetDataLinks:
    """Test the get_data_links helper function."""

    def test_get_direct_links(self) -> None:
        """Test getting S3 direct links from granules."""
        from earthaccess.store.access import get_data_links

        mock_granule1 = Mock()
        mock_granule1.data_links.return_value = ["s3://bucket/file1.nc"]

        mock_granule2 = Mock()
        mock_granule2.data_links.return_value = ["s3://bucket/file2.nc"]

        granules = [mock_granule1, mock_granule2]

        result = get_data_links(granules, access="direct")

        assert len(result) == 2
        assert "s3://bucket/file1.nc" in result
        assert "s3://bucket/file2.nc" in result

    def test_get_external_links(self) -> None:
        """Test getting HTTPS external links from granules."""
        from earthaccess.store.access import get_data_links

        mock_granule = Mock()
        mock_granule.data_links.return_value = [
            "https://data.example.com/file1.nc",
            "https://data.example.com/file2.nc",
        ]

        granules = [mock_granule]

        result = get_data_links(granules, access="external")

        assert len(result) == 2
        mock_granule.data_links.assert_called_with(access="external")
