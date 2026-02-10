"""Tests for SFTPClient.download_companion_files method."""

import os
from unittest.mock import MagicMock, call

import pytest

from src.config.models import SFTPConfig
from src.sftp.client import SFTPClient


def _make_client(remote_path: str = "/data") -> SFTPClient:
    """
    Create an SFTPClient with a mocked SFTP connection.

    Sets ``_sftp`` to a MagicMock so that ``_ensure_connected()`` passes
    without a real paramiko connection.
    """
    config = SFTPConfig(
        host="sftp.example.com",
        username="testuser",
        password="testpass",
        remote_path=remote_path,
    )
    client = SFTPClient(config)
    client._sftp = MagicMock()
    return client


class TestDownloadCompanionFilesFound:
    """Companion file exists on the remote server and should be downloaded."""

    def test_download_companion_files_found(self, tmp_path):
        client = _make_client()
        client._sftp.listdir.return_value = ["DATA.dbf", "DATA.fpt"]

        # Make _sftp.get create the file locally (simulates a real download)
        def fake_get(remote, local):
            with open(local, "w") as f:
                f.write("fake")

        client._sftp.get.side_effect = fake_get

        result = client.download_companion_files(
            remote_files=["DATA.dbf"],
            companion_extensions=[".fpt"],
            temp_dir=str(tmp_path),
        )

        # Verify the SFTP get was called with the correct remote and local paths
        client._sftp.get.assert_called_once_with(
            "/data/DATA.fpt",
            os.path.join(str(tmp_path), "DATA.fpt"),
        )

        # Verify the returned list contains the local path
        assert len(result) == 1
        assert result[0] == os.path.join(str(tmp_path), "DATA.fpt")


class TestDownloadCompanionFilesMissing:
    """Companion file does NOT exist on the remote server."""

    def test_download_companion_files_missing(self, tmp_path):
        client = _make_client()
        # Remote directory only has the primary file, no companion
        client._sftp.listdir.return_value = ["DATA.dbf"]

        result = client.download_companion_files(
            remote_files=["DATA.dbf"],
            companion_extensions=[".fpt"],
            temp_dir=str(tmp_path),
        )

        # get() should never be called since there is no companion file
        client._sftp.get.assert_not_called()

        # Return list should be empty
        assert result == []


class TestDownloadCompanionFilesCaseInsensitive:
    """Case-insensitive matching finds companions regardless of case."""

    def test_download_companion_files_case_insensitive(self, tmp_path):
        client = _make_client()
        # Remote files use uppercase extensions
        client._sftp.listdir.return_value = ["DATA.DBF", "DATA.FPT"]

        def fake_get(remote, local):
            with open(local, "w") as f:
                f.write("fake")

        client._sftp.get.side_effect = fake_get

        # Request with lowercase companion extension
        result = client.download_companion_files(
            remote_files=["DATA.DBF"],
            companion_extensions=[".fpt"],
            temp_dir=str(tmp_path),
        )

        # The method should resolve the actual remote filename (DATA.FPT)
        # via the case-insensitive lookup and use it for the download
        client._sftp.get.assert_called_once_with(
            "/data/DATA.FPT",
            os.path.join(str(tmp_path), "DATA.FPT"),
        )

        assert len(result) == 1
        assert result[0].endswith("DATA.FPT")


class TestDownloadCompanionFilesMultipleExtensions:
    """Multiple companion extensions should all be downloaded."""

    def test_download_companion_files_multiple_extensions(self, tmp_path):
        client = _make_client()
        client._sftp.listdir.return_value = ["DATA.dbf", "DATA.fpt", "DATA.cdx"]

        def fake_get(remote, local):
            with open(local, "w") as f:
                f.write("fake")

        client._sftp.get.side_effect = fake_get

        result = client.download_companion_files(
            remote_files=["DATA.dbf"],
            companion_extensions=[".fpt", ".cdx"],
            temp_dir=str(tmp_path),
        )

        # Both companions should have been downloaded
        assert len(result) == 2

        expected_calls = [
            call("/data/DATA.fpt", os.path.join(str(tmp_path), "DATA.fpt")),
            call("/data/DATA.cdx", os.path.join(str(tmp_path), "DATA.cdx")),
        ]
        client._sftp.get.assert_has_calls(expected_calls, any_order=True)

        # Verify local paths are returned
        local_basenames = sorted(os.path.basename(p) for p in result)
        assert local_basenames == ["DATA.cdx", "DATA.fpt"]


class TestDownloadCompanionFilesGetFailure:
    """SFTP get() failure should be handled gracefully."""

    def test_download_companion_files_get_failure(self, tmp_path):
        client = _make_client()
        client._sftp.listdir.return_value = ["DATA.dbf", "DATA.fpt"]
        client._sftp.get.side_effect = IOError("Permission denied")

        result = client.download_companion_files(
            remote_files=["DATA.dbf"],
            companion_extensions=[".fpt"],
            temp_dir=str(tmp_path),
        )

        # get() was called but failed
        client._sftp.get.assert_called_once()

        # Return list should be empty since download failed
        assert result == []


class TestDownloadCompanionFilesListdirFailure:
    """SFTP listdir() failure should be handled gracefully."""

    def test_download_companion_files_listdir_failure(self, tmp_path):
        client = _make_client()
        client._sftp.listdir.side_effect = IOError("Connection lost")

        result = client.download_companion_files(
            remote_files=["DATA.dbf"],
            companion_extensions=[".fpt"],
            temp_dir=str(tmp_path),
        )

        # get() should never be called since listdir failed
        client._sftp.get.assert_not_called()

        # Return list should be empty
        assert result == []


class TestDownloadCompanionFilesEmptyList:
    """Empty remote_files list should result in no SFTP operations."""

    def test_download_companion_files_empty_list(self, tmp_path):
        client = _make_client()

        result = client.download_companion_files(
            remote_files=[],
            companion_extensions=[".fpt"],
            temp_dir=str(tmp_path),
        )

        # No SFTP calls should be made at all
        client._sftp.listdir.assert_not_called()
        client._sftp.get.assert_not_called()

        # Return list should be empty
        assert result == []
