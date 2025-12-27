"""
SFTP client for pulling CSV files from remote servers.

This module provides a context-managed SFTP client that:
- Connects with password or SSH key authentication
- Lists files matching glob patterns
- Downloads files to a temporary directory
- Cleans up after processing
"""

import fnmatch
import logging
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import paramiko

from src.config.models import SFTPConfig

logger = logging.getLogger(__name__)


class SFTPError(Exception):
    """Raised when SFTP operations fail."""
    pass


@dataclass
class DownloadResult:
    """
    Result of downloading files from SFTP.

    Attributes:
        local_paths: List of local file paths where files were downloaded
        remote_files: List of remote filenames that were downloaded
        temp_dir: Temporary directory containing downloaded files
        errors: List of error messages for failed downloads
    """
    local_paths: List[str] = field(default_factory=list)
    remote_files: List[str] = field(default_factory=list)
    temp_dir: Optional[str] = None
    errors: List[str] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        """Number of successfully downloaded files."""
        return len(self.local_paths)

    @property
    def has_errors(self) -> bool:
        """Check if any downloads failed."""
        return len(self.errors) > 0


class SFTPClient:
    """
    SFTP client for pulling files from remote servers.

    Supports both password and SSH key authentication.
    Use as a context manager to ensure proper cleanup.

    Example:
        ```python
        config = SFTPConfig(
            host="sftp.example.com",
            username="user",
            key_path="~/.ssh/id_rsa",
            remote_path="/exports/"
        )

        with SFTPClient(config) as sftp:
            files = sftp.list_files("*.csv")
            result = sftp.download_files(files)
            # Process result.local_paths
        # Temp files cleaned up automatically
        ```
    """

    def __init__(self, config: SFTPConfig):
        """
        Initialize SFTP client with configuration.

        Args:
            config: SFTPConfig with connection details
        """
        self.config = config
        self._transport: Optional[paramiko.Transport] = None
        self._sftp: Optional[paramiko.SFTPClient] = None
        self._temp_dir: Optional[str] = None

    def __enter__(self) -> "SFTPClient":
        """Connect to SFTP server."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Disconnect and cleanup."""
        self.disconnect()
        self.cleanup()

    def connect(self) -> None:
        """
        Establish connection to SFTP server.

        Raises:
            SFTPError: If connection fails
        """
        try:
            logger.info(f"Connecting to SFTP: {self.config.host}:{self.config.port}")

            # Create transport
            self._transport = paramiko.Transport((self.config.host, self.config.port))

            # Authenticate
            if self.config.key_path:
                key_path = os.path.expanduser(self.config.key_path)
                if not os.path.exists(key_path):
                    raise SFTPError(f"SSH key file not found: {key_path}")

                # Try different key types
                pkey = self._load_private_key(key_path)
                self._transport.connect(username=self.config.username, pkey=pkey)
                logger.debug(f"Authenticated with SSH key: {key_path}")

            elif self.config.password:
                self._transport.connect(
                    username=self.config.username,
                    password=self.config.password
                )
                logger.debug("Authenticated with password")

            else:
                raise SFTPError(
                    "No authentication method provided. "
                    "Set either 'password' or 'key_path' in SFTP config."
                )

            # Create SFTP client
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            logger.info(f"Connected to SFTP server: {self.config.host}")

        except paramiko.SSHException as e:
            raise SFTPError(f"SSH connection failed: {e}") from e
        except Exception as e:
            raise SFTPError(f"SFTP connection failed: {e}") from e

    def _load_private_key(self, key_path: str) -> paramiko.PKey:
        """
        Load private key from file, trying different key types.

        Args:
            key_path: Path to private key file

        Returns:
            Loaded private key

        Raises:
            SFTPError: If key cannot be loaded
        """
        key_classes = [
            paramiko.RSAKey,
            paramiko.Ed25519Key,
            paramiko.ECDSAKey,
            paramiko.DSSKey,
        ]

        last_error = None
        for key_class in key_classes:
            try:
                return key_class.from_private_key_file(key_path)
            except paramiko.SSHException as e:
                last_error = e
                continue

        raise SFTPError(f"Could not load SSH key {key_path}: {last_error}")

    def disconnect(self) -> None:
        """Close SFTP connection."""
        if self._sftp:
            try:
                self._sftp.close()
            except Exception as e:
                logger.warning(f"Error closing SFTP client: {e}")
            self._sftp = None

        if self._transport:
            try:
                self._transport.close()
            except Exception as e:
                logger.warning(f"Error closing transport: {e}")
            self._transport = None

        logger.debug("SFTP connection closed")

    def cleanup(self) -> None:
        """
        Remove temporary directory and downloaded files.

        Call this after processing downloaded files.
        Automatically called when using context manager.
        """
        if self._temp_dir and os.path.exists(self._temp_dir):
            try:
                import shutil
                shutil.rmtree(self._temp_dir)
                logger.debug(f"Cleaned up temp directory: {self._temp_dir}")
                self._temp_dir = None
            except Exception as e:
                logger.warning(f"Failed to cleanup temp directory: {e}")

    def _ensure_connected(self) -> None:
        """Raise error if not connected."""
        if not self._sftp:
            raise SFTPError("Not connected to SFTP server. Call connect() first.")

    def list_files(self, pattern: str = "*") -> List[str]:
        """
        List files in remote directory matching pattern.

        Args:
            pattern: Glob pattern to match files (e.g., "*.csv", "IxExp*.csv")

        Returns:
            List of filenames matching the pattern

        Raises:
            SFTPError: If listing fails
        """
        self._ensure_connected()

        try:
            remote_path = self.config.remote_path
            logger.debug(f"Listing files in {remote_path} matching '{pattern}'")

            all_files = self._sftp.listdir(remote_path)

            # Filter by pattern
            matching_files = [
                f for f in all_files
                if fnmatch.fnmatch(f, pattern)
            ]

            # Filter out directories
            result = []
            for filename in matching_files:
                try:
                    full_path = os.path.join(remote_path, filename)
                    stat = self._sftp.stat(full_path)
                    # Check if it's a regular file (not directory)
                    if not stat.st_mode & 0o40000:  # S_IFDIR
                        result.append(filename)
                except Exception:
                    # Skip files we can't stat
                    continue

            logger.info(f"Found {len(result)} files matching '{pattern}' in {remote_path}")
            return sorted(result)

        except IOError as e:
            raise SFTPError(f"Failed to list files in {self.config.remote_path}: {e}") from e

    def download_files(
        self,
        files: List[str],
        temp_dir: Optional[str] = None
    ) -> DownloadResult:
        """
        Download files from remote server to local temp directory.

        Args:
            files: List of filenames to download
            temp_dir: Optional custom temp directory (created if not exists)

        Returns:
            DownloadResult with local paths and any errors

        Raises:
            SFTPError: If download fails critically
        """
        self._ensure_connected()

        result = DownloadResult()

        # Create or use temp directory
        if temp_dir:
            os.makedirs(temp_dir, exist_ok=True)
            result.temp_dir = temp_dir
        else:
            result.temp_dir = tempfile.mkdtemp(prefix="csv_import_")
            self._temp_dir = result.temp_dir  # Track for cleanup

        logger.info(f"Downloading {len(files)} files to {result.temp_dir}")

        for filename in files:
            remote_path = os.path.join(self.config.remote_path, filename)
            local_path = os.path.join(result.temp_dir, filename)

            try:
                logger.debug(f"Downloading: {remote_path} -> {local_path}")
                self._sftp.get(remote_path, local_path)

                result.local_paths.append(local_path)
                result.remote_files.append(filename)

            except IOError as e:
                error_msg = f"Failed to download {filename}: {e}"
                logger.error(error_msg)
                result.errors.append(error_msg)

        logger.info(
            f"Downloaded {result.success_count}/{len(files)} files"
            + (f" ({len(result.errors)} errors)" if result.errors else "")
        )

        return result

    def download_matching_files(self, pattern: str = "*.csv") -> DownloadResult:
        """
        List and download all files matching pattern.

        Convenience method combining list_files() and download_files().

        Args:
            pattern: Glob pattern to match files

        Returns:
            DownloadResult with local paths
        """
        files = self.list_files(pattern)
        if not files:
            logger.warning(f"No files found matching '{pattern}'")
            return DownloadResult()

        return self.download_files(files)


def test_connection(config: SFTPConfig) -> bool:
    """
    Test SFTP connection without downloading files.

    Args:
        config: SFTPConfig with connection details

    Returns:
        True if connection successful, False otherwise
    """
    try:
        with SFTPClient(config) as sftp:
            files = sftp.list_files()
            logger.info(f"Connection test successful. Found {len(files)} files.")
            return True
    except SFTPError as e:
        logger.error(f"Connection test failed: {e}")
        return False
