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
import stat
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import paramiko

from src.config.models import SFTPConfig, is_path_pattern, matches_pattern

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

    def list_files_recursive(
        self,
        pattern: str = "*",
        max_depth: int = 5
    ) -> List[str]:
        """
        List files matching pattern, recursively scanning subdirectories.

        Use this when the pattern contains path components (e.g., "reports/*.csv").

        Args:
            pattern: Glob pattern that may include path components
                (e.g., "*.csv", "reports/*.csv", "*/daily/*.csv")
            max_depth: Maximum directory depth to scan (default: 5)

        Returns:
            List of relative paths (from remote_path) matching the pattern

        Raises:
            SFTPError: If listing fails
        """
        self._ensure_connected()

        try:
            base_path = self.config.remote_path.rstrip("/")
            results: List[str] = []

            logger.debug(f"Recursively listing files in {base_path} matching '{pattern}'")

            def scan_directory(dir_path: str, relative_prefix: str, depth: int) -> None:
                if depth > max_depth:
                    logger.debug(f"Max depth {max_depth} reached at {dir_path}")
                    return

                try:
                    entries = self._sftp.listdir_attr(dir_path)
                except IOError as e:
                    logger.warning(f"Cannot list directory {dir_path}: {e}")
                    return

                for entry in entries:
                    entry_name = entry.filename
                    full_path = f"{dir_path}/{entry_name}"
                    relative_path = f"{relative_prefix}/{entry_name}" if relative_prefix else entry_name

                    # Check if it's a directory
                    is_dir = stat.S_ISDIR(entry.st_mode) if entry.st_mode else False

                    if is_dir:
                        # Recurse into subdirectory
                        scan_directory(full_path, relative_path, depth + 1)
                    else:
                        # Check if file matches pattern
                        if matches_pattern(pattern, relative_path):
                            results.append(relative_path)

            scan_directory(base_path, "", 0)

            logger.info(f"Found {len(results)} files matching '{pattern}' (recursive)")
            return sorted(results)

        except IOError as e:
            raise SFTPError(f"Failed to list files recursively in {self.config.remote_path}: {e}") from e

    def download_files(
        self,
        files: List[str],
        temp_dir: Optional[str] = None
    ) -> DownloadResult:
        """
        Download files from remote server to local temp directory.

        Args:
            files: List of relative paths (from remote_path) to download.
                Can be simple filenames or paths with subdirectories.
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

        for relative_path in files:
            remote_full_path = f"{self.config.remote_path.rstrip('/')}/{relative_path}"
            local_path = os.path.join(result.temp_dir, relative_path)

            # Create subdirectories if needed (for path patterns)
            local_dir = os.path.dirname(local_path)
            if local_dir and not os.path.exists(local_dir):
                os.makedirs(local_dir, exist_ok=True)

            try:
                logger.debug(f"Downloading: {remote_full_path} -> {local_path}")
                self._sftp.get(remote_full_path, local_path)

                result.local_paths.append(local_path)
                result.remote_files.append(relative_path)

            except IOError as e:
                error_msg = f"Failed to download {relative_path}: {e}"
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
        Automatically uses recursive listing when pattern contains path components.

        Args:
            pattern: Glob pattern to match files. Can include path components
                (e.g., "reports/*.csv") for recursive scanning.

        Returns:
            DownloadResult with local paths
        """
        # Use recursive listing if pattern contains path components
        if is_path_pattern(pattern):
            files = self.list_files_recursive(pattern)
        else:
            files = self.list_files(pattern)

        if not files:
            logger.warning(f"No files found matching '{pattern}'")
            return DownloadResult()

        return self.download_files(files)

    def download_companion_files(
        self,
        remote_files: List[str],
        companion_extensions: List[str],
        temp_dir: str
    ) -> List[str]:
        """
        Download companion files that share the same base name as the primary files.

        For each remote file (e.g., DATA.dbf), looks for files with the same stem
        but different extensions (e.g., DATA.fpt, DATA.dbt). Matching is
        case-insensitive to handle Linux servers with mixed-case filenames.

        Args:
            remote_files: List of relative paths from remote_path (e.g., ["DATA.dbf"])
            companion_extensions: Extensions to look for (e.g., [".fpt", ".dbt"])
            temp_dir: Local directory to download companion files into

        Returns:
            List of local paths of successfully downloaded companion files
        """
        self._ensure_connected()

        downloaded: List[str] = []

        if not remote_files or not companion_extensions:
            return downloaded

        # List the remote directory once for case-insensitive matching
        remote_path = self.config.remote_path.rstrip("/")
        try:
            all_files = self._sftp.listdir(remote_path)
        except IOError as e:
            logger.error(f"Failed to list remote directory {remote_path}: {e}")
            return downloaded

        # Build case-insensitive lookup: lowered name -> actual name on server
        name_lookup = {name.lower(): name for name in all_files}

        for relative_path in remote_files:
            stem = Path(relative_path).stem
            parent = str(Path(relative_path).parent)

            for ext in companion_extensions:
                # Compute the expected companion filename
                companion_name = f"{stem}{ext}"
                actual_name = name_lookup.get(companion_name.lower())

                if not actual_name:
                    logger.debug(
                        f"Companion file not found: {companion_name} "
                        f"(for {relative_path})"
                    )
                    continue

                remote_full_path = f"{remote_path}/{actual_name}"

                # Place companion in the same local subdirectory as the primary file
                if parent and parent != ".":
                    local_dir = os.path.join(temp_dir, parent)
                    os.makedirs(local_dir, exist_ok=True)
                    local_path = os.path.join(local_dir, actual_name)
                else:
                    local_path = os.path.join(temp_dir, actual_name)

                try:
                    # Log file size for progress visibility
                    try:
                        file_stat = self._sftp.stat(remote_full_path)
                        size_mb = file_stat.st_size / (1024 * 1024)
                        logger.info(f"Downloading companion: {actual_name} ({size_mb:.1f} MB)")
                    except Exception:
                        logger.info(f"Downloading companion: {actual_name}")
                    self._sftp.get(remote_full_path, local_path)
                    downloaded.append(local_path)
                except Exception as e:
                    logger.error(
                        f"Failed to download companion file {actual_name}: {e}"
                    )

        logger.info(f"Downloaded {len(downloaded)} companion files")
        return downloaded


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
