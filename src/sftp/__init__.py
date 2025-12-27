"""
SFTP module for pulling CSV files from remote servers.

This module provides:
- SFTPClient: Context-managed client for SFTP operations
- test_connection: Quick connection test utility
"""

from src.sftp.client import (
    SFTPClient,
    SFTPError,
    DownloadResult,
    test_connection,
)

__all__ = [
    "SFTPClient",
    "SFTPError",
    "DownloadResult",
    "test_connection",
]
