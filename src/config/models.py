"""
Pydantic models for project configuration.

These models define the structure of YAML configuration files
for CSV import projects. Each project has connection settings,
optional SFTP configuration, and table mappings.

Supports two modes:
1. Explicit table mappings (list each file pattern)
2. Auto-discovery with defaults (process all matching files with shared settings)
"""

import fnmatch
import logging
import re
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

if TYPE_CHECKING:
    from src.hooks.models import HookConfig

logger = logging.getLogger(__name__)


def is_path_pattern(pattern: str) -> bool:
    """
    Check if a pattern includes directory components.

    Args:
        pattern: Glob pattern to check

    Returns:
        True if pattern contains forward slash (path separator)
    """
    return "/" in pattern


def normalize_path_for_matching(path: str) -> str:
    """
    Normalize path separators to forward slash for cross-platform matching.

    Args:
        path: File path or filename

    Returns:
        Path with backslashes converted to forward slashes
    """
    return path.replace("\\", "/")


def matches_pattern(pattern: str, path_or_filename: str) -> bool:
    """
    Match a pattern against a path or filename.

    If pattern contains '/', match against the full relative path.
    Otherwise, match against just the filename (basename) for backward compatibility.

    Args:
        pattern: Glob pattern (e.g., "*.csv" or "reports/*.csv")
        path_or_filename: Either a filename or a relative path

    Returns:
        True if the pattern matches
    """
    normalized = normalize_path_for_matching(path_or_filename)

    if is_path_pattern(pattern):
        # Pattern includes path components - match against full path
        return fnmatch.fnmatch(normalized, pattern)
    else:
        # Pattern is filename-only - extract basename and match (backward compatible)
        filename = normalized.rsplit("/", 1)[-1] if "/" in normalized else normalized
        return fnmatch.fnmatch(filename, pattern)


def extract_filename(path_or_filename: str) -> str:
    """
    Extract the filename (basename) from a path.

    Args:
        path_or_filename: File path or filename

    Returns:
        Just the filename without directory components
    """
    normalized = normalize_path_for_matching(path_or_filename)
    return normalized.rsplit("/", 1)[-1] if "/" in normalized else normalized


class SFTPConfig(BaseModel):
    """
    SFTP connection configuration.

    Attributes:
        host: SFTP server hostname
        port: SFTP server port (default: 22)
        username: SFTP username
        password: SFTP password (use password OR key_path, not both)
        key_path: Path to SSH private key file
        remote_path: Remote directory to pull files from
    """
    host: str
    port: int = 22
    username: str
    password: Optional[str] = None
    key_path: Optional[str] = None
    remote_path: str = "/"


class TableNamingConfig(BaseModel):
    """
    Configuration for transforming filenames into table names.

    Attributes:
        strip_prefix: Prefix to remove from filename (e.g., "IxExp")
        strip_suffix: Suffix to remove before extension (e.g., "_Export")
        lowercase: Convert table name to lowercase (default: True)

    Example:
        With strip_prefix="IxExp", strip_suffix="_Daily", lowercase=True:
        - IxExpKonto_Daily.csv → konto
        - IxExpMieter.csv → mieter
    """
    strip_prefix: str = ""
    strip_suffix: str = ""
    lowercase: bool = True

    def transform(self, path_or_filename: str) -> str:
        """
        Transform a path or filename into a table name.

        For paths, extracts the filename first before transformation.

        Args:
            path_or_filename: Original path or filename (e.g., "reports/IxExpKonto.csv")

        Returns:
            Transformed table name (e.g., "konto")
        """
        # Extract filename from path if needed
        filename = extract_filename(path_or_filename)

        # Remove extension
        name = Path(filename).stem

        # Strip prefix (case-insensitive)
        if self.strip_prefix and name.lower().startswith(self.strip_prefix.lower()):
            name = name[len(self.strip_prefix):]

        # Strip suffix (case-insensitive)
        if self.strip_suffix and name.lower().endswith(self.strip_suffix.lower()):
            name = name[:-len(self.strip_suffix)]

        # Apply lowercase
        if self.lowercase:
            name = name.lower()

        return name


class DefaultsConfig(BaseModel):
    """
    Default settings applied to all files matching the file_pattern.

    Used for projects with many similar files that share the same
    primary key, delimiter, encoding, etc.

    Attributes:
        file_pattern: Glob pattern for files to process. Can be filename-only
            (e.g., "*.csv", "IxExp*.csv") or include paths (e.g., "reports/*.csv")
        primary_key: Default primary key column(s) for upsert
        delimiter: CSV column separator (default: ",")
        encoding: CSV file encoding (default: "utf-8")
        skiprows: Number of rows to skip before header (default: 0)
        rebuild_table: If True, TRUNCATE tables before import (default: False)
        datestyle: PostgreSQL datestyle for date parsing (e.g., "DMY" for European)
        schema: Database schema name (default: "public")
        download_pattern: Glob pattern for SFTP downloads, separate from
            file_pattern used for import matching (e.g., "*.dbf" to download
            DBF files while file_pattern matches converted CSVs).
    """
    model_config = ConfigDict(populate_by_name=True)

    file_pattern: str = "*.csv"
    download_pattern: Optional[str] = None
    companion_extensions: Optional[List[str]] = None
    primary_key: Union[str, List[str]]
    delimiter: str = ","
    encoding: str = "auto"
    skiprows: int = 0
    rebuild_table: bool = False
    datestyle: Optional[str] = None
    db_schema: str = Field(default="public", alias="schema")
    @field_validator("primary_key")
    @classmethod
    def normalize_primary_key(cls, v):
        """Ensure primary_key is always a list internally."""
        if isinstance(v, str):
            return [v]
        return v

    def matches_file(self, path_or_filename: str) -> bool:
        """
        Check if a path or filename matches this default's file_pattern.

        For path patterns (containing '/'), matches against the full relative path.
        For filename patterns, matches against just the basename.

        Args:
            path_or_filename: File path or filename to check

        Returns:
            True if it matches the pattern
        """
        return matches_pattern(self.file_pattern, path_or_filename)


class TableConfig(BaseModel):
    """
    Configuration for a single table import mapping.

    Use this for explicit file-to-table mappings, or to override
    defaults for specific files.

    Attributes:
        file_pattern: Glob pattern to match CSV files. Can be filename-only
            (e.g., "customers*.csv") or include paths (e.g., "archive/2024/*.csv")
        target_table: PostgreSQL table name to import into
        primary_key: Column(s) for upsert conflict resolution
        column_mapping: Optional mapping of CSV column names to table columns
        rebuild_table: If True, TRUNCATE table before import (default: False)
        delimiter: CSV column separator (default: ",")
        encoding: CSV file encoding (default: "utf-8")
        skiprows: Number of rows to skip before header (default: 0)
        datestyle: PostgreSQL datestyle for date parsing (e.g., "DMY" for European)
        db_schema: Database schema name (default: "public")
    """
    model_config = ConfigDict(populate_by_name=True)

    file_pattern: str
    target_table: str
    primary_key: Union[str, List[str]]
    column_mapping: Optional[Dict[str, str]] = None
    rebuild_table: bool = False
    delimiter: str = ","
    encoding: str = "auto"
    skiprows: int = 0
    datestyle: Optional[str] = None
    db_schema: str = Field(default="public", alias="schema")

    @field_validator("primary_key")
    @classmethod
    def normalize_primary_key(cls, v):
        """Ensure primary_key is always a list internally."""
        if isinstance(v, str):
            return [v]
        return v

    def matches_file(self, path_or_filename: str) -> bool:
        """
        Check if a path or filename matches this table's file_pattern.

        For path patterns (containing '/'), matches against the full relative path.
        For filename patterns, matches against just the basename.

        Args:
            path_or_filename: File path or filename to check

        Returns:
            True if it matches the pattern
        """
        return matches_pattern(self.file_pattern, path_or_filename)


class ConnectionConfig(BaseModel):
    """
    Database connection configuration.

    Attributes:
        env_var: Name of environment variable containing the connection string
                (e.g., "DATABASE_URL" or "DATABASE_URL_PROJECT_ABC")
    """
    env_var: str = "DATABASE_URL"


class ProjectConfig(BaseModel):
    """
    Complete project configuration.

    A project defines a set of CSV-to-table mappings for a specific
    database/customer. Supports two modes:

    1. **Auto-discovery mode**: Use `defaults` + `table_naming` to process
       all matching files with shared settings. Table names derived from filenames.

    2. **Explicit mode**: Use `tables` list to define each file-to-table mapping.

    Both modes can be combined - explicit `tables` entries override defaults.

    Attributes:
        project: Unique project identifier
        connection: Database connection settings
        sftp: Optional SFTP configuration for remote file pulling
        defaults: Default settings for auto-discovery mode
        table_naming: Rules for transforming filenames to table names
        tables: List of explicit table configurations (override defaults)
        refresh_materialized_views: If True, refresh all materialized views after import

    Example YAML (auto-discovery mode):
        ```yaml
        project: customer_abc
        connection:
          env_var: DATABASE_URL_ABC
        defaults:
          file_pattern: "IxExp*.csv"
          primary_key: HDR_ID
          delimiter: "|"
          encoding: "latin-1"
          skiprows: 1
        table_naming:
          strip_prefix: "IxExp"
          lowercase: true
        sftp:
          host: sftp.customer.com
          remote_path: /exports/daily/
        refresh_materialized_views: true
        ```

    Example YAML (explicit mode):
        ```yaml
        project: customer_abc
        tables:
          - file_pattern: "customers*.csv"
            target_table: customers
            primary_key: customer_id
        refresh_materialized_views: true
        ```
    """
    project: str
    connection: ConnectionConfig = Field(default_factory=ConnectionConfig)
    sftp: Optional[SFTPConfig] = None
    defaults: Optional[DefaultsConfig] = None
    table_naming: TableNamingConfig = Field(default_factory=TableNamingConfig)
    tables: List[TableConfig] = Field(default_factory=list)
    refresh_materialized_views: bool = False
    hooks: Optional[Dict] = Field(
        default=None,
        description="Hook configuration for pre/post import actions"
    )

    def get_effective_hooks(self) -> "HookConfig":
        """
        Get the effective hook configuration, converting legacy settings.

        This method:
        1. Returns the hooks config if explicitly set
        2. Converts refresh_materialized_views=True to equivalent hook
        3. Returns empty HookConfig if neither is set

        The legacy refresh_materialized_views setting is converted to:
            hooks:
              post_import:
                - type: refresh_views
                  on_error: warn

        Returns:
            HookConfig with all effective hooks
        """
        from src.hooks.models import HookAction, HookConfig, OnErrorBehavior

        # Start with explicit hooks config or empty
        if self.hooks:
            config = HookConfig(**self.hooks)
        else:
            config = HookConfig()

        # Convert legacy refresh_materialized_views to hook
        if self.refresh_materialized_views:
            # Check if refresh_views is already in post_import hooks
            has_refresh_hook = any(
                action.type == "refresh_views" for action in config.post_import
            )

            if not has_refresh_hook:
                # Emit deprecation warning
                warnings.warn(
                    "refresh_materialized_views is deprecated. "
                    "Use hooks.post_import with type: refresh_views instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                logger.warning(
                    f"Project '{self.project}' uses deprecated "
                    f"refresh_materialized_views. Migrate to hooks config.",
                    extra={"project": self.project},
                )

                # Add refresh_views hook
                config.post_import.append(
                    HookAction(
                        type="refresh_views",
                        name="refresh_views (legacy)",
                        on_error=OnErrorBehavior.WARN,
                    )
                )

        return config

    def get_table_for_file(self, path_or_filename: str) -> Optional[TableConfig]:
        """
        Find or generate the table configuration for a given path or filename.

        Resolution order:
        1. Check explicit `tables` list for matching pattern
        2. If `defaults` is set and file matches, generate config from defaults

        Args:
            path_or_filename: Filename or relative path to match

        Returns:
            TableConfig if a match is found or generated, None otherwise
        """
        # First, check explicit table configs
        for table_config in self.tables:
            if table_config.matches_file(path_or_filename):
                return table_config

        # If defaults are set and file matches, generate config
        if self.defaults and self.defaults.matches_file(path_or_filename):
            table_name = self.table_naming.transform(path_or_filename)
            return TableConfig(
                file_pattern=extract_filename(path_or_filename),
                target_table=table_name,
                primary_key=self.defaults.primary_key,
                delimiter=self.defaults.delimiter,
                encoding=self.defaults.encoding,
                skiprows=self.defaults.skiprows,
                rebuild_table=self.defaults.rebuild_table,
                datestyle=self.defaults.datestyle,
                db_schema=self.defaults.db_schema,
            )

        return None

    def get_all_matching_tables(self, path_or_filename: str) -> List[TableConfig]:
        """
        Find all table configurations that match a given path or filename.

        Useful for debugging when patterns might overlap.

        Args:
            path_or_filename: Filename or relative path to match

        Returns:
            List of all matching TableConfig objects
        """
        matches = [tc for tc in self.tables if tc.matches_file(path_or_filename)]

        # Also include defaults-generated config if applicable
        if self.defaults and self.defaults.matches_file(path_or_filename):
            # Only add if no explicit match exists
            if not matches:
                table_name = self.table_naming.transform(path_or_filename)
                matches.append(TableConfig(
                    file_pattern=extract_filename(path_or_filename),
                    target_table=table_name,
                    primary_key=self.defaults.primary_key,
                    delimiter=self.defaults.delimiter,
                    encoding=self.defaults.encoding,
                    skiprows=self.defaults.skiprows,
                    rebuild_table=self.defaults.rebuild_table,
                    db_schema=self.defaults.db_schema,
                ))

        return matches

    def should_process_file(self, path_or_filename: str) -> bool:
        """
        Check if a file should be processed by this project.

        Returns True if file matches either:
        - An explicit table config pattern
        - The defaults file_pattern

        Args:
            path_or_filename: Filename or relative path to check

        Returns:
            True if file should be processed
        """
        # Check explicit tables
        for table_config in self.tables:
            if table_config.matches_file(path_or_filename):
                return True

        # Check defaults
        if self.defaults and self.defaults.matches_file(path_or_filename):
            return True

        return False
