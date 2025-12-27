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
import re
from pathlib import Path
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator


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

    def transform(self, filename: str) -> str:
        """
        Transform a filename into a table name.

        Args:
            filename: Original filename (e.g., "IxExpKonto.csv")

        Returns:
            Transformed table name (e.g., "konto")
        """
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
        file_pattern: Glob pattern for files to process (e.g., "*.csv", "IxExp*.csv")
        primary_key: Default primary key column(s) for upsert
        delimiter: CSV column separator (default: ",")
        encoding: CSV file encoding (default: "utf-8")
        skiprows: Number of rows to skip before header (default: 0)
        rebuild_table: If True, TRUNCATE tables before import (default: False)
        schema: Database schema name (default: "public")
    """
    model_config = ConfigDict(populate_by_name=True)

    file_pattern: str = "*.csv"
    primary_key: Union[str, List[str]]
    delimiter: str = ","
    encoding: str = "utf-8"
    skiprows: int = 0
    rebuild_table: bool = False
    db_schema: str = Field(default="public", alias="schema")

    @field_validator("primary_key")
    @classmethod
    def normalize_primary_key(cls, v):
        """Ensure primary_key is always a list internally."""
        if isinstance(v, str):
            return [v]
        return v

    def matches_file(self, filename: str) -> bool:
        """Check if a filename matches this default's file_pattern."""
        return fnmatch.fnmatch(filename, self.file_pattern)


class TableConfig(BaseModel):
    """
    Configuration for a single table import mapping.

    Use this for explicit file-to-table mappings, or to override
    defaults for specific files.

    Attributes:
        file_pattern: Glob pattern to match CSV files (e.g., "customers*.csv")
        target_table: PostgreSQL table name to import into
        primary_key: Column(s) for upsert conflict resolution
        column_mapping: Optional mapping of CSV column names to table columns
        rebuild_table: If True, TRUNCATE table before import (default: False)
        delimiter: CSV column separator (default: ",")
        encoding: CSV file encoding (default: "utf-8")
        skiprows: Number of rows to skip before header (default: 0)
        db_schema: Database schema name (default: "public")
    """
    model_config = ConfigDict(populate_by_name=True)

    file_pattern: str
    target_table: str
    primary_key: Union[str, List[str]]
    column_mapping: Optional[Dict[str, str]] = None
    rebuild_table: bool = False
    delimiter: str = ","
    encoding: str = "utf-8"
    skiprows: int = 0
    db_schema: str = Field(default="public", alias="schema")

    @field_validator("primary_key")
    @classmethod
    def normalize_primary_key(cls, v):
        """Ensure primary_key is always a list internally."""
        if isinstance(v, str):
            return [v]
        return v

    def matches_file(self, filename: str) -> bool:
        """
        Check if a filename matches this table's file_pattern.

        Args:
            filename: Name of file to check (not full path)

        Returns:
            True if filename matches the pattern
        """
        return fnmatch.fnmatch(filename, self.file_pattern)


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
        ```

    Example YAML (explicit mode):
        ```yaml
        project: customer_abc
        tables:
          - file_pattern: "customers*.csv"
            target_table: customers
            primary_key: customer_id
        ```
    """
    project: str
    connection: ConnectionConfig = Field(default_factory=ConnectionConfig)
    sftp: Optional[SFTPConfig] = None
    defaults: Optional[DefaultsConfig] = None
    table_naming: TableNamingConfig = Field(default_factory=TableNamingConfig)
    tables: List[TableConfig] = Field(default_factory=list)

    def get_table_for_file(self, filename: str) -> Optional[TableConfig]:
        """
        Find or generate the table configuration for a given filename.

        Resolution order:
        1. Check explicit `tables` list for matching pattern
        2. If `defaults` is set and file matches, generate config from defaults

        Args:
            filename: Name of file to match (not full path)

        Returns:
            TableConfig if a match is found or generated, None otherwise
        """
        # First, check explicit table configs
        for table_config in self.tables:
            if table_config.matches_file(filename):
                return table_config

        # If defaults are set and file matches, generate config
        if self.defaults and self.defaults.matches_file(filename):
            table_name = self.table_naming.transform(filename)
            return TableConfig(
                file_pattern=filename,
                target_table=table_name,
                primary_key=self.defaults.primary_key,
                delimiter=self.defaults.delimiter,
                encoding=self.defaults.encoding,
                skiprows=self.defaults.skiprows,
                rebuild_table=self.defaults.rebuild_table,
                db_schema=self.defaults.db_schema,
            )

        return None

    def get_all_matching_tables(self, filename: str) -> List[TableConfig]:
        """
        Find all table configurations that match a given filename.

        Useful for debugging when patterns might overlap.

        Args:
            filename: Name of file to match

        Returns:
            List of all matching TableConfig objects
        """
        matches = [tc for tc in self.tables if tc.matches_file(filename)]

        # Also include defaults-generated config if applicable
        if self.defaults and self.defaults.matches_file(filename):
            # Only add if no explicit match exists
            if not matches:
                table_name = self.table_naming.transform(filename)
                matches.append(TableConfig(
                    file_pattern=filename,
                    target_table=table_name,
                    primary_key=self.defaults.primary_key,
                    delimiter=self.defaults.delimiter,
                    encoding=self.defaults.encoding,
                    skiprows=self.defaults.skiprows,
                    rebuild_table=self.defaults.rebuild_table,
                    db_schema=self.defaults.db_schema,
                ))

        return matches

    def should_process_file(self, filename: str) -> bool:
        """
        Check if a file should be processed by this project.

        Returns True if file matches either:
        - An explicit table config pattern
        - The defaults file_pattern

        Args:
            filename: Name of file to check

        Returns:
            True if file should be processed
        """
        # Check explicit tables
        for table_config in self.tables:
            if table_config.matches_file(filename):
                return True

        # Check defaults
        if self.defaults and self.defaults.matches_file(filename):
            return True

        return False
