"""
CSV and DBF import functionality using COPY command with staging table for upserts.

This module implements a fast, memory-efficient import strategy:
1. Stream CSV/DBF in chunks to staging table using COPY
2. Upsert from staging to target with ON CONFLICT
3. Clean up staging table

CSV files are read via pandas. DBF files are read via pyogrio (optional dependency).

All operations follow ADR-001 for memory-efficient streaming and ADR-002
for schema handling (VARCHAR columns, no DROP operations).
"""

import csv
import io
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Union

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as Connection

from src.db.connection import get_connection_from_url
from src.utils.columns import sanitize_column_name, deduplicate_columns
from src.db.schema import (
    table_exists,
    get_table_columns,
    create_table_from_columns,
    add_columns_to_table,
    create_staging_table,
    drop_staging_table,
    truncate_table,
)


def _get_conn_manager(database_url: Optional[str] = None):
    """Get the appropriate connection context manager.

    Args:
        database_url: Database URL (required for API operations)

    Raises:
        ValueError: If database_url is not provided
    """
    if not database_url:
        raise ValueError("database_url is required - no fallback to DATABASE_URL env var")
    return get_connection_from_url(database_url)

logger = logging.getLogger(__name__)


@dataclass
class ImportResult:
    """
    Result of a CSV import operation.

    Attributes:
        inserted: Number of rows inserted
        updated: Number of rows updated (via upsert, only when data changed)
        skipped: Number of rows skipped (existing rows with no changes)
        errors: List of error messages encountered
        file_path: Path to the imported file
        table_name: Target table name
        total_rows: Total rows processed (inserted + updated + skipped)
    """
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    errors: List[str] = field(default_factory=list)
    file_path: Optional[str] = None
    table_name: Optional[str] = None

    @property
    def total_rows(self) -> int:
        """Total number of rows successfully processed."""
        return self.inserted + self.updated + self.skipped

    @property
    def has_errors(self) -> bool:
        """Check if any errors occurred during import."""
        return len(self.errors) > 0

    @property
    def success(self) -> bool:
        """Check if import completed without errors."""
        return not self.has_errors


class ImportError(Exception):
    """Raised when CSV import fails."""
    pass


def _get_file_size_mb(file_path: str) -> float:
    """Get file size in megabytes."""
    size_bytes = Path(file_path).stat().st_size
    return size_bytes / (1024 * 1024)


def _apply_column_mapping(
    df: pd.DataFrame,
    column_mapping: Optional[Dict[str, str]]
) -> pd.DataFrame:
    """Apply column name mapping to DataFrame."""
    if not column_mapping:
        return df

    rename_dict = {
        old: new for old, new in column_mapping.items()
        if old in df.columns
    }

    if rename_dict:
        df = df.rename(columns=rename_dict)
        logger.debug(f"Applied column mapping: {rename_dict}")

    return df


def _get_csv_columns(
    file_path: str,
    delimiter: str = ",",
    encoding: str = "utf-8",
    skiprows: int = 0
) -> List[str]:
    """
    Get column names from CSV file header.

    Args:
        file_path: Path to CSV file
        delimiter: Column separator (default: ",")
        encoding: File encoding (default: "utf-8")
        skiprows: Number of rows to skip before header (default: 0)

    Returns:
        List of column names
    """
    try:
        with open(file_path, 'r', encoding=encoding) as f:
            reader = csv.reader(f, delimiter=delimiter)
            for _ in range(skiprows):
                next(reader)
            headers = next(reader)
            return headers
    except Exception as e:
        logger.error(f"Failed to read CSV headers: {e}", exc_info=True)
        raise ImportError(f"Could not read CSV file headers: {e}") from e


_PYOGRIO_AVAILABLE: Optional[bool] = None


def _check_pyogrio_available() -> bool:
    """Check if pyogrio is available for DBF reading."""
    global _PYOGRIO_AVAILABLE
    if _PYOGRIO_AVAILABLE is None:
        try:
            import pyogrio
            _PYOGRIO_AVAILABLE = True
        except ImportError:
            _PYOGRIO_AVAILABLE = False
    return _PYOGRIO_AVAILABLE


def _copy_chunk_to_staging(
    cur,
    staging_table: str,
    columns: List[str],
    chunk: pd.DataFrame,
    schema: str = "public"
) -> int:
    """
    COPY a DataFrame chunk to staging table.

    Returns:
        Number of rows copied
    """
    buffer = io.StringIO()
    chunk.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    copy_query = sql.SQL("COPY {table} ({columns}) FROM STDIN WITH CSV").format(
        table=sql.Identifier(schema, staging_table),
        columns=sql.SQL(", ").join(map(sql.Identifier, columns))
    )

    cur.copy_expert(copy_query, buffer)
    return len(chunk)


def _upsert_from_staging(
    cur,
    target_table: str,
    staging_table: str,
    columns: List[str],
    primary_key: List[str],
    schema: str = "public"
) -> tuple[int, int]:
    """
    Upsert rows from staging table to target table.

    Only updates rows where at least one non-PK column has changed,
    using IS DISTINCT FROM for proper NULL handling. Rows with no
    changes are skipped (not counted in updated).

    Returns:
        Tuple of (inserted_count, updated_count)
    """
    # Identify non-primary key columns for updates
    non_pk_columns = [col for col in columns if col not in primary_key]

    # Build the SET clause for updates
    updates = sql.SQL(", ").join([
        sql.SQL("{} = EXCLUDED.{}").format(
            sql.Identifier(col),
            sql.Identifier(col)
        )
        for col in non_pk_columns
    ])

    # Build WHERE clause with IS DISTINCT FROM for conditional updates
    # Only update if at least one non-PK column has changed
    if non_pk_columns:
        where_conditions = sql.SQL(" OR ").join([
            sql.SQL("{}.{} IS DISTINCT FROM EXCLUDED.{}").format(
                sql.Identifier(target_table),
                sql.Identifier(col),
                sql.Identifier(col)
            )
            for col in non_pk_columns
        ])
        where_clause = sql.SQL(" WHERE ").format() + where_conditions
    else:
        # No non-PK columns: no updates possible, all conflicts become skipped
        where_clause = sql.SQL("")

    upsert_query = sql.SQL("""
        WITH upserted AS (
            INSERT INTO {target_table} ({columns})
            SELECT {columns} FROM {staging_table}
            ON CONFLICT ({pk_columns})
            DO UPDATE SET {updates}{where_clause}
            RETURNING xmax
        )
        SELECT
            COUNT(*) FILTER (WHERE xmax = 0) as inserted,
            COUNT(*) FILTER (WHERE xmax != 0) as updated
        FROM upserted
    """).format(
        target_table=sql.Identifier(schema, target_table),
        staging_table=sql.Identifier(schema, staging_table),
        columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
        pk_columns=sql.SQL(", ").join(map(sql.Identifier, primary_key)),
        updates=updates,
        where_clause=where_clause
    )

    cur.execute(upsert_query)
    result = cur.fetchone()
    return (result[0], result[1])


def _import_chunks(
    chunks: Iterable[pd.DataFrame],
    table_name: str,
    primary_key: List[str],
    final_columns: List[str],
    column_mapping: Optional[Dict[str, str]] = None,
    rebuild_table: bool = False,
    schema: str = "public",
    datestyle: Optional[str] = None,
    database_url: Optional[str] = None,
    file_path: Optional[str] = None,
) -> ImportResult:
    """Run the shared staging/upsert pipeline for an iterable of DataFrame chunks.

    This is the internal engine used by both import_csv() and import_dbf().
    It handles table creation, staging, chunk streaming, upsert, and cleanup.
    """
    result = ImportResult(file_path=file_path, table_name=table_name)
    staging_table = None

    try:
        # Check if table exists, create if not
        if not table_exists(table_name, schema, database_url):
            logger.info(f"Table {table_name} does not exist, creating...")
            create_table_from_columns(table_name, final_columns, primary_key, schema, database_url=database_url)
        else:
            # Table exists - check for missing columns and add them
            added_columns = add_columns_to_table(table_name, final_columns, schema, database_url)
            if added_columns:
                logger.info(f"Added {len(added_columns)} new columns to existing table: {added_columns}")

        # Validate primary key columns exist
        table_columns = get_table_columns(table_name, schema, database_url)
        for pk_col in primary_key:
            if pk_col not in table_columns:
                raise ImportError(
                    f"Primary key column '{pk_col}' not found in table. "
                    f"Available columns: {table_columns}"
                )

        # Truncate if rebuild requested
        if rebuild_table:
            logger.info(f"Truncating table {table_name} (rebuild_table=True)")
            truncate_table(table_name, schema, database_url)

        # Create staging table
        staging_table = create_staging_table(table_name, schema, database_url)
        logger.info(f"Created staging table: {staging_table}")

        with _get_conn_manager(database_url) as conn:
            with conn.cursor() as cur:
                # Set datestyle if specified (e.g., "DMY" for European, "MDY" for US)
                if datestyle:
                    valid_datestyles = {"DMY", "MDY", "YMD"}
                    safe_datestyle = datestyle.upper()
                    if safe_datestyle not in valid_datestyles:
                        raise ImportError(
                            f"Invalid datestyle '{datestyle}', "
                            f"must be one of {valid_datestyles}"
                        )
                    cur.execute(f"SET datestyle = 'ISO, {safe_datestyle}'")
                    logger.debug(f"Set datestyle to 'ISO, {safe_datestyle}'")

                # Stream chunks to staging table
                total_rows = 0
                chunk_num = 0

                for chunk in chunks:
                    chunk_num += 1

                    # Apply column mapping
                    chunk = _apply_column_mapping(chunk, column_mapping)
                    columns = chunk.columns.tolist()

                    # COPY chunk to staging
                    rows_copied = _copy_chunk_to_staging(
                        cur, staging_table, columns, chunk, schema
                    )
                    total_rows += rows_copied

                    logger.debug(f"Chunk {chunk_num}: copied {rows_copied} rows to staging")

                conn.commit()
                logger.info(f"Copied {total_rows} rows to staging table in {chunk_num} chunks")

                # Upsert from staging to target
                inserted, updated = _upsert_from_staging(
                    cur, table_name, staging_table, final_columns, primary_key, schema
                )
                conn.commit()

                result.inserted = inserted
                result.updated = updated
                # Calculate skipped: rows that existed but had no changes
                # Use max(0, ...) as a guard against any edge cases
                result.skipped = max(0, total_rows - inserted - updated)

        logger.info(
            f"Import completed: {result.inserted} inserted, {result.updated} updated, "
            f"{result.skipped} skipped (unchanged)"
        )

    except Exception as e:
        error_msg = f"Import failed: {e}"
        logger.error(error_msg, exc_info=True)
        result.errors.append(error_msg)

    finally:
        # Clean up staging table
        if staging_table:
            try:
                drop_staging_table(staging_table, schema, database_url)
                logger.debug(f"Dropped staging table: {staging_table}")
            except Exception as e:
                logger.warning(f"Failed to drop staging table: {e}")

    return result


def import_csv(
    file_path: str,
    table_name: str,
    primary_key: Union[str, List[str]],
    column_mapping: Optional[Dict[str, str]] = None,
    rebuild_table: bool = False,
    schema: str = "public",
    chunk_size: Optional[int] = None,
    delimiter: str = ",",
    encoding: str = "utf-8",
    skiprows: int = 0,
    datestyle: Optional[str] = None,
    database_url: Optional[str] = None
) -> ImportResult:
    """
    Import CSV file into PostgreSQL table using COPY with staging table.

    Uses a fast, memory-efficient strategy:
    1. Stream CSV in chunks to staging table using COPY command
    2. Upsert from staging to target with ON CONFLICT DO UPDATE
    3. Clean up staging table

    If the target table doesn't exist, it will be created with all VARCHAR columns.
    If rebuild_table is True, the table will be truncated before import.

    Args:
        file_path: Path to CSV file to import
        table_name: Target PostgreSQL table name
        primary_key: Column name(s) for upsert conflict resolution.
                    Can be a single string or list of strings for composite keys.
        column_mapping: Optional dictionary mapping CSV column names to table
                       column names (e.g., {"Kunde Nr.": "customer_id"})
        rebuild_table: If True, TRUNCATE table before import (default: False)
        schema: Database schema name (default: "public")
        chunk_size: Rows per chunk for streaming (default: from env or 10000)
        delimiter: CSV column separator (default: ",")
        encoding: CSV file encoding (default: "utf-8", use "latin-1" for Windows)
        skiprows: Number of rows to skip before header (default: 0)
        datestyle: PostgreSQL datestyle for parsing dates (default: None = use DB default).
                  Use "DMY" for European dates (DD.MM.YYYY), "MDY" for US dates (MM/DD/YYYY).
        database_url: Optional database URL. If provided, uses direct connection
                     instead of the connection pool. Useful for importing to
                     different target databases.

    Returns:
        ImportResult object with statistics (inserted, updated, errors)

    Raises:
        ImportError: If file doesn't exist or import fails critically
        ValueError: If primary_key is invalid

    Example:
        # Simple import
        result = import_csv("customers.csv", "customers", primary_key="id")

        # Import with custom CSV format
        result = import_csv(
            "data.csv",
            "data_table",
            primary_key="id",
            delimiter="|",
            encoding="latin-1",
            skiprows=1
        )
    """
    # Validate inputs
    if not Path(file_path).exists():
        raise ImportError(f"CSV file not found: {file_path}")

    if not primary_key:
        raise ValueError("primary_key is required for upsert operations")

    # Get configuration
    if chunk_size is None:
        chunk_size = int(os.getenv("CSV_CHUNK_SIZE", "10000"))

    # Normalize primary key to list
    pk_list = [primary_key] if isinstance(primary_key, str) else list(primary_key)

    file_size_mb = _get_file_size_mb(file_path)
    logger.info(
        f"Starting CSV import: {file_path} ({file_size_mb:.2f}MB) -> {table_name}"
    )

    # Get CSV columns
    csv_columns = _get_csv_columns(file_path, delimiter, encoding, skiprows)

    # Apply column mapping to determine final column names
    if column_mapping:
        final_columns = [column_mapping.get(col, col) for col in csv_columns]
    else:
        final_columns = csv_columns

    # Create chunk iterator from pandas
    chunks = pd.read_csv(
        file_path,
        chunksize=chunk_size,
        sep=delimiter,
        encoding=encoding,
        skiprows=skiprows,
        dtype=str,
    )

    return _import_chunks(
        chunks=chunks,
        table_name=table_name,
        primary_key=pk_list,
        final_columns=final_columns,
        column_mapping=column_mapping,
        rebuild_table=rebuild_table,
        schema=schema,
        datestyle=datestyle,
        database_url=database_url,
        file_path=file_path,
    )


def import_dbf(
    file_path: str,
    table_name: str,
    primary_key: Union[str, List[str]],
    column_mapping: Optional[Dict[str, str]] = None,
    rebuild_table: bool = False,
    schema: str = "public",
    chunk_size: Optional[int] = None,
    encoding: Optional[str] = None,
    datestyle: Optional[str] = None,
    database_url: Optional[str] = None,
) -> ImportResult:
    """
    Import DBF file directly into PostgreSQL using pyogrio.

    Uses pyogrio (C-backed GDAL bindings) to read DBF files into DataFrames
    in chunks, then feeds them to the staging/upsert pipeline. Eliminates
    the intermediate CSV conversion for ~10-20x performance improvement.

    Args:
        file_path: Path to DBF file
        table_name: Target PostgreSQL table name
        primary_key: Column name(s) for upsert conflict resolution
        column_mapping: Optional mapping of column names to table columns
        rebuild_table: If True, TRUNCATE table before import
        schema: Database schema name (default: "public")
        chunk_size: Rows per chunk for COPY (default: from env or 10000)
        encoding: DBF encoding (e.g., "cp850", "latin-1"). Passed to pyogrio/GDAL.
        datestyle: PostgreSQL datestyle for date parsing
        database_url: Database connection URL

    Returns:
        ImportResult with insert/update/skip counts

    Raises:
        ImportError: If file doesn't exist or pyogrio not available
        ValueError: If primary_key is invalid
    """
    if not _check_pyogrio_available():
        raise ImportError(
            "pyogrio package not installed. Install GDAL and pyogrio: "
            "apt install gdal-bin libgdal-dev && pip install pyogrio"
        )

    import pyogrio

    if not Path(file_path).exists():
        raise ImportError(f"DBF file not found: {file_path}")

    if not primary_key:
        raise ValueError("primary_key is required for upsert operations")

    if chunk_size is None:
        chunk_size = int(os.getenv("DBF_CHUNK_SIZE", os.getenv("CSV_CHUNK_SIZE", "10000")))

    pk_list = [primary_key] if isinstance(primary_key, str) else list(primary_key)

    file_size_mb = _get_file_size_mb(file_path)
    logger.info(f"Starting DBF import: {file_path} ({file_size_mb:.2f}MB) -> {table_name}")

    # Build pyogrio read kwargs
    read_kwargs = {"read_geometry": False}
    if encoding:
        read_kwargs["encoding"] = encoding

    # Get file info for logging
    info = pyogrio.read_info(file_path)
    total_rows = info.get("features") or 0

    # Read full DBF file at once (C-level, fast)
    # Then chunk with DataFrame.iloc for COPY operations
    df = pyogrio.read_dataframe(file_path, **read_kwargs)

    if len(df) == 0:
        logger.info(f"DBF file is empty: {file_path}")
        return ImportResult(file_path=file_path, table_name=table_name)

    raw_columns = df.columns.tolist()

    # Sanitize column names for database compatibility
    sanitized = [sanitize_column_name(c) for c in raw_columns]
    sanitized = deduplicate_columns(sanitized)

    # Column mapping uses raw DBF names (e.g., {"Customer Nr.": "customer_id"})
    # to match import_csv behavior where mapping keys are original CSV headers
    if column_mapping:
        final_columns = [
            column_mapping.get(raw, sanitized[i])
            for i, raw in enumerate(raw_columns)
        ]
    else:
        final_columns = sanitized

    logger.info(f"Read {len(df):,} rows, {len(raw_columns)} columns from DBF")

    # Rename columns to sanitized names
    df.columns = sanitized
    # Match CSV pipeline: all VARCHAR, empty string for NULLs
    df = df.fillna("").astype(str)

    # Generator that yields DataFrame chunks via iloc (zero-copy slicing)
    def _chunk_generator():
        for start in range(0, len(df), chunk_size):
            yield df.iloc[start:start + chunk_size]

    return _import_chunks(
        chunks=_chunk_generator(),
        table_name=table_name,
        primary_key=pk_list,
        final_columns=final_columns,
        column_mapping=column_mapping,
        rebuild_table=rebuild_table,
        schema=schema,
        datestyle=datestyle,
        database_url=database_url,
        file_path=file_path,
    )
