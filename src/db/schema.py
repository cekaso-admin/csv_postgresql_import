"""
Database schema operations for table management.

This module provides functions for checking table existence, creating tables
with VARCHAR columns, managing staging tables, and retrieving table metadata.
"""

import logging
import uuid
from typing import List, Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as Connection

from src.db.connection import get_connection_from_url

logger = logging.getLogger(__name__)


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


class TableNotFoundError(Exception):
    """Raised when a table does not exist."""
    pass


class SchemaOperationError(Exception):
    """Raised when a schema operation fails."""
    pass


def table_exists(
    table_name: str,
    schema: str = "public",
    database_url: Optional[str] = None
) -> bool:
    """
    Check if a table exists in the database.

    Args:
        table_name: Name of the table to check
        schema: Database schema name (default: "public")
        database_url: Optional database URL (uses pool if not provided)

    Returns:
        True if table exists, False otherwise

    Raises:
        SchemaOperationError: If the database query fails
    """
    try:
        with _get_conn_manager(database_url) as conn:
            with conn.cursor() as cur:
                query = sql.SQL("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = %s
                        AND table_name = %s
                    )
                """)

                cur.execute(query, (schema, table_name))
                exists = cur.fetchone()[0]

                logger.debug(
                    f"Table existence check: {table_name}",
                    extra={"table": table_name, "exists": exists, "schema": schema}
                )
                return exists

    except psycopg2.Error as e:
        logger.error(f"Failed to check table existence: {e}", exc_info=True)
        raise SchemaOperationError(
            f"Could not check if table '{table_name}' exists: {e}"
        ) from e


def get_table_columns(
    table_name: str,
    schema: str = "public",
    database_url: Optional[str] = None
) -> List[str]:
    """
    Get the list of column names for a table.

    Args:
        table_name: Name of the table
        schema: Database schema name (default: "public")
        database_url: Optional database URL (uses pool if not provided)

    Returns:
        List of column names in order

    Raises:
        TableNotFoundError: If the table does not exist
        SchemaOperationError: If the database query fails
    """
    if not table_exists(table_name, schema, database_url):
        raise TableNotFoundError(f"Table '{table_name}' does not exist")

    try:
        with _get_conn_manager(database_url) as conn:
            with conn.cursor() as cur:
                query = sql.SQL("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s
                    AND table_name = %s
                    ORDER BY ordinal_position
                """)

                cur.execute(query, (schema, table_name))
                columns = [row[0] for row in cur.fetchall()]

                logger.debug(
                    f"Retrieved columns for table: {table_name}",
                    extra={"table": table_name, "column_count": len(columns)}
                )
                return columns

    except psycopg2.Error as e:
        logger.error(f"Failed to get table columns: {e}", exc_info=True)
        raise SchemaOperationError(
            f"Could not retrieve columns for table '{table_name}': {e}"
        ) from e


def create_table_from_columns(
    table_name: str,
    columns: List[str],
    primary_key: Optional[List[str]] = None,
    schema: str = "public",
    if_not_exists: bool = True,
    database_url: Optional[str] = None
) -> None:
    """
    Create a table with all columns as VARCHAR type.

    As per ADR-002, all columns are created as VARCHAR to avoid type mismatch
    errors and provide maximum flexibility. Type casting can be done in views.

    Args:
        table_name: Name of the table to create
        columns: List of column names
        primary_key: Column(s) for PRIMARY KEY constraint (required for upserts)
        schema: Database schema name (default: "public")
        if_not_exists: If True, use IF NOT EXISTS clause (default: True)
        database_url: Optional database URL (uses pool if not provided)

    Raises:
        ValueError: If columns list is empty or contains invalid names
        SchemaOperationError: If table creation fails
    """
    if not columns:
        raise ValueError("Cannot create table with empty column list")

    # Validate column names (basic validation)
    for col in columns:
        if not col or not isinstance(col, str):
            raise ValueError(f"Invalid column name: {col}")

    # Validate primary key columns exist in columns list
    if primary_key:
        for pk_col in primary_key:
            if pk_col not in columns:
                raise ValueError(f"Primary key column '{pk_col}' not in columns list")

    try:
        with _get_conn_manager(database_url) as conn:
            with conn.cursor() as cur:
                # Build column definitions
                column_defs = [
                    sql.SQL("{} VARCHAR").format(sql.Identifier(col))
                    for col in columns
                ]

                # Add PRIMARY KEY constraint if specified
                if primary_key:
                    pk_constraint = sql.SQL("PRIMARY KEY ({})").format(
                        sql.SQL(", ").join([sql.Identifier(pk) for pk in primary_key])
                    )
                    column_defs.append(pk_constraint)

                # Build CREATE TABLE query
                if if_not_exists:
                    query = sql.SQL(
                        "CREATE TABLE IF NOT EXISTS {table} ({columns})"
                    ).format(
                        table=sql.Identifier(schema, table_name),
                        columns=sql.SQL(", ").join(column_defs)
                    )
                else:
                    query = sql.SQL(
                        "CREATE TABLE {table} ({columns})"
                    ).format(
                        table=sql.Identifier(schema, table_name),
                        columns=sql.SQL(", ").join(column_defs)
                    )

                cur.execute(query)
                conn.commit()

                logger.info(
                    f"Created table: {table_name}",
                    extra={
                        "table": table_name,
                        "schema": schema,
                        "column_count": len(columns),
                        "primary_key": primary_key
                    }
                )

    except psycopg2.Error as e:
        logger.error(f"Failed to create table: {e}", exc_info=True)
        raise SchemaOperationError(
            f"Could not create table '{table_name}': {e}"
        ) from e


def create_staging_table(
    target_table: str,
    schema: str = "public",
    database_url: Optional[str] = None
) -> str:
    """
    Create a staging table with the same structure as the target table.

    The staging table name will be: staging_{target_table}_{uuid}

    Args:
        target_table: Name of the target table to clone structure from
        schema: Database schema name (default: "public")
        database_url: Optional database URL (uses pool if not provided)

    Returns:
        Name of the created staging table (without schema)

    Raises:
        TableNotFoundError: If the target table does not exist
        SchemaOperationError: If staging table creation fails
    """
    if not table_exists(target_table, schema, database_url):
        raise TableNotFoundError(
            f"Cannot create staging table: target table '{target_table}' does not exist"
        )

    # Generate unique staging table name
    staging_table_name = f"staging_{target_table}_{uuid.uuid4().hex[:8]}"

    try:
        with _get_conn_manager(database_url) as conn:
            with conn.cursor() as cur:
                # Create staging table with same structure as target
                query = sql.SQL(
                    "CREATE TABLE {staging} (LIKE {target} INCLUDING ALL)"
                ).format(
                    staging=sql.Identifier(schema, staging_table_name),
                    target=sql.Identifier(schema, target_table)
                )

                cur.execute(query)
                conn.commit()

                logger.info(
                    f"Created staging table: {staging_table_name}",
                    extra={
                        "staging_table": staging_table_name,
                        "target_table": target_table,
                        "schema": schema
                    }
                )

                return staging_table_name

    except psycopg2.Error as e:
        logger.error(f"Failed to create staging table: {e}", exc_info=True)
        raise SchemaOperationError(
            f"Could not create staging table for '{target_table}': {e}"
        ) from e


def drop_staging_table(
    staging_table: str,
    schema: str = "public",
    database_url: Optional[str] = None
) -> None:
    """
    Drop a staging table.

    Args:
        staging_table: Name of the staging table to drop
        schema: Database schema name (default: "public")
        database_url: Optional database URL (uses pool if not provided)

    Raises:
        SchemaOperationError: If table drop fails
    """
    try:
        with _get_conn_manager(database_url) as conn:
            with conn.cursor() as cur:
                query = sql.SQL("DROP TABLE IF EXISTS {table}").format(
                    table=sql.Identifier(schema, staging_table)
                )

                cur.execute(query)
                conn.commit()

                logger.info(
                    f"Dropped staging table: {staging_table}",
                    extra={"staging_table": staging_table, "schema": schema}
                )

    except psycopg2.Error as e:
        logger.error(f"Failed to drop staging table: {e}", exc_info=True)
        raise SchemaOperationError(
            f"Could not drop staging table '{staging_table}': {e}"
        ) from e


def truncate_table(
    table_name: str,
    schema: str = "public",
    database_url: Optional[str] = None
) -> None:
    """
    Truncate a table (remove all rows but keep structure).

    Used when rebuild_table option is enabled. This preserves table structure,
    views, and triggers unlike DROP TABLE.

    Args:
        table_name: Name of the table to truncate
        schema: Database schema name (default: "public")
        database_url: Optional database URL (uses pool if not provided)

    Raises:
        TableNotFoundError: If the table does not exist
        SchemaOperationError: If truncate fails
    """
    if not table_exists(table_name, schema, database_url):
        raise TableNotFoundError(f"Cannot truncate: table '{table_name}' does not exist")

    try:
        with _get_conn_manager(database_url) as conn:
            with conn.cursor() as cur:
                query = sql.SQL("TRUNCATE TABLE {table}").format(
                    table=sql.Identifier(schema, table_name)
                )

                cur.execute(query)
                conn.commit()

                logger.info(
                    f"Truncated table: {table_name}",
                    extra={"table": table_name, "schema": schema}
                )

    except psycopg2.Error as e:
        logger.error(f"Failed to truncate table: {e}", exc_info=True)
        raise SchemaOperationError(
            f"Could not truncate table '{table_name}': {e}"
        ) from e
