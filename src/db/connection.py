"""
Database connection management with connection pooling.

This module provides thread-safe connection pooling using psycopg2's
ThreadedConnectionPool and context managers for safe resource handling.
"""

import logging
import os
from contextlib import contextmanager
from typing import Generator, Optional

import psycopg2
from psycopg2 import pool
from psycopg2.extensions import connection as Connection
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# Global connection pool
_connection_pool: Optional[pool.ThreadedConnectionPool] = None


class DatabaseConnectionError(Exception):
    """Raised when database connection fails."""
    pass


class PoolExhaustedError(Exception):
    """Raised when connection pool has no available connections."""
    pass


def _initialize_pool() -> pool.ThreadedConnectionPool:
    """
    Initialize the connection pool with settings from environment variables.

    Returns:
        ThreadedConnectionPool configured with environment settings

    Raises:
        DatabaseConnectionError: If DATABASE_URL is not set or connection fails
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise DatabaseConnectionError(
            "DATABASE_URL environment variable is not set. "
            "Please set it in your .env file."
        )

    # Get pool configuration from environment
    min_conn = int(os.getenv("DB_POOL_MIN_CONN", "1"))
    max_conn = int(os.getenv("DB_POOL_MAX_CONN", "10"))

    try:
        logger.info(
            "Initializing connection pool",
            extra={"min_connections": min_conn, "max_connections": max_conn}
        )

        connection_pool = pool.ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            dsn=database_url
        )

        logger.info("Connection pool initialized successfully")
        return connection_pool

    except psycopg2.OperationalError as e:
        logger.error(f"Failed to initialize connection pool: {e}", exc_info=True)
        raise DatabaseConnectionError(
            f"Could not connect to database: {e}"
        ) from e


def _get_pool() -> pool.ThreadedConnectionPool:
    """
    Get or create the global connection pool.

    Returns:
        The global ThreadedConnectionPool instance

    Raises:
        DatabaseConnectionError: If pool initialization fails
    """
    global _connection_pool

    if _connection_pool is None:
        _connection_pool = _initialize_pool()

    return _connection_pool


@contextmanager
def get_connection() -> Generator[Connection, None, None]:
    """
    Context manager for obtaining a database connection from the pool.

    Automatically returns the connection to the pool when the context exits.
    Handles both successful execution and exceptions gracefully.

    Usage:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM users")
                results = cur.fetchall()

    Yields:
        psycopg2 connection object

    Raises:
        DatabaseConnectionError: If connection cannot be obtained
        PoolExhaustedError: If pool has no available connections
    """
    connection_pool = _get_pool()
    conn = None

    try:
        logger.debug("Acquiring connection from pool")
        conn = connection_pool.getconn()

        if conn is None:
            raise PoolExhaustedError(
                "Connection pool exhausted. No connections available."
            )

        logger.debug("Connection acquired successfully")
        yield conn

    except psycopg2.Error as e:
        logger.error(f"Database error occurred: {e}", exc_info=True)
        if conn:
            conn.rollback()
            logger.debug("Transaction rolled back due to error")
        raise DatabaseConnectionError(f"Database operation failed: {e}") from e

    except Exception as e:
        logger.error(f"Unexpected error with connection: {e}", exc_info=True)
        if conn:
            conn.rollback()
            logger.debug("Transaction rolled back due to unexpected error")
        raise

    finally:
        if conn:
            connection_pool.putconn(conn)
            logger.debug("Connection returned to pool")


def close_pool() -> None:
    """
    Close all connections in the pool and clean up resources.

    Should be called when the application is shutting down.
    After calling this, the pool will be reinitialized on the next
    get_connection() call.
    """
    global _connection_pool

    if _connection_pool is not None:
        logger.info("Closing connection pool")
        _connection_pool.closeall()
        _connection_pool = None
        logger.info("Connection pool closed successfully")
    else:
        logger.debug("No connection pool to close")


def test_connection() -> bool:
    """
    Test the database connection by executing a simple query.

    Returns:
        True if connection is successful, False otherwise
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                logger.info("Connection test successful")
                return result == (1,)
    except Exception as e:
        logger.error(f"Connection test failed: {e}", exc_info=True)
        return False


@contextmanager
def get_connection_from_url(database_url: str) -> Generator[Connection, None, None]:
    """
    Context manager for creating a direct database connection from a URL.

    Unlike get_connection() which uses a pool, this creates a fresh connection
    for each call. Useful when connecting to different target databases.

    Args:
        database_url: PostgreSQL connection string

    Usage:
        with get_connection_from_url("postgresql://...") as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM users")

    Yields:
        psycopg2 connection object

    Raises:
        DatabaseConnectionError: If connection cannot be established
    """
    conn = None
    try:
        logger.debug("Creating direct connection from URL")
        conn = psycopg2.connect(database_url)
        logger.debug("Direct connection established")
        yield conn

    except psycopg2.Error as e:
        logger.error(f"Database error occurred: {e}", exc_info=True)
        if conn:
            conn.rollback()
        raise DatabaseConnectionError(f"Database operation failed: {e}") from e

    except Exception as e:
        logger.error(f"Unexpected error with connection: {e}", exc_info=True)
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            conn.close()
            logger.debug("Direct connection closed")
