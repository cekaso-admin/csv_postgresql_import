"""
Management database for storing project configs and job monitoring.

This module handles:
- Connection to the management PostgreSQL database
- Schema creation (projects, jobs, job_files, job_errors tables)
- CRUD operations for projects and jobs
"""

import json
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

logger = logging.getLogger(__name__)

# Connection pool for management database
_pool: Optional[ThreadedConnectionPool] = None


def get_management_pool() -> ThreadedConnectionPool:
    """Get or create the management database connection pool."""
    global _pool
    if _pool is None:
        database_url = os.getenv("MANAGEMENT_DATABASE_URL")
        if not database_url:
            raise ValueError("MANAGEMENT_DATABASE_URL environment variable not set")

        min_conn = int(os.getenv("DB_POOL_MIN_CONN", "1"))
        max_conn = int(os.getenv("DB_POOL_MAX_CONN", "10"))

        # TCP keepalive settings to prevent connection timeouts
        # These help keep connections alive through firewalls and load balancers
        keepalive_kwargs = {
            "keepalives": 1,              # Enable TCP keepalives
            "keepalives_idle": 30,        # Seconds before sending keepalive
            "keepalives_interval": 10,    # Seconds between keepalives
            "keepalives_count": 5,        # Number of keepalives before giving up
        }

        _pool = ThreadedConnectionPool(
            min_conn, max_conn, database_url, **keepalive_kwargs
        )
        logger.info("Management database connection pool created with keepalive settings")

    return _pool


def close_management_pool() -> None:
    """Close the management database connection pool."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None
        logger.info("Management database connection pool closed")


def _is_connection_alive(conn) -> bool:
    """Check if a connection is still alive and usable."""
    if conn.closed:
        return False
    try:
        # Quick test query to verify connection is responsive
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except Exception:
        return False


@contextmanager
def get_management_connection():
    """
    Context manager for getting a connection from the management pool.

    Handles stale connections by validating before use and retrying
    with a fresh connection if the pooled one is dead.
    """
    pool = get_management_pool()
    conn = pool.getconn()
    connection_is_bad = False

    try:
        # Check if connection is still alive
        if not _is_connection_alive(conn):
            logger.warning("Pooled connection is stale, getting fresh connection")
            # Mark for discard and get a new one
            pool.putconn(conn, close=True)
            conn = pool.getconn()

            # If still bad after getting new connection, raise
            if not _is_connection_alive(conn):
                connection_is_bad = True
                raise psycopg2.OperationalError("Unable to establish database connection")

        yield conn

        # Only commit if connection is still good
        if not conn.closed:
            conn.commit()

    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        # Connection-level errors - mark connection as bad
        connection_is_bad = True
        logger.error(f"Database connection error: {e}")
        raise
    except Exception:
        # Other errors - try to rollback if connection is still alive
        if not conn.closed:
            try:
                conn.rollback()
            except Exception as rollback_error:
                logger.warning(f"Rollback failed (connection may be closed): {rollback_error}")
                connection_is_bad = True
        else:
            connection_is_bad = True
        raise
    finally:
        # Return connection to pool, closing it if it's bad
        try:
            pool.putconn(conn, close=connection_is_bad)
        except Exception as e:
            logger.warning(f"Error returning connection to pool: {e}")


# Schema creation SQL
SCHEMA_SQL = """
-- Connections table: reusable database connections
CREATE TABLE IF NOT EXISTS cpi_connections (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    database_url TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Projects table: stores project configurations
CREATE TABLE IF NOT EXISTS cpi_projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    connection_id UUID REFERENCES cpi_connections(id) ON DELETE SET NULL,
    config JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Jobs table: stores import job execution history
CREATE TABLE IF NOT EXISTS cpi_jobs (
    id UUID PRIMARY KEY,
    project_id UUID REFERENCES cpi_projects(id) ON DELETE SET NULL,
    project_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    files_processed INTEGER DEFAULT 0,
    files_failed INTEGER DEFAULT 0,
    total_inserted INTEGER DEFAULT 0,
    total_updated INTEGER DEFAULT 0,
    callback_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Job files table: per-file results within a job
CREATE TABLE IF NOT EXISTS cpi_job_files (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID REFERENCES cpi_jobs(id) ON DELETE CASCADE,
    filename VARCHAR(500) NOT NULL,
    table_name VARCHAR(255),
    inserted INTEGER DEFAULT 0,
    updated INTEGER DEFAULT 0,
    success BOOLEAN DEFAULT FALSE,
    error TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Job errors table: job-level errors
CREATE TABLE IF NOT EXISTS cpi_job_errors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID REFERENCES cpi_jobs(id) ON DELETE CASCADE,
    error_type VARCHAR(100),
    message TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Sources table: reusable SFTP configurations
CREATE TABLE IF NOT EXISTS cpi_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    host VARCHAR(255) NOT NULL,
    port INTEGER DEFAULT 22,
    username VARCHAR(255) NOT NULL,
    password TEXT,
    key_path TEXT,
    remote_path VARCHAR(500) DEFAULT '/',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Schedules table: recurring job configurations
CREATE TABLE IF NOT EXISTS cpi_schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    project_id UUID REFERENCES cpi_projects(id) ON DELETE CASCADE,

    -- Schedule configuration
    schedule_type VARCHAR(20) NOT NULL CHECK (schedule_type IN ('cron', 'interval')),
    cron_expression VARCHAR(100),
    interval_seconds INTEGER CHECK (interval_seconds >= 3600),
    timezone VARCHAR(50) DEFAULT 'UTC',

    -- Execution settings
    enabled BOOLEAN DEFAULT TRUE,
    callback_url TEXT,
    sftp_override JSONB,
    local_files JSONB,

    -- Tracking
    last_run_at TIMESTAMP WITH TIME ZONE,
    next_run_at TIMESTAMP WITH TIME ZONE,
    last_job_id UUID REFERENCES cpi_jobs(id) ON DELETE SET NULL,
    total_runs INTEGER DEFAULT 0,
    successful_runs INTEGER DEFAULT 0,
    failed_runs INTEGER DEFAULT 0,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Add source_id to projects (idempotent with DO block)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'cpi_projects' AND column_name = 'source_id'
    ) THEN
        ALTER TABLE cpi_projects ADD COLUMN source_id UUID REFERENCES cpi_sources(id) ON DELETE SET NULL;
    END IF;
END $$;

-- Add schedule_id to jobs (idempotent with DO block)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'cpi_jobs' AND column_name = 'schedule_id'
    ) THEN
        ALTER TABLE cpi_jobs ADD COLUMN schedule_id UUID REFERENCES cpi_schedules(id) ON DELETE SET NULL;
    END IF;
END $$;

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_cpi_jobs_project_id ON cpi_jobs(project_id);
CREATE INDEX IF NOT EXISTS idx_cpi_jobs_status ON cpi_jobs(status);
CREATE INDEX IF NOT EXISTS idx_cpi_jobs_created_at ON cpi_jobs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_cpi_jobs_schedule_id ON cpi_jobs(schedule_id);
CREATE INDEX IF NOT EXISTS idx_cpi_job_files_job_id ON cpi_job_files(job_id);
CREATE INDEX IF NOT EXISTS idx_cpi_job_errors_job_id ON cpi_job_errors(job_id);
CREATE INDEX IF NOT EXISTS idx_cpi_sources_name ON cpi_sources(name);
CREATE INDEX IF NOT EXISTS idx_cpi_schedules_project_id ON cpi_schedules(project_id);
CREATE INDEX IF NOT EXISTS idx_cpi_schedules_enabled ON cpi_schedules(enabled);
"""


def init_management_schema() -> None:
    """Initialize the management database schema."""
    with get_management_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
    logger.info("Management database schema initialized")


def test_management_connection() -> bool:
    """Test connection to the management database."""
    try:
        with get_management_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return True
    except Exception as e:
        logger.error(f"Management database connection failed: {e}")
        return False


# =============================================================================
# Connection CRUD Operations
# =============================================================================

@dataclass
class ConnectionRecord:
    """Connection record from database."""
    id: str
    name: str
    description: Optional[str]
    database_url: str
    created_at: datetime
    updated_at: datetime


def create_connection(
    name: str,
    database_url: str,
    description: Optional[str] = None,
) -> ConnectionRecord:
    """
    Create a new database connection.

    Args:
        name: Unique connection name
        database_url: PostgreSQL connection string
        description: Optional description

    Returns:
        Created ConnectionRecord

    Raises:
        ValueError: If connection with name already exists
    """
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO cpi_connections (name, description, database_url)
                    VALUES (%s, %s, %s)
                    RETURNING id, name, description, database_url, created_at, updated_at
                    """,
                    (name, description, database_url)
                )
                row = cur.fetchone()
                logger.info(f"Created connection: {name}")
                return ConnectionRecord(
                    id=str(row["id"]),
                    name=row["name"],
                    description=row["description"],
                    database_url=row["database_url"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            except psycopg2.errors.UniqueViolation:
                raise ValueError(f"Connection '{name}' already exists")


def get_connection(connection_id: str) -> Optional[ConnectionRecord]:
    """Get a connection by ID."""
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, description, database_url, created_at, updated_at
                FROM cpi_connections
                WHERE id = %s
                """,
                (connection_id,)
            )
            row = cur.fetchone()
            if row:
                return ConnectionRecord(
                    id=str(row["id"]),
                    name=row["name"],
                    description=row["description"],
                    database_url=row["database_url"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            return None


def get_connection_by_name(name: str) -> Optional[ConnectionRecord]:
    """Get a connection by name."""
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, description, database_url, created_at, updated_at
                FROM cpi_connections
                WHERE name = %s
                """,
                (name,)
            )
            row = cur.fetchone()
            if row:
                return ConnectionRecord(
                    id=str(row["id"]),
                    name=row["name"],
                    description=row["description"],
                    database_url=row["database_url"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            return None


def list_connections() -> List[ConnectionRecord]:
    """List all connections."""
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, description, database_url, created_at, updated_at
                FROM cpi_connections
                ORDER BY name
                """
            )
            rows = cur.fetchall()
            return [
                ConnectionRecord(
                    id=str(row["id"]),
                    name=row["name"],
                    description=row["description"],
                    database_url=row["database_url"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
                for row in rows
            ]


def update_connection(
    connection_id: str,
    name: Optional[str] = None,
    database_url: Optional[str] = None,
    description: Optional[str] = None,
) -> Optional[ConnectionRecord]:
    """
    Update a connection.

    Args:
        connection_id: Connection ID
        name: New name (optional)
        database_url: New database URL (optional)
        description: New description (optional)

    Returns:
        Updated ConnectionRecord or None if not found
    """
    updates = []
    values = []

    if name is not None:
        updates.append("name = %s")
        values.append(name)
    if database_url is not None:
        updates.append("database_url = %s")
        values.append(database_url)
    if description is not None:
        updates.append("description = %s")
        values.append(description)

    if not updates:
        return get_connection(connection_id)

    updates.append("updated_at = NOW()")
    values.append(connection_id)

    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                UPDATE cpi_connections
                SET {', '.join(updates)}
                WHERE id = %s
                RETURNING id, name, description, database_url, created_at, updated_at
                """,
                values
            )
            row = cur.fetchone()
            if row:
                logger.info(f"Updated connection: {row['name']}")
                return ConnectionRecord(
                    id=str(row["id"]),
                    name=row["name"],
                    description=row["description"],
                    database_url=row["database_url"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            return None


def delete_connection(connection_id: str) -> bool:
    """
    Delete a connection.

    Args:
        connection_id: Connection ID

    Returns:
        True if deleted, False if not found
    """
    with get_management_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM cpi_connections WHERE id = %s RETURNING id",
                (connection_id,)
            )
            deleted = cur.fetchone() is not None
            if deleted:
                logger.info(f"Deleted connection: {connection_id}")
            return deleted


def test_target_connection(database_url: str) -> bool:
    """
    Test a target database connection.

    Args:
        database_url: PostgreSQL connection string to test

    Returns:
        True if connection successful, False otherwise
    """
    try:
        import psycopg2
        conn = psycopg2.connect(database_url)
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"Target connection test failed: {e}")
        return False


# =============================================================================
# Project CRUD Operations
# =============================================================================

@dataclass
class ProjectRecord:
    """Project record from database."""
    id: str
    name: str
    connection_id: Optional[str]
    source_id: Optional[str]
    config: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


def create_project(
    name: str,
    config: Dict[str, Any],
    connection_id: Optional[str] = None,
    source_id: Optional[str] = None,
) -> ProjectRecord:
    """
    Create a new project.

    Args:
        name: Unique project name
        config: Project configuration dictionary
        connection_id: Optional connection ID for target database
        source_id: Optional source ID for SFTP configuration

    Returns:
        Created ProjectRecord

    Raises:
        ValueError: If project with name already exists
    """
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO cpi_projects (name, connection_id, source_id, config)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, name, connection_id, source_id, config, created_at, updated_at
                    """,
                    (name, connection_id, source_id, json.dumps(config))
                )
                row = cur.fetchone()
                logger.info(f"Created project: {name}")
                return ProjectRecord(
                    id=str(row["id"]),
                    name=row["name"],
                    connection_id=str(row["connection_id"]) if row["connection_id"] else None,
                    source_id=str(row["source_id"]) if row["source_id"] else None,
                    config=row["config"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            except psycopg2.errors.UniqueViolation:
                raise ValueError(f"Project '{name}' already exists")


def get_project(name: str) -> Optional[ProjectRecord]:
    """
    Get a project by name.

    Args:
        name: Project name

    Returns:
        ProjectRecord or None if not found
    """
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, connection_id, source_id, config, created_at, updated_at
                FROM cpi_projects
                WHERE name = %s
                """,
                (name,)
            )
            row = cur.fetchone()
            if row:
                return ProjectRecord(
                    id=str(row["id"]),
                    name=row["name"],
                    connection_id=str(row["connection_id"]) if row["connection_id"] else None,
                    source_id=str(row["source_id"]) if row["source_id"] else None,
                    config=row["config"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            return None


def get_project_by_id(project_id: str) -> Optional[ProjectRecord]:
    """Get a project by ID."""
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, connection_id, source_id, config, created_at, updated_at
                FROM cpi_projects
                WHERE id = %s
                """,
                (project_id,)
            )
            row = cur.fetchone()
            if row:
                return ProjectRecord(
                    id=str(row["id"]),
                    name=row["name"],
                    connection_id=str(row["connection_id"]) if row["connection_id"] else None,
                    source_id=str(row["source_id"]) if row["source_id"] else None,
                    config=row["config"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            return None


def list_projects() -> List[ProjectRecord]:
    """List all projects."""
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, connection_id, source_id, config, created_at, updated_at
                FROM cpi_projects
                ORDER BY name
                """
            )
            rows = cur.fetchall()
            return [
                ProjectRecord(
                    id=str(row["id"]),
                    name=row["name"],
                    connection_id=str(row["connection_id"]) if row["connection_id"] else None,
                    source_id=str(row["source_id"]) if row["source_id"] else None,
                    config=row["config"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
                for row in rows
            ]


def update_project(
    name: str,
    config: Optional[Dict[str, Any]] = None,
    connection_id: Optional[str] = None,
    source_id: Optional[str] = None,
) -> Optional[ProjectRecord]:
    """
    Update a project's configuration, connection, and/or source.

    Args:
        name: Project name
        config: New configuration dictionary (optional)
        connection_id: New connection ID (optional, use empty string to clear)
        source_id: New source ID (optional, use empty string to clear)

    Returns:
        Updated ProjectRecord or None if not found
    """
    updates = []
    values = []

    if config is not None:
        updates.append("config = %s")
        values.append(json.dumps(config))
    if connection_id is not None:
        updates.append("connection_id = %s")
        values.append(connection_id if connection_id else None)
    if source_id is not None:
        updates.append("source_id = %s")
        values.append(source_id if source_id else None)

    if not updates:
        return get_project(name)

    updates.append("updated_at = NOW()")
    values.append(name)

    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                UPDATE cpi_projects
                SET {', '.join(updates)}
                WHERE name = %s
                RETURNING id, name, connection_id, source_id, config, created_at, updated_at
                """,
                values
            )
            row = cur.fetchone()
            if row:
                logger.info(f"Updated project: {name}")
                return ProjectRecord(
                    id=str(row["id"]),
                    name=row["name"],
                    connection_id=str(row["connection_id"]) if row["connection_id"] else None,
                    source_id=str(row["source_id"]) if row["source_id"] else None,
                    config=row["config"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            return None


def delete_project(name: str) -> bool:
    """
    Delete a project.

    Args:
        name: Project name

    Returns:
        True if deleted, False if not found
    """
    with get_management_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM cpi_projects WHERE name = %s RETURNING id",
                (name,)
            )
            deleted = cur.fetchone() is not None
            if deleted:
                logger.info(f"Deleted project: {name}")
            return deleted


# =============================================================================
# Job CRUD Operations
# =============================================================================

@dataclass
class JobRecord:
    """Job record from database."""
    id: str
    project_id: Optional[str]
    project_name: str
    status: str
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    files_processed: int
    files_failed: int
    total_inserted: int
    total_updated: int
    callback_url: Optional[str]
    schedule_id: Optional[str]
    created_at: datetime


@dataclass
class JobFileRecord:
    """Job file record from database."""
    id: str
    job_id: str
    filename: str
    table_name: Optional[str]
    inserted: int
    updated: int
    success: bool
    error: Optional[str]
    created_at: datetime


@dataclass
class JobErrorRecord:
    """Job error record from database."""
    id: str
    job_id: str
    error_type: Optional[str]
    message: str
    created_at: datetime


def create_job(
    project_name: str,
    job_id: Optional[str] = None,
    callback_url: Optional[str] = None,
    schedule_id: Optional[str] = None,
) -> JobRecord:
    """
    Create a new job record.

    Args:
        project_name: Name of the project
        job_id: Optional custom job ID (generated if not provided)
        callback_url: Optional webhook callback URL
        schedule_id: Optional schedule ID if job is triggered by a schedule

    Returns:
        Created JobRecord
    """
    job_id = job_id or str(uuid4())

    # Get project ID if project exists
    project = get_project(project_name)
    project_id = project.id if project else None

    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO cpi_jobs (id, project_id, project_name, callback_url, schedule_id)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id, project_id, project_name, status, started_at,
                          completed_at, files_processed, files_failed,
                          total_inserted, total_updated, callback_url, schedule_id, created_at
                """,
                (job_id, project_id, project_name, callback_url, schedule_id)
            )
            row = cur.fetchone()
            logger.info(f"Created job: {job_id} for project '{project_name}'")
            return _row_to_job_record(row)


def update_job_status(
    job_id: str,
    status: str,
    started_at: Optional[datetime] = None,
    completed_at: Optional[datetime] = None,
    files_processed: Optional[int] = None,
    files_failed: Optional[int] = None,
    total_inserted: Optional[int] = None,
    total_updated: Optional[int] = None,
) -> Optional[JobRecord]:
    """Update job status and statistics."""
    updates = ["status = %s"]
    values = [status]

    if started_at is not None:
        updates.append("started_at = %s")
        values.append(started_at)
    if completed_at is not None:
        updates.append("completed_at = %s")
        values.append(completed_at)
    if files_processed is not None:
        updates.append("files_processed = %s")
        values.append(files_processed)
    if files_failed is not None:
        updates.append("files_failed = %s")
        values.append(files_failed)
    if total_inserted is not None:
        updates.append("total_inserted = %s")
        values.append(total_inserted)
    if total_updated is not None:
        updates.append("total_updated = %s")
        values.append(total_updated)

    values.append(job_id)

    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                UPDATE cpi_jobs
                SET {', '.join(updates)}
                WHERE id = %s
                RETURNING id, project_id, project_name, status, started_at,
                          completed_at, files_processed, files_failed,
                          total_inserted, total_updated, callback_url, schedule_id, created_at
                """,
                values
            )
            row = cur.fetchone()
            if row:
                logger.debug(f"Updated job {job_id}: status={status}")
                return _row_to_job_record(row)
            return None


def get_job(job_id: str) -> Optional[JobRecord]:
    """Get a job by ID."""
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, project_id, project_name, status, started_at,
                       completed_at, files_processed, files_failed,
                       total_inserted, total_updated, callback_url, schedule_id, created_at
                FROM cpi_jobs
                WHERE id = %s
                """,
                (job_id,)
            )
            row = cur.fetchone()
            if row:
                return _row_to_job_record(row)
            return None


def list_jobs(
    project_name: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> List[JobRecord]:
    """
    List jobs with optional filtering.

    Args:
        project_name: Filter by project name
        status: Filter by status
        limit: Maximum number of jobs to return
        offset: Number of jobs to skip

    Returns:
        List of JobRecords
    """
    conditions = []
    values = []

    if project_name:
        conditions.append("project_name = %s")
        values.append(project_name)
    if status:
        conditions.append("status = %s")
        values.append(status)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    values.extend([limit, offset])

    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT id, project_id, project_name, status, started_at,
                       completed_at, files_processed, files_failed,
                       total_inserted, total_updated, callback_url, schedule_id, created_at
                FROM cpi_jobs
                {where_clause}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                values
            )
            rows = cur.fetchall()
            return [_row_to_job_record(row) for row in rows]


def _row_to_job_record(row: Dict) -> JobRecord:
    """Convert database row to JobRecord."""
    return JobRecord(
        id=str(row["id"]),
        project_id=str(row["project_id"]) if row["project_id"] else None,
        project_name=row["project_name"],
        status=row["status"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
        files_processed=row["files_processed"],
        files_failed=row["files_failed"],
        total_inserted=row["total_inserted"],
        total_updated=row["total_updated"],
        callback_url=row["callback_url"],
        schedule_id=str(row["schedule_id"]) if row["schedule_id"] else None,
        created_at=row["created_at"],
    )


# =============================================================================
# Job File Operations
# =============================================================================

def add_job_file(
    job_id: str,
    filename: str,
    table_name: Optional[str] = None,
    inserted: int = 0,
    updated: int = 0,
    success: bool = False,
    error: Optional[str] = None,
) -> JobFileRecord:
    """Add a file result to a job."""
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO cpi_job_files (job_id, filename, table_name, inserted, updated, success, error)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id, job_id, filename, table_name, inserted, updated, success, error, created_at
                """,
                (job_id, filename, table_name, inserted, updated, success, error)
            )
            row = cur.fetchone()
            return JobFileRecord(
                id=str(row["id"]),
                job_id=str(row["job_id"]),
                filename=row["filename"],
                table_name=row["table_name"],
                inserted=row["inserted"],
                updated=row["updated"],
                success=row["success"],
                error=row["error"],
                created_at=row["created_at"],
            )


def get_job_files(job_id: str) -> List[JobFileRecord]:
    """Get all file results for a job."""
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, job_id, filename, table_name, inserted, updated, success, error, created_at
                FROM cpi_job_files
                WHERE job_id = %s
                ORDER BY created_at
                """,
                (job_id,)
            )
            rows = cur.fetchall()
            return [
                JobFileRecord(
                    id=str(row["id"]),
                    job_id=str(row["job_id"]),
                    filename=row["filename"],
                    table_name=row["table_name"],
                    inserted=row["inserted"],
                    updated=row["updated"],
                    success=row["success"],
                    error=row["error"],
                    created_at=row["created_at"],
                )
                for row in rows
            ]


# =============================================================================
# Job Error Operations
# =============================================================================

def add_job_error(
    job_id: str,
    message: str,
    error_type: Optional[str] = None,
) -> JobErrorRecord:
    """Add an error to a job."""
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO cpi_job_errors (job_id, error_type, message)
                VALUES (%s, %s, %s)
                RETURNING id, job_id, error_type, message, created_at
                """,
                (job_id, error_type, message)
            )
            row = cur.fetchone()
            return JobErrorRecord(
                id=str(row["id"]),
                job_id=str(row["job_id"]),
                error_type=row["error_type"],
                message=row["message"],
                created_at=row["created_at"],
            )


def get_job_errors(job_id: str) -> List[JobErrorRecord]:
    """Get all errors for a job."""
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, job_id, error_type, message, created_at
                FROM cpi_job_errors
                WHERE job_id = %s
                ORDER BY created_at
                """,
                (job_id,)
            )
            rows = cur.fetchall()
            return [
                JobErrorRecord(
                    id=str(row["id"]),
                    job_id=str(row["job_id"]),
                    error_type=row["error_type"],
                    message=row["message"],
                    created_at=row["created_at"],
                )
                for row in rows
            ]


# =============================================================================
# Source CRUD Operations
# =============================================================================

@dataclass
class SourceRecord:
    """Source (SFTP) record from database."""
    id: str
    name: str
    description: Optional[str]
    host: str
    port: int
    username: str
    password: Optional[str]
    key_path: Optional[str]
    remote_path: str
    created_at: datetime
    updated_at: datetime


def create_source(
    name: str,
    host: str,
    username: str,
    port: int = 22,
    password: Optional[str] = None,
    key_path: Optional[str] = None,
    remote_path: str = "/",
    description: Optional[str] = None,
) -> SourceRecord:
    """
    Create a new SFTP source.

    Args:
        name: Unique source name
        host: SFTP server hostname
        username: SFTP username
        port: SFTP port (default: 22)
        password: SFTP password (optional)
        key_path: Path to SSH private key (optional)
        remote_path: Remote directory path (default: "/")
        description: Optional description

    Returns:
        Created SourceRecord

    Raises:
        ValueError: If source with name already exists
    """
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO cpi_sources (name, description, host, port, username, password, key_path, remote_path)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, name, description, host, port, username, password, key_path, remote_path, created_at, updated_at
                    """,
                    (name, description, host, port, username, password, key_path, remote_path)
                )
                row = cur.fetchone()
                logger.info(f"Created source: {name}")
                return SourceRecord(
                    id=str(row["id"]),
                    name=row["name"],
                    description=row["description"],
                    host=row["host"],
                    port=row["port"],
                    username=row["username"],
                    password=row["password"],
                    key_path=row["key_path"],
                    remote_path=row["remote_path"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            except psycopg2.errors.UniqueViolation:
                raise ValueError(f"Source '{name}' already exists")


def get_source(source_id: str) -> Optional[SourceRecord]:
    """Get a source by ID."""
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, description, host, port, username, password, key_path, remote_path, created_at, updated_at
                FROM cpi_sources
                WHERE id = %s
                """,
                (source_id,)
            )
            row = cur.fetchone()
            if row:
                return SourceRecord(
                    id=str(row["id"]),
                    name=row["name"],
                    description=row["description"],
                    host=row["host"],
                    port=row["port"],
                    username=row["username"],
                    password=row["password"],
                    key_path=row["key_path"],
                    remote_path=row["remote_path"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            return None


def get_source_by_name(name: str) -> Optional[SourceRecord]:
    """Get a source by name."""
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, description, host, port, username, password, key_path, remote_path, created_at, updated_at
                FROM cpi_sources
                WHERE name = %s
                """,
                (name,)
            )
            row = cur.fetchone()
            if row:
                return SourceRecord(
                    id=str(row["id"]),
                    name=row["name"],
                    description=row["description"],
                    host=row["host"],
                    port=row["port"],
                    username=row["username"],
                    password=row["password"],
                    key_path=row["key_path"],
                    remote_path=row["remote_path"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            return None


def list_sources() -> List[SourceRecord]:
    """List all sources."""
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, description, host, port, username, password, key_path, remote_path, created_at, updated_at
                FROM cpi_sources
                ORDER BY name
                """
            )
            rows = cur.fetchall()
            return [
                SourceRecord(
                    id=str(row["id"]),
                    name=row["name"],
                    description=row["description"],
                    host=row["host"],
                    port=row["port"],
                    username=row["username"],
                    password=row["password"],
                    key_path=row["key_path"],
                    remote_path=row["remote_path"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
                for row in rows
            ]


def update_source(
    source_id: str,
    name: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    key_path: Optional[str] = None,
    remote_path: Optional[str] = None,
    description: Optional[str] = None,
) -> Optional[SourceRecord]:
    """
    Update a source.

    Args:
        source_id: Source ID
        name: New name (optional)
        host: New host (optional)
        port: New port (optional)
        username: New username (optional)
        password: New password (optional)
        key_path: New key path (optional)
        remote_path: New remote path (optional)
        description: New description (optional)

    Returns:
        Updated SourceRecord or None if not found
    """
    updates = []
    values = []

    if name is not None:
        updates.append("name = %s")
        values.append(name)
    if host is not None:
        updates.append("host = %s")
        values.append(host)
    if port is not None:
        updates.append("port = %s")
        values.append(port)
    if username is not None:
        updates.append("username = %s")
        values.append(username)
    if password is not None:
        updates.append("password = %s")
        values.append(password)
    if key_path is not None:
        updates.append("key_path = %s")
        values.append(key_path)
    if remote_path is not None:
        updates.append("remote_path = %s")
        values.append(remote_path)
    if description is not None:
        updates.append("description = %s")
        values.append(description)

    if not updates:
        return get_source(source_id)

    updates.append("updated_at = NOW()")
    values.append(source_id)

    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                UPDATE cpi_sources
                SET {', '.join(updates)}
                WHERE id = %s
                RETURNING id, name, description, host, port, username, password, key_path, remote_path, created_at, updated_at
                """,
                values
            )
            row = cur.fetchone()
            if row:
                logger.info(f"Updated source: {row['name']}")
                return SourceRecord(
                    id=str(row["id"]),
                    name=row["name"],
                    description=row["description"],
                    host=row["host"],
                    port=row["port"],
                    username=row["username"],
                    password=row["password"],
                    key_path=row["key_path"],
                    remote_path=row["remote_path"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
            return None


def delete_source(source_id: str) -> bool:
    """
    Delete a source.

    Args:
        source_id: Source ID

    Returns:
        True if deleted, False if not found
    """
    with get_management_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM cpi_sources WHERE id = %s RETURNING id",
                (source_id,)
            )
            deleted = cur.fetchone() is not None
            if deleted:
                logger.info(f"Deleted source: {source_id}")
            return deleted


def test_sftp_source(source: SourceRecord) -> tuple[bool, Optional[int], Optional[str]]:
    """
    Test an SFTP source connection.

    Args:
        source: SourceRecord to test

    Returns:
        Tuple of (success, file_count, error_message)
    """
    from src.config.models import SFTPConfig
    from src.sftp import SFTPClient

    try:
        sftp_config = SFTPConfig(
            host=source.host,
            port=source.port,
            username=source.username,
            password=source.password,
            key_path=source.key_path,
            remote_path=source.remote_path,
        )
        with SFTPClient(sftp_config) as sftp:
            files = sftp.list_files()
            return True, len(files), None
    except Exception as e:
        logger.warning(f"SFTP source test failed: {e}")
        return False, None, str(e)
