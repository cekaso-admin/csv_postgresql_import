"""
Database operations module for CSV PostgreSQL import.

This module provides:
- Connection management for project databases
- Schema operations and CSV import functionality
- Management database for project configs and job monitoring
"""

from src.db.connection import get_connection, get_connection_from_url, close_pool
from src.db.schema import (
    table_exists,
    get_table_columns,
    create_table_from_columns,
    create_staging_table,
    drop_staging_table,
    truncate_table,
)
from src.db.importer import import_csv, ImportResult
from src.db.management import (
    init_management_schema,
    test_management_connection,
    get_management_connection,
    close_management_pool,
    # Connection operations
    create_connection,
    get_connection,
    get_connection_by_name,
    list_connections,
    update_connection,
    delete_connection,
    test_target_connection,
    # Project operations
    create_project,
    get_project,
    list_projects,
    update_project,
    delete_project,
    # Job operations
    create_job,
    get_job,
    list_jobs,
    update_job_status,
    add_job_file,
    get_job_files,
    add_job_error,
    get_job_errors,
    # Records
    ConnectionRecord,
    ProjectRecord,
    JobRecord,
    JobFileRecord,
    JobErrorRecord,
)

__all__ = [
    # Project DB connection (from connection.py)
    "get_connection_from_url",
    "close_pool",
    # Schema operations
    "table_exists",
    "get_table_columns",
    "create_table_from_columns",
    "create_staging_table",
    "drop_staging_table",
    "truncate_table",
    # CSV import
    "import_csv",
    "ImportResult",
    # Management DB
    "init_management_schema",
    "test_management_connection",
    "get_management_connection",
    "close_management_pool",
    # Connection CRUD
    "create_connection",
    "get_connection",
    "get_connection_by_name",
    "list_connections",
    "update_connection",
    "delete_connection",
    "test_target_connection",
    # Project CRUD
    "create_project",
    "get_project",
    "list_projects",
    "update_project",
    "delete_project",
    # Job CRUD
    "create_job",
    "get_job",
    "list_jobs",
    "update_job_status",
    "add_job_file",
    "get_job_files",
    "add_job_error",
    "get_job_errors",
    # Records
    "ConnectionRecord",
    "ProjectRecord",
    "JobRecord",
    "JobFileRecord",
    "JobErrorRecord",
]
