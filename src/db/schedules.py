"""
Schedule CRUD operations for the management database.

This module handles:
- CRUD operations for recurring import schedules
- Schedule execution tracking and statistics
- Helper functions for scheduler service
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from src.db.management import get_management_connection

logger = logging.getLogger(__name__)


# =============================================================================
# Schedule Data Models
# =============================================================================

@dataclass
class ScheduleRecord:
    """Schedule record from database."""
    id: str
    name: str
    project_id: str
    schedule_type: str
    cron_expression: Optional[str]
    interval_seconds: Optional[int]
    timezone: str
    enabled: bool
    callback_url: Optional[str]
    sftp_override: Optional[Dict[str, Any]]
    local_files: Optional[List[str]]
    last_run_at: Optional[datetime]
    next_run_at: Optional[datetime]
    last_job_id: Optional[str]
    total_runs: int
    successful_runs: int
    failed_runs: int
    created_at: datetime
    updated_at: datetime


# =============================================================================
# Schedule CRUD Operations
# =============================================================================

def create_schedule(
    name: str,
    project_id: str,
    schedule_type: str,
    cron_expression: Optional[str] = None,
    interval_seconds: Optional[int] = None,
    timezone: str = "UTC",
    enabled: bool = True,
    callback_url: Optional[str] = None,
    sftp_override: Optional[Dict[str, Any]] = None,
    local_files: Optional[List[str]] = None,
) -> ScheduleRecord:
    """
    Create a new schedule.

    Args:
        name: Unique schedule name
        project_id: Project UUID to schedule
        schedule_type: Either 'cron' or 'interval'
        cron_expression: Cron expression (required if schedule_type='cron')
        interval_seconds: Interval in seconds (required if schedule_type='interval', min 3600)
        timezone: Timezone for schedule (default: UTC)
        enabled: Whether schedule is active (default: True)
        callback_url: Optional webhook callback URL
        sftp_override: Optional SFTP configuration override
        local_files: Optional list of local file paths

    Returns:
        Created ScheduleRecord

    Raises:
        ValueError: If schedule with name already exists or validation fails
    """
    # Validation
    if schedule_type not in ('cron', 'interval'):
        raise ValueError("schedule_type must be 'cron' or 'interval'")

    if schedule_type == 'cron' and not cron_expression:
        raise ValueError("cron_expression required when schedule_type='cron'")

    if schedule_type == 'interval':
        if not interval_seconds:
            raise ValueError("interval_seconds required when schedule_type='interval'")
        if interval_seconds < 3600:
            raise ValueError("interval_seconds must be at least 3600 (1 hour)")

    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO cpi_schedules (
                        name, project_id, schedule_type, cron_expression,
                        interval_seconds, timezone, enabled, callback_url,
                        sftp_override, local_files
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id, name, project_id, schedule_type, cron_expression,
                              interval_seconds, timezone, enabled, callback_url,
                              sftp_override, local_files, last_run_at, next_run_at,
                              last_job_id, total_runs, successful_runs, failed_runs,
                              created_at, updated_at
                    """,
                    (
                        name,
                        project_id,
                        schedule_type,
                        cron_expression,
                        interval_seconds,
                        timezone,
                        enabled,
                        callback_url,
                        json.dumps(sftp_override) if sftp_override else None,
                        json.dumps(local_files) if local_files else None,
                    )
                )
                row = cur.fetchone()
                logger.info(f"Created schedule: {name}")
                return _row_to_schedule_record(row)
            except psycopg2.errors.UniqueViolation:
                raise ValueError(f"Schedule '{name}' already exists")


def get_schedule(schedule_id: str) -> Optional[ScheduleRecord]:
    """
    Get a schedule by ID.

    Args:
        schedule_id: Schedule UUID

    Returns:
        ScheduleRecord or None if not found
    """
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, project_id, schedule_type, cron_expression,
                       interval_seconds, timezone, enabled, callback_url,
                       sftp_override, local_files, last_run_at, next_run_at,
                       last_job_id, total_runs, successful_runs, failed_runs,
                       created_at, updated_at
                FROM cpi_schedules
                WHERE id = %s
                """,
                (schedule_id,)
            )
            row = cur.fetchone()
            if row:
                return _row_to_schedule_record(row)
            return None


def get_schedule_by_name(name: str) -> Optional[ScheduleRecord]:
    """
    Get a schedule by name.

    Args:
        name: Schedule name

    Returns:
        ScheduleRecord or None if not found
    """
    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, project_id, schedule_type, cron_expression,
                       interval_seconds, timezone, enabled, callback_url,
                       sftp_override, local_files, last_run_at, next_run_at,
                       last_job_id, total_runs, successful_runs, failed_runs,
                       created_at, updated_at
                FROM cpi_schedules
                WHERE name = %s
                """,
                (name,)
            )
            row = cur.fetchone()
            if row:
                return _row_to_schedule_record(row)
            return None


def list_schedules(
    project_id: Optional[str] = None,
    enabled: Optional[bool] = None,
    limit: int = 50,
    offset: int = 0,
) -> List[ScheduleRecord]:
    """
    List schedules with optional filtering.

    Args:
        project_id: Filter by project UUID
        enabled: Filter by enabled status
        limit: Maximum number of schedules to return (default: 50)
        offset: Number of schedules to skip (default: 0)

    Returns:
        List of ScheduleRecords
    """
    conditions = []
    values = []

    if project_id is not None:
        conditions.append("project_id = %s")
        values.append(project_id)
    if enabled is not None:
        conditions.append("enabled = %s")
        values.append(enabled)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    values.extend([limit, offset])

    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT id, name, project_id, schedule_type, cron_expression,
                       interval_seconds, timezone, enabled, callback_url,
                       sftp_override, local_files, last_run_at, next_run_at,
                       last_job_id, total_runs, successful_runs, failed_runs,
                       created_at, updated_at
                FROM cpi_schedules
                {where_clause}
                ORDER BY name
                LIMIT %s OFFSET %s
                """,
                values
            )
            rows = cur.fetchall()
            return [_row_to_schedule_record(row) for row in rows]


def update_schedule(
    schedule_id: str,
    name: Optional[str] = None,
    schedule_type: Optional[str] = None,
    cron_expression: Optional[str] = None,
    interval_seconds: Optional[int] = None,
    timezone: Optional[str] = None,
    enabled: Optional[bool] = None,
    callback_url: Optional[str] = None,
    sftp_override: Optional[Dict[str, Any]] = None,
    local_files: Optional[List[str]] = None,
) -> Optional[ScheduleRecord]:
    """
    Update a schedule.

    Args:
        schedule_id: Schedule UUID
        name: New name (optional)
        schedule_type: New schedule type (optional)
        cron_expression: New cron expression (optional)
        interval_seconds: New interval (optional, min 3600)
        timezone: New timezone (optional)
        enabled: New enabled status (optional)
        callback_url: New callback URL (optional)
        sftp_override: New SFTP override (optional)
        local_files: New local files list (optional)

    Returns:
        Updated ScheduleRecord or None if not found

    Raises:
        ValueError: If validation fails
    """
    updates = []
    values = []

    if name is not None:
        updates.append("name = %s")
        values.append(name)
    if schedule_type is not None:
        if schedule_type not in ('cron', 'interval'):
            raise ValueError("schedule_type must be 'cron' or 'interval'")
        updates.append("schedule_type = %s")
        values.append(schedule_type)
    if cron_expression is not None:
        updates.append("cron_expression = %s")
        values.append(cron_expression)
    if interval_seconds is not None:
        if interval_seconds < 3600:
            raise ValueError("interval_seconds must be at least 3600 (1 hour)")
        updates.append("interval_seconds = %s")
        values.append(interval_seconds)
    if timezone is not None:
        updates.append("timezone = %s")
        values.append(timezone)
    if enabled is not None:
        updates.append("enabled = %s")
        values.append(enabled)
    if callback_url is not None:
        updates.append("callback_url = %s")
        values.append(callback_url)
    if sftp_override is not None:
        updates.append("sftp_override = %s")
        values.append(json.dumps(sftp_override))
    if local_files is not None:
        updates.append("local_files = %s")
        values.append(json.dumps(local_files))

    if not updates:
        return get_schedule(schedule_id)

    updates.append("updated_at = NOW()")
    values.append(schedule_id)

    with get_management_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                UPDATE cpi_schedules
                SET {', '.join(updates)}
                WHERE id = %s
                RETURNING id, name, project_id, schedule_type, cron_expression,
                          interval_seconds, timezone, enabled, callback_url,
                          sftp_override, local_files, last_run_at, next_run_at,
                          last_job_id, total_runs, successful_runs, failed_runs,
                          created_at, updated_at
                """,
                values
            )
            row = cur.fetchone()
            if row:
                logger.info(f"Updated schedule: {row['name']}")
                return _row_to_schedule_record(row)
            return None


def delete_schedule(schedule_id: str) -> bool:
    """
    Delete a schedule.

    Args:
        schedule_id: Schedule UUID

    Returns:
        True if deleted, False if not found
    """
    with get_management_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM cpi_schedules WHERE id = %s RETURNING id",
                (schedule_id,)
            )
            deleted = cur.fetchone() is not None
            if deleted:
                logger.info(f"Deleted schedule: {schedule_id}")
            return deleted


def update_schedule_execution(
    schedule_id: str,
    job_id: Optional[str],
    success: bool,
    next_run_at: Optional[datetime] = None,
) -> None:
    """
    Update schedule execution statistics after a run.

    Args:
        schedule_id: Schedule UUID
        job_id: Job UUID that was created (None if job creation failed)
        success: Whether the job completed successfully
        next_run_at: Next scheduled run time (optional)
    """
    with get_management_connection() as conn:
        with conn.cursor() as cur:
            if next_run_at:
                cur.execute(
                    """
                    UPDATE cpi_schedules
                    SET last_run_at = NOW(),
                        last_job_id = %s,
                        next_run_at = %s,
                        total_runs = total_runs + 1,
                        successful_runs = CASE WHEN %s THEN successful_runs + 1 ELSE successful_runs END,
                        failed_runs = CASE WHEN %s THEN failed_runs ELSE failed_runs + 1 END,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (job_id, next_run_at, success, success, schedule_id)
                )
            else:
                cur.execute(
                    """
                    UPDATE cpi_schedules
                    SET last_run_at = NOW(),
                        last_job_id = %s,
                        total_runs = total_runs + 1,
                        successful_runs = CASE WHEN %s THEN successful_runs + 1 ELSE successful_runs END,
                        failed_runs = CASE WHEN %s THEN failed_runs ELSE failed_runs + 1 END,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (job_id, success, success, schedule_id)
                )
    logger.debug(f"Updated schedule {schedule_id} execution stats: success={success}")


def list_enabled_schedules() -> List[ScheduleRecord]:
    """
    Get all enabled schedules.

    Returns:
        List of enabled ScheduleRecords
    """
    return list_schedules(enabled=True, limit=1000)


# =============================================================================
# Helper Functions
# =============================================================================

def _row_to_schedule_record(row: Dict) -> ScheduleRecord:
    """Convert database row to ScheduleRecord."""
    return ScheduleRecord(
        id=str(row["id"]),
        name=row["name"],
        project_id=str(row["project_id"]),
        schedule_type=row["schedule_type"],
        cron_expression=row["cron_expression"],
        interval_seconds=row["interval_seconds"],
        timezone=row["timezone"],
        enabled=row["enabled"],
        callback_url=row["callback_url"],
        sftp_override=row["sftp_override"],
        local_files=row["local_files"],
        last_run_at=row["last_run_at"],
        next_run_at=row["next_run_at"],
        last_job_id=str(row["last_job_id"]) if row["last_job_id"] else None,
        total_runs=row["total_runs"],
        successful_runs=row["successful_runs"],
        failed_runs=row["failed_runs"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
