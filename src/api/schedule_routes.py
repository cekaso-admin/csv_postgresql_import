"""
FastAPI routes for schedule management.

Endpoints:
- Schedule CRUD operations
- Schedule control (enable, disable, run)
- Schedule job history
"""

import logging
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query

from src.api.auth import require_api_key
from src.api.schedule_schemas import (
    ScheduleControlResponse,
    ScheduleCreate,
    ScheduleListResponse,
    ScheduleResponse,
    ScheduleUpdate,
)
from src.api.schemas import ImportRequest, ImportResponse, JobListResponse, JobResponse
from src.db.management import (
    get_job,
    get_job_errors,
    get_job_files,
    get_project,
    list_jobs,
)
from src.db.schedules import (
    create_schedule,
    delete_schedule,
    get_schedule,
    get_schedule_by_name,
    list_schedules,
    update_schedule,
)

logger = logging.getLogger(__name__)

# Create router with API key authentication
schedules_router = APIRouter(
    prefix="/schedules",
    tags=["schedules"],
    dependencies=[Depends(require_api_key)],
)


# =============================================================================
# Schedule CRUD Routes
# =============================================================================

@schedules_router.post("", response_model=ScheduleResponse, status_code=201)
async def create_schedule_endpoint(schedule: ScheduleCreate):
    """
    Create a new schedule.

    A schedule defines when and how to run an import job automatically.
    """
    try:
        # Validate project exists
        project = get_project(schedule.project_name)
        if not project:
            raise HTTPException(
                status_code=404,
                detail=f"Project '{schedule.project_name}' not found"
            )

        # Validate project has connection
        if not project.connection_id:
            raise HTTPException(
                status_code=400,
                detail=f"Project '{schedule.project_name}' has no connection configured"
            )

        # Prepare SFTP override
        sftp_override = None
        if schedule.sftp_override:
            sftp_override = schedule.sftp_override.model_dump()

        # Create schedule record
        record = create_schedule(
            name=schedule.name,
            project_id=project.id,
            schedule_type=schedule.schedule_type,
            cron_expression=schedule.cron_expression,
            interval_seconds=schedule.interval_seconds,
            timezone=schedule.timezone,
            enabled=schedule.enabled,
            callback_url=schedule.callback_url,
            sftp_override=sftp_override,
            local_files=schedule.local_files,
        )

        # Register with scheduler if enabled
        if record.enabled:
            try:
                from src.services.scheduler import get_scheduler_service
                scheduler = get_scheduler_service()
                if scheduler:
                    scheduler.add_schedule(record)
            except Exception as e:
                logger.error(f"Failed to add schedule to scheduler: {e}", exc_info=True)
                # Don't fail the request - schedule is created in DB

        return ScheduleResponse(
            id=record.id,
            name=record.name,
            project_id=record.project_id,
            project_name=schedule.project_name,
            schedule_type=record.schedule_type,
            cron_expression=record.cron_expression,
            interval_seconds=record.interval_seconds,
            timezone=record.timezone,
            enabled=record.enabled,
            callback_url=record.callback_url,
            sftp_override=record.sftp_override,
            local_files=record.local_files,
            last_run_at=record.last_run_at,
            next_run_at=record.next_run_at,
            last_job_id=record.last_job_id,
            total_runs=record.total_runs,
            successful_runs=record.successful_runs,
            failed_runs=record.failed_runs,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating schedule: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to create schedule")


@schedules_router.get("", response_model=ScheduleListResponse)
async def list_schedules_endpoint(
    project: Optional[str] = Query(None, description="Filter by project name"),
    enabled: Optional[bool] = Query(None, description="Filter by enabled status"),
    limit: int = Query(50, ge=1, le=100, description="Maximum number of schedules"),
    offset: int = Query(0, ge=0, description="Number of schedules to skip"),
):
    """List all schedules with optional filtering."""
    try:
        # Get project_id if project name provided
        project_id = None
        if project:
            project_record = get_project(project)
            if not project_record:
                raise HTTPException(
                    status_code=404,
                    detail=f"Project '{project}' not found"
                )
            project_id = project_record.id

        records = list_schedules(
            project_id=project_id,
            enabled=enabled,
            limit=limit,
            offset=offset,
        )

        schedules = []
        for r in records:
            # Get project name
            project_record = get_project(r.project_id)
            project_name = project_record.name if project_record else None

            schedules.append(ScheduleResponse(
                id=r.id,
                name=r.name,
                project_id=r.project_id,
                project_name=project_name,
                schedule_type=r.schedule_type,
                cron_expression=r.cron_expression,
                interval_seconds=r.interval_seconds,
                timezone=r.timezone,
                enabled=r.enabled,
                callback_url=r.callback_url,
                sftp_override=r.sftp_override,
                local_files=r.local_files,
                last_run_at=r.last_run_at,
                next_run_at=r.next_run_at,
                last_job_id=r.last_job_id,
                total_runs=r.total_runs,
                successful_runs=r.successful_runs,
                failed_runs=r.failed_runs,
                created_at=r.created_at,
                updated_at=r.updated_at,
            ))

        return ScheduleListResponse(schedules=schedules, total=len(schedules))

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing schedules: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to list schedules")


@schedules_router.get("/{schedule_id}", response_model=ScheduleResponse)
async def get_schedule_endpoint(schedule_id: str):
    """Get a schedule by ID."""
    record = get_schedule(schedule_id)
    if not record:
        raise HTTPException(
            status_code=404,
            detail=f"Schedule '{schedule_id}' not found"
        )

    # Get project name
    project_record = get_project(record.project_id)
    project_name = project_record.name if project_record else None

    return ScheduleResponse(
        id=record.id,
        name=record.name,
        project_id=record.project_id,
        project_name=project_name,
        schedule_type=record.schedule_type,
        cron_expression=record.cron_expression,
        interval_seconds=record.interval_seconds,
        timezone=record.timezone,
        enabled=record.enabled,
        callback_url=record.callback_url,
        sftp_override=record.sftp_override,
        local_files=record.local_files,
        last_run_at=record.last_run_at,
        next_run_at=record.next_run_at,
        last_job_id=record.last_job_id,
        total_runs=record.total_runs,
        successful_runs=record.successful_runs,
        failed_runs=record.failed_runs,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


@schedules_router.put("/{schedule_id}", response_model=ScheduleResponse)
async def update_schedule_endpoint(schedule_id: str, schedule: ScheduleUpdate):
    """Update a schedule."""
    try:
        # Prepare SFTP override
        sftp_override = None
        if schedule.sftp_override:
            sftp_override = schedule.sftp_override.model_dump()

        record = update_schedule(
            schedule_id=schedule_id,
            name=schedule.name,
            schedule_type=schedule.schedule_type,
            cron_expression=schedule.cron_expression,
            interval_seconds=schedule.interval_seconds,
            timezone=schedule.timezone,
            enabled=schedule.enabled,
            callback_url=schedule.callback_url,
            sftp_override=sftp_override,
            local_files=schedule.local_files,
        )

        if not record:
            raise HTTPException(
                status_code=404,
                detail=f"Schedule '{schedule_id}' not found"
            )

        # Update scheduler if needed
        try:
            from src.services.scheduler import get_scheduler_service
            scheduler = get_scheduler_service()
            if scheduler:
                scheduler.update_schedule(record)
        except Exception as e:
            logger.error(f"Failed to update schedule in scheduler: {e}", exc_info=True)

        # Get project name
        project_record = get_project(record.project_id)
        project_name = project_record.name if project_record else None

        return ScheduleResponse(
            id=record.id,
            name=record.name,
            project_id=record.project_id,
            project_name=project_name,
            schedule_type=record.schedule_type,
            cron_expression=record.cron_expression,
            interval_seconds=record.interval_seconds,
            timezone=record.timezone,
            enabled=record.enabled,
            callback_url=record.callback_url,
            sftp_override=record.sftp_override,
            local_files=record.local_files,
            last_run_at=record.last_run_at,
            next_run_at=record.next_run_at,
            last_job_id=record.last_job_id,
            total_runs=record.total_runs,
            successful_runs=record.successful_runs,
            failed_runs=record.failed_runs,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating schedule: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to update schedule")


@schedules_router.delete("/{schedule_id}", status_code=204)
async def delete_schedule_endpoint(schedule_id: str):
    """Delete a schedule."""
    try:
        # Remove from scheduler first
        try:
            from src.services.scheduler import get_scheduler_service
            scheduler = get_scheduler_service()
            if scheduler:
                scheduler.remove_schedule(schedule_id)
        except Exception as e:
            logger.error(f"Failed to remove schedule from scheduler: {e}", exc_info=True)

        # Delete from database
        deleted = delete_schedule(schedule_id)
        if not deleted:
            raise HTTPException(
                status_code=404,
                detail=f"Schedule '{schedule_id}' not found"
            )

        return None

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting schedule: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to delete schedule")


# =============================================================================
# Schedule Control Routes
# =============================================================================

@schedules_router.post("/{schedule_id}/enable", response_model=ScheduleControlResponse)
async def enable_schedule_endpoint(schedule_id: str):
    """Enable a schedule."""
    try:
        # Update database
        record = update_schedule(schedule_id=schedule_id, enabled=True)
        if not record:
            raise HTTPException(
                status_code=404,
                detail=f"Schedule '{schedule_id}' not found"
            )

        # Add to scheduler
        try:
            from src.services.scheduler import get_scheduler_service
            scheduler = get_scheduler_service()
            if scheduler:
                scheduler.add_schedule(record)
        except Exception as e:
            logger.error(f"Failed to add schedule to scheduler: {e}", exc_info=True)

        # Get project name
        project_record = get_project(record.project_id)
        project_name = project_record.name if project_record else None

        return ScheduleControlResponse(
            success=True,
            message="Schedule enabled successfully",
            schedule=ScheduleResponse(
                id=record.id,
                name=record.name,
                project_id=record.project_id,
                project_name=project_name,
                schedule_type=record.schedule_type,
                cron_expression=record.cron_expression,
                interval_seconds=record.interval_seconds,
                timezone=record.timezone,
                enabled=record.enabled,
                callback_url=record.callback_url,
                sftp_override=record.sftp_override,
                local_files=record.local_files,
                last_run_at=record.last_run_at,
                next_run_at=record.next_run_at,
                last_job_id=record.last_job_id,
                total_runs=record.total_runs,
                successful_runs=record.successful_runs,
                failed_runs=record.failed_runs,
                created_at=record.created_at,
                updated_at=record.updated_at,
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error enabling schedule: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to enable schedule")


@schedules_router.post("/{schedule_id}/disable", response_model=ScheduleControlResponse)
async def disable_schedule_endpoint(schedule_id: str):
    """Disable a schedule."""
    try:
        # Remove from scheduler
        try:
            from src.services.scheduler import get_scheduler_service
            scheduler = get_scheduler_service()
            if scheduler:
                scheduler.remove_schedule(schedule_id)
        except Exception as e:
            logger.error(f"Failed to remove schedule from scheduler: {e}", exc_info=True)

        # Update database
        record = update_schedule(schedule_id=schedule_id, enabled=False)
        if not record:
            raise HTTPException(
                status_code=404,
                detail=f"Schedule '{schedule_id}' not found"
            )

        # Get project name
        project_record = get_project(record.project_id)
        project_name = project_record.name if project_record else None

        return ScheduleControlResponse(
            success=True,
            message="Schedule disabled successfully",
            schedule=ScheduleResponse(
                id=record.id,
                name=record.name,
                project_id=record.project_id,
                project_name=project_name,
                schedule_type=record.schedule_type,
                cron_expression=record.cron_expression,
                interval_seconds=record.interval_seconds,
                timezone=record.timezone,
                enabled=record.enabled,
                callback_url=record.callback_url,
                sftp_override=record.sftp_override,
                local_files=record.local_files,
                last_run_at=record.last_run_at,
                next_run_at=record.next_run_at,
                last_job_id=record.last_job_id,
                total_runs=record.total_runs,
                successful_runs=record.successful_runs,
                failed_runs=record.failed_runs,
                created_at=record.created_at,
                updated_at=record.updated_at,
            ),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error disabling schedule: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to disable schedule")


@schedules_router.post("/{schedule_id}/run", response_model=ImportResponse, status_code=202)
async def run_schedule_endpoint(schedule_id: str, background_tasks: BackgroundTasks):
    """
    Manually trigger a schedule to run immediately.

    This creates a job and runs the import in the background.
    """
    try:
        # Get schedule
        schedule = get_schedule(schedule_id)
        if not schedule:
            raise HTTPException(
                status_code=404,
                detail=f"Schedule '{schedule_id}' not found"
            )

        # Get project
        project_record = get_project(schedule.project_id)
        if not project_record:
            raise HTTPException(
                status_code=404,
                detail=f"Project '{schedule.project_id}' not found"
            )

        # Trigger execution
        from src.services.scheduler import trigger_schedule_execution
        job_id = trigger_schedule_execution(schedule_id, background_tasks)

        return ImportResponse(
            job_id=job_id,
            project=project_record.name,
            status="pending",
            message=f"Schedule '{schedule.name}' triggered manually. Use GET /jobs/{job_id} to check status.",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error running schedule: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to run schedule")


@schedules_router.get("/{schedule_id}/history", response_model=JobListResponse)
async def get_schedule_history_endpoint(
    schedule_id: str,
    limit: int = Query(50, ge=1, le=100, description="Maximum number of jobs"),
    offset: int = Query(0, ge=0, description="Number of jobs to skip"),
):
    """
    Get job history for a schedule.

    Returns all jobs that were triggered by this schedule.
    """
    # Verify schedule exists
    schedule = get_schedule(schedule_id)
    if not schedule:
        raise HTTPException(
            status_code=404,
            detail=f"Schedule '{schedule_id}' not found"
        )

    # Get all jobs for this schedule
    # Note: list_jobs doesn't support schedule_id filter, so we need to query manually
    from src.db.management import get_management_connection
    from psycopg2.extras import RealDictCursor

    try:
        with get_management_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, project_id, project_name, status, started_at,
                           completed_at, files_processed, files_failed,
                           total_inserted, total_updated, total_skipped, callback_url, schedule_id, created_at
                    FROM cpi_jobs
                    WHERE schedule_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (schedule_id, limit, offset)
                )
                rows = cur.fetchall()

        jobs = []
        for row in rows:
            duration = None
            if row["started_at"] and row["completed_at"]:
                duration = (row["completed_at"] - row["started_at"]).total_seconds()

            # Get job details
            job = get_job(str(row["id"]))
            if job:
                file_records = get_job_files(job.id)
                error_records = get_job_errors(job.id)

                from src.api.schemas import JobErrorResponse, JobFileResponse
                jobs.append(JobResponse(
                    id=job.id,
                    project_name=job.project_name,
                    status=job.status,
                    started_at=job.started_at,
                    completed_at=job.completed_at,
                    duration_seconds=duration,
                    files_processed=job.files_processed,
                    files_failed=job.files_failed,
                    total_inserted=job.total_inserted,
                    total_updated=job.total_updated,
                    total_skipped=job.total_skipped,
                    callback_url=job.callback_url,
                    created_at=job.created_at,
                    file_results=[
                        JobFileResponse(
                            filename=f.filename,
                            table_name=f.table_name,
                            inserted=f.inserted,
                            updated=f.updated,
                            skipped=f.skipped,
                            success=f.success,
                            error=f.error,
                        )
                        for f in file_records
                    ],
                    errors=[
                        JobErrorResponse(
                            error_type=e.error_type,
                            message=e.message,
                            created_at=e.created_at,
                        )
                        for e in error_records
                    ],
                ))

        return JobListResponse(jobs=jobs, total=len(jobs))

    except Exception as e:
        logger.error(f"Error getting schedule history: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get schedule history")
