"""
APScheduler service for managing scheduled import jobs.

This module provides:
- SchedulerService class for managing the scheduler lifecycle
- Job execution callback for scheduled imports
- Helper functions for manual triggering
"""

import logging
from datetime import datetime
from typing import Optional
from uuid import uuid4

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import BackgroundTasks

logger = logging.getLogger(__name__)

# Global scheduler instance
_scheduler_service: Optional["SchedulerService"] = None


def get_scheduler_service() -> Optional["SchedulerService"]:
    """
    Get the global scheduler service instance.

    Returns:
        SchedulerService or None if not initialized
    """
    return _scheduler_service


class SchedulerService:
    """
    Singleton service for managing scheduled import jobs.

    Responsibilities:
    - Start/stop APScheduler
    - Load schedules from database on startup
    - Add/remove/update scheduled jobs
    - Execute scheduled imports
    """

    def __init__(self):
        """Initialize the scheduler service."""
        self.scheduler = AsyncIOScheduler(
            job_defaults={
                'coalesce': True,           # Combine missed runs
                'max_instances': 1,         # No overlapping
                'misfire_grace_time': 300,  # 5 min grace period
            }
        )
        self._started = False

    def start(self) -> None:
        """
        Start the scheduler and load enabled schedules from database.

        Raises:
            Exception: If scheduler fails to start
        """
        global _scheduler_service

        if self._started:
            logger.warning("Scheduler already started")
            return

        try:
            # Load all enabled schedules
            from src.db.schedules import list_enabled_schedules
            schedules = list_enabled_schedules()

            for schedule in schedules:
                try:
                    self.add_schedule(schedule)
                except Exception as e:
                    logger.error(f"Failed to add schedule '{schedule.name}': {e}", exc_info=True)

            self.scheduler.start()
            self._started = True
            _scheduler_service = self
            logger.info(f"Scheduler started with {len(schedules)} active schedules")

        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}", exc_info=True)
            raise

    def shutdown(self) -> None:
        """
        Shutdown the scheduler gracefully.

        Waits for running jobs to complete before shutting down.
        """
        global _scheduler_service

        if not self._started:
            logger.warning("Scheduler not started")
            return

        try:
            self.scheduler.shutdown(wait=True)
            self._started = False
            _scheduler_service = None
            logger.info("Scheduler shutdown complete")
        except Exception as e:
            logger.error(f"Error during scheduler shutdown: {e}", exc_info=True)

    def add_schedule(self, schedule) -> None:
        """
        Add a schedule to APScheduler.

        Args:
            schedule: ScheduleRecord to add
        """
        from src.db.schedules import ScheduleRecord

        if not isinstance(schedule, ScheduleRecord):
            logger.error(f"Invalid schedule type: {type(schedule)}")
            return

        job_id = f"schedule_{schedule.id}"

        try:
            # Build trigger based on schedule type
            if schedule.schedule_type == "cron":
                trigger = CronTrigger.from_crontab(
                    schedule.cron_expression,
                    timezone=schedule.timezone
                )
            elif schedule.schedule_type == "interval":
                trigger = IntervalTrigger(
                    seconds=schedule.interval_seconds,
                    timezone=schedule.timezone
                )
            else:
                logger.error(f"Unknown schedule type: {schedule.schedule_type}")
                return

            # Add job to scheduler
            self.scheduler.add_job(
                execute_scheduled_import,
                trigger=trigger,
                id=job_id,
                name=schedule.name,
                kwargs={'schedule_id': schedule.id},
                replace_existing=True,
            )

            logger.info(f"Added schedule '{schedule.name}' (ID: {schedule.id})")

        except Exception as e:
            logger.error(f"Failed to add schedule '{schedule.name}': {e}", exc_info=True)
            raise

    def remove_schedule(self, schedule_id: str) -> None:
        """
        Remove a schedule from APScheduler.

        Args:
            schedule_id: Schedule UUID
        """
        job_id = f"schedule_{schedule_id}"

        try:
            self.scheduler.remove_job(job_id)
            logger.info(f"Removed schedule {schedule_id}")
        except Exception as e:
            logger.warning(f"Failed to remove schedule {schedule_id}: {e}")

    def update_schedule(self, schedule) -> None:
        """
        Update an existing schedule.

        Args:
            schedule: Updated ScheduleRecord
        """
        from src.db.schedules import ScheduleRecord

        if not isinstance(schedule, ScheduleRecord):
            logger.error(f"Invalid schedule type: {type(schedule)}")
            return

        # Remove and re-add if enabled, otherwise just remove
        self.remove_schedule(schedule.id)
        if schedule.enabled:
            self.add_schedule(schedule)

    def pause_schedule(self, schedule_id: str) -> None:
        """
        Pause a schedule without removing it.

        Args:
            schedule_id: Schedule UUID
        """
        job_id = f"schedule_{schedule_id}"

        try:
            self.scheduler.pause_job(job_id)
            logger.info(f"Paused schedule {schedule_id}")
        except Exception as e:
            logger.warning(f"Failed to pause schedule {schedule_id}: {e}")

    def resume_schedule(self, schedule_id: str) -> None:
        """
        Resume a paused schedule.

        Args:
            schedule_id: Schedule UUID
        """
        job_id = f"schedule_{schedule_id}"

        try:
            self.scheduler.resume_job(job_id)
            logger.info(f"Resumed schedule {schedule_id}")
        except Exception as e:
            logger.warning(f"Failed to resume schedule {schedule_id}: {e}")


def execute_scheduled_import(schedule_id: str) -> None:
    """
    Callback function executed by APScheduler when a schedule triggers.

    This function:
    1. Loads the schedule from database
    2. Builds an ImportRequest
    3. Creates a job with schedule_id reference
    4. Calls run_import_job() to execute the import
    5. Updates schedule statistics

    Args:
        schedule_id: Schedule UUID
    """
    from src.api.routes import run_import_job
    from src.api.schemas import ImportRequest
    from src.db.management import create_job, get_job, get_project_by_id
    from src.db.schedules import get_schedule, update_schedule_execution

    logger.info(f"Executing scheduled import for schedule {schedule_id}")

    job_id = None
    success = False

    try:
        # Load schedule
        schedule = get_schedule(schedule_id)
        if not schedule:
            logger.error(f"Schedule {schedule_id} not found")
            return

        # Load project
        project = get_project_by_id(schedule.project_id)
        if not project:
            logger.error(f"Project {schedule.project_id} not found for schedule {schedule_id}")
            return

        # Build ImportRequest
        sftp_override_dict = None
        if schedule.sftp_override:
            from src.api.schemas import SFTPConfigSchema
            sftp_override_dict = SFTPConfigSchema(**schedule.sftp_override)

        request = ImportRequest(
            project=project.name,
            callback_url=schedule.callback_url,
            sftp_override=sftp_override_dict,
            local_files=schedule.local_files,
        )

        # Create job with schedule_id
        job_record = create_job(
            project_name=project.name,
            callback_url=schedule.callback_url,
            schedule_id=schedule_id,
        )
        job_id = job_record.id

        # Execute import (synchronously - APScheduler runs in executor)
        run_import_job(job_id, project.name, request)

        # Check job status
        job = get_job(job_id)
        success = (job.status == "completed") if job else False

        logger.info(f"Scheduled import completed for schedule {schedule_id}, job {job_id}, success={success}")

    except Exception as e:
        logger.error(f"Scheduled import failed for schedule {schedule_id}: {e}", exc_info=True)

    finally:
        # Update schedule execution stats
        try:
            update_schedule_execution(
                schedule_id=schedule_id,
                job_id=job_id,
                success=success,
            )
        except Exception as stats_error:
            logger.error(f"Failed to update schedule stats: {stats_error}")


def trigger_schedule_execution(schedule_id: str, background_tasks: BackgroundTasks) -> str:
    """
    Manually trigger a schedule to run immediately.

    This is used by the /schedules/{id}/run endpoint.

    Args:
        schedule_id: Schedule UUID
        background_tasks: FastAPI BackgroundTasks instance

    Returns:
        Job ID

    Raises:
        ValueError: If schedule not found or validation fails
    """
    from src.api.routes import run_import_job
    from src.api.schemas import ImportRequest
    from src.db.management import create_job, get_project_by_id
    from src.db.schedules import get_schedule

    # Load schedule
    schedule = get_schedule(schedule_id)
    if not schedule:
        raise ValueError(f"Schedule {schedule_id} not found")

    # Load project
    project = get_project_by_id(schedule.project_id)
    if not project:
        raise ValueError(f"Project {schedule.project_id} not found")

    # Build ImportRequest
    sftp_override_dict = None
    if schedule.sftp_override:
        from src.api.schemas import SFTPConfigSchema
        sftp_override_dict = SFTPConfigSchema(**schedule.sftp_override)

    request = ImportRequest(
        project=project.name,
        callback_url=schedule.callback_url,
        sftp_override=sftp_override_dict,
        local_files=schedule.local_files,
    )

    # Create job with schedule_id
    job_record = create_job(
        project_name=project.name,
        callback_url=schedule.callback_url,
        schedule_id=schedule_id,
    )

    # Start background task
    background_tasks.add_task(run_import_job, job_record.id, project.name, request)

    logger.info(f"Manually triggered schedule {schedule_id}, job {job_record.id}")

    return job_record.id
