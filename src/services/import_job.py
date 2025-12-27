"""
Import job orchestration service.

This module coordinates the full import workflow:
1. Load project configuration
2. Pull files from SFTP (if configured)
3. Match files to table configurations
4. Import each file to the database
5. Track statistics and errors
6. Send webhook callback with results
"""

import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

from src.config import load_project_config, ProjectConfig
from src.config.models import SFTPConfig
from src.db.importer import import_csv, ImportResult
from src.sftp import SFTPClient, DownloadResult
from src.services.webhook import send_webhook, WebhookPayload

logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    """Status of an import job."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"  # Some files succeeded, some failed


@dataclass
class FileResult:
    """Result of importing a single file."""
    filename: str
    table_name: str
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    success: bool = False
    error: Optional[str] = None


@dataclass
class JobResult:
    """
    Complete result of an import job.

    Attributes:
        job_id: Unique identifier for this job
        project: Project name
        status: Final job status
        started_at: Job start timestamp
        completed_at: Job completion timestamp
        files_processed: Number of files successfully processed
        files_failed: Number of files that failed
        total_inserted: Total rows inserted across all files
        total_updated: Total rows updated across all files
        total_skipped: Total rows skipped (unchanged) across all files
        file_results: Detailed results per file
        errors: List of job-level error messages
    """
    job_id: str
    project: str
    status: JobStatus = JobStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    files_processed: int = 0
    files_failed: int = 0
    total_inserted: int = 0
    total_updated: int = 0
    total_skipped: int = 0
    file_results: List[FileResult] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def total_files(self) -> int:
        """Total number of files attempted."""
        return self.files_processed + self.files_failed

    @property
    def duration_seconds(self) -> Optional[float]:
        """Job duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "job_id": self.job_id,
            "project": self.project,
            "status": self.status.value,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "files_processed": self.files_processed,
            "files_failed": self.files_failed,
            "total_inserted": self.total_inserted,
            "total_updated": self.total_updated,
            "total_skipped": self.total_skipped,
            "file_results": [
                {
                    "filename": fr.filename,
                    "table_name": fr.table_name,
                    "inserted": fr.inserted,
                    "updated": fr.updated,
                    "skipped": fr.skipped,
                    "success": fr.success,
                    "error": fr.error,
                }
                for fr in self.file_results
            ],
            "errors": self.errors,
        }


class ImportJob:
    """
    Orchestrates the full CSV import workflow.

    Coordinates:
    - Configuration loading
    - SFTP file retrieval (optional)
    - File-to-table matching
    - Database imports
    - Webhook callbacks

    Example:
        ```python
        # Run import for a project
        job = ImportJob(project="customer_abc")
        result = job.run()

        # Run with SFTP override
        job = ImportJob(
            project="customer_abc",
            sftp_override=SFTPConfig(host="...", ...),
            callback_url="https://n8n.example.com/webhook/abc"
        )
        result = job.run()

        # Run with local files (no SFTP)
        job = ImportJob(project="customer_abc")
        result = job.run_local(files=["/path/to/file1.csv", "/path/to/file2.csv"])
        ```
    """

    def __init__(
        self,
        project: str,
        sftp_override: Optional[SFTPConfig] = None,
        callback_url: Optional[str] = None,
        job_id: Optional[str] = None,
    ):
        """
        Initialize import job.

        Args:
            project: Project name (matches config file)
            sftp_override: Optional SFTP config to override project settings
            callback_url: Optional webhook URL for job completion notification
            job_id: Optional custom job ID (auto-generated if not provided)
        """
        self.project = project
        self.sftp_override = sftp_override
        self.callback_url = callback_url
        self.job_id = job_id or str(uuid.uuid4())

        self._config: Optional[ProjectConfig] = None
        self._result: Optional[JobResult] = None

    @property
    def config(self) -> ProjectConfig:
        """Load and cache project configuration."""
        if self._config is None:
            self._config = load_project_config(self.project)
        return self._config

    @property
    def result(self) -> JobResult:
        """Get or create job result."""
        if self._result is None:
            self._result = JobResult(job_id=self.job_id, project=self.project)
        return self._result

    def run(self) -> JobResult:
        """
        Run the full import workflow with SFTP.

        1. Connect to SFTP and download files
        2. Match files to table configurations
        3. Import each file
        4. Send webhook callback

        Returns:
            JobResult with statistics and status
        """
        self.result.started_at = datetime.utcnow()
        self.result.status = JobStatus.RUNNING

        logger.info(f"Starting import job {self.job_id} for project '{self.project}'")

        try:
            # Get SFTP config (override or from project)
            sftp_config = self.sftp_override or self.config.sftp

            if not sftp_config:
                raise ValueError(
                    f"No SFTP configuration for project '{self.project}'. "
                    "Provide sftp_override or configure SFTP in project config."
                )

            # Download files from SFTP
            with SFTPClient(sftp_config) as sftp:
                # Use defaults file_pattern if available
                pattern = "*.csv"
                if self.config.defaults:
                    pattern = self.config.defaults.file_pattern

                download_result = sftp.download_matching_files(pattern)

                if download_result.has_errors:
                    for error in download_result.errors:
                        self.result.errors.append(f"Download error: {error}")

                if not download_result.local_paths:
                    logger.warning("No files downloaded from SFTP")
                    self.result.status = JobStatus.COMPLETED
                    self._finalize()
                    return self.result

                # Process downloaded files
                self._process_files(download_result.local_paths)

        except Exception as e:
            error_msg = f"Job failed: {e}"
            logger.error(error_msg, exc_info=True)
            self.result.errors.append(error_msg)
            self.result.status = JobStatus.FAILED

        self._finalize()
        return self.result

    def run_local(self, files: List[str]) -> JobResult:
        """
        Run import with local files (no SFTP).

        Args:
            files: List of local file paths to import

        Returns:
            JobResult with statistics and status
        """
        self.result.started_at = datetime.utcnow()
        self.result.status = JobStatus.RUNNING

        logger.info(
            f"Starting local import job {self.job_id} for project '{self.project}' "
            f"with {len(files)} files"
        )

        try:
            # Validate files exist
            valid_files = []
            for file_path in files:
                if os.path.exists(file_path):
                    valid_files.append(file_path)
                else:
                    self.result.errors.append(f"File not found: {file_path}")

            if valid_files:
                self._process_files(valid_files)

        except Exception as e:
            error_msg = f"Job failed: {e}"
            logger.error(error_msg, exc_info=True)
            self.result.errors.append(error_msg)
            self.result.status = JobStatus.FAILED

        self._finalize()
        return self.result

    def _process_files(self, file_paths: List[str]) -> None:
        """
        Process a list of local files.

        Args:
            file_paths: List of local file paths to import
        """
        for file_path in file_paths:
            filename = os.path.basename(file_path)

            # Get table config for this file
            table_config = self.config.get_table_for_file(filename)

            if not table_config:
                logger.warning(f"No table config for file: {filename}, skipping")
                self.result.file_results.append(FileResult(
                    filename=filename,
                    table_name="",
                    success=False,
                    error="No matching table configuration"
                ))
                self.result.files_failed += 1
                continue

            # Import the file
            file_result = self._import_file(file_path, table_config)
            self.result.file_results.append(file_result)

            if file_result.success:
                self.result.files_processed += 1
                self.result.total_inserted += file_result.inserted
                self.result.total_updated += file_result.updated
                self.result.total_skipped += file_result.skipped
            else:
                self.result.files_failed += 1

    def _import_file(self, file_path: str, table_config) -> FileResult:
        """
        Import a single file using its table configuration.

        Args:
            file_path: Path to the CSV file
            table_config: TableConfig for this file

        Returns:
            FileResult with import statistics
        """
        filename = os.path.basename(file_path)
        file_result = FileResult(
            filename=filename,
            table_name=table_config.target_table
        )

        try:
            logger.info(f"Importing {filename} -> {table_config.target_table}")

            import_result = import_csv(
                file_path=file_path,
                table_name=table_config.target_table,
                primary_key=table_config.primary_key,
                column_mapping=table_config.column_mapping,
                rebuild_table=table_config.rebuild_table,
                schema=table_config.db_schema,
                delimiter=table_config.delimiter,
                encoding=table_config.encoding,
                skiprows=table_config.skiprows,
            )

            file_result.inserted = import_result.inserted
            file_result.updated = import_result.updated
            file_result.skipped = import_result.skipped
            file_result.success = import_result.success

            if import_result.has_errors:
                file_result.error = "; ".join(import_result.errors)

            logger.info(
                f"Imported {filename}: {import_result.inserted} inserted, "
                f"{import_result.updated} updated, {import_result.skipped} skipped"
            )

        except Exception as e:
            file_result.error = str(e)
            logger.error(f"Failed to import {filename}: {e}", exc_info=True)

        return file_result

    def _finalize(self) -> None:
        """Finalize job: set status and send webhook."""
        self.result.completed_at = datetime.utcnow()

        # Determine final status
        if self.result.status != JobStatus.FAILED:
            if self.result.files_failed == 0 and self.result.files_processed > 0:
                self.result.status = JobStatus.COMPLETED
            elif self.result.files_processed > 0 and self.result.files_failed > 0:
                self.result.status = JobStatus.PARTIAL
            elif self.result.files_processed == 0:
                self.result.status = JobStatus.FAILED

        logger.info(
            f"Job {self.job_id} completed: status={self.result.status.value}, "
            f"processed={self.result.files_processed}, failed={self.result.files_failed}, "
            f"inserted={self.result.total_inserted}, updated={self.result.total_updated}, "
            f"skipped={self.result.total_skipped}"
        )

        # Send webhook callback
        if self.callback_url:
            self._send_callback()

    def _send_callback(self) -> None:
        """Send webhook callback with job results."""
        try:
            payload = WebhookPayload(
                job_id=self.result.job_id,
                project=self.result.project,
                status=self.result.status.value,
                files_processed=self.result.files_processed,
                files_failed=self.result.files_failed,
                total_inserted=self.result.total_inserted,
                total_updated=self.result.total_updated,
                total_skipped=self.result.total_skipped,
                errors=self.result.errors,
                duration_seconds=self.result.duration_seconds,
            )

            success = send_webhook(self.callback_url, payload)

            if success:
                logger.info(f"Webhook callback sent to {self.callback_url}")
            else:
                logger.error(f"Webhook callback failed for {self.callback_url}")

        except Exception as e:
            logger.error(f"Error sending webhook callback: {e}", exc_info=True)


def run_import(
    project: str,
    sftp_override: Optional[SFTPConfig] = None,
    callback_url: Optional[str] = None,
    local_files: Optional[List[str]] = None,
) -> JobResult:
    """
    Convenience function to run an import job.

    Args:
        project: Project name
        sftp_override: Optional SFTP config override
        callback_url: Optional webhook URL
        local_files: Optional list of local files (skips SFTP if provided)

    Returns:
        JobResult with statistics
    """
    job = ImportJob(
        project=project,
        sftp_override=sftp_override,
        callback_url=callback_url,
    )

    if local_files:
        return job.run_local(local_files)
    else:
        return job.run()
