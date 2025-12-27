"""
Pydantic models for schedule API request/response schemas.

This module defines the data structures for:
- Schedule CRUD operations
- Schedule control endpoints (enable, disable, run)
- Schedule history and monitoring
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from src.api.schemas import SFTPConfigSchema


# =============================================================================
# Schedule Schemas
# =============================================================================

class ScheduleCreate(BaseModel):
    """Request body for creating a schedule."""
    name: str = Field(..., min_length=1, max_length=255, description="Unique schedule name")
    project_name: str = Field(..., description="Project name to schedule")
    schedule_type: str = Field(..., description="Either 'cron' or 'interval'")
    cron_expression: Optional[str] = Field(None, description="Cron expression (required if schedule_type='cron')")
    interval_seconds: Optional[int] = Field(None, ge=3600, description="Interval in seconds (required if schedule_type='interval', min 3600)")
    timezone: str = Field("UTC", description="Timezone for schedule")
    enabled: bool = Field(True, description="Whether schedule is active")
    callback_url: Optional[str] = Field(None, description="Optional webhook callback URL")
    sftp_override: Optional[SFTPConfigSchema] = Field(None, description="Override SFTP settings from project")
    local_files: Optional[List[str]] = Field(None, description="List of local file paths to import")

    @field_validator('schedule_type')
    @classmethod
    def validate_schedule_type(cls, v: str) -> str:
        """Validate schedule_type is either 'cron' or 'interval'."""
        if v not in ('cron', 'interval'):
            raise ValueError("schedule_type must be 'cron' or 'interval'")
        return v

    @model_validator(mode='after')
    def validate_schedule_config(self):
        """Validate that required fields are present based on schedule_type."""
        if self.schedule_type == "cron" and not self.cron_expression:
            raise ValueError("cron_expression required when schedule_type='cron'")
        if self.schedule_type == "interval" and not self.interval_seconds:
            raise ValueError("interval_seconds required when schedule_type='interval'")
        return self


class ScheduleUpdate(BaseModel):
    """Request body for updating a schedule."""
    name: Optional[str] = Field(None, min_length=1, max_length=255, description="New schedule name")
    schedule_type: Optional[str] = Field(None, description="New schedule type")
    cron_expression: Optional[str] = Field(None, description="New cron expression")
    interval_seconds: Optional[int] = Field(None, ge=3600, description="New interval in seconds (min 3600)")
    timezone: Optional[str] = Field(None, description="New timezone")
    enabled: Optional[bool] = Field(None, description="New enabled status")
    callback_url: Optional[str] = Field(None, description="New callback URL")
    sftp_override: Optional[SFTPConfigSchema] = Field(None, description="New SFTP override")
    local_files: Optional[List[str]] = Field(None, description="New local files list")

    @field_validator('schedule_type')
    @classmethod
    def validate_schedule_type(cls, v: Optional[str]) -> Optional[str]:
        """Validate schedule_type is either 'cron' or 'interval'."""
        if v is not None and v not in ('cron', 'interval'):
            raise ValueError("schedule_type must be 'cron' or 'interval'")
        return v


class ScheduleResponse(BaseModel):
    """Response for schedule operations."""
    id: str
    name: str
    project_id: str
    project_name: Optional[str] = None  # Populated by joining with projects table
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


class ScheduleListResponse(BaseModel):
    """Response for listing schedules."""
    schedules: List[ScheduleResponse]
    total: int


class ScheduleControlResponse(BaseModel):
    """Response for schedule control operations (enable, disable)."""
    success: bool
    message: str
    schedule: ScheduleResponse
