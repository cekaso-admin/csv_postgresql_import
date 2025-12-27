"""
Pydantic models for API request/response schemas.

This module defines the data structures for:
- Connection CRUD operations
- Project CRUD operations
- Import job requests and responses
- Job status and results
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


# =============================================================================
# Connection Schemas
# =============================================================================

class ConnectionCreate(BaseModel):
    """Request body for creating a connection."""
    name: str = Field(..., min_length=1, max_length=255, description="Unique connection name")
    database_url: str = Field(..., min_length=1, description="PostgreSQL connection string")
    description: Optional[str] = Field(None, description="Optional description")


class ConnectionUpdate(BaseModel):
    """Request body for updating a connection."""
    name: Optional[str] = Field(None, min_length=1, max_length=255, description="New connection name")
    database_url: Optional[str] = Field(None, min_length=1, description="New PostgreSQL connection string")
    description: Optional[str] = Field(None, description="New description")


class ConnectionResponse(BaseModel):
    """Response for connection operations."""
    id: str
    name: str
    description: Optional[str]
    database_url: str
    created_at: datetime
    updated_at: datetime


class ConnectionResponseSafe(BaseModel):
    """Response for connection operations (without sensitive data)."""
    id: str
    name: str
    description: Optional[str]
    created_at: datetime
    updated_at: datetime


class ConnectionListResponse(BaseModel):
    """Response for listing connections."""
    connections: List[ConnectionResponseSafe]
    total: int


class ConnectionTestResponse(BaseModel):
    """Response for connection test."""
    success: bool
    message: str


# =============================================================================
# Source (SFTP) Schemas
# =============================================================================

class SourceCreate(BaseModel):
    """Request body for creating an SFTP source."""
    name: str = Field(..., min_length=1, max_length=255, description="Unique source name")
    host: str = Field(..., min_length=1, description="SFTP server hostname")
    port: int = Field(22, ge=1, le=65535, description="SFTP server port")
    username: str = Field(..., min_length=1, description="SFTP username")
    password: Optional[str] = Field(None, description="SFTP password")
    key_path: Optional[str] = Field(None, description="Path to SSH private key")
    remote_path: str = Field("/", description="Remote directory path")
    description: Optional[str] = Field(None, description="Optional description")


class SourceUpdate(BaseModel):
    """Request body for updating an SFTP source."""
    name: Optional[str] = Field(None, min_length=1, max_length=255, description="New source name")
    host: Optional[str] = Field(None, min_length=1, description="New SFTP server hostname")
    port: Optional[int] = Field(None, ge=1, le=65535, description="New SFTP server port")
    username: Optional[str] = Field(None, min_length=1, description="New SFTP username")
    password: Optional[str] = Field(None, description="New SFTP password")
    key_path: Optional[str] = Field(None, description="New path to SSH private key")
    remote_path: Optional[str] = Field(None, description="New remote directory path")
    description: Optional[str] = Field(None, description="New description")


class SourceResponse(BaseModel):
    """Response for source operations (includes sensitive data)."""
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


class SourceResponseSafe(BaseModel):
    """Response for source operations (without sensitive data)."""
    id: str
    name: str
    description: Optional[str]
    host: str
    port: int
    username: str
    remote_path: str
    created_at: datetime
    updated_at: datetime


class SourceListResponse(BaseModel):
    """Response for listing sources."""
    sources: List[SourceResponseSafe]
    total: int


class SourceTestResponse(BaseModel):
    """Response for SFTP source test."""
    success: bool
    message: str
    file_count: Optional[int] = None


# =============================================================================
# Project Schemas
# =============================================================================

class SFTPConfigSchema(BaseModel):
    """SFTP connection configuration."""
    host: str
    port: int = 22
    username: str
    password: Optional[str] = None
    key_path: Optional[str] = None
    remote_path: str = "/"


class TableNamingSchema(BaseModel):
    """Table naming transformation rules."""
    strip_prefix: Optional[str] = None
    strip_suffix: Optional[str] = None
    lowercase: bool = True


class DefaultsSchema(BaseModel):
    """Default settings for auto-discovered tables."""
    file_pattern: str = "*.csv"
    primary_key: Union[str, List[str]] = "id"
    delimiter: str = ","
    encoding: str = "utf-8"
    skiprows: int = 0
    rebuild_table: bool = False
    db_schema: Optional[str] = Field(None, alias="schema")

    class Config:
        populate_by_name = True


class TableConfigSchema(BaseModel):
    """Configuration for a specific table/file mapping."""
    file_pattern: str
    target_table: str
    primary_key: Union[str, List[str]] = "id"
    column_mapping: Optional[Dict[str, str]] = None
    rebuild_table: bool = False
    delimiter: str = ","
    encoding: str = "utf-8"
    skiprows: int = 0
    db_schema: Optional[str] = Field(None, alias="schema")

    class Config:
        populate_by_name = True


class ProjectConfigSchema(BaseModel):
    """Full project configuration (without connection - now managed separately)."""
    name: str
    sftp: Optional[SFTPConfigSchema] = None
    defaults: Optional[DefaultsSchema] = None
    table_naming: Optional[TableNamingSchema] = None
    tables: Optional[List[TableConfigSchema]] = None


class ProjectCreate(BaseModel):
    """Request body for creating a project."""
    name: str = Field(..., min_length=1, max_length=255, description="Unique project name")
    connection_id: Optional[str] = Field(None, description="Connection ID for target database")
    source_id: Optional[str] = Field(None, description="Source ID for SFTP configuration")
    config: ProjectConfigSchema = Field(..., description="Project configuration")


class ProjectUpdate(BaseModel):
    """Request body for updating a project."""
    connection_id: Optional[str] = Field(None, description="Connection ID (use empty string to clear)")
    source_id: Optional[str] = Field(None, description="Source ID (use empty string to clear)")
    config: Optional[ProjectConfigSchema] = Field(None, description="Updated project configuration")


class ProjectResponse(BaseModel):
    """Response for project operations."""
    id: str
    name: str
    connection_id: Optional[str]
    source_id: Optional[str]
    config: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


class ProjectListResponse(BaseModel):
    """Response for listing projects."""
    projects: List[ProjectResponse]
    total: int


# =============================================================================
# Import/Job Schemas
# =============================================================================

class ImportRequest(BaseModel):
    """Request to start an import job."""
    project: str = Field(..., description="Project name")
    sftp_override: Optional[SFTPConfigSchema] = Field(
        None, description="Override SFTP settings from project config"
    )
    callback_url: Optional[str] = Field(
        None, description="Webhook URL for job completion notification"
    )
    local_files: Optional[List[str]] = Field(
        None, description="List of local file paths (skips SFTP if provided)"
    )


class ImportResponse(BaseModel):
    """Response when import job is started."""
    job_id: str
    project: str
    status: str
    message: str


class JobFileResponse(BaseModel):
    """Result of importing a single file."""
    filename: str
    table_name: Optional[str]
    inserted: int
    updated: int
    success: bool
    error: Optional[str]


class JobErrorResponse(BaseModel):
    """Job error details."""
    error_type: Optional[str]
    message: str
    created_at: datetime


class JobResponse(BaseModel):
    """Full job status and results."""
    id: str
    project_name: str
    status: str
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    duration_seconds: Optional[float] = None
    files_processed: int
    files_failed: int
    total_inserted: int
    total_updated: int
    callback_url: Optional[str]
    schedule_id: Optional[str] = None
    created_at: datetime
    file_results: Optional[List[JobFileResponse]] = None
    errors: Optional[List[JobErrorResponse]] = None


class JobListResponse(BaseModel):
    """Response for listing jobs."""
    jobs: List[JobResponse]
    total: int


# =============================================================================
# Health Check Schema
# =============================================================================

class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    management_db: bool
    version: str = "1.0.0"
