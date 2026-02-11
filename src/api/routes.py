"""
FastAPI routes for the CSV import API.

Endpoints:
- Connections: CRUD operations for database connections
- Projects: CRUD operations for project configurations
- Jobs: Import job management and monitoring
- Health: System health check
"""

import logging
import os
import tempfile
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query

from src.hooks import HookContext, HookEngine, HookPoint

from src.api.auth import require_api_key
from src.api.schemas import (
    ConnectionCreate,
    ConnectionListResponse,
    ConnectionResponse,
    ConnectionResponseSafe,
    ConnectionTestResponse,
    ConnectionUpdate,
    HealthResponse,
    ImportRequest,
    ImportResponse,
    JobErrorResponse,
    JobFileResponse,
    JobListResponse,
    JobResponse,
    ProjectCreate,
    ProjectListResponse,
    ProjectResponse,
    ProjectUpdate,
    SourceCreate,
    SourceListResponse,
    SourceResponse,
    SourceResponseSafe,
    SourceTestResponse,
    SourceUpdate,
)
from src.db.management import (
    add_job_error,
    add_job_file,
    create_connection,
    create_job,
    create_project,
    create_source,
    delete_connection,
    delete_project,
    delete_source,
    get_connection,
    get_job,
    get_job_errors,
    get_job_files,
    get_project,
    get_source,
    list_connections,
    list_jobs,
    list_projects,
    list_sources,
    test_management_connection,
    test_sftp_source,
    test_target_connection,
    update_connection,
    update_job_status,
    update_project,
    update_source,
)

logger = logging.getLogger(__name__)

# Create routers with API key authentication
# Health router is public (no auth required)
connections_router = APIRouter(
    prefix="/connections",
    tags=["connections"],
    dependencies=[Depends(require_api_key)],
)
sources_router = APIRouter(
    prefix="/sources",
    tags=["sources"],
    dependencies=[Depends(require_api_key)],
)
projects_router = APIRouter(
    prefix="/projects",
    tags=["projects"],
    dependencies=[Depends(require_api_key)],
)
jobs_router = APIRouter(
    prefix="/jobs",
    tags=["jobs"],
    dependencies=[Depends(require_api_key)],
)
import_router = APIRouter(
    prefix="/import",
    tags=["import"],
    dependencies=[Depends(require_api_key)],
)
health_router = APIRouter(tags=["health"])  # Public, no auth


# =============================================================================
# Connection Routes
# =============================================================================

@connections_router.post("", response_model=ConnectionResponse, status_code=201)
async def create_connection_endpoint(connection: ConnectionCreate):
    """Create a new database connection."""
    try:
        record = create_connection(
            name=connection.name,
            database_url=connection.database_url,
            description=connection.description,
        )
        return ConnectionResponse(
            id=record.id,
            name=record.name,
            description=record.description,
            database_url=record.database_url,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating connection: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to create connection")


@connections_router.get("", response_model=ConnectionListResponse)
async def list_connections_endpoint():
    """List all connections (without sensitive data)."""
    try:
        records = list_connections()
        connections = [
            ConnectionResponseSafe(
                id=r.id,
                name=r.name,
                description=r.description,
                created_at=r.created_at,
                updated_at=r.updated_at,
            )
            for r in records
        ]
        return ConnectionListResponse(connections=connections, total=len(connections))
    except Exception as e:
        logger.error(f"Error listing connections: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to list connections")


@connections_router.get("/{connection_id}", response_model=ConnectionResponse)
async def get_connection_endpoint(connection_id: str):
    """Get a connection by ID (includes database_url)."""
    record = get_connection(connection_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")
    return ConnectionResponse(
        id=record.id,
        name=record.name,
        description=record.description,
        database_url=record.database_url,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


@connections_router.put("/{connection_id}", response_model=ConnectionResponse)
async def update_connection_endpoint(connection_id: str, connection: ConnectionUpdate):
    """Update a connection."""
    record = update_connection(
        connection_id=connection_id,
        name=connection.name,
        database_url=connection.database_url,
        description=connection.description,
    )
    if not record:
        raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")
    return ConnectionResponse(
        id=record.id,
        name=record.name,
        description=record.description,
        database_url=record.database_url,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


@connections_router.delete("/{connection_id}", status_code=204)
async def delete_connection_endpoint(connection_id: str):
    """Delete a connection."""
    deleted = delete_connection(connection_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")
    return None


@connections_router.post("/{connection_id}/test", response_model=ConnectionTestResponse)
async def test_connection_endpoint(connection_id: str):
    """Test a connection to verify it works."""
    record = get_connection(connection_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Connection '{connection_id}' not found")

    success = test_target_connection(record.database_url)
    return ConnectionTestResponse(
        success=success,
        message="Connection successful" if success else "Connection failed",
    )


# =============================================================================
# Source Routes
# =============================================================================

@sources_router.post("", response_model=SourceResponse, status_code=201)
async def create_source_endpoint(source: SourceCreate):
    """Create a new SFTP source."""
    try:
        record = create_source(
            name=source.name,
            host=source.host,
            port=source.port,
            username=source.username,
            password=source.password,
            key_path=source.key_path,
            remote_path=source.remote_path,
            description=source.description,
        )
        return SourceResponse(
            id=record.id,
            name=record.name,
            description=record.description,
            host=record.host,
            port=record.port,
            username=record.username,
            password=record.password,
            key_path=record.key_path,
            remote_path=record.remote_path,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating source: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to create source")


@sources_router.get("", response_model=SourceListResponse)
async def list_sources_endpoint():
    """List all sources (without sensitive data)."""
    try:
        records = list_sources()
        sources = [
            SourceResponseSafe(
                id=r.id,
                name=r.name,
                description=r.description,
                host=r.host,
                port=r.port,
                username=r.username,
                remote_path=r.remote_path,
                created_at=r.created_at,
                updated_at=r.updated_at,
            )
            for r in records
        ]
        return SourceListResponse(sources=sources, total=len(sources))
    except Exception as e:
        logger.error(f"Error listing sources: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to list sources")


@sources_router.get("/{source_id}", response_model=SourceResponse)
async def get_source_endpoint(source_id: str):
    """Get a source by ID (includes sensitive data)."""
    record = get_source(source_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found")
    return SourceResponse(
        id=record.id,
        name=record.name,
        description=record.description,
        host=record.host,
        port=record.port,
        username=record.username,
        password=record.password,
        key_path=record.key_path,
        remote_path=record.remote_path,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


@sources_router.put("/{source_id}", response_model=SourceResponse)
async def update_source_endpoint(source_id: str, source: SourceUpdate):
    """Update a source."""
    record = update_source(
        source_id=source_id,
        name=source.name,
        host=source.host,
        port=source.port,
        username=source.username,
        password=source.password,
        key_path=source.key_path,
        remote_path=source.remote_path,
        description=source.description,
    )
    if not record:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found")
    return SourceResponse(
        id=record.id,
        name=record.name,
        description=record.description,
        host=record.host,
        port=record.port,
        username=record.username,
        password=record.password,
        key_path=record.key_path,
        remote_path=record.remote_path,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


@sources_router.delete("/{source_id}", status_code=204)
async def delete_source_endpoint(source_id: str):
    """Delete a source."""
    deleted = delete_source(source_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found")
    return None


@sources_router.post("/{source_id}/test", response_model=SourceTestResponse)
async def test_source_endpoint(source_id: str):
    """Test an SFTP source connection."""
    record = get_source(source_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Source '{source_id}' not found")

    success, file_count, error = test_sftp_source(record)
    return SourceTestResponse(
        success=success,
        message="Connection successful" if success else f"Connection failed: {error}",
        file_count=file_count,
    )


# =============================================================================
# Project Routes
# =============================================================================

@projects_router.post("", response_model=ProjectResponse, status_code=201)
async def create_project_endpoint(project: ProjectCreate):
    """Create a new project configuration."""
    try:
        config_dict = project.config.model_dump(by_alias=True, exclude_none=True)
        record = create_project(
            name=project.name,
            config=config_dict,
            connection_id=project.connection_id,
            source_id=project.source_id,
        )
        return ProjectResponse(
            id=record.id,
            name=record.name,
            connection_id=record.connection_id,
            source_id=record.source_id,
            config=record.config,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating project: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to create project")


@projects_router.get("", response_model=ProjectListResponse)
async def list_projects_endpoint():
    """List all projects."""
    try:
        records = list_projects()
        projects = [
            ProjectResponse(
                id=r.id,
                name=r.name,
                connection_id=r.connection_id,
                source_id=r.source_id,
                config=r.config,
                created_at=r.created_at,
                updated_at=r.updated_at,
            )
            for r in records
        ]
        return ProjectListResponse(projects=projects, total=len(projects))
    except Exception as e:
        logger.error(f"Error listing projects: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to list projects")


@projects_router.get("/{name}", response_model=ProjectResponse)
async def get_project_endpoint(name: str):
    """Get a project by name."""
    record = get_project(name)
    if not record:
        raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
    return ProjectResponse(
        id=record.id,
        name=record.name,
        connection_id=record.connection_id,
        source_id=record.source_id,
        config=record.config,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


@projects_router.put("/{name}", response_model=ProjectResponse)
async def update_project_endpoint(name: str, project: ProjectUpdate):
    """Update a project's configuration, connection, and/or source."""
    config_dict = None
    if project.config:
        config_dict = project.config.model_dump(by_alias=True, exclude_none=True)
    record = update_project(
        name,
        config=config_dict,
        connection_id=project.connection_id,
        source_id=project.source_id,
    )
    if not record:
        raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
    return ProjectResponse(
        id=record.id,
        name=record.name,
        connection_id=record.connection_id,
        source_id=record.source_id,
        config=record.config,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


@projects_router.delete("/{name}", status_code=204)
async def delete_project_endpoint(name: str):
    """Delete a project."""
    deleted = delete_project(name)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
    return None


# =============================================================================
# Import Routes
# =============================================================================

def _import_file(file_path: str, table_config, database_url: str):
    """Dispatch to the appropriate importer based on file extension."""
    from src.db.importer import import_csv, import_dbf
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".dbf":
        return import_dbf(
            file_path=file_path,
            table_name=table_config.target_table,
            primary_key=table_config.primary_key,
            column_mapping=table_config.column_mapping,
            rebuild_table=table_config.rebuild_table,
            schema=table_config.db_schema,
            encoding=table_config.encoding if table_config.encoding != "utf-8" else None,
            datestyle=table_config.datestyle,
            database_url=database_url,
        )
    else:
        return import_csv(
            file_path=file_path,
            table_name=table_config.target_table,
            primary_key=table_config.primary_key,
            column_mapping=table_config.column_mapping,
            rebuild_table=table_config.rebuild_table,
            schema=table_config.db_schema,
            delimiter=table_config.delimiter,
            encoding=table_config.encoding,
            skiprows=table_config.skiprows,
            datestyle=table_config.datestyle,
            database_url=database_url,
        )


def run_import_job(job_id: str, project_name: str, request: ImportRequest):
    """
    Background task to run an import job.

    This function runs the actual import and updates the job record
    in the management database.
    """
    from datetime import datetime

    from src.config.loader import load_config_from_dict
    from src.config.models import SFTPConfig
    from src.services.webhook import send_webhook, WebhookPayload

    logger.info(f"Starting background import job {job_id}")

    # Update job status to running
    update_job_status(job_id, "running", started_at=datetime.utcnow())

    files_processed = 0
    files_failed = 0
    total_inserted = 0
    total_updated = 0
    total_skipped = 0
    database_url = None

    try:
        # Load project config
        project = get_project(project_name)
        if not project:
            raise ValueError(f"Project '{project_name}' not found in database")

        # Inject project name into config (stored separately in DB row)
        config_dict = project.config.copy() if project.config else {}
        config_dict['project'] = project_name
        config = load_config_from_dict(config_dict)

        # Get database URL from connection (required)
        if not project.connection_id:
            raise ValueError(f"Project '{project_name}' has no connection configured")

        connection = get_connection(project.connection_id)
        if not connection:
            raise ValueError(f"Connection '{project.connection_id}' not found")

        database_url = connection.database_url
        logger.info(f"Using connection '{connection.name}' for import")

        # Initialize hook system
        hooks_config = config.get_effective_hooks()
        hook_engine = HookEngine(hooks_config)

        # Log deprecation warning if legacy refresh_materialized_views is used
        if config.refresh_materialized_views:
            logger.warning(
                f"Project '{project_name}' uses deprecated refresh_materialized_views. "
                "Migrate to hooks.post_import with type: refresh_views."
            )

        # Determine files to process
        local_files = request.local_files

        if local_files:
            # Process local files directly
            base_path = request.local_files_base_path

            # Validate files exist first
            valid_file_paths = []
            for file_path in local_files:
                if not os.path.exists(file_path):
                    add_job_file(job_id, os.path.basename(file_path), error="File not found")
                    add_job_error(job_id, f"File not found: {file_path}", "FileNotFound")
                    files_failed += 1
                else:
                    valid_file_paths.append(file_path)

            # Create temp_dir for hook file transformations (if needed)
            temp_dir = base_path or tempfile.mkdtemp(prefix=f"cpi_local_{job_id}_")

            # Initialize hook context with file paths
            hook_context = HookContext(
                job_id=job_id,
                project_name=project_name,
                database_url=database_url,
                temp_dir=temp_dir,
                file_paths=list(valid_file_paths),
            )

            # Execute post_file_prepare hooks (e.g., DBF->CSV conversion)
            if hooks_config.post_file_prepare:
                hook_result = hook_engine.execute_hooks(
                    HookPoint.POST_FILE_PREPARE, hook_context
                )
                # Record hook errors
                for hr in hook_result.results:
                    if not hr.success:
                        add_job_error(job_id, hr.error or "Hook failed", "HookError")

                if hook_result.should_abort:
                    logger.error(f"Job {job_id} aborted due to critical hook failure")
                    raise ValueError("Import aborted: critical hook failure in post_file_prepare")

            # Import loop uses context.file_paths (may have been transformed by hooks)
            for file_path in hook_context.file_paths:
                # Compute relative path for pattern matching (supports path patterns)
                if base_path:
                    try:
                        relative_path = os.path.relpath(file_path, base_path)
                        # Normalize to forward slashes for pattern matching
                        relative_path = relative_path.replace("\\", "/")
                    except ValueError:
                        # relpath fails on Windows if paths are on different drives
                        relative_path = os.path.basename(file_path)
                else:
                    relative_path = os.path.basename(file_path)

                table_config = config.get_table_for_file(relative_path)

                if not table_config:
                    add_job_file(job_id, relative_path, error="No matching table configuration")
                    files_failed += 1
                    continue

                try:
                    result = _import_file(file_path, table_config, database_url)

                    add_job_file(
                        job_id,
                        relative_path,
                        table_name=table_config.target_table,
                        inserted=result.inserted,
                        updated=result.updated,
                        skipped=result.skipped,
                        success=result.success,
                        error="; ".join(result.errors) if result.errors else None,
                    )

                    if result.success:
                        files_processed += 1
                        total_inserted += result.inserted
                        total_updated += result.updated
                        total_skipped += result.skipped
                    else:
                        files_failed += 1

                except Exception as e:
                    add_job_file(job_id, relative_path, table_name=table_config.target_table, error=str(e))
                    add_job_error(job_id, str(e), "ImportError")
                    files_failed += 1

        else:
            # SFTP workflow
            from src.sftp import SFTPClient

            # SFTP config resolution order:
            # 1. sftp_override in request (highest priority)
            # 2. source_id on project (load from cpi_sources)
            # 3. inline sftp config in project.config
            sftp_config = None
            if request.sftp_override:
                sftp_config = SFTPConfig(**request.sftp_override.model_dump())
            elif project.source_id:
                source = get_source(project.source_id)
                if source:
                    sftp_config = SFTPConfig(
                        host=source.host,
                        port=source.port,
                        username=source.username,
                        password=source.password,
                        key_path=source.key_path,
                        remote_path=source.remote_path,
                    )
                    logger.info(f"Using source '{source.name}' for SFTP")
                else:
                    logger.warning(f"Source '{project.source_id}' not found, falling back to inline config")

            if not sftp_config and config.sftp:
                sftp_config = config.sftp

            if not sftp_config:
                raise ValueError("No SFTP configuration available (set source_id on project or provide inline sftp config)")

            with SFTPClient(sftp_config) as sftp:
                pattern = "*.csv"
                if config.defaults:
                    pattern = config.defaults.download_pattern or config.defaults.file_pattern

                download_result = sftp.download_matching_files(pattern)

                # Download companion files (e.g., .fpt memo files for .dbf)
                companion_extensions = []
                if config.defaults and config.defaults.companion_extensions:
                    companion_extensions = list(config.defaults.companion_extensions)

                if companion_extensions and download_result.remote_files:
                    companion_paths = sftp.download_companion_files(
                        remote_files=download_result.remote_files,
                        companion_extensions=companion_extensions,
                        temp_dir=download_result.temp_dir,
                    )
                    if companion_paths:
                        logger.info(f"Downloaded {len(companion_paths)} companion file(s)")

                for error in download_result.errors:
                    add_job_error(job_id, error, "SFTPError")

                # Use temp_dir as base for computing relative paths (supports path patterns)
                sftp_base_path = download_result.temp_dir

                # Initialize hook context with downloaded file paths
                hook_context = HookContext(
                    job_id=job_id,
                    project_name=project_name,
                    database_url=database_url,
                    temp_dir=sftp_base_path,
                    file_paths=list(download_result.local_paths),
                )

                # Execute post_file_prepare hooks (e.g., DBF->CSV conversion)
                if hooks_config.post_file_prepare:
                    hook_result = hook_engine.execute_hooks(
                        HookPoint.POST_FILE_PREPARE, hook_context
                    )
                    # Record hook errors
                    for hr in hook_result.results:
                        if not hr.success:
                            add_job_error(job_id, hr.error or "Hook failed", "HookError")

                    if hook_result.should_abort:
                        logger.error(f"Job {job_id} aborted due to critical hook failure")
                        raise ValueError("Import aborted: critical hook failure in post_file_prepare")

                # Import loop uses context.file_paths (may have been transformed by hooks)
                for file_path in hook_context.file_paths:
                    # Compute relative path for pattern matching
                    if sftp_base_path:
                        try:
                            relative_path = os.path.relpath(file_path, sftp_base_path)
                            relative_path = relative_path.replace("\\", "/")
                        except ValueError:
                            relative_path = os.path.basename(file_path)
                    else:
                        relative_path = os.path.basename(file_path)

                    table_config = config.get_table_for_file(relative_path)

                    if not table_config:
                        add_job_file(job_id, relative_path, error="No matching table configuration")
                        files_failed += 1
                        continue

                    try:
                        result = _import_file(file_path, table_config, database_url)

                        add_job_file(
                            job_id,
                            relative_path,
                            table_name=table_config.target_table,
                            inserted=result.inserted,
                            updated=result.updated,
                            skipped=result.skipped,
                            success=result.success,
                            error="; ".join(result.errors) if result.errors else None,
                        )

                        if result.success:
                            files_processed += 1
                            total_inserted += result.inserted
                            total_updated += result.updated
                            total_skipped += result.skipped
                        else:
                            files_failed += 1

                    except Exception as e:
                        add_job_file(job_id, relative_path, table_name=table_config.target_table, error=str(e))
                        add_job_error(job_id, str(e), "ImportError")
                        files_failed += 1

                # Update hook context with file processing results for SFTP path
                hook_context.files_processed = files_processed
                hook_context.files_failed = files_failed
                hook_context.total_inserted = total_inserted
                hook_context.total_updated = total_updated

        # Determine final status first (needed for hook context)
        if files_failed == 0 and files_processed > 0:
            status = "completed"
        elif files_processed > 0 and files_failed > 0:
            status = "partial"
        else:
            status = "failed"

        # Execute post_import hooks (refresh views, run SQL, etc.)
        if hooks_config.post_import and database_url:
            # Create/update hook context for post_import hooks
            # Use existing context if SFTP, create new one if local files
            if not local_files:
                # SFTP path - context already exists, update status
                hook_context.status = status
            else:
                # Local files path - context exists from earlier, update it
                hook_context.files_processed = files_processed
                hook_context.files_failed = files_failed
                hook_context.total_inserted = total_inserted
                hook_context.total_updated = total_updated
                hook_context.status = status

            post_result = hook_engine.execute_hooks(HookPoint.POST_IMPORT, hook_context)
            for hr in post_result.results:
                if not hr.success:
                    add_job_error(job_id, hr.error or "Hook failed", "HookError")

        # Update job with final status
        update_job_status(
            job_id,
            status,
            completed_at=datetime.utcnow(),
            files_processed=files_processed,
            files_failed=files_failed,
            total_inserted=total_inserted,
            total_updated=total_updated,
            total_skipped=total_skipped,
        )

        logger.info(f"Job {job_id} completed: {status}")

        # Send webhook callback
        if request.callback_url:
            job_record = get_job(job_id)
            if job_record:
                duration = None
                if job_record.started_at and job_record.completed_at:
                    duration = (job_record.completed_at - job_record.started_at).total_seconds()

                errors = [e.message for e in get_job_errors(job_id)]
                payload = WebhookPayload(
                    job_id=job_id,
                    project=project_name,
                    status=status,
                    files_processed=files_processed,
                    files_failed=files_failed,
                    total_inserted=total_inserted,
                    total_updated=total_updated,
                    total_skipped=total_skipped,
                    errors=errors,
                    duration_seconds=duration,
                )
                send_webhook(request.callback_url, payload)

    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}", exc_info=True)
        add_job_error(job_id, str(e), "JobError")
        update_job_status(
            job_id,
            "failed",
            completed_at=datetime.utcnow(),
            files_processed=files_processed,
            files_failed=files_failed,
            total_inserted=total_inserted,
            total_updated=total_updated,
            total_skipped=total_skipped,
        )


@import_router.post("", response_model=ImportResponse, status_code=202)
async def start_import(request: ImportRequest, background_tasks: BackgroundTasks):
    """
    Start an import job.

    The import runs as a background task. Use GET /jobs/{job_id} to check status.
    """
    # Verify project exists in database
    project = get_project(request.project)
    if not project:
        raise HTTPException(
            status_code=404,
            detail=f"Project '{request.project}' not found"
        )

    # Verify project has a connection configured
    if not project.connection_id:
        raise HTTPException(
            status_code=400,
            detail=f"Project '{request.project}' has no connection configured"
        )

    # Verify connection exists
    connection = get_connection(project.connection_id)
    if not connection:
        raise HTTPException(
            status_code=400,
            detail=f"Connection '{project.connection_id}' not found"
        )

    # Create job record
    job_record = create_job(
        project_name=request.project,
        callback_url=request.callback_url,
    )

    # Start background task
    background_tasks.add_task(run_import_job, job_record.id, request.project, request)

    return ImportResponse(
        job_id=job_record.id,
        project=request.project,
        status="pending",
        message="Import job started. Use GET /jobs/{job_id} to check status.",
    )


# =============================================================================
# Job Routes
# =============================================================================

@jobs_router.get("", response_model=JobListResponse)
async def list_jobs_endpoint(
    project: Optional[str] = Query(None, description="Filter by project name"),
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(50, ge=1, le=100, description="Maximum number of jobs"),
    offset: int = Query(0, ge=0, description="Number of jobs to skip"),
):
    """List jobs with optional filtering."""
    records = list_jobs(project_name=project, status=status, limit=limit, offset=offset)

    jobs = []
    for r in records:
        duration = None
        if r.started_at and r.completed_at:
            duration = (r.completed_at - r.started_at).total_seconds()

        jobs.append(JobResponse(
            id=r.id,
            project_name=r.project_name,
            status=r.status,
            started_at=r.started_at,
            completed_at=r.completed_at,
            duration_seconds=duration,
            files_processed=r.files_processed,
            files_failed=r.files_failed,
            total_inserted=r.total_inserted,
            total_updated=r.total_updated,
            total_skipped=r.total_skipped,
            callback_url=r.callback_url,
            schedule_id=r.schedule_id,
            created_at=r.created_at,
        ))

    return JobListResponse(jobs=jobs, total=len(jobs))


@jobs_router.get("/{job_id}", response_model=JobResponse)
async def get_job_endpoint(job_id: str, include_details: bool = Query(True)):
    """
    Get job status and results.

    Set include_details=true to include file results and errors.
    """
    record = get_job(job_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

    duration = None
    if record.started_at and record.completed_at:
        duration = (record.completed_at - record.started_at).total_seconds()

    response = JobResponse(
        id=record.id,
        project_name=record.project_name,
        status=record.status,
        started_at=record.started_at,
        completed_at=record.completed_at,
        duration_seconds=duration,
        files_processed=record.files_processed,
        files_failed=record.files_failed,
        total_inserted=record.total_inserted,
        total_updated=record.total_updated,
        total_skipped=record.total_skipped,
        callback_url=record.callback_url,
        schedule_id=record.schedule_id,
        created_at=record.created_at,
    )

    if include_details:
        # Include file results
        file_records = get_job_files(job_id)
        response.file_results = [
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
        ]

        # Include errors
        error_records = get_job_errors(job_id)
        response.errors = [
            JobErrorResponse(
                error_type=e.error_type,
                message=e.message,
                created_at=e.created_at,
            )
            for e in error_records
        ]

    return response


# =============================================================================
# Health Check Route
# =============================================================================

@health_router.get("/health", response_model=HealthResponse)
async def health_check():
    """Check system health."""
    management_db = test_management_connection()

    status = "healthy" if management_db else "unhealthy"

    return HealthResponse(
        status=status,
        management_db=management_db,
    )
