# TASK-001: Implementation Plan

**Status:** Phase 5 Complete
**Created:** 2024-12-03
**Updated:** 2024-12-10

## Overview

Implement CSV to PostgreSQL import system with SFTP pull and n8n webhook integration.

## Project Structure

```
csv_postgresql_import/
├── src/
│   ├── __init__.py
│   ├── main.py              # FastAPI app entry point
│   ├── config/
│   │   ├── __init__.py      ✅
│   │   ├── loader.py        # YAML config loading ✅
│   │   └── models.py        # Pydantic models for config ✅
│   ├── db/
│   │   ├── __init__.py
│   │   ├── connection.py    # PostgreSQL connection management ✅
│   │   ├── importer.py      # Core import logic (COPY, upsert) ✅
│   │   └── schema.py        # Table creation, schema operations ✅
│   ├── sftp/
│   │   ├── __init__.py      ✅
│   │   └── client.py        # SFTP file pulling ✅
│   ├── api/
│   │   ├── __init__.py
│   │   ├── routes.py        # API endpoints
│   │   └── schemas.py       # Request/response Pydantic models
│   └── services/
│       ├── __init__.py      ✅
│       ├── import_job.py    # Orchestrates full import workflow ✅
│       └── webhook.py       # Webhook callback handler ✅
├── config/
│   └── example_project.yaml # Example project configuration ✅
├── tests/
│   └── ...
├── .env.example             # ✅
├── requirements.txt         # ✅
└── CLAUDE.md                # ✅
```

## Implementation Phases

---

### Phase 1: Core Database Operations ✅ COMPLETE

**Goal:** Import a local CSV into PostgreSQL with upsert logic.

#### 1.1 Database Connection (`src/db/connection.py`) ✅
- [x] Connection pool using psycopg2 (ThreadedConnectionPool)
- [x] Context manager for safe connection handling
- [x] Load connection string from environment
- [x] test_connection() helper function

#### 1.2 Schema Operations (`src/db/schema.py`) ✅
- [x] Check if table exists
- [x] Create table from CSV headers (all VARCHAR)
- [x] Create staging table (clone of target structure)
- [x] Get table columns for validation
- [x] Truncate table (for rebuild_table option)
- [x] Drop staging table

#### 1.3 Core Importer (`src/db/importer.py`) ✅
- [x] Stream CSV in chunks (configurable chunk size)
- [x] COPY to staging table for ALL files (simplified from hybrid approach)
- [x] Upsert from staging to target (ON CONFLICT DO UPDATE)
- [x] Handle TRUNCATE when rebuild_table=true
- [x] CSV options: delimiter, encoding, skiprows
- [x] Column mapping support
- [x] Composite primary key support

```python
# Final interface
result = import_csv(
    file_path="data.csv",
    table_name="my_table",
    primary_key="id",              # or ["id", "year"] for composite
    column_mapping={"Old": "new"}, # optional
    rebuild_table=False,           # optional, TRUNCATE before import
    delimiter="|",                 # optional, default ","
    encoding="latin-1",            # optional, default "utf-8"
    skiprows=1                     # optional, skip meta rows
)
# result = ImportResult(inserted=1500, updated=300, errors=[])
```

#### 1.1-1.3 Verification ✅
- [x] Test with small CSV (test_data.csv) - 3 rows
- [x] Test with large CSV (FlAbrKsLst.csv) - 65k rows, 98MB
- [x] Test upsert (run twice, verify updates)
- [x] Test table creation from new CSV
- [x] Test encoding conversion (Latin-1 → UTF-8)
- [x] Test custom delimiter (pipe |)
- [x] Test skiprows (meta info row)

**Performance:** 65k rows imported in ~25 seconds using COPY + staging strategy.

---

### Phase 2: Configuration System ✅ COMPLETE

**Goal:** Load per-project settings from YAML files.

#### 2.1 Config Models (`src/config/models.py`) ✅
- [x] Pydantic models for type-safe config
- [x] TableConfig: file_pattern, target_table, primary_key, column_mapping, rebuild_table, delimiter, encoding, skiprows, db_schema
- [x] ProjectConfig: project name, connection env var, tables list
- [x] SFTPConfig: host, port, username, key_path or password, remote_path
- [x] ConnectionConfig: env_var for database connection string
- [x] **DefaultsConfig**: Auto-discovery settings (file_pattern, primary_key, CSV options)
- [x] **TableNamingConfig**: Filename-to-table transformation (strip_prefix, strip_suffix, lowercase)

#### 2.2 Config Loader (`src/config/loader.py`) ✅
- [x] Load YAML file by project name
- [x] Validate against Pydantic models
- [x] Match CSV filename to table config (glob pattern)
- [x] list_available_projects() helper
- [x] load_config_from_dict() for programmatic config
- [x] **Auto-generate TableConfig from defaults + table_naming**

```python
# Final interface - supports both explicit and auto-discovery modes
config = load_project_config("customer_abc")

# Explicit mode: matches tables list
table_config = config.get_table_for_file("customers_2024.csv")

# Auto-discovery mode: generates config from defaults + table_naming
# IxExpKonto.csv → table "konto" with default settings
table_config = config.get_table_for_file("IxExpKonto.csv")
```

#### 2.3 Example Config ✅
- [x] Create `config/example_project.yaml` with documented options
- [x] Document auto-discovery mode with defaults + table_naming
- [x] Document explicit mode with tables list

#### 2.4 Auto-Discovery Mode (ADR-003) ✅
- [x] Support `defaults` section for shared settings across many files
- [x] Support `table_naming` for filename → table name transformation
- [x] Strip prefix/suffix from filenames (e.g., `IxExpKonto.csv` → `konto`)
- [x] Explicit `tables` entries override defaults

#### 2.1-2.4 Verification ✅
- [x] Test loading valid config
- [x] Test validation errors on invalid config
- [x] Test file pattern matching (fnmatch glob patterns)
- [x] Test auto-discovery with table naming transformation

---

### Phase 3: SFTP Client ✅ COMPLETE

**Goal:** Pull files from remote SFTP server.

#### 3.1 SFTP Client (`src/sftp/client.py`) ✅
- [x] Connect with password or SSH key authentication
- [x] List files in remote directory with glob pattern matching
- [x] Download files matching patterns
- [x] Download to temp directory (auto-created or custom)
- [x] Cleanup after processing (context manager)
- [x] Support multiple SSH key types (RSA, Ed25519, ECDSA, DSS)

```python
# Final interface
with SFTPClient(config.sftp) as sftp:
    # List files matching pattern
    files = sftp.list_files("*.csv")

    # Download to temp dir (auto-created, auto-cleaned)
    result = sftp.download_files(files)
    # result.local_paths, result.remote_files, result.temp_dir

    # Or combine both in one call
    result = sftp.download_matching_files("IxExp*.csv")

# test_connection(config) for quick connection test
```

#### 3.1 Verification ✅
- [x] Module imports correctly
- [x] SFTPConfig integrates with SFTPClient
- [x] DownloadResult tracks success/errors
- [ ] Live SFTP test (requires real server)

---

### Phase 4: Import Job Orchestration ✅ COMPLETE

**Goal:** Combine all pieces into a complete workflow.

#### 4.1 Import Job Service (`src/services/import_job.py`) ✅
- [x] Create job with unique UUID
- [x] Pull files from SFTP (via SFTPClient)
- [x] Match files to table configs (auto-discovery + explicit)
- [x] Import each file with proper config
- [x] Track statistics (files, rows inserted/updated, errors)
- [x] Cleanup temp files (via context manager)
- [x] Support local file mode (run_local)

```python
# Final interface
from src.services import ImportJob, run_import

# Full SFTP workflow
job = ImportJob(project="customer_abc", callback_url="https://...")
result = job.run()

# Local files only (no SFTP)
result = job.run_local(files=["file1.csv", "file2.csv"])

# Convenience function
result = run_import(project="customer_abc", local_files=["file.csv"])

# Result: JobResult with status, file_results, statistics
```

#### 4.2 Webhook Callback (`src/services/webhook.py`) ✅
- [x] POST JSON result to callback URL
- [x] Retry on failure (3 attempts with delay)
- [x] Log callback success/failure
- [x] Async version available (send_webhook_async)

#### 4.1-4.2 Verification ✅
- [x] Module imports correctly
- [x] Local workflow tested with real files
- [x] Auto-discovery generates correct table configs
- [x] Statistics tracked correctly (inserted, updated, errors)
- [ ] Live SFTP + webhook test (requires real servers)

---

### Phase 5: REST API + Management Database ✅ COMPLETE

**Goal:** Expose import functionality via FastAPI with persistent project configs and job monitoring.

#### 5.1 Management Database Schema (`src/db/management.py`) ✅
- [x] Connection to management PostgreSQL (separate from project DBs)
- [x] `cpi_connections` table: id, name, description, database_url, created_at, updated_at
- [x] `cpi_projects` table: id, name, connection_id (FK), config JSON, created_at, updated_at
- [x] `cpi_jobs` table: id, project_id, status, started_at, completed_at, stats
- [x] `cpi_job_files` table: id, job_id, filename, table_name, inserted, updated, error
- [x] `cpi_job_errors` table: id, job_id, error_type, message, timestamp
- [x] Auto-create tables on startup (with `cpi_` prefix)

#### 5.2 API Schemas (`src/api/schemas.py`) ✅
- [x] ConnectionCreate, ConnectionUpdate, ConnectionResponse, ConnectionResponseSafe
- [x] ProjectCreate, ProjectUpdate, ProjectResponse (with connection_id)
- [x] ImportRequest: project, sftp override, callback_url
- [x] ImportResponse: job_id, status
- [x] JobResponse: job_id, status, progress, result, file_results

#### 5.3 Connection CRUD Routes (`src/api/routes.py`) ✅
- [x] POST /connections - Create database connection
- [x] GET /connections - List all connections (safe, without database_url)
- [x] GET /connections/{id} - Get connection details (includes database_url)
- [x] PUT /connections/{id} - Update connection
- [x] DELETE /connections/{id} - Delete connection
- [x] POST /connections/{id}/test - Test connection

#### 5.4 Project CRUD Routes (`src/api/routes.py`) ✅
- [x] POST /projects - Create project config (with connection_id)
- [x] GET /projects - List all projects
- [x] GET /projects/{name} - Get project config
- [x] PUT /projects/{name} - Update project config
- [x] DELETE /projects/{name} - Delete project config

#### 5.5 Import/Job Routes (`src/api/routes.py`) ✅
- [x] POST /import - Start import job (background task)
- [x] GET /jobs - List recent jobs
- [x] GET /jobs/{job_id} - Get job status and results
- [x] GET /health - Health check

#### 5.6 Main App (`src/main.py`) ✅
- [x] FastAPI app setup
- [x] Lifespan handler (init management DB on startup)
- [x] Background task handling for imports
- [x] Error handling middleware
- [x] CORS configuration

#### 5.1-5.5 Verification ✅
- [x] Test project CRUD endpoints
- [x] Test health endpoint
- [ ] Test import job execution (requires SFTP server)
- [ ] Test with real n8n webhook

---

### Phase 6: Production Readiness

- [x] Logging throughout (structured, with context)
- [x] Error handling and graceful failures
- [x] Environment variable documentation (.env.example)
- [x] Update CLAUDE.md with run commands
- [ ] Basic integration test

---

## Progress Summary

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 1 | ✅ Complete | Core import working with COPY strategy |
| Phase 2 | ✅ Complete | Config system with Pydantic + YAML + auto-discovery |
| Phase 3 | ✅ Complete | SFTP client with paramiko |
| Phase 4 | ✅ Complete | Job orchestration + webhook |
| Phase 5 | ✅ Complete | REST API + Management DB (cpi_* tables) |
| Phase 6 | Partial | Logging done, tests pending |

## Key Design Decisions

1. **COPY-only strategy**: Removed hybrid approach. COPY + staging is faster for all file sizes.
2. **All VARCHAR columns**: Avoids type issues, casting done in views.
3. **Streaming chunks**: Memory-efficient, supports 3GB+ files.
4. **Staging table per import**: Isolated, auto-cleaned up.
5. **Pydantic config models**: Type-safe YAML config with validation and helpful error messages.
6. **Auto-discovery mode** (ADR-003): For projects with 150+ similar files, use defaults + table_naming instead of listing each file. Explicit tables override defaults.
7. **Management DB on PostgreSQL** (ADR-004): Using Supabase PostgreSQL instead of SQLite for monitoring data scalability.
8. **Separate connections table** (ADR-005): Database connections stored in `cpi_connections` table, not in env vars. Enables API management and reuse across projects. Connection is required for import jobs (no env var fallback).

## Notes

- Phase 1 tested with real 98MB file (65k rows, 120 columns)
- Upsert correctly tracks inserted vs updated using PostgreSQL xmax
- German characters (ß, ü, ä) correctly converted from Latin-1
