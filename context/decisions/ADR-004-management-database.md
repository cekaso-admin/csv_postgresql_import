# ADR-004: Management Database for API and Monitoring

**Status:** Accepted
**Date:** 2024-12-10

## Context

The CSV import system needs to:
1. Store project configurations persistently (not just YAML files)
2. Track job execution history for monitoring
3. Expose functionality via REST API for n8n integration
4. Support multiple projects, each with their own target database

Questions addressed:
- Where to store project configs and job history?
- Which database to use for management data?
- How to structure the API?

## Decision

### 1. Separate Management Database

Use a dedicated PostgreSQL database for management data, separate from project databases where CSV data is imported.

**Rationale:**
- Each project can have a DIFFERENT target database (different Supabase instances)
- Management data (configs, jobs, monitoring) is metadata ABOUT projects
- Keeps management concerns isolated from imported data
- PostgreSQL handles concurrent writes well (multiple simultaneous jobs)
- Supports analytical queries for dashboards

**Rejected alternatives:**
- **SQLite**: Simpler, but monitoring data can grow large and benefits from PostgreSQL's query capabilities
- **Store in project DBs**: Projects have different databases, no single source of truth
- **YAML files only**: No job history, no monitoring, harder to query

### 2. Table Naming Convention

All management tables use `cpi_` prefix (CSV PostgreSQL Import):

| Table | Purpose |
|-------|---------|
| `cpi_connections` | Database connection credentials (see ADR-005) |
| `cpi_projects` | Project configurations (JSONB) with connection_id FK |
| `cpi_jobs` | Job execution history |
| `cpi_job_files` | Per-file import results |
| `cpi_job_errors` | Job-level error logs |

**Rationale:**
- Avoids collision with user tables if sharing a database
- Clear namespace for management tables
- Easy to identify and backup management data

### 3. REST API Structure

FastAPI-based REST API with these endpoint groups:

```
/health           - System health check
/connections      - CRUD for database connections (see ADR-005)
/projects         - CRUD for project configs
/import           - Start import jobs
/jobs             - Job status and monitoring
```

**Key decisions:**
- **Background tasks**: Import jobs run as FastAPI BackgroundTasks, not blocking
- **Webhook callbacks**: Jobs notify n8n on completion via configurable callback URL
- **Connection required**: Projects must have a connection configured (see ADR-005)
- **Auto-init**: Management schema created on server startup

### 4. Project Config Storage

Store full project config as JSONB in `cpi_projects` table:

```sql
CREATE TABLE cpi_projects (
    id UUID PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    connection_id UUID REFERENCES cpi_connections(id) ON DELETE SET NULL,
    config JSONB NOT NULL,  -- Full config including SFTP, tables, defaults
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

**Rationale:**
- Flexible schema - config structure can evolve
- Easy to query specific config fields with PostgreSQL JSONB operators
- Single source of truth for project settings
- Connection reference enables reuse and separate management (ADR-005)

### 5. Job Monitoring Schema

Track job execution with related tables:

```sql
-- Job execution
cpi_jobs (id, project_id, status, started_at, completed_at, stats...)

-- Per-file results
cpi_job_files (id, job_id, filename, table_name, inserted, updated, error)

-- Job errors
cpi_job_errors (id, job_id, error_type, message, created_at)
```

**Status values:** pending, running, completed, partial, failed

**Rationale:**
- Separate tables allow efficient queries (list files for a job, count errors)
- Cascading deletes keep data consistent
- Indexes on project_id, status, created_at for common queries

## Configuration

```bash
# .env
MANAGEMENT_DATABASE_URL=postgresql://user:pass@host:5432/dbname
```

Note: URL-encode special characters in password (e.g., `@` → `%40`)

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check with DB status |
| `/connections` | GET | List connections (safe, no credentials) |
| `/connections` | POST | Create connection |
| `/connections/{id}` | GET | Get connection (with credentials) |
| `/connections/{id}` | PUT | Update connection |
| `/connections/{id}` | DELETE | Delete connection |
| `/connections/{id}/test` | POST | Test connection |
| `/projects` | GET | List all projects |
| `/projects` | POST | Create project (with connection_id) |
| `/projects/{name}` | GET | Get project config |
| `/projects/{name}` | PUT | Update project |
| `/projects/{name}` | DELETE | Delete project |
| `/import` | POST | Start import job (async) |
| `/jobs` | GET | List jobs (filterable) |
| `/jobs/{id}` | GET | Job status + file results |

## Consequences

### Positive

- Persistent project configs with API access
- Full job history for monitoring/debugging
- Multiple projects with different target databases
- n8n integration via webhook callbacks
- Swagger/OpenAPI docs at `/docs`

### Negative

- Requires separate Supabase/PostgreSQL for management
- Additional infrastructure to maintain
- Connection string must be URL-encoded for special characters

### Risks

- Management database becomes single point of failure (mitigate: use managed PostgreSQL like Supabase)
- Job history can grow large (mitigate: add retention policy, delete old jobs)

## Examples

### 1. Create connection

```bash
curl -X POST http://localhost:8000/connections \
  -H "Content-Type: application/json" \
  -d '{
    "name": "customer_abc_prod",
    "description": "Customer ABC production database",
    "database_url": "postgresql://user:pass@host:5432/dbname"
  }'
# Returns: {"id": "uuid-here", "name": "customer_abc_prod", ...}
```

### 2. Create project with connection

```bash
curl -X POST http://localhost:8000/projects \
  -H "Content-Type: application/json" \
  -d '{
    "name": "customer_abc",
    "connection_id": "uuid-from-step-1",
    "config": {
      "name": "customer_abc",
      "defaults": {
        "file_pattern": "*.csv",
        "primary_key": "id"
      }
    }
  }'
```

### 3. Start import job

```bash
curl -X POST http://localhost:8000/import \
  -H "Content-Type: application/json" \
  -d '{
    "project": "customer_abc",
    "callback_url": "https://n8n.example.com/webhook/xyz"
  }'
```

### 4. Check job status

```bash
curl http://localhost:8000/jobs/{job_id}
```
