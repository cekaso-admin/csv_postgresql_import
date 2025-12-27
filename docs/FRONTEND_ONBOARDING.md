# Frontend Developer Onboarding

Welcome! This document explains how the CSV Import API works so you can build a frontend for it.

## What This System Does

This is a **CSV to PostgreSQL import service**. Users can:

1. Configure database connections (where to import data)
2. Create projects (how to import data)
3. Run import jobs (actually import CSV files)
4. Monitor job progress and results

Think of it like a data pipeline tool: CSV files go in, database rows come out.

---

## Core Concepts

### 1. Connections

A **Connection** represents a PostgreSQL database where CSV data will be imported.

**Key points:**
- Connections are reusable across multiple projects
- They store database credentials (sensitive data)
- A connection must be tested before use to verify it works
- When listing connections, credentials are hidden for security
- When viewing a single connection, credentials are shown

**Typical user flow:**
1. User enters database credentials (host, port, user, password, database)
2. System stores them as a connection with a friendly name
3. User tests the connection to verify it works
4. Connection can now be used in projects

**Data model:**
```
Connection
├── id (UUID)
├── name (unique, user-friendly identifier)
├── description (optional)
├── database_url (the actual PostgreSQL connection string)
├── created_at
└── updated_at
```

---

### 2. Sources

A **Source** represents an SFTP server where CSV files can be pulled from.

**Key points:**
- Sources are reusable across multiple projects
- They store SFTP credentials (host, username, password or SSH key)
- A source must be tested before use to verify it works
- When listing sources, passwords are hidden for security
- When viewing a single source, passwords are shown
- Projects can reference a source by ID instead of embedding SFTP config inline

**Typical user flow:**
1. User enters SFTP credentials (host, port, username, password/key, remote path)
2. System stores them as a source with a friendly name
3. User tests the source to verify it connects
4. Source can now be linked to projects

**Data model:**
```
Source
├── id (UUID)
├── name (unique, user-friendly identifier)
├── description (optional)
├── host (SFTP server hostname)
├── port (default: 22)
├── username
├── password (optional, hidden in list view)
├── key_path (optional, path to SSH private key)
├── remote_path (directory to pull files from)
├── created_at
└── updated_at
```

---

### 3. Projects

A **Project** defines HOW to import CSV files. It links a connection and optionally a source to import rules.

**Key points:**
- Each project has exactly one connection (or none, but then it can't run imports)
- Each project can optionally have one source (for SFTP imports)
- Projects contain configuration for file matching, table naming, CSV parsing
- A project can handle many different CSV files using pattern matching
- SFTP config resolution order: request override > source_id > inline config

**Project configuration has two modes:**

#### Auto-Discovery Mode (recommended for many similar files)

When you have 50+ CSV files with similar structure:

```
defaults:
  file_pattern: "*.csv"      # Match all CSV files
  primary_key: "id"          # Use 'id' column for upserts
  delimiter: ","             # CSV separator
  encoding: "utf-8"          # File encoding

table_naming:
  strip_prefix: "Export_"    # Remove prefix from filename
  strip_suffix: "_data"      # Remove suffix from filename
  lowercase: true            # Convert to lowercase
```

Example: `Export_Customers_data.csv` → table `customers`

#### Explicit Mode (for specific file-to-table mappings)

When you need precise control:

```
tables:
  - file_pattern: "customers_*.csv"
    target_table: "customers"
    primary_key: ["customer_id", "region"]
    column_mapping:
      "Kunde Nr.": "customer_id"
      "Name": "customer_name"
```

**Data model:**
```
Project
├── id (UUID)
├── name (unique, user-friendly identifier)
├── connection_id (FK to Connection, can be null)
├── source_id (FK to Source, can be null)
├── config (JSON object with all import settings)
├── created_at
└── updated_at
```

---

### 4. Import Jobs

A **Job** is a single import execution. When you run an import, a job is created.

**Key points:**
- Jobs run asynchronously (in the background)
- You start a job and get back a job_id immediately
- Poll the job status to see progress
- Jobs track per-file results and errors

**Job lifecycle:**

```
pending → running → completed/partial/failed
```

| Status | Meaning |
|--------|---------|
| `pending` | Job created, not started yet |
| `running` | Import in progress |
| `completed` | All files imported successfully |
| `partial` | Some files succeeded, some failed |
| `failed` | All files failed or critical error |

**Job can import files from two sources:**

1. **Local files**: Provide file paths on the server
2. **SFTP**: Pull files from remote SFTP server (configured in project)

**Data model:**
```
Job
├── id (UUID)
├── project_name
├── status (pending/running/completed/partial/failed)
├── started_at
├── completed_at
├── files_processed (count)
├── files_failed (count)
├── total_inserted (row count)
├── total_updated (row count)
├── callback_url (optional webhook)
├── created_at
├── file_results[] (details per file)
└── errors[] (error messages)
```

**File Result:**
```
JobFile
├── filename
├── table_name (where it was imported)
├── inserted (row count)
├── updated (row count)
├── success (boolean)
└── error (message if failed)
```

---

## User Workflows

### Workflow 1: First-Time Setup

```
┌─────────────────┐     ┌─────────────────┐
│  Create         │     │  Create         │
│  Connection     │     │  Source         │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│  Test           │     │  Test           │
│  Connection     │     │  Source         │
└────────┬────────┘     └────────┬────────┘
         │                       │
         └───────────┬───────────┘
                     │
                     ▼
          ┌─────────────────┐
          │  Create         │
          │  Project        │
          │  (link both)    │
          └────────┬────────┘
                   │
                   ▼
          ┌─────────────────┐
          │  Ready to       │
          │  Import!        │
          └─────────────────┘
```

### Workflow 2: Running an Import

```
┌─────────────────┐
│  Select         │
│  Project        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Start Import   │
│  (get job_id)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Poll Job       │◄──────┐
│  Status         │       │
└────────┬────────┘       │
         │                │
         ▼                │
    ┌────────────┐        │
    │  Running?  │────Yes─┘
    └────────────┘
         │ No
         ▼
┌─────────────────┐
│  Show Results   │
│  (success/fail) │
└─────────────────┘
```

### Workflow 3: Monitoring Dashboard

```
┌─────────────────────────────────────────┐
│  Jobs List (recent)                     │
│  ┌────────────────────────────────────┐ │
│  │ Job 1 - customer-import - completed│ │
│  │ Job 2 - sales-data - running       │ │
│  │ Job 3 - inventory - failed         │ │
│  └────────────────────────────────────┘ │
│                                         │
│  Click job → Show details:              │
│  - Duration                             │
│  - Files processed                      │
│  - Rows inserted/updated                │
│  - Errors (if any)                      │
└─────────────────────────────────────────┘
```

---

## API Authentication

All API requests (except `/health` and `/docs`) require the `X-API-Key` header.

```
X-API-Key: your-api-key-here
```

**Error responses:**
- `401 Unauthorized`: No API key provided
- `403 Forbidden`: Invalid API key

---

## API Endpoints Summary

### Connections

| Action | Method | Endpoint | Notes |
|--------|--------|----------|-------|
| List | GET | `/connections` | Returns list WITHOUT database_url |
| Create | POST | `/connections` | |
| Get | GET | `/connections/{id}` | Returns WITH database_url |
| Update | PUT | `/connections/{id}` | |
| Delete | DELETE | `/connections/{id}` | |
| Test | POST | `/connections/{id}/test` | Returns success/failure |

### Sources

| Action | Method | Endpoint | Notes |
|--------|--------|----------|-------|
| List | GET | `/sources` | Returns list WITHOUT password |
| Create | POST | `/sources` | |
| Get | GET | `/sources/{id}` | Returns WITH password |
| Update | PUT | `/sources/{id}` | |
| Delete | DELETE | `/sources/{id}` | |
| Test | POST | `/sources/{id}/test` | Returns success/failure + file count |

### Projects

| Action | Method | Endpoint | Notes |
|--------|--------|----------|-------|
| List | GET | `/projects` | |
| Create | POST | `/projects` | Include connection_id and optionally source_id |
| Get | GET | `/projects/{name}` | By name, not ID |
| Update | PUT | `/projects/{name}` | |
| Delete | DELETE | `/projects/{name}` | |

### Jobs

| Action | Method | Endpoint | Notes |
|--------|--------|----------|-------|
| List | GET | `/jobs` | Supports filtering |
| Start | POST | `/import` | Returns job_id immediately |
| Get | GET | `/jobs/{id}` | Includes file results if done |

### Utility

| Action | Method | Endpoint | Notes |
|--------|--------|----------|-------|
| Health | GET | `/health` | No auth required |
| Docs | GET | `/docs` | Swagger UI, no auth |

---

## Important Business Rules

### Connections

1. Connection names must be unique
2. Cannot delete a connection that is used by a project (or it becomes null)
3. Database URL should be validated format before saving

### Sources

1. Source names must be unique
2. Cannot delete a source that is used by a project (or it becomes null)
3. Either password OR key_path should be provided, not both
4. Test source connection before allowing it to be used in projects

### Projects

1. Project names must be unique
2. A project MUST have a connection_id to run imports
3. A project can optionally have a source_id for SFTP imports
4. Project config is flexible JSON - validate on frontend for better UX
5. SFTP config resolution: request override > source_id > inline config

### Jobs

1. Cannot start import if project has no connection
2. Jobs are immutable once created (no update/delete)
3. Job status should be polled (suggest: every 2-3 seconds while running)
4. Jobs can be filtered by project name and status

---

## Suggested UI Components

### Connection Form
- Name (text, required, unique)
- Description (textarea, optional)
- Database URL builder:
  - Host (text)
  - Port (number, default 5432)
  - Database (text)
  - Username (text)
  - Password (password)
- "Test Connection" button
- Show success/error feedback

### Source Form
- Name (text, required, unique)
- Description (textarea, optional)
- Host (text, required)
- Port (number, default 22)
- Username (text, required)
- Authentication mode toggle (Password / SSH Key)
  - Password (password field)
  - Key Path (text, path to SSH key file)
- Remote Path (text, default "/")
- "Test Connection" button
- Show success/error feedback (includes file count on success)

### Project Form
- Name (text, required, unique)
- Connection (dropdown of existing connections)
- Source (dropdown of existing sources, optional - for SFTP imports)
- Configuration mode toggle (Auto-discovery / Explicit)
- Auto-discovery fields:
  - File pattern (text, default "*.csv")
  - Primary key (text, default "id")
  - Delimiter (text, default ",")
  - Encoding (dropdown: utf-8, latin-1, etc.)
  - Skip rows (number, default 0)
  - Table naming rules
- Explicit mode:
  - Table list with add/remove
  - Each table: pattern, target, primary key, column mapping

### Job Monitor
- List view with status badges (color-coded)
- Filter by project, status
- Auto-refresh toggle for running jobs
- Detail view showing:
  - Summary stats
  - File-by-file breakdown
  - Error messages (expandable)
  - Duration

### Dashboard
- Recent jobs (last 10)
- Quick stats: total imports today, success rate
- Connections count, Sources count, Projects count

---

## Error Handling

Common API errors to handle:

| Code | Meaning | User Action |
|------|---------|-------------|
| 400 | Bad request / validation error | Show field errors |
| 401 | No API key | Check configuration |
| 403 | Invalid API key | Check API key |
| 404 | Resource not found | Item was deleted |
| 409 | Conflict (duplicate name) | Choose different name |
| 500 | Server error | Retry or contact admin |

---

## Development Tips

1. **Swagger UI**: Use `/docs` to explore and test the API interactively

2. **Polling jobs**: When a job is `running`, poll every 2-3 seconds. Stop when status changes.

3. **Connection testing**: Always test a connection after creation before allowing it to be used in projects.

4. **Source testing**: Always test a source after creation. The test response includes file count which helps verify the remote path is correct.

5. **Sensitive data**: Never log or display full database URLs or passwords in production. The list endpoints already hide them.

6. **Optimistic UI**: Job creation returns immediately - you can show "Job started" before polling confirms it.

7. **Pagination**: The jobs list supports `limit` and `offset` query params for pagination.

---

## Questions?

- API documentation: http://localhost:8000/docs
- Architecture decisions: See `context/decisions/` folder
- Implementation details: See `README.md`
