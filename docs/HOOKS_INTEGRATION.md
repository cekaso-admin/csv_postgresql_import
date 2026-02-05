# Hooks Integration Guide

This document explains the new **Hooks System** for frontend developers. Hooks allow users to run automated actions before and after CSV imports.

---

## What Are Hooks?

Hooks are **automated actions** that run at specific points during the import process. Think of them as plugins or extensions that can:

- **Transform files** before import (e.g., convert DBF files to CSV)
- **Run database operations** after import (e.g., refresh materialized views)
- **Execute custom scripts** for special requirements

**Example use case:** A customer has legacy DBF files from an old system. Instead of manually converting them, hooks automatically convert DBF → CSV before the import runs.

---

## Hook Lifecycle Points

Hooks can run at these points in the import process:

| Hook Point | When It Runs | Common Use Cases |
|------------|--------------|------------------|
| `pre_import` | Before any processing starts | Setup, validation, notifications |
| `post_file_prepare` | After files are downloaded (SFTP) or provided (local) | **File conversion** (DBF→CSV, Excel→CSV) |
| `pre_file_import` | Before each file is imported | Per-file validation |
| `post_file_import` | After each file is imported | Per-file notifications |
| `post_import` | After all files are processed | **Refresh views**, run SQL, webhooks |

**Most commonly used:** `post_file_prepare` (for file transformations) and `post_import` (for post-processing).

---

## Configuration Schema

Hooks are configured as part of the project's `config` object:

```json
{
  "name": "my_project",
  "connection_id": "uuid-here",
  "config": {
    "defaults": {
      "file_pattern": "*.csv",
      "primary_key": "ID"
    },
    "hooks": {
      "post_file_prepare": [
        {
          "type": "dbf_to_csv",
          "input_pattern": "*.dbf",
          "encoding": "auto",
          "delete_original": true,
          "on_error": "fail"
        }
      ],
      "post_import": [
        {
          "type": "refresh_views",
          "on_error": "warn"
        }
      ]
    }
  }
}
```

### Hook Action Schema

Each hook action has these common fields:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | *required* | The hook type (see Available Hook Types below) |
| `name` | string | null | Human-readable name for logging/display |
| `enabled` | boolean | `true` | Whether this hook is active |
| `on_error` | string | `"fail"` | Error handling: `"fail"`, `"warn"`, or `"ignore"` |
| `timeout_seconds` | integer | `300` | Maximum execution time (1-3600 seconds) |

**Error handling options:**
- `"fail"` - Stop execution, mark job as failed
- `"warn"` - Log warning, continue processing, record error in job
- `"ignore"` - Silently continue (not recommended for most cases)

---

## Available Hook Types

### 1. `dbf_to_csv` - Convert DBF Files

Converts dBASE/FoxPro DBF files to CSV format. Runs at `post_file_prepare`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `input_pattern` | string | `"*.dbf"` | Glob pattern for files to convert |
| `encoding` | string | `"auto"` | Source encoding (`"auto"`, `"cp850"`, `"cp1252"`, `"utf-8"`, etc.) |
| `delete_original` | boolean | `false` | Delete the original DBF file after conversion |

**Example:**
```json
{
  "type": "dbf_to_csv",
  "name": "Convert legacy DBF files",
  "input_pattern": "*.DBF",
  "encoding": "cp850",
  "delete_original": true,
  "on_error": "fail"
}
```

**Notes:**
- Auto-encoding tries common European encodings (cp850, cp437, cp1252, latin-1, utf-8)
- Column names are automatically sanitized for database compatibility
- Date, boolean, and numeric types are properly formatted

---

### 2. `refresh_views` - Refresh Materialized Views

Refreshes all PostgreSQL materialized views in the target database. Runs at `post_import`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `schema` | string | `"public"` | Database schema containing the views |

**Example:**
```json
{
  "type": "refresh_views",
  "name": "Refresh analytics views",
  "schema": "public",
  "on_error": "warn"
}
```

**Notes:**
- Views are refreshed in dependency order (base views first, then dependent views)
- Individual view failures don't stop other views from refreshing
- Recommended: Use `on_error: "warn"` so view failures don't fail the entire job

---

### 3. `run_sql` (Coming Soon)

Execute custom SQL statements after import.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sql` | string | null | Inline SQL to execute |
| `sql_file` | string | null | Path to SQL file (alternative to `sql`) |

---

### 4. `shell` (Coming Soon - Requires Opt-in)

Execute shell commands. **Disabled by default** for security.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `command` | string | *required* | Shell command to execute |
| `working_dir` | string | null | Working directory |
| `env` | object | null | Additional environment variables |

**Security:** Requires `CPI_ALLOW_SHELL_HOOKS=true` environment variable on the server.

---

## API Integration

### Creating a Project with Hooks

```http
POST /projects
Content-Type: application/json
X-API-Key: your-api-key

{
  "name": "legacy_import",
  "connection_id": "conn-uuid-here",
  "source_id": "sftp-uuid-here",
  "config": {
    "defaults": {
      "file_pattern": "*.csv",
      "primary_key": "HDR_ID"
    },
    "hooks": {
      "post_file_prepare": [
        {
          "type": "dbf_to_csv",
          "encoding": "auto"
        }
      ],
      "post_import": [
        {
          "type": "refresh_views"
        }
      ]
    }
  }
}
```

### Updating Hooks on Existing Project

```http
PUT /projects/legacy_import
Content-Type: application/json
X-API-Key: your-api-key

{
  "config": {
    "defaults": {
      "file_pattern": "*.csv",
      "primary_key": "HDR_ID"
    },
    "hooks": {
      "post_file_prepare": [
        {
          "type": "dbf_to_csv",
          "input_pattern": "*.dbf",
          "encoding": "cp1252",
          "delete_original": true
        }
      ],
      "post_import": [
        {
          "type": "refresh_views",
          "on_error": "warn"
        }
      ]
    }
  }
}
```

### Viewing Project Hooks

```http
GET /projects/legacy_import
```

Response includes the full config with hooks:

```json
{
  "name": "legacy_import",
  "connection_id": "...",
  "config": {
    "defaults": { ... },
    "hooks": {
      "post_file_prepare": [ ... ],
      "post_import": [ ... ]
    }
  }
}
```

---

## Migration: `refresh_materialized_views`

The old `refresh_materialized_views` boolean field is **deprecated** but still works.

### Old Format (Deprecated)
```json
{
  "config": {
    "defaults": { ... },
    "refresh_materialized_views": true
  }
}
```

### New Format (Recommended)
```json
{
  "config": {
    "defaults": { ... },
    "hooks": {
      "post_import": [
        {
          "type": "refresh_views",
          "on_error": "warn"
        }
      ]
    }
  }
}
```

**Automatic conversion:** The backend automatically converts the old format to the new format internally. No action required for existing projects, but new projects should use the hooks format.

**Frontend recommendation:**
- If a project has `refresh_materialized_views: true` but no `hooks.post_import`, display it as having refresh views enabled
- When saving, convert to the new hooks format

---

## Job Results with Hooks

When hooks run, their results appear in the job record.

### Successful Hook Execution

Job errors will be empty or only contain non-hook errors:
```json
{
  "job_id": "...",
  "status": "completed",
  "files_processed": 5,
  "errors": []
}
```

### Hook Errors

Hook errors are recorded with type `HookError`:
```json
{
  "job_id": "...",
  "status": "completed",
  "files_processed": 5,
  "errors": [
    {
      "message": "Failed to refresh materialized view: analytics_summary",
      "error_type": "HookError"
    }
  ]
}
```

**Note:** With `on_error: "warn"`, hook errors don't change the job status. The job can still be `"completed"` even with hook warnings.

---

## UI Recommendations

### Hook Configuration Form

When building a UI for hook configuration:

1. **Use a visual builder** - Let users add hooks from a dropdown of available types
2. **Show/hide fields based on type** - Different hook types have different options
3. **Validate on_error** - Only allow `"fail"`, `"warn"`, `"ignore"`
4. **Default to sensible values**:
   - `on_error: "fail"` for pre-import hooks (stop if something's wrong)
   - `on_error: "warn"` for post-import hooks (don't fail job after successful import)

### Suggested UI Layout

```
Hooks Configuration
├── Pre-Import Hooks
│   └── [+ Add Hook]
├── File Preparation Hooks (runs after download)
│   ├── [DBF to CSV] pattern: *.dbf, encoding: auto, delete: yes [x]
│   └── [+ Add Hook]
├── Post-Import Hooks
│   ├── [Refresh Views] schema: public, on_error: warn [x]
│   └── [+ Add Hook]
```

### Displaying Hook Status in Job Details

```
Job: abc123
Status: Completed
Files: 5 processed, 0 failed

Hooks Executed:
✓ DBF to CSV: Converted 3 files
✓ Refresh Views: Refreshed 2 views
```

Or with warnings:
```
Hooks Executed:
✓ DBF to CSV: Converted 3 files
⚠ Refresh Views: Failed to refresh 'analytics_daily' (warning only)
```

---

## Common Patterns

### Pattern 1: Legacy System Migration

Customer has DBF files from an old FoxPro system:

```json
{
  "hooks": {
    "post_file_prepare": [
      {
        "type": "dbf_to_csv",
        "input_pattern": "*.DBF",
        "encoding": "cp850",
        "delete_original": true
      }
    ]
  }
}
```

### Pattern 2: Analytics Refresh

Import updates base tables, then refresh materialized views:

```json
{
  "hooks": {
    "post_import": [
      {
        "type": "refresh_views",
        "schema": "analytics",
        "on_error": "warn"
      }
    ]
  }
}
```

### Pattern 3: Combined (DBF + Refresh)

Full pipeline from DBF to refreshed views:

```json
{
  "hooks": {
    "post_file_prepare": [
      {
        "type": "dbf_to_csv",
        "encoding": "auto"
      }
    ],
    "post_import": [
      {
        "type": "refresh_views",
        "on_error": "warn"
      }
    ]
  }
}
```

---

## TypeScript Types (for Frontend)

```typescript
type OnErrorBehavior = 'fail' | 'warn' | 'ignore';

type HookPoint =
  | 'pre_import'
  | 'post_file_prepare'
  | 'pre_file_import'
  | 'post_file_import'
  | 'post_import';

interface BaseHookAction {
  type: string;
  name?: string;
  enabled?: boolean;
  on_error?: OnErrorBehavior;
  timeout_seconds?: number;
}

interface DbfToCsvHook extends BaseHookAction {
  type: 'dbf_to_csv';
  input_pattern?: string;  // default: "*.dbf"
  encoding?: string;       // default: "auto"
  delete_original?: boolean; // default: false
}

interface RefreshViewsHook extends BaseHookAction {
  type: 'refresh_views';
  schema?: string;  // default: "public"
}

type HookAction = DbfToCsvHook | RefreshViewsHook;

interface HookConfig {
  pre_import?: HookAction[];
  post_file_prepare?: HookAction[];
  pre_file_import?: HookAction[];
  post_file_import?: HookAction[];
  post_import?: HookAction[];
}

interface ProjectConfig {
  name?: string;
  defaults?: DefaultsConfig;
  table_naming?: TableNamingConfig;
  tables?: TableConfig[];
  hooks?: HookConfig;

  // Deprecated - use hooks.post_import with refresh_views instead
  refresh_materialized_views?: boolean;
}
```

---

## Questions?

Contact the backend team or check the API documentation at `/docs` (Swagger UI) when the server is running.
