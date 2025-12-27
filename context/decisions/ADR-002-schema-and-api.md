# ADR-002: Schema Inference and API Design

**Status:** Accepted
**Date:** 2024-12-03

## Context

Two remaining architectural questions:
1. How to determine column types when creating new tables
2. How the script integrates with n8n workflows

## Decisions

### Schema Inference: All VARCHAR

When creating new tables from CSV, all columns will be VARCHAR.

**Rationale:**
- Avoids type mismatch errors (e.g., unexpected non-numeric values)
- Avoids field length issues
- Views and derived tables handle casting as needed
- Simple and predictable

```sql
-- Generated table structure
CREATE TABLE customers (
    customer_id VARCHAR,
    name VARCHAR,
    amount VARCHAR,  -- cast in views if needed
    created_at VARCHAR
);
```

### API Design: REST with SFTP Pull + Webhook Callback

**Flow:**
```
n8n trigger → REST API (with config) → Pull from SFTP → Import to DB → Webhook callback to n8n
```

**Components:**
1. **REST endpoint**: Receives import request with project/config info
2. **SFTP client**: Pulls files from configured SFTP server (per-project)
3. **Webhook callback**: Notifies n8n when complete (success/failure + stats)

**API contract (draft):**

```
POST /import
{
    "project": "customer_abc",
    "sftp": {
        "host": "sftp.customer.com",
        "path": "/exports/daily/"
    },
    "callback_url": "https://n8n.example.com/webhook/abc123"
}

Response: 202 Accepted
{
    "job_id": "uuid",
    "status": "started"
}

Callback POST to callback_url:
{
    "job_id": "uuid",
    "status": "completed",  // or "failed"
    "files_processed": 6,
    "rows_inserted": 15000,
    "rows_updated": 3200,
    "errors": []
}
```

### Table Rebuild Option: Configurable

Add optional `rebuild_table: true` in per-table config. Default: `false`.

```yaml
tables:
  - file_pattern: "temp_data*.csv"
    target_table: "temp_data"
    primary_key: "id"
    rebuild_table: true  # TRUNCATE before import (rare use case)
```

When `rebuild_table: true`:
- TRUNCATE table before import (keeps structure, views, triggers)
- Still not DROP (preserves dependencies)

### CSV Parsing Options

Per-file configuration for various CSV formats:

```python
import_csv(
    file_path="data.csv",
    table_name="table",
    primary_key="id",
    delimiter="|",      # Default: ","
    encoding="latin-1", # Default: "utf-8" (common: latin-1, cp1252)
    skiprows=1          # Default: 0 (rows to skip before header)
)
```

**Common encodings:**
- `utf-8` - Modern default
- `latin-1` / `iso-8859-1` - Windows Western European
- `cp1252` - Windows code page 1252

## Consequences

### Positive
- VARCHAR-only = no type surprises
- Clear integration pattern with n8n
- Async processing with callback = no timeout issues
- Rebuild option available if needed
- Flexible CSV parsing (delimiter, encoding, skip rows)

### Negative
- All data as text = larger storage, slower numeric operations
- Need type casting in views/queries
- SFTP adds complexity (credentials, connection handling)

### Dependencies
- FastAPI for REST API
- paramiko or similar for SFTP
- Background task runner (or simple threading for MVP)
