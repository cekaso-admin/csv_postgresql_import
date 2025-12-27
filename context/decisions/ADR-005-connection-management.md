# ADR-005: Database Connection Management via API

**Status:** Accepted
**Date:** 2024-12-10

## Context

The CSV import system needs to connect to different target PostgreSQL databases for each project. The original design used environment variable references in project configs:

```yaml
connection:
  env_var: "CUSTOMER_ABC_DB_URL"  # Reference to env var
```

This approach has limitations:
1. New connections require server restart to add env vars
2. No API for managing connections
3. No validation of connections before use
4. Credentials scattered across environment files
5. Difficult to build a frontend for connection management

## Decision

### 1. Separate Connections Table

Store database connections in a dedicated `cpi_connections` table:

```sql
CREATE TABLE cpi_connections (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    database_url TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

Projects reference connections via foreign key:

```sql
CREATE TABLE cpi_projects (
    id UUID PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    connection_id UUID REFERENCES cpi_connections(id) ON DELETE SET NULL,
    config JSONB NOT NULL,
    ...
);
```

### 2. Connection API Endpoints

Full CRUD operations for connections:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/connections` | GET | List all (safe, no database_url) |
| `/connections` | POST | Create connection |
| `/connections/{id}` | GET | Get details (includes database_url) |
| `/connections/{id}` | PUT | Update connection |
| `/connections/{id}` | DELETE | Delete connection |
| `/connections/{id}/test` | POST | Test connectivity |

**Security:** The list endpoint (`GET /connections`) returns `ConnectionResponseSafe` which excludes the `database_url`. Only the detail endpoint (`GET /connections/{id}`) returns the full connection including credentials.

### 3. No Environment Variable Fallback

Import jobs **require** a connection from the database. No fallback to `DATABASE_URL` environment variable.

**Rationale:**
- Prevents accidental use of wrong database
- Forces explicit connection configuration
- Clear error messages when connection missing
- Avoids confusion about which connection is used

### 4. Connection Required for Import

Before starting an import job, the system validates:
1. Project exists in database
2. Project has a `connection_id` configured
3. Referenced connection exists

If any validation fails, the import request is rejected with a clear error.

## Consequences

### Positive

- Connections managed via API (no server restart)
- Reusable connections across multiple projects
- Frontend can manage connections separately from projects
- Test endpoint validates connectivity before use
- Clear audit trail of available connections
- `ON DELETE SET NULL` prevents orphan projects on connection deletion

### Negative

- Breaking change from env var approach (migration needed)
- Database credentials stored in management database
- Must create connection before creating project

### Security Considerations

- Database credentials stored in PostgreSQL (encrypted at rest if using Supabase)
- API should be secured (authentication recommended for production)
- List endpoint hides credentials, detail endpoint exposes them
- Consider encrypting `database_url` column for additional security

## Migration

Projects using the old `connection.env_var` format must:
1. Create a connection via `/connections` endpoint
2. Update project to set `connection_id`
3. Remove `connection.env_var` from project config

## Examples

### Create connection

```bash
curl -X POST http://localhost:8000/connections \
  -H "Content-Type: application/json" \
  -d '{
    "name": "customer_abc_prod",
    "description": "Customer ABC production database",
    "database_url": "postgresql://user:pass@host:5432/dbname"
  }'
```

### Create project with connection

```bash
curl -X POST http://localhost:8000/projects \
  -H "Content-Type: application/json" \
  -d '{
    "name": "customer_abc",
    "connection_id": "uuid-from-connection-create",
    "config": {
      "name": "customer_abc",
      "defaults": {
        "file_pattern": "*.csv",
        "primary_key": "id"
      }
    }
  }'
```

### Test connection

```bash
curl -X POST http://localhost:8000/connections/{id}/test
# Returns: {"success": true, "message": "Connection successful"}
```
