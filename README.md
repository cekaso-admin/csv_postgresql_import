# CSV PostgreSQL Import API

A REST API service for importing CSV files into PostgreSQL databases with SFTP support, job monitoring, and n8n webhook integration.

## Features

- **CSV Import**: Stream large CSV files (tested with 100MB+) efficiently using PostgreSQL COPY
- **Upsert Support**: Insert new rows or update existing ones based on primary key
- **SFTP Pull**: Download CSV files from remote SFTP servers
- **Multiple Databases**: Connect to different PostgreSQL databases per project
- **Job Monitoring**: Track import progress, success/failure, row counts
- **Webhook Callbacks**: Notify external systems (like n8n) when jobs complete
- **API Authentication**: Secure endpoints with API key

## Quick Start

### 1. Clone and Setup

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy and configure environment
cp .env.example .env
# Edit .env with your settings
```

### 2. Configure Environment

Edit `.env` with your settings:

```bash
# Required: Management database (stores configs, jobs, connections)
MANAGEMENT_DATABASE_URL=postgresql://user:pass@host:5432/dbname

# Required: API authentication key
# Generate with: python -c "import secrets; print(secrets.token_urlsafe(32))"
API_KEY=your-secure-api-key-here
```

### 3. Start the Server

```bash
# Development
uvicorn src.main:app --reload

# Production
uvicorn src.main:app --host 0.0.0.0 --port 8000
```

### 4. Access the API

- **API Docs**: http://localhost:8000/docs
- **Health Check**: http://localhost:8000/health

## API Endpoints

All endpoints except `/health` and `/docs` require the `X-API-Key` header.

### Connections

Manage database connections for target databases where CSV data is imported.

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/connections` | List all connections (without credentials) |
| POST | `/connections` | Create a new connection |
| GET | `/connections/{id}` | Get connection details (with credentials) |
| PUT | `/connections/{id}` | Update a connection |
| DELETE | `/connections/{id}` | Delete a connection |
| POST | `/connections/{id}/test` | Test connection connectivity |

### Projects

Manage import project configurations.

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/projects` | List all projects |
| POST | `/projects` | Create a new project |
| GET | `/projects/{name}` | Get project configuration |
| PUT | `/projects/{name}` | Update project |
| DELETE | `/projects/{name}` | Delete project |

### Import Jobs

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/import` | Start an import job |
| GET | `/jobs` | List jobs (filterable) |
| GET | `/jobs/{id}` | Get job status and results |

### Health

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check (no auth required) |

## Usage Examples

### 1. Create a Database Connection

```bash
curl -X POST http://localhost:8000/connections \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "name": "production-db",
    "description": "Production PostgreSQL database",
    "database_url": "postgresql://user:pass@host:5432/dbname"
  }'
```

### 2. Create a Project

```bash
curl -X POST http://localhost:8000/projects \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "name": "customer-import",
    "connection_id": "uuid-from-step-1",
    "config": {
      "name": "customer-import",
      "defaults": {
        "file_pattern": "*.csv",
        "primary_key": "id",
        "delimiter": ",",
        "encoding": "utf-8"
      },
      "table_naming": {
        "strip_prefix": "Export_",
        "strip_suffix": "_data",
        "lowercase": true
      }
    }
  }'
```

### 3. Start an Import Job

```bash
# Import local files
curl -X POST http://localhost:8000/import \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "project": "customer-import",
    "local_files": ["/path/to/customers.csv", "/path/to/orders.csv"],
    "callback_url": "https://your-webhook.com/callback"
  }'
```

### 4. Check Job Status

```bash
curl -H "X-API-Key: your-api-key" \
  http://localhost:8000/jobs/{job_id}
```

## Project Configuration

### Auto-Discovery Mode

For projects with many similar CSV files, use `defaults` and `table_naming`:

```json
{
  "name": "erp-export",
  "defaults": {
    "file_pattern": "*.csv",
    "primary_key": "id",
    "delimiter": "|",
    "encoding": "latin-1",
    "skiprows": 1
  },
  "table_naming": {
    "strip_prefix": "IxExp",
    "lowercase": true
  }
}
```

This will:
- Match any `.csv` file
- Use `|` as delimiter, `latin-1` encoding
- Skip first row (metadata)
- Transform `IxExpCustomers.csv` → table `customers`

### Explicit Table Configuration

For specific file-to-table mappings:

```json
{
  "name": "specific-import",
  "tables": [
    {
      "file_pattern": "customers_*.csv",
      "target_table": "customers",
      "primary_key": ["customer_id", "region"],
      "column_mapping": {
        "Kunde Nr.": "customer_id",
        "Name": "customer_name"
      },
      "rebuild_table": false
    }
  ]
}
```

### SFTP Configuration

For pulling files from remote servers:

```json
{
  "name": "sftp-import",
  "sftp": {
    "host": "sftp.example.com",
    "port": 22,
    "username": "user",
    "password": "pass",
    "remote_path": "/exports/"
  },
  "defaults": {
    "file_pattern": "*.csv",
    "primary_key": "id"
  }
}
```

## n8n Integration

### HTTP Request Node Setup

1. **Method**: POST
2. **URL**: `http://your-server:8000/import`
3. **Authentication**: Header Auth
   - Name: `X-API-Key`
   - Value: `your-api-key`
4. **Body**:
```json
{
  "project": "your-project-name",
  "callback_url": "{{ $webhook.url }}"
}
```

### Webhook Payload

When a job completes, the callback URL receives:

```json
{
  "job_id": "uuid",
  "project": "project-name",
  "status": "completed",
  "files_processed": 5,
  "files_failed": 0,
  "total_inserted": 1500,
  "total_updated": 300,
  "errors": [],
  "duration_seconds": 45.2
}
```

## Architecture

```
csv_postgresql_import/
├── src/
│   ├── main.py              # FastAPI application
│   ├── api/
│   │   ├── auth.py          # API key authentication
│   │   ├── routes.py        # API endpoints
│   │   └── schemas.py       # Pydantic models
│   ├── config/
│   │   ├── loader.py        # YAML/dict config loading
│   │   └── models.py        # Config Pydantic models
│   ├── db/
│   │   ├── connection.py    # PostgreSQL connection pool
│   │   ├── importer.py      # CSV import logic (COPY + upsert)
│   │   ├── management.py    # Management DB operations
│   │   └── schema.py        # Table operations
│   ├── sftp/
│   │   └── client.py        # SFTP file downloading
│   └── services/
│       ├── import_job.py    # Job orchestration
│       └── webhook.py       # Callback notifications
├── context/
│   ├── decisions/           # Architecture Decision Records
│   └── tasks/               # Implementation tasks
├── .env.example
├── requirements.txt
└── README.md
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `MANAGEMENT_DATABASE_URL` | Yes | PostgreSQL URL for management data |
| `API_KEY` | Yes | API authentication key |
| `DB_POOL_MIN_CONN` | No | Min pool connections (default: 1) |
| `DB_POOL_MAX_CONN` | No | Max pool connections (default: 10) |
| `CSV_CHUNK_SIZE` | No | Rows per chunk (default: 10000) |
| `HOST` | No | Server host (default: 0.0.0.0) |
| `PORT` | No | Server port (default: 8000) |
| `CORS_ORIGINS` | No | Allowed origins (default: *) |
| `LOG_LEVEL` | No | Logging level (default: INFO) |

## Database Tables

The management database uses tables with `cpi_` prefix:

| Table | Purpose |
|-------|---------|
| `cpi_connections` | Database connection credentials |
| `cpi_projects` | Project configurations |
| `cpi_jobs` | Job execution history |
| `cpi_job_files` | Per-file import results |
| `cpi_job_errors` | Job error logs |

Tables are auto-created on server startup.

## Key Design Decisions

1. **COPY + Staging**: Uses PostgreSQL COPY command with staging tables for fast bulk imports
2. **All VARCHAR**: Target tables use VARCHAR columns to avoid type mismatches
3. **Streaming**: Processes CSV files in chunks for memory efficiency
4. **Separate Connections**: Database connections stored in management DB, not env vars
5. **Required Connection**: Projects must have a connection configured (no fallback)

See `context/decisions/` for detailed Architecture Decision Records.

## Development

```bash
# Activate environment
source venv/bin/activate

# Run with auto-reload
uvicorn src.main:app --reload

# Type check
python -m py_compile src/**/*.py
```

## License

MIT
