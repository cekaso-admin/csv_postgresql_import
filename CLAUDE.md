# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Summary

Python script for importing CSV files into PostgreSQL (Supabase). Creates tables if they don't exist, upserts rows if they do. Will expose an API for n8n workflow orchestration.

## Tech Stack

- **Language**: Python 3.x
- **Database**: PostgreSQL via Supabase (direct connection + REST API available)
- **Orchestration**: n8n (external, will call our API)

## Project Structure

```
csv_postgresql_import/
├── CLAUDE.md              # This file
├── src/
│   ├── db/                # Database operations
│   │   ├── connection.py  # Connection pool management
│   │   ├── schema.py      # Table operations (create, staging)
│   │   └── importer.py    # Core CSV import logic
│   ├── config/            # YAML config loading (Phase 2)
│   ├── sftp/              # SFTP client (Phase 3)
│   ├── api/               # FastAPI routes (Phase 5)
│   └── services/          # Job orchestration (Phase 4)
├── config/                # Project YAML configs
├── context/               # Documentation for LLMs
│   ├── INDEX.md           # START HERE - navigation guide
│   ├── decisions/         # Architecture Decision Records
│   └── tasks/             # Implementation tasks
└── tests/
```

## For LLMs

1. Read `context/INDEX.md` first for navigation
2. Pick only the files relevant to your task
3. Check `context/decisions/` for past architectural choices

## Environment Setup

**Always use the virtual environment:**
```bash
# Create (first time only)
python3 -m venv venv

# Activate (every session)
source venv/bin/activate  # macOS/Linux

# Install dependencies
pip install -r requirements.txt
```

## Commands

```bash
# Setup
cp .env.example .env          # Edit with MANAGEMENT_DATABASE_URL and API_KEY
source venv/bin/activate
pip install -r requirements.txt

# Run the API server (development)
uvicorn src.main:app --reload

# Run the API server (production)
uvicorn src.main:app --host 0.0.0.0 --port 8000

# Test management database connection
python -c "from src.db.management import test_management_connection; print(test_management_connection())"
```

Note: Target database connections are managed via the `/connections` API endpoint, not environment variables.
SFTP sources can be managed via the `/sources` API endpoint and referenced by projects.

## API Endpoints

Once the server is running, visit http://localhost:8000/docs for the interactive API documentation.

All endpoints except `/health` require the `X-API-Key` header.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check (no auth) |
| `/connections` | GET/POST | List/create database connections |
| `/connections/{id}` | GET/PUT/DELETE | Manage connection |
| `/connections/{id}/test` | POST | Test database connection |
| `/sources` | GET/POST | List/create SFTP sources |
| `/sources/{id}` | GET/PUT/DELETE | Manage source |
| `/sources/{id}/test` | POST | Test SFTP connection |
| `/projects` | GET/POST | List/create projects |
| `/projects/{name}` | GET/PUT/DELETE | Manage project |
| `/import` | POST | Start import job |
| `/jobs` | GET | List recent jobs |
| `/jobs/{job_id}` | GET | Get job status/results |

## Code Style

- Follow PEP 8, use type hints
- See `context/BEST_PRACTICES.md` for full guidelines
