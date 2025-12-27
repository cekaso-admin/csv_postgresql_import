# Project Overview

## Summary

A Python script that imports CSV files into PostgreSQL tables hosted on Supabase. Designed for automation via n8n workflows.

## Core Requirements

### CSV Import Logic
1. **Table doesn't exist**: Create table with schema inferred from CSV
2. **Table exists**:
   - Insert new rows
   - Update existing rows (upsert behavior)

### Integration Points
- **n8n**: Will orchestrate this script as part of larger workflows
- **API layer**: Required for n8n integration (to be designed)

## Tech Stack

| Component | Technology | Notes |
|-----------|------------|-------|
| Language | Python 3.x | Primary development language |
| Database | PostgreSQL | Via Supabase |
| Hosting | Supabase | Provides PostgreSQL + REST API |
| Orchestration | n8n | External, will call our API |

## Supabase Considerations

Two access methods available:

### 1. Direct PostgreSQL Connection
- Standard psycopg2/SQLAlchemy connection
- Full SQL capabilities
- Better for bulk operations and complex queries
- Connection string from Supabase dashboard

### 2. Supabase REST API
- Auto-generated API from database schema
- Built-in authentication
- Rate limits apply
- Better for CRUD operations from external services

## Decided

See `decisions/` folder for full context.

- **Import Strategy** (ADR-001): Hybrid approach
  - Large files (>100MB): COPY to staging table → upsert
  - Small files (<100MB): Direct chunked upsert
  - Always use `INSERT ... ON CONFLICT DO UPDATE`
  - `rebuild_table` option available (TRUNCATE before import, default: false)

- **Primary Key Detection**: Per-project YAML configuration

- **Large File Handling**: Stream in chunks, never load full file into memory

- **Schema Inference** (ADR-002): All columns as VARCHAR
  - Avoids type mismatches and length issues
  - Casting handled in views and derived tables

- **API Design** (ADR-002): REST + SFTP + Webhook
  - n8n triggers REST endpoint with project config
  - Script pulls files from SFTP (per-project config)
  - Webhook callback to n8n on completion (stats, errors)

## Project Status

**Phase**: Ready for Implementation Planning

All architectural decisions made. Next: create implementation plan and start coding.
