# ADR-001: CSV Import Strategy

**Status:** Accepted
**Date:** 2024-12-03

## Context

We need to import CSV files (up to 3GB) into PostgreSQL tables on Supabase. Current AirByte solution fails on large file upserts, forcing full table rebuilds.

### Constraints
- Files range from small deltas to 3GB full exports
- Primary keys vary per project/file (configured per project)
- Existing tables have views and triggers that must be preserved
- Supabase Pro tier with IPv4 addon (direct PostgreSQL access)
- Daily imports, sometimes more frequent
- Projects have 5-150 different file types

## Decision

**COPY + Staging Table strategy for all files**

```
CSV → stream chunks → COPY to staging_table → INSERT ON CONFLICT → drop staging
```

We initially considered a hybrid approach with row-by-row inserts for small files, but testing showed the COPY strategy is faster for all file sizes. Row-by-row inserts over network are too slow even for small files.

### Key principles
1. **Never DROP/TRUNCATE target tables** - preserves views, triggers, relationships
2. **Stream files in chunks** - never load full file into memory (supports 3GB+)
3. **Use COPY command** - fastest PostgreSQL bulk load
4. **ON CONFLICT DO UPDATE** - true upsert behavior
5. **Staging table per import** - isolated, cleaned up after
6. **Per-project configuration** - YAML files define table mappings and primary keys

## Configuration Structure

```yaml
# config/<project_name>.yaml
project: customer_abc
connection:
  env_var: DATABASE_URL_ABC  # reference to .env

tables:
  - file_pattern: "customers*.csv"
    target_table: "customers"
    primary_key: "customer_id"

  - file_pattern: "orders*.csv"
    target_table: "orders"
    primary_key: ["order_id", "line_number"]
    column_mapping:
      "Kunde Nr.": "customer_id"
```

## Consequences

### Positive
- Handles 3GB files efficiently
- Preserves existing database objects (views, triggers)
- Memory-efficient streaming (chunked processing)
- Flexible per-project configuration
- Direct PostgreSQL = fastest possible
- Simple, single code path

### Negative
- Requires staging table management (auto-cleaned up)
- Per-project config maintenance

### Risks
- Long-running transactions on very large files (mitigate: chunk commits to staging)
- Schema drift if CSV changes (mitigate: validate before import)

## Alternatives Considered

1. **Supabase REST API** - Rejected: Too slow for large files, no COPY support
2. **Full table rebuild** - Rejected: Destroys views/triggers
3. **AirByte** - Current solution, failing on large upserts
4. **Row-by-row INSERT** - Rejected after testing: Too slow even for small files over network
5. **Hybrid approach** - Initially planned, but COPY is faster for all sizes
