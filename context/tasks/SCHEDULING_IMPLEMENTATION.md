# Scheduling Feature Implementation

## Overview

This document describes the scheduling feature implementation for the CSV PostgreSQL Import API. The feature enables recurring imports without external orchestration tools.

## Implementation Summary

### Phase 1: Database Foundation ✓
**File Modified:** `src/db/management.py`

- Added `cpi_schedules` table to SCHEMA_SQL with:
  - Schedule configuration (cron/interval, timezone)
  - Execution settings (enabled, callback_url, sftp_override, local_files)
  - Tracking stats (last_run_at, total_runs, successful_runs, failed_runs)
- Added `schedule_id` column to `cpi_jobs` table (idempotent migration)
- Updated `JobRecord` dataclass to include `schedule_id` field
- Updated `create_job()` function to accept optional `schedule_id` parameter
- Updated all job queries to include `schedule_id` in SELECT statements
- Updated `_row_to_job_record()` helper to include `schedule_id`

### Phase 2: Schedule CRUD ✓
**File Created:** `src/db/schedules.py` (500+ lines)

Implemented complete CRUD operations following existing patterns:
- `ScheduleRecord` dataclass with all schedule fields
- `create_schedule()` with validation (min interval: 3600 seconds)
- `get_schedule()` and `get_schedule_by_name()`
- `list_schedules()` with filtering by project_id and enabled status
- `update_schedule()` with dynamic SQL UPDATE builder
- `delete_schedule()`
- `update_schedule_execution()` for tracking stats after each run
- `list_enabled_schedules()` helper for scheduler startup
- `_row_to_schedule_record()` converter

### Phase 3: API Schemas ✓
**File Created:** `src/api/schedule_schemas.py` (200+ lines)

Pydantic models with comprehensive validation:
- `ScheduleCreate` with field validators for schedule_type and model validator for config
- `ScheduleUpdate` for partial updates
- `ScheduleResponse` with all schedule fields including stats
- `ScheduleListResponse` for list endpoints
- `ScheduleControlResponse` for enable/disable operations

Validation rules:
- `schedule_type` must be 'cron' or 'interval'
- `cron_expression` required if type='cron'
- `interval_seconds` required if type='interval', minimum 3600 (1 hour)

### Phase 4: API Routes ✓
**File Created:** `src/api/schedule_routes.py` (600+ lines)

Implemented 9 REST API endpoints:
1. `POST /schedules` - Create schedule (validates project, registers with scheduler)
2. `GET /schedules` - List schedules (filters: project, enabled, limit, offset)
3. `GET /schedules/{id}` - Get schedule details
4. `PUT /schedules/{id}` - Update schedule (updates scheduler if needed)
5. `DELETE /schedules/{id}` - Delete schedule (removes from scheduler)
6. `POST /schedules/{id}/enable` - Enable schedule (adds to scheduler)
7. `POST /schedules/{id}/disable` - Disable schedule (removes from scheduler)
8. `POST /schedules/{id}/run` - Manual trigger (creates job immediately)
9. `GET /schedules/{id}/history` - Get job history for schedule

All endpoints:
- Require API key authentication via `require_api_key` dependency
- Return appropriate HTTP status codes (200, 201, 204, 400, 404, 409, 500)
- Include comprehensive error handling and logging
- Populate project_name in responses by joining with projects table

### Phase 5: Scheduler Service ✓
**File Created:** `src/services/scheduler.py` (400+ lines)

Implemented APScheduler integration:
- `SchedulerService` singleton class with lifecycle management
- `start()` - Initializes scheduler, loads enabled schedules from DB
- `shutdown()` - Gracefully stops scheduler with wait=True
- `add_schedule()` - Registers schedule with APScheduler (cron or interval trigger)
- `remove_schedule()` - Removes schedule from APScheduler
- `update_schedule()` - Removes and re-adds schedule if enabled
- `pause_schedule()` and `resume_schedule()` for temporary control
- `execute_scheduled_import()` - Callback executed by APScheduler
- `trigger_schedule_execution()` - Manual trigger helper for API endpoint

APScheduler configuration:
```python
job_defaults={
    'coalesce': True,           # Combine missed runs
    'max_instances': 1,         # No overlapping
    'misfire_grace_time': 300,  # 5 min grace period
}
```

### Phase 6: Integration ✓
**Files Modified:**
- `src/main.py` - Integrated scheduler into application lifespan
- `src/api/routes.py` - Added schedule_id to JobResponse
- `src/api/schemas.py` - Added schedule_id field to JobResponse

Changes to `src/main.py`:
- Import `schedules_router` and `SchedulerService`
- Start scheduler in lifespan startup (graceful degradation if fails)
- Shutdown scheduler in lifespan cleanup
- Register `schedules_router` with app

Changes to API responses:
- `JobResponse` now includes `schedule_id` field (nullable)
- Job list and detail endpoints return `schedule_id` for scheduled jobs

### Phase 7: Dependencies ✓
**File Modified:** `requirements.txt`

Added:
```
APScheduler>=3.10.0        # Job scheduling
```

Installed successfully with dependency `tzlocal>=3.0`.

## Database Schema

### New Table: `cpi_schedules`
```sql
CREATE TABLE IF NOT EXISTS cpi_schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    project_id UUID REFERENCES cpi_projects(id) ON DELETE CASCADE,
    
    -- Schedule configuration
    schedule_type VARCHAR(20) NOT NULL CHECK (schedule_type IN ('cron', 'interval')),
    cron_expression VARCHAR(100),
    interval_seconds INTEGER CHECK (interval_seconds >= 3600),
    timezone VARCHAR(50) DEFAULT 'UTC',
    
    -- Execution settings
    enabled BOOLEAN DEFAULT TRUE,
    callback_url TEXT,
    sftp_override JSONB,
    local_files JSONB,
    
    -- Tracking
    last_run_at TIMESTAMP WITH TIME ZONE,
    next_run_at TIMESTAMP WITH TIME ZONE,
    last_job_id UUID REFERENCES cpi_jobs(id) ON DELETE SET NULL,
    total_runs INTEGER DEFAULT 0,
    successful_runs INTEGER DEFAULT 0,
    failed_runs INTEGER DEFAULT 0,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

### Modified Table: `cpi_jobs`
Added column (idempotent):
```sql
schedule_id UUID REFERENCES cpi_schedules(id) ON DELETE SET NULL
```

### Indexes
```sql
CREATE INDEX IF NOT EXISTS idx_cpi_schedules_project_id ON cpi_schedules(project_id);
CREATE INDEX IF NOT EXISTS idx_cpi_schedules_enabled ON cpi_schedules(enabled);
CREATE INDEX IF NOT EXISTS idx_cpi_jobs_schedule_id ON cpi_jobs(schedule_id);
```

## API Endpoints

All endpoints require `X-API-Key` header.

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/schedules` | POST | Create a new schedule |
| `/schedules` | GET | List schedules (filter by project, enabled) |
| `/schedules/{id}` | GET | Get schedule details |
| `/schedules/{id}` | PUT | Update schedule |
| `/schedules/{id}` | DELETE | Delete schedule |
| `/schedules/{id}/enable` | POST | Enable schedule |
| `/schedules/{id}/disable` | POST | Disable schedule |
| `/schedules/{id}/run` | POST | Trigger manual execution |
| `/schedules/{id}/history` | GET | Get job history for schedule |

## Example Usage

### Create an Interval Schedule (Every 2 Hours)
```bash
curl -X POST http://localhost:8000/schedules \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hourly-customer-import",
    "project_name": "customer_project",
    "schedule_type": "interval",
    "interval_seconds": 7200,
    "timezone": "UTC",
    "enabled": true,
    "callback_url": "https://webhook.example.com/notify"
  }'
```

### Create a Cron Schedule (Daily at Midnight)
```bash
curl -X POST http://localhost:8000/schedules \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "daily-sales-import",
    "project_name": "sales_project",
    "schedule_type": "cron",
    "cron_expression": "0 0 * * *",
    "timezone": "America/New_York",
    "enabled": true
  }'
```

### List Schedules for a Project
```bash
curl -X GET "http://localhost:8000/schedules?project=customer_project&enabled=true" \
  -H "X-API-Key: your-api-key"
```

### Manually Trigger a Schedule
```bash
curl -X POST http://localhost:8000/schedules/{schedule-id}/run \
  -H "X-API-Key: your-api-key"
```

### View Schedule History
```bash
curl -X GET http://localhost:8000/schedules/{schedule-id}/history \
  -H "X-API-Key: your-api-key"
```

### Disable a Schedule
```bash
curl -X POST http://localhost:8000/schedules/{schedule-id}/disable \
  -H "X-API-Key: your-api-key"
```

## Key Features

1. **Persistent Schedules**: All schedules stored in PostgreSQL, survive API restarts
2. **Job Linking**: Each scheduled run creates a job in `cpi_jobs` with `schedule_id` reference
3. **Multiple Schedules**: One project can have multiple schedules with different patterns
4. **Minimum Interval**: Enforced 1-hour minimum to prevent system overload
5. **Timezone Support**: Per-schedule timezone configuration
6. **Statistics Tracking**: Total runs, successful runs, failed runs tracked per schedule
7. **Graceful Degradation**: API continues working even if scheduler fails to start
8. **No Overlapping**: APScheduler configured with `max_instances=1`
9. **Missed Run Handling**: Coalescing enabled to combine missed runs
10. **Manual Triggering**: Schedules can be executed on-demand via API

## Testing Checklist

- [x] Database schema migration (idempotent)
- [x] Python syntax validation
- [x] Application imports successfully
- [ ] Create interval schedule via API
- [ ] Create cron schedule via API
- [ ] List schedules with filters
- [ ] Enable/disable schedule
- [ ] Manual trigger creates job
- [ ] View schedule history
- [ ] Schedule persists after restart
- [ ] Scheduled job executes automatically
- [ ] Job has schedule_id populated
- [ ] Statistics update after run
- [ ] Multiple schedules per project work
- [ ] Minimum interval validation works

## Files Created

1. `/Users/engincetinkaya/scripts/csv_postgresql_import/src/db/schedules.py` (500+ lines)
2. `/Users/engincetinkaya/scripts/csv_postgresql_import/src/api/schedule_schemas.py` (200+ lines)
3. `/Users/engincetinkaya/scripts/csv_postgresql_import/src/api/schedule_routes.py` (600+ lines)
4. `/Users/engincetinkaya/scripts/csv_postgresql_import/src/services/scheduler.py` (400+ lines)
5. `/Users/engincetinkaya/scripts/csv_postgresql_import/src/services/__init__.py`

## Files Modified

1. `/Users/engincetinkaya/scripts/csv_postgresql_import/src/db/management.py` (SCHEMA_SQL, JobRecord, create_job, queries)
2. `/Users/engincetinkaya/scripts/csv_postgresql_import/src/api/routes.py` (JobResponse objects)
3. `/Users/engincetinkaya/scripts/csv_postgresql_import/src/api/schemas.py` (JobResponse schema)
4. `/Users/engincetinkaya/scripts/csv_postgresql_import/src/main.py` (lifespan, router registration)
5. `/Users/engincetinkaya/scripts/csv_postgresql_import/requirements.txt` (APScheduler)

## Next Steps

1. **Test the implementation:**
   - Start the API: `uvicorn src.main:app --reload`
   - Access API docs: http://localhost:8000/docs
   - Test schedule CRUD operations
   - Verify scheduled jobs execute

2. **Monitor the scheduler:**
   - Check logs for "Scheduler started with X active schedules"
   - Monitor job creation from scheduled runs
   - Verify statistics update correctly

3. **Production considerations:**
   - Set appropriate `MANAGEMENT_DATABASE_URL` environment variable
   - Configure `API_KEY` for authentication
   - Monitor scheduler health
   - Set up alerting for failed scheduled jobs

## Success Criteria

All implementation phases completed:
- ✓ Database schema updated with schedules table and schedule_id column
- ✓ Schedule CRUD operations implemented
- ✓ API schemas with validation created
- ✓ 9 REST API endpoints implemented
- ✓ APScheduler service integrated
- ✓ Scheduler lifecycle managed in app lifespan
- ✓ Dependencies installed
- ✓ Code compiles without syntax errors
- ✓ Application imports successfully

The scheduling feature is **fully implemented** and ready for testing.
