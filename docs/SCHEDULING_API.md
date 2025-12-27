# Scheduling API Documentation

This document describes the scheduling API endpoints for the CSV PostgreSQL Import system. Schedules allow you to automatically run import jobs at specified intervals or cron times.

## Authentication

All endpoints require the `X-API-Key` header:

```
X-API-Key: your-api-key-here
```

---

## Overview

### Schedule Types

| Type | Description | Example |
|------|-------------|---------|
| `cron` | Run at specific times using cron syntax | `0 0 * * *` (daily at midnight) |
| `interval` | Run every N seconds (minimum 3600 = 1 hour) | `3600` (hourly) |

### Key Constraints

- **Minimum interval**: 3600 seconds (1 hour)
- **Multiple schedules per project**: Yes, allowed
- **Timezone support**: Yes, per schedule (default: UTC)

---

## Endpoints

### 1. Create Schedule

**POST** `/schedules`

Creates a new schedule for automatic imports.

#### Request Body

```json
{
  "name": "daily-customer-import",
  "project_name": "customer_abc",
  "schedule_type": "cron",
  "cron_expression": "0 0 * * *",
  "timezone": "Europe/Berlin",
  "enabled": true,
  "callback_url": "https://webhook.example.com/notify"
}
```

#### Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique schedule name (1-255 chars) |
| `project_name` | string | Yes | Name of the project to run |
| `schedule_type` | string | Yes | Either `"cron"` or `"interval"` |
| `cron_expression` | string | If cron | Cron expression (see examples below) |
| `interval_seconds` | integer | If interval | Seconds between runs (min: 3600) |
| `timezone` | string | No | Timezone for schedule (default: `"UTC"`) |
| `enabled` | boolean | No | Whether schedule is active (default: `true`) |
| `callback_url` | string | No | Webhook URL for job completion notifications |
| `sftp_override` | object | No | Override project's SFTP settings |
| `local_files` | string[] | No | Local file paths to import |

#### Cron Expression Examples

| Expression | Meaning |
|------------|---------|
| `0 0 * * *` | Daily at midnight |
| `0 */6 * * *` | Every 6 hours |
| `30 8 * * 1-5` | Weekdays at 8:30 AM |
| `0 0 * * 0` | Weekly on Sunday at midnight |
| `0 0 1 * *` | Monthly on the 1st at midnight |

#### Response (201 Created)

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "daily-customer-import",
  "project_id": "660e8400-e29b-41d4-a716-446655440001",
  "project_name": "customer_abc",
  "schedule_type": "cron",
  "cron_expression": "0 0 * * *",
  "interval_seconds": null,
  "timezone": "Europe/Berlin",
  "enabled": true,
  "callback_url": "https://webhook.example.com/notify",
  "sftp_override": null,
  "local_files": null,
  "last_run_at": null,
  "next_run_at": "2025-12-19T00:00:00+01:00",
  "last_job_id": null,
  "total_runs": 0,
  "successful_runs": 0,
  "failed_runs": 0,
  "created_at": "2025-12-18T10:30:00Z",
  "updated_at": "2025-12-18T10:30:00Z"
}
```

#### Errors

| Status | Reason |
|--------|--------|
| 400 | Invalid schedule configuration |
| 404 | Project not found |
| 409 | Schedule name already exists |

---

### 2. List Schedules

**GET** `/schedules`

List all schedules with optional filtering.

#### Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `project` | string | - | Filter by project name |
| `enabled` | boolean | - | Filter by enabled status |
| `limit` | integer | 50 | Max results (1-100) |
| `offset` | integer | 0 | Skip N results |

#### Example Request

```
GET /schedules?project=customer_abc&enabled=true&limit=10
```

#### Response (200 OK)

```json
{
  "schedules": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "daily-customer-import",
      "project_id": "660e8400-e29b-41d4-a716-446655440001",
      "project_name": "customer_abc",
      "schedule_type": "cron",
      "cron_expression": "0 0 * * *",
      "interval_seconds": null,
      "timezone": "Europe/Berlin",
      "enabled": true,
      "callback_url": "https://webhook.example.com/notify",
      "sftp_override": null,
      "local_files": null,
      "last_run_at": "2025-12-18T00:00:00Z",
      "next_run_at": "2025-12-19T00:00:00Z",
      "last_job_id": "770e8400-e29b-41d4-a716-446655440002",
      "total_runs": 5,
      "successful_runs": 4,
      "failed_runs": 1,
      "created_at": "2025-12-13T10:30:00Z",
      "updated_at": "2025-12-18T00:00:05Z"
    }
  ],
  "total": 1
}
```

---

### 3. Get Schedule

**GET** `/schedules/{schedule_id}`

Get details of a specific schedule.

#### Response (200 OK)

Same structure as single schedule in list response.

#### Errors

| Status | Reason |
|--------|--------|
| 404 | Schedule not found |

---

### 4. Update Schedule

**PUT** `/schedules/{schedule_id}`

Update an existing schedule. Only include fields you want to change.

#### Request Body

```json
{
  "cron_expression": "0 6 * * *",
  "timezone": "America/New_York",
  "callback_url": "https://new-webhook.example.com/notify"
}
```

All fields are optional. Only provided fields will be updated.

#### Response (200 OK)

Returns the updated schedule object.

#### Errors

| Status | Reason |
|--------|--------|
| 400 | Invalid update values |
| 404 | Schedule not found |

---

### 5. Delete Schedule

**DELETE** `/schedules/{schedule_id}`

Delete a schedule permanently.

#### Response (204 No Content)

No body returned.

#### Errors

| Status | Reason |
|--------|--------|
| 404 | Schedule not found |

---

### 6. Enable Schedule

**POST** `/schedules/{schedule_id}/enable`

Enable a disabled schedule.

#### Response (200 OK)

```json
{
  "success": true,
  "message": "Schedule enabled successfully",
  "schedule": { /* full schedule object */ }
}
```

---

### 7. Disable Schedule

**POST** `/schedules/{schedule_id}/disable`

Disable an active schedule. The schedule remains in the database but won't run.

#### Response (200 OK)

```json
{
  "success": true,
  "message": "Schedule disabled successfully",
  "schedule": { /* full schedule object */ }
}
```

---

### 8. Run Schedule Manually

**POST** `/schedules/{schedule_id}/run`

Trigger a schedule to run immediately, regardless of its cron/interval timing.

#### Response (202 Accepted)

```json
{
  "job_id": "880e8400-e29b-41d4-a716-446655440003",
  "project": "customer_abc",
  "status": "pending",
  "message": "Schedule 'daily-customer-import' triggered manually. Use GET /jobs/880e8400-e29b-41d4-a716-446655440003 to check status."
}
```

Use the returned `job_id` with `GET /jobs/{job_id}` to monitor progress.

---

### 9. Get Schedule History

**GET** `/schedules/{schedule_id}/history`

Get execution history (jobs) for a specific schedule.

#### Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | integer | 50 | Max results (1-100) |
| `offset` | integer | 0 | Skip N results |

#### Response (200 OK)

```json
{
  "jobs": [
    {
      "id": "880e8400-e29b-41d4-a716-446655440003",
      "project_name": "customer_abc",
      "status": "completed",
      "started_at": "2025-12-18T00:00:01Z",
      "completed_at": "2025-12-18T00:00:15Z",
      "duration_seconds": 14.0,
      "files_processed": 5,
      "files_failed": 0,
      "total_inserted": 1250,
      "total_updated": 340,
      "callback_url": "https://webhook.example.com/notify",
      "created_at": "2025-12-18T00:00:00Z",
      "file_results": [
        {
          "filename": "customers.csv",
          "table_name": "customers",
          "inserted": 250,
          "updated": 50,
          "success": true,
          "error": null
        }
      ],
      "errors": []
    }
  ],
  "total": 1
}
```

---

## Schedule Object Reference

| Field | Type | Description |
|-------|------|-------------|
| `id` | string (UUID) | Unique schedule identifier |
| `name` | string | Schedule name (unique) |
| `project_id` | string (UUID) | Associated project ID |
| `project_name` | string | Associated project name |
| `schedule_type` | string | `"cron"` or `"interval"` |
| `cron_expression` | string | Cron expression (if type is cron) |
| `interval_seconds` | integer | Interval in seconds (if type is interval) |
| `timezone` | string | Timezone (e.g., `"UTC"`, `"Europe/Berlin"`) |
| `enabled` | boolean | Whether schedule is active |
| `callback_url` | string | Webhook URL for notifications |
| `sftp_override` | object | SFTP settings override |
| `local_files` | string[] | Local file paths |
| `last_run_at` | datetime | When schedule last ran |
| `next_run_at` | datetime | When schedule will next run |
| `last_job_id` | string | Job ID of last execution |
| `total_runs` | integer | Total execution count |
| `successful_runs` | integer | Successful execution count |
| `failed_runs` | integer | Failed execution count |
| `created_at` | datetime | When schedule was created |
| `updated_at` | datetime | When schedule was last updated |

---

## Example Workflows

### Create a Daily Import Schedule

```bash
curl -X POST http://localhost:8000/schedules \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "daily-orders",
    "project_name": "orders_project",
    "schedule_type": "cron",
    "cron_expression": "0 2 * * *",
    "timezone": "Europe/Berlin"
  }'
```

### Create an Hourly Import Schedule

```bash
curl -X POST http://localhost:8000/schedules \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hourly-sync",
    "project_name": "live_data",
    "schedule_type": "interval",
    "interval_seconds": 3600
  }'
```

### Pause and Resume a Schedule

```bash
# Disable
curl -X POST http://localhost:8000/schedules/{id}/disable \
  -H "X-API-Key: your-api-key"

# Enable
curl -X POST http://localhost:8000/schedules/{id}/enable \
  -H "X-API-Key: your-api-key"
```

### Check Schedule Status and History

```bash
# Get schedule details
curl http://localhost:8000/schedules/{id} \
  -H "X-API-Key: your-api-key"

# Get execution history
curl "http://localhost:8000/schedules/{id}/history?limit=10" \
  -H "X-API-Key: your-api-key"
```

---

## Job Relationship

When a schedule runs (automatically or manually triggered), it creates a job with a `schedule_id` reference. You can:

1. **Get the latest job** from `schedule.last_job_id`
2. **Check job status** via `GET /jobs/{last_job_id}`
3. **View all jobs** via `GET /schedules/{id}/history`

Jobs created by schedules appear in the regular `/jobs` endpoint as well, with the `schedule_id` field populated.

---

## Error Handling

All error responses follow this format:

```json
{
  "detail": "Error message describing what went wrong"
}
```

| Status Code | Meaning |
|-------------|---------|
| 400 | Bad Request - Invalid input data |
| 404 | Not Found - Schedule or project doesn't exist |
| 409 | Conflict - Schedule name already exists |
| 500 | Internal Server Error |
