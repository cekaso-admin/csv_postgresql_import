# Path Pattern Support for File Imports

This document describes the new path pattern feature for file imports, which allows `file_pattern` configurations to include subdirectory paths.

## Overview

Previously, `file_pattern` only matched against filenames (e.g., `*.csv`, `IxExp*.csv`). Now patterns can include directory paths (e.g., `reports/*.csv`, `archive/2024/*.csv`).

**This change is fully backward compatible.** Existing configurations and API calls continue to work without modification.

## API Changes

### ImportRequest Schema

A new optional field has been added:

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `local_files_base_path` | `string` | No | `null` | Base path for computing relative paths when using path patterns with local files |

### Example Request (without path patterns - unchanged)

```json
{
  "project": "customer_abc",
  "local_files": [
    "/data/exports/IxExpKonto.csv",
    "/data/exports/IxExpMieter.csv"
  ]
}
```

This continues to work exactly as before. Files are matched by filename only.

### Example Request (with path patterns - new)

```json
{
  "project": "customer_abc",
  "local_files": [
    "/data/exports/reports/sales.csv",
    "/data/exports/reports/inventory.csv",
    "/data/exports/archive/2024/history.csv"
  ],
  "local_files_base_path": "/data/exports"
}
```

With `local_files_base_path` set, the system computes relative paths:
- `/data/exports/reports/sales.csv` → `reports/sales.csv`
- `/data/exports/reports/inventory.csv` → `reports/inventory.csv`
- `/data/exports/archive/2024/history.csv` → `archive/2024/history.csv`

These relative paths are then matched against patterns like `reports/*.csv` or `archive/*/*.csv`.

## Configuration Examples

### Filename Patterns (existing behavior)

```yaml
defaults:
  file_pattern: "*.csv"           # All CSV files
  primary_key: id

tables:
  - file_pattern: "IxExp*.csv"    # Files starting with "IxExp"
    target_table: exports
    primary_key: HDR_ID
```

### Path Patterns (new capability)

```yaml
defaults:
  file_pattern: "daily/*.csv"     # Only files in daily/ subdirectory
  primary_key: id

tables:
  - file_pattern: "reports/*.csv"
    target_table: reports
    primary_key: report_id

  - file_pattern: "archive/2024/*.csv"
    target_table: historical_2024
    primary_key: id

  - file_pattern: "*/exports/*.csv"   # Wildcard directory
    target_table: all_exports
    primary_key: export_id
```

## Pattern Syntax

| Pattern | Matches | Does Not Match |
|---------|---------|----------------|
| `*.csv` | `data.csv`, `report.csv` | `data.txt` |
| `IxExp*.csv` | `IxExpKonto.csv` | `Konto.csv` |
| `reports/*.csv` | `reports/sales.csv` | `archive/sales.csv`, `sales.csv` |
| `archive/2024/*.csv` | `archive/2024/data.csv` | `archive/2023/data.csv` |
| `*/daily/*.csv` | `reports/daily/data.csv`, `archive/daily/export.csv` | `reports/weekly/data.csv` |

## SFTP Imports

For SFTP imports, path pattern support is automatic:

1. If `file_pattern` contains `/`, the system recursively scans subdirectories
2. Downloaded files preserve their directory structure
3. Relative paths (from `remote_path`) are used for pattern matching

No API changes needed for SFTP imports - just update the project configuration with path patterns.

## Backward Compatibility

| Scenario | Change Required? |
|----------|------------------|
| Existing projects with filename patterns | None |
| Existing API calls without `local_files_base_path` | None |
| SFTP imports | None (automatic) |
| New projects wanting path patterns | Update `file_pattern` in config |
| Local imports wanting path patterns | Optionally send `local_files_base_path` |

## Frontend Implementation Notes

### When to use `local_files_base_path`

Only needed when:
1. The project configuration uses path patterns (patterns containing `/`)
2. You're importing local files (not SFTP)

### Determining the base path

The base path should be the common parent directory of all files being imported. For example:

```
Files to import:
  /data/exports/reports/sales.csv
  /data/exports/reports/inventory.csv
  /data/exports/archive/old.csv

Base path: /data/exports

Resulting relative paths:
  reports/sales.csv
  reports/inventory.csv
  archive/old.csv
```

### UI Considerations

If building a file picker UI:
- When users select files from different subdirectories, compute the common parent as `local_files_base_path`
- Display the relative paths to users so they understand how patterns will match
- Consider showing which files match which table configurations before starting the import

## Response Format

The job response `filename` field now contains the relative path (when `local_files_base_path` was provided) or the filename:

```json
{
  "file_results": [
    {
      "filename": "reports/sales.csv",
      "table_name": "reports",
      "inserted": 100,
      "updated": 5,
      "success": true
    }
  ]
}
```

## Questions?

Contact the backend team if you have questions about implementing path pattern support in the frontend.
