# DBF Memo Field Support

This document explains the new **memo field support** for DBF imports. It extends the existing `dbf_to_csv` hook to read memo/text data stored in companion `.fpt`/`.dbt` files.

---

## Background

DBF (dBASE/FoxPro) files can store large text data in **memo fields**. This data lives in a separate companion file alongside the `.dbf`:

| Format | Companion Extension | Description |
|--------|-------------------|-------------|
| FoxPro / dBASE IV+ | `.fpt` | Most common for modern DBF files |
| dBASE III | `.dbt` | Older format |

**Example:** `CUSTOMERS.dbf` + `CUSTOMERS.fpt` = full data including memo fields.

Previously, the `dbf_to_csv` hook skipped memo fields entirely. With this update, they can be included in the CSV output.

---

## What Changed

Two new configuration options were added:

### 1. `companion_extensions` (on project defaults)

Tells the SFTP downloader to also fetch companion files alongside the primary files.

**Location:** `config.defaults.companion_extensions`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `companion_extensions` | string[] | `[]` | File extensions to download alongside primary files |

**Validation rules:**
- Each entry must start with `.` (e.g., `".fpt"`, not `"fpt"`)
- Values are normalized to lowercase and deduplicated automatically

### 2. `ignore_memos` (on `dbf_to_csv` hook)

Controls whether the DBF converter reads memo field data.

**Location:** `config.hooks.post_file_prepare[].ignore_memos`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ignore_memos` | boolean | `true` | When `false`, includes memo fields in CSV output |

**Important:** The default is `true` (skip memos) for backward compatibility. Users must explicitly set `false` to enable memo support.

---

## Configuration

Both options must be set together for memo support to work with SFTP imports:

```json
{
  "name": "legacy_import",
  "connection_id": "conn-uuid",
  "source_id": "sftp-uuid",
  "config": {
    "defaults": {
      "file_pattern": "*.dbf",
      "primary_key": "ID",
      "companion_extensions": [".fpt"]
    },
    "hooks": {
      "post_file_prepare": [
        {
          "type": "dbf_to_csv",
          "input_pattern": "*.dbf",
          "encoding": "auto",
          "ignore_memos": false,
          "delete_original": true,
          "on_error": "fail"
        }
      ]
    }
  }
}
```

### Why two separate settings?

| Setting | Purpose | Where it acts |
|---------|---------|---------------|
| `companion_extensions` | Download `.fpt` files from SFTP | Before hooks run (SFTP download step) |
| `ignore_memos` | Read memo data from `.fpt` files | During `dbf_to_csv` hook execution |

The SFTP downloader needs to know which extra files to fetch. The converter needs to know whether to read them. These are independent steps — for **local file imports**, only `ignore_memos: false` is needed (companion files are already on disk).

---

## How It Works

```
SFTP Server                    Import Pipeline
┌──────────────┐
│ DATA.dbf     │──── download (file_pattern: "*.dbf") ────┐
│ DATA.fpt     │──── download (companion_extensions) ─────┤
│ DATA.cdx     │     (not configured, skipped)            │
└──────────────┘                                          ▼
                                                   ┌─────────────┐
                                                   │  temp_dir/  │
                                                   │  DATA.dbf   │
                                                   │  DATA.fpt   │
                                                   └──────┬──────┘
                                                          │
                                                          ▼
                                                   dbf_to_csv hook
                                                   (ignore_memos: false)
                                                          │
                                                          ▼
                                                   ┌─────────────┐
                                                   │  DATA.csv   │ ← includes memo fields
                                                   └──────┬──────┘
                                                          │
                                                          ▼
                                                   CSV → PostgreSQL
```

**Key detail:** Companion files are downloaded to the same temp directory as the primary files but are **not** added to the import file list. They are only used by the `dbf_to_csv` converter.

---

## Error Handling

### Missing companion file

If `ignore_memos: false` is set but no `.fpt`/`.dbt` file exists for a DBF, the converter returns a clear error:

```
Memo support requested (ignore_memos=false) but no .fpt/.dbt companion
file found for DATA.dbf. Ensure the companion file exists in the same
directory as the .dbf file, and that companion_extensions is configured
in project defaults to download companion files via SFTP.
```

This error respects the hook's `on_error` setting (`"fail"`, `"warn"`, or `"ignore"`).

### Companion cleanup on `delete_original`

When `delete_original: true` is set, deleting a DBF file also deletes any companion files (`.fpt`, `.dbt`, `.cdx`, `.mdx`, `.ntx`) with the same base name.

---

## API Examples

### Creating a project with memo support

```http
POST /projects
Content-Type: application/json
X-API-Key: your-api-key

{
  "name": "legacy_foxpro_import",
  "connection_id": "conn-uuid",
  "source_id": "sftp-uuid",
  "config": {
    "defaults": {
      "file_pattern": "*.dbf",
      "primary_key": "HDR_ID",
      "companion_extensions": [".fpt"]
    },
    "hooks": {
      "post_file_prepare": [
        {
          "type": "dbf_to_csv",
          "encoding": "auto",
          "ignore_memos": false,
          "delete_original": true,
          "on_error": "fail"
        }
      ]
    }
  }
}
```

### Enabling memo support on an existing project

```http
PUT /projects/legacy_foxpro_import
Content-Type: application/json
X-API-Key: your-api-key

{
  "config": {
    "defaults": {
      "file_pattern": "*.dbf",
      "primary_key": "HDR_ID",
      "companion_extensions": [".fpt"]
    },
    "hooks": {
      "post_file_prepare": [
        {
          "type": "dbf_to_csv",
          "encoding": "auto",
          "ignore_memos": false,
          "delete_original": true,
          "on_error": "fail"
        }
      ]
    }
  }
}
```

---

## Updated TypeScript Types

```typescript
// Updated DefaultsConfig - new field
interface DefaultsConfig {
  file_pattern?: string;       // default: "*.csv"
  primary_key: string | string[];
  delimiter?: string;          // default: ","
  encoding?: string;           // default: "utf-8"
  skiprows?: number;           // default: 0
  rebuild_table?: boolean;     // default: false
  datestyle?: string;          // e.g., "DMY"
  schema?: string;             // default: "public"
  companion_extensions?: string[];  // NEW - e.g., [".fpt", ".dbt"]
}

// Updated DbfToCsvHook - new field
interface DbfToCsvHook extends BaseHookAction {
  type: 'dbf_to_csv';
  input_pattern?: string;      // default: "*.dbf"
  encoding?: string;           // default: "auto"
  delete_original?: boolean;   // default: false
  ignore_memos?: boolean;      // NEW - default: true
}
```

---

## UI Recommendations

### Project form changes

When the user selects `dbf_to_csv` as a hook type, show an additional toggle:

```
File Preparation Hooks
├── [DBF to CSV]
│   ├── Pattern: [*.dbf        ]
│   ├── Encoding: [auto ▼]
│   ├── Delete original: [x]
│   ├── Include memo fields: [ ]     ← NEW toggle
│   └── On error: [fail ▼]
```

When "Include memo fields" is toggled on:
1. Set `ignore_memos: false` on the hook action
2. Automatically add `".fpt"` to `config.defaults.companion_extensions` (if not already present)
3. Show a hint: "Companion .fpt files will be downloaded alongside .dbf files"

When toggled off:
1. Set `ignore_memos: true` (or remove the field)
2. Optionally remove `".fpt"` from `companion_extensions`

### Validation hints

| Condition | Message |
|-----------|---------|
| `ignore_memos: false` but `companion_extensions` is empty | "Memo support is enabled but no companion extensions are configured. Add .fpt to companion extensions so memo files are downloaded from SFTP." |
| `companion_extensions` has entries but no `dbf_to_csv` hook | "Companion extensions are configured but no DBF conversion hook is set up." |

---

## Common Patterns

### Pattern 1: FoxPro with memo fields (most common)

```json
{
  "defaults": {
    "file_pattern": "*.dbf",
    "primary_key": "ID",
    "companion_extensions": [".fpt"]
  },
  "hooks": {
    "post_file_prepare": [
      {
        "type": "dbf_to_csv",
        "ignore_memos": false,
        "delete_original": true,
        "on_error": "fail"
      }
    ]
  }
}
```

### Pattern 2: dBASE III with memo fields

```json
{
  "defaults": {
    "file_pattern": "*.dbf",
    "primary_key": "ID",
    "companion_extensions": [".dbt"]
  },
  "hooks": {
    "post_file_prepare": [
      {
        "type": "dbf_to_csv",
        "ignore_memos": false,
        "on_error": "fail"
      }
    ]
  }
}
```

### Pattern 3: DBF without memo fields (no change needed)

Existing configs continue to work unchanged:

```json
{
  "defaults": {
    "file_pattern": "*.dbf",
    "primary_key": "ID"
  },
  "hooks": {
    "post_file_prepare": [
      {
        "type": "dbf_to_csv",
        "encoding": "auto"
      }
    ]
  }
}
```

---

## Questions?

Contact the backend team or check the API documentation at `/docs` (Swagger UI) when the server is running.
