# DBF Memo Support — Update 1.2

This document covers changes since `DBF_MEMO_SUPPORT_UPDATE_1.1.md`. Read the previous documents first for full context.

---

## Breaking Changes

### 1. The `dbf_to_csv` hook has been removed

The backend no longer uses a hook to convert DBF files to CSV. Instead, DBF files are **imported directly** into PostgreSQL using a new C-backed reader (pyogrio/GDAL). This is ~10-20x faster than the old approach.

**What this means:**
- The `dbf_to_csv` hook type no longer exists on the backend
- Any `hooks.post_file_prepare` entries with `type: "dbf_to_csv"` are now ignored
- The fields `ignore_memos`, `input_pattern`, `delete_original` on the hook are gone
- DBF file detection is now **automatic** based on file extension — no hook configuration needed

### 2. `companion_extensions` moved from hook to defaults

Previously `companion_extensions` lived on the `dbf_to_csv` hook action. It now lives on `defaults`.

**Before (1.1):**
```json
{
  "defaults": {
    "download_pattern": "*.dbf",
    "file_pattern": "*.csv",
    "primary_key": "ID"
  },
  "hooks": {
    "post_file_prepare": [
      {
        "type": "dbf_to_csv",
        "companion_extensions": [".fpt"],
        "ignore_memos": false,
        "delete_original": true,
        "on_error": "fail"
      }
    ]
  }
}
```

**After (1.2):**
```json
{
  "defaults": {
    "file_pattern": "*.dbf",
    "companion_extensions": [".fpt"],
    "primary_key": "ID"
  }
}
```

Key differences:
- No `hooks` section needed at all
- `file_pattern` is now `"*.dbf"` (not `"*.csv"`) — files are no longer converted
- `download_pattern` is no longer needed when `file_pattern` already matches the SFTP files
- `companion_extensions` is on `defaults`, not on a hook action

### 3. `file_pattern` must be `"*.dbf"` for DBF projects

Since there is no conversion step, the import loop matches files by their original `.dbf` extension. Projects that previously used `file_pattern: "*.csv"` with the hook-based conversion **must** change to `file_pattern: "*.dbf"`.

| Version | `file_pattern` | `download_pattern` | Why |
|---------|---------------|-------------------|-----|
| 1.0/1.1 | `"*.csv"` | `"*.dbf"` | Hook converted DBF to CSV before import |
| **1.2** | **`"*.dbf"`** | not needed | Files imported directly as DBF |

---

## Frontend Changes Required

### TypeScript type updates

```typescript
// DELETE this interface — hook type no longer exists
// interface DbfToCsvHook extends BaseHookAction { ... }

// UPDATED — companion_extensions moved here
interface DefaultsConfig {
  file_pattern?: string;              // default: "*.csv"
  download_pattern?: string;          // optional — falls back to file_pattern
  companion_extensions?: string[];    // NEW HERE — e.g., [".fpt", ".dbt"]
  primary_key: string | string[];
  delimiter?: string;                 // default: ","
  encoding?: string;                  // default: "utf-8"
  skiprows?: number;                  // default: 0
  rebuild_table?: boolean;            // default: false
  datestyle?: string;                 // e.g., "DMY"
  schema?: string;                    // default: "public"
}
```

### Form changes

Remove the entire `dbf_to_csv` hook configuration section from the hooks form. Replace it with a simpler setup in the defaults section:

**Before (1.1):**
```
Defaults
├── Download pattern: [*.dbf      ]
├── File pattern:     [*.csv      ]
├── Primary key:      [ID         ]
└── ...

File Preparation Hooks
├── [DBF to CSV]
│   ├── Pattern: [*.dbf        ]
│   ├── Encoding: [auto ▼]
│   ├── Delete original: [x]
│   ├── Include memo fields: [ ]
│   └── On error: [fail ▼]
```

**After (1.2):**
```
Defaults
├── File pattern:             [*.dbf      ]
├── Companion extensions:     [.fpt       ]   ← moved here from hook
├── Encoding:                 [cp850 ▼    ]   ← encoding for DBF reading
├── Primary key:              [ID         ]
└── ...
```

No hooks section needed for DBF projects.

### Auto-detection hint

When `file_pattern` ends with `.dbf`, show an info message:

> "DBF files are imported directly into PostgreSQL — no conversion hook needed. Companion files (.fpt/.dbt) are downloaded automatically when configured."

### Validation hints

| Condition | Message |
|-----------|---------|
| `file_pattern` is `"*.dbf"` and `companion_extensions` is empty | "If your DBF files use memo fields, add .fpt to companion extensions" |
| `file_pattern` is `"*.csv"` but a `dbf_to_csv` hook still exists | "The dbf_to_csv hook has been removed. Change file_pattern to *.dbf for direct DBF import" |
| `download_pattern` is `"*.dbf"` but `file_pattern` is `"*.csv"` | "file_pattern should be *.dbf — DBF files are now imported directly without conversion" |

### Encoding note

The `encoding` field on defaults is now passed directly to the DBF reader (GDAL). When set to `"utf-8"` (the default), GDAL auto-detects the encoding from the DBF file header. For old German/European DBF files, users should set an explicit encoding like `"cp850"` or `"latin-1"`.

---

## Migration Guide

### For existing DBF projects

1. Change `file_pattern` from `"*.csv"` to `"*.dbf"`
2. Move `companion_extensions` from the hook to `defaults`
3. Remove the `download_pattern` field (no longer needed when `file_pattern` matches)
4. Remove the entire `hooks.post_file_prepare` section (if it only contained `dbf_to_csv`)
5. If encoding was set on the hook (e.g., `"cp850"`), move it to `defaults.encoding`

### Full migration example

**Before (1.1 config):**
```json
{
  "defaults": {
    "download_pattern": "*.dbf",
    "file_pattern": "*.csv",
    "primary_key": "HDR_ID",
    "encoding": "utf-8"
  },
  "hooks": {
    "post_file_prepare": [
      {
        "type": "dbf_to_csv",
        "encoding": "cp850",
        "ignore_memos": false,
        "companion_extensions": [".fpt"],
        "delete_original": true,
        "on_error": "fail"
      }
    ]
  }
}
```

**After (1.2 config):**
```json
{
  "defaults": {
    "file_pattern": "*.dbf",
    "companion_extensions": [".fpt"],
    "primary_key": "HDR_ID",
    "encoding": "cp850"
  }
}
```

### For CSV-only projects

No changes needed. CSV projects are completely unaffected.

---

## Updated Config Patterns

### Pattern 1: FoxPro with memo fields

```json
{
  "defaults": {
    "file_pattern": "*.dbf",
    "companion_extensions": [".fpt"],
    "primary_key": "ID",
    "encoding": "cp850"
  }
}
```

### Pattern 2: dBASE III with memo fields

```json
{
  "defaults": {
    "file_pattern": "*.dbf",
    "companion_extensions": [".dbt"],
    "primary_key": "ID"
  }
}
```

### Pattern 3: DBF without memo fields

```json
{
  "defaults": {
    "file_pattern": "*.dbf",
    "primary_key": "ID"
  }
}
```

### Pattern 4: Mixed CSV and DBF files

The import loop auto-detects by file extension. Both file types can coexist:

```json
{
  "defaults": {
    "file_pattern": "*",
    "primary_key": "ID"
  }
}
```

---

## Non-Breaking Changes (no frontend action needed)

### Performance improvement

DBF imports are now ~10-20x faster. A 30 GB batch that previously took ~4 hours should complete in ~10-20 minutes. This is transparent to the frontend — the same job status endpoints work as before.

### Memo fields handled automatically

GDAL (the new DBF reader) reads companion `.fpt`/`.dbt` files automatically when they exist in the same directory. The `ignore_memos` setting is no longer needed — memo fields are always included if the companion file is present.

---

## Summary of Removed Fields

| Field | Where it was | Status |
|-------|-------------|--------|
| `hooks.post_file_prepare[].type: "dbf_to_csv"` | Hook config | Removed |
| `hooks.post_file_prepare[].ignore_memos` | Hook config | Removed (memos always included) |
| `hooks.post_file_prepare[].input_pattern` | Hook config | Removed |
| `hooks.post_file_prepare[].delete_original` | Hook config | Removed |
| `hooks.post_file_prepare[].companion_extensions` | Hook config | Moved to `defaults.companion_extensions` |

## Summary of New/Moved Fields

| Field | Where | Type | Default | Description |
|-------|-------|------|---------|-------------|
| `defaults.companion_extensions` | Defaults | string[] | `[]` | Companion file extensions for SFTP download |

---

## Questions?

Contact the backend team or check the API docs at `/docs` (Swagger UI).
