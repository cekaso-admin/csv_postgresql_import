# DBF Memo Support — Update 1.1

This document covers changes since the initial `DBF_MEMO_SUPPORT.md`. Read that document first for full context.

---

## Breaking Changes

### 1. New `download_pattern` field on defaults (required for DBF projects)

DBF projects now need **two** pattern fields in `defaults`:

| Field | Purpose | Example |
|-------|---------|---------|
| `download_pattern` | What to fetch from SFTP | `"*.dbf"` |
| `file_pattern` | What to match for import (after conversion) | `"*.csv"` |

**Why:** The `dbf_to_csv` hook converts `.dbf` files to `.csv`. Previously `file_pattern` controlled both download and import matching, so the import loop would try to match `LIEFER.csv` against `*.dbf` and fail. Now each concern has its own field.

**Before (broken):**
```json
"defaults": {
  "file_pattern": "*.dbf",
  "primary_key": "ID"
}
```

**After (correct):**
```json
"defaults": {
  "download_pattern": "*.dbf",
  "file_pattern": "*.csv",
  "primary_key": "ID"
}
```

When `download_pattern` is not set, it falls back to `file_pattern`. This means non-DBF projects (plain CSV) are unaffected.

---

## Frontend Changes Required

### TypeScript type update

```typescript
interface DefaultsConfig {
  download_pattern?: string;   // NEW — SFTP download pattern (e.g., "*.dbf")
  file_pattern?: string;       // default: "*.csv" — import matching pattern
  primary_key: string | string[];
  delimiter?: string;
  encoding?: string;
  skiprows?: number;
  rebuild_table?: boolean;
  datestyle?: string;
  schema?: string;
}
```

`DbfToCsvHook` is unchanged from the previous version.

### Form changes

Add a `download_pattern` field to the defaults section. Suggested UX:

```
Defaults
├── Download pattern: [*.dbf      ]   ← NEW (optional)
├── File pattern:     [*.csv      ]
├── Primary key:      [ID         ]
├── Delimiter:        [,          ]
└── ...
```

**Auto-fill logic:** When the user adds a `dbf_to_csv` hook and `download_pattern` is empty:
1. Auto-set `download_pattern` to `"*.dbf"`
2. Auto-set `file_pattern` to `"*.csv"` (if it was `"*.dbf"`)
3. Show a hint: "Download pattern fetches files from SFTP. File pattern matches converted CSV files for import."

**Validation hints:**

| Condition | Message |
|-----------|---------|
| `dbf_to_csv` hook exists but `download_pattern` is empty | "Set download_pattern to *.dbf so DBF files are fetched from SFTP" |
| `dbf_to_csv` hook exists but `file_pattern` is `*.dbf` | "file_pattern should be *.csv — after conversion, the import matches CSV files" |

### Updated config examples

All JSON config examples from `DBF_MEMO_SUPPORT.md` need `download_pattern` added. Here are the corrected versions:

**Project creation (POST /projects):**
```json
{
  "name": "legacy_foxpro_import",
  "connection_id": "conn-uuid",
  "source_id": "sftp-uuid",
  "config": {
    "defaults": {
      "download_pattern": "*.dbf",
      "file_pattern": "*.csv",
      "primary_key": "HDR_ID"
    },
    "hooks": {
      "post_file_prepare": [
        {
          "type": "dbf_to_csv",
          "encoding": "auto",
          "ignore_memos": false,
          "companion_extensions": [".fpt"],
          "delete_original": true,
          "on_error": "fail"
        }
      ]
    }
  }
}
```

**FoxPro with memo fields (most common pattern):**
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
        "ignore_memos": false,
        "companion_extensions": [".fpt"],
        "delete_original": true,
        "on_error": "fail"
      }
    ]
  }
}
```

**DBF without memo fields:**
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
        "encoding": "auto"
      }
    ]
  }
}
```

---

## Non-Breaking Changes (no frontend action needed)

### Progress logging for large files

The DBF converter now logs progress during conversion:
- Record count logged at start: `"Starting conversion of DATA.dbf: 1,200,000 records"`
- Progress every 50,000 rows: `"Converting DATA.dbf: 50,000/1,200,000 rows"`

This is backend-only. No UI changes required, but the job logs endpoint (`GET /jobs/{id}`) may surface these in the future.

### Batched CSV writes

CSV output is now written in batches of 10,000 rows with a 1 MB file buffer. This is a performance improvement with no config or API changes.

---

## Questions?

Contact the backend team or check the API docs at `/docs` (Swagger UI).
