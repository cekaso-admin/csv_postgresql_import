# ADR-003: Auto-Discovery Mode with Table Naming

**Status:** Accepted
**Date:** 2024-12-10

## Context

Some projects have many CSV files (e.g., 150 files) that share the same structure:
- Same primary key column (e.g., `HDR_ID`)
- Same CSV format (delimiter, encoding, skiprows)
- Filenames follow a pattern (e.g., `IxExpKonto.csv`, `IxExpMieter.csv`)

Listing each file individually in the config file is:
- Tedious and error-prone
- Hard to maintain when new files are added
- Unnecessary when all files have the same settings

## Decision

**Support two configuration modes:**

### 1. Auto-Discovery Mode (new)

Use `defaults` + `table_naming` to process all matching files with shared settings:

```yaml
defaults:
  file_pattern: "IxExp*.csv"   # Which files to process
  primary_key: HDR_ID          # Same for all files
  delimiter: "|"
  encoding: "latin-1"
  skiprows: 1

table_naming:
  strip_prefix: "IxExp"        # IxExpKonto.csv → Konto
  strip_suffix: ""             # Optional suffix removal
  lowercase: true              # Konto → konto
```

Result: `IxExpKonto.csv` automatically maps to table `konto` with all default settings.

### 2. Explicit Mode (existing)

Use `tables` list for specific file-to-table mappings:

```yaml
tables:
  - file_pattern: "special_file.csv"
    target_table: custom_table
    primary_key: custom_id
```

### Combined Mode

Both modes can be used together. Explicit `tables` entries **override** defaults for matching files.

```yaml
defaults:
  file_pattern: "*.csv"
  primary_key: HDR_ID

tables:
  # This file uses a different primary key
  - file_pattern: "orders*.csv"
    target_table: orders
    primary_key: [order_id, line_number]
```

## Table Name Transformation

The `table_naming` config supports:

| Setting | Description | Example |
|---------|-------------|---------|
| `strip_prefix` | Remove prefix from filename | `IxExpKonto.csv` → `Konto.csv` |
| `strip_suffix` | Remove suffix before extension | `Konto_Daily.csv` → `Konto.csv` |
| `lowercase` | Convert to lowercase | `Konto` → `konto` |

Transformation is case-insensitive for prefix/suffix matching.

## Resolution Order

When `get_table_for_file(filename)` is called:

1. Check explicit `tables` list for matching pattern → return if found
2. Check if `defaults` exists and file matches `defaults.file_pattern` → generate config
3. Return `None` if no match

## Consequences

### Positive

- Projects with 150+ files need only ~10 lines of config instead of 500+
- New files are automatically processed without config changes
- Explicit overrides still possible for special cases
- Backwards compatible - existing explicit configs still work

### Negative

- More complex config parsing logic
- Users must understand both modes
- Filename conventions become important (consistent prefixes/suffixes)

### Risks

- Accidental file processing if `file_pattern` is too broad (mitigate: use specific patterns like `IxExp*.csv`)
- Table name collisions if transformation produces duplicates (mitigate: validate on config load)

## Examples

### Project with 150 similar files

```yaml
project: large_customer
defaults:
  file_pattern: "IxExp*.csv"
  primary_key: HDR_ID
  delimiter: "|"
  encoding: "latin-1"
  skiprows: 1
table_naming:
  strip_prefix: "IxExp"
  lowercase: true
```

Files processed:
- `IxExpKonto.csv` → table `konto`
- `IxExpMieter.csv` → table `mieter`
- `IxExpObjekt.csv` → table `objekt`
- ... (147 more files automatically)

### Mixed project (defaults + overrides)

```yaml
project: mixed_customer
defaults:
  file_pattern: "*.csv"
  primary_key: id
table_naming:
  lowercase: true
tables:
  # This file needs special handling
  - file_pattern: "orders*.csv"
    target_table: order_lines
    primary_key: [order_id, line_number]
    column_mapping:
      "Bestellung": order_id
```
