# Quick Start Guide - Phase 1

## Setup (First Time)

```bash
# 1. Navigate to project
cd /Users/engincetinkaya/scripts/csv_postgresql_import

# 2. Activate virtual environment
source venv/bin/activate

# 3. Create environment file
cp .env.example .env

# 4. Edit .env with your credentials
nano .env  # or use your preferred editor
```

Edit `.env`:
```
DATABASE_URL=postgresql://user:password@host:5432/database
```

## Basic Usage

### Test Connection
```python
from src.db.connection import test_connection

if test_connection():
    print("Connected to database!")
```

### Import CSV File
```python
from src.db.importer import import_csv

result = import_csv(
    file_path="your_file.csv",
    table_name="your_table",
    primary_key="id"  # or ["col1", "col2"] for composite key
)

print(f"Inserted: {result.inserted}")
print(f"Updated: {result.updated}")
print(f"Success: {result.success}")
```

## Common Scenarios

### 1. First Import (Creates Table)
```python
# If table doesn't exist, it will be created automatically
result = import_csv("customers.csv", "customers", "customer_id")
```

### 2. Update Existing Data
```python
# Second import updates existing rows
result = import_csv("customers.csv", "customers", "customer_id")
# result.updated will show number of updated rows
```

### 3. Rebuild Table (Fresh Start)
```python
# Truncate table before import
result = import_csv(
    "data.csv",
    "my_table",
    "id",
    rebuild_table=True  # Removes all existing rows first
)
```

### 4. Column Name Mapping
```python
# Map CSV columns to database columns
result = import_csv(
    "german_data.csv",
    "customers",
    "customer_id",
    column_mapping={
        "Kunde Nr.": "customer_id",
        "Name": "name",
        "E-Mail": "email"
    }
)
```

### 5. Composite Primary Key
```python
# Use multiple columns as primary key
result = import_csv(
    "order_lines.csv",
    "order_lines",
    primary_key=["order_id", "line_number"]
)
```

## Important Notes

1. **File Size**: Automatically uses optimal strategy
   - <100MB: Direct batch insert
   - ≥100MB: Staging table with COPY

2. **All Columns are VARCHAR**: Type casting should be done in views

3. **Tables Never Dropped**: Only TRUNCATE when rebuild_table=True

4. **Upsert Behavior**: ON CONFLICT DO UPDATE
   - First import: All rows inserted
   - Second import: Matching rows updated, new rows inserted

5. **Memory Efficient**: Files are streamed in chunks, never fully loaded

## Troubleshooting

### Connection Failed
```python
# Check DATABASE_URL is correct
import os
from dotenv import load_dotenv
load_dotenv()
print(os.getenv("DATABASE_URL"))
```

### Table Not Found
```python
# Check if table exists
from src.db.schema import table_exists
if not table_exists("my_table"):
    print("Table will be created on first import")
```

### Import Errors
```python
result = import_csv(...)
if result.has_errors:
    print("Errors:", result.errors)
```

## File Paths (Absolute)

All code is in: `/Users/engincetinkaya/scripts/csv_postgresql_import/`

- Main modules: `src/db/`
- Examples: `example_usage.py`
- Documentation: `README_PHASE1.md`

## Running Example Script
```bash
python example_usage.py
```

## Next Phase

Phase 2 will add:
- YAML configuration files for projects
- Automatic file-to-table mapping
- Multiple file processing

For now, Phase 1 provides all core import functionality!
