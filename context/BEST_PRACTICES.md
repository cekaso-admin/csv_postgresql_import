# Best Practices

Python and general coding standards for this project.

## Core Principles

### DRY (Don't Repeat Yourself)
- Extract repeated logic into functions
- Use configuration over hardcoding
- Single source of truth for constants and settings

### KISS (Keep It Simple, Stupid)
- Prefer simple solutions over clever ones
- Optimize for readability, not brevity
- If a function needs extensive comments, it's probably too complex

### YAGNI (You Aren't Gonna Need It)
- Don't build features until they're needed
- Avoid premature abstraction
- Start simple, refactor when patterns emerge

## Python Standards

### Code Style
- Follow PEP 8
- Use type hints for function signatures
- Maximum line length: 88 characters (Black formatter default)

### Naming Conventions
```python
# Variables and functions: snake_case
user_count = 0
def get_user_by_id(): pass

# Classes: PascalCase
class DatabaseConnection: pass

# Constants: UPPER_SNAKE_CASE
MAX_RETRIES = 3
DATABASE_URL = "..."

# Private/internal: prefix with underscore
_internal_cache = {}
def _helper_function(): pass
```

### Imports
```python
# Standard library first
import os
import sys
from pathlib import Path

# Third-party packages
import pandas as pd
from sqlalchemy import create_engine

# Local modules
from src.database import connection
from src.utils import helpers
```

### Functions
- Single responsibility: one function, one purpose
- Keep functions under 30 lines when possible
- Use early returns to reduce nesting

```python
# Good: Early return
def process_user(user):
    if not user:
        return None
    if not user.is_active:
        return None
    return user.process()

# Avoid: Deep nesting
def process_user(user):
    if user:
        if user.is_active:
            return user.process()
    return None
```

### Error Handling
- Catch specific exceptions, not bare `except:`
- Use custom exceptions for domain-specific errors
- Log errors with context

```python
# Good
try:
    result = database.query(sql)
except psycopg2.OperationalError as e:
    logger.error(f"Database connection failed: {e}")
    raise DatabaseConnectionError(f"Could not connect: {e}")

# Avoid
try:
    result = database.query(sql)
except:
    pass
```

### Type Hints
```python
from typing import Optional, List, Dict

def import_csv(
    file_path: str,
    table_name: str,
    batch_size: int = 1000
) -> int:
    """Import CSV to database, return row count."""
    ...

def get_table_schema(table_name: str) -> Optional[Dict[str, str]]:
    """Return column types or None if table doesn't exist."""
    ...
```

## Database Practices

### Connection Management
- Use connection pooling
- Always close connections/cursors
- Prefer context managers (`with` statements)

```python
# Good: Context manager ensures cleanup
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute(query)
```

### SQL Safety
- Always use parameterized queries
- Never concatenate user input into SQL

```python
# Good: Parameterized
cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))

# DANGEROUS: SQL injection risk
cursor.execute(f"SELECT * FROM users WHERE id = {user_id}")
```

### Transactions
- Use explicit transactions for multi-step operations
- Rollback on failure, commit on success

## Project-Specific Guidelines

### CSV Processing
- Validate CSV structure before processing
- Handle encoding issues (UTF-8 with fallback)
- Process in batches for large files
- Log progress for long-running imports

### Configuration
- Use environment variables for secrets
- Never commit credentials
- Provide sensible defaults where appropriate

```python
import os

DATABASE_URL = os.environ.get("DATABASE_URL")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
```

### Logging
- Use structured logging
- Include context (file name, row count, table name)
- Log at appropriate levels (DEBUG, INFO, WARNING, ERROR)

```python
import logging

logger = logging.getLogger(__name__)

logger.info(f"Starting import: {file_path} -> {table_name}")
logger.debug(f"Batch {batch_num}: {len(rows)} rows")
logger.error(f"Failed at row {row_num}: {error}")
```
