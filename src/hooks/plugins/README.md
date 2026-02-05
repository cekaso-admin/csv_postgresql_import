# Hook Plugin Development Guide

Create custom hook actions without modifying core code.

## Quick Start

1. Create a `.py` file in this directory (e.g., `my_transform.py`)
2. Import the base classes and implement your executor:

```python
"""
My custom file transformer.

Usage in project config:
  hooks:
    post_file_prepare:
      - type: my_transform
        source_ext: ".dat"
        target_ext: ".csv"
"""
from typing import ClassVar, Optional

from src.hooks import HookExecutor, HookResult, HookContext


class MyTransformExecutor(HookExecutor):
    action_type: ClassVar[str] = "my_transform"

    def validate_config(self, action: dict) -> Optional[str]:
        """Validate config at load time. Return error message or None."""
        if not action.get("source_ext"):
            return "source_ext is required"
        return None

    def execute(self, action: dict, context: HookContext) -> HookResult:
        """Execute the transformation."""
        source_ext = action.get("source_ext", ".dat")
        target_ext = action.get("target_ext", ".csv")
        transformed = {}

        for file_path in context.file_paths:
            if file_path.endswith(source_ext):
                new_path = file_path.replace(source_ext, target_ext)
                # ... perform transformation ...
                transformed[file_path] = new_path

        return HookResult(
            success=True,
            hook_name=action.get("name", self.action_type),
            hook_type=self.action_type,
            message=f"Transformed {len(transformed)} file(s)",
            transformed_files=transformed,
        )
```

3. Restart the server - your plugin is auto-discovered!

## HookExecutor Interface

Every plugin must extend `HookExecutor` and set `action_type`:

```python
from src.hooks import HookExecutor, HookResult, HookContext

class MyExecutor(HookExecutor):
    # Required: unique identifier used in YAML config
    action_type: ClassVar[str] = "my_action"

    # Required: main execution method
    def execute(self, action: dict, context: HookContext) -> HookResult:
        ...

    # Optional: custom run conditions (default: check enabled flag)
    def should_run(self, action: dict, context: HookContext) -> bool:
        return action.get("enabled", True)

    # Optional: validate config at load time
    def validate_config(self, action: dict) -> Optional[str]:
        return None  # or error message
```

## HookContext Reference

The `context` parameter provides runtime information:

| Field | Type | Description |
|-------|------|-------------|
| `job_id` | `str` | Unique job identifier |
| `project_name` | `str` | Project being processed |
| `database_url` | `str` | PostgreSQL connection URL |
| `temp_dir` | `Optional[str]` | Working directory for file operations |
| `file_paths` | `List[str]` | **Mutable** list of file paths |
| `files_processed` | `int` | Files successfully imported (post_import only) |
| `files_failed` | `int` | Files that failed import (post_import only) |
| `total_inserted` | `int` | Total rows inserted (post_import only) |
| `total_updated` | `int` | Total rows updated (post_import only) |
| `status` | `str` | Job status: "running", "completed", "failed", "partial" |

## HookResult Reference

Return a `HookResult` from your `execute()` method:

```python
from src.hooks import HookResult

# Success
return HookResult(
    success=True,
    hook_name="my_action",
    hook_type=self.action_type,
    message="Completed successfully",
    duration_seconds=1.5,  # Optional, auto-calculated if omitted
)

# Failure
return HookResult(
    success=False,
    hook_name="my_action",
    hook_type=self.action_type,
    error="Something went wrong",
)

# File transformation (for post_file_prepare hooks)
return HookResult(
    success=True,
    hook_name="my_transform",
    hook_type=self.action_type,
    transformed_files={
        "/tmp/data.dbf": "/tmp/data.csv",
        "/tmp/other.dbf": "/tmp/other.csv",
    },
)
```

## Configuration in Project YAML

Users configure your hook in their project config:

```yaml
project: my_project
hooks:
  post_file_prepare:
    - type: my_transform       # Your action_type
      name: "Custom Transform" # Optional friendly name
      enabled: true            # Default: true
      on_error: warn           # fail, warn, or ignore
      source_ext: ".dat"       # Your custom fields
      target_ext: ".csv"
```

All fields except `type` are optional. The `action` dict passed to `execute()`
contains all these fields plus any extras the user provides.

## Error Handling Policies

The `on_error` setting controls behavior when your hook fails:

| Value | Behavior |
|-------|----------|
| `fail` | Stop all processing, mark job as failed |
| `warn` | Log warning, record in job errors, continue |
| `ignore` | Silently continue (for non-critical hooks) |

Default is `warn`. Handle errors gracefully in your executor:

```python
def execute(self, action: dict, context: HookContext) -> HookResult:
    try:
        # Your logic here
        return HookResult(success=True, ...)
    except SpecificError as e:
        return HookResult(
            success=False,
            hook_name=action.get("name", self.action_type),
            hook_type=self.action_type,
            error=f"Failed to process: {e}",
        )
```

## Hook Points

Your hook can be configured at these points:

| Hook Point | When | Common Use Cases |
|------------|------|------------------|
| `pre_import` | Before any processing | Setup, validation, notifications |
| `post_file_prepare` | After files ready | File transformations (DBF→CSV, Excel→CSV) |
| `pre_file_import` | Per file, before import | Per-file validation |
| `post_file_import` | Per file, after import | Per-file notifications |
| `post_import` | After all files processed | Refresh views, run SQL, webhooks |

## Best Practices

1. **Don't raise exceptions** - catch all errors and return `HookResult(success=False, error=...)`

2. **Use structured logging**:
   ```python
   import logging
   logger = logging.getLogger(__name__)

   logger.info(
       "Processing file",
       extra={"job_id": context.job_id, "file": file_path}
   )
   ```

3. **Validate early** - implement `validate_config()` to catch issues at load time

4. **Be idempotent** - hooks may be retried; ensure safe re-execution

5. **Respect timeouts** - for long-running operations, consider chunking work

## Testing Your Plugin

Create a test file in `tests/hooks/plugins/`:

```python
import pytest
from src.hooks import HookContext
from src.hooks.plugins.my_transform import MyTransformExecutor


class TestMyTransformExecutor:
    def test_validate_config_missing_source_ext(self):
        executor = MyTransformExecutor()
        error = executor.validate_config({"type": "my_transform"})
        assert error == "source_ext is required"

    def test_execute_transforms_files(self, tmp_path):
        # Create test file
        test_file = tmp_path / "data.dat"
        test_file.write_text("test data")

        context = HookContext(
            job_id="test-job",
            project_name="test",
            database_url="postgresql://...",
            file_paths=[str(test_file)],
        )

        executor = MyTransformExecutor()
        result = executor.execute(
            {"type": "my_transform", "source_ext": ".dat", "target_ext": ".csv"},
            context,
        )

        assert result.success
        assert str(test_file) in result.transformed_files
```

Run with: `pytest tests/hooks/plugins/test_my_transform.py`

## Strict Mode

Set `CPI_STRICT_PLUGINS=true` in production to fail startup on plugin errors:

```bash
export CPI_STRICT_PLUGINS=true
```

This catches:
- Import errors in plugin files
- Missing `action_type` attributes
- Duplicate action_type registrations

## Need Help?

- Check built-in executors in `src/hooks/executors/` for examples
- Review `src/hooks/models.py` for complete type definitions
- The engine code in `src/hooks/engine.py` handles plugin discovery
