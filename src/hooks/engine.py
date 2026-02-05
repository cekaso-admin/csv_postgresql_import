"""
Hook engine for executing hooks at import lifecycle points.

The HookEngine discovers and manages hook executors, and orchestrates
the execution of hooks at each hook point.
"""

import importlib
import logging
import os
import time
from pathlib import Path
from typing import Dict, Optional, Type

from src.hooks.executors.base import HookExecutor
from src.hooks.models import (
    HookAction,
    HookConfig,
    HookContext,
    HookExecutionResult,
    HookPoint,
    HookResult,
    OnErrorBehavior,
)

logger = logging.getLogger(__name__)

# Registry of executor classes by action_type
_executors: Dict[str, Type[HookExecutor]] = {}

# Track registration sources for better error messages
_executor_sources: Dict[str, str] = {}


def _register_executor(
    action_type: str, cls: Type[HookExecutor], source: str
) -> None:
    """
    Register a hook executor class.

    Args:
        action_type: Unique identifier for the action type
        cls: The executor class to register
        source: Source description (e.g., "built-in" or filename)

    Raises:
        ValueError: If action_type is already registered by another class
    """
    if action_type in _executors:
        existing_source = _executor_sources.get(action_type, "unknown")
        raise ValueError(
            f"action_type '{action_type}' already registered by {existing_source}, "
            f"cannot load from {source}"
        )

    _executors[action_type] = cls
    _executor_sources[action_type] = source
    logger.info(f"Registered hook executor: {action_type} from {source}")


def _register_builtin_executors() -> None:
    """Register all built-in hook executors."""
    from src.hooks.executors.dbf_to_csv import DbfToCsvExecutor
    from src.hooks.executors.refresh_views import RefreshViewsExecutor

    builtin_executors = [
        DbfToCsvExecutor,
        RefreshViewsExecutor,
    ]

    for cls in builtin_executors:
        if not hasattr(cls, "action_type"):
            logger.error(f"Executor {cls.__name__} missing action_type")
            continue
        _register_executor(cls.action_type, cls, "built-in")


def _discover_plugins() -> None:
    """
    Auto-discover hook executors from the plugins/ directory.

    Each .py file (except those starting with _) is loaded as a module.
    Any class extending HookExecutor with a unique action_type is registered.

    Set CPI_STRICT_PLUGINS=true to fail on plugin errors (recommended for production).
    """
    plugins_dir = Path(__file__).parent / "plugins"
    strict_mode = os.getenv("CPI_STRICT_PLUGINS", "false").lower() == "true"

    if not plugins_dir.exists():
        logger.debug(f"Plugins directory not found: {plugins_dir}")
        return

    for py_file in plugins_dir.glob("*.py"):
        if py_file.name.startswith("_"):
            continue

        module_name = f"src.hooks.plugins.{py_file.stem}"
        try:
            module = importlib.import_module(module_name)

            for attr_name in dir(module):
                attr = getattr(module, attr_name)

                # Check if it's a HookExecutor subclass (but not HookExecutor itself)
                if (
                    isinstance(attr, type)
                    and issubclass(attr, HookExecutor)
                    and attr is not HookExecutor
                ):
                    if not hasattr(attr, "action_type"):
                        logger.warning(
                            f"Plugin class {attr.__name__} in {py_file.name} "
                            f"missing action_type, skipping"
                        )
                        continue

                    _register_executor(attr.action_type, attr, py_file.name)

        except ValueError as e:
            # Re-raise registration collisions
            if strict_mode:
                raise
            logger.error(str(e))

        except Exception as e:
            error_msg = f"Failed to load plugin {py_file.name}: {e}"
            if strict_mode:
                raise RuntimeError(error_msg) from e
            logger.error(error_msg, exc_info=True)


def _ensure_executors_loaded() -> None:
    """Ensure executors are loaded (lazy initialization)."""
    if not _executors:
        _register_builtin_executors()
        _discover_plugins()


def get_executor(action_type: str) -> Optional[Type[HookExecutor]]:
    """
    Get the executor class for an action type.

    Args:
        action_type: The action type identifier

    Returns:
        The executor class, or None if not found
    """
    _ensure_executors_loaded()
    return _executors.get(action_type)


def get_registered_action_types() -> list[str]:
    """
    Get list of all registered action types.

    Returns:
        List of registered action type identifiers
    """
    _ensure_executors_loaded()
    return list(_executors.keys())


class HookEngine:
    """
    Engine for executing hooks at import lifecycle points.

    The engine manages hook configuration and orchestrates execution,
    handling error policies and result aggregation.

    Example:
        ```python
        engine = HookEngine(config.get_effective_hooks())
        context = HookContext(
            job_id="job-123",
            project_name="my_project",
            database_url="postgresql://...",
            file_paths=["/tmp/data.csv"],
        )

        result = engine.execute_hooks(HookPoint.POST_IMPORT, context)
        if result.should_abort:
            # Handle critical failure
            ...
        ```
    """

    def __init__(self, config: Optional[HookConfig] = None) -> None:
        """
        Initialize the hook engine.

        Args:
            config: Hook configuration. If None, no hooks will execute.
        """
        self._config = config or HookConfig()
        _ensure_executors_loaded()

    @property
    def config(self) -> HookConfig:
        """Get the hook configuration."""
        return self._config

    def validate_config(self) -> list[str]:
        """
        Validate all hook configurations.

        Returns:
            List of validation error messages (empty if all valid)
        """
        errors: list[str] = []

        for hook_point in HookPoint:
            hooks = self._config.get_hooks_for_point(hook_point)

            for i, action in enumerate(hooks):
                action_dict = action.model_dump()
                action_type = action_dict.get("type")

                if not action_type:
                    errors.append(
                        f"{hook_point.value}[{i}]: missing 'type' field"
                    )
                    continue

                executor_cls = get_executor(action_type)
                if not executor_cls:
                    errors.append(
                        f"{hook_point.value}[{i}]: unknown action type '{action_type}'"
                    )
                    continue

                # Run executor-specific validation
                executor = executor_cls()
                validation_error = executor.validate_config(action_dict)
                if validation_error:
                    errors.append(
                        f"{hook_point.value}[{i}] ({action_type}): {validation_error}"
                    )

        return errors

    def execute_hooks(
        self, hook_point: HookPoint, context: HookContext
    ) -> HookExecutionResult:
        """
        Execute all hooks configured for a hook point.

        Hooks are executed in order. Error handling is determined by
        each hook's on_error setting:
        - fail: Stop execution and set should_abort=True
        - warn: Log warning, record error, continue
        - ignore: Silently continue

        File transformation hooks update context.file_paths in place.

        Args:
            hook_point: The hook point to execute
            context: The execution context (may be modified by hooks)

        Returns:
            HookExecutionResult with individual results and abort flag
        """
        hooks = self._config.get_hooks_for_point(hook_point)
        result = HookExecutionResult()

        if not hooks:
            logger.debug(f"No hooks configured for {hook_point.value}")
            return result

        logger.info(
            f"Executing {len(hooks)} hook(s) at {hook_point.value}",
            extra={
                "job_id": context.job_id,
                "project": context.project_name,
                "hook_point": hook_point.value,
                "hook_count": len(hooks),
            },
        )

        for action in hooks:
            action_dict = action.model_dump()
            action_type = action_dict.get("type")
            hook_name = action.get_name()

            # Get executor
            executor_cls = get_executor(action_type)
            if not executor_cls:
                error_msg = f"Unknown action type: {action_type}"
                logger.error(error_msg, extra={"job_id": context.job_id})
                result.errors.append(error_msg)

                if action.on_error == OnErrorBehavior.FAIL:
                    result.should_abort = True
                    return result
                continue

            executor = executor_cls()

            # Check if hook should run
            if not executor.should_run(action_dict, context):
                logger.debug(
                    f"Hook '{hook_name}' skipped (should_run=False)",
                    extra={"job_id": context.job_id},
                )
                continue

            # Execute hook
            logger.debug(
                f"Executing hook '{hook_name}' ({action_type})",
                extra={"job_id": context.job_id},
            )

            try:
                start_time = time.time()
                hook_result = executor.execute(action_dict, context)

                # Ensure duration is set
                if hook_result.duration_seconds == 0:
                    hook_result.duration_seconds = time.time() - start_time

                result.results.append(hook_result)

                if hook_result.success:
                    logger.info(
                        f"Hook '{hook_name}' completed successfully",
                        extra={
                            "job_id": context.job_id,
                            "hook_name": hook_name,
                            "hook_type": action_type,
                            "duration_seconds": hook_result.duration_seconds,
                            "message": hook_result.message,
                        },
                    )

                    # Apply file transformations
                    if hook_result.transformed_files:
                        self._apply_file_transformations(
                            context, hook_result.transformed_files
                        )

                else:
                    self._handle_hook_error(
                        hook_name,
                        action_type,
                        action.on_error,
                        hook_result.error,
                        context,
                        result,
                    )

                    if result.should_abort:
                        return result

            except Exception as e:
                # Executor should not raise, but handle gracefully if it does
                error_msg = f"Unexpected error in hook '{hook_name}': {e}"
                logger.error(error_msg, extra={"job_id": context.job_id}, exc_info=True)

                hook_result = HookResult(
                    success=False,
                    hook_name=hook_name,
                    hook_type=action_type,
                    error=error_msg,
                    duration_seconds=time.time() - start_time,
                )
                result.results.append(hook_result)

                self._handle_hook_error(
                    hook_name,
                    action_type,
                    action.on_error,
                    error_msg,
                    context,
                    result,
                )

                if result.should_abort:
                    return result

        logger.info(
            f"Completed {len(result.results)} hook(s) at {hook_point.value}",
            extra={
                "job_id": context.job_id,
                "hook_point": hook_point.value,
                "successful": sum(1 for r in result.results if r.success),
                "failed": sum(1 for r in result.results if not r.success),
                "total_duration": result.total_duration_seconds,
            },
        )

        return result

    def _handle_hook_error(
        self,
        hook_name: str,
        action_type: str,
        on_error: OnErrorBehavior,
        error_msg: Optional[str],
        context: HookContext,
        result: HookExecutionResult,
    ) -> None:
        """Handle a hook execution error based on the on_error policy."""
        error_msg = error_msg or "Unknown error"

        if on_error == OnErrorBehavior.FAIL:
            logger.error(
                f"Hook '{hook_name}' failed (on_error=fail), aborting",
                extra={
                    "job_id": context.job_id,
                    "hook_name": hook_name,
                    "error": error_msg,
                },
            )
            result.errors.append(f"{hook_name}: {error_msg}")
            result.should_abort = True

        elif on_error == OnErrorBehavior.WARN:
            logger.warning(
                f"Hook '{hook_name}' failed (on_error=warn), continuing",
                extra={
                    "job_id": context.job_id,
                    "hook_name": hook_name,
                    "error": error_msg,
                },
            )
            result.errors.append(f"{hook_name}: {error_msg}")

        else:  # IGNORE
            logger.debug(
                f"Hook '{hook_name}' failed (on_error=ignore), silently continuing",
                extra={
                    "job_id": context.job_id,
                    "hook_name": hook_name,
                    "error": error_msg,
                },
            )

    def _apply_file_transformations(
        self, context: HookContext, transformed_files: Dict[str, str]
    ) -> None:
        """
        Apply file transformations to context.file_paths in place.

        Args:
            context: The hook context with file_paths to update
            transformed_files: Mapping of original paths to new paths
        """
        if not transformed_files:
            return

        # Build new file_paths list, replacing transformed files
        new_paths = []
        for path in context.file_paths:
            if path in transformed_files:
                new_path = transformed_files[path]
                logger.debug(f"Transformed file: {path} -> {new_path}")
                new_paths.append(new_path)
            else:
                new_paths.append(path)

        # Update in place
        context.file_paths.clear()
        context.file_paths.extend(new_paths)

        logger.info(
            f"Applied {len(transformed_files)} file transformation(s)",
            extra={"transformed_files": transformed_files},
        )
