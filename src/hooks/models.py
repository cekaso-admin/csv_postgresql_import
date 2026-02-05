"""
Data models for the hook system.

This module defines the core data structures for hooks:
- HookPoint: Enum of hook execution points in the import lifecycle
- HookContext: Runtime context passed to all hook executors
- HookResult: Result returned by individual hook executors
- HookExecutionResult: Aggregated result from executing all hooks at a point
- HookAction: Configuration for a single hook action
- HookConfig: Configuration for all hooks in a project
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


class HookPoint(str, Enum):
    """
    Points in the import lifecycle where hooks can execute.

    Attributes:
        PRE_IMPORT: Before any processing (setup, validation, notifications)
        POST_FILE_PREPARE: After files ready (SFTP download OR local files provided)
        PRE_FILE_IMPORT: Per file, before import
        POST_FILE_IMPORT: Per file, after import
        POST_IMPORT: After all files processed (refresh views, run SQL, webhooks)
    """

    PRE_IMPORT = "pre_import"
    POST_FILE_PREPARE = "post_file_prepare"
    PRE_FILE_IMPORT = "pre_file_import"
    POST_FILE_IMPORT = "post_file_import"
    POST_IMPORT = "post_import"


class OnErrorBehavior(str, Enum):
    """
    How to handle errors in hook execution.

    Attributes:
        FAIL: Stop execution, mark job failed
        WARN: Log warning, continue, record in job errors
        IGNORE: Silently continue
    """

    FAIL = "fail"
    WARN = "warn"
    IGNORE = "ignore"


@dataclass
class HookContext:
    """
    Runtime context passed to all hook executors.

    This mutable context is shared across all hooks at a given hook point.
    Hooks can modify file_paths to transform files (e.g., DBF to CSV).

    Attributes:
        job_id: Unique job identifier
        project_name: Name of the project being processed
        database_url: Database connection URL (required for SQL/refresh hooks)
        temp_dir: Working directory for file transforms (optional)
        file_paths: Mutable list of file paths - hooks can modify this
        files_processed: Number of files successfully processed (updated after imports)
        files_failed: Number of files that failed import
        total_inserted: Total rows inserted across all files
        total_updated: Total rows updated across all files
        status: Current job status (running, completed, failed, partial)
    """

    job_id: str
    project_name: str
    database_url: str
    temp_dir: Optional[str] = None
    file_paths: List[str] = field(default_factory=list)
    files_processed: int = 0
    files_failed: int = 0
    total_inserted: int = 0
    total_updated: int = 0
    status: str = "running"


@dataclass
class HookResult:
    """
    Result returned by a hook executor after execution.

    Attributes:
        success: Whether the hook executed successfully
        hook_name: Name identifier for this hook execution
        hook_type: The action_type of the executor that ran
        error: Error message if success is False
        message: Informational message about what the hook did
        duration_seconds: How long the hook took to execute
        transformed_files: Mapping of original file paths to transformed paths
            (e.g., {"original.dbf": "original.csv"})
    """

    success: bool
    hook_name: str
    hook_type: str
    error: Optional[str] = None
    message: Optional[str] = None
    duration_seconds: float = 0.0
    transformed_files: Dict[str, str] = field(default_factory=dict)


@dataclass
class HookExecutionResult:
    """
    Aggregated result from executing all hooks at a hook point.

    Attributes:
        results: List of individual HookResult from each executed hook
        should_abort: Whether the import should abort due to a critical failure
        errors: List of error messages from failed hooks
    """

    results: List[HookResult] = field(default_factory=list)
    should_abort: bool = False
    errors: List[str] = field(default_factory=list)

    @property
    def all_successful(self) -> bool:
        """Check if all hooks executed successfully."""
        return all(r.success for r in self.results)

    @property
    def total_duration_seconds(self) -> float:
        """Total time spent executing all hooks."""
        return sum(r.duration_seconds for r in self.results)


class HookAction(BaseModel):
    """
    Configuration for a single hook action.

    Attributes:
        type: The action type (e.g., "refresh_views", "dbf_to_csv", "run_sql")
        name: Optional friendly name for this hook (defaults to type)
        enabled: Whether this hook is enabled (default: True)
        on_error: Error handling behavior (fail, warn, ignore)
        Additional fields depend on the action type.
    """

    type: str = Field(..., description="Action type identifier")
    name: Optional[str] = Field(None, description="Friendly name for this hook")
    enabled: bool = Field(True, description="Whether this hook is enabled")
    on_error: OnErrorBehavior = Field(
        OnErrorBehavior.WARN, description="Error handling behavior"
    )

    # Allow additional fields for action-specific configuration
    model_config = ConfigDict(extra="allow")

    def get_name(self) -> str:
        """Get the effective name for this hook."""
        return self.name if self.name else self.type


class HookConfig(BaseModel):
    """
    Configuration for all hooks in a project.

    Each attribute is a list of HookAction configurations that will
    execute at that hook point.

    Attributes:
        pre_import: Hooks to run before any processing
        post_file_prepare: Hooks to run after files are ready
        pre_file_import: Hooks to run before each file import
        post_file_import: Hooks to run after each file import
        post_import: Hooks to run after all files are processed
    """

    pre_import: List[HookAction] = Field(default_factory=list)
    post_file_prepare: List[HookAction] = Field(default_factory=list)
    pre_file_import: List[HookAction] = Field(default_factory=list)
    post_file_import: List[HookAction] = Field(default_factory=list)
    post_import: List[HookAction] = Field(default_factory=list)

    def get_hooks_for_point(self, hook_point: HookPoint) -> List[HookAction]:
        """
        Get the list of hooks configured for a specific hook point.

        Args:
            hook_point: The hook point to get hooks for

        Returns:
            List of HookAction configurations for that point
        """
        mapping = {
            HookPoint.PRE_IMPORT: self.pre_import,
            HookPoint.POST_FILE_PREPARE: self.post_file_prepare,
            HookPoint.PRE_FILE_IMPORT: self.pre_file_import,
            HookPoint.POST_FILE_IMPORT: self.post_file_import,
            HookPoint.POST_IMPORT: self.post_import,
        }
        return mapping.get(hook_point, [])

    def has_hooks(self) -> bool:
        """Check if any hooks are configured."""
        return bool(
            self.pre_import
            or self.post_file_prepare
            or self.pre_file_import
            or self.post_file_import
            or self.post_import
        )
