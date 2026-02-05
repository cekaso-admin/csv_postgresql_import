"""
Base class for hook executors.

All hook executors must extend HookExecutor and implement the execute() method.
This is the primary interface for plugin developers.
"""

from abc import ABC, abstractmethod
from typing import ClassVar, Optional

from src.hooks.models import HookContext, HookResult


class HookExecutor(ABC):
    """
    Abstract base class for hook executors.

    Extend this class to create custom hook actions. Each executor handles
    a specific action type (e.g., "refresh_views", "dbf_to_csv", "run_sql").

    Class Attributes:
        action_type: Unique identifier used in config (e.g., type: "my_action").
            Must be set by subclasses.

    Example:
        ```python
        from src.hooks import HookExecutor, HookResult, HookContext

        class MyCustomExecutor(HookExecutor):
            action_type = "my_custom"

            def execute(self, action: dict, context: HookContext) -> HookResult:
                # Your implementation here
                return HookResult(
                    success=True,
                    hook_name=action.get("name", self.action_type),
                    hook_type=self.action_type,
                    message="Custom action completed",
                )
        ```
    """

    action_type: ClassVar[str]

    @abstractmethod
    def execute(self, action: dict, context: HookContext) -> HookResult:
        """
        Execute the hook action.

        This is the main method that performs the hook's work. It receives
        the action configuration (from YAML) and the runtime context.

        Args:
            action: The action config dict containing:
                - type: Action type identifier (e.g., "refresh_views")
                - name: Optional friendly name
                - enabled: Whether enabled (default True)
                - on_error: Error handling ("fail", "warn", "ignore")
                - Plus any action-specific fields
            context: HookContext with runtime information:
                - job_id: Current job identifier
                - project_name: Project being processed
                - database_url: Database connection URL
                - temp_dir: Working directory for file operations
                - file_paths: List of files (mutable - can be modified)
                - files_processed, files_failed, total_inserted, etc.

        Returns:
            HookResult with:
                - success: True if action completed successfully
                - hook_name: Name for logging/tracking
                - hook_type: The action_type
                - error: Error message if failed
                - message: Success message
                - duration_seconds: Execution time
                - transformed_files: Dict of original->new paths (for transforms)

        Raises:
            Should not raise exceptions - catch all errors and return
            HookResult with success=False and error message.
        """
        pass

    def should_run(self, action: dict, context: HookContext) -> bool:
        """
        Determine if this hook should run.

        Override to add custom conditions beyond the enabled flag.
        The default implementation checks the "enabled" field.

        Args:
            action: The action configuration dict
            context: The hook execution context

        Returns:
            True if the hook should execute, False to skip
        """
        return action.get("enabled", True)

    def validate_config(self, action: dict) -> Optional[str]:
        """
        Validate the action configuration.

        Override to check that required fields are present and valid.
        Called during project config loading, before any hooks execute.

        Args:
            action: The action configuration dict

        Returns:
            Error message string if validation fails, None if valid
        """
        return None
