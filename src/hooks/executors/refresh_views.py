"""
Refresh materialized views executor.

This executor wraps the existing refresh_materialized_views() function
from src.db.schema to provide it as a hook action.
"""

import logging
import time
from typing import ClassVar

from src.db.schema import refresh_materialized_views
from src.hooks.executors.base import HookExecutor
from src.hooks.models import HookContext, HookResult

logger = logging.getLogger(__name__)


class RefreshViewsExecutor(HookExecutor):
    """
    Hook executor that refreshes all materialized views.

    Configuration:
        ```yaml
        hooks:
          post_import:
            - type: refresh_views
              schema: public  # optional, defaults to "public"
              on_error: warn
        ```

    This wraps the existing refresh_materialized_views() function,
    which refreshes views in dependency order.
    """

    action_type: ClassVar[str] = "refresh_views"

    def execute(self, action: dict, context: HookContext) -> HookResult:
        """
        Refresh all materialized views in the database.

        Args:
            action: Configuration dict with optional "schema" field
            context: Hook context with database_url

        Returns:
            HookResult indicating success/failure
        """
        start_time = time.time()
        hook_name = action.get("name", self.action_type)
        schema = action.get("schema", "public")

        try:
            logger.info(
                f"Refreshing materialized views in schema '{schema}'",
                extra={
                    "job_id": context.job_id,
                    "project": context.project_name,
                    "schema": schema,
                },
            )

            result = refresh_materialized_views(
                schema=schema,
                database_url=context.database_url,
            )

            duration = time.time() - start_time

            if result.success:
                message = (
                    f"Refreshed {len(result.views_refreshed)} materialized view(s): "
                    f"{', '.join(result.views_refreshed)}"
                )
                logger.info(
                    message,
                    extra={
                        "job_id": context.job_id,
                        "views_refreshed": result.views_refreshed,
                        "duration_seconds": duration,
                    },
                )
                return HookResult(
                    success=True,
                    hook_name=hook_name,
                    hook_type=self.action_type,
                    message=message,
                    duration_seconds=duration,
                )
            else:
                # Partial success or complete failure
                error_msg = "; ".join(result.errors) if result.errors else "Unknown error"
                message = (
                    f"Materialized view refresh completed with errors. "
                    f"Refreshed: {len(result.views_refreshed)}, "
                    f"Failed: {len(result.views_failed)}"
                )
                logger.warning(
                    message,
                    extra={
                        "job_id": context.job_id,
                        "views_refreshed": result.views_refreshed,
                        "views_failed": result.views_failed,
                        "errors": result.errors,
                        "duration_seconds": duration,
                    },
                )
                return HookResult(
                    success=len(result.views_failed) == 0,
                    hook_name=hook_name,
                    hook_type=self.action_type,
                    error=error_msg if result.views_failed else None,
                    message=message,
                    duration_seconds=duration,
                )

        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Failed to refresh materialized views: {e}"
            logger.error(
                error_msg,
                extra={
                    "job_id": context.job_id,
                    "project": context.project_name,
                },
                exc_info=True,
            )
            return HookResult(
                success=False,
                hook_name=hook_name,
                hook_type=self.action_type,
                error=error_msg,
                duration_seconds=duration,
            )

    def should_run(self, action: dict, context: HookContext) -> bool:
        """
        Check if refresh should run.

        Always runs if enabled - relies on refresh_materialized_views()
        to handle the case of no views gracefully.
        """
        return action.get("enabled", True)
