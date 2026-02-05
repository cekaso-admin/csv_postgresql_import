"""Tests for RefreshViewsExecutor."""

import pytest
from unittest.mock import MagicMock, patch

from src.db.schema import RefreshResult
from src.hooks.executors.refresh_views import RefreshViewsExecutor
from src.hooks.models import HookContext, HookResult


class TestRefreshViewsExecutor:
    """Tests for RefreshViewsExecutor class."""

    def test_action_type(self):
        """Verify action_type is set correctly."""
        assert RefreshViewsExecutor.action_type == "refresh_views"

    def test_execute_success(self):
        """Test successful view refresh."""
        executor = RefreshViewsExecutor()
        context = HookContext(
            job_id="test-job",
            project_name="test",
            database_url="postgresql://localhost/test",
        )
        action = {"type": "refresh_views", "schema": "public"}

        with patch(
            'src.hooks.executors.refresh_views.refresh_materialized_views',
            return_value=RefreshResult(
                views_refreshed=["view_a", "view_b"],
                views_failed=[],
                errors=[],
            )
        ) as mock_refresh:
            result = executor.execute(action, context)

        mock_refresh.assert_called_once_with(
            schema="public",
            database_url="postgresql://localhost/test",
        )
        assert result.success is True
        assert result.hook_type == "refresh_views"
        assert "view_a" in result.message
        assert "view_b" in result.message
        assert result.duration_seconds > 0

    def test_execute_custom_schema(self):
        """Test refresh with custom schema."""
        executor = RefreshViewsExecutor()
        context = HookContext(
            job_id="test-job",
            project_name="test",
            database_url="postgresql://localhost/test",
        )
        action = {"type": "refresh_views", "schema": "custom_schema"}

        with patch(
            'src.hooks.executors.refresh_views.refresh_materialized_views',
            return_value=RefreshResult(
                views_refreshed=["view_x"],
                views_failed=[],
                errors=[],
            )
        ) as mock_refresh:
            result = executor.execute(action, context)

        mock_refresh.assert_called_once_with(
            schema="custom_schema",
            database_url="postgresql://localhost/test",
        )
        assert result.success is True

    def test_execute_partial_failure(self):
        """Test refresh with some views failing."""
        executor = RefreshViewsExecutor()
        context = HookContext(
            job_id="test-job",
            project_name="test",
            database_url="postgresql://localhost/test",
        )
        action = {"type": "refresh_views"}

        with patch(
            'src.hooks.executors.refresh_views.refresh_materialized_views',
            return_value=RefreshResult(
                views_refreshed=["view_a"],
                views_failed=["view_b"],
                errors=["Failed to refresh view_b: timeout"],
            )
        ):
            result = executor.execute(action, context)

        assert result.success is False  # Has failed views
        assert "view_b" in result.message or "1" in result.message
        assert result.error is not None
        assert "timeout" in result.error

    def test_execute_complete_failure(self):
        """Test refresh with all views failing."""
        executor = RefreshViewsExecutor()
        context = HookContext(
            job_id="test-job",
            project_name="test",
            database_url="postgresql://localhost/test",
        )
        action = {"type": "refresh_views"}

        with patch(
            'src.hooks.executors.refresh_views.refresh_materialized_views',
            return_value=RefreshResult(
                views_refreshed=[],
                views_failed=["view_a", "view_b"],
                errors=["Error A", "Error B"],
            )
        ):
            result = executor.execute(action, context)

        assert result.success is False
        assert result.error is not None

    def test_execute_no_views(self):
        """Test refresh when no views exist."""
        executor = RefreshViewsExecutor()
        context = HookContext(
            job_id="test-job",
            project_name="test",
            database_url="postgresql://localhost/test",
        )
        action = {"type": "refresh_views"}

        with patch(
            'src.hooks.executors.refresh_views.refresh_materialized_views',
            return_value=RefreshResult(
                views_refreshed=[],
                views_failed=[],
                errors=[],
            )
        ):
            result = executor.execute(action, context)

        # No views refreshed but also no failures = still considered success
        # (the refresh function handled empty views gracefully)
        assert result.success is True

    def test_execute_exception(self):
        """Test handling of unexpected exception."""
        executor = RefreshViewsExecutor()
        context = HookContext(
            job_id="test-job",
            project_name="test",
            database_url="postgresql://localhost/test",
        )
        action = {"type": "refresh_views"}

        with patch(
            'src.hooks.executors.refresh_views.refresh_materialized_views',
            side_effect=RuntimeError("Connection failed")
        ):
            result = executor.execute(action, context)

        assert result.success is False
        assert "Connection failed" in result.error
        assert result.duration_seconds >= 0

    def test_execute_with_custom_name(self):
        """Test that custom hook name is used in result."""
        executor = RefreshViewsExecutor()
        context = HookContext(
            job_id="test-job",
            project_name="test",
            database_url="postgresql://localhost/test",
        )
        action = {"type": "refresh_views", "name": "My Custom Refresh"}

        with patch(
            'src.hooks.executors.refresh_views.refresh_materialized_views',
            return_value=RefreshResult(
                views_refreshed=["view_a"],
                views_failed=[],
                errors=[],
            )
        ):
            result = executor.execute(action, context)

        assert result.hook_name == "My Custom Refresh"

    def test_should_run_enabled(self):
        """Test should_run when enabled is True."""
        executor = RefreshViewsExecutor()
        context = HookContext(
            job_id="test",
            project_name="test",
            database_url="postgresql://localhost/test",
        )

        assert executor.should_run({"enabled": True}, context) is True
        assert executor.should_run({}, context) is True  # Default enabled

    def test_should_run_disabled(self):
        """Test should_run when enabled is False."""
        executor = RefreshViewsExecutor()
        context = HookContext(
            job_id="test",
            project_name="test",
            database_url="postgresql://localhost/test",
        )

        assert executor.should_run({"enabled": False}, context) is False
