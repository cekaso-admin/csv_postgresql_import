"""Tests for hook engine."""

import pytest
from unittest.mock import MagicMock, patch

from src.hooks.engine import (
    HookEngine,
    get_executor,
    get_registered_action_types,
)
from src.hooks.executors.base import HookExecutor
from src.hooks.executors.refresh_views import RefreshViewsExecutor
from src.hooks.models import (
    HookAction,
    HookConfig,
    HookContext,
    HookPoint,
    HookResult,
    OnErrorBehavior,
)


class TestExecutorRegistry:
    """Tests for executor registration and discovery."""

    def test_refresh_views_registered(self):
        """Verify refresh_views executor is registered."""
        action_types = get_registered_action_types()
        assert "refresh_views" in action_types

    def test_get_executor_known_type(self):
        """Test getting a known executor class."""
        executor_cls = get_executor("refresh_views")
        assert executor_cls is not None
        assert executor_cls is RefreshViewsExecutor

    def test_get_executor_unknown_type(self):
        """Test getting an unknown executor returns None."""
        executor_cls = get_executor("nonexistent_action")
        assert executor_cls is None


class TestHookEngine:
    """Tests for HookEngine class."""

    def test_init_with_config(self):
        """Test engine initialization with config."""
        config = HookConfig(
            post_import=[HookAction(type="refresh_views")]
        )
        engine = HookEngine(config)

        assert engine.config is config

    def test_init_without_config(self):
        """Test engine initialization without config."""
        engine = HookEngine()

        assert engine.config is not None
        assert engine.config.has_hooks() is False

    def test_validate_config_valid(self):
        """Test validation of valid config."""
        config = HookConfig(
            post_import=[HookAction(type="refresh_views")]
        )
        engine = HookEngine(config)

        errors = engine.validate_config()
        assert errors == []

    def test_validate_config_unknown_type(self):
        """Test validation catches unknown action types."""
        config = HookConfig(
            post_import=[HookAction(type="nonexistent_type")]
        )
        engine = HookEngine(config)

        errors = engine.validate_config()
        assert len(errors) == 1
        assert "unknown action type" in errors[0]

    def test_validate_config_missing_type(self):
        """Test validation catches missing type field."""
        # This shouldn't happen with Pydantic, but test the validation logic
        config = HookConfig()
        config.post_import = [HookAction(type="")]

        engine = HookEngine(config)
        errors = engine.validate_config()
        # Empty string is technically a valid type value but unknown
        assert len(errors) >= 1

    def test_execute_hooks_empty_config(self):
        """Test executing hooks with no hooks configured."""
        engine = HookEngine(HookConfig())
        context = HookContext(
            job_id="test",
            project_name="test",
            database_url="postgresql://localhost/test",
        )

        result = engine.execute_hooks(HookPoint.POST_IMPORT, context)

        assert result.results == []
        assert result.should_abort is False

    def test_execute_hooks_disabled_hook(self):
        """Test that disabled hooks are skipped."""
        config = HookConfig(
            post_import=[HookAction(type="refresh_views", enabled=False)]
        )
        engine = HookEngine(config)
        context = HookContext(
            job_id="test",
            project_name="test",
            database_url="postgresql://localhost/test",
        )

        with patch.object(RefreshViewsExecutor, 'execute') as mock_execute:
            result = engine.execute_hooks(HookPoint.POST_IMPORT, context)
            mock_execute.assert_not_called()

        assert len(result.results) == 0

    def test_execute_hooks_success(self):
        """Test successful hook execution."""
        config = HookConfig(
            post_import=[HookAction(type="refresh_views")]
        )
        engine = HookEngine(config)
        context = HookContext(
            job_id="test",
            project_name="test",
            database_url="postgresql://localhost/test",
        )

        with patch.object(
            RefreshViewsExecutor, 'execute',
            return_value=HookResult(
                success=True,
                hook_name="refresh_views",
                hook_type="refresh_views",
                message="Refreshed 2 views",
            )
        ) as mock_execute:
            result = engine.execute_hooks(HookPoint.POST_IMPORT, context)

        assert len(result.results) == 1
        assert result.results[0].success is True
        assert result.should_abort is False

    def test_execute_hooks_failure_warn(self):
        """Test hook failure with on_error=warn continues execution."""
        config = HookConfig(
            post_import=[
                HookAction(type="refresh_views", on_error=OnErrorBehavior.WARN),
            ]
        )
        engine = HookEngine(config)
        context = HookContext(
            job_id="test",
            project_name="test",
            database_url="postgresql://localhost/test",
        )

        with patch.object(
            RefreshViewsExecutor, 'execute',
            return_value=HookResult(
                success=False,
                hook_name="refresh_views",
                hook_type="refresh_views",
                error="View refresh failed",
            )
        ):
            result = engine.execute_hooks(HookPoint.POST_IMPORT, context)

        assert len(result.results) == 1
        assert result.results[0].success is False
        assert result.should_abort is False
        assert len(result.errors) == 1

    def test_execute_hooks_failure_fail(self):
        """Test hook failure with on_error=fail aborts execution."""
        config = HookConfig(
            post_import=[
                HookAction(type="refresh_views", on_error=OnErrorBehavior.FAIL),
            ]
        )
        engine = HookEngine(config)
        context = HookContext(
            job_id="test",
            project_name="test",
            database_url="postgresql://localhost/test",
        )

        with patch.object(
            RefreshViewsExecutor, 'execute',
            return_value=HookResult(
                success=False,
                hook_name="refresh_views",
                hook_type="refresh_views",
                error="Critical failure",
            )
        ):
            result = engine.execute_hooks(HookPoint.POST_IMPORT, context)

        assert result.should_abort is True
        assert len(result.errors) == 1

    def test_execute_hooks_failure_ignore(self):
        """Test hook failure with on_error=ignore silently continues."""
        config = HookConfig(
            post_import=[
                HookAction(type="refresh_views", on_error=OnErrorBehavior.IGNORE),
            ]
        )
        engine = HookEngine(config)
        context = HookContext(
            job_id="test",
            project_name="test",
            database_url="postgresql://localhost/test",
        )

        with patch.object(
            RefreshViewsExecutor, 'execute',
            return_value=HookResult(
                success=False,
                hook_name="refresh_views",
                hook_type="refresh_views",
                error="Ignored failure",
            )
        ):
            result = engine.execute_hooks(HookPoint.POST_IMPORT, context)

        assert result.should_abort is False
        # Errors with ignore are not added to result.errors
        assert len(result.errors) == 0

    def test_execute_hooks_file_transformation(self):
        """Test that file transformations are applied to context."""
        config = HookConfig(
            post_file_prepare=[HookAction(type="refresh_views")]  # Using as mock
        )
        engine = HookEngine(config)
        context = HookContext(
            job_id="test",
            project_name="test",
            database_url="postgresql://localhost/test",
            file_paths=["/tmp/data.dbf", "/tmp/other.csv"],
        )

        with patch.object(
            RefreshViewsExecutor, 'execute',
            return_value=HookResult(
                success=True,
                hook_name="transform",
                hook_type="refresh_views",
                transformed_files={"/tmp/data.dbf": "/tmp/data.csv"},
            )
        ):
            result = engine.execute_hooks(HookPoint.POST_FILE_PREPARE, context)

        # Verify file_paths was updated
        assert "/tmp/data.csv" in context.file_paths
        assert "/tmp/data.dbf" not in context.file_paths
        assert "/tmp/other.csv" in context.file_paths

    def test_execute_hooks_unknown_action_type(self):
        """Test handling of unknown action type during execution."""
        config = HookConfig(
            post_import=[HookAction(type="nonexistent")]
        )
        engine = HookEngine(config)
        context = HookContext(
            job_id="test",
            project_name="test",
            database_url="postgresql://localhost/test",
        )

        result = engine.execute_hooks(HookPoint.POST_IMPORT, context)

        assert len(result.errors) == 1
        assert "Unknown action type" in result.errors[0]

    def test_execute_hooks_multiple_hooks_order(self):
        """Test that hooks execute in order."""
        execution_order = []

        class MockExecutor1(HookExecutor):
            action_type = "mock1"

            def execute(self, action, context):
                execution_order.append("mock1")
                return HookResult(success=True, hook_name="mock1", hook_type="mock1")

        class MockExecutor2(HookExecutor):
            action_type = "mock2"

            def execute(self, action, context):
                execution_order.append("mock2")
                return HookResult(success=True, hook_name="mock2", hook_type="mock2")

        # Register mock executors
        with patch(
            'src.hooks.engine._executors',
            {"mock1": MockExecutor1, "mock2": MockExecutor2}
        ):
            config = HookConfig(
                post_import=[
                    HookAction(type="mock1"),
                    HookAction(type="mock2"),
                ]
            )
            engine = HookEngine(config)
            context = HookContext(
                job_id="test",
                project_name="test",
                database_url="postgresql://localhost/test",
            )

            engine.execute_hooks(HookPoint.POST_IMPORT, context)

        assert execution_order == ["mock1", "mock2"]

    def test_execute_hooks_exception_handling(self):
        """Test that exceptions in executors are handled gracefully."""
        config = HookConfig(
            post_import=[HookAction(type="refresh_views", on_error=OnErrorBehavior.WARN)]
        )
        engine = HookEngine(config)
        context = HookContext(
            job_id="test",
            project_name="test",
            database_url="postgresql://localhost/test",
        )

        with patch.object(
            RefreshViewsExecutor, 'execute',
            side_effect=RuntimeError("Unexpected error")
        ):
            result = engine.execute_hooks(HookPoint.POST_IMPORT, context)

        # Exception should be caught and converted to failure result
        assert len(result.results) == 1
        assert result.results[0].success is False
        assert "Unexpected error" in result.results[0].error
        assert result.should_abort is False  # on_error=warn
