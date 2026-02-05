"""Tests for hook models."""

import pytest

from src.hooks.models import (
    HookAction,
    HookConfig,
    HookContext,
    HookExecutionResult,
    HookPoint,
    HookResult,
    OnErrorBehavior,
)


class TestHookPoint:
    """Tests for HookPoint enum."""

    def test_all_hook_points_defined(self):
        """Verify all expected hook points exist."""
        assert HookPoint.PRE_IMPORT == "pre_import"
        assert HookPoint.POST_FILE_PREPARE == "post_file_prepare"
        assert HookPoint.PRE_FILE_IMPORT == "pre_file_import"
        assert HookPoint.POST_FILE_IMPORT == "post_file_import"
        assert HookPoint.POST_IMPORT == "post_import"

    def test_hook_point_is_string_enum(self):
        """HookPoint values should be usable as strings."""
        assert f"Executing {HookPoint.POST_IMPORT.value}" == "Executing post_import"


class TestOnErrorBehavior:
    """Tests for OnErrorBehavior enum."""

    def test_all_behaviors_defined(self):
        """Verify all expected behaviors exist."""
        assert OnErrorBehavior.FAIL == "fail"
        assert OnErrorBehavior.WARN == "warn"
        assert OnErrorBehavior.IGNORE == "ignore"


class TestHookContext:
    """Tests for HookContext dataclass."""

    def test_minimal_context(self):
        """Test creating context with required fields only."""
        ctx = HookContext(
            job_id="job-123",
            project_name="test_project",
            database_url="postgresql://localhost/test",
        )

        assert ctx.job_id == "job-123"
        assert ctx.project_name == "test_project"
        assert ctx.database_url == "postgresql://localhost/test"
        assert ctx.temp_dir is None
        assert ctx.file_paths == []
        assert ctx.files_processed == 0
        assert ctx.files_failed == 0
        assert ctx.total_inserted == 0
        assert ctx.total_updated == 0
        assert ctx.status == "running"

    def test_full_context(self):
        """Test creating context with all fields."""
        ctx = HookContext(
            job_id="job-456",
            project_name="full_project",
            database_url="postgresql://localhost/test",
            temp_dir="/tmp/job-456",
            file_paths=["/tmp/a.csv", "/tmp/b.csv"],
            files_processed=5,
            files_failed=1,
            total_inserted=100,
            total_updated=50,
            status="completed",
        )

        assert ctx.temp_dir == "/tmp/job-456"
        assert len(ctx.file_paths) == 2
        assert ctx.files_processed == 5
        assert ctx.status == "completed"

    def test_file_paths_is_mutable(self):
        """Verify file_paths can be modified (for transforms)."""
        ctx = HookContext(
            job_id="job-123",
            project_name="test",
            database_url="postgresql://localhost/test",
            file_paths=["/tmp/data.dbf"],
        )

        # Simulate transformation
        ctx.file_paths[0] = "/tmp/data.csv"
        assert ctx.file_paths[0] == "/tmp/data.csv"

        # Add more files
        ctx.file_paths.append("/tmp/extra.csv")
        assert len(ctx.file_paths) == 2


class TestHookResult:
    """Tests for HookResult dataclass."""

    def test_success_result(self):
        """Test creating a successful result."""
        result = HookResult(
            success=True,
            hook_name="my_hook",
            hook_type="my_action",
            message="Completed successfully",
            duration_seconds=1.5,
        )

        assert result.success is True
        assert result.hook_name == "my_hook"
        assert result.hook_type == "my_action"
        assert result.error is None
        assert result.message == "Completed successfully"
        assert result.duration_seconds == 1.5
        assert result.transformed_files == {}

    def test_failure_result(self):
        """Test creating a failure result."""
        result = HookResult(
            success=False,
            hook_name="failing_hook",
            hook_type="failing_action",
            error="Something went wrong",
        )

        assert result.success is False
        assert result.error == "Something went wrong"
        assert result.duration_seconds == 0.0

    def test_result_with_transformations(self):
        """Test result with file transformations."""
        result = HookResult(
            success=True,
            hook_name="transform_hook",
            hook_type="dbf_to_csv",
            transformed_files={
                "/tmp/data.dbf": "/tmp/data.csv",
                "/tmp/other.dbf": "/tmp/other.csv",
            },
        )

        assert len(result.transformed_files) == 2
        assert result.transformed_files["/tmp/data.dbf"] == "/tmp/data.csv"


class TestHookExecutionResult:
    """Tests for HookExecutionResult dataclass."""

    def test_empty_result(self):
        """Test empty execution result."""
        result = HookExecutionResult()

        assert result.results == []
        assert result.should_abort is False
        assert result.errors == []
        assert result.all_successful is True
        assert result.total_duration_seconds == 0.0

    def test_all_successful(self):
        """Test result with all successful hooks."""
        result = HookExecutionResult(
            results=[
                HookResult(success=True, hook_name="a", hook_type="type_a", duration_seconds=1.0),
                HookResult(success=True, hook_name="b", hook_type="type_b", duration_seconds=2.0),
            ]
        )

        assert result.all_successful is True
        assert result.total_duration_seconds == 3.0

    def test_partial_failure(self):
        """Test result with some failed hooks."""
        result = HookExecutionResult(
            results=[
                HookResult(success=True, hook_name="a", hook_type="type_a"),
                HookResult(success=False, hook_name="b", hook_type="type_b", error="Failed"),
            ],
            errors=["b: Failed"],
        )

        assert result.all_successful is False
        assert len(result.errors) == 1

    def test_abort_result(self):
        """Test result that triggers abort."""
        result = HookExecutionResult(
            should_abort=True,
            errors=["Critical failure"],
        )

        assert result.should_abort is True


class TestHookAction:
    """Tests for HookAction Pydantic model."""

    def test_minimal_action(self):
        """Test action with required fields only."""
        action = HookAction(type="refresh_views")

        assert action.type == "refresh_views"
        assert action.name is None
        assert action.enabled is True
        assert action.on_error == OnErrorBehavior.WARN

    def test_full_action(self):
        """Test action with all standard fields."""
        action = HookAction(
            type="run_sql",
            name="Post-import SQL",
            enabled=True,
            on_error=OnErrorBehavior.FAIL,
        )

        assert action.type == "run_sql"
        assert action.name == "Post-import SQL"
        assert action.on_error == OnErrorBehavior.FAIL

    def test_action_with_extra_fields(self):
        """Test action with action-specific extra fields."""
        action = HookAction(
            type="dbf_to_csv",
            input_pattern="*.dbf",
            encoding="cp1252",
            delete_original=True,
        )

        data = action.model_dump()
        assert data["input_pattern"] == "*.dbf"
        assert data["encoding"] == "cp1252"
        assert data["delete_original"] is True

    def test_get_name_with_name(self):
        """Test get_name when name is set."""
        action = HookAction(type="my_type", name="My Name")
        assert action.get_name() == "My Name"

    def test_get_name_without_name(self):
        """Test get_name when name is not set."""
        action = HookAction(type="my_type")
        assert action.get_name() == "my_type"


class TestHookConfig:
    """Tests for HookConfig Pydantic model."""

    def test_empty_config(self):
        """Test empty hook configuration."""
        config = HookConfig()

        assert config.pre_import == []
        assert config.post_file_prepare == []
        assert config.pre_file_import == []
        assert config.post_file_import == []
        assert config.post_import == []
        assert config.has_hooks() is False

    def test_config_with_hooks(self):
        """Test configuration with hooks at various points."""
        config = HookConfig(
            pre_import=[HookAction(type="setup")],
            post_import=[
                HookAction(type="refresh_views"),
                HookAction(type="notify"),
            ],
        )

        assert len(config.pre_import) == 1
        assert len(config.post_import) == 2
        assert config.has_hooks() is True

    def test_get_hooks_for_point(self):
        """Test retrieving hooks for specific points."""
        config = HookConfig(
            post_import=[HookAction(type="refresh_views")],
        )

        assert len(config.get_hooks_for_point(HookPoint.POST_IMPORT)) == 1
        assert len(config.get_hooks_for_point(HookPoint.PRE_IMPORT)) == 0

    def test_get_hooks_for_all_points(self):
        """Test retrieving hooks for all defined points."""
        config = HookConfig(
            pre_import=[HookAction(type="a")],
            post_file_prepare=[HookAction(type="b")],
            pre_file_import=[HookAction(type="c")],
            post_file_import=[HookAction(type="d")],
            post_import=[HookAction(type="e")],
        )

        for hook_point in HookPoint:
            hooks = config.get_hooks_for_point(hook_point)
            assert len(hooks) == 1
