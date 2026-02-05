"""Tests for DbfToCsvExecutor."""

import csv
import os
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from src.hooks.models import HookContext


class TestDbfToCsvExecutor:
    """Tests for DbfToCsvExecutor class."""

    def test_action_type(self):
        """Verify action_type is set correctly."""
        from src.hooks.executors.dbf_to_csv import DbfToCsvExecutor

        assert DbfToCsvExecutor.action_type == "dbf_to_csv"

    def test_execute_no_matching_files(self):
        """Test when no files match the pattern."""
        from src.hooks.executors.dbf_to_csv import DbfToCsvExecutor

        executor = DbfToCsvExecutor()
        context = HookContext(
            job_id="test-job",
            project_name="test",
            database_url="postgresql://localhost/test",
            file_paths=["/path/to/file.csv", "/path/to/file.txt"],
        )
        action = {"type": "dbf_to_csv", "input_pattern": "*.dbf"}

        with patch(
            "src.hooks.executors.dbf_to_csv._check_dbf_available",
            return_value=True,
        ):
            result = executor.execute(action, context)

        assert result.success is True
        assert "No files matching" in result.message
        assert result.transformed_files == {}

    def test_execute_dbf_not_installed(self):
        """Test error handling when dbf is not installed."""
        from src.hooks.executors.dbf_to_csv import DbfToCsvExecutor

        executor = DbfToCsvExecutor()
        context = HookContext(
            job_id="test-job",
            project_name="test",
            database_url="postgresql://localhost/test",
            file_paths=["/path/to/test.dbf"],
        )
        action = {"type": "dbf_to_csv"}

        with patch(
            "src.hooks.executors.dbf_to_csv._check_dbf_available",
            return_value=False,
        ):
            result = executor.execute(action, context)

        assert result.success is False
        assert "dbf package not installed" in result.error
        assert "pip install dbf" in result.error

    def test_execute_success_with_mock(self, tmp_path: Path):
        """Test successful DBF to CSV conversion with mocked dbf library."""
        from src.hooks.executors.dbf_to_csv import DbfToCsvExecutor

        executor = DbfToCsvExecutor()

        dbf_path = str(tmp_path / "test_data.dbf")
        csv_path = str(tmp_path / "test_data.csv")
        Path(dbf_path).touch()

        context = HookContext(
            job_id="test-job",
            project_name="test",
            database_url="postgresql://localhost/test",
            file_paths=[dbf_path],
        )
        action = {"type": "dbf_to_csv", "encoding": "cp850"}

        # Create mock table that behaves like dbf.Table
        mock_table = MagicMock()
        mock_table.field_names = ["ID", "NAME", "VALUE"]
        mock_table.codepage = MagicMock()
        mock_table.codepage.name = "cp850"

        # Create mock records
        mock_record1 = MagicMock()
        mock_record1.__getitem__ = lambda self, key: {"ID": 1, "NAME": "Alice", "VALUE": 100.5}[key]
        mock_record2 = MagicMock()
        mock_record2.__getitem__ = lambda self, key: {"ID": 2, "NAME": "Bob", "VALUE": 200.0}[key]

        mock_table.__iter__ = lambda self: iter([mock_record1, mock_record2])
        mock_table.open = MagicMock()
        mock_table.close = MagicMock()

        mock_dbf_module = MagicMock()
        mock_dbf_module.Table = MagicMock(return_value=mock_table)

        with patch(
            "src.hooks.executors.dbf_to_csv._check_dbf_available",
            return_value=True,
        ):
            with patch.dict("sys.modules", {"dbf": mock_dbf_module}):
                result = executor.execute(action, context)

        assert result.success is True
        assert result.hook_type == "dbf_to_csv"
        assert "1" in result.message
        assert result.duration_seconds > 0
        assert dbf_path in result.transformed_files
        assert result.transformed_files[dbf_path] == csv_path

        # Verify CSV was created
        assert os.path.exists(csv_path)
        with open(csv_path, "r") as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert rows[0] == ["ID", "NAME", "VALUE"]
            assert rows[1] == ["1", "Alice", "100.5"]
            assert rows[2] == ["2", "Bob", "200.0"]

    def test_execute_delete_original(self, tmp_path: Path):
        """Test delete_original option removes DBF files after conversion."""
        from src.hooks.executors.dbf_to_csv import DbfToCsvExecutor

        executor = DbfToCsvExecutor()

        dbf_path = str(tmp_path / "test.dbf")
        Path(dbf_path).touch()

        context = HookContext(
            job_id="test-job",
            project_name="test",
            database_url="postgresql://localhost/test",
            file_paths=[dbf_path],
        )
        action = {"type": "dbf_to_csv", "delete_original": True}

        # Create mock table
        mock_table = MagicMock()
        mock_table.field_names = ["id"]
        mock_table.codepage = MagicMock()
        mock_table.codepage.name = "utf-8"
        mock_table.__iter__ = lambda self: iter([])
        mock_table.open = MagicMock()
        mock_table.close = MagicMock()

        mock_dbf_module = MagicMock()
        mock_dbf_module.Table = MagicMock(return_value=mock_table)

        # Verify DBF file exists before conversion
        assert os.path.exists(dbf_path)

        with patch(
            "src.hooks.executors.dbf_to_csv._check_dbf_available",
            return_value=True,
        ):
            with patch.dict("sys.modules", {"dbf": mock_dbf_module}):
                result = executor.execute(action, context)

        assert result.success is True
        # DBF file should be deleted
        assert not os.path.exists(dbf_path)
        # CSV file should exist
        csv_path = str(tmp_path / "test.csv")
        assert os.path.exists(csv_path)

    def test_execute_conversion_error(self, tmp_path: Path):
        """Test error handling for invalid DBF file."""
        from src.hooks.executors.dbf_to_csv import DbfToCsvExecutor

        executor = DbfToCsvExecutor()

        dbf_path = str(tmp_path / "invalid.dbf")
        Path(dbf_path).write_text("not a valid DBF file")

        context = HookContext(
            job_id="test-job",
            project_name="test",
            database_url="postgresql://localhost/test",
            file_paths=[dbf_path],
        )
        action = {"type": "dbf_to_csv"}

        mock_dbf_module = MagicMock()
        mock_dbf_module.Table = MagicMock(
            side_effect=ValueError("Not a valid DBF file")
        )

        with patch(
            "src.hooks.executors.dbf_to_csv._check_dbf_available",
            return_value=True,
        ):
            with patch.dict("sys.modules", {"dbf": mock_dbf_module}):
                result = executor.execute(action, context)

        assert result.success is False
        assert "Not a valid DBF file" in result.error or "Could not find suitable encoding" in result.error

    def test_execute_with_custom_name(self, tmp_path: Path):
        """Test that custom hook name is used in result."""
        from src.hooks.executors.dbf_to_csv import DbfToCsvExecutor

        executor = DbfToCsvExecutor()

        dbf_path = str(tmp_path / "test.dbf")
        Path(dbf_path).touch()

        context = HookContext(
            job_id="test-job",
            project_name="test",
            database_url="postgresql://localhost/test",
            file_paths=[dbf_path],
        )
        action = {
            "type": "dbf_to_csv",
            "name": "Convert Legacy DBF Files",
        }

        mock_table = MagicMock()
        mock_table.field_names = ["id"]
        mock_table.codepage = MagicMock()
        mock_table.codepage.name = "utf-8"
        mock_table.__iter__ = lambda self: iter([])
        mock_table.open = MagicMock()
        mock_table.close = MagicMock()

        mock_dbf_module = MagicMock()
        mock_dbf_module.Table = MagicMock(return_value=mock_table)

        with patch(
            "src.hooks.executors.dbf_to_csv._check_dbf_available",
            return_value=True,
        ):
            with patch.dict("sys.modules", {"dbf": mock_dbf_module}):
                result = executor.execute(action, context)

        assert result.hook_name == "Convert Legacy DBF Files"

    def test_should_run_enabled(self):
        """Test should_run when enabled is True."""
        from src.hooks.executors.dbf_to_csv import DbfToCsvExecutor

        executor = DbfToCsvExecutor()
        context = HookContext(
            job_id="test",
            project_name="test",
            database_url="postgresql://localhost/test",
            file_paths=["/path/to/file.dbf"],
        )

        assert executor.should_run({"enabled": True}, context) is True
        assert executor.should_run({}, context) is True  # Default enabled

    def test_should_run_disabled(self):
        """Test should_run when enabled is False."""
        from src.hooks.executors.dbf_to_csv import DbfToCsvExecutor

        executor = DbfToCsvExecutor()
        context = HookContext(
            job_id="test",
            project_name="test",
            database_url="postgresql://localhost/test",
            file_paths=["/path/to/file.dbf"],
        )

        assert executor.should_run({"enabled": False}, context) is False

    def test_should_run_no_files(self):
        """Test should_run when no files in context."""
        from src.hooks.executors.dbf_to_csv import DbfToCsvExecutor

        executor = DbfToCsvExecutor()
        context = HookContext(
            job_id="test",
            project_name="test",
            database_url="postgresql://localhost/test",
            file_paths=[],
        )

        assert executor.should_run({}, context) is False

    def test_validate_config_valid(self):
        """Test validate_config with valid configuration."""
        from src.hooks.executors.dbf_to_csv import DbfToCsvExecutor

        executor = DbfToCsvExecutor()

        assert executor.validate_config({}) is None
        assert executor.validate_config({"encoding": "utf-8"}) is None
        assert executor.validate_config({"encoding": "cp1252"}) is None
        assert executor.validate_config({"encoding": "auto"}) is None
        assert executor.validate_config({"delete_original": True}) is None
        assert executor.validate_config({"delete_original": False}) is None
        assert executor.validate_config({
            "encoding": "latin-1",
            "delete_original": True,
            "input_pattern": "*.DBF",
        }) is None

    def test_validate_config_invalid_encoding(self):
        """Test validate_config with invalid encoding."""
        from src.hooks.executors.dbf_to_csv import DbfToCsvExecutor

        executor = DbfToCsvExecutor()

        error = executor.validate_config({"encoding": "not-a-real-encoding"})
        assert error is not None
        assert "Unknown encoding" in error

    def test_validate_config_invalid_delete_original(self):
        """Test validate_config with non-boolean delete_original."""
        from src.hooks.executors.dbf_to_csv import DbfToCsvExecutor

        executor = DbfToCsvExecutor()

        error = executor.validate_config({"delete_original": "yes"})
        assert error is not None
        assert "delete_original must be a boolean" in error

    def test_find_matching_files_case_insensitive(self):
        """Test that pattern matching is case insensitive."""
        from src.hooks.executors.dbf_to_csv import DbfToCsvExecutor

        executor = DbfToCsvExecutor()

        files = [
            "/path/to/DATA.DBF",
            "/path/to/data.dbf",
            "/path/to/Data.Dbf",
            "/path/to/file.csv",
        ]

        matches = executor._find_matching_files(files, "*.dbf")
        assert len(matches) == 3
        assert "/path/to/DATA.DBF" in matches
        assert "/path/to/data.dbf" in matches
        assert "/path/to/Data.Dbf" in matches
        assert "/path/to/file.csv" not in matches


class TestSanitizeColumnName:
    """Tests for sanitize_column_name function."""

    def test_sanitize_spaces(self):
        """Test that spaces are replaced with underscores."""
        from src.hooks.executors.dbf_to_csv import sanitize_column_name

        assert sanitize_column_name("First Name") == "First_Name"
        assert sanitize_column_name("  spaces  ") == "spaces"

    def test_sanitize_special_chars(self):
        """Test that special characters are replaced."""
        from src.hooks.executors.dbf_to_csv import sanitize_column_name

        assert sanitize_column_name("col-name") == "col_name"
        assert sanitize_column_name("col.name") == "col_name"
        assert sanitize_column_name("col/name") == "col_name"

    def test_sanitize_leading_digit(self):
        """Test that leading digits get underscore prefix."""
        from src.hooks.executors.dbf_to_csv import sanitize_column_name

        assert sanitize_column_name("123col") == "_123col"
        assert sanitize_column_name("1") == "_1"

    def test_sanitize_keeps_umlauts(self):
        """Test that German umlauts are preserved."""
        from src.hooks.executors.dbf_to_csv import sanitize_column_name

        assert "ö" in sanitize_column_name("Größe")
        assert "ä" in sanitize_column_name("Größäe")

    def test_sanitize_empty(self):
        """Test empty string returns 'column'."""
        from src.hooks.executors.dbf_to_csv import sanitize_column_name

        assert sanitize_column_name("") == "column"
        assert sanitize_column_name("   ") == "column"


class TestFormatValue:
    """Tests for format_value function."""

    def test_format_none(self):
        """Test None returns empty string."""
        from src.hooks.executors.dbf_to_csv import format_value

        assert format_value(None) == ""

    def test_format_string(self):
        """Test string is stripped."""
        from src.hooks.executors.dbf_to_csv import format_value

        assert format_value("  hello  ") == "hello"
        assert format_value("test") == "test"

    def test_format_number(self):
        """Test numbers are converted to string."""
        from src.hooks.executors.dbf_to_csv import format_value

        assert format_value(123) == "123"
        assert format_value(3.14) == "3.14"

    def test_format_boolean(self):
        """Test booleans are formatted as 'true'/'false'."""
        from src.hooks.executors.dbf_to_csv import format_value

        assert format_value(True) == "true"
        assert format_value(False) == "false"


class TestDbfAvailabilityCheck:
    """Tests for dbf availability checking."""

    def test_check_dbf_available_caches_result(self):
        """Test that availability check result is cached."""
        from src.hooks.executors import dbf_to_csv

        original_value = dbf_to_csv._DBF_AVAILABLE

        try:
            # Set cached value
            dbf_to_csv._DBF_AVAILABLE = True
            result = dbf_to_csv._check_dbf_available()
            assert result is True

            dbf_to_csv._DBF_AVAILABLE = False
            result = dbf_to_csv._check_dbf_available()
            assert result is False
        finally:
            dbf_to_csv._DBF_AVAILABLE = original_value


class TestDbfToCsvIntegration:
    """Integration tests for DBF to CSV conversion (require dbf package)."""

    @pytest.fixture
    def skip_if_no_dbf(self):
        """Skip test if dbf is not installed."""
        try:
            import dbf  # noqa: F401
        except ImportError:
            pytest.skip("dbf not installed")

    def test_real_dbf_import(self, skip_if_no_dbf):
        """Test that dbf can be imported in real environment."""
        import dbf

        assert dbf.Table is not None
