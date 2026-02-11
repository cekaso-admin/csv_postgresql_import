"""
Safety-net tests for import_csv() in src/db/importer.py.

These tests protect against regressions during refactoring. They mock all
external dependencies (database, filesystem, pandas) and verify the
function's control flow: validation, chunked streaming, upsert, cleanup.
"""

import pytest
from unittest.mock import MagicMock, patch, call

import pandas as pd

from src.db.importer import (
    ImportError as ImporterError,
    ImportResult,
    import_csv,
)

# Common module path prefix for patching
_MOD = "src.db.importer"


class TestImportCsvFileNotFound:
    """Verify import_csv raises ImportError for nonexistent files."""

    def test_raises_import_error_for_missing_file(self, tmp_path):
        """A path that does not exist must raise ImportError immediately."""
        nonexistent = str(tmp_path / "does_not_exist.csv")

        with pytest.raises(ImporterError, match="CSV file not found"):
            import_csv(
                file_path=nonexistent,
                table_name="irrelevant",
                primary_key="id",
                database_url="postgresql://localhost/test",
            )

    def test_no_database_calls_on_missing_file(self, tmp_path):
        """No schema or connection work should happen when the file is missing."""
        nonexistent = str(tmp_path / "missing.csv")

        with patch(f"{_MOD}.table_exists") as mock_te, \
             patch(f"{_MOD}.create_staging_table") as mock_cs:
            with pytest.raises(ImporterError):
                import_csv(
                    file_path=nonexistent,
                    table_name="t",
                    primary_key="id",
                    database_url="postgresql://localhost/test",
                )

            mock_te.assert_not_called()
            mock_cs.assert_not_called()


class TestImportCsvNoPrimaryKey:
    """Verify import_csv raises ValueError when primary_key is empty."""

    def test_empty_string_primary_key(self, tmp_path):
        """An empty-string primary_key must raise ValueError."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n")

        with pytest.raises(ValueError, match="primary_key is required"):
            import_csv(
                file_path=str(csv_file),
                table_name="t",
                primary_key="",
                database_url="postgresql://localhost/test",
            )

    def test_empty_list_primary_key(self, tmp_path):
        """An empty-list primary_key must raise ValueError."""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n")

        with pytest.raises(ValueError, match="primary_key is required"):
            import_csv(
                file_path=str(csv_file),
                table_name="t",
                primary_key=[],
                database_url="postgresql://localhost/test",
            )


class TestImportCsvSuccess:
    """Verify the happy-path: chunks are streamed, upsert runs, result is correct."""

    @pytest.fixture()
    def csv_file(self, tmp_path):
        """Create a minimal CSV file on disk so Path.exists() passes."""
        f = tmp_path / "data.csv"
        f.write_text("id,name\n1,Alice\n2,Bob\n")
        return str(f)

    @pytest.fixture()
    def mock_deps(self, csv_file):
        """Patch every external dependency of import_csv and yield the mocks."""
        chunk_df = pd.DataFrame({"id": ["1", "2"], "name": ["Alice", "Bob"]})

        with patch(f"{_MOD}._get_file_size_mb", return_value=0.01), \
             patch(f"{_MOD}._get_csv_columns", return_value=["id", "name"]), \
             patch(f"{_MOD}.table_exists", return_value=True), \
             patch(f"{_MOD}.add_columns_to_table", return_value=[]), \
             patch(f"{_MOD}.get_table_columns", return_value=["id", "name"]), \
             patch(f"{_MOD}.create_staging_table", return_value="stg_data_abc123") as m_create_stg, \
             patch(f"{_MOD}.drop_staging_table") as m_drop_stg, \
             patch(f"{_MOD}.pd.read_csv", return_value=[chunk_df]) as m_read_csv, \
             patch(f"{_MOD}._copy_chunk_to_staging", return_value=2) as m_copy_chunk, \
             patch(f"{_MOD}._upsert_from_staging", return_value=(1, 1)) as m_upsert, \
             patch(f"{_MOD}._get_conn_manager") as m_conn_mgr:

            # Wire up the context-manager chain: with conn as conn -> conn.cursor() -> cur
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
            m_conn_mgr.return_value.__enter__ = MagicMock(return_value=mock_conn)
            m_conn_mgr.return_value.__exit__ = MagicMock(return_value=False)

            yield {
                "create_staging": m_create_stg,
                "drop_staging": m_drop_stg,
                "read_csv": m_read_csv,
                "copy_chunk": m_copy_chunk,
                "upsert": m_upsert,
                "conn_mgr": m_conn_mgr,
                "conn": mock_conn,
                "cursor": mock_cursor,
                "chunk_df": chunk_df,
            }

    def test_returns_import_result(self, csv_file, mock_deps):
        """import_csv must return an ImportResult with correct counts."""
        result = import_csv(
            file_path=csv_file,
            table_name="data",
            primary_key="id",
            database_url="postgresql://localhost/test",
        )

        assert isinstance(result, ImportResult)
        assert result.inserted == 1
        assert result.updated == 1
        assert result.skipped == 0  # 2 total - 1 inserted - 1 updated
        assert result.errors == []
        assert result.success is True
        assert result.file_path == csv_file
        assert result.table_name == "data"

    def test_copy_chunk_called_with_staging_table(self, csv_file, mock_deps):
        """Each chunk must be copied to the staging table."""
        import_csv(
            file_path=csv_file,
            table_name="data",
            primary_key="id",
            database_url="postgresql://localhost/test",
        )

        mock_deps["copy_chunk"].assert_called_once()
        args = mock_deps["copy_chunk"].call_args
        # Positional: cur, staging_table, columns, chunk, schema
        assert args[0][1] == "stg_data_abc123"
        assert args[0][2] == ["id", "name"]

    def test_upsert_called_after_chunks(self, csv_file, mock_deps):
        """_upsert_from_staging must be called once with correct args."""
        import_csv(
            file_path=csv_file,
            table_name="data",
            primary_key="id",
            database_url="postgresql://localhost/test",
        )

        mock_deps["upsert"].assert_called_once()
        args = mock_deps["upsert"].call_args
        # Positional: cur, target_table, staging_table, columns, pk_list, schema
        assert args[0][1] == "data"
        assert args[0][2] == "stg_data_abc123"
        assert args[0][3] == ["id", "name"]
        assert args[0][4] == ["id"]

    def test_staging_table_dropped_after_success(self, csv_file, mock_deps):
        """Staging table must be cleaned up after a successful import."""
        import_csv(
            file_path=csv_file,
            table_name="data",
            primary_key="id",
            database_url="postgresql://localhost/test",
        )

        mock_deps["drop_staging"].assert_called_once_with(
            "stg_data_abc123", "public", "postgresql://localhost/test"
        )

    def test_conn_commit_called_twice(self, csv_file, mock_deps):
        """Connection must be committed after staging copy and after upsert."""
        import_csv(
            file_path=csv_file,
            table_name="data",
            primary_key="id",
            database_url="postgresql://localhost/test",
        )

        assert mock_deps["conn"].commit.call_count == 2

    def test_multiple_chunks(self, csv_file):
        """Multiple chunks should each trigger a _copy_chunk_to_staging call."""
        chunk1 = pd.DataFrame({"id": ["1"], "name": ["Alice"]})
        chunk2 = pd.DataFrame({"id": ["2"], "name": ["Bob"]})

        with patch(f"{_MOD}._get_file_size_mb", return_value=0.01), \
             patch(f"{_MOD}._get_csv_columns", return_value=["id", "name"]), \
             patch(f"{_MOD}.table_exists", return_value=True), \
             patch(f"{_MOD}.add_columns_to_table", return_value=[]), \
             patch(f"{_MOD}.get_table_columns", return_value=["id", "name"]), \
             patch(f"{_MOD}.create_staging_table", return_value="stg_t_1"), \
             patch(f"{_MOD}.drop_staging_table"), \
             patch(f"{_MOD}.pd.read_csv", return_value=[chunk1, chunk2]), \
             patch(f"{_MOD}._copy_chunk_to_staging", return_value=1) as m_copy, \
             patch(f"{_MOD}._upsert_from_staging", return_value=(2, 0)), \
             patch(f"{_MOD}._get_conn_manager") as m_conn_mgr:

            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
            m_conn_mgr.return_value.__enter__ = MagicMock(return_value=mock_conn)
            m_conn_mgr.return_value.__exit__ = MagicMock(return_value=False)

            result = import_csv(
                file_path=csv_file,
                table_name="t",
                primary_key="id",
                database_url="postgresql://localhost/test",
            )

            assert m_copy.call_count == 2
            assert result.inserted == 2
            assert result.updated == 0


class TestImportCsvStagingCleanupOnError:
    """Verify staging table is always dropped, even when errors occur."""

    @pytest.fixture()
    def csv_file(self, tmp_path):
        """Create a minimal CSV so validation passes."""
        f = tmp_path / "data.csv"
        f.write_text("id,name\n1,Alice\n")
        return str(f)

    def test_staging_dropped_when_copy_chunk_raises(self, csv_file):
        """If _copy_chunk_to_staging throws, staging table must still be dropped."""
        chunk_df = pd.DataFrame({"id": ["1"], "name": ["Alice"]})

        with patch(f"{_MOD}._get_file_size_mb", return_value=0.01), \
             patch(f"{_MOD}._get_csv_columns", return_value=["id", "name"]), \
             patch(f"{_MOD}.table_exists", return_value=True), \
             patch(f"{_MOD}.add_columns_to_table", return_value=[]), \
             patch(f"{_MOD}.get_table_columns", return_value=["id", "name"]), \
             patch(f"{_MOD}.create_staging_table", return_value="stg_err_1") as m_create, \
             patch(f"{_MOD}.drop_staging_table") as m_drop, \
             patch(f"{_MOD}.pd.read_csv", return_value=[chunk_df]), \
             patch(f"{_MOD}._copy_chunk_to_staging", side_effect=RuntimeError("DB error")), \
             patch(f"{_MOD}._upsert_from_staging") as m_upsert, \
             patch(f"{_MOD}._get_conn_manager") as m_conn_mgr:

            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
            m_conn_mgr.return_value.__enter__ = MagicMock(return_value=mock_conn)
            m_conn_mgr.return_value.__exit__ = MagicMock(return_value=False)

            result = import_csv(
                file_path=csv_file,
                table_name="t",
                primary_key="id",
                database_url="postgresql://localhost/test",
            )

            # Staging must be cleaned up despite the error
            m_drop.assert_called_once_with("stg_err_1", "public", "postgresql://localhost/test")
            # Upsert should NOT have been reached
            m_upsert.assert_not_called()
            # Result should carry the error
            assert result.has_errors
            assert "DB error" in result.errors[0]

    def test_staging_dropped_when_upsert_raises(self, csv_file):
        """If _upsert_from_staging throws, staging table must still be dropped."""
        chunk_df = pd.DataFrame({"id": ["1"], "name": ["Alice"]})

        with patch(f"{_MOD}._get_file_size_mb", return_value=0.01), \
             patch(f"{_MOD}._get_csv_columns", return_value=["id", "name"]), \
             patch(f"{_MOD}.table_exists", return_value=True), \
             patch(f"{_MOD}.add_columns_to_table", return_value=[]), \
             patch(f"{_MOD}.get_table_columns", return_value=["id", "name"]), \
             patch(f"{_MOD}.create_staging_table", return_value="stg_err_2"), \
             patch(f"{_MOD}.drop_staging_table") as m_drop, \
             patch(f"{_MOD}.pd.read_csv", return_value=[chunk_df]), \
             patch(f"{_MOD}._copy_chunk_to_staging", return_value=1), \
             patch(f"{_MOD}._upsert_from_staging", side_effect=RuntimeError("Upsert failed")), \
             patch(f"{_MOD}._get_conn_manager") as m_conn_mgr:

            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
            mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
            m_conn_mgr.return_value.__enter__ = MagicMock(return_value=mock_conn)
            m_conn_mgr.return_value.__exit__ = MagicMock(return_value=False)

            result = import_csv(
                file_path=csv_file,
                table_name="t",
                primary_key="id",
                database_url="postgresql://localhost/test",
            )

            m_drop.assert_called_once_with("stg_err_2", "public", "postgresql://localhost/test")
            assert result.has_errors
            assert "Upsert failed" in result.errors[0]

    def test_staging_not_dropped_when_never_created(self, csv_file):
        """If staging table was never created, drop should not be called."""
        with patch(f"{_MOD}._get_file_size_mb", return_value=0.01), \
             patch(f"{_MOD}._get_csv_columns", return_value=["id", "name"]), \
             patch(f"{_MOD}.table_exists", return_value=True), \
             patch(f"{_MOD}.add_columns_to_table", return_value=[]), \
             patch(f"{_MOD}.get_table_columns", return_value=["id", "name"]), \
             patch(f"{_MOD}.create_staging_table", side_effect=RuntimeError("Cannot create")), \
             patch(f"{_MOD}.drop_staging_table") as m_drop:

            result = import_csv(
                file_path=csv_file,
                table_name="t",
                primary_key="id",
                database_url="postgresql://localhost/test",
            )

            # staging_table is None because create_staging_table raised before assignment
            m_drop.assert_not_called()
            assert result.has_errors
