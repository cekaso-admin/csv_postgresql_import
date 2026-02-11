"""
Tests for import_dbf() in src/db/importer.py and _import_file() dispatch
in src/api/routes.py.

All tests are mock-based: no real database connections, no real DBF files
(except for file-existence checks using tmp_path), and no real pyogrio calls.
"""

import sys

import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from src.db.importer import (
    ImportError as ImporterError,
    import_dbf,
)

# Common module path prefix for patching
_MOD = "src.db.importer"


def _install_mock_pyogrio(**overrides) -> MagicMock:
    """Insert a mock pyogrio module into sys.modules.

    Returns the mock so callers can configure read_info / read_dataframe.
    The caller is responsible for removing it afterwards (use try/finally
    or a context manager).
    """
    mock_pyogrio = MagicMock()
    for key, value in overrides.items():
        setattr(mock_pyogrio, key, value)
    sys.modules["pyogrio"] = mock_pyogrio
    return mock_pyogrio


# =============================================================================
# Pyogrio not available
# =============================================================================


class TestImportDbfPyogrioNotAvailable:
    """Verify import_dbf raises ImportError when pyogrio is missing."""

    def test_raises_import_error_when_missing(self):
        """When pyogrio is not installed, ImportError must mention 'pyogrio'."""
        with patch(f"{_MOD}._PYOGRIO_AVAILABLE", False), \
             patch(f"{_MOD}._check_pyogrio_available", return_value=False):
            with pytest.raises(ImporterError, match="pyogrio"):
                import_dbf(
                    file_path="/tmp/dummy.dbf",
                    table_name="test_table",
                    primary_key="id",
                    database_url="postgresql://localhost/test",
                )


# =============================================================================
# File not found
# =============================================================================


class TestImportDbfFileNotFound:
    """Verify import_dbf raises ImportError for a nonexistent file."""

    def test_raises_import_error_for_missing_file(self, tmp_path):
        """A path that does not exist must raise ImportError with 'not found'."""
        nonexistent = str(tmp_path / "missing.dbf")

        mock_pyogrio = _install_mock_pyogrio()
        try:
            with patch(f"{_MOD}._check_pyogrio_available", return_value=True), \
                 patch(f"{_MOD}._PYOGRIO_AVAILABLE", True):
                with pytest.raises(ImporterError, match="not found"):
                    import_dbf(
                        file_path=nonexistent,
                        table_name="test_table",
                        primary_key="id",
                        database_url="postgresql://localhost/test",
                    )
        finally:
            sys.modules.pop("pyogrio", None)


# =============================================================================
# No primary key
# =============================================================================


class TestImportDbfNoPrimaryKey:
    """Verify import_dbf raises ValueError when primary_key is empty."""

    def test_empty_primary_key(self, tmp_path):
        """An empty-string primary_key must raise ValueError."""
        # Create a real file so the file-exists check passes
        dbf_file = tmp_path / "data.dbf"
        dbf_file.write_bytes(b"\x00")

        mock_pyogrio = _install_mock_pyogrio()
        try:
            with patch(f"{_MOD}._check_pyogrio_available", return_value=True), \
                 patch(f"{_MOD}._PYOGRIO_AVAILABLE", True):
                with pytest.raises(ValueError, match="primary_key is required"):
                    import_dbf(
                        file_path=str(dbf_file),
                        table_name="test_table",
                        primary_key="",
                        database_url="postgresql://localhost/test",
                    )
        finally:
            sys.modules.pop("pyogrio", None)


# =============================================================================
# Column sanitization
# =============================================================================


class TestImportDbfColumnSanitization:
    """Verify that DBF column names are sanitized before import."""

    def test_columns_are_sanitized(self, tmp_path):
        """Messy column names must be sanitized to database-friendly format."""
        dbf_file = tmp_path / "data.dbf"
        dbf_file.write_bytes(b"\x00")

        sample_df = pd.DataFrame({
            "Customer Nr.": ["1"],
            "Order-ID": ["100"],
            "123value": ["abc"],
        })

        mock_pyogrio = _install_mock_pyogrio()
        mock_pyogrio.read_info.return_value = {"features": 2}
        mock_pyogrio.read_dataframe.return_value = sample_df

        try:
            with patch(f"{_MOD}._check_pyogrio_available", return_value=True), \
                 patch(f"{_MOD}._PYOGRIO_AVAILABLE", True), \
                 patch(f"{_MOD}._get_file_size_mb", return_value=0.01), \
                 patch(f"{_MOD}._import_chunks") as mock_import_chunks:

                mock_import_chunks.return_value = MagicMock(
                    inserted=2, updated=0, skipped=0, errors=[]
                )

                import_dbf(
                    file_path=str(dbf_file),
                    table_name="test_table",
                    primary_key="Customer_Nr",
                    database_url="postgresql://localhost/test",
                )

                mock_import_chunks.assert_called_once()
                call_kwargs = mock_import_chunks.call_args

                # _import_chunks is called with keyword arguments
                final_columns = call_kwargs.kwargs.get("final_columns")

                assert final_columns is not None, (
                    "_import_chunks was not called with final_columns"
                )
                assert "Customer_Nr" in final_columns
                assert "Order_ID" in final_columns
                assert "_123value" in final_columns
        finally:
            sys.modules.pop("pyogrio", None)


# =============================================================================
# NaN handling
# =============================================================================


class TestImportDbfNullHandling:
    """Verify that NaN values in DBF data become empty strings."""

    def test_nan_becomes_empty_string(self, tmp_path):
        """NaN values must be replaced with empty strings before import."""
        dbf_file = tmp_path / "data.dbf"
        dbf_file.write_bytes(b"\x00")

        # Sample with clean column names so sanitization is a no-op
        sample_df = pd.DataFrame({
            "id": ["1"],
            "name": ["Alice"],
        })

        # Data chunk that will contain NaN
        data_df = pd.DataFrame({
            "id": ["1", "2"],
            "name": ["Alice", np.nan],
        })

        mock_pyogrio = _install_mock_pyogrio()
        mock_pyogrio.read_info.return_value = {"features": 2}

        # First call: sample (max_features=1); subsequent calls: data chunks
        mock_pyogrio.read_dataframe.side_effect = [
            sample_df,  # sample read for column discovery
            data_df,    # first chunk
            pd.DataFrame(columns=["id", "name"]),  # empty = end of iteration
        ]

        captured_chunks = []

        def fake_import_chunks(chunks, **kwargs):
            for chunk in chunks:
                captured_chunks.append(chunk.copy())
            return MagicMock(inserted=2, updated=0, skipped=0, errors=[])

        try:
            with patch(f"{_MOD}._check_pyogrio_available", return_value=True), \
                 patch(f"{_MOD}._PYOGRIO_AVAILABLE", True), \
                 patch(f"{_MOD}._get_file_size_mb", return_value=0.01), \
                 patch(f"{_MOD}._import_chunks", side_effect=fake_import_chunks):

                import_dbf(
                    file_path=str(dbf_file),
                    table_name="test_table",
                    primary_key="id",
                    database_url="postgresql://localhost/test",
                )
        finally:
            sys.modules.pop("pyogrio", None)

        assert len(captured_chunks) == 1, "Expected exactly one data chunk"
        chunk = captured_chunks[0]

        # The NaN value should now be an empty string
        assert chunk.iloc[1]["name"] == "", (
            f"Expected empty string for NaN, got: {chunk.iloc[1]['name']!r}"
        )
        # Non-NaN values should be converted to string
        assert chunk.iloc[0]["name"] == "Alice"


# =============================================================================
# _import_file dispatch
# =============================================================================


class TestImportFileDispatch:
    """Verify _import_file routes to the correct importer based on extension."""

    def _make_table_config(self):
        """Create a minimal mock table config for dispatch tests."""
        config = MagicMock()
        config.target_table = "test_table"
        config.primary_key = ["id"]
        config.column_mapping = None
        config.rebuild_table = False
        config.db_schema = "public"
        config.delimiter = ","
        config.encoding = "utf-8"
        config.skiprows = 0
        config.datestyle = None
        return config

    def test_csv_routes_to_import_csv(self):
        """A .csv file path must dispatch to import_csv."""
        from src.api.routes import _import_file

        table_config = self._make_table_config()

        with patch(f"{_MOD}.import_csv") as mock_csv, \
             patch(f"{_MOD}.import_dbf") as mock_dbf:
            mock_csv.return_value = MagicMock()

            _import_file(
                "/data/customers.csv", table_config, "postgresql://localhost/test"
            )

            mock_csv.assert_called_once()
            mock_dbf.assert_not_called()

    def test_dbf_routes_to_import_dbf(self):
        """A .dbf file path must dispatch to import_dbf."""
        from src.api.routes import _import_file

        table_config = self._make_table_config()

        with patch(f"{_MOD}.import_csv") as mock_csv, \
             patch(f"{_MOD}.import_dbf") as mock_dbf:
            mock_dbf.return_value = MagicMock()

            _import_file(
                "/data/orders.dbf", table_config, "postgresql://localhost/test"
            )

            mock_dbf.assert_called_once()
            mock_csv.assert_not_called()
