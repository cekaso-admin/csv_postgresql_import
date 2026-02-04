"""
Tests for file pattern matching utilities.

Tests backward compatibility (filename-only patterns) and new path pattern support.
"""

import pytest

from src.config.models import (
    is_path_pattern,
    normalize_path_for_matching,
    matches_pattern,
    extract_filename,
    DefaultsConfig,
    TableConfig,
    TableNamingConfig,
    ProjectConfig,
)


class TestIsPathPattern:
    """Tests for is_path_pattern utility function."""

    def test_filename_only_patterns(self):
        """Patterns without '/' are not path patterns."""
        assert is_path_pattern("*.csv") is False
        assert is_path_pattern("IxExp*.csv") is False
        assert is_path_pattern("customers.csv") is False
        assert is_path_pattern("data_2024*.csv") is False

    def test_path_patterns(self):
        """Patterns with '/' are path patterns."""
        assert is_path_pattern("reports/*.csv") is True
        assert is_path_pattern("archive/2024/*.csv") is True
        assert is_path_pattern("*/daily/*.csv") is True
        assert is_path_pattern("data/exports/IxExp*.csv") is True


class TestNormalizePathForMatching:
    """Tests for path normalization."""

    def test_forward_slashes_unchanged(self):
        """Paths with forward slashes stay unchanged."""
        assert normalize_path_for_matching("reports/data.csv") == "reports/data.csv"
        assert normalize_path_for_matching("a/b/c/file.csv") == "a/b/c/file.csv"

    def test_backslashes_converted(self):
        """Backslashes are converted to forward slashes."""
        assert normalize_path_for_matching("reports\\data.csv") == "reports/data.csv"
        assert normalize_path_for_matching("a\\b\\c\\file.csv") == "a/b/c/file.csv"

    def test_mixed_slashes(self):
        """Mixed slashes are normalized to forward slashes."""
        assert normalize_path_for_matching("a\\b/c\\d.csv") == "a/b/c/d.csv"

    def test_filename_only(self):
        """Filenames without slashes stay unchanged."""
        assert normalize_path_for_matching("data.csv") == "data.csv"


class TestExtractFilename:
    """Tests for filename extraction."""

    def test_path_with_forward_slashes(self):
        """Extract filename from path with forward slashes."""
        assert extract_filename("reports/2024/data.csv") == "data.csv"
        assert extract_filename("a/b/c/file.csv") == "file.csv"

    def test_path_with_backslashes(self):
        """Extract filename from Windows-style paths."""
        assert extract_filename("reports\\2024\\data.csv") == "data.csv"

    def test_filename_only(self):
        """Filenames without path components stay unchanged."""
        assert extract_filename("data.csv") == "data.csv"
        assert extract_filename("IxExpKonto.csv") == "IxExpKonto.csv"


class TestMatchesPattern:
    """Tests for the unified matches_pattern function."""

    # Backward compatibility tests - filename-only patterns
    def test_filename_pattern_matches_filename(self):
        """Filename patterns match against filenames."""
        assert matches_pattern("*.csv", "data.csv") is True
        assert matches_pattern("*.csv", "report.csv") is True
        assert matches_pattern("IxExp*.csv", "IxExpKonto.csv") is True
        assert matches_pattern("IxExp*.csv", "IxExpMieter.csv") is True

    def test_filename_pattern_no_match(self):
        """Filename patterns don't match non-matching filenames."""
        assert matches_pattern("*.csv", "data.txt") is False
        assert matches_pattern("IxExp*.csv", "Konto.csv") is False
        assert matches_pattern("customers*.csv", "orders.csv") is False

    def test_filename_pattern_matches_path_basename(self):
        """Filename patterns match against basename of paths (backward compatible)."""
        assert matches_pattern("*.csv", "reports/data.csv") is True
        assert matches_pattern("IxExp*.csv", "archive/2024/IxExpKonto.csv") is True
        assert matches_pattern("data.csv", "some/deep/path/data.csv") is True

    # New path pattern tests
    def test_path_pattern_matches_path(self):
        """Path patterns match against full relative paths."""
        assert matches_pattern("reports/*.csv", "reports/data.csv") is True
        assert matches_pattern("reports/*.csv", "reports/sales.csv") is True
        assert matches_pattern("archive/2024/*.csv", "archive/2024/data.csv") is True

    def test_path_pattern_no_match_wrong_directory(self):
        """Path patterns don't match files in wrong directories."""
        assert matches_pattern("reports/*.csv", "archive/data.csv") is False
        assert matches_pattern("archive/2024/*.csv", "archive/2023/data.csv") is False
        assert matches_pattern("exports/*.csv", "data.csv") is False

    def test_path_pattern_wildcard_directory(self):
        """Path patterns with wildcard directories work."""
        assert matches_pattern("*/daily/*.csv", "reports/daily/data.csv") is True
        assert matches_pattern("*/daily/*.csv", "archive/daily/export.csv") is True
        assert matches_pattern("*/daily/*.csv", "reports/weekly/data.csv") is False

    def test_path_pattern_no_match_filename_only(self):
        """Path patterns don't match filenames without path."""
        assert matches_pattern("reports/*.csv", "data.csv") is False

    def test_windows_path_normalization(self):
        """Windows-style paths are normalized before matching."""
        assert matches_pattern("reports/*.csv", "reports\\data.csv") is True


class TestDefaultsConfigMatchesFile:
    """Tests for DefaultsConfig.matches_file method."""

    def test_filename_pattern_backward_compatible(self):
        """Filename patterns work as before."""
        defaults = DefaultsConfig(file_pattern="IxExp*.csv", primary_key="id")
        assert defaults.matches_file("IxExpKonto.csv") is True
        assert defaults.matches_file("IxExpMieter.csv") is True
        assert defaults.matches_file("Other.csv") is False

    def test_path_pattern_support(self):
        """Path patterns are supported."""
        defaults = DefaultsConfig(file_pattern="reports/*.csv", primary_key="id")
        assert defaults.matches_file("reports/data.csv") is True
        assert defaults.matches_file("archive/data.csv") is False

    def test_filename_pattern_with_path_input(self):
        """Filename patterns match basename when given paths."""
        defaults = DefaultsConfig(file_pattern="*.csv", primary_key="id")
        assert defaults.matches_file("some/deep/path/data.csv") is True


class TestTableConfigMatchesFile:
    """Tests for TableConfig.matches_file method."""

    def test_filename_pattern_backward_compatible(self):
        """Filename patterns work as before."""
        table = TableConfig(
            file_pattern="customers*.csv",
            target_table="customers",
            primary_key="id"
        )
        assert table.matches_file("customers.csv") is True
        assert table.matches_file("customers_2024.csv") is True
        assert table.matches_file("orders.csv") is False

    def test_path_pattern_support(self):
        """Path patterns are supported."""
        table = TableConfig(
            file_pattern="archive/2024/*.csv",
            target_table="historical",
            primary_key="id"
        )
        assert table.matches_file("archive/2024/data.csv") is True
        assert table.matches_file("archive/2023/data.csv") is False


class TestTableNamingConfigTransform:
    """Tests for TableNamingConfig.transform with path inputs."""

    def test_filename_input(self):
        """Transform works with filename inputs."""
        naming = TableNamingConfig(strip_prefix="IxExp", lowercase=True)
        assert naming.transform("IxExpKonto.csv") == "konto"

    def test_path_input_extracts_filename(self):
        """Transform extracts filename from paths before transforming."""
        naming = TableNamingConfig(strip_prefix="IxExp", lowercase=True)
        assert naming.transform("reports/IxExpKonto.csv") == "konto"
        assert naming.transform("archive/2024/IxExpMieter.csv") == "mieter"

    def test_windows_path_input(self):
        """Transform handles Windows-style paths."""
        naming = TableNamingConfig(strip_prefix="IxExp", lowercase=True)
        assert naming.transform("reports\\IxExpKonto.csv") == "konto"


class TestProjectConfigGetTableForFile:
    """Tests for ProjectConfig.get_table_for_file with path support."""

    def test_explicit_table_filename_pattern(self):
        """Explicit table configs with filename patterns work."""
        config = ProjectConfig(
            project="test",
            tables=[
                TableConfig(
                    file_pattern="customers*.csv",
                    target_table="customers",
                    primary_key="id"
                )
            ]
        )
        table = config.get_table_for_file("customers_2024.csv")
        assert table is not None
        assert table.target_table == "customers"

    def test_explicit_table_path_pattern(self):
        """Explicit table configs with path patterns work."""
        config = ProjectConfig(
            project="test",
            tables=[
                TableConfig(
                    file_pattern="archive/*/*.csv",
                    target_table="historical",
                    primary_key="id"
                )
            ]
        )
        table = config.get_table_for_file("archive/2024/data.csv")
        assert table is not None
        assert table.target_table == "historical"

        # Should not match files outside archive
        assert config.get_table_for_file("data.csv") is None

    def test_defaults_with_path_pattern(self):
        """Defaults with path patterns auto-generate table configs."""
        config = ProjectConfig(
            project="test",
            defaults=DefaultsConfig(
                file_pattern="reports/*.csv",
                primary_key="id"
            ),
            table_naming=TableNamingConfig(lowercase=True)
        )
        table = config.get_table_for_file("reports/sales.csv")
        assert table is not None
        assert table.target_table == "sales"

        # Should not match files outside reports
        assert config.get_table_for_file("data.csv") is None

    def test_backward_compatibility_filename_pattern(self):
        """Existing configs with filename patterns still work."""
        config = ProjectConfig(
            project="test",
            defaults=DefaultsConfig(
                file_pattern="IxExp*.csv",
                primary_key="HDR_ID"
            ),
            table_naming=TableNamingConfig(
                strip_prefix="IxExp",
                lowercase=True
            )
        )
        # Should match by filename
        table = config.get_table_for_file("IxExpKonto.csv")
        assert table is not None
        assert table.target_table == "konto"

        # Should also match when given a path (uses basename)
        table = config.get_table_for_file("some/path/IxExpKonto.csv")
        assert table is not None
        assert table.target_table == "konto"
