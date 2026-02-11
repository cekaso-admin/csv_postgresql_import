"""
Tests for shared column name utilities in src/utils/columns.py.

Covers sanitize_column_name (special characters, leading digits, umlauts,
collapsing underscores, empty input) and deduplicate_columns (no-ops,
duplicates, triple duplicates, empty list).
"""

import pytest

from src.utils.columns import sanitize_column_name, deduplicate_columns


# =============================================================================
# sanitize_column_name
# =============================================================================


class TestSanitizeColumnName:
    """Verify column name sanitization rules."""

    def test_spaces_replaced(self):
        """Spaces should be replaced with underscores."""
        assert sanitize_column_name("Customer Name") == "Customer_Name"

    def test_hyphens_replaced(self):
        """Hyphens should be replaced with underscores."""
        assert sanitize_column_name("order-id") == "order_id"

    def test_dots_replaced(self):
        """Dots should be replaced with underscores, trailing underscore stripped."""
        assert sanitize_column_name("Kunden.Nr.") == "Kunden_Nr"

    def test_leading_digit(self):
        """Names starting with a digit should be prefixed with underscore."""
        assert sanitize_column_name("123abc") == "_123abc"

    def test_umlauts_preserved(self):
        """German umlauts and other extended Latin characters must be kept."""
        assert sanitize_column_name("Größe") == "Größe"

    def test_multiple_underscores_collapsed(self):
        """Consecutive underscores should be collapsed to a single underscore."""
        assert sanitize_column_name("a___b") == "a_b"

    def test_empty_string(self):
        """An empty string should return the fallback name 'column'."""
        assert sanitize_column_name("") == "column"

    def test_already_valid(self):
        """A name that is already clean should pass through unchanged."""
        assert sanitize_column_name("customer_id") == "customer_id"


# =============================================================================
# deduplicate_columns
# =============================================================================


class TestDeduplicateColumns:
    """Verify column deduplication with numeric suffixes."""

    def test_no_duplicates(self):
        """A list with unique names should be returned unchanged."""
        assert deduplicate_columns(["a", "b", "c"]) == ["a", "b", "c"]

    def test_with_duplicates(self):
        """Second occurrence of a name should get a _1 suffix."""
        assert deduplicate_columns(["id", "name", "id"]) == ["id", "name", "id_1"]

    def test_triple_duplicates(self):
        """Third occurrence should get _2, second gets _1."""
        assert deduplicate_columns(["x", "x", "x"]) == ["x", "x_1", "x_2"]

    def test_empty_list(self):
        """An empty list should return an empty list."""
        assert deduplicate_columns([]) == []
