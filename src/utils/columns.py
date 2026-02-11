"""Shared column name utilities for database import."""

import re
from typing import List


def sanitize_column_name(name: str) -> str:
    """
    Convert column names to database-friendly format.

    Replaces spaces, hyphens, and other problematic characters with underscores.
    Keeps original case.
    """
    # Replace common separators with underscore
    name = re.sub(r"[-\s\.\,\(\)\[\]\{\}\/\\]+", "_", name)

    # Remove leading/trailing underscores
    name = name.strip("_")

    # Ensure name starts with letter or underscore (database requirement)
    if name and name[0].isdigit():
        name = f"_{name}"

    # Remove any remaining problematic characters but keep umlauts
    name = re.sub(r"[^\w\u00C0-\u00FF]", "_", name)

    # Remove multiple consecutive underscores
    name = re.sub(r"_+", "_", name)

    return name if name else "column"


def deduplicate_columns(columns: List[str]) -> List[str]:
    """
    Deduplicate column names by appending numeric suffixes.

    When duplicate column names are found, appends _1, _2, etc.
    to make each name unique.

    Args:
        columns: List of column names (potentially with duplicates)

    Returns:
        List of unique column names with suffixes added where needed
    """
    seen: set[str] = set()
    result = list(columns)
    for i, col in enumerate(result):
        original = col
        counter = 1
        while col in seen:
            col = f"{original}_{counter}"
            counter += 1
        result[i] = col
        seen.add(col)
    return result
