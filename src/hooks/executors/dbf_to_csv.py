"""
DBF to CSV conversion executor.

This executor converts DBF (dBASE) files to CSV format, allowing
legacy database files to be imported using the standard CSV pipeline.

Uses the ethanfurman/dbf library for reliable parsing with auto-encoding detection.

Usage in project config:
    hooks:
      post_file_prepare:
        - type: dbf_to_csv
          input_pattern: "*.dbf"
          encoding: "auto"        # or specific encoding like "cp850"
          delete_original: false
          ignore_memos: true      # skip memo fields (default: true)
          on_error: fail
"""

import csv
import fnmatch
import logging
import os
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, ClassVar, List, Optional, Tuple

from src.hooks.executors.base import HookExecutor
from src.hooks.models import HookContext, HookResult

logger = logging.getLogger(__name__)

# Flag to track if dbf is available
_DBF_AVAILABLE: Optional[bool] = None

# Common encodings for DBF files with German/European characters
COMMON_ENCODINGS = [
    "cp850",  # DOS Latin-1 (most common for old German DBF files)
    "cp437",  # DOS US (also common)
    "cp1252",  # Windows Western European
    "latin-1",  # ISO-8859-1
    "iso-8859-15",  # Latin-9 (has Euro symbol)
    "utf-8",  # Modern files
    "cp852",  # DOS Central European
]


def _check_dbf_available() -> bool:
    """
    Check if the dbf package is available.

    Returns:
        True if dbf can be imported, False otherwise
    """
    global _DBF_AVAILABLE

    if _DBF_AVAILABLE is None:
        try:
            import dbf  # noqa: F401

            _DBF_AVAILABLE = True
        except ImportError:
            _DBF_AVAILABLE = False

    return _DBF_AVAILABLE


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


def format_value(value: Any) -> str:
    """
    Format values based on their type for CSV output.
    """
    if value is None:
        return ""

    # Handle dbf.Date types
    if hasattr(value, "__class__") and value.__class__.__name__ == "Date":
        try:
            return value.strftime("%Y-%m-%d")
        except Exception:
            return ""

    # Handle datetime types
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(value, date):
        return value.strftime("%Y-%m-%d")

    # Handle boolean types
    if isinstance(value, bool):
        return "true" if value else "false"

    # Handle dbf.Logical type
    if hasattr(value, "__class__") and value.__class__.__name__ == "Logical":
        return "true" if value else "false"

    # Handle string types - trim whitespace
    if isinstance(value, str):
        return value.strip()

    # Handle numeric types
    return str(value)


class DbfToCsvExecutor(HookExecutor):
    """
    Hook executor that converts DBF files to CSV format.

    Uses the ethanfurman/dbf library with auto-encoding detection.

    Configuration:
        ```yaml
        hooks:
          post_file_prepare:
            - type: dbf_to_csv
              input_pattern: "*.dbf"  # Glob pattern for DBF files (default: "*.dbf")
              encoding: "auto"        # Encoding or "auto" for detection (default: "auto")
              delete_original: false  # Delete original DBF after conversion (default: false)
              ignore_memos: true      # Skip memo fields; set false to include (default: true)
              on_error: fail          # Error handling: fail, warn, ignore
        ```

    The executor finds all files matching input_pattern in context.file_paths,
    converts each to CSV with database-friendly column names, and returns a
    transformed_files mapping so the HookEngine can update file_paths.
    """

    action_type: ClassVar[str] = "dbf_to_csv"

    def execute(self, action: dict, context: HookContext) -> HookResult:
        """
        Convert DBF files to CSV.

        Args:
            action: Configuration dict with optional fields:
                - input_pattern: Glob pattern to match DBF files (default: "*.dbf")
                - encoding: Character encoding or "auto" (default: "auto")
                - delete_original: Whether to delete original DBF files (default: False)
                - ignore_memos: Whether to skip memo fields (default: True). Set to
                  False to include memo fields; requires a companion .fpt file.
            context: Hook context with file_paths to process

        Returns:
            HookResult with transformed_files mapping original DBF paths to new CSV paths
        """
        start_time = time.time()
        hook_name = action.get("name", self.action_type)

        # Check dbf availability
        if not _check_dbf_available():
            error_msg = (
                "dbf package not installed. " "Install with: pip install dbf"
            )
            logger.error(
                error_msg,
                extra={
                    "job_id": context.job_id,
                    "project": context.project_name,
                },
            )
            return HookResult(
                success=False,
                hook_name=hook_name,
                hook_type=self.action_type,
                error=error_msg,
                duration_seconds=time.time() - start_time,
            )

        # Get configuration
        input_pattern = action.get("input_pattern", "*.dbf")
        encoding = action.get("encoding", "auto")
        delete_original = action.get("delete_original", False)
        ignore_memos = action.get("ignore_memos", True)

        logger.info(
            f"Converting DBF files matching '{input_pattern}'",
            extra={
                "job_id": context.job_id,
                "project": context.project_name,
                "pattern": input_pattern,
                "encoding": encoding,
                "delete_original": delete_original,
            },
        )

        # Find matching files
        matching_files = self._find_matching_files(context.file_paths, input_pattern)

        if not matching_files:
            message = f"No files matching pattern '{input_pattern}' found"
            logger.info(
                message,
                extra={
                    "job_id": context.job_id,
                    "file_count": len(context.file_paths),
                },
            )
            return HookResult(
                success=True,
                hook_name=hook_name,
                hook_type=self.action_type,
                message=message,
                duration_seconds=time.time() - start_time,
            )

        # Convert each DBF file
        transformed_files: dict[str, str] = {}
        errors: list[str] = []
        converted_count = 0

        for dbf_path in matching_files:
            try:
                csv_path, used_encoding = self._convert_dbf_to_csv(
                    dbf_path=dbf_path,
                    encoding=encoding,
                    ignore_memos=ignore_memos,
                )
                transformed_files[dbf_path] = csv_path
                converted_count += 1

                logger.debug(
                    f"Converted {dbf_path} -> {csv_path} (encoding: {used_encoding})",
                    extra={
                        "job_id": context.job_id,
                        "source": dbf_path,
                        "target": csv_path,
                        "encoding": used_encoding,
                    },
                )

                # Delete original if requested
                if delete_original:
                    try:
                        os.remove(dbf_path)
                        logger.debug(
                            f"Deleted original DBF file: {dbf_path}",
                            extra={"job_id": context.job_id},
                        )
                    except OSError as e:
                        logger.warning(
                            f"Failed to delete original DBF file {dbf_path}: {e}",
                            extra={"job_id": context.job_id},
                        )

                    # Delete companion files (.fpt, .dbt, .cdx, .mdx, .ntx)
                    companion_extensions = {".fpt", ".dbt", ".cdx", ".mdx", ".ntx"}
                    dbf_stem = Path(dbf_path).stem.lower()
                    dbf_dir = Path(dbf_path).parent
                    try:
                        dir_entries = os.listdir(dbf_dir)
                    except OSError:
                        dir_entries = []
                    for entry in dir_entries:
                        entry_path = Path(dbf_dir) / entry
                        if (
                            entry_path.stem.lower() == dbf_stem
                            and entry_path.suffix.lower() in companion_extensions
                        ):
                            try:
                                os.remove(str(entry_path))
                                logger.debug(
                                    f"Deleted companion file: {entry_path}",
                                    extra={"job_id": context.job_id},
                                )
                            except OSError as e:
                                logger.warning(
                                    f"Failed to delete companion file {entry_path}: {e}",
                                    extra={"job_id": context.job_id},
                                )

            except Exception as e:
                error_msg = f"Failed to convert {dbf_path}: {e}"
                errors.append(error_msg)
                logger.error(
                    error_msg,
                    extra={
                        "job_id": context.job_id,
                        "file": dbf_path,
                    },
                    exc_info=True,
                )

        duration = time.time() - start_time

        if errors:
            # Some files failed to convert
            error_summary = "; ".join(errors)
            message = (
                f"Converted {converted_count}/{len(matching_files)} DBF file(s). "
                f"Failed: {len(errors)}"
            )
            logger.warning(
                message,
                extra={
                    "job_id": context.job_id,
                    "converted": converted_count,
                    "failed": len(errors),
                    "errors": errors,
                },
            )
            return HookResult(
                success=False,
                hook_name=hook_name,
                hook_type=self.action_type,
                error=error_summary,
                message=message,
                duration_seconds=duration,
                transformed_files=transformed_files,
            )

        # All files converted successfully
        message = f"Converted {converted_count} DBF file(s) to CSV"
        logger.info(
            message,
            extra={
                "job_id": context.job_id,
                "converted": converted_count,
                "duration_seconds": duration,
            },
        )
        return HookResult(
            success=True,
            hook_name=hook_name,
            hook_type=self.action_type,
            message=message,
            duration_seconds=duration,
            transformed_files=transformed_files,
        )

    def _find_matching_files(
        self, file_paths: List[str], pattern: str
    ) -> List[str]:
        """
        Find files matching the input pattern.

        Args:
            file_paths: List of file paths to search
            pattern: Glob pattern to match (e.g., "*.dbf")

        Returns:
            List of matching file paths
        """
        matching = []
        for path in file_paths:
            # Match against filename only (not full path)
            filename = os.path.basename(path)
            if fnmatch.fnmatch(filename.lower(), pattern.lower()):
                matching.append(path)
        return matching

    def _convert_dbf_to_csv(
        self,
        dbf_path: str,
        encoding: str,
        ignore_memos: bool = True,
    ) -> Tuple[str, str]:
        """
        Convert a single DBF file to CSV using ethanfurman/dbf library.

        Uses auto-encoding detection with fallbacks for European/German files.

        Args:
            dbf_path: Path to the DBF file
            encoding: Character encoding or "auto" for detection
            ignore_memos: Whether to skip memo fields (default: True). When False,
                a companion .fpt file must exist alongside the DBF file.

        Returns:
            Tuple of (path to created CSV file, encoding used)

        Raises:
            Exception: If conversion fails with all attempted encodings, or if
                ignore_memos is False and no companion .fpt file is found
        """
        import dbf

        # Determine output path (same directory, .csv extension)
        dbf_file = Path(dbf_path)
        csv_path = str(dbf_file.with_suffix(".csv"))

        # Pre-check for companion .fpt file when memo support is requested
        if not ignore_memos:
            dbf_dir = dbf_file.parent
            dbf_stem_lower = dbf_file.stem.lower()
            fpt_found = False
            try:
                for entry in os.listdir(dbf_dir):
                    entry_path = Path(entry)
                    if (
                        entry_path.stem.lower() == dbf_stem_lower
                        and entry_path.suffix.lower() in {".fpt", ".dbt"}
                    ):
                        fpt_found = True
                        break
            except OSError:
                pass
            if not fpt_found:
                raise Exception(
                    f"Memo support requested (ignore_memos=false) but no .fpt/.dbt "
                    f"companion file found for {dbf_path}. Ensure the companion "
                    f"file exists in the same directory as the .dbf file, and that "
                    f"companion_extensions is configured in project defaults "
                    f"to download companion files via SFTP."
                )

        # Build list of encodings to try
        encodings_to_try: List[Optional[str]] = []
        if encoding and encoding.lower() != "auto":
            # User specified a specific encoding
            encodings_to_try.append(encoding)
        else:
            # Auto-detect: try None first (uses DBF header), then common fallbacks
            encodings_to_try.append(None)
            encodings_to_try.extend(COMMON_ENCODINGS)

        last_error: Optional[Exception] = None
        used_encoding = "unknown"

        for try_encoding in encodings_to_try:
            table = None
            try:
                # Open DBF file
                if try_encoding:
                    table = dbf.Table(
                        str(dbf_path), codepage=try_encoding, ignore_memos=ignore_memos
                    )
                else:
                    # Auto-detect from header
                    table = dbf.Table(str(dbf_path), ignore_memos=ignore_memos)

                table.open()

                # Get encoding name for logging
                if try_encoding:
                    used_encoding = try_encoding
                else:
                    detected_cp = table.codepage
                    used_encoding = (
                        detected_cp.name
                        if hasattr(detected_cp, "name")
                        else str(detected_cp)
                    )

                # Prepare column names (sanitize for database compatibility)
                original_columns = table.field_names
                sanitized_columns = [sanitize_column_name(col) for col in original_columns]

                # Handle duplicate column names after sanitization
                seen: set[str] = set()
                for i, col in enumerate(sanitized_columns):
                    original = col
                    counter = 1
                    while col in seen:
                        col = f"{original}_{counter}"
                        counter += 1
                    sanitized_columns[i] = col
                    seen.add(col)

                # Write CSV file
                with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

                    # Write header
                    writer.writerow(sanitized_columns)

                    # Write data rows
                    for record in table:
                        row = []
                        for field_name in original_columns:
                            value = record[field_name]
                            formatted_value = format_value(value)
                            row.append(formatted_value)
                        writer.writerow(row)

                table.close()
                return csv_path, used_encoding

            except UnicodeDecodeError as e:
                # Encoding failed, try next one
                if table:
                    try:
                        table.close()
                    except Exception:
                        pass
                last_error = e

                # Delete partial CSV file if it exists
                if Path(csv_path).exists():
                    Path(csv_path).unlink()

                if try_encoding:
                    logger.debug(f"Encoding {try_encoding} failed for {dbf_path}, trying next...")
                continue

            except Exception as e:
                # Non-encoding errors should be raised immediately
                if table:
                    try:
                        table.close()
                    except Exception:
                        pass
                raise

        # If we get here, all encodings failed
        if last_error:
            raise Exception(
                f"Could not find suitable encoding for {dbf_path}. "
                f"Last error: {last_error}"
            )
        else:
            raise Exception(f"Could not find suitable encoding for {dbf_path}")

    def should_run(self, action: dict, context: HookContext) -> bool:
        """
        Check if DBF conversion should run.

        Only runs if enabled and there are files in context.

        Args:
            action: The action configuration dict
            context: The hook execution context

        Returns:
            True if the hook should execute, False to skip
        """
        if not action.get("enabled", True):
            return False

        # Skip if no files to process
        if not context.file_paths:
            logger.debug(
                "Skipping DBF conversion - no files in context",
                extra={"job_id": context.job_id},
            )
            return False

        return True

    def validate_config(self, action: dict) -> Optional[str]:
        """
        Validate the action configuration.

        Args:
            action: The action configuration dict

        Returns:
            Error message string if validation fails, None if valid
        """
        # Validate encoding if provided (and not "auto")
        encoding = action.get("encoding")
        if encoding is not None and encoding.lower() != "auto":
            try:
                "test".encode(encoding)
            except LookupError:
                return f"Unknown encoding: {encoding}"

        # Validate delete_original is boolean if provided
        delete_original = action.get("delete_original")
        if delete_original is not None and not isinstance(delete_original, bool):
            return "delete_original must be a boolean"

        # Validate ignore_memos is boolean if provided
        ignore_memos = action.get("ignore_memos")
        if ignore_memos is not None and not isinstance(ignore_memos, bool):
            return "ignore_memos must be a boolean"

        return None
