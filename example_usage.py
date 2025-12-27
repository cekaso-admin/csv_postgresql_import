"""
Example usage of the CSV PostgreSQL import module.

This script demonstrates how to use the core database modules to import
CSV files into PostgreSQL with upsert functionality.

Prerequisites:
1. Create a .env file based on .env.example
2. Set your DATABASE_URL in the .env file
3. Ensure your database is accessible

Usage:
    python example_usage.py
"""

import logging
from pathlib import Path

from src.db.connection import get_connection, close_pool, test_connection
from src.db.schema import table_exists, get_table_columns
from src.db.importer import import_csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def example_connection_test():
    """Test database connection."""
    logger.info("Testing database connection...")

    if test_connection():
        logger.info("Database connection successful!")
        return True
    else:
        logger.error("Database connection failed!")
        return False


def example_table_operations():
    """Demonstrate table operations."""
    logger.info("\n--- Table Operations Example ---")

    table_name = "test_customers"

    # Check if table exists
    if table_exists(table_name):
        logger.info(f"Table '{table_name}' exists")

        # Get columns
        columns = get_table_columns(table_name)
        logger.info(f"Columns: {columns}")
    else:
        logger.info(f"Table '{table_name}' does not exist")


def example_csv_import():
    """
    Demonstrate CSV import with example data.

    Note: This requires a CSV file and will create a table in your database.
    Modify the paths and table names as needed for your setup.
    """
    logger.info("\n--- CSV Import Example ---")

    # Example configuration
    csv_file = "example_data.csv"
    table_name = "customers"
    primary_key = "customer_id"

    # Check if example CSV exists
    if not Path(csv_file).exists():
        logger.warning(
            f"Example CSV file '{csv_file}' not found. "
            "Create a CSV file to test import functionality."
        )
        logger.info(
            "Example CSV format:\n"
            "customer_id,name,email\n"
            "1,John Doe,john@example.com\n"
            "2,Jane Smith,jane@example.com"
        )
        return

    try:
        # Import CSV
        result = import_csv(
            file_path=csv_file,
            table_name=table_name,
            primary_key=primary_key,
            rebuild_table=False  # Set to True to truncate before import
        )

        # Display results
        logger.info(f"Import completed!")
        logger.info(f"Inserted: {result.inserted}")
        logger.info(f"Updated: {result.updated}")
        logger.info(f"Total rows: {result.total_rows}")

        if result.errors:
            logger.error(f"Errors: {result.errors}")

    except Exception as e:
        logger.error(f"Import failed: {e}", exc_info=True)


def example_with_column_mapping():
    """Demonstrate CSV import with column name mapping."""
    logger.info("\n--- CSV Import with Column Mapping Example ---")

    csv_file = "german_customers.csv"
    table_name = "customers"

    # Map German column names to English
    column_mapping = {
        "Kunde Nr.": "customer_id",
        "Name": "name",
        "E-Mail": "email"
    }

    if not Path(csv_file).exists():
        logger.warning(
            f"Example CSV file '{csv_file}' not found. "
            "Create a CSV file with German headers to test column mapping."
        )
        return

    try:
        result = import_csv(
            file_path=csv_file,
            table_name=table_name,
            primary_key="customer_id",
            column_mapping=column_mapping
        )

        logger.info(f"Import completed!")
        logger.info(f"Inserted: {result.inserted}, Updated: {result.updated}")

    except Exception as e:
        logger.error(f"Import failed: {e}", exc_info=True)


def example_composite_key():
    """Demonstrate CSV import with composite primary key."""
    logger.info("\n--- CSV Import with Composite Key Example ---")

    csv_file = "order_lines.csv"
    table_name = "order_lines"

    # Use composite primary key
    primary_key = ["order_id", "line_number"]

    if not Path(csv_file).exists():
        logger.warning(
            f"Example CSV file '{csv_file}' not found. "
            "Create a CSV file with order_id and line_number columns."
        )
        logger.info(
            "Example CSV format:\n"
            "order_id,line_number,product,quantity\n"
            "ORD001,1,Widget A,5\n"
            "ORD001,2,Widget B,3"
        )
        return

    try:
        result = import_csv(
            file_path=csv_file,
            table_name=table_name,
            primary_key=primary_key
        )

        logger.info(f"Import completed!")
        logger.info(f"Inserted: {result.inserted}, Updated: {result.updated}")

    except Exception as e:
        logger.error(f"Import failed: {e}", exc_info=True)


def main():
    """Run all examples."""
    logger.info("=== CSV PostgreSQL Import - Example Usage ===\n")

    # Test connection
    if not example_connection_test():
        logger.error("Cannot proceed without database connection")
        return

    # Run examples
    example_table_operations()

    # Uncomment the examples you want to try:
    # example_csv_import()
    # example_with_column_mapping()
    # example_composite_key()

    # Clean up
    logger.info("\n--- Cleanup ---")
    close_pool()
    logger.info("Connection pool closed")


if __name__ == "__main__":
    main()
