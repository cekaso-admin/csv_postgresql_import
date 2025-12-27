"""
YAML configuration loader for project settings.

This module provides functions to load and validate project
configuration files from the config/ directory.
"""

import logging
import os
from pathlib import Path
from typing import Optional

import yaml
from pydantic import ValidationError

from src.config.models import ProjectConfig

logger = logging.getLogger(__name__)

# Default config directory relative to project root
DEFAULT_CONFIG_DIR = "config"


class ConfigError(Exception):
    """Raised when configuration loading or validation fails."""
    pass


def get_config_path(project_name: str, config_dir: Optional[str] = None) -> Path:
    """
    Get the path to a project's configuration file.

    Args:
        project_name: Name of the project (without .yaml extension)
        config_dir: Optional custom config directory path

    Returns:
        Path to the configuration file

    Raises:
        ConfigError: If config file doesn't exist
    """
    if config_dir is None:
        # Use environment variable or default
        config_dir = os.getenv("CONFIG_DIR", DEFAULT_CONFIG_DIR)

    config_path = Path(config_dir) / f"{project_name}.yaml"

    if not config_path.exists():
        raise ConfigError(
            f"Configuration file not found: {config_path}. "
            f"Please create {project_name}.yaml in the config directory."
        )

    return config_path


def load_yaml_file(file_path: Path) -> dict:
    """
    Load and parse a YAML file.

    Args:
        file_path: Path to YAML file

    Returns:
        Parsed YAML content as dictionary

    Raises:
        ConfigError: If file cannot be read or parsed
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = yaml.safe_load(f)

        if content is None:
            raise ConfigError(f"Empty configuration file: {file_path}")

        if not isinstance(content, dict):
            raise ConfigError(
                f"Invalid configuration format in {file_path}. "
                "Expected a YAML mapping (dictionary)."
            )

        return content

    except yaml.YAMLError as e:
        raise ConfigError(f"Failed to parse YAML in {file_path}: {e}") from e
    except IOError as e:
        raise ConfigError(f"Failed to read configuration file {file_path}: {e}") from e


def load_project_config(
    project_name: str,
    config_dir: Optional[str] = None
) -> ProjectConfig:
    """
    Load and validate a project configuration.

    Args:
        project_name: Name of the project (matches filename without .yaml)
        config_dir: Optional custom config directory path

    Returns:
        Validated ProjectConfig object

    Raises:
        ConfigError: If file not found, invalid YAML, or validation fails

    Example:
        >>> config = load_project_config("customer_abc")
        >>> print(config.project)
        'customer_abc'
        >>> table = config.get_table_for_file("customers_2024.csv")
        >>> print(table.target_table)
        'customers'
    """
    config_path = get_config_path(project_name, config_dir)
    logger.info(f"Loading configuration from: {config_path}")

    raw_config = load_yaml_file(config_path)

    try:
        config = ProjectConfig(**raw_config)
        logger.info(
            f"Loaded project '{config.project}' with {len(config.tables)} table mappings"
        )
        return config

    except ValidationError as e:
        error_messages = []
        for error in e.errors():
            location = " -> ".join(str(loc) for loc in error["loc"])
            error_messages.append(f"  {location}: {error['msg']}")

        raise ConfigError(
            f"Invalid configuration in {config_path}:\n" +
            "\n".join(error_messages)
        ) from e


def load_config_from_dict(config_dict: dict) -> ProjectConfig:
    """
    Create a ProjectConfig from a dictionary.

    Useful for testing or when configuration is provided
    programmatically (e.g., via API request).

    Args:
        config_dict: Dictionary with configuration values

    Returns:
        Validated ProjectConfig object

    Raises:
        ConfigError: If validation fails
    """
    try:
        return ProjectConfig(**config_dict)
    except ValidationError as e:
        error_messages = []
        for error in e.errors():
            location = " -> ".join(str(loc) for loc in error["loc"])
            error_messages.append(f"  {location}: {error['msg']}")

        raise ConfigError(
            "Invalid configuration:\n" + "\n".join(error_messages)
        ) from e


def list_available_projects(config_dir: Optional[str] = None) -> list[str]:
    """
    List all available project configurations.

    Args:
        config_dir: Optional custom config directory path

    Returns:
        List of project names (without .yaml extension)
    """
    if config_dir is None:
        config_dir = os.getenv("CONFIG_DIR", DEFAULT_CONFIG_DIR)

    config_path = Path(config_dir)

    if not config_path.exists():
        logger.warning(f"Config directory does not exist: {config_path}")
        return []

    projects = [
        f.stem for f in config_path.glob("*.yaml")
        if f.is_file()
    ]

    return sorted(projects)
