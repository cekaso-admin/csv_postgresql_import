"""
Configuration module for CSV import projects.

This module provides Pydantic models and YAML loading utilities
for managing per-project import configurations.

Supports two modes:
1. Explicit table mappings (list each file pattern)
2. Auto-discovery with defaults (process all matching files with shared settings)
"""

from src.config.models import (
    ConnectionConfig,
    DefaultsConfig,
    ProjectConfig,
    SFTPConfig,
    TableConfig,
    TableNamingConfig,
)
from src.config.loader import (
    ConfigError,
    load_project_config,
    load_config_from_dict,
    list_available_projects,
)

__all__ = [
    # Models
    "ConnectionConfig",
    "DefaultsConfig",
    "ProjectConfig",
    "SFTPConfig",
    "TableConfig",
    "TableNamingConfig",
    # Loader
    "ConfigError",
    "load_project_config",
    "load_config_from_dict",
    "list_available_projects",
]
