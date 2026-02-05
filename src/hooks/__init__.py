"""
Hook system for pre/post-import actions.

This module provides a pluggable hook system for executing actions
at key points in the import lifecycle (pre_import, post_file_prepare,
pre_file_import, post_file_import, post_import).

Plugin developers should use the clean imports from this module:
    from src.hooks import HookExecutor, HookResult, HookContext, HookPoint

See src/hooks/plugins/README.md for plugin development guide.
"""

from src.hooks.engine import HookEngine
from src.hooks.executors.base import HookExecutor
from src.hooks.models import (
    HookAction,
    HookConfig,
    HookContext,
    HookExecutionResult,
    HookPoint,
    HookResult,
)

__all__ = [
    "HookEngine",
    "HookExecutor",
    "HookResult",
    "HookContext",
    "HookPoint",
    "HookConfig",
    "HookAction",
    "HookExecutionResult",
]
