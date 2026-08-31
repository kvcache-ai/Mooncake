"""Typing compatibility definitions for the supported Python runtimes."""

import sys

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

__all__ = ["TypeAlias"]
