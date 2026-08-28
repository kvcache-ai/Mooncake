"""Mooncake split-package namespace for source-tree development."""

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)
