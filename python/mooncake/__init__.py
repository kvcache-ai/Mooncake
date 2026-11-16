"""Public Python package for Mooncake."""

from __future__ import annotations

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

# TODO(F01-T110, replace-in=UPG1-F07-S13): Restore U's BufferPool and
# Source: UPG1-F01-3.1; tracking clarified by F02-B05/Codex, 2026-11-12.
# Due: UPG1-F07-S13 implementation/merge review, before task acceptance.
# RegisteredBufferPool lazy exports, __all__, and __dir__ after the native
# bindings, Store facade, and buffer_pool.py are available.
# Owner: F07-S13. Remove this marker with the verified replacement.
__all__ = []
