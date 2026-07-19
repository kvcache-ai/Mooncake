"""Model-specific Omni stage adapters.

The process runtime stays model agnostic; optional model workers live here so
their imports remain explicit and version-scoped.
"""

from .qwen25_omni import (
    OmniBatchEncoderOutput,
    OmniEncoderOutput,
    Qwen25OmniImageEncoderWorker,
)

__all__ = [
    "OmniBatchEncoderOutput",
    "OmniEncoderOutput",
    "Qwen25OmniImageEncoderWorker",
]
