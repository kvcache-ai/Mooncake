"""Qwen2.5-Omni worker import surface for the process-runtime package.

The implementation remains in :mod:`mooncake_epd.core.omni_encoder_worker`
for backward compatibility with the existing encoder service.  This explicit
module is the canonical process-pipeline import and deliberately contains no
transport fallback: callers choose a concrete edge in ``core.omni``.
"""

from ...omni_encoder_worker import (
    OmniBatchEncoderOutput,
    OmniEncoderOutput,
    Qwen25OmniImageEncoderWorker,
)

__all__ = [
    "OmniBatchEncoderOutput",
    "OmniEncoderOutput",
    "Qwen25OmniImageEncoderWorker",
]
