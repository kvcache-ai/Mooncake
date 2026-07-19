"""Worker-process runtime primitives for Omni multi-stage pipelines."""

from .protocol import StageEnvelope, StagePayload, TensorRef
from .runtime import OmniProcessPipeline, ProcessStage
from .transports import (
    PosixSharedMemoryTransport,
    StageTransportError,
    TCPRelayServer,
    TCPRelayTransport,
    build_stage_transport,
)
from .workers import Qwen25OmniImageEncoderWorker

__all__ = [
    "OmniProcessPipeline",
    "PosixSharedMemoryTransport",
    "ProcessStage",
    "StageEnvelope",
    "StagePayload",
    "StageTransportError",
    "TCPRelayServer",
    "TCPRelayTransport",
    "TensorRef",
    "Qwen25OmniImageEncoderWorker",
    "build_stage_transport",
]
