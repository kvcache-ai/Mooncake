"""Transfer layer: protocol-agnostic engine + policy selection.

This layer wraps the Mooncake Transfer Engine and the Mooncake Distributed
Store behind a single ``transfer(refs, plan, target)`` primitive. Every
channel in the RFC -- E->P, P->D, A2A, Offload -- is a *view* onto the
same primitive with a different policy.
"""

from .policy import (
    Channel,
    CompressMode,
    HwCaps,
    Mode,
    Precision,
    Prefetch,
    TransferPolicy,
    default_policy_for,
)
from .engine import (
    DirectPeerBuffer,
    FeatureBundlePeerBufferPlan,
    FeatureBundlePeerBufferResult,
    FeatureTensorPeerTarget,
    LayerTransferBatch,
    PeerTransferDescriptor,
    PeerTransferPlan,
    PeerTransferResult,
    TransferEngine,
    TransferHandle,
    TransferStats,
)

__all__ = [
    "Channel",
    "CompressMode",
    "DirectPeerBuffer",
    "FeatureBundlePeerBufferPlan",
    "FeatureBundlePeerBufferResult",
    "FeatureTensorPeerTarget",
    "HwCaps",
    "LayerTransferBatch",
    "Mode",
    "PeerTransferDescriptor",
    "PeerTransferPlan",
    "PeerTransferResult",
    "Precision",
    "Prefetch",
    "TransferEngine",
    "TransferHandle",
    "TransferPolicy",
    "TransferStats",
    "default_policy_for",
]
