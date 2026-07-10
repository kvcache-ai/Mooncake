"""Transfer policy: 4-dimensional parameterization + auto-selection.

A ``TransferPolicy`` bundles four orthogonal decisions:

- ``mode``        -- how data moves (stream / pull / push_batch / shm).
- ``compress``    -- which codec (none / fp8 / per_level / cacheGen).
- ``precision``   -- target tensor dtype (bf16 / fp8 / q4).
- ``prefetch``    -- whether to anticipate the next request.

Given a ``Channel`` (E->P, P->D, A2A, Offload) and an ``HwCaps`` probe,
``default_policy_for`` picks a sensible combination. Callers may override
any dimension explicitly. The engine reads the policy and dispatches to
the matching implementation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Channel(str, Enum):
    ENCODER_TO_PREFILL = "E2P"
    PREFILL_TO_DECODE = "P2D"
    AGENT_TO_AGENT = "A2A"
    OFFLOAD = "Offload"


class Mode(str, Enum):
    STREAM = "stream"          # overlapping transfer + compute
    PULL = "pull"              # on-demand fetch
    PUSH_BATCH = "push_batch"  # batched one-shot send
    SHM = "shm"                # CUDA IPC / shared memory (same host)


class CompressMode(str, Enum):
    NONE = "none"
    FP8 = "fp8"
    PER_LEVEL = "per_level"    # DeepStack: per-layer codec
    CACHEGEN = "cacheGen"      # streaming quantized codec


class Precision(str, Enum):
    BF16 = "bf16"
    FP8 = "fp8"
    Q4 = "q4"


class Prefetch(str, Enum):
    NONE = "none"
    NEXT_STEP = "next_step"
    SPECULATIVE = "speculative"


@dataclass
class HwCaps:
    """Hardware capabilities probe, filled at startup."""

    has_rdma: bool = False
    has_gpudirect: bool = False
    has_nvlink: bool = False
    has_cuda_ipc: bool = True
    num_gpus: int = 1
    nic_bandwidth_gbps: float = 25.0
    pcie_bandwidth_gbps: float = 32.0

    @classmethod
    def detect(cls) -> "HwCaps":
        """Best-effort detection; never raises."""
        import torch
        caps = cls()
        caps.num_gpus = torch.cuda.device_count() if torch.cuda.is_available() else 0
        try:
            import subprocess
            out = subprocess.check_output(
                ["ibv_devices"], stderr=subprocess.STDOUT, timeout=2,
            ).decode()
            caps.has_rdma = "node GUID" in out or "mlx" in out.lower()
        except Exception:
            caps.has_rdma = False
        # GPUDirect typically requires both RDMA and a compatible driver
        caps.has_gpudirect = caps.has_rdma and caps.num_gpus > 0
        return caps


@dataclass
class TransferPolicy:
    """A concrete policy the engine can execute."""

    mode: Mode
    compress: CompressMode = CompressMode.NONE
    precision: Precision = Precision.BF16
    prefetch: Prefetch = Prefetch.NONE
    # Optional override for target channel (for logging/metrics).
    channel: Optional[Channel] = None
    extra: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "mode": self.mode.value,
            "compress": self.compress.value,
            "precision": self.precision.value,
            "prefetch": self.prefetch.value,
            "channel": self.channel.value if self.channel else None,
        }


# ---------------------------------------------------------------------------
# Default policy selection per channel
# ---------------------------------------------------------------------------
def default_policy_for(
    channel: Channel,
    hw: Optional[HwCaps] = None,
    same_host: bool = True,
) -> TransferPolicy:
    """Pick a reasonable default policy for a channel given the hardware."""
    hw = hw or HwCaps()
    if channel is Channel.ENCODER_TO_PREFILL:
        # FeatureBundle is small but latency-sensitive.
        if same_host and hw.has_cuda_ipc:
            return TransferPolicy(Mode.SHM, CompressMode.NONE, Precision.BF16,
                                  Prefetch.NEXT_STEP,
                                  channel=channel)
        return TransferPolicy(Mode.STREAM, CompressMode.PER_LEVEL, Precision.BF16,
                              Prefetch.NEXT_STEP,
                              channel=channel)

    if channel is Channel.PREFILL_TO_DECODE:
        # KV pages: large, bandwidth-sensitive.
        if hw.has_rdma and not same_host:
            return TransferPolicy(Mode.PUSH_BATCH, CompressMode.NONE, Precision.BF16,
                                  channel=channel)
        return TransferPolicy(Mode.STREAM, CompressMode.CACHEGEN, Precision.BF16,
                              channel=channel)

    if channel is Channel.AGENT_TO_AGENT:
        # Metadata + KV increment, pull on demand.
        return TransferPolicy(Mode.PULL, CompressMode.NONE, Precision.BF16,
                              Prefetch.SPECULATIVE,
                              channel=channel)

    if channel is Channel.OFFLOAD:
        # Push to host memory pool / MooncakeStore.
        return TransferPolicy(Mode.PUSH_BATCH, CompressMode.NONE, Precision.BF16,
                              channel=channel)

    raise ValueError(f"unknown channel: {channel}")
