"""Compatibility wrapper over the unified transfer engine.

Historically the repo had two transfer stacks:

- ``core.transfer.engine.TransferEngine`` (new mainline path)
- ``core.transfer_engine.MooncakeTransferWrapper`` (older façade)

This module now keeps the old import surface but forwards all real work to the
new engine so benchmarks / demos / legacy pipeline code share one data plane.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import torch

from .transfer import Channel, Mode, TransferEngine, TransferPolicy


@dataclass
class TransferStats:
    """Compatibility stats view for legacy callers."""

    _engine: TransferEngine

    @property
    def avg_bandwidth_gbps(self) -> float:
        snap = self.snapshot()
        total_bytes = int(snap.get("total_bytes", 0) or 0)
        total_time_ms = float(snap.get("total_time_ms", 0.0) or 0.0)
        if total_time_ms <= 0:
            return 0.0
        return (total_bytes * 8) / (total_time_ms / 1000.0) / 1e9

    def record(self, nbytes: int, time_ms: float) -> None:
        self._engine.stats.record("compat", int(nbytes), float(time_ms))

    def summary(self) -> str:
        snap = self.snapshot()
        return (
            f"Transfers: {snap['total_transfers']}, "
            f"Total: {snap['total_bytes'] / 1e9:.3f} GB, "
            f"Avg BW: {snap['avg_bandwidth_gbps']:.3f} Gbps, "
            f"Peak BW: {snap['peak_bandwidth_gbps']:.3f} Gbps"
        )

    def snapshot(self) -> Dict[str, Any]:
        raw = self._engine.stats.snapshot()
        total_bytes = 0
        total_transfers = 0
        peak_bandwidth = 0.0
        total_time_ms = 0.0
        for channel in raw.values():
            total_bytes += int(channel.get("total_bytes", 0) or 0)
            total_transfers += int(channel.get("transfers", 0) or 0)
            peak_bandwidth = max(
                peak_bandwidth,
                float(channel.get("peak_bandwidth_gbps", 0.0) or 0.0),
            )
            avg_bandwidth = float(channel.get("avg_bandwidth_gbps", 0.0) or 0.0)
            if avg_bandwidth > 0:
                total_time_ms += (
                    float(channel.get("total_bytes", 0) or 0) * 8 / avg_bandwidth / 1e9 * 1000.0
                )
        avg_bandwidth = (
            (total_bytes * 8) / (total_time_ms / 1000.0) / 1e9
            if total_time_ms > 0
            else 0.0
        )
        return {
            "total_bytes": total_bytes,
            "total_transfers": total_transfers,
            "total_time_ms": total_time_ms,
            "avg_bandwidth_gbps": avg_bandwidth,
            "peak_bandwidth_gbps": peak_bandwidth,
            "channels": raw,
        }


class MooncakeTransferWrapper:
    """Compatibility façade backed by ``core.transfer.engine.TransferEngine``."""

    def __init__(
        self,
        local_hostname: str = "localhost",
        metadata_server: str = "P2PHANDSHAKE",
        protocol: str = "tcp",
        device_name: str = "",
    ):
        self.local_hostname = local_hostname
        self.metadata_server = metadata_server
        self.protocol = protocol
        self.device_name = device_name
        self._engine = TransferEngine(
            protocol=protocol,
            local_hostname=local_hostname,
            metadata_server=metadata_server,
            device_name=device_name,
        )
        self.stats = TransferStats(self._engine)

    def initialize(self) -> None:
        self._engine.initialize()

    def transfer_tensor(
        self,
        tensor: torch.Tensor,
        target_device: Optional[torch.device] = None,
    ) -> torch.Tensor:
        if target_device is None:
            return tensor
        return self._engine.transfer_tensor(
            tensor,
            target_device,
            TransferPolicy(mode=Mode.STREAM, channel=Channel.PREFILL_TO_DECODE),
        )

    def transfer_hidden_states(
        self,
        hidden_states: torch.Tensor,
        metadata: Dict[str, Any],
        target_device: Optional[torch.device] = None,
    ) -> Tuple[torch.Tensor, Dict[str, Any]]:
        transferred = self.transfer_tensor(hidden_states, target_device)
        return transferred, metadata

    def transfer_kv_cache(
        self,
        kv_cache: Tuple[torch.Tensor, torch.Tensor],
        target_device: Optional[torch.device] = None,
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        key_cache, value_cache = kv_cache
        return (
            self.transfer_tensor(key_cache, target_device),
            self.transfer_tensor(value_cache, target_device),
        )

    def get_stats(self) -> TransferStats:
        return self.stats

    def reset_stats(self) -> None:
        self._engine.stats._records.clear()  # noqa: SLF001 - compatibility reset

    def shutdown(self) -> None:
        self._engine.shutdown()
