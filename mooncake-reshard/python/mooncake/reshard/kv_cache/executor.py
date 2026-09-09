"""Generic synchronous TE writes with bounded batches and explicit receipts."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from typing import Protocol

from .completion import KVCacheWriterReceipt
from .resolved import KVCacheRegisteredRegion, KVCacheResolvedRuntimeBinding
from .transfer import KVCacheRuntimeTransferPlan, KVCacheWrite
from .types import require_nonempty_string


class KVCacheTransferEngine(Protocol):
    def register_memory(
        self, buffer_addr: int, capacity: int, location: str = "*"
    ) -> int: ...
    def unregister_memory(self, buffer_addr: int) -> int: ...
    def send_probe(self, peer_server_name: str) -> int: ...
    def batch_transfer_sync_write(
        self,
        target_hostname: str,
        buffers: list[int],
        peer_buffer_addresses: list[int],
        lengths: list[int],
    ) -> int: ...


class KVCacheTransferEngineExecutor:
    """Use one runtime-owned TE; registration and submission are serialized.

    The runtime must synchronize producer CUDA streams before execution, and
    keep all source pins and target reservations alive. Nonzero native results
    do NOT prove that DMA stopped: failed receipts conservatively mark the
    writer non-quiescent and this executor refuses unregister/retry afterwards.
    The runtime must drain or destroy the transport before reclaiming memory.
    Registration errors also leave the native registration state uncertain.
    This executor stays blocked after such errors; the runtime must rebuild the
    transport before replacing the executor. No in-place recovery is provided.
    """

    def __init__(
        self, engine: KVCacheTransferEngine, *, instance_id: str, endpoint: str
    ) -> None:
        require_nonempty_string(instance_id, "instance_id")
        require_nonempty_string(endpoint, "endpoint")
        self.engine = engine
        self.instance_id = instance_id
        self.endpoint = endpoint
        self._regions: dict[str, KVCacheRegisteredRegion] = {}
        self._results: dict[
            tuple[str, str], tuple[str, tuple[KVCacheWriterReceipt, ...]]
        ] = {}
        self._uncertain = False
        self._lock = threading.RLock()

    def _require_ready(self) -> None:
        if self._uncertain:
            raise RuntimeError(
                "executor requires transport recovery: native work may still be "
                "in flight or registration state is uncertain"
            )

    def register_region(self, region: KVCacheRegisteredRegion) -> None:
        if not isinstance(region, KVCacheRegisteredRegion):
            raise TypeError("region must be a KVCacheRegisteredRegion")
        if region.endpoint != self.endpoint:
            raise ValueError("region endpoint differs from local executor")
        with self._lock:
            self._require_ready()
            if region.region_id in self._regions:
                if self._regions[region.region_id] == region:
                    return
                raise ValueError("registered region_id is already bound")
            if any(
                region.address < r.address + r.nbytes
                and r.address < region.address + region.nbytes
                for r in self._regions.values()
            ):
                raise ValueError("registered regions overlap")
            # Enter quarantine before crossing the native boundary. Errors or
            # interruptions may occur after partial registration side effects.
            self._uncertain = True
            code = self.engine.register_memory(region.address, region.nbytes)
            if type(code) is not int or code != 0:
                raise RuntimeError(f"TE register_memory failed: {code}")
            self._regions[region.region_id] = region
            self._uncertain = False

    def unregister_region(self, region_id: str) -> None:
        with self._lock:
            self._require_ready()
            region = self._regions[region_id]
            self._uncertain = True
            code = self.engine.unregister_memory(region.address)
            if type(code) is not int or code != 0:
                raise RuntimeError(f"TE unregister_memory failed: {code}")
            del self._regions[region_id]
            self._uncertain = False

    def validate_local_binding(self, binding: KVCacheResolvedRuntimeBinding) -> None:
        """Check local owner registration before publishing a source/target binding."""
        if not isinstance(binding, KVCacheResolvedRuntimeBinding):
            raise TypeError("binding must be a KVCacheResolvedRuntimeBinding")
        with self._lock:
            self._require_ready()
            if binding.instance_id != self.instance_id:
                raise ValueError("binding instance differs from local executor")
            if any(self._regions.get(r.region_id) != r for r in binding.regions):
                raise ValueError("binding region is not registered with this executor")

    def execute(
        self, plan: KVCacheRuntimeTransferPlan, writer_id: str, *, warmup: bool = False
    ) -> tuple[KVCacheWriterReceipt, ...]:
        if not isinstance(plan, KVCacheRuntimeTransferPlan):
            raise TypeError("plan must be a KVCacheRuntimeTransferPlan")
        require_nonempty_string(writer_id, "writer_id")
        if type(warmup) is not bool:
            raise ValueError("warmup must be a boolean")
        with self._lock:
            self._require_ready()
            if any(
                op == plan.operation_id and digest != plan.digest
                for (op, _), (digest, _) in self._results.items()
            ):
                raise ValueError(
                    "operation_id was already used with a different transfer"
                )
            key = (plan.operation_id, writer_id)
            if key in self._results:
                digest, receipts = self._results[key]
                if digest != plan.digest:
                    raise ValueError(
                        "operation_id was already used with a different transfer"
                    )
                return receipts
            bindings = [
                b for b in plan.source_bindings if b.participant_id == writer_id
            ]
            if len(bindings) != 1:
                raise ValueError("writer is not selected by runtime transfer")
            binding = bindings[0]
            self.validate_local_binding(binding)
            selected = [w for w in plan.writes if w.source_participant_id == writer_id]
            target_ids = sorted({w.target_participant_id for w in selected})
            completed = dict.fromkeys(target_ids, 0)
            error: str | None = None
            quiesced = True
            try:
                if warmup:
                    for endpoint in sorted({w.endpoint for w in selected}):
                        code = self.engine.send_probe(endpoint)
                        if type(code) is not int or code != 0:
                            raise RuntimeError(f"TE peer warmup failed: {code}")
                for batch in _batches(
                    selected,
                    plan.limits.max_batch_operations,
                    plan.limits.max_batch_bytes,
                ):
                    # Once native submission starts, errors or interrupts cannot
                    # be interpreted as cancellation or permission to unpin.
                    quiesced = False
                    code = self.engine.batch_transfer_sync_write(
                        batch[0].endpoint,
                        [w.source_address for w in batch],
                        [w.target_address for w in batch],
                        [w.nbytes for w in batch],
                    )
                    if type(code) is not int or code != 0:
                        raise RuntimeError(f"TE batch write failed: {code}")
                    quiesced = True
                    for write in batch:
                        completed[write.target_participant_id] += write.nbytes
            except Exception as exc:  # noqa: BLE001 - native exceptions become failed receipts
                error = f"{type(exc).__name__}: {exc}"
            finally:
                if not quiesced:
                    self._uncertain = True
            receipts = tuple(
                KVCacheWriterReceipt(
                    plan.operation_id,
                    plan.digest,
                    writer_id,
                    target,
                    error is None,
                    completed[target],
                    quiesced,
                    error,
                )
                for target in target_ids
            )
            self._results[key] = (plan.digest, receipts)
            return receipts


def _batches(
    writes: list[KVCacheWrite], max_count: int, max_bytes: int
) -> Iterator[list[KVCacheWrite]]:
    batch: list[KVCacheWrite] = []
    size = 0
    for write in writes:
        if batch and (
            write.endpoint != batch[0].endpoint
            or len(batch) == max_count
            or size + write.nbytes > max_bytes
        ):
            yield batch
            batch, size = [], 0
        batch.append(write)
        size += write.nbytes
    if batch:
        yield batch
