"""Pure helpers for serving-path layered KV transfer scheduling."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, Hashable, List, Sequence, Tuple


_RETRYABLE_ERRNOS = {
    11,   # EAGAIN / EWOULDBLOCK
    16,   # EBUSY
    32,   # EPIPE
    100,  # ENETDOWN
    101,  # ENETUNREACH
    104,  # ECONNRESET
    105,  # ENOBUFS
    110,  # ETIMEDOUT
    111,  # ECONNREFUSED
    113,  # EHOSTUNREACH
}
_NON_RETRYABLE_TRANSFER_MARKERS = (
    "invalid buffer",
    "invalid address",
    "out of bounds",
    "out-of-bounds",
    "not registered",
    "memory registration",
    "ownership",
    "owner mismatch",
    "permission denied",
    "unauthorized",
    "lease expired",
    "layout mismatch",
    "length mismatch",
    "unaligned",
)
_RETRYABLE_TRANSFER_MARKERS = (
    "timed out",
    "timeout",
    "temporarily unavailable",
    "resource temporarily unavailable",
    "try again",
    "connection reset",
    "connection refused",
    "connection aborted",
    "broken pipe",
    "network is down",
    "network is unreachable",
    "host is unreachable",
    "no buffer space",
    "socket busy",
)


def is_retryable_transfer_failure(
    ret_code: int,
    error_message: str | None = None,
) -> bool:
    """Conservatively classify transport failures that are safe to retry.

    Retrying a descriptor with invalid registration, ownership, layout, or
    bounds cannot repair the request and only adds tail latency.  Unknown
    Mooncake error codes are therefore non-retryable unless the attached
    exception text identifies a transient network/resource condition.
    """

    code = int(ret_code)
    if code == 0:
        return False
    message = str(error_message or "").strip().lower()
    if any(marker in message for marker in _NON_RETRYABLE_TRANSFER_MARKERS):
        return False
    if abs(code) in _RETRYABLE_ERRNOS:
        return True
    return any(marker in message for marker in _RETRYABLE_TRANSFER_MARKERS)


@dataclass(frozen=True)
class TransferDescriptorCoalesceResult:
    """A coalesced descriptor batch plus auditable reduction statistics."""

    src_ptrs: List[int]
    dst_ptrs: List[int]
    lengths: List[int]
    descriptor_paths: List[str]
    input_descriptors: int
    output_descriptors: int
    coalesced_descriptors: int
    total_bytes: int


@dataclass
class LayeredTransferWorkerMeta:
    grouped_batches: int = 0
    grouped_bytes: int = 0
    grouped_descriptors: int = 0
    failed_batches: int = 0
    peer_buffer_batches: int = 0
    peer_buffer_bytes: int = 0
    fallback_batches: int = 0
    fallback_bytes: int = 0
    accumulated_group_delay_ms: float = 0.0
    received_group_batches: int = 0
    received_finished_reqs: int = 0
    layer_wait_calls: int = 0
    layer_wait_ms: float = 0.0
    receive_failures: int = 0
    transfer_attempts: int = 0
    transfer_successes: int = 0
    transfer_bytes: int = 0
    transfer_elapsed_ms: float = 0.0
    transfer_attempt_elapsed_ms: float = 0.0
    descriptor_build_calls: int = 0
    descriptor_build_input_descriptors: int = 0
    descriptor_build_output_descriptors: int = 0
    coalesced_descriptors: int = 0
    descriptor_build_ms: float = 0.0
    topology_incarnation_observations: int = 0
    topology_incarnation_refreshes: int = 0
    topology_incarnation_pending_waits: int = 0
    topology_incarnation_wait_ms: float = 0.0
    backend_counts: Dict[str, int] = field(default_factory=dict)
    backend_bytes: Dict[str, int] = field(default_factory=dict)
    backend_elapsed_ms: Dict[str, float] = field(default_factory=dict)
    backend_failures: Dict[str, int] = field(default_factory=dict)

    def aggregate(self, other: "LayeredTransferWorkerMeta") -> "LayeredTransferWorkerMeta":
        merged = LayeredTransferWorkerMeta(
            grouped_batches=self.grouped_batches + other.grouped_batches,
            grouped_bytes=self.grouped_bytes + other.grouped_bytes,
            grouped_descriptors=self.grouped_descriptors + other.grouped_descriptors,
            failed_batches=self.failed_batches + other.failed_batches,
            peer_buffer_batches=self.peer_buffer_batches + other.peer_buffer_batches,
            peer_buffer_bytes=self.peer_buffer_bytes + other.peer_buffer_bytes,
            fallback_batches=self.fallback_batches + other.fallback_batches,
            fallback_bytes=self.fallback_bytes + other.fallback_bytes,
            accumulated_group_delay_ms=self.accumulated_group_delay_ms + other.accumulated_group_delay_ms,
            received_group_batches=self.received_group_batches + other.received_group_batches,
            received_finished_reqs=self.received_finished_reqs + other.received_finished_reqs,
            layer_wait_calls=self.layer_wait_calls + other.layer_wait_calls,
            layer_wait_ms=self.layer_wait_ms + other.layer_wait_ms,
            receive_failures=self.receive_failures + other.receive_failures,
            transfer_attempts=self.transfer_attempts + other.transfer_attempts,
            transfer_successes=self.transfer_successes + other.transfer_successes,
            transfer_bytes=self.transfer_bytes + other.transfer_bytes,
            transfer_elapsed_ms=self.transfer_elapsed_ms + other.transfer_elapsed_ms,
            transfer_attempt_elapsed_ms=(
                self.transfer_attempt_elapsed_ms + other.transfer_attempt_elapsed_ms
            ),
            descriptor_build_calls=(
                self.descriptor_build_calls + other.descriptor_build_calls
            ),
            descriptor_build_input_descriptors=(
                self.descriptor_build_input_descriptors
                + other.descriptor_build_input_descriptors
            ),
            descriptor_build_output_descriptors=(
                self.descriptor_build_output_descriptors
                + other.descriptor_build_output_descriptors
            ),
            coalesced_descriptors=(
                self.coalesced_descriptors + other.coalesced_descriptors
            ),
            descriptor_build_ms=self.descriptor_build_ms + other.descriptor_build_ms,
            topology_incarnation_observations=(
                self.topology_incarnation_observations
                + other.topology_incarnation_observations
            ),
            topology_incarnation_refreshes=(
                self.topology_incarnation_refreshes
                + other.topology_incarnation_refreshes
            ),
            topology_incarnation_pending_waits=(
                self.topology_incarnation_pending_waits
                + other.topology_incarnation_pending_waits
            ),
            topology_incarnation_wait_ms=(
                self.topology_incarnation_wait_ms
                + other.topology_incarnation_wait_ms
            ),
            backend_counts=dict(self.backend_counts),
            backend_bytes=dict(self.backend_bytes),
            backend_elapsed_ms=dict(self.backend_elapsed_ms),
            backend_failures=dict(self.backend_failures),
        )
        for field_name in (
            "backend_counts",
            "backend_bytes",
            "backend_elapsed_ms",
            "backend_failures",
        ):
            target = getattr(merged, field_name)
            for key, value in getattr(other, field_name).items():
                target[key] = target.get(key, 0) + value
        return merged

    def is_empty(self) -> bool:
        return (
            self.grouped_batches == 0
            and self.grouped_bytes == 0
            and self.grouped_descriptors == 0
            and self.failed_batches == 0
            and self.peer_buffer_batches == 0
            and self.peer_buffer_bytes == 0
            and self.fallback_batches == 0
            and self.fallback_bytes == 0
            and self.accumulated_group_delay_ms == 0.0
            and self.received_group_batches == 0
            and self.received_finished_reqs == 0
            and self.layer_wait_calls == 0
            and self.layer_wait_ms == 0.0
            and self.receive_failures == 0
            and self.transfer_attempts == 0
            and self.transfer_successes == 0
            and self.transfer_bytes == 0
            and self.transfer_elapsed_ms == 0.0
            and self.transfer_attempt_elapsed_ms == 0.0
            and self.descriptor_build_calls == 0
            and self.descriptor_build_input_descriptors == 0
            and self.descriptor_build_output_descriptors == 0
            and self.coalesced_descriptors == 0
            and self.descriptor_build_ms == 0.0
            and self.topology_incarnation_observations == 0
            and self.topology_incarnation_refreshes == 0
            and self.topology_incarnation_pending_waits == 0
            and self.topology_incarnation_wait_ms == 0.0
            and not self.backend_counts
            and not self.backend_bytes
            and not self.backend_elapsed_ms
            and not self.backend_failures
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "grouped_batches": int(self.grouped_batches),
            "grouped_bytes": int(self.grouped_bytes),
            "grouped_descriptors": int(self.grouped_descriptors),
            "failed_batches": int(self.failed_batches),
            "peer_buffer_batches": int(self.peer_buffer_batches),
            "peer_buffer_bytes": int(self.peer_buffer_bytes),
            "fallback_batches": int(self.fallback_batches),
            "fallback_bytes": int(self.fallback_bytes),
            "accumulated_group_delay_ms": float(self.accumulated_group_delay_ms),
            "received_group_batches": int(self.received_group_batches),
            "received_finished_reqs": int(self.received_finished_reqs),
            "layer_wait_calls": int(self.layer_wait_calls),
            "layer_wait_ms": float(self.layer_wait_ms),
            "receive_failures": int(self.receive_failures),
            "transfer_attempts": int(self.transfer_attempts),
            "transfer_successes": int(self.transfer_successes),
            "transfer_bytes": int(self.transfer_bytes),
            "transfer_elapsed_ms": float(self.transfer_elapsed_ms),
            "transfer_attempt_elapsed_ms": float(self.transfer_attempt_elapsed_ms),
            "descriptor_build_calls": int(self.descriptor_build_calls),
            "descriptor_build_input_descriptors": int(
                self.descriptor_build_input_descriptors
            ),
            "descriptor_build_output_descriptors": int(
                self.descriptor_build_output_descriptors
            ),
            "coalesced_descriptors": int(self.coalesced_descriptors),
            "descriptor_build_ms": float(self.descriptor_build_ms),
            "topology_incarnation_observations": int(
                self.topology_incarnation_observations
            ),
            "topology_incarnation_refreshes": int(
                self.topology_incarnation_refreshes
            ),
            "topology_incarnation_pending_waits": int(
                self.topology_incarnation_pending_waits
            ),
            "topology_incarnation_wait_ms": float(
                self.topology_incarnation_wait_ms
            ),
            "descriptor_build_ms_avg": (
                float(self.descriptor_build_ms) / float(self.descriptor_build_calls)
                if self.descriptor_build_calls > 0
                else None
            ),
            "descriptor_reduction_ratio": (
                float(self.coalesced_descriptors)
                / float(self.descriptor_build_input_descriptors)
                if self.descriptor_build_input_descriptors > 0
                else None
            ),
            "descriptors_per_mb": (
                float(self.grouped_descriptors)
                / (float(self.grouped_bytes) / float(1024 * 1024))
                if self.grouped_bytes > 0
                else None
            ),
            "transfer_elapsed_ms_avg": (
                float(self.transfer_elapsed_ms) / float(self.transfer_successes)
                if self.transfer_successes > 0
                else None
            ),
            "transfer_attempt_elapsed_ms_avg": (
                float(self.transfer_attempt_elapsed_ms) / float(self.transfer_attempts)
                if self.transfer_attempts > 0
                else None
            ),
            "transfer_bandwidth_gbps": self._bandwidth_gbps(
                self.transfer_bytes,
                self.transfer_elapsed_ms,
            ),
            "backend_counts": {str(k): int(v) for k, v in self.backend_counts.items()},
            "backend_bytes": {str(k): int(v) for k, v in self.backend_bytes.items()},
            "backend_elapsed_ms": {
                str(k): float(v) for k, v in self.backend_elapsed_ms.items()
            },
            "backend_failures": {
                str(k): int(v) for k, v in self.backend_failures.items()
            },
            "backend_bandwidth_gbps": {
                str(backend): self._bandwidth_gbps(
                    int(byte_count),
                    float(self.backend_elapsed_ms.get(backend, 0.0) or 0.0),
                )
                for backend, byte_count in self.backend_bytes.items()
            },
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any] | None) -> "LayeredTransferWorkerMeta":
        payload = dict(payload or {})
        return cls(
            grouped_batches=int(payload.get("grouped_batches", 0) or 0),
            grouped_bytes=int(payload.get("grouped_bytes", 0) or 0),
            grouped_descriptors=int(payload.get("grouped_descriptors", 0) or 0),
            failed_batches=int(payload.get("failed_batches", 0) or 0),
            peer_buffer_batches=int(payload.get("peer_buffer_batches", 0) or 0),
            peer_buffer_bytes=int(payload.get("peer_buffer_bytes", 0) or 0),
            fallback_batches=int(payload.get("fallback_batches", 0) or 0),
            fallback_bytes=int(payload.get("fallback_bytes", 0) or 0),
            accumulated_group_delay_ms=float(payload.get("accumulated_group_delay_ms", 0.0) or 0.0),
            received_group_batches=int(payload.get("received_group_batches", 0) or 0),
            received_finished_reqs=int(payload.get("received_finished_reqs", 0) or 0),
            layer_wait_calls=int(payload.get("layer_wait_calls", 0) or 0),
            layer_wait_ms=float(payload.get("layer_wait_ms", 0.0) or 0.0),
            receive_failures=int(payload.get("receive_failures", 0) or 0),
            transfer_attempts=int(payload.get("transfer_attempts", 0) or 0),
            transfer_successes=int(payload.get("transfer_successes", 0) or 0),
            transfer_bytes=int(payload.get("transfer_bytes", 0) or 0),
            transfer_elapsed_ms=float(payload.get("transfer_elapsed_ms", 0.0) or 0.0),
            transfer_attempt_elapsed_ms=float(
                payload.get("transfer_attempt_elapsed_ms", 0.0) or 0.0
            ),
            descriptor_build_calls=int(
                payload.get("descriptor_build_calls", 0) or 0
            ),
            descriptor_build_input_descriptors=int(
                payload.get("descriptor_build_input_descriptors", 0) or 0
            ),
            descriptor_build_output_descriptors=int(
                payload.get("descriptor_build_output_descriptors", 0) or 0
            ),
            coalesced_descriptors=int(
                payload.get("coalesced_descriptors", 0) or 0
            ),
            descriptor_build_ms=float(
                payload.get("descriptor_build_ms", 0.0) or 0.0
            ),
            topology_incarnation_observations=int(
                payload.get("topology_incarnation_observations", 0) or 0
            ),
            topology_incarnation_refreshes=int(
                payload.get("topology_incarnation_refreshes", 0) or 0
            ),
            topology_incarnation_pending_waits=int(
                payload.get("topology_incarnation_pending_waits", 0) or 0
            ),
            topology_incarnation_wait_ms=float(
                payload.get("topology_incarnation_wait_ms", 0.0) or 0.0
            ),
            backend_counts={
                str(key): int(value)
                for key, value in dict(payload.get("backend_counts") or {}).items()
            },
            backend_bytes={
                str(key): int(value)
                for key, value in dict(payload.get("backend_bytes") or {}).items()
            },
            backend_elapsed_ms={
                str(key): float(value)
                for key, value in dict(payload.get("backend_elapsed_ms") or {}).items()
            },
            backend_failures={
                str(key): int(value)
                for key, value in dict(payload.get("backend_failures") or {}).items()
            },
        )

    @staticmethod
    def _bandwidth_gbps(byte_count: int, elapsed_ms: float) -> float | None:
        if int(byte_count) <= 0 or float(elapsed_ms) <= 0.0:
            return None
        return float(byte_count) * 8.0 / (float(elapsed_ms) * 1_000_000.0)


def coalesce_transfer_descriptors(
    src_ptrs: Sequence[int],
    dst_ptrs: Sequence[int],
    lengths: Sequence[int],
    *,
    coalesce_keys: Sequence[Hashable] | None = None,
    descriptor_paths: Sequence[str] | None = None,
) -> TransferDescriptorCoalesceResult:
    """Merge adjacent copies only inside an explicit registration provenance.

    Pointer adjacency alone is insufficient because two numerically adjacent
    addresses can belong to different registered tensors or leases.  Callers
    must therefore provide a stable ``coalesce_key`` describing the shared
    source/destination registration boundary.  Without keys this function is
    deliberately a no-op.

    Descriptors sharing a key/path may be interleaved by request.  They are
    sorted within that provenance bucket, then merged only when *both* source
    and destination ranges are exactly adjacent.  Total bytes are preserved.
    """

    if not (len(src_ptrs) == len(dst_ptrs) == len(lengths)):
        raise ValueError("src_ptrs, dst_ptrs and lengths must have identical lengths")
    descriptor_count = len(src_ptrs)
    if coalesce_keys is not None and len(coalesce_keys) != descriptor_count:
        raise ValueError("coalesce_keys must match descriptor count")
    if descriptor_paths is not None and len(descriptor_paths) != descriptor_count:
        raise ValueError("descriptor_paths must match descriptor count")

    normalized_lengths = [int(value) for value in lengths]
    if any(value <= 0 for value in normalized_lengths):
        raise ValueError("transfer descriptor lengths must be positive")
    normalized_src = [int(value) for value in src_ptrs]
    normalized_dst = [int(value) for value in dst_ptrs]
    normalized_paths = (
        [str(value) for value in descriptor_paths]
        if descriptor_paths is not None
        else []
    )
    total_bytes = sum(normalized_lengths)

    if descriptor_count == 0 or coalesce_keys is None:
        return TransferDescriptorCoalesceResult(
            src_ptrs=normalized_src,
            dst_ptrs=normalized_dst,
            lengths=normalized_lengths,
            descriptor_paths=normalized_paths,
            input_descriptors=descriptor_count,
            output_descriptors=descriptor_count,
            coalesced_descriptors=0,
            total_bytes=total_bytes,
        )

    buckets: Dict[Tuple[Hashable, str | None], List[Tuple[int, int, int, int]]] = {}
    for index, (src, dst, size, key) in enumerate(
        zip(normalized_src, normalized_dst, normalized_lengths, coalesce_keys)
    ):
        path = normalized_paths[index] if normalized_paths else None
        try:
            bucket = (key, path)
            hash(bucket)
        except TypeError as exc:
            raise TypeError("coalesce_keys must contain hashable values") from exc
        buckets.setdefault(bucket, []).append((src, dst, size, index))

    out_src: List[int] = []
    out_dst: List[int] = []
    out_lengths: List[int] = []
    out_paths: List[str] = []
    for (_, path), entries in buckets.items():
        bucket_start = len(out_src)
        entries.sort(key=lambda item: (item[0], item[1], item[3]))
        for src, dst, size, _ in entries:
            if (
                len(out_src) > bucket_start
                and (not out_paths or out_paths[-1] == path)
                and out_src[-1] + out_lengths[-1] == src
                and out_dst[-1] + out_lengths[-1] == dst
            ):
                out_lengths[-1] += size
                continue
            out_src.append(src)
            out_dst.append(dst)
            out_lengths.append(size)
            if normalized_paths:
                out_paths.append(str(path))

    output_descriptors = len(out_src)
    if sum(out_lengths) != total_bytes:
        raise AssertionError("descriptor coalescing changed total transfer bytes")
    return TransferDescriptorCoalesceResult(
        src_ptrs=out_src,
        dst_ptrs=out_dst,
        lengths=out_lengths,
        descriptor_paths=out_paths,
        input_descriptors=descriptor_count,
        output_descriptors=output_descriptors,
        coalesced_descriptors=descriptor_count - output_descriptors,
        total_bytes=total_bytes,
    )


def infer_group_count(total_regions: int, layers_per_group: int) -> int:
    total_regions = max(1, int(total_regions))
    layers_per_group = max(1, int(layers_per_group))
    return max(1, math.ceil(total_regions / layers_per_group))


def infer_descriptors_per_group(
    total_descriptors: int,
    *,
    total_regions: int,
    layers_per_group: int,
) -> int:
    total_descriptors = max(1, int(total_descriptors))
    groups = infer_group_count(total_regions, layers_per_group)
    return max(1, math.ceil(total_descriptors / groups))


def chunk_transfer_descriptors(
    src_ptrs: Sequence[int],
    dst_ptrs: Sequence[int],
    lengths: Sequence[int],
    *,
    descriptors_per_group: int,
    max_group_bytes: int = 0,
) -> List[Tuple[List[int], List[int], List[int]]]:
    if not (len(src_ptrs) == len(dst_ptrs) == len(lengths)):
        raise ValueError("src_ptrs, dst_ptrs and lengths must have identical lengths")
    if not src_ptrs:
        return []
    descriptors_per_group = max(1, int(descriptors_per_group))
    max_group_bytes = max(0, int(max_group_bytes))

    groups: List[Tuple[List[int], List[int], List[int]]] = []
    cur_src: List[int] = []
    cur_dst: List[int] = []
    cur_len: List[int] = []
    cur_bytes = 0

    for src, dst, size in zip(src_ptrs, dst_ptrs, lengths):
        next_would_overflow = (
            bool(cur_src)
            and (
                len(cur_src) >= descriptors_per_group
                or (max_group_bytes > 0 and cur_bytes + int(size) > max_group_bytes)
            )
        )
        if next_would_overflow:
            groups.append((cur_src, cur_dst, cur_len))
            cur_src, cur_dst, cur_len = [], [], []
            cur_bytes = 0
        cur_src.append(int(src))
        cur_dst.append(int(dst))
        cur_len.append(int(size))
        cur_bytes += int(size)

    if cur_src:
        groups.append((cur_src, cur_dst, cur_len))
    return groups
