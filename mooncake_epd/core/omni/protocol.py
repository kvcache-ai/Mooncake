"""Versioned envelopes exchanged by independent Omni stage workers."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Mapping, Tuple


@dataclass(frozen=True)
class TensorRef:
    """A transport-owned tensor reference; never a pickled tensor payload."""

    transport: str
    handle: str
    shape: Tuple[int, ...]
    dtype: str
    nbytes: int
    checksum: str

    def validate(self) -> None:
        if self.transport not in {"posix_shm", "cuda_ipc", "nvlink_intra", "tcp", "rdma"}:
            raise ValueError(f"unsupported transport label: {self.transport}")
        if not self.handle or self.nbytes <= 0 or not self.shape or not self.dtype or not self.checksum:
            raise ValueError("incomplete tensor reference")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "TensorRef":
        return cls(
            transport=str(payload["transport"]),
            handle=str(payload["handle"]),
            shape=tuple(int(value) for value in payload["shape"]),
            dtype=str(payload["dtype"]),
            nbytes=int(payload["nbytes"]),
            checksum=str(payload["checksum"]),
        )


StagePayload = Dict[str, TensorRef]


@dataclass(frozen=True)
class StageEnvelope:
    """Control metadata plus zero-payload tensor refs for one stage edge."""

    schema_version: int
    job_id: str
    stage: str
    attempt: int
    producer_generation: str
    consumer_generation: str
    deadline_monotonic: float
    payload: StagePayload
    metadata: Dict[str, Any] = field(default_factory=dict)
    # The producer retains ownership until the job coordinator observes a
    # terminal result.  Names are enough for POSIX SHM cleanup and are never
    # exposed as opaque user-provided input.
    cleanup_handles: Tuple[str, ...] = ()

    def validate(self) -> None:
        if self.schema_version != 1:
            raise ValueError(f"unsupported StageEnvelope schema={self.schema_version}")
        if not self.job_id or not self.stage or self.attempt < 0:
            raise ValueError("invalid StageEnvelope identity")
        for name, tensor_ref in self.payload.items():
            if not name:
                raise ValueError("payload tensor name must not be empty")
            tensor_ref.validate()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "job_id": self.job_id,
            "stage": self.stage,
            "attempt": self.attempt,
            "producer_generation": self.producer_generation,
            "consumer_generation": self.consumer_generation,
            "deadline_monotonic": self.deadline_monotonic,
            "payload": {name: ref.to_dict() for name, ref in self.payload.items()},
            "metadata": dict(self.metadata),
            "cleanup_handles": list(self.cleanup_handles),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "StageEnvelope":
        envelope = cls(
            schema_version=int(payload["schema_version"]),
            job_id=str(payload["job_id"]),
            stage=str(payload["stage"]),
            attempt=int(payload["attempt"]),
            producer_generation=str(payload["producer_generation"]),
            consumer_generation=str(payload["consumer_generation"]),
            deadline_monotonic=float(payload["deadline_monotonic"]),
            payload={
                str(name): TensorRef.from_dict(value)
                for name, value in dict(payload.get("payload") or {}).items()
            },
            metadata=dict(payload.get("metadata") or {}),
            cleanup_handles=tuple(str(value) for value in payload.get("cleanup_handles") or ()),
        )
        envelope.validate()
        return envelope
