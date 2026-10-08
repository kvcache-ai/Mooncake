"""Select one native Mooncake Store implementation for this process."""

from __future__ import annotations

import importlib
import os
from types import ModuleType

_CPP_EXPORTS = frozenset(
    {
        "RangedReadSnapshot",
        "ObjectDataType",
        "SoftPinAction",
        "ReplicateConfig",
        "ReplicaStatus",
        "MemoryDescriptor",
        "DiskDescriptor",
        "ReplicaDescriptor",
        "Descriptor",
        "TaskType",
        "TaskStatus",
        "QueryTaskResponse",
        "BufferHandle",
        "BufferLeaseView",
        "BufferLease",
        "BufferPool",
        "RegisteredBufferPool",
        "RegisteredBufferLease",
        "RegisteredBufferLeaseView",
        "MooncakeHostMemAllocator",
        "MooncakeDistributedNoFRegister",
        "MooncakeDistributedStore",
        "EngramStoreConfig",
        "EngramStore",
        "bind_to_numa_node",
        "get_alloc_func_addr",
        "get_free_func_addr",
        "UNKNOWN",
        "KVCACHE",
        "TENSOR",
        "WEIGHT",
        "SAMPLE",
        "ACTIVATION",
        "GRADIENT",
        "OPTIMIZER_STATE",
        "METADATA",
        "GENERAL",
        "UNDEFINED",
        "INITIALIZED",
        "PROCESSING",
        "COMPLETE",
        "REMOVED",
        "FAILED",
        "REPLICA_COPY",
        "REPLICA_MOVE",
        "PENDING",
        "SUCCESS",
    }
)

_CPP_INTERNAL_EXPORTS = frozenset(
    {
        "_serialize_tensor",
        "_tensor_metadata_size",
        "_deserialize_tensor",
        "_get_pyclient_from_wrapper",
    }
)

_RS_EXPORTS = frozenset(
    {
        "MooncakeDistributedStore",
        "MooncakeHostMemAllocator",
        "BufferPool",
        "RegisteredBufferPool",
        "ReplicateConfig",
        "init_tracing",
        "metrics_server_address",
        "metrics_text",
        "start_metrics_server",
        "stop_metrics_server",
        "ParallelAxis",
        "TensorParallelism",
        "ReadTarget",
        "AXIS_DP",
        "AXIS_TP",
        "AXIS_EP",
        "AXIS_PP",
        "READ_MODE_AS_STORED",
        "READ_MODE_SHARD",
        "READ_MODE_FULL",
    }
)

_BACKEND = os.environ.get("MOONCAKE_STORE_BACKEND", "cpp")
if _BACKEND == "cpp":
    try:
        _IMPLEMENTATION: ModuleType = importlib.import_module("mooncake._store")
    except ModuleNotFoundError as exc:
        if exc.name != "mooncake._store":
            raise
        raise ImportError(
            "MOONCAKE_STORE_BACKEND='cpp' requires a Mooncake wheel built with "
            "WITH_STORE=ON"
        ) from exc
    _EXPORTS = _CPP_EXPORTS
elif _BACKEND == "rs":
    try:
        _IMPLEMENTATION = importlib.import_module("mooncake.store.rs.store")
    except ModuleNotFoundError as exc:
        if exc.name != "mooncake._store_rs":
            raise
        raise ImportError(
            "MOONCAKE_STORE_BACKEND='rs' requires a Mooncake wheel built with "
            "WITH_STORE_RS=ON"
        ) from exc
    _EXPORTS = _RS_EXPORTS
else:
    raise ValueError(
        "MOONCAKE_STORE_BACKEND must be exactly 'cpp' or 'rs'; " f"got {_BACKEND!r}"
    )

__all__ = sorted(_EXPORTS)


def __getattr__(name: str):
    if name in _EXPORTS:
        return getattr(_IMPLEMENTATION, name)
    if _BACKEND == "rs" and (
        name in (_CPP_EXPORTS - _RS_EXPORTS) or name in _CPP_INTERNAL_EXPORTS
    ):
        raise AttributeError(
            f"{name} is a C++ Store API and is unavailable with "
            "MOONCAKE_STORE_BACKEND='rs'"
        )
    raise AttributeError(name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | _EXPORTS)
