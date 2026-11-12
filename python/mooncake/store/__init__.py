"""Select one native Mooncake Store implementation for this process."""

from __future__ import annotations

import importlib
import os
from types import ModuleType

# Centralized C++ Store exports.
_CPP_EXPORTS = frozenset(
    {
        "ReplicateConfig",
        "CentralizedReadConfig",
        "ReplicaStatus",
        "MemoryDescriptor",
        "DiskDescriptor",
        "ReplicaDescriptor",
        "Descriptor",
        "TaskType",
        "TaskStatus",
        "QueryTaskResponse",
        "BufferHandle",
        "MooncakeHostMemAllocator",
        "MooncakeDistributedStore",
        "bind_to_numa_node",
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

# P2P C++ Store exports.
_P2P_EXPORTS = frozenset(
    {
        "Descriptor",
        "BufferHandle",
        "MooncakeHostMemAllocator",
        "MooncakeDistributedStore",
        "bind_to_numa_node",
        "P2PClientSelectionStrategy",
        "P2PReadRouteConfig",
        "TransferDirectionMode",
        "P2PWriteRouteConfig",
        "P2PRouteDescriptor",
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

    # The native build selects the architecture; only its exports differ here.
    _ARCHITECTURE = _IMPLEMENTATION._store_architecture
    if _ARCHITECTURE == "centralized":
        _EXPORTS = _CPP_EXPORTS
    elif _ARCHITECTURE == "p2p":
        _EXPORTS = _P2P_EXPORTS
    else:
        raise ImportError(
            "mooncake._store has an invalid Store architecture; "
            f"got {_ARCHITECTURE!r}"
        )
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
        name in (_CPP_EXPORTS - _RS_EXPORTS) or name in (_P2P_EXPORTS - _RS_EXPORTS)
    ):
        raise AttributeError(
            f"{name} is a C++ Store API and is unavailable with "
            "MOONCAKE_STORE_BACKEND='rs'"
        )
    raise AttributeError(name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | _EXPORTS)
