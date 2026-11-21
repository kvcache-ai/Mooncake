"""Select one native Mooncake Store implementation for this process."""

from __future__ import annotations

import importlib
import os
from types import ModuleType

# Centralized C++ Store exports.
_CENTRALIZED_EXPORTS = frozenset(
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

_BACKEND = os.environ.get("MOONCAKE_STORE_BACKEND", "centralized")
if _BACKEND in ("centralized", "p2p"):
    if _BACKEND == "centralized":
        _MODULE = _NATIVE_MODULE = "mooncake._store"
        _BUILD_OPTION = "WITH_STORE"
        _EXPORTS = _CENTRALIZED_EXPORTS
    else:
        _MODULE = "mooncake.store.p2p.store"
        _NATIVE_MODULE = "mooncake._store_p2p"
        _BUILD_OPTION = "WITH_STORE_P2P"
        _EXPORTS = _P2P_EXPORTS
    try:
        _IMPLEMENTATION: ModuleType = importlib.import_module(_MODULE)
    except ModuleNotFoundError as exc:
        if exc.name != _NATIVE_MODULE:
            raise
        hint = (
            " For a P2P-only installation, set MOONCAKE_STORE_BACKEND=p2p before importing."
            if _BACKEND == "centralized"
            else ""
        )
        raise ImportError(
            f"MOONCAKE_STORE_BACKEND={_BACKEND!r} requires {_NATIVE_MODULE} "
            f"from a Mooncake wheel built with {_BUILD_OPTION}=ON.{hint}"
        ) from exc

    _ARCHITECTURE = getattr(_IMPLEMENTATION, "_store_architecture", None)
    if _ARCHITECTURE != _BACKEND:
        raise ImportError(
            f"{_NATIVE_MODULE} has Store architecture {_ARCHITECTURE!r}; "
            f"expected {_BACKEND!r}"
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
        "MOONCAKE_STORE_BACKEND must be 'centralized', 'p2p', or 'rs'; "
        f"got {_BACKEND!r}"
    )

__all__ = sorted(_EXPORTS)


def __getattr__(name: str):
    if name in _EXPORTS:
        return getattr(_IMPLEMENTATION, name)
    if _BACKEND == "rs" and (
        name in (_CENTRALIZED_EXPORTS - _RS_EXPORTS) or name in (_P2P_EXPORTS - _RS_EXPORTS)
    ):
        raise AttributeError(
            f"{name} is a C++ Store API and is unavailable with "
            "MOONCAKE_STORE_BACKEND='rs'"
        )
    raise AttributeError(name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | _EXPORTS)
