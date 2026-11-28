"""Type stubs for the ``mooncake._store`` nanobind extension.

Keep in sync with the bindings in
``mooncake-integration/store/store_py.cpp`` and
``mooncake-integration/store/buffer_pool.cpp``.

Most Store calls report failures through return values instead of raising:
``int`` results are ``0`` (or a byte count) on success and a negative
``ErrorCode`` on failure, and batch calls return one result per key.
Tensor arguments and results are ``torch.Tensor`` objects. Torch is an
optional dependency of the wheel, so they are typed as ``Any``.
"""

import sys
from collections.abc import Sequence
from enum import Enum
from types import TracebackType
from typing import Any, Protocol, TypedDict, overload

from typing_extensions import Buffer, Self, TypeAlias

from mooncake.engine import InnerTransferEngine
from mooncake.reshard.weight.store import (
    WeightSnapshotAdapter,
    WeightSnapshotDescriptor,
    WeightStoreWriter,
)

# A task, job or client ID: the (high, low) halves of a 128-bit UUID.
_UUID: TypeAlias = tuple[int, int]

class _PyClientCapsuleProvider(Protocol):
    def _get_pyclient_capsule(self) -> object: ...

class _MountSegmentResult(TypedDict):
    ret: int
    segment_ids: list[str]

class _AllocateAndMountSegmentResult(TypedDict):
    ret: int
    segment_ids: list[str]
    allocated_size: int

def _serialize_tensor(tensor: Any, /) -> tuple[bytes, int, int, Any]:
    """Return (metadata, data pointer, payload size, owner) for a tensor."""

def _tensor_metadata_size() -> int: ...
def _deserialize_tensor(payload: bytes, /) -> Any: ...

class RangedReadSnapshot:
    """Reusable metadata snapshot for repeated ranged reads."""

class ObjectDataType(Enum):
    _value_: int
    UNKNOWN = ...
    KVCACHE = ...
    TENSOR = ...
    WEIGHT = ...
    SAMPLE = ...
    ACTIVATION = ...
    GRADIENT = ...
    OPTIMIZER_STATE = ...
    METADATA = ...
    GENERAL = ...

UNKNOWN: ObjectDataType
KVCACHE: ObjectDataType
TENSOR: ObjectDataType
WEIGHT: ObjectDataType
SAMPLE: ObjectDataType
ACTIVATION: ObjectDataType
GRADIENT: ObjectDataType
OPTIMIZER_STATE: ObjectDataType
METADATA: ObjectDataType
GENERAL: ObjectDataType

class SoftPinAction(Enum):
    _value_: int
    PRESERVE = ...
    ENABLE = ...
    DISABLE = ...

class PhysicalPlacement:
    fabric_domain: str
    rack: str
    tray: str
    physical_gpu: int
    numa_node: int
    def __init__(self) -> None: ...

class ReplicateConfig:
    replica_num: int
    nof_replica_num: int
    dfs_replica_num: int
    soft_pin_action: SoftPinAction
    soft_pin_ttl_ms: int | None
    with_hard_pin: bool
    preferred_segments: list[str]
    preferred_nof_segments: list[str]
    preferred_segment: str
    prefer_alloc_in_same_node: bool
    data_type: ObjectDataType
    group_ids: list[str] | None
    # An empty string means unset.
    requester_self_id: str
    fabric_domain: str
    requester_placement: PhysicalPlacement | None
    @property
    def session_ids(self) -> list[str] | None: ...
    @session_ids.setter
    def session_ids(self, ids: Sequence[str | bytes | bytearray] | None, /) -> None: ...
    residency_affinity_ids: list[str] | None
    preferred_hca_names: list[str] | None
    per_key_preferred_hca_names: list[list[str]] | None
    def __init__(self) -> None: ...
    def __str__(self) -> str: ...

class ReplicaStatus(Enum):
    _value_: int
    UNDEFINED = ...
    INITIALIZED = ...
    PROCESSING = ...
    COMPLETE = ...
    REMOVED = ...
    FAILED = ...

UNDEFINED: ReplicaStatus
INITIALIZED: ReplicaStatus
COMPLETE: ReplicaStatus
REMOVED: ReplicaStatus

class ExistOptions:
    prefetch_to_memory: bool
    def __init__(self) -> None: ...
    def __str__(self) -> str: ...

class Descriptor:
    """Descriptor for allocated buffers. Only memory descriptors are supported."""

    size: int
    buffer_address: int
    transport_endpoint: str
    def __init__(self) -> None: ...
    def __repr__(self) -> str: ...

class MemoryDescriptor:
    buffer_descriptor: Descriptor

class DiskDescriptor:
    file_path: str
    object_size: int

class ReplicaDescriptor:
    @property
    def status(self) -> ReplicaStatus: ...
    def is_memory_replica(self) -> bool: ...
    def is_disk_replica(self) -> bool: ...
    def is_local_disk_replica(self) -> bool: ...
    def get_memory_descriptor(self) -> MemoryDescriptor: ...
    def get_disk_descriptor(self) -> DiskDescriptor: ...

class TaskType(Enum):
    _value_: int
    REPLICA_COPY = ...
    REPLICA_MOVE = ...

REPLICA_COPY: TaskType
REPLICA_MOVE: TaskType

class TaskStatus(Enum):
    _value_: int
    PENDING = ...
    PROCESSING = ...
    SUCCESS = ...
    FAILED = ...
    CANCELED = ...

# ReplicaStatus and TaskStatus both export PROCESSING and FAILED; TaskStatus is
# exported last, so the module attributes are the TaskStatus members.
PENDING: TaskStatus
PROCESSING: TaskStatus
SUCCESS: TaskStatus
FAILED: TaskStatus
CANCELED: TaskStatus

class QueryTaskResponse:
    """Response structure for QueryTask operation."""

    def __init__(self) -> None: ...
    @property
    def id(self) -> _UUID: ...
    @property
    def type(self) -> TaskType: ...
    @property
    def status(self) -> TaskStatus: ...
    @property
    def created_at_ms_epoch(self) -> int: ...
    @property
    def last_updated_at_ms_epoch(self) -> int: ...
    @property
    def assigned_client(self) -> _UUID: ...
    @property
    def message(self) -> str: ...
    def __repr__(self) -> str: ...

class JobType(Enum):
    _value_: int
    DRAIN = ...

class JobStatus(Enum):
    _value_: int
    CREATED = ...
    PLANNING = ...
    RUNNING = ...
    SUCCEEDED = ...
    FAILED = ...
    CANCELED = ...
    CANCELING = ...
    TIMED_OUT = ...

class QueryJobResponse:
    @property
    def id(self) -> _UUID: ...
    @property
    def type(self) -> JobType: ...
    @property
    def status(self) -> JobStatus: ...
    @property
    def created_at_ms_epoch(self) -> int: ...
    @property
    def last_updated_at_ms_epoch(self) -> int: ...
    @property
    def segments(self) -> list[str]: ...
    @property
    def succeeded_units(self) -> int: ...
    @property
    def failed_units(self) -> int: ...
    @property
    def blocked_units(self) -> int: ...
    @property
    def active_units(self) -> int: ...
    @property
    def migrated_bytes(self) -> int: ...
    @property
    def message(self) -> str: ...

class ListDrainJobsResponse:
    @property
    def jobs(self) -> list[QueryJobResponse]: ...
    @property
    def next_page_token(self) -> str: ...

class ListTasksResponse:
    @property
    def tasks(self) -> list[QueryTaskResponse]: ...
    @property
    def next_page_token(self) -> str: ...

class BufferHandle:
    """A Store-owned byte buffer that exports the buffer protocol."""

    def ptr(self) -> int: ...
    def size(self) -> int: ...
    def __len__(self) -> int: ...
    if sys.version_info >= (3, 12):
        def __buffer__(self, flags: int, /) -> memoryview: ...

class BufferLeaseView:
    @property
    def buffer(self) -> memoryview: ...
    if sys.version_info >= (3, 12):
        def __buffer__(self, flags: int, /) -> memoryview: ...

class BufferLease:
    @property
    def ptr(self) -> int: ...
    @property
    def size(self) -> int: ...
    @property
    def buffer(self) -> memoryview: ...
    def release(self) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

class BufferPool:
    def __init__(
        self,
        store: _PyClientCapsuleProvider,
        max_bytes: int = 0,
        min_size_class: int = 65536,
        max_size_class: int | None = None,
        alignment: int = 8388608,
        block_on_exhaustion: bool = True,
        default_timeout: float | None = None,
        max_regions: int | None = None,
        prewarm_size: int | None = None,
        prewarm_count: int = 0,
    ) -> None: ...
    def acquire(
        self, size: int, block: bool | None = None, timeout: float | None = None
    ) -> BufferLease: ...
    def buffer(
        self, size: int, block: bool | None = None, timeout: float | None = None
    ) -> BufferLease: ...
    def prewarm(self, size: int, count: int) -> None: ...
    def close(self) -> None: ...

RegisteredBufferPool = BufferPool
RegisteredBufferLease = BufferLease
RegisteredBufferLeaseView = BufferLeaseView

class MooncakeHostMemAllocator:
    def __init__(self) -> None: ...
    def alloc(self, size: int, /) -> int: ...
    def free(self, ptr: int, /) -> int: ...

class MooncakeDistributedNoFRegister:
    def __init__(self) -> None: ...
    def real_register(
        self,
        nqn: str,
        nsid: int,
        traddr: str,
        trsvcid: int,
        base: int,
        size: int,
        master_server_addr: str,
        /,
    ) -> int: ...
    def real_unregister_by_endpoint(
        self,
        nqn: str,
        nsid: int,
        traddr: str,
        trsvcid: int,
        master_server_addr: str,
        /,
    ) -> int: ...

class MooncakeDistributedStore:
    def __init__(self) -> None: ...
    def begin_weight_snapshot(
        self, snapshot: WeightSnapshotDescriptor, adapter: WeightSnapshotAdapter
    ) -> WeightStoreWriter:
        """Open a model-weight snapshot writer backed by this Store."""

    def _get_pyclient_capsule(self) -> object:
        """Return the native PyClient handle as a typed capsule, or None."""

    @overload
    def setup(
        self,
        local_hostname: str,
        metadata_server: str,
        global_segment_size: int,
        local_buffer_size: int,
        protocol: str,
        rdma_devices: str,
        master_server_addr: str,
        engine: InnerTransferEngine | None = None,
        enable_ssd_offload: bool = False,
        ssd_offload_path: str = "",
        tenant_id: str = "default",
        enable_client_http_server: bool = False,
        client_http_port: int = 9300,
        enable_embedded_master: bool = False,
    ) -> int: ...
    @overload
    def setup(self, config: dict[str, Any]) -> int:
        """Set up the store from a dict; keys and values are converted with str()."""

    def setup_dummy(
        self, mem_pool_size: int, local_buffer_size: int, server_address: str
    ) -> int: ...
    def init_all(
        self, protocol: str, device_name: str, mount_segment_size: int, /
    ) -> int: ...
    def mount_segment(
        self,
        path: str,
        size: int,
        offset: int = 0,
        protocol: str = "tcp",
        location: str = "",
    ) -> _MountSegmentResult: ...
    def unmount_segment(
        self, segment_ids: Sequence[str], grace_period_seconds: int = 0
    ) -> int: ...
    def allocate_and_mount_segment(
        self, size: int, protocol: str = "tcp", location: str = ""
    ) -> _AllocateAndMountSegmentResult: ...
    def unmount_and_free_segment(
        self, segment_ids: Sequence[str], grace_period_seconds: int = 0
    ) -> int: ...
    def unmount_local_disk_segment(self, grace_period_seconds: int = 0) -> int: ...
    def alloc_from_mem_pool(self, size: int, /) -> int: ...
    def get(self, key: str, /) -> bytes | None:
        """Return the value, b"" if it is missing, or None if a device copy fails."""

    def get_batch(self, keys: Sequence[str], /) -> list[bytes]: ...
    def get_offload_rpc_read_count(self) -> int: ...
    def get_buffer(self, key: str, /) -> BufferHandle | None: ...
    def batch_get_buffer(self, keys: Sequence[str], /) -> list[BufferHandle | None]: ...
    def remove(self, key: str, force: bool = False) -> int: ...
    def remove_by_regex(self, regex_pattern: str, force: bool = False) -> int: ...
    def remove_all(self, force: bool = False) -> int: ...
    def batch_remove(self, keys: Sequence[str], force: bool = False) -> list[int]: ...
    def is_exist(self, key: str, options: ExistOptions = ...) -> int:
        """Return 1 if the object exists, 0 if not, or a negative error code."""

    def batch_is_exist(
        self, keys: Sequence[str], options: ExistOptions = ...
    ) -> list[int]: ...
    def probe_key(self, key: str) -> int: ...
    def batch_probe_key(
        self, keys: Sequence[str], policy: str = "none", candidate_size: int = 1
    ) -> list[int]:
        """``policy`` is "none" or "LastHitOnly"; other values raise ValueError."""

    def close(self) -> int: ...
    def health_check(self) -> int:
        """Return 0 if healthy, 1 if not initialized or closed, 2 if the master is unreachable."""

    def get_size(self, key: str, /) -> int: ...
    def get_tensor_with_tp(
        self, key: str, tp_rank: int = 0, tp_size: int = 1, split_dim: int = 0
    ) -> Any: ...
    def batch_get_tensor_with_tp(
        self, base_keys: Sequence[str], tp_rank: int = 0, tp_size: int = 1
    ) -> list[Any]: ...
    def get_tensor(self, key: str) -> Any: ...
    def put_tensor_with_tp(
        self,
        key: str,
        tensor: Any,
        tp_rank: int = 0,
        tp_size: int = 1,
        split_dim: int = 0,
    ) -> int: ...
    def batch_put_tensor_with_tp(
        self,
        base_keys: Sequence[str],
        tensors_list: list[Any],
        tp_rank: int = 0,
        tp_size: int = 1,
        split_dim: int = 0,
    ) -> list[int]: ...
    def put_tensor(self, key: str, tensor: Any) -> int: ...
    def batch_get_tensor(self, keys: Sequence[str]) -> list[Any]: ...
    def batch_put_tensor(
        self, keys: Sequence[str], tensors_list: list[Any]
    ) -> list[int]: ...
    def save_tensor_to_safetensor(
        self, key: str, file_name: str | None = None
    ) -> int: ...
    @overload
    def load_tensor_from_safetensor(self, key: str | None, file_name: str) -> Any: ...
    @overload
    def load_tensor_from_safetensor(
        self, *, file_name: str, key: str | None = None
    ) -> Any: ...
    def pub_tensor(
        self, key: str, tensor: Any, config: ReplicateConfig = ...
    ) -> int: ...
    def batch_pub_tensor(
        self,
        keys: Sequence[str],
        tensors_list: list[Any],
        config: ReplicateConfig = ...,
    ) -> list[int]: ...
    def pub_tensor_with_tp(
        self,
        key: str,
        tensor: Any,
        config: ReplicateConfig = ...,
        tp_rank: int = 0,
        tp_size: int = 1,
        split_dim: int = 0,
    ) -> int: ...
    def batch_pub_tensor_with_tp(
        self,
        base_keys: Sequence[str],
        tensors_list: list[Any],
        config: ReplicateConfig = ...,
        tp_rank: int = 0,
        tp_size: int = 1,
        split_dim: int = 0,
    ) -> list[int]: ...
    def get_tensor_into(self, key: str, buffer_ptr: int, size: int) -> Any: ...
    def batch_get_tensor_into(
        self, keys: Sequence[str], buffer_ptrs: Sequence[int], sizes: Sequence[int]
    ) -> list[Any]:
        """Return one tensor (or None) per key, or one error code per key."""

    def get_tensor_into_cuda(self, key: str, tensor: Any) -> int: ...
    def batch_get_tensor_into_cuda(
        self, keys: Sequence[str], tensors_list: list[Any]
    ) -> list[int]: ...
    def get_tensor_with_tp_into(
        self,
        key: str,
        buffer_ptr: int,
        size: int,
        tp_rank: int = 0,
        tp_size: int = 1,
        split_dim: int = 0,
    ) -> Any: ...
    def batch_get_tensor_with_tp_into(
        self,
        base_keys: Sequence[str],
        buffer_ptrs: Sequence[int],
        sizes: Sequence[int],
        tp_rank: int = 0,
        tp_size: int = 1,
    ) -> list[Any]: ...
    def put_tensor_from(self, key: str, buffer_ptr: int, size: int) -> int: ...
    def batch_put_tensor_from(
        self, keys: Sequence[str], buffer_ptrs: Sequence[int], sizes: Sequence[int]
    ) -> list[int]: ...
    def put_tensor_with_tp_from(
        self,
        key: str,
        buffer_ptr: int,
        size: int,
        tp_rank: int = 0,
        tp_size: int = 1,
        split_dim: int = 0,
    ) -> int: ...
    def batch_put_tensor_with_tp_from(
        self,
        base_keys: Sequence[str],
        buffer_ptrs: Sequence[int],
        sizes: Sequence[int],
        tp_rank: int = 0,
        tp_size: int = 1,
        split_dim: int = 0,
    ) -> list[int]: ...
    def upsert_tensor(self, key: str, tensor: Any) -> int: ...
    def upsert_tensor_from(self, key: str, buffer_ptr: int, size: int) -> int: ...
    def batch_upsert_tensor_from(
        self, keys: Sequence[str], buffer_ptrs: Sequence[int], sizes: Sequence[int]
    ) -> list[int]: ...
    def batch_upsert_tensor(
        self, keys: Sequence[str], tensors_list: list[Any]
    ) -> list[int]: ...
    def upsert_pub_tensor(
        self, key: str, tensor: Any, config: ReplicateConfig = ...
    ) -> int: ...
    def batch_upsert_pub_tensor(
        self,
        keys: Sequence[str],
        tensors_list: list[Any],
        config: ReplicateConfig = ...,
    ) -> list[int]: ...
    def upsert_from(
        self, key: str, buffer_ptr: int, size: int, config: ReplicateConfig = ...
    ) -> int: ...
    def batch_upsert_from(
        self,
        keys: Sequence[str],
        buffer_ptrs: Sequence[int],
        sizes: Sequence[int],
        config: ReplicateConfig = ...,
    ) -> list[int]: ...
    def upsert(self, key: str, value: Buffer, config: ReplicateConfig = ...) -> int: ...
    def upsert_parts(
        self, key: str, *parts: Buffer, config: ReplicateConfig = ...
    ) -> int: ...
    def upsert_batch(
        self,
        keys: Sequence[str],
        values: Sequence[Buffer],
        config: ReplicateConfig = ...,
    ) -> int: ...
    def register_buffer(self, buffer_ptr: int, size: int) -> int: ...
    def unregister_buffer(self, buffer_ptr: int) -> int: ...
    def get_into(self, key: str, buffer_ptr: int, size: int) -> int:
        """Return the number of bytes read, or a negative error code."""

    def get_into_ranges(
        self,
        buffer_ptrs: Sequence[int],
        all_keys: Sequence[Sequence[str]],
        all_dst_offsets: Sequence[Sequence[Sequence[int]]],
        all_src_offsets: Sequence[Sequence[Sequence[int]]],
        all_sizes: Sequence[Sequence[Sequence[int]]],
    ) -> list[list[list[int]]]: ...
    def prepare_get_into_ranges_snapshot(
        self, keys: Sequence[str]
    ) -> RangedReadSnapshot: ...
    def get_into_ranges_from_snapshot(
        self,
        snapshot: RangedReadSnapshot,
        buffer_ptrs: Sequence[int],
        all_keys: Sequence[Sequence[str]],
        all_dst_offsets: Sequence[Sequence[Sequence[int]]],
        all_src_offsets: Sequence[Sequence[Sequence[int]]],
        all_sizes: Sequence[Sequence[Sequence[int]]],
    ) -> list[list[list[int]]]: ...
    def batch_get_into(
        self, keys: Sequence[str], buffer_ptrs: Sequence[int], sizes: Sequence[int]
    ) -> list[int]: ...
    def put_from(
        self, key: str, buffer_ptr: int, size: int, config: ReplicateConfig = ...
    ) -> int: ...
    def put_from_with_metadata(
        self,
        key: str,
        buffer_ptr: int,
        metadata_buffer_ptr: int,
        size: int,
        metadata_size: int,
        config: ReplicateConfig = ...,
    ) -> int: ...
    def batch_put_from(
        self,
        keys: Sequence[str],
        buffer_ptrs: Sequence[int],
        sizes: Sequence[int],
        config: ReplicateConfig = ...,
    ) -> list[int]: ...
    def put(self, key: str, value: Buffer, config: ReplicateConfig = ...) -> int: ...
    def put_parts(
        self, key: str, *parts: Buffer, config: ReplicateConfig = ...
    ) -> int: ...
    def put_batch(
        self,
        keys: Sequence[str],
        values: Sequence[Buffer],
        config: ReplicateConfig = ...,
    ) -> int: ...
    def get_hostname(self) -> str: ...
    def batch_put_from_multi_buffers(
        self,
        keys: Sequence[str],
        all_buffer_ptrs: Sequence[Sequence[int]],
        all_sizes: Sequence[Sequence[int]],
        config: ReplicateConfig = ...,
    ) -> list[int]: ...
    def batch_get_into_multi_buffers(
        self,
        keys: Sequence[str],
        all_buffer_ptrs: Sequence[Sequence[int]],
        all_sizes: Sequence[Sequence[int]],
        prefer_alloc_in_same_node: bool = False,
    ) -> list[int]: ...
    def batch_get_session_start(self, keys: Sequence[str]) -> list[int]: ...
    def batch_get_into_multi_buffer_ranges(
        self,
        keys: Sequence[str],
        all_buffer_ptrs: Sequence[Sequence[int]],
        all_sizes: Sequence[Sequence[int]],
        all_src_offsets: Sequence[Sequence[int]],
    ) -> list[int]: ...
    def batch_get_session_end(self, keys: Sequence[str]) -> int: ...
    def batch_put_session_start(
        self,
        keys: Sequence[str],
        sizes: Sequence[int],
        config: ReplicateConfig = ...,
    ) -> list[int]: ...
    def batch_put_from_multi_buffer_ranges(
        self,
        keys: Sequence[str],
        all_buffer_ptrs: Sequence[Sequence[int]],
        all_sizes: Sequence[Sequence[int]],
        all_dst_offsets: Sequence[Sequence[int]],
    ) -> list[int]: ...
    def batch_put_session_end(self, keys: Sequence[str]) -> list[int]: ...
    def batch_put_session_revoke(self, keys: Sequence[str]) -> list[int]: ...
    def get_replica_desc(self, key: str) -> list[ReplicaDescriptor]: ...
    def batch_get_replica_desc(
        self, keys: Sequence[str]
    ) -> dict[str, list[ReplicaDescriptor]]: ...
    def batch_replica_clear(
        self, keys: Sequence[str], segment_name: str = ""
    ) -> list[str]: ...
    def create_copy_task(
        self, key: str, targets: Sequence[str]
    ) -> tuple[_UUID, int]: ...
    def create_move_task(
        self, key: str, source: str, target: str
    ) -> tuple[_UUID, int]: ...
    def query_task(self, task_id: _UUID) -> tuple[QueryTaskResponse | None, int]: ...
    def create_drain_job(
        self,
        segments: Sequence[str],
        target_segments: Sequence[str] = ...,
        max_concurrency: int = 4,
        timeout_ms: int = 0,
    ) -> tuple[_UUID, int]: ...
    def query_drain_job(self, job_id: _UUID) -> tuple[QueryJobResponse | None, int]: ...
    def cancel_drain_job(self, job_id: _UUID) -> int: ...
    def list_drain_jobs(
        self,
        status: JobStatus | None = None,
        segment: str | None = None,
        limit: int = 100,
        page_token: str = "",
    ) -> tuple[ListDrainJobsResponse | None, int]: ...
    def list_tasks(
        self,
        status: TaskStatus | None = None,
        type: TaskType | None = None,
        assigned_client: _UUID | None = None,
        key: str | None = None,
        limit: int = 100,
        page_token: str = "",
    ) -> tuple[ListTasksResponse | None, int]: ...
    def cancel_task(self, task_id: _UUID) -> int: ...

def bind_to_numa_node(node: int) -> None:
    """Bind the current thread and memory allocation preference to a NUMA node."""

def get_alloc_func_addr() -> int: ...
def get_free_func_addr() -> int: ...
