"""Downstream view of ``mooncake.store``, type-checked against an installed wheel.

This file is only analyzed by mypy (``--strict``) and is never executed.
The ``type: ignore[...]`` lines are negative checks: ``--strict`` enables
``warn_unused_ignores``, so they fail if the stub stops rejecting bad calls.
"""

from __future__ import annotations

from typing import Any

from typing_extensions import assert_type

from mooncake import _store
from mooncake.store import (
    FAILED,
    KVCACHE,
    BufferHandle,
    BufferLease,
    BufferPool,
    Descriptor,
    ExistOptions,
    JobStatus,
    ListTasksResponse,
    MooncakeDistributedStore,
    ObjectDataType,
    QueryJobResponse,
    QueryTaskResponse,
    RangedReadSnapshot,
    RegisteredBufferPool,
    ReplicaDescriptor,
    ReplicaStatus,
    ReplicateConfig,
    SoftPinAction,
    TaskStatus,
    bind_to_numa_node,
)


def exercise_store(store: MooncakeDistributedStore) -> None:
    assert_type(
        store.setup(
            "127.0.0.1",
            "P2PHANDSHAKE",
            16 << 20,
            16 << 20,
            "tcp",
            "",
            "127.0.0.1:50051",
            tenant_id="default",
        ),
        int,
    )
    assert_type(store.setup({"local_hostname": "127.0.0.1", "protocol": "tcp"}), int)

    config = ReplicateConfig()
    config.replica_num = 2
    config.preferred_segments = ["segment-a"]
    config.soft_pin_action = SoftPinAction.ENABLE
    config.soft_pin_ttl_ms = None
    config.data_type = KVCACHE
    config.session_ids = [b"raw-session", "session"]
    assert_type(config.session_ids, list[str] | None)
    assert_type(config.requester_placement, _store.PhysicalPlacement | None)

    assert_type(store.put("key", b"value", config), int)
    assert_type(store.put_parts("key", b"a", bytearray(b"b"), config=config), int)
    assert_type(store.put_batch(["a", "b"], [b"1", memoryview(b"2")]), int)
    assert_type(store.get("key"), bytes | None)
    assert_type(store.get_batch(["a", "b"]), list[bytes])
    assert_type(store.get_buffer("key"), BufferHandle | None)
    assert_type(store.is_exist("key", ExistOptions()), int)
    assert_type(store.batch_is_exist(["a"]), list[int])
    assert_type(store.batch_remove(["a"], force=True), list[int])
    assert_type(store.remove_by_regex("^a", force=True), int)

    assert_type(store.register_buffer(4096, 1 << 20), int)
    assert_type(store.get_into("key", 4096, 1 << 20), int)
    assert_type(store.batch_put_from(["a"], [4096], [128], config=config), list[int])
    assert_type(store.batch_get_into_multi_buffers(["a"], [[4096]], [[128]]), list[int])
    snapshot = store.prepare_get_into_ranges_snapshot(["a"])
    assert_type(snapshot, RangedReadSnapshot)
    assert_type(
        store.get_into_ranges_from_snapshot(
            snapshot, [4096], [["a"]], [[[0]]], [[[0]]], [[[128]]]
        ),
        list[list[list[int]]],
    )

    mounted = store.mount_segment("/dev/shm/segment", 1 << 30)
    assert_type(mounted["ret"], int)
    assert_type(mounted["segment_ids"], list[str])

    replicas = store.get_replica_desc("key")
    assert_type(replicas, list[ReplicaDescriptor])
    assert_type(replicas[0].status, ReplicaStatus)
    descriptor = replicas[0].get_memory_descriptor().buffer_descriptor
    assert_type(descriptor, Descriptor)
    assert_type(descriptor.buffer_address, int)
    assert_type(
        store.batch_get_replica_desc(["key"]), dict[str, list[ReplicaDescriptor]]
    )

    task_id, error = store.create_copy_task("key", ["segment-b"])
    assert_type(task_id, tuple[int, int])
    assert_type(error, int)
    task, _ = store.query_task(task_id)
    assert_type(task, QueryTaskResponse | None)
    assert_type(
        store.list_tasks(status=TaskStatus.PENDING)[0], ListTasksResponse | None
    )
    job, _ = store.query_drain_job(task_id)
    assert_type(job, QueryJobResponse | None)
    store.list_drain_jobs(status=JobStatus.RUNNING, limit=10)

    tensor: Any = store.get_tensor("key")
    assert_type(store.put_tensor_with_tp("key", tensor, tp_size=2), int)
    assert_type(store.load_tensor_from_safetensor(file_name="weights.safetensors"), Any)
    assert_type(store.close(), int)

    store.put("key", "text")  # type: ignore[arg-type]
    store.get_batch([1])  # type: ignore[list-item]
    store.setup("127.0.0.1", "P2PHANDSHAKE")  # type: ignore[call-overload]
    store.get(key="key")  # type: ignore[call-arg]
    store.not_a_binding()  # type: ignore[attr-defined]


def exercise_buffer_pool(store: MooncakeDistributedStore) -> None:
    pool = BufferPool(store, max_bytes=1 << 30, default_timeout=1.5)
    assert_type(RegisteredBufferPool(store), BufferPool)
    with pool.acquire(4096, block=False) as lease:
        assert_type(lease, BufferLease)
        assert_type(lease.ptr, int)
        assert_type(lease.buffer, memoryview)
    pool.close()
    BufferPool(object())  # type: ignore[arg-type]


def exercise_enums() -> None:
    assert_type(KVCACHE, ObjectDataType)
    assert_type(FAILED, TaskStatus)
    assert_type(ObjectDataType.WEIGHT.value, int)
    bind_to_numa_node(0)
