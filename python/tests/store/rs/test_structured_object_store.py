"""The legacy RS module exports the shared classes, not a second implementation."""

import importlib
import ctypes
import os

import numpy as np
import pytest

from mooncake.structured_object_store import (
    BundleTransferPolicy,
    MooncakeBundleTransfer,
    StructuredMemberRange,
    StructuredObjectPayload,
    raw_destination,
    tensor_object_buffer,
)


def test_rs_structured_exports_are_shared() -> None:
    shared = importlib.import_module("mooncake.structured_object_store")
    legacy = importlib.import_module("mooncake.store.rs.structured_object_store")
    for name in legacy.__all__:
        assert getattr(legacy, name) is getattr(shared, name)


@pytest.fixture
def rs_store():
    """Use an isolated Redis namespace and the real RS extension when configured."""
    metadata_url = os.getenv("MOONCAKE_STRUCTURED_RS_TEST_REDIS_URL")
    if metadata_url is None:
        pytest.skip("set MOONCAKE_STRUCTURED_RS_TEST_REDIS_URL for native RS coverage")
    import uuid

    from mooncake.store.rs.store import MooncakeDistributedStore

    store = MooncakeDistributedStore()
    assert (
        store.setup(
            "127.0.0.1",
            metadata_url,
            64 * 1024**2,
            4 * 1024**2,
            "tcp",
            "",
            metadata_url,
            keyspace=f"structured-test-{uuid.uuid4().hex}",
            transport_backend="tent",
        )
        == 0
    )
    try:
        yield store
    finally:
        store.close()


def test_rs_shared_transport_uses_pool_and_ranges(rs_store) -> None:
    transfer = MooncakeBundleTransfer(rs_store)
    array = np.arange(96, dtype=np.int32).reshape(12, 8)
    ref = transfer.put_structured_object(
        StructuredObjectPayload(buffers={"array": array}), chunk_bytes=32
    )
    try:
        result = transfer.materialize(
            transfer.read_spec(ref).slice_member("array", axis=0, start=2, end=7)
        )
        assert np.array_equal(result.objects["array"], array[2:7])
        assert transfer._transport._buffer_pool is not None
        destination = np.empty_like(array)
        ptr = int(destination.ctypes.data)
        assert rs_store.register_buffer(ptr, destination.nbytes) == 0
        try:
            transfer.materialize_ranges_into(
                transfer.read_spec(ref),
                [
                    StructuredMemberRange("array", 8, 32, 0, 4),
                    StructuredMemberRange("array", 48, 16, 32, 4),
                ],
                {
                    "array": raw_destination(
                        ptr, destination.nbytes, destination, pre_registered=True
                    )
                },
            )
            assert destination.view(np.uint8).reshape(-1)[:48].tobytes() == (
                array.tobytes()[8:40] + array.tobytes()[48:64]
            )
        finally:
            assert rs_store.unregister_buffer(ptr) == 0
    finally:
        transfer.remove_bundle(ref)


@pytest.mark.parametrize("shape", [(6, 4), (), (0, 4), (1,) * 9])
def test_rs_shared_tensor_roundtrip_and_slice(rs_store, shape) -> None:
    torch = pytest.importorskip("torch")
    tensor = torch.arange(
        int(np.prod(shape)) if shape else 1, dtype=torch.float32
    ).reshape(shape)
    transfer = MooncakeBundleTransfer(rs_store)
    ref = transfer.put_structured_object(
        StructuredObjectPayload(buffers={"tensor": tensor})
    )
    try:
        payload = ref.manifest["buffers"]["tensor"]
        if tensor.numel() == 0 or tensor.dim() > 8:
            assert payload["format"] == "torch_save"
        else:
            assert payload["metadata_bytes"] == 304
            assert payload["bytes"] == tensor.numel() * tensor.element_size() + 304
        result = transfer.materialize(transfer.read_spec(ref))
        assert torch.equal(result.objects["tensor"], tensor)
        if shape:
            result = transfer.materialize(
                transfer.read_spec(ref).slice_member("tensor", axis=0, start=0, end=2)
            )
            assert torch.equal(result.objects["tensor"], tensor[:2])
    finally:
        transfer.remove_bundle(ref)


def test_rs_shared_tensor_object_buffer_uses_pointer_put(rs_store) -> None:
    torch = pytest.importorskip("torch")
    tensor = torch.arange(24, dtype=torch.int64).reshape(6, 4)
    assert rs_store.put_tensor("serialized-source", tensor) == 0
    payload = rs_store.get("serialized-source")
    transfer = MooncakeBundleTransfer(rs_store)
    pool = transfer._transport._ensure_buffer_pool()
    assert pool is not None
    lease = pool.acquire(len(payload))
    ref = None
    try:
        ctypes.memmove(lease.ptr, payload, len(payload))
        ref = transfer.put_structured_object(
            StructuredObjectPayload(
                buffers={"tensor": tensor_object_buffer(lease.ptr, len(payload), lease)}
            ),
            policy=BundleTransferPolicy(copy_mode="zero_copy"),
        )
        result = transfer.materialize(transfer.read_spec(ref))
        assert torch.equal(result.objects["tensor"], tensor)
    finally:
        if ref is not None:
            transfer.remove_bundle(ref)
        lease.release()
        pool.close()
        assert rs_store.remove("serialized-source", force=True) == 0


def test_rs_shared_large_write_and_retained_read_leases(rs_store) -> None:
    transfer = MooncakeBundleTransfer(rs_store)
    # Exceed the auto pool's 8 MiB budget in a single default-size chunk.
    large = np.arange(3 * 1024**2, dtype=np.int32)
    ref = transfer.put_structured_object(
        StructuredObjectPayload(buffers={"large": large})
    )
    try:
        result = transfer.materialize(transfer.read_spec(ref))
        assert np.array_equal(result.objects["large"], large)
    finally:
        transfer.remove_bundle(ref)
    # Two 6 MiB members cannot simultaneously fit in that pool.
    member = np.arange(1536 * 1024, dtype=np.int32)
    ref = transfer.put_structured_object(
        StructuredObjectPayload(buffers={"a": member, "b": member})
    )
    try:
        first = transfer.materialize(transfer.read_spec(ref))
        second = transfer.materialize(transfer.read_spec(ref))
        for result in (first, second):
            assert np.array_equal(result.objects["a"], member)
            assert np.array_equal(result.objects["b"], member)
        transfer.release_result(first)
        transfer.release_result(second)
    finally:
        transfer.remove_bundle(ref)


def test_rs_shared_tensor_config_preserves_raw_write_placement(
    rs_store, monkeypatch
) -> None:
    torch = pytest.importorskip("torch")
    from mooncake.store.rs.store import ReplicateConfig

    config = ReplicateConfig(with_soft_pin=True)
    calls = []
    invoke = rs_store._invoke_cache

    def record(name, *args, **kwargs):
        if name in ("put", "put_from", "batch_put_from_raw"):
            calls.append(kwargs.get("with_soft_pin"))
        return invoke(name, *args, **kwargs)

    monkeypatch.setattr(rs_store, "_invoke_cache", record)
    monkeypatch.setattr(
        rs_store,
        "put_tensor",
        lambda *args, **kwargs: pytest.fail(
            "configured tensor used native placement defaults"
        ),
    )
    transfer = MooncakeBundleTransfer(rs_store)
    value = torch.arange(12, dtype=torch.float32).reshape(3, 4)
    ref = transfer.put_structured_object(
        StructuredObjectPayload(buffers={"value": value}), config=config
    )
    try:
        assert torch.equal(
            transfer.materialize(transfer.read_spec(ref)).objects["value"], value
        )
        assert calls and all(calls)
    finally:
        transfer.remove_bundle(ref)


def test_rs_shared_dataproto_uses_ungrouped_backend_contract(rs_store) -> None:
    transfer = MooncakeBundleTransfer(rs_store)
    array = np.arange(24, dtype=np.int32).reshape(6, 4)
    ref = transfer.put({"array": array}, type="dict")
    try:
        from mooncake.structured_object_store import export_dataproto_ref

        assert "storage_group_id" not in export_dataproto_ref(ref)
        result = transfer.get(ref, type="dict", rows=[4, 1])
        assert np.array_equal(result["array"], array[[4, 1]])
    finally:
        transfer.cleanup_dataproto(ref)


def test_rs_shared_concurrent_immutable_source(rs_store, monkeypatch) -> None:
    import threading
    from concurrent.futures import ThreadPoolExecutor

    source = b"x" * (512 * 1024)
    barrier = threading.Barrier(2)
    register = rs_store.register_buffer

    def synchronized_register(ptr, size):
        status = register(ptr, size)
        if size == len(source) and status == 0:
            barrier.wait(timeout=10)
        return status

    monkeypatch.setattr(rs_store, "register_buffer", synchronized_register)
    transfer = MooncakeBundleTransfer(rs_store)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(transfer.put_bundle, b"meta", {"payload": source})
            for _ in range(2)
        ]
        refs = [future.result() for future in futures]
    for ref in refs:
        assert rs_store.get(ref.manifest["buffers"]["payload"]["key"]) == source
        transfer.remove_bundle(ref)


@pytest.mark.parametrize("exhausted", [False, True])
def test_rs_configured_tensor_staging_with_cpp_codec_contract(
    rs_store, monkeypatch, exhausted
) -> None:
    torch = pytest.importorskip("torch")
    import mooncake.structured_object_store as sos
    from mooncake.store.rs.store import ReplicateConfig

    tensor = torch.arange(12, dtype=torch.float32).reshape(3, 4)
    assert rs_store.put_tensor("codec-header", tensor) == 0
    header = rs_store.get("codec-header")[:304]
    assert rs_store.remove("codec-header", force=True) == 0

    def parts(value):
        owner = value.tensor.detach().cpu().contiguous()
        return header, owner.data_ptr(), owner.nbytes, owner

    monkeypatch.setattr(sos, "_has_tensor_codec_helpers", lambda: True)
    monkeypatch.setattr(sos, "_tensor_payload_parts", parts)

    class ExhaustedPool:
        def acquire(self, size, block=None):
            assert block is False
            return None

    transfer = MooncakeBundleTransfer(
        rs_store, buffer_pool=ExhaustedPool() if exhausted else None
    )
    if not exhausted:
        assert transfer._transport._ensure_buffer_pool() is not None
    ref = transfer.put(
        {"value": tensor}, type="dict", config=ReplicateConfig(with_soft_pin=True)
    )
    try:
        manifest = next(iter(ref.stage_refs.values())).manifest
        assert manifest["buffers"]["batch.value"]["metadata_bytes"] == 304
        result = transfer.get(
            ref, type="dict", rows=[2, 0], batch_slices={"value": slice(1, 3)}
        )
        assert torch.equal(result["value"], tensor[[2, 0], 1:3])
    finally:
        transfer.cleanup_dataproto(ref)


def test_rs_group_rejection_also_applies_to_append_and_raw_bundle(rs_store) -> None:
    from types import SimpleNamespace

    transfer = MooncakeBundleTransfer(rs_store)
    config = SimpleNamespace(group_ids=["explicit"])
    with pytest.raises(NotImplementedError, match="storage groups"):
        transfer.put_bundle(b"meta", {"x": b"data"}, config=config)
    ref = transfer.put({"x": np.arange(4)}, type="dict")
    try:
        with pytest.raises(NotImplementedError, match="storage groups"):
            transfer.append_dataproto_fields(
                ref, {"y": np.arange(4)}, stage="next", config=config
            )
        assert np.array_equal(transfer.get(ref, type="dict")["x"], np.arange(4))
    finally:
        transfer.cleanup_dataproto(ref)
