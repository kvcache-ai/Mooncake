from __future__ import annotations

import ctypes
import json
import os
import threading
import time

import numpy as np
import pytest

from mooncake.structured_object_store import (
    BundleTransferPolicy,
    MooncakeBundleTransfer,
    RemoteBundleRef,
    StructuredObjectPayload,
    StructuredObjectReadSpec,
    export_dataproto_ref,
    import_dataproto_ref,
    is_dataproto_ref_handle,
    raw_destination,
    tensor_object_buffer,
)


class SimpleDataProto:
    def __init__(self, batch=None, non_tensor_batch=None, meta_info=None) -> None:
        self.batch = {} if batch is None else batch
        self.non_tensor_batch = {} if non_tensor_batch is None else non_tensor_batch
        self.meta_info = {} if meta_info is None else meta_info

    @classmethod
    def from_dict(cls, batch, non_tensor_batch=None, meta_info=None):
        return cls(batch=batch, non_tensor_batch=non_tensor_batch, meta_info=meta_info)


class BadDataProto:
    def __init__(self) -> None:
        pass


class InMemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.tensor_objects: dict[str, object] = {}
        self.lock = threading.Lock()
        self.registered: set[int] = set()
        self.register_buffer_calls = 0
        self.unregister_buffer_calls = 0
        self.max_active_puts = 0
        self.max_active_gets = 0
        self.active_puts = 0
        self.active_gets = 0
        self.get_into_calls = 0
        self.get_into_ranges_calls = 0
        self.batch_get_into_calls = 0
        self.batch_put_from_calls = 0
        self.put_tensor_from_calls = 0
        self.batch_remove_calls = 0
        self.put_tensor_calls = 0
        self.get_tensor_calls = 0

    def _enter_put(self) -> None:
        with self.lock:
            self.active_puts += 1
            self.max_active_puts = max(self.max_active_puts, self.active_puts)

    def _exit_put(self) -> None:
        with self.lock:
            self.active_puts -= 1

    def _enter_get(self, count: int = 1) -> None:
        with self.lock:
            self.active_gets += count
            self.max_active_gets = max(self.max_active_gets, self.active_gets)

    def _exit_get(self, count: int = 1) -> None:
        with self.lock:
            self.active_gets -= count

    def put(self, key: str, value) -> int:
        self._enter_put()
        try:
            time.sleep(0.01)
            with self.lock:
                self.objects[key] = bytes(value)
            return 0
        finally:
            self._exit_put()

    def get(self, key: str) -> bytes:
        self._enter_get()
        try:
            time.sleep(0.01)
            with self.lock:
                return self.objects[key]
        finally:
            self._exit_get()

    def remove(self, key: str, force: bool = False) -> int:
        with self.lock:
            self.objects.pop(key, None)
            self.tensor_objects.pop(key, None)
        return 0

    def put_tensor(self, key: str, value) -> int:
        self.put_tensor_calls += 1
        with self.lock:
            self.tensor_objects[key] = value.detach().clone()
        return 0

    def get_tensor(self, key: str):
        self.get_tensor_calls += 1
        with self.lock:
            return self.tensor_objects[key].clone()

    def batch_remove(self, keys: list[str], force: bool = False) -> list[int]:
        self.batch_remove_calls += 1
        for key in keys:
            self.remove(key, force)
        return [0 for _key in keys]

    def batch_put_from(
        self, keys: list[str], buffer_ptrs: list[int], sizes: list[int]
    ) -> list[int]:
        self.batch_put_from_calls += 1
        results: list[int] = []
        for key, ptr, size in zip(keys, buffer_ptrs, sizes):
            data = ctypes.string_at(ptr, size)
            results.append(self.put(key, data))
        return results

    def put_tensor_from(self, key: str, buffer_ptr: int, size: int) -> int:
        self.put_tensor_from_calls += 1
        if buffer_ptr not in self.registered:
            return -1
        return self.put(key, ctypes.string_at(buffer_ptr, size))

    def register_buffer(self, buffer_ptr: int, size: int) -> int:
        self.register_buffer_calls += 1
        self.registered.add(buffer_ptr)
        return 0

    def unregister_buffer(self, buffer_ptr: int) -> int:
        self.unregister_buffer_calls += 1
        self.registered.remove(buffer_ptr)
        return 0

    def get_into(self, key: str, ptr: int, size: int) -> int:
        self.get_into_calls += 1
        self._enter_get()
        try:
            time.sleep(0.01)
            with self.lock:
                data = self.objects[key]
            if len(data) > size:
                return -1
            ctypes.memmove(ptr, data, len(data))
            return len(data)
        finally:
            self._exit_get()

    def get_into_ranges(
        self,
        buffer_ptrs: list[int],
        all_keys: list[list[str]],
        all_dst_offsets: list[list[list[int]]],
        all_src_offsets: list[list[list[int]]],
        all_sizes: list[list[list[int]]],
    ) -> list[list[list[int]]]:
        self.get_into_ranges_calls += 1
        total_keys = sum(len(keys) for keys in all_keys)
        self._enter_get(total_keys)
        try:
            time.sleep(0.01)
            results: list[list[list[int]]] = []
            for base_ptr, keys, dst_groups, src_groups, size_groups in zip(
                buffer_ptrs, all_keys, all_dst_offsets, all_src_offsets, all_sizes
            ):
                buffer_results: list[list[int]] = []
                for key, dst_offsets, src_offsets, sizes in zip(
                    keys, dst_groups, src_groups, size_groups
                ):
                    with self.lock:
                        data = self.objects[key]
                    key_results: list[int] = []
                    for dst_offset, src_offset, size in zip(
                        dst_offsets, src_offsets, sizes
                    ):
                        end = src_offset + size
                        if end > len(data):
                            key_results.append(-1)
                            continue
                        ctypes.memmove(
                            base_ptr + dst_offset, data[src_offset:end], size
                        )
                        key_results.append(size)
                    buffer_results.append(key_results)
                results.append(buffer_results)
            return results
        finally:
            self._exit_get(total_keys)

    def batch_get_into(
        self, keys: list[str], ptrs: list[int], sizes: list[int]
    ) -> list[int]:
        self.batch_get_into_calls += 1
        self._enter_get(len(keys))
        try:
            time.sleep(0.01)
            results = []
            for key, ptr, size in zip(keys, ptrs, sizes):
                with self.lock:
                    data = self.objects[key]
                if len(data) > size:
                    results.append(-1)
                    continue
                ctypes.memmove(ptr, data, len(data))
                results.append(len(data))
            return results
        finally:
            self._exit_get(len(keys))


class GetOnlyStore(InMemoryStore):
    batch_get_into = None


class MinimalStore(GetOnlyStore):
    batch_put_from = None
    register_buffer = None
    unregister_buffer = None


class ReadFastPathWithoutRegisterStore(MinimalStore):
    batch_get_into = InMemoryStore.batch_get_into
    get_into = InMemoryStore.get_into
    get_into_ranges = InMemoryStore.get_into_ranges


class PlainStore(MinimalStore):
    get_into = None
    get_into_ranges = None


class PutTensorOnlyStore(InMemoryStore):
    get_tensor = None


class NoTensorFastPathStore(InMemoryStore):
    put_tensor = None
    get_tensor = None


class FailingPutStore(InMemoryStore):
    def __init__(self, fail_on_put: int) -> None:
        super().__init__()
        self.put_count = 0
        self.fail_on_put = fail_on_put

    def put(self, key: str, value) -> int:
        self.put_count += 1
        if self.put_count == self.fail_on_put:
            raise RuntimeError("injected put failure")
        return super().put(key, value)


class FailingBatchGetStore(InMemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.fail_key = ""

    def batch_get_into(
        self, keys: list[str], ptrs: list[int], sizes: list[int]
    ) -> list[int]:
        if self.fail_key in keys:
            raise RuntimeError("injected get failure")
        return super().batch_get_into(keys, ptrs, sizes)


class FailingRemoveStore(FailingPutStore):
    def remove(self, key: str, force: bool = False) -> int:
        raise RuntimeError("injected remove failure")

    batch_remove = None


class ForceTrackingStore(InMemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.batch_remove_forces: list[bool] = []

    def batch_remove(self, keys: list[str], force: bool = False) -> list[int]:
        self.batch_remove_forces.append(force)
        return super().batch_remove(keys, force)


class TransientBatchRemoveStore(InMemoryStore):
    """batch_remove reports a transient failure for one key without deleting it.

    The inherited per-key remove() retry then succeeds and deletes the key, so
    cleanup should end with no outstanding error.
    """

    def batch_remove(self, keys: list[str], force: bool = False) -> list[int]:
        self.batch_remove_calls += 1
        results: list[int] = []
        for index, key in enumerate(keys):
            if index == 0:
                results.append(-1)  # transient failure, key left in place
            else:
                self.remove(key, force)
                results.append(0)
        return results


class StrictRegisterStore(InMemoryStore):
    def register_buffer(self, buffer_ptr: int, size: int) -> int:
        if buffer_ptr in self.registered:
            return -600
        return super().register_buffer(buffer_ptr, size)


class FailingRegisterStore(InMemoryStore):
    def __init__(self, fail_on_register: int) -> None:
        super().__init__()
        self.register_count = 0
        self.fail_on_register = fail_on_register

    def register_buffer(self, buffer_ptr: int, size: int) -> int:
        self.register_count += 1
        if self.register_count == self.fail_on_register:
            return -1
        return super().register_buffer(buffer_ptr, size)


def make_transfer(
    store: InMemoryStore | None = None,
    *,
    key_prefix: str = "test",
    default_chunk_bytes: int | None = None,
    buffer_pool=None,
) -> tuple[InMemoryStore, MooncakeBundleTransfer]:
    current_store = store or InMemoryStore()
    kwargs = {"key_prefix": key_prefix, "buffer_pool": buffer_pool}
    if default_chunk_bytes is not None:
        kwargs["default_chunk_bytes"] = default_chunk_bytes
    return current_store, MooncakeBundleTransfer(current_store, **kwargs)


def real_transfer(key_prefix: str) -> tuple[object, MooncakeBundleTransfer]:
    mooncake_store = pytest.importorskip("mooncake.store")
    store = mooncake_store.MooncakeDistributedStore()
    rc = store.setup(
        os.getenv("LOCAL_HOSTNAME", "localhost"),
        os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE"),
        16 * 1024 * 1024,
        4 * 1024 * 1024,
        os.getenv("PROTOCOL", "tcp"),
        os.getenv("DEVICE_NAME", ""),
        os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
    )
    if rc != 0:
        pytest.skip(f"MooncakeDistributedStore setup failed: {rc}")
    return store, MooncakeBundleTransfer(store, key_prefix=key_prefix)


def structured_payload(
    metadata: dict[str, object] | None = None, **buffers: object
) -> StructuredObjectPayload:
    return StructuredObjectPayload(metadata=metadata, buffers=buffers)


def write_manifest(
    store: InMemoryStore, manifest_key: str, manifest: dict[str, object]
) -> None:
    store.objects[manifest_key] = json.dumps(manifest, separators=(",", ":")).encode(
        "utf-8"
    )


def test_put_object_roundtrips_numpy_and_torch_tensor_fields() -> None:
    torch = pytest.importorskip("torch")
    store, transfer = make_transfer()
    array = np.arange(6, dtype=np.float32).reshape(2, 3)
    tensor = torch.arange(6, dtype=torch.float32).reshape(2, 3)
    scalar_tensor = torch.tensor(1.5, dtype=torch.float32)
    bool_tensor = torch.tensor([True, False], dtype=torch.bool)

    result = transfer.get_object(
        transfer.put_object(
            {
                "array": array,
                "tensor": tensor,
                "scalar": scalar_tensor,
                "bool_tensor": bool_tensor,
            }
        )
    )

    assert np.array_equal(result["array"], array)
    assert torch.equal(result["tensor"], tensor)
    assert torch.equal(result["scalar"], scalar_tensor)
    assert torch.equal(result["bool_tensor"], bool_tensor)
    assert store.put_tensor_calls == 3
    assert store.get_tensor_calls == 3


def test_put_object_roundtrips_wrapped_numpy_and_torch_values() -> None:
    torch = pytest.importorskip("torch")
    store, transfer = make_transfer()
    array = np.arange(6, dtype=np.float32).reshape(2, 3)
    tensor = torch.arange(6, dtype=torch.float32).reshape(2, 3)

    assert np.array_equal(transfer.get_object(transfer.put_object(array)), array)
    assert torch.equal(transfer.get_object(transfer.put_object(tensor)), tensor)
    assert store.put_tensor_calls == 1
    assert store.get_tensor_calls == 1


def test_get_object_requires_get_tensor_for_tensor_payload() -> None:
    torch = pytest.importorskip("torch")
    store, transfer = make_transfer(PutTensorOnlyStore())
    tensor = torch.arange(6, dtype=torch.float32).reshape(2, 3)

    ref = transfer.put_object({"tensor": tensor})

    with pytest.raises(RuntimeError, match="does not support get_tensor"):
        transfer.get_object(ref)


def test_put_object_torch_tensor_raw_fallback_roundtrip() -> None:
    torch = pytest.importorskip("torch")
    store, transfer = make_transfer(NoTensorFastPathStore())
    tensor = torch.arange(6, dtype=torch.float32).reshape(2, 3)
    scalar_tensor = torch.tensor(1.5, dtype=torch.float32)
    bool_tensor = torch.tensor([True, False], dtype=torch.bool)

    result = transfer.get_object(
        transfer.put_object(
            {
                "tensor": tensor,
                "scalar": scalar_tensor,
                "bool_tensor": bool_tensor,
            }
        )
    )

    assert torch.equal(result["tensor"], tensor)
    assert torch.equal(result["scalar"], scalar_tensor)
    assert torch.equal(result["bool_tensor"], bool_tensor)
    assert store.batch_get_into_calls > 0


def test_bundle_read_spec_full_read_is_partial_special_case() -> None:
    store, transfer = make_transfer()
    array = np.arange(16, dtype=np.int32).reshape(4, 4)
    payload = structured_payload({"type": "example"}, array=array, raw=b"abc")

    ref = transfer.put_structured_object(payload)
    result = transfer.materialize(transfer.read_spec(ref))

    assert np.array_equal(result.objects["array"], array)
    assert result.objects["raw"] == b"abc"
    assert store.batch_get_into_calls > 0
    assert store.registered == set()


def test_structured_object_payload_metadata_defaults_empty() -> None:
    store, transfer = make_transfer()
    array = np.arange(8, dtype=np.int32).reshape(2, 4)

    ref = transfer.put_structured_object(
        StructuredObjectPayload(buffers={"array": array})
    )
    result = transfer.materialize(transfer.read_spec(ref))

    assert np.array_equal(result.objects["array"], array)


def test_bundle_chunked_full_read_via_read_spec() -> None:
    store, transfer = make_transfer()
    payload = bytes(range(128))

    ref = transfer.put_structured_object(
        structured_payload(payload=payload),
        chunk_bytes=17,
    )
    result = transfer.materialize(transfer.read_spec(ref))

    assert result.objects["payload"] == payload
    assert len(ref.manifest["buffers"]["payload"]["chunks"]) > 1


def test_bundle_put_falls_back_to_store_put_without_batch_put_support() -> None:
    store, transfer = make_transfer(MinimalStore())
    payload = bytes(range(128))

    ref = transfer.put_structured_object(
        structured_payload(payload=payload), chunk_bytes=17
    )
    result = transfer.materialize(transfer.read_spec(ref))

    assert result.objects["payload"] == payload
    assert len(ref.manifest["buffers"]["payload"]["chunks"]) > 1
    assert store.registered == set()


def test_bundle_uses_configurable_default_chunk_size() -> None:
    store, transfer = make_transfer(default_chunk_bytes=17)
    payload = bytes(range(128))

    ref = transfer.put_bundle(b"meta", {"payload": payload})

    assert len(ref.manifest["buffers"]["payload"]["chunks"]) > 1
    assert ref.manifest["buffers"]["payload"]["chunks"][0]["bytes"] == 17


def test_bundle_copy_mode_forces_store_put() -> None:
    store, transfer = make_transfer()
    payload = bytes(range(128))

    ref = transfer.put_structured_object(
        structured_payload(payload=payload),
        chunk_bytes=17,
        policy=BundleTransferPolicy(copy_mode="copy"),
    )

    assert store.batch_put_from_calls == 0
    assert store.register_buffer_calls == 0
    assert store.unregister_buffer_calls == 0

    result = transfer.materialize(transfer.read_spec(ref))
    assert result.objects["payload"] == payload


def test_structured_object_copy_mode_skips_pre_registered_validation() -> None:
    store, transfer = make_transfer()
    payload = np.arange(64, dtype=np.uint8).reshape(8, 8).T

    ref = transfer.put_structured_object(
        structured_payload(payload=payload),
        policy=BundleTransferPolicy(copy_mode="copy"),
        pre_registered_buffers={"payload": True},
    )

    assert store.batch_put_from_calls == 0
    assert store.register_buffer_calls == 0
    assert store.unregister_buffer_calls == 0

    result = transfer.materialize(transfer.read_spec(ref))
    assert np.array_equal(result.objects["payload"], payload)


def test_bundle_zero_copy_mode_requires_batch_put_support() -> None:
    _store, transfer = make_transfer(MinimalStore())

    with pytest.raises(RuntimeError, match="zero-copy put requested"):
        transfer.put_structured_object(
            structured_payload(payload=b"data"),
            policy=BundleTransferPolicy(copy_mode="zero_copy"),
        )


def test_structured_object_pre_registered_buffers_passthrough() -> None:
    store, transfer = make_transfer()
    payload = np.arange(64, dtype=np.uint8).reshape(8, 8)
    payload_ptr = ctypes.addressof(ctypes.c_char.from_buffer(payload))
    assert store.register_buffer(payload_ptr, int(payload.nbytes)) == 0
    store.register_buffer_calls = 0
    store.unregister_buffer_calls = 0

    ref = transfer.put_structured_object(
        structured_payload(payload=payload),
        pre_registered_buffers={"payload": True},
    )

    assert store.batch_put_from_calls > 0
    assert store.register_buffer_calls == 0
    assert store.unregister_buffer_calls == 0
    assert payload_ptr in store.registered

    result = transfer.materialize(transfer.read_spec(ref))
    assert np.array_equal(result.objects["payload"], payload)
    store.unregister_buffer(payload_ptr)


def test_structured_object_pre_registered_rejects_copied_buffers() -> None:
    _store, transfer = make_transfer()
    non_contiguous = np.arange(64, dtype=np.uint8).reshape(8, 8).T

    with pytest.raises(ValueError, match="pre-registered structured buffer"):
        transfer.put_structured_object(
            structured_payload(payload=non_contiguous),
            pre_registered_buffers={"payload": True},
        )

    with pytest.raises(ValueError, match="pre-registered structured buffer"):
        transfer.put_structured_object(
            structured_payload(payload=b"readonly"),
            pre_registered_buffers={"payload": True},
        )

    with pytest.raises(ValueError, match="unknown pre-registered"):
        transfer.put_structured_object(
            structured_payload(payload=bytearray(b"data")),
            pre_registered_buffers={"missing": True},
        )


def test_structured_object_pre_registered_empty_buffer() -> None:
    _store, transfer = make_transfer()
    payload = bytearray()

    ref = transfer.put_structured_object(
        structured_payload(payload=payload),
        pre_registered_buffers={"payload": True},
    )

    result = transfer.materialize(transfer.read_spec(ref))
    assert result.objects["payload"] == b""


def test_structured_object_roundtrip() -> None:
    store, transfer = make_transfer()
    array = np.arange(12, dtype=np.int16).reshape(3, 4)
    payload = structured_payload(
        {"type": "example", "fields": ["weights", "tokens"]},
        weights=array,
        tokens=b"abc",
    )

    ref = transfer.put_structured_object(payload)
    result = transfer.materialize(transfer.read_spec(ref))

    assert result.metadata == {
        "layout": "structured",
        "type": "example",
        "fields": ["weights", "tokens"],
        "__mooncake_structured_fields__": {
            "weights": {
                "encoding": "ndarray",
                "dtype": array.dtype.str,
                "shape": [3, 4],
            },
            "tokens": {"encoding": "bytes"},
        },
    }
    assert np.array_equal(result.objects["weights"], array)
    assert result.objects["weights"].flags["C_CONTIGUOUS"]
    assert result.objects["tokens"] == b"abc"


def test_structured_object_read_spec_select_members() -> None:
    store, transfer = make_transfer()
    array = np.arange(10, dtype=np.float32).reshape(2, 5)
    payload = structured_payload(weights=array, raw=b"payload")

    ref = transfer.put_structured_object(payload)
    spec = transfer.read_spec(ref).select_members(["weights"])
    assert isinstance(spec, StructuredObjectReadSpec)
    batch_get_into_calls = store.batch_get_into_calls
    result = transfer.materialize(spec)

    assert list(result.objects) == ["weights"]
    assert np.array_equal(result.objects["weights"], array)
    assert store.get_into_calls > 0
    assert store.get_into_ranges_calls == 0
    assert store.batch_get_into_calls == batch_get_into_calls + 1


def test_structured_object_multichunk_ndarray_uses_range_gather() -> None:
    store, transfer = make_transfer()
    array = np.arange(64, dtype=np.int16).reshape(8, 8)
    payload = structured_payload(weights=array)

    ref = transfer.put_structured_object(payload, chunk_bytes=32)
    batch_get_into_calls = store.batch_get_into_calls
    result = transfer.materialize(transfer.read_spec(ref))

    assert np.array_equal(result.objects["weights"], array)
    assert store.get_into_ranges_calls > 0
    assert store.batch_get_into_calls == batch_get_into_calls + 1


def test_structured_object_slice_member_uses_partial_range_reads() -> None:
    store, transfer = make_transfer()
    array = np.arange(96, dtype=np.int16).reshape(12, 8)
    payload = structured_payload(weights=array, raw=b"payload")

    ref = transfer.put_structured_object(payload, chunk_bytes=20)
    spec = (
        transfer.read_spec(ref)
        .select_members(["weights"])
        .slice_member("weights", axis=0, start=3, end=9)
    )
    before_range_reads = store.get_into_ranges_calls
    result = transfer.materialize(spec)

    assert np.array_equal(result.objects["weights"], array[3:9])
    assert result.objects["weights"].shape == (6, 8)
    assert store.get_into_ranges_calls == before_range_reads + 1


def test_structured_object_slice_member_falls_back_to_plain_get_reads() -> None:
    store, transfer = make_transfer(PlainStore())
    array = np.arange(96, dtype=np.int16).reshape(12, 8)
    payload = structured_payload(weights=array, raw=b"payload")

    ref = transfer.put_structured_object(payload, chunk_bytes=20)
    spec = (
        transfer.read_spec(ref)
        .select_members(["weights"])
        .slice_member("weights", axis=0, start=3, end=9)
    )
    result = transfer.materialize(spec)

    assert np.array_equal(result.objects["weights"], array[3:9])
    assert result.objects["weights"].shape == (6, 8)
    assert store.get_into_calls == 0
    assert store.get_into_ranges_calls == 0
    assert store.batch_get_into_calls == 0


def test_structured_object_materialize_into_reuses_destination() -> None:
    store, transfer = make_transfer()
    array = np.arange(96, dtype=np.float32).reshape(12, 8)
    payload = structured_payload(weights=array)

    ref = transfer.put_structured_object(payload, chunk_bytes=40)
    spec = transfer.read_spec(ref).slice_member("weights", axis=0, start=2, end=10)
    destination = np.empty((8, 8), dtype=np.float32)
    result = transfer.materialize_into(spec, destinations={"weights": destination})

    assert result.objects["weights"] is destination
    assert np.array_equal(destination, array[2:10])
    assert store.get_into_ranges_calls > 0


def test_structured_object_duplicate_destination_registration_is_tolerated() -> None:
    store, transfer = make_transfer(StrictRegisterStore())
    array = np.arange(96, dtype=np.float32).reshape(12, 8)
    payload = structured_payload(weights=array)

    ref = transfer.put_structured_object(payload, chunk_bytes=40)
    spec = (
        transfer.read_spec(ref)
        .select_members(["weights"])
        .slice_member("weights", axis=0, start=2, end=10)
    )
    destination = np.empty((8, 8), dtype=np.float32)
    destination_ptr = ctypes.addressof(ctypes.c_char.from_buffer(destination))
    assert store.register_buffer(destination_ptr, int(destination.nbytes)) == 0

    result = transfer.materialize_into(spec, destinations={"weights": destination})

    assert result.objects["weights"] is destination
    assert np.array_equal(destination, array[2:10])
    assert store.get_into_ranges_calls > 0
    assert destination_ptr in store.registered
    store.unregister_buffer(destination_ptr)


def test_structured_object_slice_member_step_copy() -> None:
    store, transfer = make_transfer()
    array = np.arange(120, dtype=np.int32).reshape(15, 8)
    payload = structured_payload(weights=array)

    ref = transfer.put_structured_object(payload, chunk_bytes=24)
    spec = transfer.read_spec(ref).slice_member(
        "weights", axis=0, start=1, end=12, step=3
    )
    result = transfer.materialize(spec)

    assert np.array_equal(result.objects["weights"], array[1:12:3])
    assert result.objects["weights"].shape == array[1:12:3].shape
    assert store.get_into_ranges_calls > 0


def test_structured_object_invalid_slice_and_destination_raise() -> None:
    store, transfer = make_transfer()
    array = np.arange(24, dtype=np.int16).reshape(6, 4)
    ref = transfer.put_structured_object(
        structured_payload(weights=array, raw=b"abc"),
        chunk_bytes=8,
    )

    with pytest.raises(ValueError, match="axis=0"):
        transfer.materialize(
            transfer.read_spec(ref).slice_member("weights", axis=1, start=0, end=2)
        )
    with pytest.raises(ValueError, match="step must be positive"):
        transfer.materialize(
            transfer.read_spec(ref).slice_member(
                "weights", axis=0, start=0, end=2, step=0
            )
        )
    with pytest.raises(ValueError, match="does not support slicing"):
        transfer.materialize(
            transfer.read_spec(ref).slice_member("raw", axis=0, start=0, end=1)
        )
    with pytest.raises(ValueError, match="shape mismatch"):
        transfer.materialize_into(
            transfer.read_spec(ref).slice_member("weights", axis=0, start=1, end=3),
            destinations={"weights": np.empty((3, 4), dtype=np.int16)},
        )
    readonly_destination = np.empty((2, 4), dtype=np.int16)
    readonly_destination.flags.writeable = False
    with pytest.raises(ValueError, match="writeable"):
        transfer.materialize_into(
            transfer.read_spec(ref).slice_member("weights", axis=0, start=1, end=3),
            destinations={"weights": readonly_destination},
        )


def test_bundle_remove_deletes_payload_and_manifest() -> None:
    store, transfer = make_transfer()

    ref = transfer.put_bundle(b"meta", {"payload": b"data"})
    assert store.objects
    transfer.remove_bundle(ref)

    assert store.objects == {}
    assert store.batch_remove_calls == 1


def test_bundle_partial_put_failure_cleans_payloads() -> None:
    store, transfer = make_transfer(FailingPutStore(fail_on_put=2))

    with pytest.raises(RuntimeError, match="injected put failure"):
        transfer.put_bundle(b"meta", {"payload": b"data"}, chunk_bytes=2)

    assert store.objects == {}


def test_bundle_cleanup_failure_preserves_put_error() -> None:
    _store, transfer = make_transfer(FailingRemoveStore(fail_on_put=2))

    with pytest.raises(RuntimeError, match="injected put failure"):
        transfer.put_bundle(b"meta", {"payload": b"data"}, chunk_bytes=2)


def test_bundle_remove_uses_force_batch_remove_when_available() -> None:
    store, transfer = make_transfer(ForceTrackingStore())

    ref = transfer.put_structured_object(
        structured_payload(payload=bytes(range(64))),
        chunk_bytes=8,
    )
    transfer.materialize(transfer.read_spec(ref))
    transfer.remove_bundle(ref)

    assert store.batch_remove_forces == [True]
    assert store.objects == {}


def test_bundle_remove_recovers_after_transient_batch_failure() -> None:
    store, transfer = make_transfer(TransientBatchRemoveStore())

    ref = transfer.put_bundle(b"meta", {"a": b"x", "b": b"y", "c": b"z"})
    transfer.remove_bundle(ref)

    assert store.objects == {}
    assert store.batch_remove_calls == 1


def test_bundle_concurrent_put_and_read_spec_full_read() -> None:
    store, transfer = make_transfer(GetOnlyStore())
    payload = bytes(range(128))

    ref = transfer.put_structured_object(
        structured_payload(payload=payload),
        chunk_bytes=8,
        policy=BundleTransferPolicy(max_inflight_put=4, put_mode="parallel"),
    )
    result = transfer.materialize(transfer.read_spec(ref))

    assert result.objects["payload"] == payload
    assert store.max_active_puts > 1
    assert store.max_active_gets >= 1


def test_bundle_duplicate_source_registration_is_tolerated() -> None:
    store, transfer = make_transfer(StrictRegisterStore())
    payload = np.arange(64, dtype=np.uint8).reshape(8, 8)
    payload_ptr = ctypes.addressof(ctypes.c_char.from_buffer(payload))
    assert store.register_buffer(payload_ptr, int(payload.nbytes)) == 0

    ref = transfer.put_structured_object(structured_payload(payload=payload))

    assert ref.manifest["buffers"]["payload"]["bytes"] == int(payload.nbytes)
    assert payload_ptr in store.registered
    store.unregister_buffer(payload_ptr)


def test_bundle_partial_register_failure_unwinds_registered_buffers() -> None:
    store, transfer = make_transfer(FailingRegisterStore(fail_on_register=2))
    payload = bytes(range(64))

    with pytest.raises(RuntimeError, match="register_buffer"):
        transfer.put_structured_object(
            structured_payload(payload=payload), chunk_bytes=32
        )

    assert store.registered == set()
    assert store.objects == {}


def test_bundle_batch_get_failure_unregisters_buffer() -> None:
    store, transfer = make_transfer(FailingBatchGetStore())
    ref = transfer.put_structured_object(
        structured_payload(payload=bytes(range(64))),
        chunk_bytes=8,
    )
    store.fail_key = ref.manifest["buffers"]["payload"]["chunks"][2]["key"]

    with pytest.raises(RuntimeError, match="injected get failure"):
        transfer.materialize(transfer.read_spec(ref))

    assert store.registered == set()


def test_bundle_falls_back_to_get_without_batch_get_into() -> None:
    store, transfer = make_transfer(GetOnlyStore())

    ref = transfer.put_structured_object(
        structured_payload(payload=b"abcdef"),
        chunk_bytes=2,
    )
    result = transfer.materialize(transfer.read_spec(ref))

    assert result.objects["payload"] == b"abcdef"
    assert store.max_active_gets >= 1


def test_bundle_read_fast_paths_require_registration_support() -> None:
    store, transfer = make_transfer(ReadFastPathWithoutRegisterStore())

    ref = transfer.put_structured_object(
        structured_payload(payload=bytes(range(64))),
        chunk_bytes=8,
    )
    result = transfer.materialize(transfer.read_spec(ref))

    assert result.objects["payload"] == bytes(range(64))
    assert store.batch_get_into_calls == 0
    assert store.get_into_calls == 0
    assert store.get_into_ranges_calls == 0


def test_bundle_invalid_policy_and_chunk_size_raise() -> None:
    store, transfer = make_transfer()

    with pytest.raises(ValueError, match="max_inflight_put"):
        transfer.put_bundle(b"meta", {"payload": b"data"}, max_inflight_put=0)
    with pytest.raises(ValueError, match="selected no members"):
        transfer.materialize(
            transfer.read_spec(
                {"manifest_key": "test/default/example/manifest"}
            ).select_members([])
        )
    with pytest.raises(ValueError, match="chunk_bytes"):
        transfer.put_bundle(b"meta", {"payload": b"data"}, chunk_bytes=0)
    with pytest.raises(ValueError, match="copy_mode"):
        transfer.put_bundle(
            b"meta",
            {"payload": b"data"},
            policy=BundleTransferPolicy(copy_mode="invalid"),
        )


def test_bundle_invalid_name_and_prefix_raise() -> None:
    store, transfer = make_transfer()

    with pytest.raises(ValueError, match="buffer name"):
        transfer.put_bundle(b"meta", {"bad/name": b"data"})
    with pytest.raises(ValueError, match="partition"):
        transfer.put_bundle(b"meta", {"payload": b"data"}, partition="bad/name")
    with pytest.raises(ValueError, match="key_prefix"):
        MooncakeBundleTransfer(store, key_prefix="")
    with pytest.raises(ValueError, match="chunk_bytes"):
        MooncakeBundleTransfer(store, default_chunk_bytes=0)


def test_structured_object_invalid_metadata_raises() -> None:
    store, transfer = make_transfer()

    with pytest.raises(TypeError, match="mapping"):
        transfer.put_structured_object(
            StructuredObjectPayload(metadata=["not", "a", "mapping"], buffers={})
        )
    with pytest.raises(TypeError, match="JSON-serializable"):
        transfer.put_structured_object(
            StructuredObjectPayload(metadata={"bad": object()}, buffers={})
        )
    with pytest.raises(ValueError, match="reserved"):
        transfer.put_structured_object(
            StructuredObjectPayload(
                metadata={"__mooncake_structured_fields__": {}},
                buffers={"payload": b"abc"},
            )
        )


def test_structured_object_rejects_invalid_field_spec() -> None:
    store, transfer = make_transfer()
    manifest_key = "test/default/example/manifest"
    meta_key = "test/default/example/meta"
    payload_key = "test/default/example/buffer/payload"
    metadata_blob = (
        b'{"layout":"structured","__mooncake_structured_fields__":'
        b'{"payload":{"encoding":"unknown"}}}'
    )
    store.objects[meta_key] = metadata_blob
    store.objects[payload_key] = b"abc"
    write_manifest(
        store,
        manifest_key,
        {
            "version": 1,
            "layout": "bundle",
            "object_id": "default/example",
            "meta": {
                "key": meta_key,
                "bytes": len(metadata_blob),
                "chunks": [{"key": meta_key, "bytes": len(metadata_blob)}],
            },
            "buffers": {
                "payload": {
                    "key": payload_key,
                    "bytes": 3,
                    "chunks": [{"key": payload_key, "bytes": 3}],
                }
            },
        },
    )

    with pytest.raises(ValueError, match="unsupported structured field encoding"):
        transfer.materialize(transfer.read_spec({"manifest_key": manifest_key}))


def test_bundle_rejects_tampered_manifest() -> None:
    store, transfer = make_transfer()
    ref = transfer.put_bundle(b"meta", {"payload": b"abcdef"}, chunk_bytes=2)
    tampered = dict(ref.manifest)
    tampered["buffers"] = dict(ref.manifest["buffers"])
    tampered["buffers"]["payload"] = dict(ref.manifest["buffers"]["payload"])
    tampered["buffers"]["payload"]["chunks"] = [
        {"key": "other/object", "bytes": 6},
    ]
    write_manifest(store, ref.manifest_key, tampered)

    with pytest.raises(ValueError, match="namespace"):
        transfer.materialize(transfer.read_spec({"manifest_key": ref.manifest_key}))

    with pytest.raises(ValueError, match="manifest_key"):
        transfer.remove_bundle(
            RemoteBundleRef(manifest_key="test/other/manifest", manifest=ref.manifest)
        )


def test_structured_object_torch_tensor_falls_back_without_buffer_pool() -> None:
    torch = pytest.importorskip("torch")
    mooncake_store = pytest.importorskip("mooncake.store")
    if not hasattr(mooncake_store, "_serialize_tensor") or not hasattr(
        mooncake_store, "_deserialize_tensor"
    ):
        pytest.skip("built mooncake.store lacks tensor serialization helpers")
    store, transfer = make_transfer()
    tensor = torch.arange(12, dtype=torch.int64).reshape(3, 4)

    ref = transfer.put_structured_object(
        StructuredObjectPayload(buffers={"tensor": tensor})
    )
    result = transfer.materialize(transfer.read_spec(ref))

    assert store.put_tensor_from_calls == 0
    assert store.batch_put_from_calls == 0
    assert torch.equal(result.objects["tensor"], tensor)


def test_structured_object_direct_torch_tensor_materialize_into_uses_real_store() -> (
    None
):
    torch = pytest.importorskip("torch")
    mooncake_store = pytest.importorskip("mooncake.store")
    if not hasattr(mooncake_store, "get_tensor_into") and not hasattr(
        mooncake_store.MooncakeDistributedStore, "get_tensor_into"
    ):
        pytest.skip("built mooncake.store lacks get_tensor_into")
    store = mooncake_store.MooncakeDistributedStore()
    rc = store.setup(
        os.getenv("LOCAL_HOSTNAME", "localhost"),
        os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE"),
        16 * 1024 * 1024,
        4 * 1024 * 1024,
        os.getenv("PROTOCOL", "tcp"),
        os.getenv("DEVICE_NAME", ""),
        os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
    )
    if rc != 0:
        pytest.skip(f"MooncakeDistributedStore setup failed: {rc}")
    transfer = MooncakeBundleTransfer(store, key_prefix="structured-test-direct")
    tensor = torch.arange(12, dtype=torch.int64).reshape(3, 4)
    ref = transfer.put_structured_object(
        StructuredObjectPayload(buffers={"tensor": tensor})
    )
    total_bytes = int(ref.manifest["buffers"]["tensor"]["bytes"])
    destination = ctypes.create_string_buffer(total_bytes)

    try:
        result = transfer.materialize_into(
            transfer.read_spec(ref),
            {
                "tensor": tensor_object_buffer(
                    ctypes.addressof(destination),
                    total_bytes,
                    destination,
                    batch_size=3,
                )
            },
        )
        assert torch.equal(result.objects["tensor"], tensor)
    finally:
        transfer.remove_bundle(ref)


def test_structured_object_torch_tensor_zero_copy_rejects_plain_tensor() -> None:
    torch = pytest.importorskip("torch")
    mooncake_store = pytest.importorskip("mooncake.store")
    if not hasattr(mooncake_store, "_serialize_tensor"):
        pytest.skip("built mooncake.store lacks tensor serialization helpers")
    _store, transfer = make_transfer()
    tensor = torch.arange(12, dtype=torch.int64).reshape(3, 4)

    with pytest.raises(ValueError, match="BufferPool tensor-object buffer"):
        transfer.put_structured_object(
            StructuredObjectPayload(buffers={"tensor": tensor}),
            policy=BundleTransferPolicy(copy_mode="zero_copy"),
        )


def test_structured_object_torch_tensor_slice_uses_range_reads() -> None:
    torch = pytest.importorskip("torch")
    mooncake_store = pytest.importorskip("mooncake.store")
    if not hasattr(mooncake_store, "_serialize_tensor") or not hasattr(
        mooncake_store, "_deserialize_tensor"
    ):
        pytest.skip("built mooncake.store lacks tensor serialization helpers")
    store, transfer = make_transfer(NoTensorFastPathStore())
    tensor = torch.arange(48, dtype=torch.int64).reshape(12, 4)
    ref = transfer.put_structured_object(
        StructuredObjectPayload(buffers={"tensor": tensor}), chunk_bytes=40
    )

    result = transfer.materialize(
        transfer.read_spec(ref)
        .select_members(["tensor"])
        .slice_member("tensor", axis=0, start=3, end=9)
    )

    assert torch.equal(result.objects["tensor"], tensor[3:9])
    assert store.get_into_ranges_calls >= 2


def test_structured_object_direct_torch_tensor_slice_uses_real_store_ranges() -> None:
    torch = pytest.importorskip("torch")
    mooncake_store = pytest.importorskip("mooncake.store")
    if not hasattr(mooncake_store, "_serialize_tensor") or not hasattr(
        mooncake_store, "_deserialize_tensor"
    ):
        pytest.skip("built mooncake.store lacks tensor serialization helpers")
    _store, transfer = real_transfer("structured-test-slice")
    tensor = torch.arange(48, dtype=torch.int64).reshape(12, 4)
    ref = transfer.put_structured_object(StructuredObjectPayload(buffers={"tensor": tensor}))

    try:
        result = transfer.materialize(
            transfer.read_spec(ref)
            .select_members(["tensor"])
            .slice_member("tensor", axis=0, start=3, end=9)
        )
        assert torch.equal(result.objects["tensor"], tensor[3:9])
    finally:
        transfer.remove_bundle(ref)


def test_structured_object_tensor_object_buffer_uses_put_tensor_from() -> None:
    store, transfer = make_transfer()
    source = ctypes.create_string_buffer(b"tensor-payload")
    store.register_buffer(ctypes.addressof(source), len(source.raw))

    ref = transfer.put_structured_object(
        StructuredObjectPayload(
            buffers={
                "tensor": tensor_object_buffer(
                    ctypes.addressof(source), len(source.raw), source, batch_size=1
                )
            }
        ),
        policy=BundleTransferPolicy(copy_mode="zero_copy"),
    )

    payload = ref.manifest["buffers"]["tensor"]
    assert store.put_tensor_from_calls == 1
    assert store.batch_put_from_calls == 0
    assert payload["bytes"] == len(source.raw)
    assert payload["chunks"] == [{"key": payload["key"], "bytes": len(source.raw)}]


def test_structured_object_tensor_object_buffer_materialize_into_uses_ranges() -> None:
    store, transfer = make_transfer()
    source = ctypes.create_string_buffer(b"tensor-payload")
    store.register_buffer(ctypes.addressof(source), len(source.raw))
    ref = transfer.put_structured_object(
        StructuredObjectPayload(
            buffers={
                "tensor": tensor_object_buffer(
                    ctypes.addressof(source), len(source.raw), source, batch_size=1
                )
            }
        ),
        policy=BundleTransferPolicy(copy_mode="zero_copy"),
    )
    destination = ctypes.create_string_buffer(len(source.raw))

    result = transfer.materialize_into(
        transfer.read_spec(ref),
        {
            "tensor": tensor_object_buffer(
                ctypes.addressof(destination),
                len(destination.raw),
                destination,
                batch_size=1,
            )
        },
    )

    assert result.objects["tensor"].ptr == ctypes.addressof(destination)
    assert destination.raw == source.raw
    assert store.get_into_ranges_calls == 1


def test_structured_object_torch_tensor_zero_copy_uses_real_buffer_pool() -> None:
    torch = pytest.importorskip("torch")
    mooncake_store = pytest.importorskip("mooncake.store")
    if (
        not hasattr(mooncake_store, "BufferPool")
        or not hasattr(mooncake_store, "_serialize_tensor")
        or not hasattr(mooncake_store, "_deserialize_tensor")
    ):
        pytest.skip("built mooncake.store lacks BufferPool tensor helpers")
    store = mooncake_store.MooncakeDistributedStore()
    rc = store.setup(
        os.getenv("LOCAL_HOSTNAME", "localhost"),
        os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE"),
        16 * 1024 * 1024,
        4 * 1024 * 1024,
        os.getenv("PROTOCOL", "tcp"),
        os.getenv("DEVICE_NAME", ""),
        os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
    )
    if rc != 0:
        pytest.skip(f"MooncakeDistributedStore setup failed: {rc}")
    pool = mooncake_store.BufferPool(store, min_size_class=4096, alignment=4096)
    transfer = MooncakeBundleTransfer(
        store, key_prefix="structured-test", buffer_pool=pool
    )
    tensor = torch.arange(12, dtype=torch.int64).reshape(3, 4)
    metadata, data_ptr, tensor_nbytes, owner = mooncake_store._serialize_tensor(tensor)
    total_bytes = len(metadata) + int(tensor_nbytes)
    lease = pool.acquire(total_bytes)
    view = lease.buffer
    view[: len(metadata)] = metadata
    ctypes.memmove(lease.ptr + len(metadata), int(data_ptr), int(tensor_nbytes))
    view.release()

    try:
        ref = transfer.put_structured_object(
            StructuredObjectPayload(
                buffers={
                    "tensor": tensor_object_buffer(
                        lease.ptr, total_bytes, lease, batch_size=3
                    )
                }
            ),
            policy=BundleTransferPolicy(copy_mode="zero_copy"),
        )
        result = transfer.materialize(transfer.read_spec(ref))
        assert torch.equal(result.objects["tensor"], tensor)
        _ = owner
    finally:
        lease.release()
        pool.close()


def test_dataproto_helper_requires_batch_size_for_tensor_object_buffer() -> None:
    _store, transfer = make_transfer()
    data = SimpleDataProto(batch={"tensor": tensor_object_buffer(1, 128, object())})

    with pytest.raises(TypeError, match="batch_size"):
        transfer.put_dataproto(data, policy=BundleTransferPolicy(copy_mode="zero_copy"))


def test_dataproto_helper_roundtrip_uses_structured_object() -> None:
    store, transfer = make_transfer()
    data = SimpleDataProto(
        batch={"input_ids": np.arange(12, dtype=np.int64).reshape(4, 3)},
        non_tensor_batch={
            "reward": np.asarray([1.0, 0.0, 0.5, -1.0], dtype=np.float32)
        },
        meta_info={"step": 7, "source": "unit"},
    )

    ref = transfer.put_dataproto(
        data, namespace="roll", partition="train", stage="rollout"
    )
    result = transfer.get_dataproto(ref, data_cls=SimpleDataProto)

    assert ref.batch_size == 4
    assert set(ref.stage_refs) == {"rollout"}
    assert set(ref.field_index) == {"input_ids", "reward"}
    assert np.array_equal(result.batch["input_ids"], data.batch["input_ids"])
    assert np.array_equal(
        result.non_tensor_batch["reward"], data.non_tensor_batch["reward"]
    )
    assert result.meta_info == data.meta_info
    stage_metadata = transfer.materialize(
        transfer.read_spec(ref.stage_refs["rollout"]).select_members(
            ["batch.input_ids"]
        )
    ).metadata
    assert stage_metadata["dataproto"]["stage"] == "rollout"


def test_dataproto_manifest_view_reuses_structured_manifest_specs() -> None:
    _store, transfer = make_transfer()
    data = SimpleDataProto(
        batch={"input_ids": np.arange(12, dtype=np.int64).reshape(4, 3)},
        non_tensor_batch={
            "reward": np.asarray([1.0, 0.0, 0.5, -1.0], dtype=np.float32)
        },
        meta_info={"step": 7},
    )

    ref = transfer.put_dataproto(
        data, namespace="roll", partition="train", stage="rollout"
    )
    view = transfer.dataproto_manifest_view(ref)

    assert view["namespace"] == "roll"
    assert view["partition"] == "train"
    assert view["batch_size"] == 4
    assert view["stages"]["rollout"]["dataproto"]["stage"] == "rollout"
    assert view["batch_fields"]["input_ids"]["member"] == "batch.input_ids"
    assert view["batch_fields"]["input_ids"]["spec"]["encoding"] == "ndarray"
    assert view["batch_fields"]["input_ids"]["spec"]["shape"] == [4, 3]
    assert view["non_tensor_fields"]["reward"]["spec"]["dtype"] == "<f4"
    assert view["meta_info_keys"] == ["step"]


def test_dataproto_helper_selects_fields_and_meta() -> None:
    _store, transfer = make_transfer()
    data = SimpleDataProto(
        batch={
            "input_ids": np.arange(8, dtype=np.int64).reshape(4, 2),
            "attention_mask": np.ones((4, 2), dtype=np.int32),
        },
        non_tensor_batch={
            "reward": np.asarray([1.0, 0.0, 0.5, -1.0], dtype=np.float32),
            "uid": np.arange(4, dtype=np.int64),
        },
        meta_info={"step": 7, "source": "unit"},
    )
    ref = transfer.put_dataproto(data)

    result = transfer.get_dataproto(
        ref,
        fields=["input_ids", "reward"],
        meta_info_keys=["step"],
    )
    batch_only = transfer.get_dataproto(ref, batch_fields=["input_ids"])
    non_tensor_only = transfer.get_dataproto(ref, non_tensor_fields=["reward"])

    assert set(result["batch"]) == {"input_ids"}
    assert set(result["non_tensor_batch"]) == {"reward"}
    assert result["meta_info"] == {"step": 7}
    assert np.array_equal(result["batch"]["input_ids"], data.batch["input_ids"])
    assert np.array_equal(
        result["non_tensor_batch"]["reward"], data.non_tensor_batch["reward"]
    )
    assert set(batch_only["batch"]) == {"input_ids"}
    assert batch_only["non_tensor_batch"] == {}
    assert np.array_equal(batch_only["batch"]["input_ids"], data.batch["input_ids"])
    assert non_tensor_only["batch"] == {}
    assert set(non_tensor_only["non_tensor_batch"]) == {"reward"}
    assert np.array_equal(
        non_tensor_only["non_tensor_batch"]["reward"], data.non_tensor_batch["reward"]
    )


def test_dataproto_helper_reads_rows_with_real_store_ranges() -> None:
    torch = pytest.importorskip("torch")
    mooncake_store = pytest.importorskip("mooncake.store")
    if not hasattr(mooncake_store, "_serialize_tensor") or not hasattr(
        mooncake_store, "_deserialize_tensor"
    ):
        pytest.skip("built mooncake.store lacks tensor serialization helpers")
    _store, transfer = real_transfer("structured-test-dataproto-rows")
    tensor = torch.arange(24, dtype=torch.int64).reshape(6, 4)
    data = SimpleDataProto(
        batch={
            "tensor": tensor,
            "array": np.arange(18, dtype=np.int64).reshape(6, 3),
        },
        non_tensor_batch={
            "text": np.asarray(
                ["a", "bb", "ccc", "dddd", "eeeee", "ffffff"], dtype=object
            ),
            "json": np.asarray(
                [{"i": 0}, {"i": 1}, {"i": 2}, {"i": 3}, {"i": 4}, {"i": 5}],
                dtype=object,
            ),
            "ragged": np.asarray(
                [
                    torch.arange(1, dtype=torch.float32),
                    torch.arange(2, dtype=torch.float32),
                    torch.arange(3, dtype=torch.float32),
                    torch.arange(4, dtype=torch.float32),
                    torch.arange(5, dtype=torch.float32),
                    torch.arange(6, dtype=torch.float32),
                ],
                dtype=object,
            ),
        },
    )
    ref = transfer.put_dataproto(data)

    try:
        sliced = transfer.get_dataproto(ref, rows=slice(2, 5))
        gathered = transfer.get_dataproto(ref, rows=[4, 1, 3])
        array_dst = np.empty((3, 3), dtype=np.int64)
        into = transfer.get_dataproto(
            ref,
            batch_fields=["array"],
            rows=[4, 1, 3],
            destinations={"array": array_dst},
        )
        pool = mooncake_store.BufferPool(_store, 1024 * 1024)
        raw_array = pool.acquire(array_dst.nbytes)
        raw_array_result = transfer.get_dataproto(
            ref,
            batch_fields=["array"],
            rows=[4, 1, 3],
            destinations={
                "array": raw_destination(
                    raw_array.ptr,
                    raw_array.size,
                    raw_array,
                    pre_registered=True,
                )
            },
        )
        tensor_payload_bytes = int(
            ref.stage_refs["default"].manifest["buffers"]["batch.tensor"]["metadata_bytes"]
        ) + tensor[[4, 1, 3]].numel() * tensor.element_size()
        raw_tensor = pool.acquire(tensor_payload_bytes)
        raw_tensor_result = transfer.get_dataproto(
            ref,
            batch_fields=["tensor"],
            rows=[4, 1, 3],
            destinations={
                "tensor": raw_destination(
                    raw_tensor.ptr,
                    raw_tensor.size,
                    raw_tensor,
                    pre_registered=True,
                )
            },
        )

        assert torch.equal(sliced["batch"]["tensor"], tensor[2:5])
        assert np.array_equal(sliced["batch"]["array"], data.batch["array"][2:5])
        assert sliced["non_tensor_batch"]["text"].tolist() == [
            "ccc",
            "dddd",
            "eeeee",
        ]
        assert sliced["non_tensor_batch"]["json"].tolist() == [
            {"i": 2},
            {"i": 3},
            {"i": 4},
        ]
        actual_ragged = sliced["non_tensor_batch"]["ragged"]
        assert torch.equal(actual_ragged[0], data.non_tensor_batch["ragged"][2])
        assert torch.equal(actual_ragged[1], data.non_tensor_batch["ragged"][3])
        assert torch.equal(actual_ragged[2], data.non_tensor_batch["ragged"][4])
        assert torch.equal(gathered["batch"]["tensor"], tensor[[4, 1, 3]])
        assert np.array_equal(gathered["batch"]["array"], data.batch["array"][[4, 1, 3]])
        assert gathered["non_tensor_batch"]["text"].tolist() == ["eeeee", "bb", "dddd"]
        assert into["batch"]["array"] is array_dst
        assert np.array_equal(array_dst, data.batch["array"][[4, 1, 3]])
        assert np.array_equal(
            raw_array_result["batch"]["array"], data.batch["array"][[4, 1, 3]]
        )
        assert raw_tensor_result["batch"]["tensor"].ptr == raw_tensor.ptr
        decoded_tensor = mooncake_store._deserialize_tensor(
            bytes(raw_tensor.buffer[:tensor_payload_bytes])
        )
        assert torch.equal(decoded_tensor, tensor[[4, 1, 3]])
        raw_array.release()
        raw_tensor.release()
        pool.close()
    finally:
        transfer.cleanup_dataproto(ref)


def test_dataproto_helper_supports_dict_cls_and_reports_bad_cls() -> None:
    _store, transfer = make_transfer()
    ref = transfer.put_dataproto(
        SimpleDataProto(batch={"input_ids": np.arange(4)}, meta_info={"step": 1})
    )

    result = transfer.get_dataproto(ref, data_cls=dict)

    assert result["meta_info"] == {"step": 1}
    assert np.array_equal(result["batch"]["input_ids"], np.arange(4))
    with pytest.raises(TypeError, match="cannot be constructed"):
        transfer.get_dataproto(ref, data_cls=BadDataProto)


def test_dataproto_helper_accepts_legacy_dict_inputs() -> None:
    _store, transfer = make_transfer()
    plain_ref = transfer.put_dataproto({"input_ids": np.arange(4)})
    envelope_ref = transfer.put_dataproto(
        {
            "batch": {"tokens": np.arange(6).reshape(3, 2)},
            "non_tensor_batch": {"uid": np.asarray(["a", "b", "c"], dtype=object)},
            "meta_info": {"step": 3},
        }
    )

    plain = transfer.get_dataproto(plain_ref)
    envelope = transfer.get_dataproto(envelope_ref)

    assert np.array_equal(plain["batch"]["input_ids"], np.arange(4))
    assert np.array_equal(envelope["batch"]["tokens"], np.arange(6).reshape(3, 2))
    assert envelope["non_tensor_batch"]["uid"].tolist() == ["a", "b", "c"]
    assert envelope["meta_info"] == {"step": 3}


def test_dataproto_helper_treats_reserved_plain_dict_keys_as_batch_fields() -> None:
    _store, transfer = make_transfer()
    ref = transfer.put_dataproto({"batch": np.arange(4), "meta_info": np.arange(4)})

    result = transfer.get_dataproto(ref)

    assert np.array_equal(result["batch"]["batch"], np.arange(4))
    assert np.array_equal(result["batch"]["meta_info"], np.arange(4))
    assert result["meta_info"] == {}


def test_dataproto_helper_rejects_cross_section_field_name_collision() -> None:
    _store, transfer = make_transfer()

    with pytest.raises(ValueError, match="overlap"):
        transfer.put_dataproto(
            {
                "batch": {"uid": np.arange(4)},
                "non_tensor_batch": {"uid": np.asarray(["a", "b", "c", "d"])},
            }
        )


def test_dataproto_ref_handle_roundtrip_materializes_and_cleans_up() -> None:
    store, transfer = make_transfer()
    input_ids = np.arange(6, dtype=np.int64).reshape(3, 2)
    ref = transfer.put_dataproto(
        SimpleDataProto(
            batch={"input_ids": input_ids},
            non_tensor_batch={"uid": np.asarray(["a", "b", "c"], dtype=object)},
            meta_info={"step": 3, "tags": ["rollout"]},
        ),
        namespace="roll",
        partition="train",
        stage="rollout",
    )
    handle = export_dataproto_ref(ref)

    assert is_dataproto_ref_handle(handle)
    assert handle["stage_refs"] == {
        "rollout": {"manifest_key": ref.stage_refs["rollout"].manifest_key}
    }
    imported = import_dataproto_ref(handle)
    assert imported.stage_refs["rollout"].manifest == {}

    result = transfer.get_dataproto(handle)
    assert np.array_equal(result["batch"]["input_ids"], input_ids)
    assert result["non_tensor_batch"]["uid"].tolist() == ["a", "b", "c"]
    assert result["meta_info"] == {"step": 3, "tags": ["rollout"]}
    assert transfer.dataproto_manifest_view(handle)["meta_info_keys"] == [
        "step",
        "tags",
    ]

    transfer.cleanup_dataproto(handle)
    assert store.objects == {}


def test_dataproto_ref_handle_exports_numpy_meta_and_validates_indexes() -> None:
    _store, transfer = make_transfer()
    ref = transfer.put_dataproto(
        SimpleDataProto(
            batch={"input_ids": np.arange(4)},
            meta_info={"scores": np.asarray([1, 2], dtype=np.int64)},
        )
    )
    ref.global_indexes = [0, 2]

    handle = export_dataproto_ref(ref)
    imported = import_dataproto_ref(handle)

    assert handle["meta_info"] == {"scores": [1, 2]}
    assert imported.global_indexes == [0, 2]
    bad_indexes = dict(handle)
    bad_indexes["global_indexes"] = "0,2"
    with pytest.raises(ValueError, match="global_indexes"):
        import_dataproto_ref(bad_indexes)


def test_dataproto_ref_handle_rejects_unknown_field_stage() -> None:
    _store, transfer = make_transfer()
    ref = transfer.put_dataproto(SimpleDataProto(batch={"input_ids": np.arange(4)}))
    handle = export_dataproto_ref(ref)
    handle["field_index"]["input_ids"] = dict(handle["field_index"]["input_ids"])
    handle["field_index"]["input_ids"]["stage"] = "missing"

    with pytest.raises(ValueError, match="unknown stage"):
        import_dataproto_ref(handle)


def test_dataproto_helper_appends_stage_fields_without_rewriting_existing() -> None:
    store, transfer = make_transfer()
    rollout = SimpleDataProto(
        batch={"input_ids": np.arange(8, dtype=np.int64).reshape(4, 2)},
        meta_info={"step": 1},
    )
    ref = transfer.put_dataproto(rollout, stage="rollout")
    initial_object_count = len(store.objects)

    old_log_prob = SimpleDataProto(
        batch={"old_log_probs": np.arange(4, dtype=np.float32)},
        meta_info={"stage": "old_log_prob"},
    )
    ref = transfer.append_dataproto_fields(ref, old_log_prob, stage="old_log_prob")
    result = transfer.get_dataproto(ref)

    assert set(ref.stage_refs) == {"rollout", "old_log_prob"}
    assert len(store.objects) > initial_object_count
    assert np.array_equal(result["batch"]["input_ids"], rollout.batch["input_ids"])
    assert np.array_equal(
        result["batch"]["old_log_probs"], old_log_prob.batch["old_log_probs"]
    )
    assert result["meta_info"] == {"step": 1, "stage": "old_log_prob"}

    with pytest.raises(ValueError, match="already exist"):
        transfer.append_dataproto_fields(ref, rollout, stage="duplicate")


def test_dataproto_helper_same_stage_append_reads_rows_with_real_store() -> None:
    pytest.importorskip("mooncake.store")
    _store, transfer = real_transfer("structured-test-dataproto-same-stage")
    input_ids = np.arange(24, dtype=np.int64).reshape(6, 4)
    old_log_probs = np.linspace(0.0, 1.0, 6, dtype=np.float32)
    text = np.asarray(["", "bb", "ccc", "dddd", "eeeee", "ffffff"], dtype=object)
    ref = transfer.put_dataproto(
        SimpleDataProto(batch={"input_ids": input_ids}, meta_info={"step": 1}),
        stage="rollout",
    )
    old_ref = ref
    old_manifest_key = ref.stage_refs["rollout"].manifest_key
    ref = transfer.append_dataproto_fields(
        ref,
        SimpleDataProto(
            batch={"old_log_probs": old_log_probs},
            non_tensor_batch={"text": text},
            meta_info={"stage": "rollout-extra"},
        ),
        stage="rollout",
    )

    try:
        old_result = transfer.get_dataproto(old_ref)
        result = transfer.get_dataproto(ref)
        sliced = transfer.get_dataproto(ref, rows=slice(2, 5))
        gathered = transfer.get_dataproto(
            ref, fields=["input_ids", "text"], rows=[4, 1, 3]
        )
        view = transfer.dataproto_manifest_view(ref)

        assert set(ref.stage_refs) == {"rollout"}
        assert ref.stage_refs["rollout"].manifest_key != old_manifest_key
        assert np.array_equal(old_result["batch"]["input_ids"], input_ids)
        assert np.array_equal(result["batch"]["input_ids"], input_ids)
        assert np.array_equal(result["batch"]["old_log_probs"], old_log_probs)
        assert result["non_tensor_batch"]["text"].tolist() == text.tolist()
        assert result["meta_info"] == {"step": 1, "stage": "rollout-extra"}
        assert np.array_equal(sliced["batch"]["input_ids"], input_ids[2:5])
        assert np.array_equal(sliced["batch"]["old_log_probs"], old_log_probs[2:5])
        assert sliced["non_tensor_batch"]["text"].tolist() == text[2:5].tolist()
        assert np.array_equal(gathered["batch"]["input_ids"], input_ids[[4, 1, 3]])
        assert gathered["non_tensor_batch"]["text"].tolist() == text[[4, 1, 3]].tolist()
        assert view["batch_fields"]["input_ids"]["stage"] == "rollout"
        assert view["batch_fields"]["old_log_probs"]["stage"] == "rollout"
        assert view["non_tensor_fields"]["text"]["stage"] == "rollout"
        assert set(ref.encoded_non_tensor) == {"text"}
    finally:
        transfer.cleanup_dataproto(ref)


def test_dataproto_helper_reads_rollout_transfer_data_with_real_store() -> None:
    pytest.importorskip("mooncake.store")
    _store, transfer = real_transfer("structured-test-rollout-transfer")
    row_ids = [f"rollout-row-{index}" for index in range(6)]
    input_ids = np.arange(48, dtype=np.int64).reshape(6, 8)
    attention_mask = (input_ids % 3 != 0).astype(np.int32)
    position_ids = np.tile(np.arange(8, dtype=np.int64), (6, 1))
    responses = (input_ids[:, -3:] + 100).astype(np.int64)
    response_mask = np.ones((6, 3), dtype=np.int32)
    prompts = np.asarray(
        ["", "prompt-b", "prompt-c", "prompt-d", "prompt-e", "prompt-f"],
        dtype=object,
    )
    sample_meta = np.asarray(
        [{"row": index, "tag": row_id} for index, row_id in enumerate(row_ids)],
        dtype=object,
    )
    old_log_probs = np.linspace(-0.5, 0.5, 18, dtype=np.float32).reshape(6, 3)
    ref_log_probs = old_log_probs + np.float32(0.25)
    rewards = np.linspace(0.0, 1.0, 6, dtype=np.float32)
    advantages = rewards + np.float32(10.0)
    returns = rewards + np.float32(20.0)
    values = rewards + np.float32(30.0)
    rollout_data = {
        "batch": {
            "input_ids": input_ids,
            "attention_mask": attention_mask,
            "position_ids": position_ids,
            "responses": responses,
            "response_mask": response_mask,
        },
        "non_tensor_batch": {
            "prompts": prompts,
            "sample_meta": sample_meta,
        },
        "meta_info": {"roll_row_ids": row_ids, "global_step": 7},
    }
    logprob_data = {
        "batch": {
            "old_log_probs": old_log_probs,
            "ref_log_probs": ref_log_probs,
        },
        "non_tensor_batch": {},
        "meta_info": {"logprob_stage": "complete"},
    }
    value_data = {
        "batch": {
            "rewards": rewards,
            "advantages": advantages,
            "returns": returns,
            "values": values,
        },
        "non_tensor_batch": {},
        "meta_info": {"critic_stage": "complete"},
    }
    ref = transfer.put_dataproto(
        rollout_data,
        namespace="roll",
        partition="remote-batch",
        stage="rollout",
    )
    ref = transfer.append_dataproto_fields(ref, logprob_data, stage="logprob")
    ref = transfer.append_dataproto_fields(ref, value_data, stage="rollout")
    handle = export_dataproto_ref(ref)

    try:
        full = transfer.get_dataproto(handle)
        selected = transfer.get_dataproto(
            handle,
            fields=["input_ids", "old_log_probs", "advantages", "prompts"],
            meta_info_keys=["roll_row_ids"],
        )
        sliced = transfer.get_dataproto(
            handle,
            fields=["responses", "ref_log_probs", "returns", "sample_meta"],
            rows=slice(1, 5),
        )
        gathered = transfer.get_dataproto(
            handle,
            fields=["attention_mask", "values", "prompts", "sample_meta"],
            rows=[5, 0, 3],
        )
        imported = import_dataproto_ref(handle)
        imported_selected = transfer.get_dataproto(
            imported,
            batch_fields=["position_ids", "rewards"],
            rows=[2, 4],
        )
        view = transfer.dataproto_manifest_view(handle)

        assert full["meta_info"] == {
            "roll_row_ids": row_ids,
            "global_step": 7,
            "logprob_stage": "complete",
            "critic_stage": "complete",
        }
        assert np.array_equal(full["batch"]["input_ids"], input_ids)
        assert np.array_equal(full["batch"]["attention_mask"], attention_mask)
        assert np.array_equal(full["batch"]["position_ids"], position_ids)
        assert np.array_equal(full["batch"]["responses"], responses)
        assert np.array_equal(full["batch"]["response_mask"], response_mask)
        assert np.array_equal(full["batch"]["old_log_probs"], old_log_probs)
        assert np.array_equal(full["batch"]["ref_log_probs"], ref_log_probs)
        assert np.array_equal(full["batch"]["rewards"], rewards)
        assert np.array_equal(full["batch"]["advantages"], advantages)
        assert np.array_equal(full["batch"]["returns"], returns)
        assert np.array_equal(full["batch"]["values"], values)
        assert full["non_tensor_batch"]["prompts"].tolist() == prompts.tolist()
        assert full["non_tensor_batch"]["sample_meta"].tolist() == sample_meta.tolist()

        assert set(selected["batch"]) == {"input_ids", "old_log_probs", "advantages"}
        assert set(selected["non_tensor_batch"]) == {"prompts"}
        assert selected["meta_info"] == {"roll_row_ids": row_ids}
        assert np.array_equal(selected["batch"]["input_ids"], input_ids)
        assert np.array_equal(selected["batch"]["old_log_probs"], old_log_probs)
        assert np.array_equal(selected["batch"]["advantages"], advantages)
        assert selected["non_tensor_batch"]["prompts"].tolist() == prompts.tolist()

        assert np.array_equal(sliced["batch"]["responses"], responses[1:5])
        assert np.array_equal(sliced["batch"]["ref_log_probs"], ref_log_probs[1:5])
        assert np.array_equal(sliced["batch"]["returns"], returns[1:5])
        assert sliced["non_tensor_batch"]["sample_meta"].tolist() == sample_meta[1:5].tolist()

        assert np.array_equal(gathered["batch"]["attention_mask"], attention_mask[[5, 0, 3]])
        assert np.array_equal(gathered["batch"]["values"], values[[5, 0, 3]])
        assert gathered["non_tensor_batch"]["prompts"].tolist() == prompts[[5, 0, 3]].tolist()
        assert gathered["non_tensor_batch"]["sample_meta"].tolist() == sample_meta[[5, 0, 3]].tolist()

        assert np.array_equal(imported_selected["batch"]["position_ids"], position_ids[[2, 4]])
        assert np.array_equal(imported_selected["batch"]["rewards"], rewards[[2, 4]])
        assert view["batch_fields"]["input_ids"]["stage"] == "rollout"
        assert view["batch_fields"]["old_log_probs"]["stage"] == "logprob"
        assert view["batch_fields"]["advantages"]["stage"] == "rollout"
        assert view["non_tensor_fields"]["prompts"]["stage"] == "rollout"
    finally:
        transfer.cleanup_dataproto(ref)


def test_dataproto_helper_reads_large_rollout_transfer_data_with_real_store() -> None:
    pytest.importorskip("mooncake.store")
    _store, transfer = real_transfer("structured-test-large-rollout-transfer")
    batch_size = 96
    prompt_len = 128
    response_len = 64
    total_len = prompt_len + response_len
    row_ids = [f"large-rollout-row-{index}" for index in range(batch_size)]
    token_grid = np.arange(batch_size * total_len, dtype=np.int64).reshape(
        batch_size, total_len
    )
    input_ids = token_grid + 1000
    attention_mask = (token_grid % 7 != 0).astype(np.int32)
    position_ids = np.tile(np.arange(total_len, dtype=np.int64), (batch_size, 1))
    responses = input_ids[:, prompt_len:]
    response_mask = np.ones((batch_size, response_len), dtype=np.int32)
    action_log_probs = np.linspace(
        -3.0, 3.0, batch_size * response_len, dtype=np.float32
    ).reshape(batch_size, response_len)
    ref_log_probs = action_log_probs + np.float32(0.125)
    values = np.linspace(0.0, 1.0, batch_size, dtype=np.float32)
    rewards = values + np.float32(1.0)
    advantages = values + np.float32(2.0)
    returns = values + np.float32(3.0)
    prompts = np.asarray(
        ["" if index % 17 == 0 else f"prompt-{index}-" + "x" * (index % 23) for index in range(batch_size)],
        dtype=object,
    )
    sample_meta = np.asarray(
        [
            {"row": index, "row_id": row_id, "prompt_len": prompt_len, "response_len": response_len}
            for index, row_id in enumerate(row_ids)
        ],
        dtype=object,
    )
    ref = transfer.put_dataproto(
        {
            "batch": {
                "input_ids": input_ids,
                "attention_mask": attention_mask,
                "position_ids": position_ids,
                "responses": responses,
                "response_mask": response_mask,
            },
            "non_tensor_batch": {
                "prompts": prompts,
                "sample_meta": sample_meta,
            },
            "meta_info": {"roll_row_ids": row_ids, "global_step": 11},
        },
        namespace="roll",
        partition="large-remote-batch",
        stage="rollout",
    )
    ref = transfer.append_dataproto_fields(
        ref,
        {
            "batch": {
                "action_log_probs": action_log_probs,
                "ref_log_probs": ref_log_probs,
            },
            "non_tensor_batch": {},
            "meta_info": {"logprob_stage": "complete"},
        },
        stage="logprob",
    )
    ref = transfer.append_dataproto_fields(
        ref,
        {
            "batch": {
                "values": values,
                "rewards": rewards,
                "advantages": advantages,
                "returns": returns,
            },
            "non_tensor_batch": {},
            "meta_info": {"critic_stage": "complete"},
        },
        stage="rollout",
    )
    handle = export_dataproto_ref(ref)
    gathered_rows = [95, 0, 63, 17, 42]

    try:
        selected = transfer.get_dataproto(
            handle,
            fields=["input_ids", "action_log_probs", "values", "prompts"],
            rows=slice(12, 76),
        )
        gathered = transfer.get_dataproto(
            handle,
            fields=["responses", "ref_log_probs", "returns", "sample_meta"],
            rows=gathered_rows,
        )
        destination = np.empty((len(gathered_rows), total_len), dtype=np.int64)
        into = transfer.get_dataproto(
            handle,
            batch_fields=["position_ids"],
            rows=gathered_rows,
            destinations={"position_ids": destination},
        )
        meta_only = transfer.get_dataproto(handle, fields=[], meta_info_keys=["roll_row_ids"])
        full_tail = transfer.get_dataproto(
            handle,
            fields=["attention_mask", "advantages"],
            rows=slice(batch_size - 8, batch_size),
        )

        assert np.array_equal(selected["batch"]["input_ids"], input_ids[12:76])
        assert np.array_equal(
            selected["batch"]["action_log_probs"], action_log_probs[12:76]
        )
        assert np.array_equal(selected["batch"]["values"], values[12:76])
        assert selected["non_tensor_batch"]["prompts"].tolist() == prompts[12:76].tolist()
        assert np.array_equal(gathered["batch"]["responses"], responses[gathered_rows])
        assert np.array_equal(
            gathered["batch"]["ref_log_probs"], ref_log_probs[gathered_rows]
        )
        assert np.array_equal(gathered["batch"]["returns"], returns[gathered_rows])
        assert gathered["non_tensor_batch"]["sample_meta"].tolist() == sample_meta[gathered_rows].tolist()
        assert into["batch"]["position_ids"] is destination
        assert np.array_equal(destination, position_ids[gathered_rows])
        assert meta_only["batch"] == {}
        assert meta_only["non_tensor_batch"] == {}
        assert meta_only["meta_info"] == {"roll_row_ids": row_ids}
        assert np.array_equal(
            full_tail["batch"]["attention_mask"], attention_mask[batch_size - 8 :]
        )
        assert np.array_equal(full_tail["batch"]["advantages"], advantages[batch_size - 8 :])
    finally:
        transfer.cleanup_dataproto(ref)


def test_dataproto_helper_selection_errors_and_destinations_with_real_store() -> None:
    pytest.importorskip("mooncake.store")
    _store, transfer = real_transfer("structured-test-selection-destinations")
    batch_size = 12
    input_ids = np.arange(batch_size * 12, dtype=np.int64).reshape(batch_size, 12)
    scores = np.linspace(0.0, 1.0, batch_size, dtype=np.float32)
    prompts = np.asarray([f"prompt-{index}" for index in range(batch_size)], dtype=object)
    ref = transfer.put_dataproto(
        {
            "batch": {"input_ids": input_ids, "scores": scores},
            "non_tensor_batch": {"prompts": prompts},
            "meta_info": {"step": 1},
        },
        namespace="roll",
        partition="selection-destinations",
        stage="rollout",
    )

    try:
        with pytest.raises(ValueError, match="fields cannot be combined"):
            transfer.get_dataproto(ref, fields=["input_ids"], batch_fields=["scores"])
        with pytest.raises(KeyError, match="unknown DataProto fields"):
            transfer.get_dataproto(ref, fields=["missing"])
        with pytest.raises(IndexError, match="out of range"):
            transfer.get_dataproto(ref, fields=["input_ids"], rows=[batch_size])
        with pytest.raises(TypeError, match="row indices"):
            transfer.get_dataproto(ref, fields=["input_ids"], rows=["0"])
        with pytest.raises(ValueError, match="step must be positive"):
            transfer.get_dataproto(ref, fields=["input_ids"], rows=slice(None, None, -1))
        with pytest.raises(ValueError, match="destination shape mismatch"):
            transfer.get_dataproto(
                ref,
                batch_fields=["input_ids"],
                rows=[0, 1, 2],
                destinations={"input_ids": np.empty((2, 12), dtype=np.int64)},
            )

        scores_destination = np.empty((4,), dtype=np.float32)
        result = transfer.get_dataproto(
            ref,
            batch_fields=["scores"],
            rows=[11, 3, 3, 0],
            destinations={"scores": scores_destination},
        )
        non_tensor_only = transfer.get_dataproto(
            ref,
            non_tensor_fields=["prompts"],
            rows=[2, 4, 6],
        )

        assert result["batch"]["scores"] is scores_destination
        assert np.array_equal(scores_destination, scores[[11, 3, 3, 0]])
        assert result["non_tensor_batch"] == {}
        assert non_tensor_only["batch"] == {}
        assert non_tensor_only["non_tensor_batch"]["prompts"].tolist() == prompts[[2, 4, 6]].tolist()
    finally:
        transfer.cleanup_dataproto(ref)


def test_dataproto_helper_imported_handle_same_stage_append_with_real_store() -> None:
    pytest.importorskip("mooncake.store")
    _store, transfer = real_transfer("structured-test-imported-handle-append")
    batch_size = 8
    input_ids = np.arange(batch_size * 6, dtype=np.int64).reshape(batch_size, 6)
    masks = np.ones((batch_size, 6), dtype=np.int32)
    values = np.linspace(1.0, 2.0, batch_size, dtype=np.float32)
    rewards = values + np.float32(5.0)
    ref = transfer.put_dataproto(
        {
            "batch": {"input_ids": input_ids, "masks": masks},
            "non_tensor_batch": {},
            "meta_info": {"step": 2},
        },
        namespace="roll",
        partition="imported-handle-append",
        stage="rollout",
    )
    imported = import_dataproto_ref(export_dataproto_ref(ref))
    appended = transfer.append_dataproto_fields(
        imported,
        {
            "batch": {"values": values, "rewards": rewards},
            "non_tensor_batch": {},
            "meta_info": {"critic": True},
        },
        stage="rollout",
    )
    handle = export_dataproto_ref(appended)

    try:
        result = transfer.get_dataproto(handle, fields=["input_ids", "values", "rewards"], rows=[7, 1, 4])
        assert np.array_equal(result["batch"]["input_ids"], input_ids[[7, 1, 4]])
        assert np.array_equal(result["batch"]["values"], values[[7, 1, 4]])
        assert np.array_equal(result["batch"]["rewards"], rewards[[7, 1, 4]])
        assert result["meta_info"] == {"step": 2, "critic": True}
    finally:
        transfer.cleanup_dataproto(appended)


def test_bundle_manifest_rejects_tampered_cleanup_keys() -> None:
    store, transfer = make_transfer()
    ref = transfer.put_bundle(b"meta", {"payload": b"abcdef"})
    tampered = dict(ref.manifest)
    tampered["cleanup_keys"] = ["other/object"]
    write_manifest(store, ref.manifest_key, tampered)

    with pytest.raises(ValueError, match="cleanup_keys"):
        transfer.remove_bundle({"manifest_key": ref.manifest_key})


def test_dataproto_helper_rollout_edge_cases_with_real_store() -> None:
    pytest.importorskip("mooncake.store")
    _store, transfer = real_transfer("structured-test-rollout-edge-cases")
    batch_size = 10
    row_ids = [f"edge-row-{index}" for index in range(batch_size)]
    input_ids = np.arange(batch_size * 16, dtype=np.int64).reshape(batch_size, 16)
    attention_mask = (input_ids % 2 == 0).astype(np.int32)
    prompts = np.asarray(
        ["", "short", None, "三", "bytes", "long-" + "x" * 64, "7", "8", "9", "tail"],
        dtype=object,
    )
    scores = np.linspace(-1.0, 1.0, batch_size, dtype=np.float32)
    values = scores + np.float32(2.0)
    replacement_scores = scores + np.float32(10.0)
    ref = transfer.put_dataproto(
        {
            "batch": {"input_ids": input_ids, "attention_mask": attention_mask},
            "non_tensor_batch": {"prompts": prompts},
            "meta_info": {"roll_row_ids": row_ids},
        },
        namespace="roll",
        partition="edge-remote-batch",
        stage="rollout",
    )
    ref = transfer.append_dataproto_fields(
        ref,
        {"batch": {"scores": scores}, "non_tensor_batch": {}, "meta_info": {}},
        stage="scores",
    )

    try:
        with pytest.raises(ValueError, match="already exist"):
            transfer.append_dataproto_fields(
                ref,
                {"batch": {"scores": scores}, "non_tensor_batch": {}, "meta_info": {}},
                stage="other_scores",
            )
        with pytest.raises(ValueError, match="batch size"):
            transfer.append_dataproto_fields(
                ref,
                {
                    "batch": {"bad": np.arange(batch_size - 1, dtype=np.int64)},
                    "non_tensor_batch": {},
                    "meta_info": {},
                },
                stage="bad_size",
            )

        empty = transfer.get_dataproto(
            ref,
            fields=["input_ids", "scores", "prompts"],
            rows=[],
        )
        tail_and_repeat = transfer.get_dataproto(
            ref,
            fields=["input_ids", "attention_mask", "scores", "prompts"],
            rows=[-1, 0, -1, 3],
        )
        step_slice = transfer.get_dataproto(
            ref,
            batch_fields=["input_ids", "scores"],
            rows=slice(1, 9, 2),
        )
        ref = transfer.append_dataproto_fields(
            ref,
            {
                "batch": {"values": values},
                "non_tensor_batch": {},
                "meta_info": {"same_stage": True},
            },
            stage="rollout",
        )
        same_stage = transfer.get_dataproto(
            ref,
            fields=["input_ids", "values", "prompts"],
            rows=[2, 5, 9],
        )
        ref = transfer.append_dataproto_fields(
            ref,
            {
                "batch": {"scores": replacement_scores},
                "non_tensor_batch": {},
                "meta_info": {"scores_overwritten": True},
            },
            stage="scores",
            overwrite=True,
        )
        overwritten = transfer.get_dataproto(
            ref,
            fields=["scores", "values"],
            rows=[0, 4, 9],
        )

        assert empty["batch"]["input_ids"].shape == (0, 16)
        assert empty["batch"]["scores"].shape == (0,)
        assert empty["non_tensor_batch"]["prompts"].tolist() == []
        assert np.array_equal(tail_and_repeat["batch"]["input_ids"], input_ids[[9, 0, 9, 3]])
        assert np.array_equal(
            tail_and_repeat["batch"]["attention_mask"], attention_mask[[9, 0, 9, 3]]
        )
        assert np.array_equal(tail_and_repeat["batch"]["scores"], scores[[9, 0, 9, 3]])
        assert tail_and_repeat["non_tensor_batch"]["prompts"].tolist() == prompts[[9, 0, 9, 3]].tolist()
        assert np.array_equal(step_slice["batch"]["input_ids"], input_ids[1:9:2])
        assert np.array_equal(step_slice["batch"]["scores"], scores[1:9:2])
        assert np.array_equal(same_stage["batch"]["input_ids"], input_ids[[2, 5, 9]])
        assert np.array_equal(same_stage["batch"]["values"], values[[2, 5, 9]])
        assert same_stage["non_tensor_batch"]["prompts"].tolist() == prompts[[2, 5, 9]].tolist()
        assert np.array_equal(overwritten["batch"]["scores"], replacement_scores[[0, 4, 9]])
        assert np.array_equal(overwritten["batch"]["values"], values[[0, 4, 9]])
        assert overwritten["meta_info"]["same_stage"] is True
        assert overwritten["meta_info"]["scores_overwritten"] is True
    finally:
        transfer.cleanup_dataproto(ref)

    with pytest.raises(Exception):
        transfer.get_dataproto(ref, fields=["input_ids"])


def test_structured_object_zero_byte_payload_skips_store_put() -> None:
    store, transfer = make_transfer()
    ref = transfer.put_structured_object(
        StructuredObjectPayload(buffers={"empty": np.empty((4, 0), dtype=np.float32)})
    )
    result = transfer.materialize(transfer.read_spec(ref))

    assert result.objects["empty"].shape == (4, 0)
    assert ref.manifest["buffers"]["empty"]["bytes"] == 0
    assert ref.manifest["buffers"]["empty"]["chunks"] == []
    assert store.objects == {ref.manifest_key: store.objects[ref.manifest_key], ref.manifest["meta"]["key"]: store.objects[ref.manifest["meta"]["key"]]}


def test_dataproto_helper_multidim_boundary_reads_with_real_store() -> None:
    pytest.importorskip("mooncake.store")
    _store, transfer = real_transfer("structured-test-multidim-boundaries")
    batch_size = 18
    image_features = np.arange(batch_size * 3 * 16 * 16, dtype=np.float32).reshape(
        batch_size, 3, 16, 16
    )
    logits = np.linspace(-2.0, 2.0, batch_size * 4 * 5 * 6, dtype=np.float32).reshape(
        batch_size, 4, 5, 6
    )
    token_matrix = np.arange(batch_size * 7 * 9, dtype=np.int64).reshape(
        batch_size, 7, 9
    )
    zero_width = np.empty((batch_size, 0), dtype=np.float32)
    row_scores = np.linspace(10.0, 20.0, batch_size, dtype=np.float32)
    byte_blobs = np.asarray(
        [
            b"",
            b"alpha",
            bytearray(b"beta"),
            memoryview(b"gamma"),
            *[f"blob-{index}".encode() for index in range(4, batch_size)],
        ],
        dtype=object,
    )
    json_meta = np.asarray(
        [
            None if index % 5 == 0 else {"row": index, "shape": [3, 16, 16]}
            for index in range(batch_size)
        ],
        dtype=object,
    )
    ref = transfer.put_dataproto(
        {
            "batch": {
                "image_features": image_features,
                "logits": logits,
                "token_matrix": token_matrix,
                "zero_width": zero_width,
            },
            "non_tensor_batch": {
                "byte_blobs": byte_blobs,
                "json_meta": json_meta,
            },
            "meta_info": {"batch_size": batch_size, "layout": "multidim"},
        },
        namespace="roll",
        partition="multidim-boundaries",
        stage="rollout",
    )
    ref = transfer.append_dataproto_fields(
        ref,
        {
            "batch": {"row_scores": row_scores},
            "non_tensor_batch": {},
            "meta_info": {"score_stage": "same-stage"},
        },
        stage="rollout",
    )
    handle = export_dataproto_ref(ref)
    rows = [17, 0, 5, 17, 9]

    try:
        full = transfer.get_dataproto(
            handle,
            fields=["image_features", "zero_width", "byte_blobs", "json_meta"],
        )
        empty_slice = transfer.get_dataproto(
            handle,
            batch_fields=["image_features", "zero_width", "row_scores"],
            rows=slice(4, 4),
        )
        duplicate_gather = transfer.get_dataproto(
            handle,
            fields=["logits", "token_matrix", "row_scores", "byte_blobs", "json_meta"],
            rows=rows,
        )
        image_destination = np.empty((len(rows), 3, 16, 16), dtype=np.float32)
        into = transfer.get_dataproto(
            handle,
            batch_fields=["image_features"],
            rows=rows,
            destinations={"image_features": image_destination},
        )
        step_batch = transfer.get_dataproto(
            handle,
            batch_fields=["logits", "token_matrix"],
            rows=slice(2, 17, 3),
        )
        tail_non_tensor = transfer.get_dataproto(
            handle,
            non_tensor_fields=["byte_blobs", "json_meta"],
            rows=slice(batch_size - 3, batch_size),
        )
        meta_selected = transfer.get_dataproto(
            handle,
            fields=[],
            meta_info_keys=["layout", "score_stage"],
        )

        assert np.array_equal(full["batch"]["image_features"], image_features)
        assert full["batch"]["zero_width"].shape == (batch_size, 0)
        assert full["non_tensor_batch"]["byte_blobs"].tolist() == [
            bytes(value) for value in byte_blobs.tolist()
        ]
        assert full["non_tensor_batch"]["json_meta"].tolist() == json_meta.tolist()
        assert empty_slice["batch"]["image_features"].shape == (0, 3, 16, 16)
        assert empty_slice["batch"]["zero_width"].shape == (0, 0)
        assert empty_slice["batch"]["row_scores"].shape == (0,)
        assert np.array_equal(duplicate_gather["batch"]["logits"], logits[rows])
        assert np.array_equal(
            duplicate_gather["batch"]["token_matrix"], token_matrix[rows]
        )
        assert np.array_equal(duplicate_gather["batch"]["row_scores"], row_scores[rows])
        assert duplicate_gather["non_tensor_batch"]["byte_blobs"].tolist() == [
            bytes(value) for value in byte_blobs[rows].tolist()
        ]
        assert duplicate_gather["non_tensor_batch"]["json_meta"].tolist() == json_meta[rows].tolist()
        assert into["batch"]["image_features"] is image_destination
        assert np.array_equal(image_destination, image_features[rows])
        assert np.array_equal(step_batch["batch"]["logits"], logits[2:17:3])
        assert np.array_equal(step_batch["batch"]["token_matrix"], token_matrix[2:17:3])
        assert tail_non_tensor["batch"] == {}
        assert tail_non_tensor["non_tensor_batch"]["byte_blobs"].tolist() == [
            bytes(value) for value in byte_blobs[batch_size - 3 :].tolist()
        ]
        assert tail_non_tensor["non_tensor_batch"]["json_meta"].tolist() == json_meta[batch_size - 3 :].tolist()
        assert meta_selected["batch"] == {}
        assert meta_selected["non_tensor_batch"] == {}
        assert meta_selected["meta_info"] == {
            "layout": "multidim",
            "score_stage": "same-stage",
        }
    finally:
        transfer.cleanup_dataproto(ref)


def test_dataproto_helper_rejects_inconsistent_batch_sizes() -> None:
    _store, transfer = make_transfer()
    data = SimpleDataProto(
        batch={
            "input_ids": np.arange(4, dtype=np.int64),
            "attention_mask": np.arange(3, dtype=np.int64),
        }
    )

    with pytest.raises(ValueError, match="inconsistent batch sizes"):
        transfer.put_dataproto(data)


def test_dataproto_helper_overwrite_rejects_partial_stage_replacement() -> None:
    _store, transfer = make_transfer()
    ref = transfer.put_dataproto(
        SimpleDataProto(
            batch={"input_ids": np.arange(4), "attention_mask": np.arange(4)}
        ),
        stage="rollout",
    )

    with pytest.raises(ValueError, match="must include existing fields"):
        transfer.append_dataproto_fields(
            ref,
            SimpleDataProto(batch={"input_ids": np.arange(4)}),
            stage="rollout",
            overwrite=True,
        )


def test_dataproto_helper_overwrite_replaces_encoded_metadata() -> None:
    store, transfer = make_transfer()
    ref = transfer.put_dataproto(
        SimpleDataProto(
            non_tensor_batch={"text": np.asarray(["a", None], dtype=object)}
        ),
        stage="meta",
    )
    old_manifest_key = ref.stage_refs["meta"].manifest_key
    ref = transfer.append_dataproto_fields(
        ref,
        SimpleDataProto(non_tensor_batch={"text": np.asarray([1, 2], dtype=np.int64)}),
        stage="meta",
        overwrite=True,
    )
    result = transfer.get_dataproto(ref)

    assert "text" not in ref.encoded_non_tensor
    assert old_manifest_key not in store.objects
    assert ref.stage_refs["meta"].manifest_key in store.objects
    assert np.array_equal(result["non_tensor_batch"]["text"], np.asarray([1, 2]))


def test_dataproto_helper_cleanup_removes_all_stage_objects() -> None:
    store, transfer = make_transfer()
    ref = transfer.put_dataproto(
        SimpleDataProto(batch={"input_ids": np.arange(4, dtype=np.int64)}),
        stage="rollout",
    )
    ref = transfer.append_dataproto_fields(
        ref,
        SimpleDataProto(batch={"values": np.arange(4, dtype=np.float32)}),
        stage="critic",
    )

    assert store.objects
    transfer.cleanup_dataproto(ref)

    assert store.objects == {}


def test_dataproto_helper_object_non_tensor_codecs_roundtrip() -> None:
    _store, transfer = make_transfer()
    data = SimpleDataProto(
        batch={"input_ids": np.arange(4, dtype=np.int64)},
        non_tensor_batch={
            "text": np.asarray(["hello", None, "world", "moon"], dtype=object),
            "json": np.asarray(
                [{"a": 1}, None, {"b": [2, 3]}, {"c": "x"}], dtype=object
            ),
            "blob": np.asarray(
                [b"a", None, bytearray(b"bc"), memoryview(b"def")], dtype=object
            ),
            "nullable_int": np.asarray([1, None, 3, 4], dtype=object),
        },
    )

    ref = transfer.put_dataproto(data)
    result = transfer.get_dataproto(ref)

    assert set(ref.encoded_non_tensor) == {"text", "json", "blob", "nullable_int"}
    assert ref.encoded_non_tensor["json"]["codec"] == "msgpack_ragged"
    assert result["non_tensor_batch"]["text"].tolist() == [
        "hello",
        None,
        "world",
        "moon",
    ]
    assert result["non_tensor_batch"]["json"].tolist() == [
        {"a": 1},
        None,
        {"b": [2, 3]},
        {"c": "x"},
    ]
    assert result["non_tensor_batch"]["blob"].tolist() == [b"a", None, b"bc", b"def"]
    assert result["non_tensor_batch"]["nullable_int"].tolist() == [1, None, 3, 4]


def test_dataproto_helper_rejects_unsupported_object_non_tensor() -> None:
    _store, transfer = make_transfer()
    data = SimpleDataProto(
        non_tensor_batch={"fallback": np.asarray([object(), None], dtype=object)}
    )

    with pytest.raises(ValueError, match="unsupported structured non-tensor field"):
        transfer.put_dataproto(data)


def test_dataproto_helper_ragged_tensor_non_tensor_roundtrip() -> None:
    torch = pytest.importorskip("torch")
    _store, transfer = make_transfer()
    ragged = np.asarray(
        [
            torch.arange(1, dtype=torch.float32),
            None,
            torch.arange(3, dtype=torch.float32),
            torch.arange(2, dtype=torch.float32),
        ],
        dtype=object,
    )
    data = SimpleDataProto(non_tensor_batch={"ragged": ragged})

    ref = transfer.put_dataproto(data)
    result = transfer.get_dataproto(ref)

    assert ref.encoded_non_tensor["ragged"]["codec"] == "ragged_tensor"
    actual = result["non_tensor_batch"]["ragged"]
    assert torch.equal(actual[0], ragged[0])
    assert actual[1] is None
    assert torch.equal(actual[2], ragged[2])
    assert torch.equal(actual[3], ragged[3])


def _assert_tensor_object_equal(actual, expected) -> None:
    torch = pytest.importorskip("torch")
    if expected is None:
        assert actual is None
        return
    if isinstance(expected, dict):
        assert isinstance(actual, dict)
        assert set(actual) == set(expected)
        for key, value in expected.items():
            _assert_tensor_object_equal(actual[key], value)
        return
    if isinstance(expected, list):
        assert isinstance(actual, list)
        assert len(actual) == len(expected)
        for actual_item, expected_item in zip(actual, expected):
            _assert_tensor_object_equal(actual_item, expected_item)
        return
    if isinstance(expected, torch.Tensor):
        assert torch.equal(actual, expected)
        return
    assert actual == expected


def test_dataproto_helper_dict_of_tensors_object_array_roundtrip() -> None:
    torch = pytest.importorskip("torch")
    _store, transfer = make_transfer()
    samples = np.asarray(
        [
            {"tokens": torch.arange(2, dtype=torch.int64), "reward": 1.0},
            {"tokens": torch.arange(3, dtype=torch.int64), "reward": 2.0},
            {"tokens": torch.arange(1, dtype=torch.int64), "reward": None},
            {"tokens": None, "reward": 4.0},
        ],
        dtype=object,
    )

    ref = transfer.put_dataproto(
        SimpleDataProto(
            batch={"input_ids": np.arange(8, dtype=np.int64).reshape(4, 2)},
            non_tensor_batch={"samples": samples},
            meta_info={"source": "dict-of-tensors"},
        )
    )
    result = transfer.get_dataproto(ref)
    view = transfer.dataproto_manifest_view(ref)

    assert ref.encoded_non_tensor["samples"]["codec"] == "structured_recursive"
    assert view["non_tensor_fields"]["samples"]["spec"]["codec"] == "structured_recursive"
    assert result["meta_info"] == {"source": "dict-of-tensors"}
    actual = result["non_tensor_batch"]["samples"]
    for row, expected in enumerate(samples):
        _assert_tensor_object_equal(actual[row], expected)


def test_dataproto_helper_dict_of_tensors_distinguishes_missing_keys_and_nulls() -> None:
    torch = pytest.importorskip("torch")
    _store, transfer = make_transfer()
    samples = np.asarray(
        [
            {"tokens": torch.arange(2), "label": None},
            {"label": 1},
            None,
            {"tokens": None, "label": 3},
        ],
        dtype=object,
    )

    ref = transfer.put_dataproto(SimpleDataProto(non_tensor_batch={"samples": samples}))
    actual = transfer.get_dataproto(ref)["non_tensor_batch"]["samples"]

    assert "tokens" in actual[0]
    assert actual[0]["label"] is None
    assert "tokens" not in actual[1]
    assert actual[1]["label"] == 1
    assert actual[2] is None
    assert "tokens" in actual[3]
    assert actual[3]["tokens"] is None
    assert actual[3]["label"] == 3


def test_dataproto_helper_nested_tensor_object_array_rows() -> None:
    torch = pytest.importorskip("torch")
    _store, transfer = make_transfer()
    samples = np.asarray(
        [
            {"images": [{"pixels": torch.arange(2)}], "meta": {"rank": 0}},
            {"images": [{"pixels": torch.arange(3)}, {"pixels": None}], "meta": {}},
            {"images": [], "meta": {"rank": None}},
        ],
        dtype=object,
    )

    ref = transfer.put_dataproto(SimpleDataProto(non_tensor_batch={"samples": samples}))
    actual = transfer.get_dataproto(ref)["non_tensor_batch"]["samples"]

    for row, expected in enumerate(samples):
        _assert_tensor_object_equal(actual[row], expected)


def test_dataproto_helper_dict_of_native_object_leaves_uses_recursive_codec() -> None:
    _store, transfer = make_transfer()
    samples = np.asarray(
        [
            {
                "media": [b"a", b"bc"],
                "blob": b"payload-0",
                "scores": np.asarray([1, 2], dtype=np.int64),
                "label": "x",
            },
            {
                "media": [],
                "blob": b"payload-1",
                "scores": np.asarray([3], dtype=np.int64),
                "label": "y",
            },
            {"label": "missing-native"},
            None,
        ],
        dtype=object,
    )

    ref = transfer.put_dataproto(SimpleDataProto(non_tensor_batch={"samples": samples}))
    result = transfer.get_dataproto(ref)
    actual = result["non_tensor_batch"]["samples"]

    assert ref.encoded_non_tensor["samples"]["codec"] == "structured_recursive"
    assert actual[0]["media"] == [b"a", b"bc"]
    assert actual[0]["blob"] == b"payload-0"
    assert actual[0]["scores"] == [1, 2]
    assert actual[0]["label"] == "x"
    assert actual[1]["media"] == []
    assert actual[1]["blob"] == b"payload-1"
    assert actual[1]["scores"] == [3]
    assert actual[1]["label"] == "y"
    assert actual[2] == {"label": "missing-native"}
    assert actual[3] is None


def test_dataproto_helper_reads_dict_of_tensors_rows_and_selected_field() -> None:
    torch = pytest.importorskip("torch")
    _store, transfer = make_transfer()
    samples = np.asarray(
        [
            {"tokens": torch.arange(i + 1, dtype=torch.int64), "row": i}
            for i in range(6)
        ],
        dtype=object,
    )
    samples[2] = {"tokens": None, "row": 2}
    samples[4] = None
    input_ids = np.arange(12, dtype=np.int64).reshape(6, 2)
    ref = transfer.put_dataproto(
        SimpleDataProto(
            batch={"input_ids": input_ids},
            non_tensor_batch={"samples": samples},
            meta_info={"kind": "dict-tensor"},
        )
    )

    sliced = transfer.get_dataproto(ref, fields=["samples"], rows=slice(1, 5))
    gathered = transfer.get_dataproto(ref, fields=["input_ids", "samples"], rows=[5, 0, 2, 4])

    assert sliced["batch"] == {}
    assert sliced["meta_info"] == {"kind": "dict-tensor"}
    for row, expected in enumerate(samples[1:5]):
        _assert_tensor_object_equal(sliced["non_tensor_batch"]["samples"][row], expected)
    assert np.array_equal(gathered["batch"]["input_ids"], input_ids[[5, 0, 2, 4]])
    for row, expected_index in enumerate([5, 0, 2, 4]):
        _assert_tensor_object_equal(
            gathered["non_tensor_batch"]["samples"][row], samples[expected_index]
        )


def test_dataproto_helper_recursive_manifest_export_append_and_overwrite() -> None:
    torch = pytest.importorskip("torch")
    store, transfer = make_transfer()
    input_ids = np.arange(4, dtype=np.int64)
    ref = transfer.put_dataproto(SimpleDataProto(batch={"input_ids": input_ids}), stage="rollout")
    samples = np.asarray(
        [
            {"tokens": torch.arange(1, dtype=torch.float32), "score": 0.0},
            {"tokens": torch.arange(2, dtype=torch.float32), "score": 1.0},
            {"tokens": torch.arange(3, dtype=torch.float32), "score": None},
            {"tokens": None, "score": 3.0},
        ],
        dtype=object,
    )

    ref = transfer.append_dataproto_fields(
        ref,
        SimpleDataProto(non_tensor_batch={"samples": samples}, meta_info={"stage": "samples"}),
        stage="rollout",
    )
    handle = export_dataproto_ref(ref)
    json.dumps(handle)
    imported = import_dataproto_ref(handle)
    result = transfer.get_dataproto(imported)
    view = transfer.dataproto_manifest_view(imported)

    assert np.array_equal(result["batch"]["input_ids"], input_ids)
    _assert_tensor_object_equal(result["non_tensor_batch"]["samples"][1], samples[1])
    spec = view["non_tensor_fields"]["samples"]["spec"]
    assert spec["codec"] == "structured_recursive"
    assert spec["metadata"]["nodes"]
    assert spec["metadata"]["leaves"]
    assert any(name.endswith(".missing") for name in spec["payload_members"])

    old_manifest_key = ref.stage_refs["rollout"].manifest_key
    ref = transfer.append_dataproto_fields(
        ref,
        SimpleDataProto(
            batch={"input_ids": input_ids},
            non_tensor_batch={"samples": np.asarray(["a", "b", "c", "d"], dtype=object)},
        ),
        stage="rollout",
        overwrite=True,
    )
    overwritten = transfer.get_dataproto(ref)
    assert ref.encoded_non_tensor["samples"]["codec"] == "utf8_ragged"
    assert overwritten["non_tensor_batch"]["samples"].tolist() == ["a", "b", "c", "d"]
    assert old_manifest_key not in store.objects
