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

    assert set(result["batch"]) == {"input_ids"}
    assert set(result["non_tensor_batch"]) == {"reward"}
    assert result["meta_info"] == {"step": 7}
    assert np.array_equal(result["batch"]["input_ids"], data.batch["input_ids"])
    assert np.array_equal(
        result["non_tensor_batch"]["reward"], data.non_tensor_batch["reward"]
    )


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
