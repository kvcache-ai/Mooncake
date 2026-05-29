from __future__ import annotations

import ctypes
import json
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
)


class InMemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.lock = threading.Lock()
        self.registered: set[int] = set()
        self.max_active_puts = 0
        self.max_active_gets = 0
        self.active_puts = 0
        self.active_gets = 0
        self.get_into_calls = 0
        self.get_into_ranges_calls = 0
        self.batch_get_into_calls = 0
        self.batch_remove_calls = 0

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
        return 0

    def batch_remove(self, keys: list[str], force: bool = False) -> list[int]:
        self.batch_remove_calls += 1
        for key in keys:
            self.remove(key, force)
        return [0 for _key in keys]

    def batch_put_from(
        self, keys: list[str], buffer_ptrs: list[int], sizes: list[int]
    ) -> list[int]:
        results: list[int] = []
        for key, ptr, size in zip(keys, buffer_ptrs, sizes):
            data = ctypes.string_at(ptr, size)
            results.append(self.put(key, data))
        return results

    def register_buffer(self, buffer_ptr: int, size: int) -> int:
        self.registered.add(buffer_ptr)
        return 0

    def unregister_buffer(self, buffer_ptr: int) -> int:
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
) -> tuple[InMemoryStore, MooncakeBundleTransfer]:
    current_store = store or InMemoryStore()
    kwargs = {"key_prefix": key_prefix}
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
