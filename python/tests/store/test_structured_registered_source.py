from __future__ import annotations

import ctypes

import numpy as np
import pytest

from mooncake.structured_object_store import (
    BundleTransferPolicy,
    RegisteredSourceBuffer,
    StructuredObjectPayload,
    registered_source_buffer,
)
from test_structured_object_store import InMemoryStore, make_transfer


class TrackingStore(InMemoryStore):
    def __init__(self) -> None:
        super().__init__()
        self.registration_ranges: list[tuple[int, int]] = []

    def register_buffer(self, ptr: int, size: int) -> int:
        self.registration_ranges.append((ptr, size))
        return super().register_buffer(ptr, size)


class NoBatchPutStore(InMemoryStore):
    batch_put_from = None


class PreRegisteredOnlyStore(TrackingStore):
    register_buffer = None
    unregister_buffer = None


class FailingBatchPutStore(InMemoryStore):
    def batch_put_from(
        self,
        keys: list[str],
        buffer_ptrs: list[int],
        sizes: list[int],
        config=None,
    ) -> list[int]:
        del buffer_ptrs, sizes, config
        self.batch_put_from_calls += 1
        return [-1 for _key in keys]


def test_registered_source_batches_chunks_and_publishes_manifest_last() -> None:
    owner = ctypes.create_string_buffer(bytes(range(32)))
    source = registered_source_buffer(
        ctypes.addressof(owner),
        32,
        owner,
        pre_registered=True,
        dtype="uint8",
        shape=(32,),
    )
    store, transfer = make_transfer(PreRegisteredOnlyStore())
    store.registered.add(source.ptr)

    ref = transfer.put_structured_object(
        StructuredObjectPayload(buffers={"values": source}),
        chunk_bytes=8,
        policy=BundleTransferPolicy(copy_mode="zero_copy"),
    )

    assert store.batch_put_from_calls == 1
    assert store.register_buffer_calls == 0
    assert store.unregister_buffer_calls == 0
    assert store.events[-1] == ("put", ref.manifest_key)
    assert [
        chunk["bytes"] for chunk in ref.manifest["buffers"]["values"]["chunks"]
    ] == [8, 8, 8, 8]
    result = transfer.materialize(transfer.read_spec(ref))
    assert np.array_equal(result.objects["values"], np.arange(32, dtype=np.uint8))


def test_registered_sources_share_registration_and_batch_put() -> None:
    first_owner = ctypes.create_string_buffer(b"first-value")
    second_owner = ctypes.create_string_buffer(b"second-value")
    store, transfer = make_transfer(TrackingStore())

    ref = transfer.put_structured_object(
        StructuredObjectPayload(
            buffers={
                "first": registered_source_buffer(
                    ctypes.addressof(first_owner), len(first_owner.raw), first_owner
                ),
                "second": registered_source_buffer(
                    ctypes.addressof(second_owner), len(second_owner.raw), second_owner
                ),
            }
        ),
        chunk_bytes=4,
    )

    assert store.batch_put_from_calls == 1
    assert store.register_buffer_calls == 2
    assert store.unregister_buffer_calls == 2
    assert store.registered == set()
    assert store.events[-1] == ("put", ref.manifest_key)
    assert transfer.get_object(ref) == {
        "first": first_owner.raw,
        "second": second_owner.raw,
    }


def test_registered_sources_coalesce_shared_base_pointer_registration() -> None:
    owner = ctypes.create_string_buffer(bytes(range(16)))
    ptr = ctypes.addressof(owner)
    store, transfer = make_transfer(TrackingStore())

    ref = transfer.put_structured_object(
        StructuredObjectPayload(
            buffers={
                "prefix": registered_source_buffer(ptr, 8, owner),
                "all": registered_source_buffer(ptr, 16, owner),
            }
        ),
        chunk_bytes=4,
    )

    assert store.registration_ranges == [(ptr, 16)]
    assert store.unregister_buffer_calls == 1
    assert transfer.get_object(ref) == {
        "prefix": bytes(range(8)),
        "all": bytes(range(16)),
    }


def test_registered_source_requires_batch_put_before_writes() -> None:
    store, transfer = make_transfer(NoBatchPutStore())
    owner = ctypes.create_string_buffer(b"value")

    with pytest.raises(RuntimeError, match="object-level batch_put_from"):
        transfer.put_structured_object(
            StructuredObjectPayload(
                buffers={
                    "value": registered_source_buffer(
                        ctypes.addressof(owner), len(owner.raw), owner
                    )
                }
            )
        )

    assert store.objects == {}


def test_registered_source_failure_cleans_payloads_and_registration() -> None:
    store, transfer = make_transfer(FailingBatchPutStore())
    owner = ctypes.create_string_buffer(b"abcdefgh")

    with pytest.raises(RuntimeError, match="batch_put_from"):
        transfer.put_structured_object(
            StructuredObjectPayload(
                buffers={
                    "value": registered_source_buffer(ctypes.addressof(owner), 8, owner)
                }
            ),
            chunk_bytes=4,
        )

    assert store.objects == {}
    assert store.register_buffer_calls == 1
    assert store.unregister_buffer_calls == 1
    assert store.registered == set()


def test_registered_source_validates_contract_and_supports_empty_members() -> None:
    owner = ctypes.create_string_buffer(8)
    with pytest.raises(ValueError, match="owner is required"):
        registered_source_buffer(ctypes.addressof(owner), 8, None)
    with pytest.raises(ValueError, match="requires 16 bytes"):
        registered_source_buffer(
            ctypes.addressof(owner),
            8,
            owner,
            dtype="int32",
            shape=(4,),
        )

    store, transfer = make_transfer()
    with pytest.raises(ValueError, match="copy_mode='copy'"):
        transfer.put_structured_object(
            StructuredObjectPayload(
                buffers={
                    "value": registered_source_buffer(ctypes.addressof(owner), 8, owner)
                }
            ),
            policy=BundleTransferPolicy(copy_mode="copy"),
        )
    assert store.objects == {}

    ref = transfer.put_structured_object(
        StructuredObjectPayload(
            buffers={
                "empty": RegisteredSourceBuffer(
                    ptr=0,
                    nbytes=0,
                    owner=None,
                    pre_registered=True,
                )
            }
        )
    )

    assert ref.manifest["buffers"]["empty"]["chunks"] == []
    assert store.batch_put_from_calls == 0
    assert transfer.get_object(ref)["empty"] == b""
