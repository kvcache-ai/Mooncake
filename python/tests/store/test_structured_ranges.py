"""Byte-range contracts, using the existing StructuredObject Store test double."""

from __future__ import annotations

import ctypes
import weakref
from dataclasses import replace

import numpy as np
import pytest

from mooncake.structured_object_store import (
    PreparedRangeLayoutChanged,
    StructuredMemberRange,
    StructuredObjectPayload,
    raw_destination,
)
from test_structured_object_store import InMemoryStore, PlainStore
from test_structured_object_store import make_transfer as _make_transfer


class RecordingStore(InMemoryStore):
    def __init__(self):
        super().__init__()
        self.registration_ptrs = []
        self.unregistration_ptrs = []

    def register_buffer(self, ptr, size):
        self.registration_ptrs.append(ptr)
        return super().register_buffer(ptr, size)

    def unregister_buffer(self, ptr):
        self.unregistration_ptrs.append(ptr)
        return super().unregister_buffer(ptr)


def make_transfer(store=None):
    return _make_transfer(RecordingStore() if store is None else store)


def registration_mark(store):
    return len(store.registration_ptrs), len(store.unregistration_ptrs)


def assert_borrowed(store, mark, *destinations):
    registered = store.registration_ptrs[mark[0] :]
    unregistered = store.unregistration_ptrs[mark[1] :]
    for item in destinations:
        assert item.ptr not in registered
        assert item.ptr not in unregistered


def destination(size, owner=None):
    owner = ctypes.create_string_buffer(size) if owner is None else owner
    return raw_destination(ctypes.addressof(owner), size, owner, pre_registered=True)


def publish(transfer, value, *, chunk_bytes=8, name="values"):
    return transfer.put_structured_object(
        StructuredObjectPayload(buffers={name: value}), chunk_bytes=chunk_bytes
    )


def test_cross_chunk_ranges_are_grouped_into_one_direct_request_without_registration():
    store, transfer = make_transfer()
    ref = publish(transfer, bytes(range(64)))
    dest = destination(24)
    ctypes.memset(dest.ptr, 0x7F, dest.size)
    mark = registration_mark(store)
    result = transfer.materialize_ranges_into(
        transfer.read_spec(ref),
        (
            StructuredMemberRange("values", 2, 4, 0),
            StructuredMemberRange("values", 14, 6, 8),
        ),
        {"values": dest},
    )
    assert (
        dest.owner.raw
        == bytes(range(2, 6)) + b"\x7f" * 4 + bytes(range(14, 20)) + b"\x7f" * 10
    )
    assert result.objects == {"values": dest}
    assert store.get_into_ranges_calls == 1
    assert_borrowed(store, mark, dest)


def test_multiple_members_share_one_request_and_cannot_alias_destinations():
    store, transfer = make_transfer()
    ref = transfer.put_structured_object(
        StructuredObjectPayload(buffers={"a": bytes(range(8)), "b": b"ABCDEFGH"})
    )
    first, second = destination(4), destination(4)
    spans = (StructuredMemberRange("a", 2, 4, 0), StructuredMemberRange("b", 1, 4, 0))
    transfer.materialize_ranges_into(
        transfer.read_spec(ref), spans, {"a": first, "b": second}
    )
    assert first.owner.raw == bytes(range(2, 6)) and second.owner.raw == b"BCDE"
    assert store.get_into_ranges_calls == 1
    before = store.get_into_ranges_calls
    with pytest.raises(ValueError, match="overlap"):
        transfer.materialize_ranges_into(
            transfer.read_spec(ref), spans, {"a": first, "b": first}
        )
    assert store.get_into_ranges_calls == before


def test_prepared_plan_rebinds_current_keys_and_owner_without_retaining_old_destination():
    store, transfer = make_transfer()
    values = np.arange(16, dtype=np.uint32)
    ref = publish(transfer, values)
    spans = (
        StructuredMemberRange("values", 0, 8, 0),
        StructuredMemberRange("values", 12, 12, 8),
    )
    old = destination(20)
    owner_ref = weakref.ref(old.owner)
    plan = transfer.prepare_member_ranges(transfer.read_spec(ref), spans, old)
    assert store.get_into_ranges_calls == 0
    del old
    assert owner_ref() is None
    for generation in (100, 200):
        fresh_values = values + generation
        fresh = publish(transfer, fresh_values)
        actual, control = destination(20), destination(20)
        mark = registration_mark(store)
        transfer.materialize_ranges_into(
            transfer.read_spec(fresh), spans, {"values": control}
        )
        expected_vectors = store.get_into_ranges_requests[-1][1:]
        result = transfer.materialize_prepared_ranges_into(
            transfer.read_spec(fresh), plan, actual
        )
        assert store.get_into_ranges_requests[-1][1:] == expected_vectors
        assert (
            actual.owner.raw
            == control.owner.raw
            == fresh_values.tobytes()[:8] + fresh_values.tobytes()[12:24]
        )
        assert result.objects["values"] is actual
        assert_borrowed(store, mark, actual, control)


@pytest.mark.parametrize("change", ["dtype", "chunk_bytes", "payload_bytes"])
def test_prepared_layout_changes_are_rejected_before_data_transfer(change):
    store, transfer = make_transfer()
    ref = publish(transfer, np.arange(16, dtype=np.uint32))
    dest = destination(8)
    spans = (StructuredMemberRange("values", 0, 8, 0),)
    plan = transfer.prepare_member_ranges(transfer.read_spec(ref), spans, dest)
    changed = publish(
        transfer,
        np.arange(
            17 if change == "payload_bytes" else 16,
            dtype=np.int32 if change == "dtype" else np.uint32,
        ),
        chunk_bytes=16 if change == "chunk_bytes" else 8,
    )
    with pytest.raises(PreparedRangeLayoutChanged):
        transfer.materialize_prepared_ranges_into(
            transfer.read_spec(changed), plan, dest
        )
    assert store.get_into_ranges_calls == 0


@pytest.mark.parametrize(
    "change", ["capacity", "alignment", "owner", "registration", "selection", "slices"]
)
def test_prepared_read_checks_the_current_destination_and_selection(change):
    store, transfer = make_transfer()
    ref = transfer.put_structured_object(
        StructuredObjectPayload(
            buffers={"values": np.arange(4, dtype=np.uint32), "other": b"unused"}
        )
    )
    spec, dest = transfer.read_spec(ref), destination(16)
    plan = transfer.prepare_member_ranges(
        spec, (StructuredMemberRange("values", 0, 16, 0),), dest
    )
    if change == "capacity":
        dest.size = 12
    elif change == "alignment":
        dest.ptr += 1
    elif change == "owner":
        dest.owner = None
    elif change == "registration":
        dest.pre_registered = False
    elif change == "selection":
        spec = spec.select_members(["other"])
    else:
        spec = spec.slice_member("values", 0, 0, 1)
    with pytest.raises((ValueError, KeyError)):
        transfer.materialize_prepared_ranges_into(spec, plan, dest)
    assert store.get_into_ranges_calls == 0


@pytest.mark.parametrize("vectors", [[], [[]], [[[]]], [[[-1]]], [[[7]]]])
def test_malformed_or_failed_native_results_are_not_success(vectors, monkeypatch):
    store, transfer = make_transfer()
    ref = publish(transfer, bytes(8))
    dest = destination(8)
    mark = registration_mark(store)
    monkeypatch.setattr(transfer._transport, "_get_into_ranges", lambda *_: vectors)
    with pytest.raises(RuntimeError):
        transfer.materialize_ranges_into(
            transfer.read_spec(ref),
            (StructuredMemberRange("values", 0, 8, 0),),
            {"values": dest},
        )
    assert_borrowed(store, mark, dest)


@pytest.mark.parametrize(
    "span",
    [
        StructuredMemberRange("values", -1, 4, 0),
        StructuredMemberRange("values", 0, 0, 0),
        StructuredMemberRange("values", 7, 4, 0),
        StructuredMemberRange("values", 0, 8, 4),
        StructuredMemberRange("values", True, 4, 0),
        StructuredMemberRange("values", 0, 4, 0, 3),
        StructuredMemberRange("values", 2, 4, 0, 4),
    ],
)
def test_invalid_ranges_are_rejected_before_native_read(span):
    store, transfer = make_transfer()
    ref = publish(transfer, bytes(8))
    with pytest.raises((TypeError, ValueError)):
        transfer.materialize_ranges_into(
            transfer.read_spec(ref), (span,), {"values": destination(8)}
        )
    assert store.get_into_ranges_calls == 0


def test_missing_transport_is_rejected_without_whole_member_fallback():
    _, transfer = make_transfer(PlainStore())
    ref = publish(transfer, bytes(8))
    with pytest.raises(RuntimeError, match="unavailable"):
        transfer.materialize_ranges_into(
            transfer.read_spec(ref),
            (StructuredMemberRange("values", 0, 4, 0),),
            {"values": destination(4)},
        )


def test_prepared_constructor_checks_immutable_vectors_and_native_bounds():
    _, transfer = make_transfer()
    ref = publish(transfer, bytes(8))
    plan = transfer.prepare_member_ranges(
        transfer.read_spec(ref),
        (StructuredMemberRange("values", 0, 4, 0),),
        destination(4),
    )
    with pytest.raises(ValueError, match="bounds"):
        replace(plan, source_offsets=((100,),))
    with pytest.raises(TypeError, match="tuple"):
        replace(plan, sizes=([4],))
