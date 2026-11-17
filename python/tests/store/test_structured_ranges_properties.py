"""Property tests for structured member byte-range plans.

Planning is a pure function from a member schema, a chunk layout and
requested byte ranges to per-chunk fragments bound to a borrowed
destination.  Each test states one contract of that function as an
invariant over random layouts, ranges and destinations and checks the
executed bytes against position-coded payloads, so any fragment that
lands on the wrong source or destination byte changes the result.
"""

from __future__ import annotations

import ctypes
from dataclasses import FrozenInstanceError, replace

import pytest
from hypothesis import assume, given, settings, strategies as st

from mooncake._structured_ranges import (
    PreparedRangeLayoutChanged,
    StructuredMemberRange,
    bind_prepared,
    execute_ranges,
    prepare_ranges,
)
from mooncake.structured_object_store import raw_destination

_MEMBER_NAMES = ("alpha", "beta")
_SENTINEL = 0x7F


def _coded_payload(member_index: int, length: int, generation: int = 0) -> bytes:
    return bytes(
        (64 * member_index + position + 101 * generation) % 256
        for position in range(length)
    )


def _buffers_entry(member_index: int, sizes, generation: int = 0):
    keys = [f"m{member_index}-g{generation}-c{index}" for index in range(len(sizes))]
    return {
        "chunks": [{"key": key, "bytes": size} for key, size in zip(keys, sizes)],
        "bytes": sum(sizes),
    }


def _chunk_store(buffers, payloads):
    objects = {}
    for name, payload in payloads.items():
        offset = 0
        for chunk in buffers[name]["chunks"]:
            objects[chunk["key"]] = payload[offset : offset + chunk["bytes"]]
            offset += chunk["bytes"]
    return objects


def _faithful_executor(objects):
    def get_into_ranges(ptrs, all_keys, all_destinations, all_sources, all_sizes):
        results = []
        for ptr, keys, destinations, sources, sizes in zip(
            ptrs, all_keys, all_destinations, all_sources, all_sizes
        ):
            per_buffer = []
            for key, fragment_destinations, fragment_sources, fragment_sizes in zip(
                keys, destinations, sources, sizes
            ):
                data = objects[key]
                written = []
                for target, source, size in zip(
                    fragment_destinations, fragment_sources, fragment_sizes
                ):
                    ctypes.memmove(ptr + target, data[source : source + size], size)
                    written.append(size)
                per_buffer.append(written)
            results.append(per_buffer)
        return results

    return get_into_ranges


@st.composite
def _members(draw):
    """Members with a chunked payload, disjoint-destination ranges and element width."""

    count = draw(st.integers(1, 2))
    shared_destination = count == 2 and draw(st.booleans())
    members = []
    cursor = 0
    for member_index in range(count):
        element = draw(st.sampled_from((1, 1, 4)))
        length = draw(st.integers(1, 16)) * element
        cuts = draw(st.lists(st.integers(0, length), max_size=3))
        bounds = sorted([0, *cuts, length])
        sizes = tuple(b - a for a, b in zip(bounds, bounds[1:]))
        if shared_destination:
            cursor += (-cursor) % element
        else:
            cursor = 0
        spans = []
        for _ in range(draw(st.integers(1, 3))):
            size = draw(st.integers(1, length // element)) * element
            source = draw(st.integers(0, (length - size) // element)) * element
            cursor += draw(st.integers(0, 2)) * element
            spans.append(
                StructuredMemberRange(_MEMBER_NAMES[member_index], source, size, cursor)
            )
            cursor += size
        required = cursor
        members.append((member_index, element, length, sizes, tuple(spans), required))
    return members, shared_destination


def _scenario(members, shared_destination, generation=0, slack=0):
    metadata_fields, buffers, payloads, ranges = {}, {}, {}, []
    for member_index, element, length, sizes, spans, _ in members:
        name = _MEMBER_NAMES[member_index]
        if element > 1:
            metadata_fields[name] = {
                "encoding": "ndarray",
                "dtype": "uint32",
                "shape": [length // element],
            }
        buffers[name] = _buffers_entry(member_index, sizes, generation)
        payloads[name] = _coded_payload(member_index, length, generation)
        ranges.extend(spans)
    metadata = (
        {"__mooncake_structured_fields__": metadata_fields} if metadata_fields else {}
    )
    capacity = max(member[5] for member in members) + slack
    alignment = max(member[1] for member in members)
    arena = (ctypes.c_char * (capacity + alignment))()
    base = ctypes.addressof(arena)
    base += (-base) % alignment
    destinations = {}
    for member_index, element, _, _, _, required in members:
        name = _MEMBER_NAMES[member_index]
        if shared_destination:
            destinations[name] = raw_destination(
                base, capacity, owner=arena, pre_registered=True
            )
        else:
            local = (ctypes.c_char * (required + slack + element))()
            pointer = ctypes.addressof(local)
            pointer += (-pointer) % element
            destinations[name] = raw_destination(
                pointer, required + slack, owner=local, pre_registered=True
            )
    selected = [_MEMBER_NAMES[member[0]] for member in members]
    return metadata, buffers, payloads, tuple(ranges), destinations, selected


@given(_members())
@settings(max_examples=200, deadline=None)
def test_fragments_stay_inside_chunks_and_destination_and_never_overlap(case):
    members, shared = case
    metadata, buffers, _, ranges, destinations, selected = _scenario(members, shared)
    plans = prepare_ranges(metadata, buffers, selected, ranges, destinations)
    assert set(plans) == {span.member_name for span in ranges}
    covered = {}
    for name, plan in plans.items():
        destination = destinations[name]
        assert plan.destination_bytes <= destination.size
        for index, fragment_destinations, fragment_sources, sizes in zip(
            plan.chunk_indices,
            plan.destination_offsets,
            plan.source_offsets,
            plan.sizes,
        ):
            assert plan.chunk_bytes[index] > 0
            for target, source, size in zip(
                fragment_destinations, fragment_sources, sizes
            ):
                assert 0 <= source and source + size <= plan.chunk_bytes[index]
                assert 0 <= target and target + size <= destination.size
                for absolute in range(
                    destination.ptr + target, destination.ptr + target + size
                ):
                    assert absolute not in covered
                    covered[absolute] = name
    requested = {
        destinations[span.member_name].ptr + span.destination_byte_offset + offset
        for span in ranges
        for offset in range(span.byte_length)
    }
    assert set(covered) == requested


@given(_members(), st.integers(0, 8))
@settings(max_examples=200, deadline=None)
def test_execution_writes_exactly_the_requested_bytes_and_nothing_else(case, slack):
    members, shared = case
    metadata, buffers, payloads, ranges, destinations, selected = _scenario(
        members, shared, slack=slack
    )
    for destination in destinations.values():
        ctypes.memset(destination.ptr, _SENTINEL, destination.size)
    plans = prepare_ranges(metadata, buffers, selected, ranges, destinations)
    requests = [
        (
            destinations[name],
            bind_prepared(metadata, buffers, selected, plan, destinations[name]),
            plan,
        )
        for name, plan in plans.items()
    ]
    execute_ranges(_faithful_executor(_chunk_store(buffers, payloads)), requests)
    for span in ranges:
        actual = ctypes.string_at(
            destinations[span.member_name].ptr + span.destination_byte_offset,
            span.byte_length,
        )
        expected = payloads[span.member_name][
            span.source_byte_offset : span.source_byte_offset + span.byte_length
        ]
        assert actual == expected
    written = {
        (destinations[span.member_name].ptr, span.destination_byte_offset + offset)
        for span in ranges
        for offset in range(span.byte_length)
    }
    for destination in destinations.values():
        remainder = ctypes.string_at(destination.ptr, destination.size)
        for offset, value in enumerate(remainder):
            if (destination.ptr, offset) not in written:
                assert value == _SENTINEL


@given(_members())
@settings(max_examples=200, deadline=None)
def test_fragments_in_one_chunk_are_maximally_coalesced(case):
    members, shared = case
    metadata, buffers, _, ranges, destinations, selected = _scenario(members, shared)
    plans = prepare_ranges(metadata, buffers, selected, ranges, destinations)
    for plan in plans.values():
        for fragment_destinations, fragment_sources, sizes in zip(
            plan.destination_offsets, plan.source_offsets, plan.sizes
        ):
            fragments = sorted(zip(fragment_sources, fragment_destinations, sizes))
            for (source_a, target_a, size_a), (source_b, target_b, _) in zip(
                fragments, fragments[1:]
            ):
                assert not (
                    source_a + size_a == source_b and target_a + size_a == target_b
                )


@given(_members())
@settings(max_examples=150, deadline=None)
def test_preparing_is_deterministic_and_plans_are_immutable(case):
    members, shared = case
    metadata, buffers, _, ranges, destinations, selected = _scenario(members, shared)
    first = prepare_ranges(metadata, buffers, selected, ranges, destinations)
    second = prepare_ranges(metadata, buffers, selected, ranges, destinations)
    assert first == second
    plan = next(iter(first.values()))
    with pytest.raises(FrozenInstanceError):
        plan.destination_bytes = 1
    if plan.chunk_indices:
        with pytest.raises(ValueError):
            replace(
                plan,
                source_offsets=tuple(
                    tuple(source + plan.chunk_bytes[index] for source in sources)
                    for index, sources in zip(plan.chunk_indices, plan.source_offsets)
                ),
            )


@given(_members(), st.integers(1, 3))
@settings(max_examples=150, deadline=None)
def test_prepared_plans_rebind_to_fresh_keys_and_read_the_fresh_bytes(case, generation):
    members, shared = case
    metadata, buffers, _, ranges, destinations, selected = _scenario(members, shared)
    plans = prepare_ranges(metadata, buffers, selected, ranges, destinations)
    fresh_metadata, fresh_buffers, fresh_payloads, _, fresh_destinations, _ = _scenario(
        members, shared, generation=generation
    )
    assert fresh_metadata == metadata
    requests = []
    for name, plan in plans.items():
        keys = bind_prepared(
            fresh_metadata, fresh_buffers, selected, plan, fresh_destinations[name]
        )
        current = [chunk["key"] for chunk in fresh_buffers[name]["chunks"]]
        assert keys == [current[index] for index in plan.chunk_indices]
        requests.append((fresh_destinations[name], keys, plan))
    execute_ranges(
        _faithful_executor(_chunk_store(fresh_buffers, fresh_payloads)), requests
    )
    for span in ranges:
        actual = ctypes.string_at(
            fresh_destinations[span.member_name].ptr + span.destination_byte_offset,
            span.byte_length,
        )
        expected = fresh_payloads[span.member_name][
            span.source_byte_offset : span.source_byte_offset + span.byte_length
        ]
        assert actual == expected


@given(_members())
@settings(max_examples=150, deadline=None)
def test_changed_chunk_layout_or_schema_is_rejected_before_execution(case):
    members, shared = case
    metadata, buffers, _, ranges, destinations, selected = _scenario(members, shared)
    plans = prepare_ranges(metadata, buffers, selected, ranges, destinations)
    name, plan = next(iter(plans.items()))
    grown = dict(buffers)
    member_index = _MEMBER_NAMES.index(name)
    element = members[member_index][1]
    grown[name] = _buffers_entry(
        member_index, (*plan.chunk_bytes, element), generation=7
    )
    with pytest.raises(PreparedRangeLayoutChanged):
        bind_prepared(metadata, grown, selected, plan, destinations[name])
    if element > 1:
        changed = {
            "__mooncake_structured_fields__": {
                **metadata["__mooncake_structured_fields__"],
                name: {
                    **metadata["__mooncake_structured_fields__"][name],
                    "dtype": "int32",
                },
            }
        }
        with pytest.raises(PreparedRangeLayoutChanged):
            bind_prepared(changed, buffers, selected, plan, destinations[name])


@given(_members(), st.data())
@settings(max_examples=150, deadline=None)
def test_overlapping_destinations_and_out_of_bounds_ranges_are_rejected(case, data):
    members, shared = case
    metadata, buffers, _, ranges, destinations, selected = _scenario(members, shared)
    span = data.draw(st.sampled_from(ranges))
    element = members[_MEMBER_NAMES.index(span.member_name)][1]
    overlapping = ranges + (replace(span, source_byte_offset=0),)
    with pytest.raises(ValueError, match="overlap"):
        prepare_ranges(metadata, buffers, selected, overlapping, destinations)
    if shared:
        aliased = tuple(
            replace(spans[0], destination_byte_offset=0) for *_, spans, _ in members
        )
        with pytest.raises(ValueError, match="overlap"):
            prepare_ranges(metadata, buffers, selected, aliased, destinations)
    member_bytes = buffers[span.member_name]["bytes"]
    beyond_source = (replace(span, source_byte_offset=member_bytes),)
    with pytest.raises(ValueError, match="bounds|positive|aligned"):
        prepare_ranges(
            metadata,
            buffers,
            selected,
            beyond_source,
            {span.member_name: destinations[span.member_name]},
        )
    destination = destinations[span.member_name]
    beyond_destination = (replace(span, destination_byte_offset=destination.size),)
    with pytest.raises(ValueError, match="bounds|aligned"):
        prepare_ranges(
            metadata,
            buffers,
            selected,
            beyond_destination,
            {span.member_name: destination},
        )
    if element > 1:
        assume(span.byte_length > 1)
        misaligned = (replace(span, byte_length=span.byte_length - 1),)
        with pytest.raises(ValueError, match="aligned"):
            prepare_ranges(
                metadata,
                buffers,
                selected,
                misaligned,
                {span.member_name: destination},
            )


@given(_members(), st.data())
@settings(max_examples=100, deadline=None)
def test_executor_byte_count_mismatches_are_failures(case, data):
    members, shared = case
    metadata, buffers, payloads, ranges, destinations, selected = _scenario(
        members, shared
    )
    plans = prepare_ranges(metadata, buffers, selected, ranges, destinations)
    requests = [
        (
            destinations[name],
            bind_prepared(metadata, buffers, selected, plan, destinations[name]),
            plan,
        )
        for name, plan in plans.items()
    ]
    faithful = _faithful_executor(_chunk_store(buffers, payloads))
    request_index = data.draw(st.integers(0, len(requests) - 1))

    def tampered(*arguments):
        results = faithful(*arguments)
        results[request_index][-1][-1] -= 1
        return results

    with pytest.raises(RuntimeError, match="byte count|fragment|shape"):
        execute_ranges(tampered, requests)
