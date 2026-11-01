"""Relative byte-range plans for caller-owned, registered structured buffers.

This module does not register memory or own a pool lease. The synchronous
executor borrows a destination through completion; the caller owns registration
and must not release or modify the buffer while the read is running.
"""

from __future__ import annotations

import ctypes
import json
import math
from bisect import bisect_right
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np


@dataclass(frozen=True)
class StructuredMemberRange:
    """Copy a member byte interval to a destination byte interval."""

    member_name: str
    source_byte_offset: int
    byte_length: int
    destination_byte_offset: int
    alignment: int = 1


class PreparedRangeLayoutChanged(ValueError):
    """The current schema or chunk layout requires preparing a new plan."""


@dataclass(frozen=True)
class PreparedStructuredMemberRanges:
    """Relative chunk geometry, with no generation keys, addresses or owners.

    Construct through ``prepare_member_ranges``. A changed delta distribution
    needs new ranges; only unchanged relative range geometry is reusable.
    """

    member_name: str
    field_signature: str
    chunk_bytes: tuple[int, ...]
    chunk_indices: tuple[int, ...]
    destination_offsets: tuple[tuple[int, ...], ...]
    source_offsets: tuple[tuple[int, ...], ...]
    sizes: tuple[tuple[int, ...], ...]
    destination_bytes: int
    alignment: int

    def __post_init__(self) -> None:
        if not isinstance(self.member_name, str) or not self.member_name:
            raise ValueError("prepared member_name must be a non-empty string")
        if not isinstance(self.field_signature, str):
            raise TypeError("prepared field_signature must be a string")
        for name in (
            "chunk_bytes",
            "chunk_indices",
            "destination_offsets",
            "source_offsets",
            "sizes",
        ):
            value = getattr(self, name)
            if not isinstance(value, tuple):
                raise TypeError(f"prepared {name} must be a tuple")
        if not self.chunk_indices or not (
            len(self.chunk_indices)
            == len(self.destination_offsets)
            == len(self.source_offsets)
            == len(self.sizes)
        ):
            raise ValueError("prepared range columns differ in length or are empty")
        if any(_integer(size, "chunk bytes") < 0 for size in self.chunk_bytes):
            raise ValueError("chunk bytes must be non-negative")
        alignment = _integer(self.alignment, "alignment")
        capacity = _integer(self.destination_bytes, "destination bytes")
        if alignment <= 0 or capacity <= 0:
            raise ValueError(
                "prepared alignment and destination bytes must be positive"
            )
        _, natural_alignment = _schema(
            {
                "__mooncake_structured_fields__": {
                    self.member_name: json.loads(self.field_signature)
                }
            },
            self.member_name,
        )
        if alignment % natural_alignment:
            raise ValueError("prepared alignment differs from the member element width")
        intervals = []
        for index, destinations, sources, sizes in zip(
            self.chunk_indices,
            self.destination_offsets,
            self.source_offsets,
            self.sizes,
        ):
            index = _integer(index, "chunk index")
            if index < 0 or index >= len(self.chunk_bytes):
                raise ValueError("prepared chunk index is out of bounds")
            if any(
                not isinstance(column, tuple)
                for column in (destinations, sources, sizes)
            ):
                raise TypeError("prepared range columns must contain tuples")
            if not sizes or not (len(destinations) == len(sources) == len(sizes)):
                raise ValueError(
                    "prepared fragment columns differ in length or are empty"
                )
            for destination, source, size in zip(destinations, sources, sizes):
                destination = _integer(destination, "destination offset")
                source = _integer(source, "source offset")
                size = _integer(size, "range size")
                if source < 0 or destination < 0 or size <= 0:
                    raise ValueError(
                        "prepared offsets must be non-negative and sizes positive"
                    )
                if (
                    source + size > self.chunk_bytes[index]
                    or destination + size > capacity
                ):
                    raise ValueError("prepared range exceeds buffer bounds")
                intervals.append((destination, destination + size))
        _nonoverlap(intervals)


def _integer(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)):
        raise TypeError(f"{label} must be an integer")
    return int(value)


def _destination(
    destination: Any, label: str, alignment: int = 1, capacity: int = 1
) -> None:
    ptr = _integer(destination.ptr, f"{label} destination ptr")
    size = _integer(destination.size, f"{label} destination size")
    if (
        ptr <= 0
        or size < capacity
        or ptr + size > 1 << (ctypes.sizeof(ctypes.c_void_p) * 8)
    ):
        raise ValueError(f"{label} destination bounds are invalid")
    if destination.owner is None:
        raise ValueError(f"{label} destination owner is required")
    if destination.pre_registered is not True:
        raise ValueError(f"{label} requires a pre_registered destination")
    if ptr % alignment:
        raise ValueError(f"{label} destination must be aligned to {alignment} bytes")


def _nonoverlap(intervals: Sequence[tuple[int, int]]) -> None:
    ordered = sorted(intervals)
    if any(current[0] < previous[1] for previous, current in zip(ordered, ordered[1:])):
        raise ValueError("structured ranged destinations overlap")


def _schema(metadata: Mapping[str, Any], name: str) -> tuple[str, int]:
    spec = metadata.get("__mooncake_structured_fields__", {}).get(
        name, {"encoding": "bytes"}
    )
    if spec.get("encoding", "bytes") not in {"bytes", "ndarray"}:
        raise ValueError(
            "structured ranges require raw-layout bytes or ndarray members"
        )
    alignment = 1
    if spec.get("encoding") == "ndarray":
        dtype = np.dtype(spec["dtype"])
        if dtype.hasobject or dtype.itemsize <= 0:
            raise ValueError(
                "structured ndarray member must have a fixed raw element width"
            )
        alignment = dtype.itemsize
    return json.dumps(spec, sort_keys=True, separators=(",", ":")), alignment


def _chunks(payload: Mapping[str, Any]) -> tuple[list[str], tuple[int, ...]]:
    keys = [chunk["key"] for chunk in payload["chunks"]]
    sizes = tuple(
        _integer(chunk["bytes"], "chunk bytes") for chunk in payload["chunks"]
    )
    if any(not isinstance(key, str) or not key for key in keys) or len(
        set(keys)
    ) != len(keys):
        raise ValueError("structured ranges require distinct non-empty chunk keys")
    if any(size < 0 for size in sizes) or sum(sizes) != _integer(
        payload["bytes"], "payload bytes"
    ):
        raise ValueError("structured chunk byte layout is inconsistent")
    return keys, sizes


def prepare_ranges(
    metadata: Mapping[str, Any],
    buffers: Mapping[str, Any],
    selected: Sequence[str],
    ranges: Sequence[StructuredMemberRange],
    destinations: Mapping[str, Any],
) -> dict[str, PreparedStructuredMemberRanges]:
    if not ranges:
        raise ValueError("structured ranged materialization requires ranges")
    grouped: dict[str, list[StructuredMemberRange]] = {}
    for span in ranges:
        if not isinstance(span, StructuredMemberRange):
            raise TypeError(
                "structured ranges must contain StructuredMemberRange values"
            )
        if not isinstance(span.member_name, str) or not span.member_name:
            raise ValueError("structured range member_name must be a non-empty string")
        if span.member_name not in selected:
            raise KeyError(
                f"structured range references unselected member: {span.member_name!r}"
            )
        grouped.setdefault(span.member_name, []).append(span)
    if set(destinations) != set(grouped):
        raise ValueError("structured ranged destinations must match range members")
    plans = {}
    absolute_intervals = []
    for name, spans in grouped.items():
        destination = destinations[name]
        _destination(destination, name)
        signature, natural_alignment = _schema(metadata, name)
        _, chunk_sizes = _chunks(buffers[name])
        offsets = [0]
        for size in chunk_sizes:
            offsets.append(offsets[-1] + size)
        fragments: dict[int, list[tuple[int, int, int]]] = {}
        required_bytes = 0
        member_alignment = natural_alignment
        for span in spans:
            source = _integer(span.source_byte_offset, "source_byte_offset")
            target = _integer(span.destination_byte_offset, "destination_byte_offset")
            length = _integer(span.byte_length, "byte_length")
            alignment = _integer(span.alignment, "alignment")
            if source < 0 or target < 0 or length <= 0:
                raise ValueError(
                    "range offsets must be non-negative and byte_length positive"
                )
            if alignment <= 0 or alignment & (alignment - 1):
                raise ValueError("range alignment must be a positive power of two")
            alignment = math.lcm(natural_alignment, alignment)
            if any(
                value % alignment for value in (source, target, length, destination.ptr)
            ):
                raise ValueError(
                    f"structured range must be aligned to {alignment} bytes"
                )
            if source + length > offsets[-1] or target + length > destination.size:
                raise ValueError(
                    "structured range exceeds source or destination bounds"
                )
            absolute_intervals.append(
                (destination.ptr + target, destination.ptr + target + length)
            )
            required_bytes = max(required_bytes, target + length)
            member_alignment = math.lcm(member_alignment, alignment)
            index = max(0, bisect_right(offsets, source) - 1)
            while index < len(chunk_sizes) and offsets[index] < source + length:
                begin, end = (
                    max(source, offsets[index]),
                    min(source + length, offsets[index + 1]),
                )
                if begin < end:
                    fragments.setdefault(index, []).append(
                        (begin - offsets[index], target + begin - source, end - begin)
                    )
                index += 1
        _destination(destination, name, member_alignment, required_bytes)
        sources, targets, lengths = [], [], []
        for spans_in_chunk in fragments.values():
            merged = []
            for source, target, length in sorted(spans_in_chunk):
                if (
                    merged
                    and merged[-1][0] + merged[-1][2] == source
                    and merged[-1][1] + merged[-1][2] == target
                ):
                    previous_source, previous_target, previous_length = merged[-1]
                    merged[-1] = (
                        previous_source,
                        previous_target,
                        previous_length + length,
                    )
                else:
                    merged.append((source, target, length))
            sources.append(tuple(item[0] for item in merged))
            targets.append(tuple(item[1] for item in merged))
            lengths.append(tuple(item[2] for item in merged))
        plans[name] = PreparedStructuredMemberRanges(
            name,
            signature,
            chunk_sizes,
            tuple(fragments),
            tuple(targets),
            tuple(sources),
            tuple(lengths),
            required_bytes,
            member_alignment,
        )
    _nonoverlap(absolute_intervals)
    return plans


def bind_prepared(
    metadata: Mapping[str, Any],
    buffers: Mapping[str, Any],
    selected: Sequence[str],
    prepared: PreparedStructuredMemberRanges,
    destination: Any,
) -> list[str]:
    if not isinstance(prepared, PreparedStructuredMemberRanges):
        raise TypeError("prepared must be PreparedStructuredMemberRanges")
    name = prepared.member_name
    if name not in selected:
        raise KeyError(f"structured range references unselected member: {name!r}")
    signature, natural_alignment = _schema(metadata, name)
    keys, chunk_sizes = _chunks(buffers[name])
    if signature != prepared.field_signature or chunk_sizes != prepared.chunk_bytes:
        raise PreparedRangeLayoutChanged(
            "prepared member layout changed; prepare ranges again"
        )
    if prepared.alignment % natural_alignment:
        raise ValueError("prepared alignment differs from the member element width")
    _destination(destination, name, prepared.alignment, prepared.destination_bytes)
    return [keys[index] for index in prepared.chunk_indices]


def execute_ranges(
    get_into_ranges: Any,
    requests: Sequence[tuple[Any, list[str], PreparedStructuredMemberRanges]],
) -> None:
    if not callable(get_into_ranges):
        raise RuntimeError(
            "get_into_ranges is unavailable for structured ranged materialization"
        )
    # requests retain the current destination owners until synchronous completion.
    results = get_into_ranges(
        [destination.ptr for destination, _, _ in requests],
        [keys for _, keys, _ in requests],
        [
            [list(column) for column in plan.destination_offsets]
            for _, _, plan in requests
        ],
        [[list(column) for column in plan.source_offsets] for _, _, plan in requests],
        [[list(column) for column in plan.sizes] for _, _, plan in requests],
    )
    if len(results) != len(requests):
        raise RuntimeError("get_into_ranges returned invalid destination result count")
    for (_, keys, plan), actual in zip(requests, results):
        if len(actual) != len(keys):
            raise RuntimeError("get_into_ranges returned invalid ranged result shape")
        for expected_sizes, actual_sizes in zip(plan.sizes, actual):
            if len(expected_sizes) != len(actual_sizes):
                raise RuntimeError(
                    "get_into_ranges returned invalid ranged fragment count"
                )
            if any(
                size != expected for size, expected in zip(actual_sizes, expected_sizes)
            ):
                raise RuntimeError(
                    "get_into_ranges failed: returned byte count differs"
                )
