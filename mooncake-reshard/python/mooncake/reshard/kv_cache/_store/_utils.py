"""Internal helpers for Store KV reshard geometry, batching and metadata."""

from __future__ import annotations

import hashlib
import json
from bisect import bisect_right
from collections.abc import Iterable, Iterator, Mapping
from typing import Generic, NamedTuple, TypeVar, cast

from ..resolved import KVCacheResolvedRange
from ..types import KVCacheComponent, KVCacheDescriptor

_T = TypeVar("_T")


class HeadIntervalIndex(Generic[_T]):
    """Index validated, disjoint intervals with optional exact-interval replicas.

    Candidates within an interval retain caller order. Callers validate source
    ownership; the index contains no object keys or runtime addresses.
    """

    def __init__(self, entries: Iterable[tuple[int, int, _T]]) -> None:
        grouped: dict[tuple[int, int], list[_T]] = {}
        for start, end, value in entries:
            grouped.setdefault((start, end), []).append(value)
        self._runs = tuple(
            (start, end, tuple(grouped[(start, end)])) for start, end in sorted(grouped)
        )
        for previous, current in zip(self._runs, self._runs[1:]):
            if previous[1] > current[0]:
                raise ValueError(
                    "overlapping source head intervals without exact replicas"
                )
        self._starts = tuple(run[0] for run in self._runs)

    def cover(self, start: int, end: int) -> Iterator[tuple[int, int, tuple[_T, ...]]]:
        """Yield a complete clipped cover without visiting individual heads."""
        index = max(0, bisect_right(self._starts, start) - 1)
        cursor = start
        while index < len(self._runs) and self._runs[index][0] < end:
            lo, hi, candidates = self._runs[index]
            lo, hi = max(start, lo), min(end, hi)
            if lo != cursor or hi <= lo:
                raise ValueError("source head coverage has a gap")
            yield lo, hi, candidates
            cursor = hi
            index += 1
        if cursor != end:
            raise ValueError("source head coverage is incomplete")


class KVCacheLogicalShard(NamedTuple):
    layer_ids: tuple[int, ...]
    head_start: int
    head_count: int
    replica_ordinal: int = 0
    replica_count: int = 1


def validate_logical_coverage(
    descriptor: KVCacheDescriptor,
    shards: tuple[KVCacheLogicalShard, ...],
    *,
    label: str,
) -> None:
    """Prove one complete logical copy, allowing only exact declared replicas."""
    layers: dict[int, list[KVCacheLogicalShard]] = {
        layer: [] for layer in descriptor.global_layer_ids
    }
    for shard in shards:
        end = shard.head_start + shard.head_count
        if end > descriptor.total_kv_heads:
            raise ValueError(f"{label} head interval exceeds total_kv_heads")
        for layer in shard.layer_ids:
            if layer not in layers:
                raise ValueError(f"{label} contains an unknown global layer")
            layers[layer].append(shard)
    for layer, items in layers.items():
        try:
            index = HeadIntervalIndex(
                (s.head_start, s.head_start + s.head_count, s) for s in items
            )
            for _, _, owners in index.cover(0, descriptor.total_kv_heads):
                if len(owners) == 1:
                    if owners[0].replica_count != 1:
                        raise ValueError("incomplete declared KV-head replica set")
                elif {s.replica_count for s in owners} != {len(owners)} or {
                    s.replica_ordinal for s in owners
                } != set(range(len(owners))):
                    raise ValueError(
                        "overlapping KV heads must be an exact declared replica set"
                    )
        except ValueError as error:
            raise ValueError(f"{label} layer={layer}: {error}") from error


class KVCacheRangeGeometry(NamedTuple):
    token_start: int
    token_count: int
    head_start: int
    head_count: int
    offset_bytes: int
    token_stride_bytes: int
    head_stride_bytes: int


class KVCacheCopyGeometry(NamedTuple):
    source: KVCacheRangeGeometry | KVCacheResolvedRange
    target: KVCacheRangeGeometry | KVCacheResolvedRange
    token_start: int
    token_count: int
    head_start: int
    head_count: int
    token_step: int
    head_step: int
    head_bytes: int

    @property
    def segment_count(self) -> int:
        return (self.token_count // self.token_step) * (
            self.head_count // self.head_step
        )

    @property
    def nbytes(self) -> int:
        return self.token_count * self.head_count * self.head_bytes

    def segments(self) -> Iterator[tuple[int, int, int]]:
        """Yield source offset, target offset, length in token/head order."""
        source, target = self.source, self.target
        source_base = (
            source.offset_bytes
            + (self.token_start - source.token_start) * source.token_stride_bytes
            + (self.head_start - source.head_start) * source.head_stride_bytes
        )
        target_base = (
            target.offset_bytes
            + (self.token_start - target.token_start) * target.token_stride_bytes
            + (self.head_start - target.head_start) * target.head_stride_bytes
        )
        size = self.token_step * self.head_step * self.head_bytes
        for token in range(0, self.token_count, self.token_step):
            source_row = source_base + token * source.token_stride_bytes
            target_row = target_base + token * target.token_stride_bytes
            for head in range(0, self.head_count, self.head_step):
                yield (
                    source_row + head * source.head_stride_bytes,
                    target_row + head * target.head_stride_bytes,
                    size,
                )


def plan_range_copy(
    source: KVCacheRangeGeometry | KVCacheResolvedRange,
    target: KVCacheRangeGeometry | KVCacheResolvedRange,
    head_bytes: int,
    *,
    head_window: tuple[int, int] | None = None,
) -> KVCacheCopyGeometry | None:
    """Intersect logical ranges and coalesce only bytes contiguous on both sides."""
    start = max(source.token_start, target.token_start)
    end = min(
        source.token_start + source.token_count, target.token_start + target.token_count
    )
    h0 = max(source.head_start, target.head_start)
    h1 = min(
        source.head_start + source.head_count, target.head_start + target.head_count
    )
    if head_window is not None:
        h0, h1 = max(h0, head_window[0]), min(h1, head_window[1])
    if start >= end or h0 >= h1:
        return None
    head_step = (
        h1 - h0
        if source.head_stride_bytes == target.head_stride_bytes == head_bytes
        else 1
    )
    width = head_step * head_bytes
    token_step = (
        end - start
        if head_step == h1 - h0
        and source.token_stride_bytes == target.token_stride_bytes == width
        else 1
    )
    return KVCacheCopyGeometry(
        source,
        target,
        start,
        end - start,
        h0,
        h1 - h0,
        token_step,
        head_step,
        head_bytes,
    )


def index_runtime_ranges(
    ranges: tuple[KVCacheResolvedRange, ...],
) -> dict[tuple[int, KVCacheComponent], list[KVCacheResolvedRange]]:
    """Index each validated runtime range once, preserving its canonical order."""
    result: dict[tuple[int, KVCacheComponent], list[KVCacheResolvedRange]] = {}
    for item in ranges:
        result.setdefault((item.global_layer_id, item.component), []).append(item)
    return result


def canonical_json(value: object, *, allow_nan: bool = True) -> str:
    """Keep canonical encoding identical across IDs and wire envelopes."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=allow_nan)


def canonical_digest(value: object) -> str:
    encoded = canonical_json(value).encode()
    return hashlib.sha256(encoded).hexdigest()


def json_string(value: object, label: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{label} must be a non-empty string")
    return value


def json_integer(value: object, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{label} must be an integer at least {minimum}")
    return value


def load_json_object(value: str, label: str) -> Mapping[str, object]:
    if type(value) is not str or len(value.encode("utf-8")) > 16 * 1024 * 1024:
        raise ValueError("KV-cache JSON must be text within the 16 MiB wire limit")

    def reject_constant(constant: str) -> None:
        raise ValueError(f"non-finite JSON number is unsupported: {constant}")

    def reject_duplicate_fields(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON field: {key}")
            result[key] = item
        return result

    try:
        payload = json.loads(
            value,
            parse_constant=reject_constant,
            object_pairs_hook=reject_duplicate_fields,
        )
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} is not valid JSON") from error
    if not isinstance(payload, Mapping):
        raise ValueError(f"{label} must be a JSON object")  # noqa: TRY004
    return cast(Mapping[str, object], payload)
