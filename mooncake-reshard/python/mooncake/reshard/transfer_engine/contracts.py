"""Resource-neutral physical transfer batches."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


_MAX_U64 = (1 << 64) - 1


class TransferDirection(str, Enum):
    READ = "read"
    WRITE = "write"


@dataclass(frozen=True)
class TransferBatchRange:
    """Fragments sharing one contiguous source and target allocation."""

    source_base_address: int
    source_capacity: int
    target_base_address: int
    target_capacity: int
    source_offsets: tuple[int, ...]
    target_offsets: tuple[int, ...]
    sizes: tuple[int, ...]

    def __post_init__(self) -> None:
        for name in (
            "source_base_address",
            "source_capacity",
            "target_base_address",
            "target_capacity",
        ):
            value = getattr(self, name)
            if type(value) is not int or value <= 0 or value > _MAX_U64:
                raise ValueError(f"transfer range {name} is invalid")
        for side in ("source", "target"):
            base = getattr(self, f"{side}_base_address")
            capacity = getattr(self, f"{side}_capacity")
            if capacity > _MAX_U64 - base:
                raise ValueError(f"transfer range {side} allocation overflows")

        object.__setattr__(self, "source_offsets", tuple(self.source_offsets))
        object.__setattr__(self, "target_offsets", tuple(self.target_offsets))
        object.__setattr__(self, "sizes", tuple(self.sizes))
        lengths = {
            len(self.source_offsets),
            len(self.target_offsets),
            len(self.sizes),
        }
        if lengths != {len(self.sizes)} or not self.sizes:
            raise ValueError("transfer range fragments must be non-empty and aligned")
        for index, size in enumerate(self.sizes):
            if type(size) is not int or size <= 0 or size > _MAX_U64:
                raise ValueError("transfer range size is invalid")
            for side in ("source", "target"):
                offset = getattr(self, f"{side}_offsets")[index]
                capacity = getattr(self, f"{side}_capacity")
                if type(offset) is not int or offset < 0 or offset > _MAX_U64:
                    raise ValueError(f"transfer range {side} offset is invalid")
                if offset > capacity or size > capacity - offset:
                    raise ValueError(f"transfer range exceeds {side} allocation bounds")

    @property
    def operation_count(self) -> int:
        return len(self.sizes)


@dataclass(frozen=True)
class TransferBatch:
    endpoint: str
    source_addresses: tuple[int, ...]
    target_addresses: tuple[int, ...]
    sizes: tuple[int, ...]
    ranges: tuple[TransferBatchRange, ...] = ()

    def __post_init__(self) -> None:
        if type(self.endpoint) is not str or not self.endpoint:
            raise ValueError("transfer endpoint must be a non-empty string")
        object.__setattr__(self, "source_addresses", tuple(self.source_addresses))
        object.__setattr__(self, "target_addresses", tuple(self.target_addresses))
        object.__setattr__(self, "sizes", tuple(self.sizes))
        object.__setattr__(self, "ranges", tuple(self.ranges))
        lengths = {
            len(self.source_addresses),
            len(self.target_addresses),
            len(self.sizes),
        }
        if lengths != {len(self.sizes)} or not self.sizes:
            raise ValueError("transfer batch ranges must be non-empty and aligned")
        for name, values in (
            ("source address", self.source_addresses),
            ("target address", self.target_addresses),
            ("size", self.sizes),
        ):
            for value in values:
                if type(value) is not int or value <= 0 or value > _MAX_U64:
                    raise ValueError(f"transfer batch {name} is invalid")
        if self.ranges:
            if not all(isinstance(item, TransferBatchRange) for item in self.ranges):
                raise ValueError("transfer batch ranges are invalid")
            range_source_addresses = tuple(
                item.source_base_address + offset
                for item in self.ranges
                for offset in item.source_offsets
            )
            range_target_addresses = tuple(
                item.target_base_address + offset
                for item in self.ranges
                for offset in item.target_offsets
            )
            range_sizes = tuple(size for item in self.ranges for size in item.sizes)
            if (
                range_source_addresses != self.source_addresses
                or range_target_addresses != self.target_addresses
                or range_sizes != self.sizes
            ):
                raise ValueError(
                    "transfer batch ranges do not match flattened addresses"
                )

    @classmethod
    def from_ranges(
        cls,
        *,
        endpoint: str,
        ranges: tuple[TransferBatchRange, ...],
    ) -> TransferBatch:
        ranges = tuple(ranges)
        if not ranges:
            raise ValueError("transfer batch ranges must not be empty")
        return cls(
            endpoint=endpoint,
            source_addresses=tuple(
                item.source_base_address + offset
                for item in ranges
                for offset in item.source_offsets
            ),
            target_addresses=tuple(
                item.target_base_address + offset
                for item in ranges
                for offset in item.target_offsets
            ),
            sizes=tuple(size for item in ranges for size in item.sizes),
            ranges=ranges,
        )

    @property
    def operation_count(self) -> int:
        return len(self.sizes)

    @property
    def nbytes(self) -> int:
        return sum(self.sizes)


@dataclass(frozen=True)
class TransferBatchReceipt:
    endpoint: str
    direction: TransferDirection
    operation_count: int
    nbytes: int
