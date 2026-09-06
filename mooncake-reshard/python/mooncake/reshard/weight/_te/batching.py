from __future__ import annotations

from collections.abc import Iterable, Iterator

from ...transfer_engine import TransferBatch, TransferBatchRange
from ..planner import BoundWeightFragment, LiveTransferOperation


def iter_transfer_batches(
    endpoint: str,
    operations: Iterable[
        tuple[LiveTransferOperation, BoundWeightFragment, BoundWeightFragment]
    ],
    *,
    max_batch_operations: int,
    max_region_segments: int,
) -> Iterator[TransferBatch]:
    """Preserve allocation boundaries while bounding physical operations."""

    ranges: list[TransferBatchRange] = []
    batch_operation_count = 0

    for operation, source, target in operations:
        source_offsets: list[int] = []
        target_offsets: list[int] = []
        sizes: list[int] = []

        for source_offset, target_offset, nbytes in operation.iter_segments(
            max_segments=max_region_segments
        ):
            source_offsets.append(source_offset)
            target_offsets.append(target_offset)
            sizes.append(nbytes)
            batch_operation_count += 1

            if batch_operation_count == max_batch_operations:
                ranges.append(
                    _range_from_fragments(
                        source,
                        target,
                        source_offsets,
                        target_offsets,
                        sizes,
                    )
                )
                yield TransferBatch.from_ranges(
                    endpoint=endpoint,
                    ranges=tuple(ranges),
                )
                ranges = []
                batch_operation_count = 0
                source_offsets = []
                target_offsets = []
                sizes = []

        if sizes:
            ranges.append(
                _range_from_fragments(
                    source,
                    target,
                    source_offsets,
                    target_offsets,
                    sizes,
                )
            )

    if ranges:
        yield TransferBatch.from_ranges(
            endpoint=endpoint,
            ranges=tuple(ranges),
        )


def _range_from_fragments(
    source: BoundWeightFragment,
    target: BoundWeightFragment,
    source_offsets: list[int],
    target_offsets: list[int],
    sizes: list[int],
) -> TransferBatchRange:
    return TransferBatchRange(
        source_base_address=source.storage_address,
        source_capacity=source.storage_nbytes,
        target_base_address=target.storage_address,
        target_capacity=target.storage_nbytes,
        source_offsets=tuple(
            source.storage_offset_bytes + offset for offset in source_offsets
        ),
        target_offsets=tuple(
            target.storage_offset_bytes + offset for offset in target_offsets
        ),
        sizes=tuple(sizes),
    )
