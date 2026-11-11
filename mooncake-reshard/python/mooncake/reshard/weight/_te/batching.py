from __future__ import annotations

from collections.abc import Iterable, Iterator

from ...transfer_engine import TransferBatch, TransferBatchRange
from ..planner import BoundWeightFragment, LiveTransferOperation, TransferRegion


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
        # Canonical regions can lower each innermost run in bounded C loops.
        # Subclasses retain their custom segment iterator.
        if (
            type(operation) is TransferRegion
            and operation.outer_loop_counts
            and (
                len(operation.outer_loop_counts) == 1
                or operation.outer_loop_counts[-1] > 1
            )
            and operation.source_strides[-1] > 0
            and operation.target_strides[-1] > 0
        ):
            # Preserve the iterator's budget rejection before producing a batch.
            next(iter(operation.iter_segments(max_segments=max_region_segments)))
            source_stride = operation.source_strides[-1]
            target_stride = operation.target_strides[-1]
            prefix_counts = operation.outer_loop_counts[:-1]
            prefix_indices = [0] * len(prefix_counts)
            prefix_source_offset = (
                source.storage_offset_bytes + operation.source_base_offset
            )
            prefix_target_offset = (
                target.storage_offset_bytes + operation.target_base_offset
            )
            while True:
                remaining = operation.outer_loop_counts[-1]
                source_offset = prefix_source_offset
                target_offset = prefix_target_offset
                while remaining:
                    count = min(remaining, max_batch_operations - batch_operation_count)
                    source_stop = source_offset + count * source_stride
                    target_stop = target_offset + count * target_stride
                    ranges.append(
                        TransferBatchRange(
                            source_base_address=source.storage_address,
                            source_capacity=source.storage_nbytes,
                            target_base_address=target.storage_address,
                            target_capacity=target.storage_nbytes,
                            source_offsets=tuple(
                                range(source_offset, source_stop, source_stride)
                            ),
                            target_offsets=tuple(
                                range(target_offset, target_stop, target_stride)
                            ),
                            sizes=(operation.inner_bytes,) * count,
                        )
                    )
                    remaining -= count
                    source_offset = source_stop
                    target_offset = target_stop
                    batch_operation_count += count
                    if batch_operation_count == max_batch_operations:
                        yield TransferBatch.from_ranges(
                            endpoint=endpoint, ranges=tuple(ranges)
                        )
                        ranges = []
                        batch_operation_count = 0
                # Carry the prefix without materializing any dimension's pool.
                for dim in range(len(prefix_indices) - 1, -1, -1):
                    prefix_indices[dim] += 1
                    prefix_source_offset += operation.source_strides[dim]
                    prefix_target_offset += operation.target_strides[dim]
                    if prefix_indices[dim] < prefix_counts[dim]:
                        break
                    prefix_indices[dim] = 0
                    prefix_source_offset -= (
                        prefix_counts[dim] * operation.source_strides[dim]
                    )
                    prefix_target_offset -= (
                        prefix_counts[dim] * operation.target_strides[dim]
                    )
                else:
                    break
            continue
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
