"""Bounded dense chunk scatter into retained canonical runtime views."""

from __future__ import annotations

from ..contracts import RuntimeBindingFragment
from .tiled_coo import TiledCOODenseFragment


def dense_fragment_spans(fragment, binding):
    """Yield byte spans without constructing per-row runtime fragments."""
    if not isinstance(fragment, TiledCOODenseFragment):
        raise TypeError("fragment must be a TiledCOODenseFragment")
    if not isinstance(binding, RuntimeBindingFragment):
        raise TypeError("binding must be a RuntimeBindingFragment")
    values = (
        fragment.source_byte_offset,
        fragment.source_byte_count,
        fragment.row_count,
        fragment.source_row_elements,
        fragment.target_row_offset,
        fragment.target_column_offset,
        fragment.source_column_offset,
        fragment.column_count,
    )
    if any(type(value) is not int for value in values):
        raise ValueError("Dense geometry must contain integers")
    source, nbytes, rows, columns, dr, dc, sc, width = values
    item = binding.itemsize
    if len(binding.local_shape) != 2:
        raise ValueError("Canonical target views must be rank two")
    if min(source, dr, dc, sc) < 0 or min(rows, columns, width) <= 0:
        raise ValueError("Invalid dense geometry")
    if rows * columns * item != nbytes or sc + width > columns:
        raise ValueError("Source geometry exceeds its byte span")
    if dr + rows > binding.local_shape[0] or dc + width > binding.local_shape[1]:
        raise ValueError("Dense geometry exceeds target view")
    sr, scol = binding.strides_bytes
    last = binding.storage_offset_bytes + (dr + rows - 1) * sr + (dc + width - 1) * scol
    if last + item > binding.storage_nbytes:
        raise ValueError("Dense geometry exceeds target allocation")
    if scol == item and sr == width * item and sc == 0 and width == columns:
        yield source, binding.address + dr * sr + dc * item, rows * width * item
        return
    pending = None
    for row in range(rows):
        for column in range(1 if scol == item else width):
            size = width * item if scol == item else item
            s = source + (row * columns + sc + column) * item
            d = binding.address + (dr + row) * sr + (dc + column) * scol
            if (
                pending
                and pending[0] + pending[2] == s
                and pending[1] + pending[2] == d
            ):
                pending = pending[0], pending[1], pending[2] + size
            else:
                if pending:
                    yield pending
                pending = s, d, size
    if pending:
        yield pending


class RegisteredDenseTargets:
    """Retain owners and registrations across explicitly ended generations.

    The framework must fence producer writes and call end only after postload
    consumption is terminal. A poisoned generation remains poisoned after a
    transport drain; draining establishes memory safety, not model validity.
    """

    def __init__(self, transport):
        self.transport = transport
        self.owners = {}
        self.bindings = {}
        self.active = False
        self.poisoned = False
        self.metrics = {}

    def begin(self, bindings):
        import time

        self.transport._check_active()
        if self.active or self.poisoned:
            raise RuntimeError("Previous dense generation has not ended")
        bindings = tuple(bindings)
        snapshots, allocations = {}, {}
        for binding in bindings:
            if not isinstance(binding, RuntimeBindingFragment) or binding.owner is None:
                raise ValueError("Targets require owned runtime bindings")
            if len(binding.local_shape) != 2:
                raise ValueError("Canonical target views must be rank two")
            if binding.placement_fragment_id in snapshots:
                raise ValueError("Duplicate canonical target identity")
            # RuntimeBindingFragment is immutable and validates its snapshot at
            # construction. Check the strided physical extent separately.
            extent = sum(
                (n - 1) * s for n, s in zip(binding.local_shape, binding.strides_bytes)
            )
            if (
                extent + binding.itemsize
                > binding.storage_nbytes - binding.storage_offset_bytes
            ):
                raise ValueError("Target view exceeds its allocation")
            existing = allocations.get(binding.storage_address)
            allocation = binding.storage_nbytes, binding.owner
            if existing and (
                existing[0] != allocation[0] or existing[1] is not allocation[1]
            ):
                raise ValueError("Conflicting allocation owners")
            allocations[binding.storage_address] = allocation
            snapshots[binding.placement_fragment_id] = binding
        began = time.perf_counter()
        reused = 0
        try:
            for ptr, owner in tuple(self.owners.items()):
                current = allocations.get(ptr)
                if (
                    current is None
                    or current[1] is not owner
                    or self.transport.registered.get(ptr) != current[0]
                ):
                    self.transport.unregister_buffer(ptr)
                    del self.owners[ptr]
                else:
                    reused += 1
            new = 0
            for ptr, (capacity, owner) in allocations.items():
                if ptr not in self.owners:
                    self.transport.register_buffer(ptr, capacity, owner=owner)
                    self.owners[ptr] = owner
                    new += 1
        except BaseException:
            self.poisoned = True
            raise
        self.bindings = snapshots
        self.active = True
        self.metrics = dict(
            read_calls=0,
            transfer_segments=0,
            staging_allocated_bytes=0,
            registration_new_owner_count=new,
            registration_reused_owner_count=reused,
            registration_seconds=time.perf_counter() - began,
            registered_owner_bytes=sum(allocations[p][0] for p in self.owners),
            registered_owner_count=len(self.owners),
        )
        return dict(self.metrics)

    def read_dense(
        self,
        endpoint,
        source_address,
        capacity,
        fragments,
        itemsize,
        validate_target=None,
    ):
        from ..transfer_engine import TransferBatchRange

        self.transport._check_active()
        if not self.active or self.poisoned:
            raise RuntimeError("Dense targets are inactive or poisoned")
        if type(itemsize) is not int or itemsize <= 0:
            raise ValueError("Invalid dense itemsize")
        if type(endpoint) is not str or not endpoint:
            raise ValueError("Source endpoint must be non-empty")
        # Reuse the common address/capacity contract once, before expansion.
        TransferBatchRange(
            source_address, capacity, source_address, capacity, (0,), (0,), (1,)
        )
        fragments = tuple(fragments)
        used = {}
        total = segments = 0
        # Validate every span before the first transfer, without retaining the
        # expanded row list. A second bounded pass performs the same lowering.
        for fragment in fragments:
            if not isinstance(fragment, TiledCOODenseFragment):
                raise TypeError("Expected typed dense fragments")
            binding = self.bindings.get(fragment.target_fragment_id)
            if binding is None or binding.itemsize != itemsize:
                raise ValueError("Dense target identity or itemsize differs")
            if binding.placement_fragment_id not in used:
                if validate_target is not None:
                    validate_target(binding)
                if (
                    self.transport.owners.get(binding.storage_address)
                    is not binding.owner
                    or self.transport.registered.get(binding.storage_address)
                    != binding.storage_nbytes
                ):
                    raise ValueError("Target registration changed")
                used[binding.placement_fragment_id] = binding
            for offset, address, size in dense_fragment_spans(fragment, binding):
                if offset > capacity or size > capacity - offset:
                    raise ValueError("Dense source exceeds its allocation")
                total += size
                segments += 1
            if fragment.source_byte_offset > capacity - fragment.source_byte_count:
                raise ValueError("Dense source fragment exceeds its allocation")
        pending = []
        started = False
        try:
            for fragment in fragments:
                binding = used[fragment.target_fragment_id]
                for offset, address, size in dense_fragment_spans(fragment, binding):
                    pending.append((offset, address, size, binding.owner))
                    if len(pending) == 8192:
                        started = True
                        self.transport.read_into_targets(
                            endpoint, source_address, capacity, pending
                        )
                        pending = []
            if pending:
                started = True
                self.transport.read_into_targets(
                    endpoint, source_address, capacity, pending
                )
        except BaseException:
            if started:
                self.poisoned = True
            raise
        self.metrics["read_calls"] += 1
        self.metrics["transfer_segments"] += segments
        return None, total

    def end(self, *, terminal=True, retain_registration=False):
        if not self.active or not terminal or self.poisoned:
            self.poisoned = True
            raise RuntimeError("Dense generation completion is unresolved")
        self.transport._check_active()
        self.active = False
        metrics = dict(self.metrics)
        if not retain_registration:
            self.release()
        return metrics

    def release(self):
        if self.active or self.poisoned:
            raise RuntimeError("Dense generation completion is unresolved")
        try:
            for ptr in tuple(self.owners):
                self.transport.unregister_buffer(ptr)
                del self.owners[ptr]
        except BaseException:
            self.poisoned = True
            raise
        self.bindings.clear()
