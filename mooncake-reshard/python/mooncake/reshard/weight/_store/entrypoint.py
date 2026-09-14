"""Native Store entry point for manifest-backed model-weight snapshots."""

from __future__ import annotations

from .store import WeightStore
from .snapshot import (
    WeightSnapshotAdapter,
    WeightSnapshotDescriptor,
)
from .writer import (
    WeightStoreWriter,
)


def begin_weight_snapshot(
    store: object,
    snapshot: WeightSnapshotDescriptor,
    adapter: WeightSnapshotAdapter,
) -> WeightStoreWriter:
    """Open an explicit manifest-backed model-weight snapshot writer."""

    return WeightStore(store).begin_weight_snapshot(snapshot, adapter)


__all__ = ["begin_weight_snapshot"]
