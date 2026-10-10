"""Integration smoke test against Mooncake's current structured-object API."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys
import unittest

import numpy as np


_ROOT = Path(__file__).resolve().parents[1]
for _path in (
    _ROOT / "mooncake-reshard" / "python",
    _ROOT / "mooncake-wheel",
    _ROOT / "mooncake-rl",
):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from mooncake.structured_object_store import (  # noqa: E402
    MooncakeBundleTransfer,
    StructuredObjectReadSpec,
)
from sparse_object_store import (  # noqa: E402
    SparseObjectStore,
    object_ref_key,
)
from mooncake.reshard.weight import (  # noqa: E402
    OwnershipAxis,
    SplitAxis,
    TensorDescriptor,
)


class InMemoryBundleStore:
    """Low-level structured-object store double for this test module."""

    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}

    def put(self, key: str, value: object, config: object = None) -> int:
        del config
        self._objects[key] = bytes(value)
        return 0

    def get(self, key: str) -> bytes:
        return self._objects[key]

    def remove(self, key: str, force: bool = False) -> int:
        del force
        self._objects.pop(key, None)
        return 0

    def is_exist(self, key: str) -> int:
        return int(key in self._objects)


class TrackingBundleTransfer(MooncakeBundleTransfer):
    """Record structured read specs while retaining the production layer."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.materialized_specs: list[StructuredObjectReadSpec] = []

    def materialize(self, spec: StructuredObjectReadSpec):  # type: ignore[override]
        self.materialized_specs.append(spec)
        return super().materialize(spec)


@dataclass(frozen=True)
class _Source:
    tensor_id: str
    global_shape: tuple[int, ...]
    global_offset: tuple[int, ...]
    local_shape: tuple[int, ...]
    source_placement: tuple[tuple[str, int], ...]
    object_ref: object


@dataclass(frozen=True)
class _Target:
    tensor_id: str
    global_shape: tuple[int, ...]
    global_offset: tuple[int, ...]
    local_shape: tuple[int, ...]
    target_placement: tuple[tuple[str, int], ...]


class StructuredObjectIntegrationTest(unittest.TestCase):
    def test_real_structured_object_member_slices(self) -> None:
        low_level = InMemoryBundleStore()
        writer_transfer = MooncakeBundleTransfer(low_level, key_prefix="integration")
        writer = SparseObjectStore(writer_transfer)
        stored = writer.put_sparse_object(
            tensor_id="layer.weight",
            global_shape=(8, 12),
            global_offset=(0, 0),
            local_shape=(8, 12),
            source_placement=(("ep", 0),),
            indices=np.asarray([[0, 0], [4, 8], [7, 9]], dtype=np.uint32),
            values=np.asarray([0.1, 0.4, 0.7], dtype=np.float32),
            generation=(1, 2),
            tile_shape=(4, 4),
        )

        reader_transfer = TrackingBundleTransfer(low_level, key_prefix="integration")
        reader = SparseObjectStore(reader_transfer)
        source = _Source(
            "layer.weight", (8, 12), (0, 0), (8, 12), (("ep", 0),), stored.ref
        )
        target = _Target(
            "layer.weight", (8, 12), (0, 0), (4, 10), (("ep", 0), ("tp", 0))
        )
        tensor = TensorDescriptor(
            tensor_id="layer.weight",
            global_shape=(8, 12),
            dtype="float32",
            itemsize=4,
            shard_dims=(1,),
            layout_fingerprint="integration:layer.weight",
            parallel_axes=(OwnershipAxis("ep"), SplitAxis("tp", 1)),
        )
        plan = reader.plan_target(
            tensor_id="layer.weight",
            tensor=tensor,
            target=target,
            source_fragments=(source,),
            base_generation=1,
            delta_generation=2,
        )
        indices, values = reader.materialize_plan(plan)
        np.testing.assert_array_equal(indices, [[0, 0]])
        np.testing.assert_allclose(values, [0.1])

        source_key = object_ref_key(stored.ref)
        source_specs = [
            spec
            for spec in reader_transfer.materialized_specs
            if object_ref_key(spec.ref) == source_key
        ]
        self.assertEqual(
            [spec.member_names for spec in source_specs],
            [("tile_coords", "tile_ptr"), ("indices", "values")],
        )
        slices = dict(source_specs[-1].member_slices)
        self.assertEqual(slices["indices"], slices["values"])
        self.assertEqual(slices["indices"].start, 0)
        self.assertEqual(slices["indices"].end, 1)


if __name__ == "__main__":
    unittest.main()
