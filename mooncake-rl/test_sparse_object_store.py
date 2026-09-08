"""Dependency-free smoke tests for ``sparse_object_store.py``."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys
import threading
import time
import unittest

import numpy as np

# Keep the smoke test runnable both from this directory and from the Mooncake
# repository root; ``mooncake-rl`` is an integration directory, not a package.
_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "mooncake-reshard" / "python"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from sparse_object_store import (  # noqa: E402
    SparseGenerationFence,
    SparseGenerationMismatchError,
    SparseGenerationNotInitializedError,
    SparseObjectIndex,
    SparseObjectStorePlanner,
    SparseObjectStore,
    StaleSparseGenerationError,
    canonical_source_placement,
    is_canonical_source,
    object_ref_key,
)
from mooncake.reshard.weight import (  # noqa: E402
    OwnershipAxis,
    ReplicatedAxis,
    SplitAxis,
    TensorDescriptor,
)


@dataclass(frozen=True)
class _Source:
    tensor_id: str
    global_shape: tuple[int, ...]
    global_offset: tuple[int, ...]
    local_shape: tuple[int, ...]
    source_placement: tuple[tuple[str, int], ...]
    object_ref: str


@dataclass(frozen=True)
class _Target:
    global_offset: tuple[int, ...]
    local_shape: tuple[int, ...]
    target_placement: tuple[tuple[str, int], ...]
    physical_node: str | None = None


def _tensor_descriptor(
    tensor_id: str,
    shape: tuple[int, ...],
    *,
    owned_by_ep: bool = False,
) -> TensorDescriptor:
    axes = (
        (OwnershipAxis("ep"), SplitAxis("tp", 1))
        if owned_by_ep and len(shape) > 1
        else (ReplicatedAxis("ep"),)
    )
    return TensorDescriptor(
        tensor_id=tensor_id,
        global_shape=shape,
        dtype="float32",
        itemsize=4,
        shard_dims=(1,) if owned_by_ep and len(shape) > 1 else (),
        layout_fingerprint=f"test:{tensor_id}",
        parallel_axes=axes,
    )


@dataclass(frozen=True)
class _Ref:
    key: str


@dataclass(frozen=True)
class _RemoteRef:
    manifest_key: str


@dataclass(frozen=True)
class _Payload:
    metadata: dict
    buffers: dict


@dataclass(frozen=True)
class _Result:
    metadata: dict
    objects: dict


class _Spec:
    def __init__(self, backend, ref, names=None, slices=None):
        self.backend = backend
        self.ref = ref
        self.names = names
        self.slices = {} if slices is None else dict(slices)

    def select_members(self, names):
        return _Spec(self.backend, self.ref, tuple(names), self.slices)

    def slice_member(self, name, axis, start, end, step=1):
        assert axis == 0 and step == 1
        slices = dict(self.slices)
        slices[name] = (start, end)
        return _Spec(self.backend, self.ref, self.names, slices)


class _Backend:
    def __init__(self):
        self.objects = {}
        self.materialize_members = []

    def put_structured_object(self, payload, **kwargs):
        del kwargs
        ref = _Ref(f"obj-{len(self.objects)}")
        self.objects[ref.key] = payload
        return ref

    def read_spec(self, ref):
        return _Spec(self, ref)

    def materialize(self, spec):
        key = object_ref_key(spec.ref)
        payload = self.objects[key]
        names = list(payload.buffers) if spec.names is None else list(spec.names)
        self.materialize_members.append((key, tuple(names), dict(spec.slices)))
        objects = {}
        for name in names:
            value = payload.buffers[name]
            if name in spec.slices:
                start, end = spec.slices[name]
                value = value[start:end]
            objects[name] = value.copy()
        return _Result(dict(payload.metadata), objects)


class _BlockingBackend(_Backend):
    def __init__(self):
        super().__init__()
        self.first_coo_read_started = threading.Event()
        self.release_first_coo_read = threading.Event()
        self.coo_read_count = 0

    def materialize(self, spec):
        if spec.names == ("indices", "values"):
            self.coo_read_count += 1
            if self.coo_read_count == 1:
                self.first_coo_read_started.set()
                if not self.release_first_coo_read.wait(timeout=5):
                    raise TimeoutError("test did not release the first COO read")
        return super().materialize(spec)


class SparseObjectStorePlannerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.index = SparseObjectIndex(
            object_ref="source-0",
            tensor_id="layer.weight",
            global_shape=(8, 12),
            tile_shape=(4, 4),
            tile_coords=((0, 0), (1, 2)),
            tile_ptr=(0, 2, 3),
            nnz=3,
            base_generation=10,
            delta_generation=11,
        )
        self.source = _Source(
            "layer.weight", (8, 12), (0, 0), (8, 12), (("ep", 0),), "source-0"
        )

    def test_merges_adjacent_ranges_and_marks_boundary_filter(self) -> None:
        target = _Target((0, 0), (8, 10), (("ep", 0), ("tp", 0)))
        plan = SparseObjectStorePlanner().plan_target(
            tensor_id="layer.weight",
            tensor=_tensor_descriptor("layer.weight", (8, 12), owned_by_ep=True),
            target=target,
            source_fragments=(self.source,),
            source_indexes={"source-0": self.index},
            base_generation=10,
            delta_generation=11,
        )
        self.assertEqual(plan.source_ranges, (("source-0", (0, 3)),))
        self.assertTrue(plan.regions[0].exact_coordinate_filter)
        self.assertEqual(plan.apply, "scatter_add")
        self.assertEqual(plan.regions[0].apply, "scatter_add")

    def test_2d_coo_tile_lookup_skips_unmatched_columns_and_rows(self) -> None:
        # The 2-D fast path indexes COO tiles by row group and binary-searches
        # columns.  Only the first two tiles intersect this target; the tile
        # at tile column 20 and the tile in row group 3 must not enter the
        # plan.
        index = SparseObjectIndex(
            object_ref="source-fast",
            tensor_id="layer.fast",
            global_shape=(20, 100),
            tile_shape=(4, 4),
            tile_coords=((0, 0), (0, 2), (0, 20), (3, 1)),
            tile_ptr=(0, 1, 2, 3, 4),
            nnz=4,
            base_generation=1,
            delta_generation=2,
        )
        source = _Source("layer.fast", (20, 100), (0, 0), (20, 100), (), "source-fast")
        target = _Target((0, 0), (8, 12), ())
        plan = SparseObjectStorePlanner().plan_target(
            tensor_id="layer.fast",
            tensor=_tensor_descriptor("layer.fast", (20, 100)),
            target=target,
            source_fragments=(source,),
            source_indexes={"source-fast": index},
            base_generation=1,
            delta_generation=2,
        )
        self.assertEqual(plan.source_ranges, (("source-fast", (0, 2)),))

        # A column-oriented target is also answered from the compact index.
        # Every row group has several columns, but only tile column 5 is in
        # the target box.  The returned ranges are the ten one-entry tiles in
        # that column; no other column can contribute an entry.
        tile_coords = tuple((row, column) for row in range(10) for column in range(8))
        column_index = SparseObjectIndex(
            object_ref="source-column",
            tensor_id="layer.column",
            global_shape=(40, 32),
            tile_shape=(4, 4),
            tile_coords=tile_coords,
            tile_ptr=tuple(range(len(tile_coords) + 1)),
            nnz=len(tile_coords),
            base_generation=1,
            delta_generation=2,
        )
        column_source = _Source(
            "layer.column", (40, 32), (0, 0), (40, 32), (), "source-column"
        )
        column_target = _Target((0, 20), (40, 4), ())
        column_plan = SparseObjectStorePlanner().plan_target(
            tensor_id="layer.column",
            tensor=_tensor_descriptor("layer.column", (40, 32)),
            target=column_target,
            source_fragments=(column_source,),
            source_indexes={"source-column": column_index},
            base_generation=1,
            delta_generation=2,
        )
        self.assertEqual(
            column_plan.source_ranges,
            (
                ("source-column", (5, 6)),
                ("source-column", (13, 14)),
                ("source-column", (21, 22)),
                ("source-column", (29, 30)),
                ("source-column", (37, 38)),
                ("source-column", (45, 46)),
                ("source-column", (53, 54)),
                ("source-column", (61, 62)),
                ("source-column", (69, 70)),
                ("source-column", (77, 78)),
            ),
        )

    def test_supports_n_dimensional_coordinate(self) -> None:
        index = SparseObjectIndex(
            "source-n-dim",
            "tensor.n_dim",
            (4, 4, 4),
            (2, 2, 2),
            ((0, 0, 0),),
            (0, 1),
            1,
            1,
            2,
        )
        source = _Source(
            "tensor.n_dim", (4, 4, 4), (0, 0, 0), (4, 4, 4), (), "source-n-dim"
        )
        target = _Target((1, 1, 1), (2, 2, 2), (("stage", 0),))
        plan = SparseObjectStorePlanner().plan_target(
            tensor_id="tensor.n_dim",
            tensor=_tensor_descriptor("tensor.n_dim", (4, 4, 4)),
            target=target,
            source_fragments=(source,),
            source_indexes={"source-n-dim": index},
            base_generation=1,
            delta_generation=2,
        )
        self.assertEqual(plan.range_request_count, 1)

    def test_named_owner_mapping_is_not_ep_tp_specific(self) -> None:
        tensor = TensorDescriptor(
            tensor_id="tensor.n_dim",
            global_shape=(4, 4),
            dtype="float32",
            itemsize=4,
            shard_dims=(),
            layout_fingerprint="test:owner",
            parallel_axes=(OwnershipAxis("pp"),),
        )
        from mooncake.reshard.sparse import placement_matches

        self.assertTrue(placement_matches(tensor, (("pp", 0),), (("pp", 0),)))
        self.assertFalse(placement_matches(tensor, (("pp", 0),), (("pp", 1),)))
        self.assertFalse(placement_matches(tensor, (), ()))

    def test_source_owner_admission_is_deterministic_for_replicas(self) -> None:
        candidates = ((("ep", 1),), (("ep", 0),))
        self.assertEqual(canonical_source_placement(candidates), (("ep", 0),))
        self.assertTrue(is_canonical_source((("ep", 0),), candidates))
        self.assertFalse(is_canonical_source((("ep", 1),), candidates))

    def test_object_ref_key_accepts_mooncake_remote_bundle_ref(self) -> None:
        self.assertEqual(object_ref_key(_RemoteRef("manifest-0")), "manifest-0")
        self.assertEqual(object_ref_key({"manifest_key": "manifest-1"}), "manifest-1")
        self.assertEqual(
            object_ref_key({"key": None, "manifest_key": "manifest-2"}), "manifest-2"
        )

    def test_store_facade_reads_index_then_aligned_coo_ranges(self) -> None:
        backend = _Backend()
        store = SparseObjectStore(backend, payload_type=_Payload)
        stored = store.put_sparse_object(
            tensor_id="layer.weight",
            global_shape=(8, 12),
            indices=np.asarray([[7, 9], [0, 0], [4, 8]], dtype=np.uint32),
            values=np.asarray([0.7, 0.1, 0.4], dtype=np.float32),
            generation=(10, 11),
            tile_shape=(4, 4),
        )
        # PUT owns sorting/index construction; source reads only compact index
        # members, then only the requested aligned COO range.
        reader = SparseObjectStore(backend, payload_type=_Payload)
        index, metadata = reader.read_index_with_metadata(stored.ref)
        self.assertEqual(metadata["schema"], "mooncake.sparse_object")
        self.assertEqual(metadata["coordinate_rank"], 2)
        self.assertEqual(metadata["nnz"], 3)
        self.assertIs(reader.read_index(stored.ref), index)
        self.assertEqual(index.tile_coords, ((0, 0), (1, 2)))
        indices, values = reader.read_coo_range(stored.ref, (0, 2))
        self.assertEqual(indices.tolist(), [[0, 0], [4, 8]])
        np.testing.assert_allclose(values, [0.1, 0.4])
        self.assertEqual(
            backend.materialize_members,
            [
                ("obj-0", ("tile_coords", "tile_ptr"), {}),
                ("obj-0", ("indices", "values"), {"indices": (0, 2), "values": (0, 2)}),
            ],
        )
        source = _Source(
            "layer.weight", (8, 12), (0, 0), (8, 12), (("ep", 0),), stored.ref
        )
        target = _Target((0, 0), (8, 10), (("ep", 0), ("tp", 0)))
        plan = reader.plan_target(
            tensor_id="layer.weight",
            tensor=_tensor_descriptor("layer.weight", (8, 12), owned_by_ep=True),
            target=target,
            source_fragments=(source,),
            base_generation=10,
            delta_generation=11,
        )
        chunks = reader.read_plan(plan)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0][0].source_object_ref, "obj-0")
        materialized_indices, materialized_values = reader.materialize_plan(plan)
        self.assertEqual(materialized_indices.tolist(), [[0, 0], [4, 8], [7, 9]])
        np.testing.assert_allclose(materialized_values, [0.1, 0.4, 0.7])

    def test_empty_coo_source_is_a_valid_noop_update(self) -> None:
        backend = _Backend()
        writer = SparseObjectStore(backend, payload_type=_Payload)
        stored = writer.put_sparse_object(
            tensor_id="layer.weight",
            global_shape=(4, 4),
            indices=np.empty((0, 2), dtype=np.uint32),
            values=np.empty((0,), dtype=np.float32),
            generation=(1, 2),
        )
        reader = SparseObjectStore(backend, payload_type=_Payload)
        source = _Source("layer.weight", (4, 4), (0, 0), (4, 4), (), stored.ref)
        target = _Target((0, 0), (4, 4), ())
        plan = reader.plan_target(
            tensor_id="layer.weight",
            tensor=_tensor_descriptor("layer.weight", (4, 4)),
            target=target,
            source_fragments=(source,),
            base_generation=1,
            delta_generation=2,
        )
        self.assertEqual(plan.regions, ())
        indices, values = reader.materialize_plan(plan)
        self.assertEqual(indices.shape, (0, 2))
        self.assertEqual(values.shape, (0,))

    def test_store_facade_can_commit_non_source_structured_object(self) -> None:
        backend = _Backend()
        store = SparseObjectStore(backend, payload_type=_Payload)
        ref = store.put_structured_object(
            metadata={"coordinate_space": "target_local"},
            buffers={"indices": np.asarray([[0, 1]], dtype=np.uint32)},
        )
        self.assertEqual(ref.key, "obj-0")
        result = backend.materialize(backend.read_spec(ref))
        self.assertEqual(result.metadata["coordinate_space"], "target_local")
        self.assertEqual(result.objects["indices"].tolist(), [[0, 1]])

    def test_store_plan_filters_inventory_before_index_reads(self) -> None:
        backend = _Backend()
        writer = SparseObjectStore(backend, payload_type=_Payload)
        selected = writer.put_sparse_object(
            tensor_id="selected",
            global_shape=(4, 4),
            indices=np.asarray([[0, 0]], dtype=np.uint32),
            values=np.asarray([1.0], dtype=np.float32),
            generation=(1, 2),
        )
        unrelated = writer.put_sparse_object(
            tensor_id="unrelated",
            global_shape=(4, 4),
            indices=np.asarray([[0, 0]], dtype=np.uint32),
            values=np.asarray([2.0], dtype=np.float32),
            generation=(1, 2),
        )
        reader = SparseObjectStore(backend, payload_type=_Payload)
        source = _Source("selected", (4, 4), (0, 0), (4, 4), (), selected.ref)
        unrelated_source = _Source(
            "unrelated", (4, 4), (0, 0), (4, 4), (), unrelated.ref
        )
        target = _Target((0, 0), (4, 4), ())
        plan = reader.plan_target(
            tensor_id="selected",
            tensor=_tensor_descriptor("selected", (4, 4)),
            target=target,
            source_fragments=(source, unrelated_source),
            base_generation=1,
            delta_generation=2,
        )
        self.assertEqual(plan.source_ranges, ((object_ref_key(selected.ref), (0, 1)),))
        self.assertEqual(
            [key for key, names, _slices in backend.materialize_members],
            [object_ref_key(selected.ref)],
        )

    def test_store_plan_keeps_multiple_source_shards_and_selects_replicas(self) -> None:
        backend = _Backend()
        writer = SparseObjectStore(backend, payload_type=_Payload)
        left = writer.put_sparse_object(
            tensor_id="layer.weight",
            global_shape=(4, 8),
            global_offset=(0, 0),
            local_shape=(4, 4),
            indices=np.asarray([[0, 0]], dtype=np.uint32),
            values=np.asarray([1.0], dtype=np.float32),
            generation=(1, 2),
        )
        right = writer.put_sparse_object(
            tensor_id="layer.weight",
            global_shape=(4, 8),
            global_offset=(0, 4),
            local_shape=(4, 4),
            indices=np.asarray([[0, 4]], dtype=np.uint32),
            values=np.asarray([2.0], dtype=np.float32),
            generation=(1, 2),
        )
        left_replica = writer.put_sparse_object(
            tensor_id="layer.weight",
            global_shape=(4, 8),
            global_offset=(0, 0),
            local_shape=(4, 4),
            indices=np.asarray([[0, 0]], dtype=np.uint32),
            values=np.asarray([1.0], dtype=np.float32),
            generation=(1, 2),
        )
        reader = SparseObjectStore(backend, payload_type=_Payload)
        target = _Target((0, 0), (4, 8), ())
        tensor = _tensor_descriptor("layer.weight", (4, 8))
        source_left = _Source("layer.weight", (4, 8), (0, 0), (4, 4), (), left.ref)
        source_right = _Source("layer.weight", (4, 8), (0, 4), (4, 4), (), right.ref)
        source_left_replica = _Source(
            "layer.weight", (4, 8), (0, 0), (4, 4), (), left_replica.ref
        )
        selected = reader.planner.select_source_fragments(
            tensor_id="layer.weight",
            target=target,
            source_fragments=(source_left_replica, source_right, source_left),
            tensor=tensor,
        )
        self.assertEqual(
            {(item.global_offset, item.local_shape) for item in selected},
            {((0, 0), (4, 4)), ((0, 4), (4, 4))},
        )
        plan = reader.planner.plan_target(
            tensor_id="layer.weight",
            tensor=tensor,
            target=target,
            source_fragments=(source_left_replica, source_right, source_left),
            source_indexes={
                object_ref_key(left.ref): left.index,
                object_ref_key(left_replica.ref): left_replica.index,
                object_ref_key(right.ref): right.index,
            },
            base_generation=1,
            delta_generation=2,
        )
        self.assertEqual(
            {region.source_object_ref for region in plan.regions},
            {object_ref_key(left.ref), object_ref_key(right.ref)},
        )

    def test_source_index_retains_geometry_and_rejects_inventory_mismatch(self) -> None:
        backend = _Backend()
        writer = SparseObjectStore(backend, payload_type=_Payload)
        stored = writer.put_sparse_object(
            tensor_id="layer.weight",
            global_shape=(8, 8),
            global_offset=(4, 0),
            local_shape=(4, 8),
            indices=np.asarray([[4, 0]], dtype=np.uint32),
            values=np.asarray([1.0], dtype=np.float32),
            generation=(1, 2),
        )
        reader = SparseObjectStore(backend, payload_type=_Payload)
        with self.assertRaisesRegex(ValueError, "source geometry"):
            reader.plan_target(
                tensor_id="layer.weight",
                tensor=_tensor_descriptor("layer.weight", (8, 8)),
                target=_Target((0, 0), (8, 8), ()),
                source_fragments=(
                    _Source("layer.weight", (8, 8), (0, 0), (8, 8), (), stored.ref),
                ),
                base_generation=1,
                delta_generation=2,
            )

    def test_uint16_index_uses_wide_tile_arithmetic(self) -> None:
        backend = _Backend()
        store = SparseObjectStore(backend, payload_type=_Payload)
        stored = store.put_sparse_object(
            tensor_id="large.tile",
            global_shape=(70001, 256),
            indices=np.asarray([[65535, 0]], dtype=np.uint16),
            values=np.asarray([1.0], dtype=np.float32),
            generation=(1, 2),
            tile_shape=(70000, 256),
            index_dtype=np.uint16,
        )
        payload = backend.objects[object_ref_key(stored.ref)]
        self.assertEqual(payload.buffers["tile_coords"].tolist(), [[0, 0]])

    def test_generation_must_be_initialized_before_first_apply(self) -> None:
        backend = _Backend()
        store = SparseObjectStore(backend, payload_type=_Payload)
        target = store.put_target_sparse_object(
            tensor_id="layer.weight",
            global_shape=(2, 2),
            global_offset=(0, 0),
            local_shape=(2, 2),
            indices=np.asarray([[0, 0]], dtype=np.uint32),
            values=np.asarray([1.0], dtype=np.float32),
            generation=(10, 11),
        )
        with self.assertRaises(SparseGenerationNotInitializedError):
            store.apply_target_sparse_object(
                np.zeros((2, 2), dtype=np.float32),
                target.ref,
                generation_fence=SparseGenerationFence(),
                target_key="uninitialized",
            )

    def test_node_local_range_cache_deduplicates_raw_gets_by_generation(self) -> None:
        backend = _Backend()
        writer = SparseObjectStore(backend, payload_type=_Payload)
        stored = writer.put_sparse_object(
            tensor_id="layer.weight",
            global_shape=(4, 4),
            indices=np.asarray([[0, 0], [3, 3]], dtype=np.uint32),
            values=np.asarray([1.0, 2.0], dtype=np.float32),
            generation=(1, 2),
            tile_shape=(4, 4),
        )
        reader = SparseObjectStore(backend, payload_type=_Payload)
        source = _Source("layer.weight", (4, 4), (0, 0), (4, 4), (), stored.ref)
        tensor = _tensor_descriptor("layer.weight", (4, 4))
        targets = (
            _Target((0, 0), (4, 4), (("ep", 0),), physical_node="node-a"),
            _Target((0, 0), (4, 4), (("ep", 1),), physical_node="node-a"),
            _Target((0, 0), (4, 4), (("ep", 2),), physical_node="node-b"),
        )
        for target in targets:
            plan = reader.plan_target(
                tensor_id="layer.weight",
                tensor=tensor,
                target=target,
                source_fragments=(source,),
                base_generation=1,
                delta_generation=2,
            )
            reader.materialize_plan(plan)
        coo_reads = [
            event
            for event in backend.materialize_members
            if event[1] == ("indices", "values")
        ]
        self.assertEqual(len(coo_reads), 2)
        reader.clear_range_cache(physical_node="node-a")
        reader.materialize_plan(
            reader.plan_target(
                tensor_id="layer.weight",
                tensor=tensor,
                target=targets[0],
                source_fragments=(source,),
                base_generation=1,
                delta_generation=2,
            )
        )
        self.assertEqual(
            len(
                [
                    event
                    for event in backend.materialize_members
                    if event[1] == ("indices", "values")
                ]
            ),
            3,
        )

    def test_node_local_range_cache_coalesces_concurrent_identical_reads(self) -> None:
        backend = _BlockingBackend()
        writer = SparseObjectStore(backend, payload_type=_Payload)
        stored = writer.put_sparse_object(
            tensor_id="layer.weight",
            global_shape=(4, 4),
            indices=np.asarray([[0, 0]], dtype=np.uint32),
            values=np.asarray([1.0], dtype=np.float32),
            generation=(1, 2),
            tile_shape=(4, 4),
        )
        reader = SparseObjectStore(backend, payload_type=_Payload)
        results = []
        errors = []

        def read_range():
            try:
                results.append(
                    reader.read_coo_range_cached(
                        stored.ref,
                        (0, 1),
                        base_generation=1,
                        delta_generation=2,
                        physical_node="node-a",
                    )
                )
            except Exception as error:  # pragma: no cover - assertion below
                errors.append(error)

        first = threading.Thread(target=read_range)
        second = threading.Thread(target=read_range)
        first.start()
        self.assertTrue(backend.first_coo_read_started.wait(timeout=2))
        second.start()
        time.sleep(0.05)
        self.assertEqual(backend.coo_read_count, 1)
        backend.release_first_coo_read.set()
        first.join(timeout=2)
        second.join(timeout=2)
        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(backend.coo_read_count, 1)

    def test_node_local_range_cache_returns_immutable_payload(self) -> None:
        backend = _Backend()
        writer = SparseObjectStore(backend, payload_type=_Payload)
        stored = writer.put_sparse_object(
            tensor_id="layer.weight",
            global_shape=(4, 4),
            indices=np.asarray([[0, 0]], dtype=np.uint32),
            values=np.asarray([1.0], dtype=np.float32),
            generation=(1, 2),
        )
        reader = SparseObjectStore(backend, payload_type=_Payload)
        first = reader.read_coo_range_cached(
            stored.ref,
            (0, 1),
            base_generation=1,
            delta_generation=2,
            physical_node="node-a",
        )
        self.assertFalse(first[0].flags.writeable)
        self.assertFalse(first[1].flags.writeable)
        with self.assertRaises(ValueError):
            first[1][0] = 2.0
        second = reader.read_coo_range_cached(
            stored.ref,
            (0, 1),
            base_generation=1,
            delta_generation=2,
            physical_node="node-a",
        )
        np.testing.assert_allclose(second[1], [1.0])

    def test_clear_cache_releases_index_and_range_entries(self) -> None:
        backend = _Backend()
        writer = SparseObjectStore(backend, payload_type=_Payload)
        stored = writer.put_sparse_object(
            tensor_id="layer.weight",
            global_shape=(4, 4),
            indices=np.asarray([[0, 0]], dtype=np.uint32),
            values=np.asarray([1.0], dtype=np.float32),
            generation=(1, 2),
        )
        reader = SparseObjectStore(backend, payload_type=_Payload)
        reader.read_index(stored.ref)
        reader.read_coo_range_cached(
            stored.ref,
            (0, 1),
            base_generation=1,
            delta_generation=2,
            physical_node="node-a",
        )
        self.assertEqual(len(reader._index_cache), 1)
        self.assertEqual(len(reader._range_cache), 1)

        reader.clear_cache(object_ref=stored.ref)

        self.assertEqual(reader._index_cache, {})
        self.assertEqual(reader._metadata_cache, {})
        self.assertEqual(reader._range_cache, {})

    def test_materialize_target_rejects_tensor_dtype_mismatch(self) -> None:
        backend = _Backend()
        writer = SparseObjectStore(backend, payload_type=_Payload)
        stored = writer.put_sparse_object(
            tensor_id="layer.weight",
            global_shape=(2, 2),
            indices=np.asarray([[0, 1]], dtype=np.uint32),
            values=np.asarray([1.0], dtype=np.float64),
            generation=(1, 2),
        )
        reader = SparseObjectStore(backend, payload_type=_Payload)
        with self.assertRaisesRegex(ValueError, "TensorDescriptor dtype"):
            reader.materialize_target(
                tensor_id="layer.weight",
                tensor=_tensor_descriptor("layer.weight", (2, 2)),
                target=_Target((0, 0), (2, 2), ()),
                source_fragments=(
                    _Source("layer.weight", (2, 2), (0, 0), (2, 2), (), stored.ref),
                ),
                base_generation=1,
                delta_generation=2,
            )

    def test_target_apply_uses_batched_additive_update(self) -> None:
        backend = _Backend()
        store = SparseObjectStore(backend, payload_type=_Payload)
        target = store.put_target_sparse_object(
            tensor_id="layer.weight",
            global_shape=(2, 3),
            global_offset=(0, 0),
            local_shape=(2, 3),
            indices=np.asarray([[0, 1], [0, 1], [1, 2]], dtype=np.uint32),
            values=np.asarray([1.0, 2.0, 4.0], dtype=np.float32),
            generation=(1, 2),
        )
        base = np.zeros((2, 3), dtype=np.float32)
        np.testing.assert_allclose(
            store.apply_target_sparse_object(base, target.ref, inplace=False),
            [[0.0, 3.0, 0.0], [0.0, 0.0, 4.0]],
        )

        try:
            import torch
        except ImportError:
            return
        torch_base = torch.zeros((2, 3), dtype=torch.float32)
        torch_result = store.apply_target_sparse_object(
            torch_base, target.ref, inplace=False
        )
        self.assertTrue(
            torch.equal(torch_result, torch.tensor([[0.0, 3.0, 0.0], [0.0, 0.0, 4.0]]))
        )

    def test_target_apply_rejects_base_dtype_mismatch(self) -> None:
        backend = _Backend()
        store = SparseObjectStore(backend, payload_type=_Payload)
        target = store.put_target_sparse_object(
            tensor_id="layer.weight",
            global_shape=(2, 2),
            global_offset=(0, 0),
            local_shape=(2, 2),
            indices=np.asarray([[0, 1]], dtype=np.uint32),
            values=np.asarray([2.0], dtype=np.float32),
            generation=(1, 2),
        )
        with self.assertRaisesRegex(ValueError, "value dtype does not match base"):
            store.apply_target_sparse_object(
                np.zeros((2, 2), dtype=np.float64), target.ref, inplace=False
            )

    def test_equal_generation_retry_without_update_key_is_rejected(self) -> None:
        backend = _Backend()
        store = SparseObjectStore(backend, payload_type=_Payload)
        target = store.put_target_sparse_object(
            tensor_id="layer.weight",
            global_shape=(2, 2),
            global_offset=(0, 0),
            local_shape=(2, 2),
            indices=np.asarray([[0, 1]], dtype=np.uint32),
            values=np.asarray([2.0], dtype=np.float32),
            generation=(1, 2),
        )
        fence = SparseGenerationFence()
        fence.set_current("worker-0", 1)
        base = np.zeros((2, 2), dtype=np.float32)
        store.apply_target_sparse_object(
            base, target.ref, generation_fence=fence, target_key="worker-0"
        )
        with self.assertRaises(SparseGenerationMismatchError):
            store.apply_target_sparse_object(
                base, target.ref, generation_fence=fence, target_key="worker-0"
            )

    def test_target_apply_is_generation_fenced_and_identity_checked(self) -> None:
        backend = _Backend()
        store = SparseObjectStore(backend, payload_type=_Payload)
        target = store.put_target_sparse_object(
            tensor_id="layer.weight",
            global_shape=(2, 2),
            global_offset=(0, 0),
            local_shape=(2, 2),
            indices=np.asarray([[0, 1]], dtype=np.uint32),
            values=np.asarray([2.0], dtype=np.float32),
            generation=(10, 11),
            target_placement=(("ep", 0), ("tp", 0)),
            logical_update_key="layer.weight:10:11",
            target_key="worker-0:layer.weight",
        )
        fence = SparseGenerationFence()
        fence.set_current("worker-0:layer.weight", 10)
        base = np.zeros((2, 2), dtype=np.float32)
        store.apply_target_sparse_object(
            base,
            target.ref,
            generation_fence=fence,
            target_key="worker-0:layer.weight",
            expected_tensor_id="layer.weight",
            expected_target_placement=(("ep", 0), ("tp", 0)),
            expected_generation=(10, 11),
            expected_update_key="layer.weight:10:11",
            expected_target_key="worker-0:layer.weight",
        )
        np.testing.assert_allclose(base, [[0.0, 2.0], [0.0, 0.0]])
        different_key = store.put_target_sparse_object(
            tensor_id="layer.weight",
            global_shape=(2, 2),
            global_offset=(0, 0),
            local_shape=(2, 2),
            indices=np.asarray([[1, 0]], dtype=np.uint32),
            values=np.asarray([4.0], dtype=np.float32),
            generation=(10, 11),
            logical_update_key="layer.weight:other:10:11",
        )
        with self.assertRaises(SparseGenerationMismatchError):
            store.apply_target_sparse_object(
                base,
                different_key.ref,
                generation_fence=fence,
                target_key="worker-0:layer.weight",
            )
        # Equal-generation retry is idempotent.
        store.apply_target_sparse_object(
            base,
            target.ref,
            generation_fence=fence,
            target_key="worker-0:layer.weight",
        )
        np.testing.assert_allclose(base, [[0.0, 2.0], [0.0, 0.0]])

        stale = store.put_target_sparse_object(
            tensor_id="layer.weight",
            global_shape=(2, 2),
            global_offset=(0, 0),
            local_shape=(2, 2),
            indices=np.asarray([[1, 1]], dtype=np.uint32),
            values=np.asarray([3.0], dtype=np.float32),
            generation=(9, 10),
        )
        with self.assertRaises(StaleSparseGenerationError):
            store.apply_target_sparse_object(
                base,
                stale.ref,
                generation_fence=fence,
                target_key="worker-0:layer.weight",
            )
        skipped = store.put_target_sparse_object(
            tensor_id="layer.weight",
            global_shape=(2, 2),
            global_offset=(0, 0),
            local_shape=(2, 2),
            indices=np.asarray([[1, 1]], dtype=np.uint32),
            values=np.asarray([3.0], dtype=np.float32),
            generation=(12, 13),
        )
        with self.assertRaises(SparseGenerationMismatchError):
            store.apply_target_sparse_object(
                base,
                skipped.ref,
                generation_fence=fence,
                target_key="worker-0:layer.weight",
            )
        np.testing.assert_allclose(base, [[0.0, 2.0], [0.0, 0.0]])


if __name__ == "__main__":
    unittest.main()
