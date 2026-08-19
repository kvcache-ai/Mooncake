from __future__ import annotations

import json
import unittest

from mooncake.reshard.weight import (
    ParallelRank,
    PlacementFragment,
    RuntimeBindingFragment,
    WeightRuntimeBindingManifest,
    TensorDescriptor,
    OwnershipAxis,
    SplitAxis,
    validate_runtime_binding,
    weight_placement_from_json,
    weight_placement_to_json,
)
from model_weight_planner.helpers import (
    global_placement_from_fragments,
)

from .helpers import (
    MODEL_ID,
    REVISION,
    descriptor,
    split_target_binding,
    split_target_placement,
    target_binding,
    target_placement,
)


def _contains_json_key(value, key: str) -> bool:
    if isinstance(value, dict):
        return key in value or any(
            _contains_json_key(item, key) for item in value.values()
        )
    if isinstance(value, list):
        return any(_contains_json_key(item, key) for item in value)
    return False


class TargetPlacementManifestTest(unittest.TestCase):
    def test_json_round_trip_is_stable_and_contains_no_runtime_location(self) -> None:
        placement = target_placement()

        encoded = weight_placement_to_json(placement)
        payload = json.loads(encoded)

        self.assertEqual(weight_placement_from_json(encoded), placement)
        self.assertEqual(
            weight_placement_from_json(encoded).digest,
            placement.digest,
        )
        self.assertNotIn("address", encoded)
        self.assertNotIn("endpoint", encoded)
        self.assertNotIn("worker_id", encoded)
        self.assertNotIn("instance_id", encoded)
        self.assertNotIn('"generation":', encoded)
        self.assertNotIn("lease", encoded)
        self.assertFalse(_contains_json_key(payload, "owner"))
        self.assertNotIn("fragment_leases", encoded)
        self.assertEqual(payload["placement_id"], placement.placement_id)

    def test_json_round_trip_accepts_canonical_split_descriptor(self) -> None:
        tensor = TensorDescriptor(
            tensor_id="layers.0.single_axis.weight",
            global_shape=(8,),
            dtype="uint8",
            itemsize=1,
            shard_dims=(0,),
            layout_fingerprint="test:partition-dim",
            parallel_axes=(SplitAxis(kind="tp", dim=0),),
        )
        fragments = tuple(
            PlacementFragment(
                placement_fragment_id=f"single-axis-part-{tp}",
                tensor_id=tensor.tensor_id,
                global_offset=(tp * 4,),
                local_shape=(4,),
                nbytes=4,
                rank=ParallelRank(tp=tp),
            )
            for tp in range(2)
        )
        placement = global_placement_from_fragments(
            resource_id=MODEL_ID,
            revision=REVISION,
            placement_set_id="single-axis",
            tensors=(tensor,),
            fragments=fragments,
        )

        self.assertEqual(
            weight_placement_from_json(weight_placement_to_json(placement)),
            placement,
        )

    def test_complete_placement_accepts_canonical_split_descriptor(self) -> None:
        participants = tuple(
            (f"runtime-tp{tp}", ParallelRank(tp=tp)) for tp in range(2)
        )
        tensor = TensorDescriptor(
            tensor_id="layers.0.single_axis.weight",
            global_shape=(8,),
            dtype="uint8",
            itemsize=1,
            shard_dims=(0,),
            layout_fingerprint="test:canonical-split",
            parallel_axes=(SplitAxis(kind="tp", dim=0),),
        )
        fragments = tuple(
            PlacementFragment(
                placement_fragment_id=f"single-axis-runtime-{rank.tp}",
                tensor_id=tensor.tensor_id,
                global_offset=(rank.tp * 4,),
                local_shape=(4,),
                nbytes=4,
                rank=rank,
            )
            for _, rank in participants
        )
        placement = global_placement_from_fragments(
            resource_id=MODEL_ID,
            revision=REVISION,
            placement_set_id="runtime-single-axis",
            tensors=(tensor,),
            fragments=fragments,
            participant_ids={
                rank: participant_id for participant_id, rank in participants
            },
        )

        self.assertEqual(placement.tensors[0].shard_dims, (0,))
        self.assertEqual(
            placement.tensors[0].parallel_axes,
            (SplitAxis(kind="tp", dim=0),),
        )

    def test_binding_validates_without_combining_logical_and_physical_state(
        self,
    ) -> None:
        placement = target_placement()
        binding = target_binding()

        validate_runtime_binding(placement, binding)

        self.assertEqual(binding.resource_id, MODEL_ID)
        self.assertEqual(binding.revision, REVISION)
        self.assertEqual(binding.instance_id, "target-instance")
        self.assertEqual(binding.lease_id, "target-lease")
        self.assertEqual(binding.placement_id, placement.placement_id)
        self.assertEqual(binding.fragments[0].address, 0x9000)
        self.assertEqual(binding.generation, 7)
        self.assertEqual(
            binding.fragments[0].placement_fragment_id,
            "placement-fragment",
        )
        self.assertEqual(placement.fragments[0].rank, ParallelRank(tp=1))
        self.assertEqual(placement.fragments[0].global_offset, (4,))
        self.assertEqual(placement.fragments[0].aliases, ())
        self.assertEqual(len(binding.fragments), 1)

    def test_binding_rejects_overlapping_independent_runtime_ranges(self) -> None:
        placement = split_target_placement()

        for right_address in (0x9000, 0x9002):
            with self.subTest(right_address=right_address):
                with self.assertRaisesRegex(
                    ValueError, "runtime (binding|manifest) address ranges overlap"
                ):
                    validate_runtime_binding(
                        placement,
                        split_target_binding(right_address=right_address),
                    )

    def test_binding_allows_exact_runtime_alias_ranges_declared_by_placement(
        self,
    ) -> None:
        aliases = ("lm_head.weight", "model.embed_tokens.weight")
        tensors = (
            TensorDescriptor(
                tensor_id=tensor_id,
                global_shape=(4,),
                dtype="uint8",
                itemsize=1,
                shard_dims=(),
                layout_fingerprint="test:contiguous:v1",
                parallel_axes=(),
            )
            for tensor_id in aliases
        )
        placement = global_placement_from_fragments(
            resource_id=MODEL_ID,
            revision=REVISION,
            placement_set_id="target-alias",
            tensors=tuple(tensors),
            fragments=tuple(
                PlacementFragment(
                    placement_fragment_id=f"placement-{tensor_id}",
                    tensor_id=tensor_id,
                    global_offset=(0,),
                    local_shape=(4,),
                    nbytes=4,
                    rank=ParallelRank(),
                    aliases=aliases,
                )
                for tensor_id in aliases
            ),
            participant_ids={ParallelRank(): "target-alias"},
        )
        binding = WeightRuntimeBindingManifest(
            resource_id=MODEL_ID,
            revision=REVISION,
            placement_id=placement.placement_id,
            placement_digest=placement.digest,
            instance_id="target-instance",
            participant_id="target-alias",
            generation=7,
            lease_id="target-lease",
            fragments=tuple(
                RuntimeBindingFragment(
                    placement_fragment_id=fragment.placement_fragment_id,
                    fragment_id=f"runtime-{fragment.tensor_id}",
                    address=0x9000,
                    nbytes=4,
                    worker_id="target-worker",
                    endpoint="target-endpoint",
                    device="cuda:0",
                    itemsize=1,
                    local_shape=(4,),
                    strides_bytes=(1,),
                    storage_address=0x9000,
                    storage_nbytes=4,
                    storage_offset_bytes=0,
                )
                for fragment in placement.fragments
            ),
        )

        validate_runtime_binding(placement, binding)

        self.assertEqual(
            tuple(fragment.address for fragment in binding.fragments),
            (0x9000, 0x9000),
        )
        self.assertTrue(
            all(fragment.aliases == aliases for fragment in placement.fragments)
        )

    def test_binding_rejects_partial_runtime_alias_overlap(self) -> None:
        aliases = ("alias.a", "alias.b")
        placement = global_placement_from_fragments(
            resource_id=MODEL_ID,
            revision=REVISION,
            placement_set_id="target-partial-alias",
            tensors=tuple(
                TensorDescriptor(
                    tensor_id=tensor_id,
                    global_shape=(4,),
                    dtype="uint8",
                    itemsize=1,
                    shard_dims=(),
                    layout_fingerprint="test:contiguous:v1",
                    parallel_axes=(),
                )
                for tensor_id in aliases
            ),
            fragments=tuple(
                PlacementFragment(
                    placement_fragment_id=f"placement-{tensor_id}",
                    tensor_id=tensor_id,
                    global_offset=(0,),
                    local_shape=(4,),
                    nbytes=4,
                    rank=ParallelRank(),
                    aliases=aliases,
                )
                for tensor_id in aliases
            ),
            participant_ids={ParallelRank(): "target-partial-alias"},
        )
        binding = WeightRuntimeBindingManifest(
            resource_id=MODEL_ID,
            revision=REVISION,
            placement_id=placement.placement_id,
            placement_digest=placement.digest,
            instance_id="target-instance",
            participant_id="target-partial-alias",
            generation=7,
            lease_id="target-lease",
            fragments=(
                RuntimeBindingFragment(
                    placement_fragment_id="placement-alias.a",
                    fragment_id="runtime-alias.a",
                    address=0x9000,
                    nbytes=4,
                    worker_id="target-worker",
                    endpoint="target-endpoint",
                    device="cuda:0",
                    itemsize=1,
                    local_shape=(4,),
                    strides_bytes=(1,),
                    storage_address=0x9000,
                    storage_nbytes=4,
                    storage_offset_bytes=0,
                ),
                RuntimeBindingFragment(
                    placement_fragment_id="placement-alias.b",
                    fragment_id="runtime-alias.b",
                    address=0x9002,
                    nbytes=4,
                    worker_id="target-worker",
                    endpoint="target-endpoint",
                    device="cuda:0",
                    itemsize=1,
                    local_shape=(4,),
                    strides_bytes=(1,),
                    storage_address=0x9002,
                    storage_nbytes=4,
                    storage_offset_bytes=0,
                ),
            ),
        )

        with self.assertRaisesRegex(
            ValueError, "runtime (binding|manifest) address ranges overlap"
        ):
            validate_runtime_binding(placement, binding)

    def test_binding_allows_adjacent_runtime_ranges(self) -> None:
        placement = split_target_placement()
        binding = split_target_binding(right_address=0x9004)
        validate_runtime_binding(placement, binding)

        self.assertEqual(
            tuple(fragment.address for fragment in binding.fragments),
            (0x9000, 0x9004),
        )

    def test_binding_allows_same_address_on_different_workers(self) -> None:
        placement = split_target_placement()
        binding = split_target_binding(
            right_address=0x9000,
            right_worker_id="target-worker-1",
        )
        validate_runtime_binding(placement, binding)

        self.assertEqual(
            tuple(fragment.address for fragment in binding.fragments),
            (0x9000, 0x9000),
        )

    def test_binding_rejects_wrong_identity_and_fragment_sets(self) -> None:
        placement = target_placement()

        with self.assertRaisesRegex(ValueError, "placement_id"):
            validate_runtime_binding(
                placement,
                target_binding(placement_id="different-placement"),
            )
        with self.assertRaisesRegex(ValueError, "unknown placement fragment"):
            validate_runtime_binding(
                placement,
                target_binding(
                    placement=placement,
                    placement_fragment_id="unknown-fragment",
                ),
            )
        with self.assertRaisesRegex(ValueError, "byte size"):
            validate_runtime_binding(placement, target_binding(nbytes=8))

    def test_binding_rejects_missing_or_duplicate_fragments(self) -> None:
        placement = target_placement()
        empty = WeightRuntimeBindingManifest(
            resource_id=MODEL_ID,
            revision=REVISION,
            placement_id=placement.placement_id,
            placement_digest=placement.digest,
            instance_id="target-instance",
            participant_id="target-tp1",
            generation=7,
            lease_id="target-lease",
            fragments=(),
        )

        with self.assertRaisesRegex(ValueError, "missing placement fragment"):
            validate_runtime_binding(placement, empty)

        fragment = target_binding().fragments[0]
        with self.assertRaisesRegex(ValueError, "duplicate placement fragment"):
            WeightRuntimeBindingManifest(
                resource_id=MODEL_ID,
                revision=REVISION,
                placement_id=placement.placement_id,
                placement_digest=placement.digest,
                instance_id="target-instance",
                participant_id="target-tp1",
                generation=7,
                lease_id="target-lease",
                fragments=(fragment, fragment),
            )

    def test_binding_allows_one_rank_spanning_multiple_runtime_locations(self) -> None:
        tensor = descriptor()
        placement = global_placement_from_fragments(
            resource_id=MODEL_ID,
            revision=REVISION,
            placement_set_id="target-multi-location",
            tensors=(tensor,),
            fragments=(
                PlacementFragment(
                    placement_fragment_id="placement-left",
                    tensor_id=tensor.tensor_id,
                    global_offset=(0,),
                    local_shape=(4,),
                    nbytes=4,
                    rank=ParallelRank(tp=0),
                ),
                PlacementFragment(
                    placement_fragment_id="placement-right",
                    tensor_id=tensor.tensor_id,
                    global_offset=(4,),
                    local_shape=(4,),
                    nbytes=4,
                    rank=ParallelRank(tp=0),
                ),
            ),
            participant_ids={ParallelRank(): "target-multi-location"},
        )
        binding = WeightRuntimeBindingManifest(
            resource_id=MODEL_ID,
            revision=REVISION,
            placement_id=placement.placement_id,
            placement_digest=placement.digest,
            instance_id="target-instance",
            participant_id="target-multi-location",
            generation=7,
            lease_id="target-lease",
            fragments=(
                RuntimeBindingFragment(
                    placement_fragment_id="placement-left",
                    fragment_id="runtime-left",
                    address=0x9000,
                    nbytes=4,
                    worker_id="target-worker-0",
                    endpoint="target-endpoint-0",
                    device="cuda:0",
                    itemsize=1,
                    local_shape=(4,),
                    strides_bytes=(1,),
                    storage_address=0x9000,
                    storage_nbytes=4,
                    storage_offset_bytes=0,
                ),
                RuntimeBindingFragment(
                    placement_fragment_id="placement-right",
                    fragment_id="runtime-right",
                    address=0xA000,
                    nbytes=4,
                    worker_id="target-worker-1",
                    endpoint="target-endpoint-1",
                    device="cuda:0",
                    itemsize=1,
                    local_shape=(4,),
                    strides_bytes=(1,),
                    storage_address=0xA000,
                    storage_nbytes=4,
                    storage_offset_bytes=0,
                ),
            ),
        )

        validate_runtime_binding(placement, binding)

        self.assertEqual(
            tuple(fragment.worker_id for fragment in binding.fragments),
            ("target-worker-0", "target-worker-1"),
        )

    def test_accepts_framework_adapted_canonical_split_manifests(self) -> None:
        target_rank = ParallelRank(tp=1, pp=2, ep=3)
        complement_rank = ParallelRank(tp=0, pp=2, ep=3)
        tensor = TensorDescriptor(
            tensor_id="layers.0.weight",
            global_shape=(8,),
            dtype="uint8",
            itemsize=1,
            shard_dims=(0,),
            layer_id=0,
            layout_fingerprint="test:logical-box:v2",
            parallel_axes=(
                OwnershipAxis(kind="pp"),
                OwnershipAxis(kind="ep"),
                SplitAxis(kind="tp", dim=0),
            ),
        )
        target_fragment = PlacementFragment(
            placement_fragment_id="sglang-placement-fragment",
            tensor_id=tensor.tensor_id,
            global_offset=(4,),
            local_shape=(4,),
            nbytes=4,
            rank=target_rank,
        )
        complement_fragment = PlacementFragment(
            placement_fragment_id="sglang-complement-fragment",
            tensor_id=tensor.tensor_id,
            global_offset=(0,),
            local_shape=(4,),
            nbytes=4,
            rank=complement_rank,
        )
        placement = global_placement_from_fragments(
            resource_id=MODEL_ID,
            revision=REVISION,
            placement_set_id="sglang-placement",
            tensors=(tensor,),
            fragments=(target_fragment, complement_fragment),
            participant_ids={
                target_rank: "sglang-target",
                complement_rank: "sglang-complement",
            },
        )
        binding = WeightRuntimeBindingManifest(
            resource_id=MODEL_ID,
            revision=REVISION,
            placement_id=placement.placement_id,
            placement_digest=placement.digest,
            instance_id="sglang-target",
            participant_id="sglang-target",
            generation=11,
            lease_id="sglang-lease",
            fragments=(
                RuntimeBindingFragment(
                    placement_fragment_id=target_fragment.placement_fragment_id,
                    fragment_id="sglang-runtime-fragment",
                    address=0xA000,
                    nbytes=4,
                    worker_id="sglang-worker",
                    endpoint="sglang-endpoint",
                    device="cuda:0",
                    itemsize=1,
                    local_shape=(4,),
                    strides_bytes=(1,),
                    storage_address=0xA000,
                    storage_nbytes=4,
                    storage_offset_bytes=0,
                ),
            ),
        )
        validate_runtime_binding(placement, binding)

        self.assertEqual(binding.fragments[0].address, 0xA000)
        actual_target_fragment = next(
            fragment
            for fragment in placement.fragments
            if fragment.placement_fragment_id == "sglang-placement-fragment"
        )
        self.assertEqual(actual_target_fragment.rank, target_rank)
        self.assertEqual(binding.generation, 11)


if __name__ == "__main__":
    unittest.main()
