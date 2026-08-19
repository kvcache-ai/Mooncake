from __future__ import annotations

import json
import unittest
from dataclasses import asdict, replace

from mooncake.reshard.weight import (
    ParallelRank,
    ParallelTopology,
    PlacementFragment,
    TopologyParticipant,
    WeightPlacementManifest,
    WeightRuntimeBindingManifest,
    bind_logical_transfer_plan,
    plan_placement_transfer,
    plan_placement_transfer_to_local_target,
)

from model_weight_planner.helpers import global_placement_from_fragments

from .helpers import (
    TARGET_PARTICIPANT_ID,
    replicated_source_placement,
    source_binding,
    source_placement,
    target_binding,
    target_placement,
)


class PlacementPlannerTest(unittest.TestCase):
    def test_plans_without_addresses_then_binds_both_sides(self) -> None:
        source = source_placement()
        target = target_placement()

        logical = plan_placement_transfer_to_local_target(
            source,
            target,
            TARGET_PARTICIPANT_ID,
        )

        encoded = json.dumps(asdict(logical), sort_keys=True)
        self.assertEqual(logical.source_placement_id, source.placement_id)
        self.assertEqual(logical.target_placement_id, target.placement_id)
        self.assertIsInstance(logical.operations[0].source, PlacementFragment)
        self.assertIsInstance(logical.operations[0].target, PlacementFragment)
        for physical_field in (
            "address",
            "endpoint",
            "worker_id",
            "instance_id",
            "lease",
            "owner",
        ):
            self.assertNotIn(physical_field, encoded)

        bound = bind_logical_transfer_plan(
            logical,
            (target_binding(),),
            source_bindings=(source_binding(),),
        )

        self.assertEqual(bound.operations[0].source.address, 0x1000)
        self.assertEqual(bound.operations[0].target.address, 0x9000)
        self.assertEqual(bound.operations[0].source.rank.tp, 0)
        self.assertEqual(bound.operations[0].target.rank.tp, 1)
        self.assertEqual(
            bound.source_executors[0].fragment_leases[0].lease_generation,
            3,
        )

    def test_source_binding_is_required_and_validated_fail_closed(self) -> None:
        logical = plan_placement_transfer_to_local_target(
            source_placement(),
            target_placement(),
            TARGET_PARTICIPANT_ID,
        )

        with self.assertRaisesRegex(ValueError, "requires source runtime bindings"):
            bind_logical_transfer_plan(logical, (target_binding(),))
        with self.assertRaisesRegex(ValueError, "source placement IDs differ"):
            bind_logical_transfer_plan(
                logical,
                (target_binding(),),
                source_bindings=(target_binding(),),
            )
        with self.assertRaisesRegex(ValueError, "byte size"):
            bind_logical_transfer_plan(
                logical,
                (target_binding(),),
                source_bindings=(source_binding(nbytes=4),),
            )

    def test_bind_rejects_runtime_stride_evidence_that_differs_from_placement(
        self,
    ) -> None:
        logical = plan_placement_transfer_to_local_target(
            source_placement(),
            target_placement(),
            TARGET_PARTICIPANT_ID,
        )
        binding = target_binding()
        invalid = replace(
            binding,
            fragments=(replace(binding.fragments[0], strides_bytes=(2,)),),
        )

        with self.assertRaisesRegex(ValueError, "canonical contiguous view"):
            bind_logical_transfer_plan(
                logical,
                (invalid,),
                source_bindings=(source_binding(),),
            )

    def test_bind_rejects_runtime_binding_for_empty_participant(self) -> None:
        base = target_placement()
        topology = ParallelTopology(
            tp_size=2,
            pp_size=2,
            ep_size=1,
            dp_size=1,
            participants=base.topology.participants
            + (
                TopologyParticipant(
                    participant_id="target-empty-pp1",
                    rank=ParallelRank(pp=1),
                ),
            ),
        )
        target = WeightPlacementManifest.from_fragments(
            resource_id=base.resource_id,
            revision=base.revision,
            weight_generation=base.weight_generation,
            placement_set_id=base.placement_set_id,
            topology=topology,
            tensors=base.tensors,
            fragments=base.fragments,
        )
        logical = plan_placement_transfer_to_local_target(
            source_placement(),
            target,
            TARGET_PARTICIPANT_ID,
        )
        empty_binding = WeightRuntimeBindingManifest(
            resource_id=target.resource_id,
            revision=target.revision,
            placement_id=target.placement_id,
            placement_digest=target.digest,
            instance_id="target-empty-instance",
            participant_id="target-empty-pp1",
            generation=7,
            lease_id="target-empty-lease",
            fragments=(),
        )

        with self.assertRaisesRegex(
            ValueError,
            "target runtime binding participants differ",
        ):
            bind_logical_transfer_plan(
                logical,
                (target_binding(placement=target), empty_binding),
                source_bindings=(source_binding(),),
            )

    def test_source_binding_tracks_independent_dp_participants(self) -> None:
        source = replicated_source_placement()
        logical = plan_placement_transfer_to_local_target(
            source,
            target_placement(),
            TARGET_PARTICIPANT_ID,
        )

        selected = source_binding(
            placement=source,
            placement_fragment_id="source-dp0-fragment",
            instance_id="source-dp0-instance",
            generation=3,
            address=0x1000,
        )
        unselected = source_binding(
            placement=source,
            placement_fragment_id="source-dp1-fragment",
            instance_id="source-dp1-instance",
            generation=4,
            address=0x2000,
        )

        plan = bind_logical_transfer_plan(
            logical,
            (target_binding(),),
            source_bindings=(selected, unselected),
        )
        self.assertEqual(
            {
                executor.instance_id: executor.fragment_leases[0].lease_generation
                for executor in plan.source_executors
            },
            {"source-dp0-instance": 3, "source-dp1-instance": 4},
        )

    def test_partial_source_bindings_reject_cross_participant_overlap(self) -> None:
        tensor = source_placement().tensors[0]
        source_fragments = tuple(
            PlacementFragment(
                placement_fragment_id=f"source-dp{dp}-tp{tp}",
                tensor_id=tensor.tensor_id,
                global_offset=(tp * 4,),
                local_shape=(4,),
                nbytes=4,
                rank=ParallelRank(dp=dp, tp=tp),
            )
            for dp in range(2)
            for tp in range(2)
        )
        source = global_placement_from_fragments(
            resource_id=source_placement().resource_id,
            revision=source_placement().revision,
            placement_set_id="source-two-dp-two-tp",
            tensors=(tensor,),
            fragments=source_fragments,
            participant_ids={
                fragment.rank: (f"source-dp{fragment.rank.dp}-tp{fragment.rank.tp}")
                for fragment in source_fragments
            },
        )
        target = target_placement()
        logical = plan_placement_transfer(source, target)
        selected_source_ids = {
            executor.participant_id for executor in logical.source_executors
        }
        self.assertEqual(selected_source_ids, {"source-dp0-tp0", "source-dp0-tp1"})
        source_bindings = []
        for tp in range(2):
            binding = source_binding(
                placement=source,
                placement_fragment_id=f"source-dp0-tp{tp}",
                instance_id="shared-source-instance",
                lease_id=f"source-tp{tp}-lease",
                address=0x1000,
                nbytes=4,
                worker_id="shared-source-worker",
                endpoint="shared-source-endpoint",
            )
            source_bindings.append(
                replace(
                    binding,
                    fragments=(
                        replace(
                            binding.fragments[0],
                            fragment_id=f"source-runtime-tp{tp}",
                        ),
                    ),
                )
            )
        target_bindings = (
            source_binding(
                placement=target,
                placement_fragment_id="target-complement-fragment",
                instance_id="target-tp0-instance",
                lease_id="target-tp0-lease",
                address=0x8000,
                nbytes=4,
            ),
            target_binding(placement=target),
        )

        with self.assertRaisesRegex(ValueError, "address ranges overlap"):
            bind_logical_transfer_plan(
                logical,
                target_bindings,
                source_bindings=tuple(source_bindings),
            )

    def test_source_address_rebinds_without_replanning(self) -> None:
        logical = plan_placement_transfer_to_local_target(
            source_placement(),
            target_placement(),
            TARGET_PARTICIPANT_ID,
        )

        first = bind_logical_transfer_plan(
            logical,
            (target_binding(),),
            source_bindings=(source_binding(address=0x1000),),
        )
        second = bind_logical_transfer_plan(
            logical,
            (target_binding(),),
            source_bindings=(
                source_binding(
                    instance_id="source-instance-2",
                    generation=4,
                    lease_id="source-lease-2",
                    address=0x2000,
                    worker_id="source-worker-2",
                    endpoint="source-endpoint-2",
                ),
            ),
        )

        self.assertEqual(first.operations[0].source.address, 0x1000)
        self.assertEqual(second.operations[0].source.address, 0x2000)
        self.assertEqual(second.source_executors[0].instance_id, "source-instance-2")
        self.assertEqual(second.source_executors[0].worker_id, "source-worker-2")
        self.assertEqual(
            first.operations[0].overlap_offset,
            second.operations[0].overlap_offset,
        )
        self.assertEqual(
            first.operations[0].overlap_shape,
            second.operations[0].overlap_shape,
        )

    def test_rejects_binding_from_another_target_placement(self) -> None:
        logical = plan_placement_transfer_to_local_target(
            source_placement(),
            target_placement(),
            TARGET_PARTICIPANT_ID,
        )
        other_placement = target_placement(fragment_id="other-fragment")
        other_binding = target_binding(
            placement=other_placement,
            placement_fragment_id="other-fragment",
        )

        with self.assertRaisesRegex(ValueError, "placement"):
            bind_logical_transfer_plan(
                logical,
                (other_binding,),
                source_bindings=(source_binding(),),
            )


if __name__ == "__main__":
    unittest.main()
