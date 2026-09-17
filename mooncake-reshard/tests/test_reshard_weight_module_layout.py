from __future__ import annotations

import importlib.util

import mooncake.reshard.weight as model_weight
import mooncake.reshard.weight._planner.contracts as planner_contracts
from mooncake.reshard.weight._planner.bound_contracts import (
    TransferPlan as BoundTransferPlan,
)
from mooncake.reshard.weight.binding import (
    validate_runtime_binding,
    validate_runtime_bindings,
)
from mooncake.reshard.weight.manifest import (
    ParallelRank,
    ParallelTopology,
    PlacementFragment,
    RuntimeBindingFragment,
    TensorDescriptor,
    TopologyParticipant,
    WeightPlacementManifest,
    WeightPlacementPart,
    WeightRuntimeBindingManifest,
)
from mooncake.reshard.weight.part import WeightPlacementPart as PlacementPartContract
from mooncake.reshard.weight.placement import (
    WeightPlacementManifest as PlacementContract,
)
from mooncake.reshard.weight.runtime import (
    WeightRuntimeBindingManifest as RuntimeBindingContract,
)
from mooncake.reshard.weight.types import (
    ParallelRank as ParallelRankContract,
)
from mooncake.reshard.weight.types import (
    PlacementFragment as PlacementFragmentContract,
)
from mooncake.reshard.weight.types import (
    RuntimeBindingFragment as RuntimeBindingFragmentContract,
)
from mooncake.reshard.weight.types import TensorDescriptor as TensorContract
from mooncake.reshard.weight.topology import (
    ParallelTopology as ParallelTopologyContract,
)
from mooncake.reshard.weight.topology import (
    TopologyParticipant as TopologyParticipantContract,
)


def test_responsibility_modules_preserve_public_contract_identity() -> None:
    assert model_weight.ParallelRank is ParallelRank is ParallelRankContract
    assert model_weight.ParallelTopology is ParallelTopology is ParallelTopologyContract
    assert (
        model_weight.TopologyParticipant
        is TopologyParticipant
        is TopologyParticipantContract
    )
    assert model_weight.TensorDescriptor is TensorDescriptor is TensorContract
    assert (
        model_weight.PlacementFragment is PlacementFragment is PlacementFragmentContract
    )
    assert (
        model_weight.RuntimeBindingFragment
        is RuntimeBindingFragment
        is RuntimeBindingFragmentContract
    )
    assert (
        model_weight.WeightPlacementManifest
        is WeightPlacementManifest
        is PlacementContract
    )
    assert (
        model_weight.WeightPlacementPart is WeightPlacementPart is PlacementPartContract
    )
    assert (
        model_weight.WeightRuntimeBindingManifest
        is WeightRuntimeBindingManifest
        is RuntimeBindingContract
    )
    assert model_weight.validate_runtime_binding is validate_runtime_binding
    assert model_weight.validate_runtime_bindings is validate_runtime_bindings


def test_runtime_binding_phase_exports_bound_contracts() -> None:
    assert model_weight.bind_logical_transfer_plan is not None
    assert hasattr(planner_contracts, "BoundWeightFragment")
    assert model_weight.TransferPlan is BoundTransferPlan
    assert (
        importlib.util.find_spec("mooncake.reshard.weight._planner.attestation")
        is not None
    )
