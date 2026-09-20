from __future__ import annotations

from dataclasses import replace

import pytest

from mooncake.reshard.weight import plan_placement_transfer

from .helpers import tp_manifests


def test_logical_plan_rejects_regions_that_do_not_cover_target() -> None:
    source = tp_manifests(
        tp=2,
        pp_rank=0,
        ep_rank=0,
        address_base=0x10000,
        worker_prefix="source",
    )
    target = tp_manifests(
        tp=1,
        pp_rank=0,
        ep_rank=0,
        address_base=0x40000,
        worker_prefix="target",
    )
    logical = plan_placement_transfer(source.placement, target.placement)
    with pytest.raises(ValueError, match="target fragment is not fully covered"):
        replace(logical, operations=(logical.operations[0],))
