"""Address-free fragment aliases used by logical planning."""

from __future__ import annotations

from typing import Union

from ..._typing import TypeAlias
from ..manifest import PlacementFragment
from ..storage_manifest import StoredFragment


# The logical planner only sees immutable placement boxes or Store object
# ranges. Runtime addresses, leases, and allocation owners belong to PR2B.
LogicalSourceFragment: TypeAlias = Union[PlacementFragment, StoredFragment]
LogicalTargetFragment: TypeAlias = PlacementFragment
GeometryFragment: TypeAlias = Union[PlacementFragment, StoredFragment]


__all__ = [
    "GeometryFragment",
    "LogicalSourceFragment",
    "LogicalTargetFragment",
]
