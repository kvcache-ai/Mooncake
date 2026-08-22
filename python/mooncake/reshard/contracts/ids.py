"""Static identity categories shared by canonical reshard contracts.

``NewType`` preserves the string wire representation while allowing checked
Python callers to distinguish unrelated resource, placement, and runtime IDs.
Wire decoders and framework adapters construct these values at their boundary.
"""

from __future__ import annotations

from typing import NewType

ResourceId = NewType("ResourceId", str)
PlacementId = NewType("PlacementId", str)
ParticipantId = NewType("ParticipantId", str)
PlacementFragmentId = NewType("PlacementFragmentId", str)
RuntimeFragmentId = NewType("RuntimeFragmentId", str)
TensorId = NewType("TensorId", str)
TopologyId = NewType("TopologyId", str)
PlacementSetId = NewType("PlacementSetId", str)
RevisionId = NewType("RevisionId", str)
RuntimeInstanceId = NewType("RuntimeInstanceId", str)
LeaseId = NewType("LeaseId", str)
StoredFragmentId = NewType("StoredFragmentId", str)


__all__ = [
    "LeaseId",
    "ParticipantId",
    "PlacementFragmentId",
    "PlacementId",
    "PlacementSetId",
    "ResourceId",
    "RevisionId",
    "RuntimeFragmentId",
    "RuntimeInstanceId",
    "StoredFragmentId",
    "TensorId",
    "TopologyId",
]
