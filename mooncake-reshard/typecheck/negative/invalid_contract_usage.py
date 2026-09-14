"""Negative static checks for canonical reshard contract categories.

This file is intentionally invalid. The type-check script requires pyright to
reject it after the production contract has passed strict checking.
"""

from mooncake.reshard.contracts import ParticipantId, PlacementId
from mooncake.reshard.weight.types import SplitAxis


participant_id = ParticipantId("participant-0")
placement_id: PlacementId = participant_id
SplitAxis(kind="pp", dim=0)
