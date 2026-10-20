# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json


class Plan:
    def __init__(self, plan_path: str, rank: int, start_phase: int = 0):
        """Initialize plan for a specific rank."""
        with open(plan_path, "r") as f:
            self.phases = json.load(f)
        self.rank = rank
        # Auto-detect starting phase for this rank
        self.current_phase = self._find_starting_phase(start_phase)
        self.starting_phase = self.current_phase  # Store the starting phase

    def _find_starting_phase(self, start_search_from_phase: int) -> int:
        """Find the first phase where this rank appears after the start_search_from_phase"""

        for i in range(start_search_from_phase, len(self.phases)):
            if self.rank in self.phases[i]:
                return i

        return -1

    def get_new_ranks(self):
        """Get ranks to connect to at current phase (only the new ones)."""
        if self.current_phase == self.starting_phase:
            # First phase for this rank: connect to all other ranks in the phase (only positive)
            return [
                r for r in self.phases[self.current_phase] if r >= 0 and r != self.rank
            ]
        else:
            # Later phases: only connect to newly added ranks
            prev_ranks = set(self.phases[self.current_phase - 1])
            curr_ranks = set(self.phases[self.current_phase])
            new_ranks = curr_ranks - prev_ranks
            return [r for r in new_ranks if r >= 0 and r != self.rank]

    def get_removed_ranks(self):
        """Get ranks to remove at current phase."""
        if self.current_phase == self.starting_phase:
            # First phase: no ranks to remove
            return []

        # Use absolute values for comparison
        prev_ranks_abs = set(abs(r) for r in self.phases[self.current_phase - 1])
        curr_ranks_abs = set(abs(r) for r in self.phases[self.current_phase])

        # Find negative ranks in current and previous phase
        curr_negative_abs = set(
            abs(r) for r in self.phases[self.current_phase] if r < 0
        )
        prev_negative_abs = set(
            abs(r) for r in self.phases[self.current_phase - 1] if r < 0
        )

        # Ranks cleanly removed: in prev (absolute) but not in curr (absolute),
        # excluding those being killed now and those that were killed before
        cleanly_removed = list(
            prev_ranks_abs - curr_ranks_abs - curr_negative_abs - prev_negative_abs
        )

        return cleanly_removed

    def get_killed_ranks(self):
        """Get ranks that were killed at current phase."""
        return list(set(abs(r) for r in self.phases[self.current_phase] if r < 0))

    def get_active_ranks(self):
        """Get all active ranks in current phase."""
        return [r for r in self.phases[self.current_phase] if r >= 0]

    def get_max_rank(self):
        """Get the maximum participating rank index"""
        return max(max(phase) for phase in self.phases)

    def get_min_active_ranks(self):
        """Get the minimum number of active ranks in all phases."""
        return min([len(phase) for phase in self.phases])

    def next(self):
        """Advance to next phase."""
        if self.current_phase < len(self.phases) - 1:
            self.current_phase += 1
            return True
        return False

    def get_phase(self):
        """Get current phase index."""
        return self.current_phase
