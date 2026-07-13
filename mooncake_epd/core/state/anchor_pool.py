"""A2A anchor pool (RFC §6.6, KVCOMM-style).

Maintains per-agent token-offset anchors that index the pages backing a
state. On handoff, the receiving agent can look up which of its tokens
share an anchor with the sender, and directly reuse the matching pages
instead of running a full RadixTree scan.

The anchor structure is lightweight (a dict of lists keyed by agent_id)
and supports multi-agent topologies with O(1) lookup by the first token.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from .page_manager import BlockRef, PagedKVManager


@dataclass
class Anchor:
    agent_id: str
    token_ids: Tuple[int, ...]
    refs: List[BlockRef]
    feature_hashes: List[str] = field(default_factory=list)


class AnchorPool:
    """Cross-agent anchor index for fast A2A handoff reuse."""

    def __init__(self, page_manager: PagedKVManager):
        self.pm = page_manager
        self._anchors: Dict[str, List[Anchor]] = {}
        self._first_token_index: Dict[int, List[Anchor]] = {}
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    def register_anchor(
        self,
        agent_id: str,
        token_ids: Sequence[int],
        refs: Sequence[BlockRef],
        feature_hashes: Optional[Sequence[str]] = None,
    ) -> None:
        if not token_ids or not refs:
            return
        anchor = Anchor(
            agent_id=agent_id,
            token_ids=tuple(int(t) for t in token_ids),
            refs=list(refs),
            feature_hashes=list(feature_hashes or []),
        )
        # Incref each page -- the pool holds its own reference.
        for ref in refs:
            self.pm.incref(ref.physical_id)
        with self._lock:
            self._anchors.setdefault(agent_id, []).append(anchor)
            self._first_token_index.setdefault(anchor.token_ids[0], []).append(anchor)

    # ------------------------------------------------------------------
    def lookup(
        self,
        token_ids: Sequence[int],
        exclude_agent: Optional[str] = None,
    ) -> Tuple[List[BlockRef], List[int]]:
        """Return (matched_refs, unmatched_suffix_tokens) across all anchors.

        Picks the anchor whose token_ids is the longest common prefix of
        the query. Pages under the matching anchor are incref'd.
        """
        tokens = [int(t) for t in token_ids]
        if not tokens:
            return [], []
        candidates = self._first_token_index.get(tokens[0], [])
        best: Optional[Anchor] = None
        best_len = 0
        with self._lock:
            for anchor in candidates:
                if exclude_agent is not None and anchor.agent_id == exclude_agent:
                    continue
                L = self._common_prefix(tokens, anchor.token_ids)
                if L > best_len:
                    best_len = L
                    best = anchor
        if best is None or best_len == 0:
            return [], list(tokens)
        # The matching anchor covers best_len tokens. Map tokens -> pages:
        # pages cover the anchor in page_size chunks.
        refs = self._refs_covering(best.refs, best_len)
        for ref in refs:
            self.pm.incref(ref.physical_id)
        return refs, list(tokens[best_len:])

    # ------------------------------------------------------------------
    def release_all(self) -> None:
        with self._lock:
            for agent_anchors in self._anchors.values():
                for anchor in agent_anchors:
                    for ref in anchor.refs:
                        self.pm.decref(ref.physical_id)
            self._anchors.clear()
            self._first_token_index.clear()

    # ------------------------------------------------------------------
    @staticmethod
    def _common_prefix(a: Sequence[int], b: Sequence[int]) -> int:
        n = min(len(a), len(b))
        for i in range(n):
            if a[i] != b[i]:
                return i
        return n

    @staticmethod
    def _refs_covering(refs: Sequence[BlockRef], n_tokens: int) -> List[BlockRef]:
        out: List[BlockRef] = []
        covered = 0
        for ref in refs:
            if covered >= n_tokens:
                break
            out.append(ref)
            covered += ref.filled
        return out
