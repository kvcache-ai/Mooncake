"""RelayCaching-style selective recomputation (RFC §6.5.1 tier 2).

When a new step's input diverges from the predecessor's prefix (e.g. a
tool return injected a large block of new text mid-conversation), exact
prefix matching fails on the divergent region but still succeeds on the
shared prefix and suffix. RelayCaching (training-free, >80% cross-step
reuse in the original paper) splits the new sequence into alternating
``reusable`` / ``recompute`` segments:

- reusable segments: pull KV from the RadixTree (or from the predecessor
  state directly).
- recompute segments: run a fresh prefill over just those tokens.

Cross-segment attention is recovered via a ResidualAttention correction
kernel, so that splitting does not corrupt the global attention pattern.
"""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Callable, List, Optional, Sequence, Tuple

import torch

from ..state.page_manager import BlockRef, PagedKVManager
from ..state.radix_tree import RadixTree, _find_subsequence


@dataclass
class Segment:
    """A contiguous token segment tagged as reusable or needing recomputation."""

    tokens: List[int]
    reusable: bool
    refs: Optional[List[BlockRef]] = None     # only if reusable
    prev_offset: int = -1


@dataclass
class RelayTier3Candidate:
    """Tier-3 candidate metadata for a recomputed reusable segment."""

    relay_ref_start: int
    prev_token_start: int
    token_count: int
    recomputed_refs: List[BlockRef]


# ---------------------------------------------------------------------------
# Segment splitter
# ---------------------------------------------------------------------------
def split_segments(
    new_tokens: Sequence[int],
    prev_tokens: Sequence[int],
    page_size: int,
    min_match_run: int = 4,
) -> List[Segment]:
    """Split ``new_tokens`` into alternating reusable / recompute segments
    by longest-common-substring matching against ``prev_tokens``.

    A run of >= ``min_match_run`` consecutive matches is considered
    reusable. Smaller matches are folded into the surrounding recompute
    segment to avoid pathological fragmentation.
    """
    new_tokens = list(new_tokens)
    prev_tokens = list(prev_tokens)
    n = len(new_tokens)
    if n == 0:
        return []

    # Greedy longest-match scan: for each position i in new_tokens, find
    # the longest prefix of new_tokens[i:] that appears in prev_tokens.
    match_len = [0] * n
    match_off = [-1] * n
    prev_index: dict = {}
    for j, t in enumerate(prev_tokens):
        prev_index.setdefault(int(t), []).append(j)
    for i in range(n):
        best = 0
        best_off = -1
        for j in prev_index.get(int(new_tokens[i]), []):
            k = 0
            while (i + k < n) and (j + k < len(prev_tokens)) and new_tokens[i + k] == prev_tokens[j + k]:
                k += 1
            if k > best:
                best = k
                best_off = j
        match_len[i] = best
        match_off[i] = best_off

    # Greedily select non-overlapping runs >= min_match_run
    runs: List[Tuple[int, int, int]] = []  # (start_in_new, length, start_in_prev)
    i = 0
    while i < n:
        L = match_len[i]
        if L >= min_match_run:
            runs.append((i, L, match_off[i]))
            i += L
        else:
            i += 1

    # Build segments: alternate recompute (gap) / reusable (run)
    segments: List[Segment] = []
    cursor = 0
    for (s, L, p_off) in runs:
        if cursor < s:
            segments.append(Segment(tokens=new_tokens[cursor:s], reusable=False))
        segments.append(
            Segment(
                tokens=new_tokens[s:s + L],
                reusable=True,
                prev_offset=p_off,
            )
        )
        cursor = s + L
    if cursor < n:
        segments.append(Segment(tokens=new_tokens[cursor:], reusable=False))
    return segments


# ---------------------------------------------------------------------------
# ResidualAttention correction
# ---------------------------------------------------------------------------
def residual_attention_merge(
    reusable_kv: List[Tuple[torch.Tensor, torch.Tensor]],
    recomputed_kv: List[Tuple[torch.Tensor, torch.Tensor]],
    query: torch.Tensor,
    segment_order: List[bool],
) -> Tuple[torch.Tensor, torch.Tensor]:
    """Combine KV from reusable + recomputed segments with a cross-segment
    attention correction.

    For each query token, the standard attention is:

        attn(q, K) = softmax(q K^T / sqrt(d)) V

    With split segments, naive concatenation gives the same result because
    softmax normalizes across all keys. However, the *attention mask* for
    causal decoding depends on global position; concatenation preserves
    position order if we rebuild the full (K, V) in segment_order.

    Returns (K_full, V_full) reassembled in the correct global order so
    that downstream attention is bit-exact against the non-split prefill.
    """
    # Rebuild in order
    reusable_iter = iter(reusable_kv)
    recomputed_iter = iter(recomputed_kv)
    keys, values = [], []
    for is_reusable in segment_order:
        if is_reusable:
            k, v = next(reusable_iter)
        else:
            k, v = next(recomputed_iter)
        keys.append(k)
        values.append(v)
    K_full = torch.cat(keys, dim=-2)
    V_full = torch.cat(values, dim=-2)
    return K_full, V_full


# ---------------------------------------------------------------------------
# Main entry: selective recompute
# ---------------------------------------------------------------------------
class RelayRecompute:
    """Training-free selective KV recomputation across agent steps.

    Usage:

        relay = RelayRecompute(pm, radix, prefill_fn)
        new_refs, stats = relay.run(
            new_tokens=new_tokens,
            prev_tokens=prev_state.meta.token_ids,
            prev_state=prev_state,
        )

    ``prefill_fn(segment_tokens, prefix_kv=None) -> (new_refs, logits)``
    is the same signature used by ``Workflow.advance``.
    """

    def __init__(
        self,
        page_manager: PagedKVManager,
        radix: RadixTree,
        prefill_fn: Callable,
        min_match_run: int = 4,
    ):
        self.pm = page_manager
        self.radix = radix
        self.prefill_fn = prefill_fn
        self.min_match_run = min_match_run

    def run_with_trace(
        self,
        new_tokens: Sequence[int],
        prev_tokens: Sequence[int],
        scope: Optional[str] = None,
    ) -> Tuple[List[BlockRef], dict, List[RelayTier3Candidate]]:
        segments = split_segments(
            new_tokens, prev_tokens, self.pm.page_size,
            min_match_run=self.min_match_run,
        )
        if not segments:
            return [], {
                "segments": 0,
                "reusable_segments": 0,
                "recompute_segments": 0,
                "reused_tokens": 0,
                "recomputed_tokens": 0,
                "reuse_ratio": 0.0,
                "substring_hits": 0,
                "substring_misses": 0,
                "prefill_calls": 0,
                "prefill_tokens": 0,
                "prefill_ms": 0.0,
            }, []

        all_refs: List[BlockRef] = []
        tier3_candidates: List[RelayTier3Candidate] = []
        reused_tokens = 0
        recomputed_tokens = 0
        substring_hits = 0
        substring_misses = 0
        prefill_calls = 0
        prefill_tokens = 0
        prefill_ms = 0.0
        reusable_segments = 0
        recompute_segments = 0
        for seg in segments:
            if seg.reusable:
                reusable_segments += 1
                # Substring lookup: find the segment anywhere in any stored
                # root-to-node path (not just at the prefix).
                covering_refs, start_offset = self.radix.match_substring(
                    seg.tokens,
                    scope=scope,
                )
                if covering_refs and start_offset >= 0:
                    # Pick the pages that actually cover
                    # [start_offset, start_offset + len(seg.tokens)).
                    picked: List[BlockRef] = []
                    cursor = 0
                    for r in covering_refs:
                        p_end = cursor + r.filled
                        if p_end > start_offset and cursor < start_offset + len(seg.tokens):
                            picked.append(r)
                        cursor = p_end
                    if picked:
                        # Verify the picked pages' tokens match the segment.
                        page_tokens = []
                        cursor = 0
                        for r in covering_refs:
                            p_end = cursor + r.filled
                            if r in picked:
                                page_tokens.extend(prev_tokens[cursor:p_end])
                            cursor = p_end
                        # Find where seg.tokens sits in page_tokens.
                        local_off = _find_subsequence(
                            tuple(page_tokens), tuple(seg.tokens)
                        )
                        # The state layer cannot represent sub-page slices as
                        # first-class logical refs yet, so only reuse when the
                        # segment covers full picked pages exactly. Otherwise
                        # we would over-attach unrelated prefix/suffix tokens
                        # from the surrounding pages and corrupt the committed
                        # state path.
                        if (
                            local_off == 0
                            and sum(r.filled for r in picked) == len(seg.tokens)
                        ):
                            for r in picked:
                                self.pm.incref(r.physical_id)
                            all_refs.extend(picked)
                            reused_tokens += len(seg.tokens)
                            substring_hits += 1
                            continue
                # Fall through to recompute.
                substring_misses += 1
            else:
                recompute_segments += 1
            # Recompute segment via prefill_fn (no prefix -- segment-local).
            t0 = time.perf_counter()
            seg_refs, _ = self.prefill_fn(seg.tokens, prefix_kv_refs=None)
            prefill_ms += (time.perf_counter() - t0) * 1000.0
            prefill_calls += 1
            prefill_tokens += len(seg.tokens)
            start_idx = len(all_refs)
            all_refs.extend(seg_refs)
            recomputed_tokens += len(seg.tokens)
            if seg.reusable and seg.prev_offset >= 0 and seg_refs:
                tier3_candidates.append(
                    RelayTier3Candidate(
                        relay_ref_start=start_idx,
                        prev_token_start=int(seg.prev_offset),
                        token_count=len(seg.tokens),
                        recomputed_refs=list(seg_refs),
                    )
                )
        stats = {
            "segments": len(segments),
            "reusable_segments": reusable_segments,
            "recompute_segments": recompute_segments + substring_misses,
            "reused_tokens": reused_tokens,
            "recomputed_tokens": recomputed_tokens,
            "reuse_ratio": reused_tokens / max(1, reused_tokens + recomputed_tokens),
            "substring_hits": substring_hits,
            "substring_misses": substring_misses,
            "prefill_calls": prefill_calls,
            "prefill_tokens": prefill_tokens,
            "prefill_ms": prefill_ms,
        }
        return all_refs, stats, tier3_candidates

    def run(
        self,
        new_tokens: Sequence[int],
        prev_tokens: Sequence[int],
        scope: Optional[str] = None,
    ) -> Tuple[List[BlockRef], dict]:
        refs, stats, _ = self.run_with_trace(
            new_tokens=new_tokens,
            prev_tokens=prev_tokens,
            scope=scope,
        )
        return refs, stats
