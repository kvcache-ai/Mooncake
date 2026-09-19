"""Three-tier reuse pipeline (RFC §6.5.1).

Orchestrates the three reuse tiers used by ``Workflow.advance``:

1. **Exact prefix reuse** (RadixTree longest-prefix match). Zero quality
   loss; highest coverage when the input is a pure append.
2. **Selective recomputation** (RelayCaching + ResidualAttention). Kicks
   in when the input diverges mid-sequence. Recovers reuse by splitting
   into reusable / recompute segments.
3. **Attention-similarity fallback** (RKSC). Page-level reuse gated by
   key-norm-profile cosine similarity. Used as a last-ditch optimization
   on pages that tier-2 could not reuse.

Each tier runs in order and hands off unreused segments to the next.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Sequence, Tuple

import torch

from ...core.state import (
    AttentionSimilarityReuse,
    BlockRef,
    PagedKVManager,
    RadixTree,
    RelayRecompute,
    StateLayer,
    split_segments,
)


@dataclass
class ReuseStats:
    tier1_matched_tokens: int = 0
    tier2_reused_tokens: int = 0
    tier2_recomputed_tokens: int = 0
    relay_segments: int = 0
    relay_reusable_segments: int = 0
    relay_recompute_segments: int = 0
    relay_substring_hits: int = 0
    relay_substring_misses: int = 0
    delta_prefill_calls: int = 0
    delta_prefill_tokens: int = 0
    delta_prefill_ms: float = 0.0
    tier3_accepted_pages: int = 0
    tier3_rejected_pages: int = 0
    tier3_candidate_pages: int = 0
    tier3_reused_tokens: int = 0
    tier3_mean_similarity: float = 0.0
    total_tokens: int = 0
    delta_tokens: int = 0
    elapsed_ms: float = 0.0
    tier1_ms: float = 0.0
    tier2_ms: float = 0.0
    tier3_ms: float = 0.0
    cost_gate_enabled: bool = False
    cost_gate_decision: str = "not_evaluated"
    fallback_reason: str = ""
    predicted_reusable_tokens: int = 0
    predicted_recompute_tokens: int = 0
    estimated_prefill_ms_per_token: Optional[float] = None
    predicted_saved_prefill_ms: Optional[float] = None
    predicted_relay_overhead_ms: Optional[float] = None
    predicted_net_benefit_ms: Optional[float] = None

    @property
    def reused_tokens(self) -> int:
        return (
            self.tier1_matched_tokens
            + self.tier2_reused_tokens
            + self.tier3_reused_tokens
        )

    @property
    def reuse_ratio(self) -> float:
        return self.reused_tokens / max(1, self.total_tokens)

    @property
    def delta_reuse_ratio(self) -> float:
        return (
            self.tier2_reused_tokens + self.tier3_reused_tokens
        ) / max(1, self.delta_tokens)

    @property
    def delta_recompute_ratio(self) -> float:
        return max(
            0,
            self.tier2_recomputed_tokens - self.tier3_reused_tokens,
        ) / max(1, self.delta_tokens)


class ReusePipeline:
    """Three-tier reuse coordinator.

    ``prefill_fn`` must have the same signature as ``Workflow.prefill_fn``:
        prefill_fn(token_ids, prefix_kv_refs=None, ...) -> (refs, logits)
    """

    def __init__(
        self,
        state_layer: StateLayer,
        prefill_fn: Callable,
        relay_threshold: float = 0.6,
        attention_threshold: float = 0.90,
        enable_tier2: bool = True,
        enable_tier3: bool = True,
        relay_min_match_run: int = 4,
        enable_cost_gate: bool = False,
        force_approximate: bool = False,
        estimated_prefill_ms_per_token: Optional[float] = None,
        relay_fixed_overhead_ms: float = 0.25,
        relay_segment_overhead_ms: float = 0.05,
        relay_tier3_page_overhead_ms: float = 0.05,
        min_net_benefit_ms: float = 0.5,
    ):
        self.sl = state_layer
        self.prefill_fn = prefill_fn
        self.relay_threshold = relay_threshold
        self.enable_tier2 = enable_tier2
        self.enable_tier3 = enable_tier3
        self.enable_cost_gate = bool(enable_cost_gate)
        self.force_approximate = bool(force_approximate)
        self.estimated_prefill_ms_per_token = (
            float(estimated_prefill_ms_per_token)
            if estimated_prefill_ms_per_token is not None
            and float(estimated_prefill_ms_per_token) > 0.0
            else None
        )
        self.relay_fixed_overhead_ms = max(0.0, float(relay_fixed_overhead_ms))
        self.relay_segment_overhead_ms = max(0.0, float(relay_segment_overhead_ms))
        self.relay_tier3_page_overhead_ms = max(
            0.0, float(relay_tier3_page_overhead_ms)
        )
        self.min_net_benefit_ms = max(0.0, float(min_net_benefit_ms))
        self.relay = RelayRecompute(
            state_layer.pm,
            state_layer.radix,
            prefill_fn,
            min_match_run=max(1, int(relay_min_match_run)),
        ) if enable_tier2 else None
        self.attn = AttentionSimilarityReuse(
            state_layer.pm, threshold=attention_threshold,
        ) if enable_tier3 else None

    def update_cost_calibration(self, ms_per_token: Optional[float]) -> None:
        if ms_per_token is None or float(ms_per_token) <= 0.0:
            return
        observed = float(ms_per_token)
        if self.estimated_prefill_ms_per_token is None:
            self.estimated_prefill_ms_per_token = observed
        else:
            self.estimated_prefill_ms_per_token = (
                0.8 * self.estimated_prefill_ms_per_token + 0.2 * observed
            )

    def _evaluate_cost_gate(
        self,
        *,
        delta_tokens: Sequence[int],
        prev_tokens: Sequence[int],
        workflow_id: Optional[str],
        stats: ReuseStats,
    ) -> bool:
        stats.cost_gate_enabled = bool(self.enable_cost_gate)
        if not self.enable_cost_gate:
            stats.cost_gate_decision = "disabled"
            return True
        segments = split_segments(
            delta_tokens,
            prev_tokens,
            self.sl.pm.page_size,
            min_match_run=(self.relay.min_match_run if self.relay is not None else 4),
        )
        cursor = 0
        reusable_tokens = 0
        candidate_pages = 0
        for segment in segments:
            token_count = len(segment.tokens)
            if (
                segment.reusable
                and segment.prev_offset >= 0
                and cursor % self.sl.pm.page_size == 0
                and segment.prev_offset % self.sl.pm.page_size == 0
                and token_count % self.sl.pm.page_size == 0
            ):
                covering_refs, start_offset = self.sl.radix.match_substring(
                    segment.tokens,
                    scope=workflow_id,
                )
                picked_tokens = 0
                picked_start = None
                path_cursor = 0
                for ref in covering_refs:
                    page_end = path_cursor + int(ref.filled)
                    if page_end > start_offset and path_cursor < start_offset + token_count:
                        if picked_start is None:
                            picked_start = path_cursor
                        picked_tokens += int(ref.filled)
                    path_cursor = page_end
                if (
                    start_offset >= 0
                    and picked_start == start_offset
                    and picked_tokens == token_count
                ):
                    reusable_tokens += token_count
                    candidate_pages += token_count // self.sl.pm.page_size
            cursor += token_count
        stats.predicted_reusable_tokens = int(reusable_tokens)
        stats.predicted_recompute_tokens = max(0, len(delta_tokens) - reusable_tokens)
        stats.estimated_prefill_ms_per_token = self.estimated_prefill_ms_per_token

        if self.force_approximate:
            stats.cost_gate_decision = "forced_relay"
            return True
        if reusable_tokens <= 0:
            stats.cost_gate_decision = "exact_fallback"
            stats.fallback_reason = "no_full_page_reuse_candidate"
            return False
        if self.estimated_prefill_ms_per_token is None:
            stats.cost_gate_decision = "exact_fallback"
            stats.fallback_reason = "no_calibrated_prefill_cost"
            return False

        saved_ms = reusable_tokens * self.estimated_prefill_ms_per_token
        overhead_ms = (
            self.relay_fixed_overhead_ms
            + len(segments) * self.relay_segment_overhead_ms
            + (
                candidate_pages * self.relay_tier3_page_overhead_ms
                if self.enable_tier3
                else 0.0
            )
        )
        net_ms = saved_ms - overhead_ms
        stats.predicted_saved_prefill_ms = float(saved_ms)
        stats.predicted_relay_overhead_ms = float(overhead_ms)
        stats.predicted_net_benefit_ms = float(net_ms)
        if net_ms < self.min_net_benefit_ms:
            stats.cost_gate_decision = "exact_fallback"
            stats.fallback_reason = "predicted_net_benefit_below_margin"
            return False
        stats.cost_gate_decision = "relay"
        return True

    def _materialize_prev_candidate_refs(
        self,
        *,
        prev_refs: Sequence[BlockRef],
        prev_token_start: int,
        token_count: int,
        template_refs: Sequence[BlockRef],
    ) -> List[BlockRef]:
        if token_count <= 0 or not template_refs:
            return []
        chunked = self.sl.pm.reference_token_chunks(
            prev_refs,
            token_start=int(prev_token_start),
            chunk_sizes=[int(ref.filled) for ref in template_refs],
        )
        if not chunked:
            return []
        expected = sum(int(ref.filled) for ref in template_refs)
        if sum(int(ref.filled) for ref in chunked) != expected:
            self.sl.pm.release_refs(chunked)
            return []
        return chunked

    def _apply_tier3_fallback(
        self,
        *,
        relay_refs: Sequence[BlockRef],
        prev_refs: Sequence[BlockRef],
        tier3_candidates,
        stats: ReuseStats,
    ) -> List[BlockRef]:
        if not self.enable_tier3 or self.attn is None or not tier3_candidates:
            return list(relay_refs)
        resolved = list(relay_refs)
        tier3_t0 = time.perf_counter()
        similarity_weighted_sum = 0.0
        similarity_pages = 0

        for candidate in tier3_candidates:
            recomputed_refs = list(candidate.recomputed_refs)
            if not recomputed_refs:
                continue
            prev_candidate_refs = self._materialize_prev_candidate_refs(
                prev_refs=prev_refs,
                prev_token_start=candidate.prev_token_start,
                token_count=candidate.token_count,
                template_refs=recomputed_refs,
            )
            if not prev_candidate_refs:
                continue
            stats.tier3_candidate_pages += len(prev_candidate_refs)
            new_keys = [
                self.sl.pm.get_page_slice(ref)[0].clone()
                for ref in recomputed_refs
            ]
            accepted, rejected, attn_stats = self.attn.evaluate_sequence(
                prev_candidate_refs,
                new_keys,
            )
            stats.tier3_accepted_pages += int(attn_stats.get("n_accepted", 0) or 0)
            stats.tier3_rejected_pages += int(attn_stats.get("n_rejected", 0) or 0)
            similarity_weighted_sum += float(attn_stats.get("mean_similarity", 0.0) or 0.0) * int(
                attn_stats.get("n_pages", 0) or len(prev_candidate_refs)
            )
            similarity_pages += int(attn_stats.get("n_pages", 0) or len(prev_candidate_refs))

            accepted_iter = iter(accepted)
            rejected_set = set(rejected)
            for local_idx, recomputed_ref in enumerate(recomputed_refs):
                if local_idx in rejected_set:
                    continue
                replacement = next(accepted_iter)
                resolved[candidate.relay_ref_start + local_idx] = replacement
                self.sl.pm.decref(recomputed_ref.physical_id)
                stats.tier3_reused_tokens += int(recomputed_ref.filled)
            self.sl.pm.release_refs(prev_candidate_refs)

        stats.tier3_ms = (time.perf_counter() - tier3_t0) * 1000.0
        if similarity_pages > 0:
            stats.tier3_mean_similarity = similarity_weighted_sum / similarity_pages
        return resolved

    # ------------------------------------------------------------------
    def run(
        self,
        new_tokens: Sequence[int],
        prev_tokens: Sequence[int],
        prev_refs: Sequence[BlockRef],
        workflow_id: Optional[str] = None,
        new_pixel_values: Optional[torch.Tensor] = None,
        new_image_grid_thw: Optional[torch.Tensor] = None,
    ) -> Tuple[List[BlockRef], Optional[torch.Tensor], ReuseStats]:
        """Execute the three-tier pipeline.

        Returns ``(kv_refs, first_logits, stats)``. ``kv_refs`` cover the
        *full* new sequence (tier-1 matches + tier-2/3 fills).
        """
        t0 = time.perf_counter()
        full_tokens = list(prev_tokens) + list(new_tokens)
        stats = ReuseStats(
            total_tokens=len(full_tokens),
            delta_tokens=len(new_tokens),
        )

        # -- Tier 1: exact prefix match --------------------------------
        tier1_t0 = time.perf_counter()
        matched_pages, delta_tokens = self.sl.borrow_prefix_refs(
            full_tokens,
            workflow_id=workflow_id,
        )
        stats.tier1_ms = (time.perf_counter() - tier1_t0) * 1000.0
        stats.tier1_matched_tokens = sum(r.filled for r in matched_pages)

        # If exact match covered everything or tier-2 is off, short-circuit.
        if not delta_tokens or not self.enable_tier2 or self.relay is None:
            if self.prefill_fn is not None and delta_tokens:
                prefill_t0 = time.perf_counter()
                new_refs, logits = self.prefill_fn(
                    delta_tokens, prefix_kv_refs=matched_pages or None,
                    pixel_values=new_pixel_values,
                    image_grid_thw=new_image_grid_thw,
                )
                stats.delta_prefill_ms += (time.perf_counter() - prefill_t0) * 1000.0
                stats.delta_prefill_calls += 1
                stats.delta_prefill_tokens += len(delta_tokens)
            else:
                new_refs, logits = [], None
            stats.elapsed_ms = (time.perf_counter() - t0) * 1000
            return self.sl._normalize_committed_refs(
                matched_pages=matched_pages,
                committed_refs=new_refs,
            ), logits, stats

        # -- Tier 2: selective recomputation ---------------------------
        reuse_ratio_after_t1 = stats.tier1_matched_tokens / max(1, len(full_tokens))
        if reuse_ratio_after_t1 >= self.relay_threshold:
            # T1 already covers enough; just prefill the small delta.
            if self.prefill_fn is not None:
                prefill_t0 = time.perf_counter()
                new_refs, logits = self.prefill_fn(
                    delta_tokens, prefix_kv_refs=matched_pages or None,
                    pixel_values=new_pixel_values,
                    image_grid_thw=new_image_grid_thw,
                )
                stats.delta_prefill_ms += (time.perf_counter() - prefill_t0) * 1000.0
                stats.delta_prefill_calls += 1
                stats.delta_prefill_tokens += len(delta_tokens)
            else:
                new_refs, logits = [], None
            stats.elapsed_ms = (time.perf_counter() - t0) * 1000
            return self.sl._normalize_committed_refs(
                matched_pages=matched_pages,
                committed_refs=new_refs,
            ), logits, stats

        if not self._evaluate_cost_gate(
            delta_tokens=delta_tokens,
            prev_tokens=prev_tokens,
            workflow_id=workflow_id,
            stats=stats,
        ):
            if self.prefill_fn is not None:
                prefill_t0 = time.perf_counter()
                new_refs, logits = self.prefill_fn(
                    delta_tokens,
                    prefix_kv_refs=matched_pages or None,
                    pixel_values=new_pixel_values,
                    image_grid_thw=new_image_grid_thw,
                )
                stats.delta_prefill_ms += (time.perf_counter() - prefill_t0) * 1000.0
                stats.delta_prefill_calls += 1
                stats.delta_prefill_tokens += len(delta_tokens)
            else:
                new_refs, logits = [], None
            stats.elapsed_ms = (time.perf_counter() - t0) * 1000.0
            return self.sl._normalize_committed_refs(
                matched_pages=matched_pages,
                committed_refs=new_refs,
            ), logits, stats

        tier2_t0 = time.perf_counter()
        relay_refs, relay_stats, tier3_candidates = self.relay.run_with_trace(
            delta_tokens,
            prev_tokens,
            scope=workflow_id,
        )
        stats.tier2_ms = (time.perf_counter() - tier2_t0) * 1000.0
        stats.tier2_reused_tokens = relay_stats["reused_tokens"]
        stats.tier2_recomputed_tokens = relay_stats["recomputed_tokens"]
        stats.relay_segments = int(relay_stats.get("segments", 0) or 0)
        stats.relay_reusable_segments = int(relay_stats.get("reusable_segments", 0) or 0)
        stats.relay_recompute_segments = int(relay_stats.get("recompute_segments", 0) or 0)
        stats.relay_substring_hits = int(relay_stats.get("substring_hits", 0) or 0)
        stats.relay_substring_misses = int(relay_stats.get("substring_misses", 0) or 0)
        stats.delta_prefill_calls += int(relay_stats.get("prefill_calls", 0) or 0)
        stats.delta_prefill_tokens += int(relay_stats.get("prefill_tokens", 0) or 0)
        stats.delta_prefill_ms += float(relay_stats.get("prefill_ms", 0.0) or 0.0)

        # -- Tier 3: attention-similarity fallback on recomputed reusable pages ---
        relay_refs = self._apply_tier3_fallback(
            relay_refs=relay_refs,
            prev_refs=prev_refs,
            tier3_candidates=tier3_candidates,
            stats=stats,
        )

        # Combine: matched_pages (tier 1) + relay_refs (tier 2 + 3)
        all_refs = list(matched_pages) + relay_refs
        stats.elapsed_ms = (time.perf_counter() - t0) * 1000
        # Tier-2's prefill returned its own logits internally; expose None
        # here -- the caller can use the last segment's logits if needed.
        return all_refs, None, stats
