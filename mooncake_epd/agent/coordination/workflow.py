"""Workflow: step chain with cross-step KV reuse (RFC §6.5).

A ``Workflow`` represents an agent's multi-step reasoning chain. Each step
builds on the previous step's state, appending new tokens (and optionally
new images). The key optimization is *cross-step KV reuse*: the new
step's input often shares a long prefix with the previous step's output
(system prompt + chat history + tool results so far), so only the delta
needs to be prefilled.

The ``advance`` method combines:

1. Longest-prefix match against the RadixTree (zero-cost).
2. Delta prefill on the unmatched suffix.
3. Optional selective recomputation when the delta diverges heavily.

The step chain is indexed by ``workflow_id``; TTL on each step is
refreshed to keep the chain alive for the expected number of steps.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Sequence

import torch

from ...core.state import (
    MultimodalState,
    StateLayer,
    StateMeta,
)


@dataclass
class WorkflowStep:
    """One step in an agent workflow."""

    step_index: int
    state: MultimodalState
    added_tokens: List[int] = field(default_factory=list)
    added_images: List[str] = field(default_factory=list)
    prefill_time_ms: float = 0.0
    reuse_ratio: float = 0.0    # fraction of KV matched from predecessor
    reused_tokens: int = 0
    total_tokens: int = 0
    delta_tokens: int = 0
    approximate: bool = False
    relay_stats: dict[str, Any] = field(default_factory=dict)


class Workflow:
    """An agent's multi-step reasoning chain with cross-step KV reuse.

    ``prefill_fn`` is a callable:

        prefill_fn(token_ids, attention_mask=None, pixel_values=None,
                   image_grid_thw=None, prefix_kv_refs=None)
        -> (kv_refs: list[BlockRef], first_logits: torch.Tensor)

    that runs the LLM forward over the given inputs (optionally seeded
    with a prefix KV cache from the RadixTree match). The workflow
    orchestrates prefix lookup, delta computation, and state commit.
    """

    def __init__(
        self,
        workflow_id: Optional[str] = None,
        agent_id: str = "",
        state_layer: Optional[StateLayer] = None,
        prefill_fn: Optional[Callable] = None,
        default_ttl_seconds: float = 3600.0,
        enable_relay: bool = False,
        relay_min_match_run: int = 4,
        enable_tier3: bool = False,
        attention_threshold: float = 0.90,
    ):
        self.workflow_id = workflow_id or uuid.uuid4().hex
        self.agent_id = agent_id
        self.sl = state_layer
        self.prefill_fn = prefill_fn
        self.default_ttl_seconds = default_ttl_seconds
        self.enable_relay = enable_relay
        self.relay_min_match_run = relay_min_match_run
        self.enable_tier3 = enable_tier3
        self.attention_threshold = attention_threshold
        self.steps: List[WorkflowStep] = []
        self._reuse_pipeline = None

    # ------------------------------------------------------------------
    @property
    def current(self) -> Optional[WorkflowStep]:
        return self.steps[-1] if self.steps else None

    @staticmethod
    def _build_reuse_telemetry(
        *,
        matched_pages,
        new_tokens: Sequence[int],
        total_tokens: int,
        reused_tokens: int,
        reuse_ratio: float,
        approximate: bool,
        prefill_ms: float,
        relay_stats: Optional[dict[str, Any]],
        cross_step: bool,
        tier1_matched_tokens: Optional[int] = None,
    ) -> dict[str, Any]:
        delta_tokens = list(new_tokens)
        relay = dict(relay_stats or {})
        delta_prefill_calls = int(relay.get("prefill_calls", 0) or 0)
        delta_prefill_tokens = int(relay.get("prefill_tokens", 0) or 0)
        if not approximate and delta_tokens:
            delta_prefill_calls = max(1, delta_prefill_calls)
            delta_prefill_tokens = max(len(delta_tokens), delta_prefill_tokens)
        return {
            "cross_step": bool(cross_step),
            "approximate": bool(approximate),
            "reuse_ratio": float(reuse_ratio),
            "reused_tokens": int(reused_tokens),
            "total_tokens": int(total_tokens),
            "delta_tokens": int(len(delta_tokens)),
            "tier1_matched_tokens": int(
                tier1_matched_tokens
                if tier1_matched_tokens is not None
                else sum(ref.filled for ref in matched_pages)
            ),
            "tier2_reused_tokens": int(relay.get("reused_tokens", 0) or 0),
            "tier2_recomputed_tokens": int(relay.get("recomputed_tokens", 0) or 0),
            "tier3_candidate_pages": int(relay.get("tier3_candidate_pages", 0) or 0),
            "tier3_accepted_pages": int(relay.get("tier3_accepted_pages", 0) or 0),
            "tier3_rejected_pages": int(relay.get("tier3_rejected_pages", 0) or 0),
            "tier3_reused_tokens": int(relay.get("tier3_reused_tokens", 0) or 0),
            "tier3_mean_similarity": float(relay.get("tier3_mean_similarity", 0.0) or 0.0),
            "relay_segments": int(relay.get("segments", 0) or 0),
            "relay_reusable_segments": int(relay.get("reusable_segments", 0) or 0),
            "relay_recompute_segments": int(relay.get("recompute_segments", 0) or 0),
            "relay_substring_hits": int(relay.get("substring_hits", 0) or 0),
            "relay_substring_misses": int(relay.get("substring_misses", 0) or 0),
            "prefill_time_ms": float(prefill_ms),
            "delta_prefill_ms": float(relay.get("prefill_ms", prefill_ms) or prefill_ms),
            "delta_prefill_calls": delta_prefill_calls,
            "delta_prefill_tokens": delta_prefill_tokens,
            "relay_stats": relay,
        }

    # ------------------------------------------------------------------
    def advance(
        self,
        new_token_ids: Sequence[int],
        new_pixel_values: Optional[torch.Tensor] = None,
        new_image_grid_thw: Optional[torch.Tensor] = None,
        new_image_hashes: Optional[Sequence[str]] = None,
        divergence_threshold: float = 0.6,
    ) -> WorkflowStep:
        """Run the next step, reusing KV from the predecessor where possible."""
        prev_state = self.current.state if self.current else None
        new_tokens = list(new_token_ids)
        new_hashes = list(new_image_hashes or [])
        full_tokens_for_state = (
            list(prev_state.meta.token_ids) + new_tokens
            if prev_state is not None
            else list(new_tokens)
        )
        matched_pages = []
        delta_tokens = list(new_tokens)
        total_tokens = len(full_tokens_for_state)
        reused_tokens = 0
        reuse_ratio = 0.0
        approximate = False
        relay_stats = None
        tier1_matched_tokens = 0

        if (
            self.enable_relay
            and self.prefill_fn is not None
            and prev_state is not None
            and self.sl is not None
            and new_tokens
        ):
            from .reuse_pipeline import ReusePipeline

            if (
                self._reuse_pipeline is None
                or abs(self._reuse_pipeline.relay_threshold - divergence_threshold) > 1e-9
                or bool(self._reuse_pipeline.enable_tier3) != bool(self.enable_tier3)
            ):
                self._reuse_pipeline = ReusePipeline(
                    self.sl,
                    self.prefill_fn,
                    relay_threshold=divergence_threshold,
                    attention_threshold=self.attention_threshold,
                    enable_tier2=True,
                    enable_tier3=self.enable_tier3,
                )
            new_kv_refs, first_logits, pipeline_stats = self._reuse_pipeline.run(
                new_tokens=new_tokens,
                prev_tokens=prev_state.meta.token_ids,
                prev_refs=prev_state.kv_refs,
                workflow_id=self.workflow_id,
                new_pixel_values=new_pixel_values,
                new_image_grid_thw=new_image_grid_thw,
            )
            relay_stats = {
                **dict(vars(pipeline_stats)),
                "reused_tokens": int(pipeline_stats.reused_tokens),
                "reuse_ratio": float(pipeline_stats.reuse_ratio),
                "delta_reuse_ratio": float(pipeline_stats.delta_reuse_ratio),
                "delta_recompute_ratio": float(pipeline_stats.delta_recompute_ratio),
            }
            tier1_matched_tokens = int(pipeline_stats.tier1_matched_tokens)
            reused_tokens = int(pipeline_stats.reused_tokens)
            reuse_ratio = float(pipeline_stats.reuse_ratio)
            prefill_ms = float(pipeline_stats.elapsed_ms)
            approximate = bool(
                pipeline_stats.tier2_reused_tokens
                or pipeline_stats.tier3_reused_tokens
                or pipeline_stats.tier3_accepted_pages
            )
            matched_pages = []
            delta_tokens = list(new_tokens)
        else:
            # 1) Prefix match (RFC §6.5.1 -- exact)
            if prev_state is not None and self.sl is not None:
                matched_pages, delta_tokens = self.sl.borrow_prefix_refs(
                    full_tokens_for_state,
                    workflow_id=self.workflow_id,
                )
            tier1_matched_tokens = int(sum(r.filled for r in matched_pages))
            reused_tokens = tier1_matched_tokens
            reuse_ratio = (reused_tokens / total_tokens) if total_tokens else 0.0
            # 2) Run prefill over the delta
            t0 = time.perf_counter()
            if self.prefill_fn is not None:
                prefix_refs = matched_pages if matched_pages else None
                new_kv_refs, first_logits = self.prefill_fn(
                    delta_tokens,
                    prefix_kv_refs=prefix_refs,
                    pixel_values=new_pixel_values,
                    image_grid_thw=new_image_grid_thw,
                )
            else:
                new_kv_refs = []
                first_logits = None
            prefill_ms = (time.perf_counter() - t0) * 1000

        reuse_telemetry = self._build_reuse_telemetry(
            matched_pages=matched_pages,
            new_tokens=new_tokens,
            total_tokens=total_tokens,
            reused_tokens=reused_tokens,
            reuse_ratio=reuse_ratio,
            approximate=approximate,
            prefill_ms=prefill_ms,
            relay_stats=relay_stats,
            cross_step=prev_state is not None,
            tier1_matched_tokens=tier1_matched_tokens,
        )
        committed_refs = self.sl._normalize_committed_refs(
            matched_pages=matched_pages,
            committed_refs=new_kv_refs,
        )

        # 3) Commit new state
        meta = StateMeta(
            token_ids=(list(prev_state.meta.token_ids) + new_tokens)
            if prev_state else new_tokens,
            image_ids=(list(prev_state.meta.image_ids) + new_hashes)
            if prev_state else new_hashes,
            agent_id=self.agent_id,
            workflow_id=self.workflow_id,
            step=len(self.steps),
            turn=prev_state.meta.turn if prev_state else 0,
            ttl_deadline=time.monotonic() + self.default_ttl_seconds,
        )
        new_state = self.sl.register(
            kv_refs=committed_refs,
            feature_hash=new_hashes[-1] if new_hashes else (
                prev_state.feature_hash if prev_state else None
            ),
            meta=meta,
            register_in_radix=False,  # we insert manually below
            approximate=approximate,
            reuse_telemetry=reuse_telemetry,
        )
        # Register the full committed page path. ``RadixTree.insert`` is
        # idempotent on already-existing exact prefixes, so this preserves
        # the old fast append case while also correctly materializing new
        # workflow paths that include reused pages after a divergent middle
        # segment.
        self.sl._insert_pagewise(
            meta.token_ids,
            committed_refs,
            scope=self.workflow_id,
        )
        if prev_state is not None:
            self.sl.refresh_ttl(prev_state)

        step = WorkflowStep(
            step_index=len(self.steps),
            state=new_state,
            added_tokens=new_tokens,
            added_images=new_hashes,
            prefill_time_ms=prefill_ms,
            reuse_ratio=reuse_ratio,
            reused_tokens=int(reused_tokens),
            total_tokens=int(total_tokens),
            delta_tokens=int(len(delta_tokens)),
            approximate=approximate,
            relay_stats=dict(relay_stats or {}),
        )
        self.steps.append(step)
        return step

    # ------------------------------------------------------------------
    def reuse_stats(self) -> dict:
        if not self.steps:
            return {}
        total_added = sum(len(s.added_tokens) for s in self.steps[1:]) or 1
        total_reused = sum(int(s.reused_tokens) for s in self.steps[1:])
        return {
            "steps": len(self.steps),
            "avg_reuse_ratio": sum(s.reuse_ratio for s in self.steps[1:])
            / max(1, len(self.steps) - 1),
            "total_tokens_added": total_added,
            "total_tokens_reused_from_prefix": total_reused,
            "total_context_tokens": sum(int(s.total_tokens) for s in self.steps[1:]),
            "approximate_steps": sum(1 for s in self.steps[1:] if s.approximate),
        }

    def release(self) -> None:
        """Release all step states."""
        if self.sl is None:
            return
        for s in self.steps:
            self.sl.release(s.state)
        self.steps.clear()
