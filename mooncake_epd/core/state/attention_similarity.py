"""RKSC-style attention-similarity KV reuse (RFC §6.5.1 tier 3).

When a new step's attention *pattern* (per-head softmax weights) is highly
similar to a previous step's pattern on the same token segment, we can
reuse the previous step's KV for that segment with bounded error. RKSC
adds a *confidence gate*: if the cosine similarity between attention
distributions is below a threshold, the reuse is rejected and the caller
falls back to a recompute.

In this implementation we approximate "attention pattern" with the L2
norm profile of the key tensor along the sequence dimension. This is a
cheap, training-free proxy: when two key tensors have similar norms per
token, their softmax attention distributions under the same query are
also similar (see RKSC Section 3.2 for the theoretical bound).
"""

from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

import torch

from .page_manager import BlockRef, PagedKVManager


def _key_norm_profile(key: torch.Tensor) -> torch.Tensor:
    """Compute per-token L2 norm along the sequence dimension.

    key shape: (num_layers, num_kv_heads, seq_len, head_dim) or
               (num_kv_heads, seq_len, head_dim).
    Returns: (seq_len,) 1-D tensor of averaged per-token norms.
    """
    if key.dim() == 4:
        # Average over layers and heads to get a (seq_len,) profile
        return key.float().norm(dim=-1).mean(dim=(0, 1))
    if key.dim() == 3:
        return key.float().norm(dim=-1).mean(dim=0)
    raise ValueError(f"unexpected key shape: {tuple(key.shape)}")


def attention_similarity(a: torch.Tensor, b: torch.Tensor) -> float:
    """Cosine similarity between two per-token norm profiles."""
    if a.dim() in (3, 4):
        a = _key_norm_profile(a)
    if b.dim() in (3, 4):
        b = _key_norm_profile(b)
    if a.shape != b.shape:
        n = min(a.shape[-1], b.shape[-1])
        a = a[..., :n]
        b = b[..., :n]
    a = a.float().reshape(-1)
    b = b.float().reshape(-1)
    denom = (a.norm() * b.norm()).clamp_min(1e-8)
    return float(torch.dot(a, b) / denom)


class AttentionSimilarityReuse:
    """Tier-3 fallback: reuse KV when attention patterns are similar.

    For each candidate page in ``prev_refs`` whose token segment overlaps
    the new step's input, compute the key-norm-profile similarity between
    the new segment and the previous segment. If similarity >= threshold,
    reuse the page (refcount +1); otherwise mark it as ``rejected`` so the
    caller re-computes it.

    Returns ``(accepted_refs, rejected_segments, similarity_stats)``.
    """

    def __init__(
        self,
        page_manager: PagedKVManager,
        threshold: float = 0.90,
    ):
        self.pm = page_manager
        self.threshold = threshold

    def evaluate_page(
        self,
        prev_ref: BlockRef,
        new_key: torch.Tensor,
    ) -> Tuple[Optional[BlockRef], float]:
        """Compare new_key against the previous page's key profile.

        Returns (accepted_ref_or_None, similarity_score).
        """
        prev_key, _ = self.pm.get_page_slice(prev_ref)
        # Align sequence lengths
        n = min(prev_key.shape[-2], new_key.shape[-2])
        if n == 0:
            return None, 0.0
        sim = attention_similarity(
            prev_key[:, :, :n, :], new_key[:, :, :n, :]
        )
        if sim >= self.threshold:
            self.pm.incref(prev_ref.physical_id)
            return BlockRef(
                physical_id=prev_ref.physical_id,
                filled=min(int(prev_ref.filled), int(new_key.shape[-2])),
                global_block_id=prev_ref.global_block_id or f"local:{prev_ref.physical_id}",
                physical_node_id=prev_ref.physical_node_id,
                logical_index=prev_ref.logical_index,
                virtual_offset=prev_ref.virtual_offset,
            ), sim
        return None, sim

    def evaluate_sequence(
        self,
        prev_refs: Sequence[BlockRef],
        new_keys: Sequence[torch.Tensor],
    ) -> Tuple[List[BlockRef], List[int], dict]:
        """Evaluate a sequence of pages; return accepted refs + rejected idxs."""
        accepted: List[BlockRef] = []
        rejected: List[int] = []
        sims: List[float] = []
        for i, (pr, nk) in enumerate(zip(prev_refs, new_keys)):
            ref, sim = self.evaluate_page(pr, nk)
            sims.append(sim)
            if ref is not None:
                accepted.append(ref)
            else:
                rejected.append(i)
        stats = {
            "n_pages": len(prev_refs),
            "n_accepted": len(accepted),
            "n_rejected": len(rejected),
            "mean_similarity": float(sum(sims) / max(1, len(sims))),
            "threshold": self.threshold,
        }
        return accepted, rejected, stats
