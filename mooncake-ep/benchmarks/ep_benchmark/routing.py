"""
Expert-parallel routing generators for the EP benchmark.

"""

import torch


def generate_topk_weights(
    num_tokens: int, top_k: int, device: torch.device, generator: torch.Generator
) -> torch.Tensor:
    """Generate softmax-normalized weights."""
    raw = torch.rand(
        num_tokens, top_k, dtype=torch.float32, device=device, generator=generator
    )
    return torch.softmax(raw, dim=-1)


def uniform_routing(
    num_tokens: int,
    num_experts: int,
    top_k: int,
    device: torch.device | None = None,
    seed: int = 0,
    **kwargs,
) -> tuple[torch.Tensor, torch.Tensor]:
    """
    Uniform routing: each token picks top-k from the full expert set with
    approximately uniform probability.
    """
    if device is None:
        device = torch.device("cpu")
    generator = torch.Generator(device=device).manual_seed(seed)
    scores = torch.randn(
        num_tokens,
        num_experts,
        dtype=torch.float32,
        device=device,
        generator=generator,
    )
    topk_idx = torch.topk(scores, top_k, dim=-1)[1].to(torch.int64)
    topk_weights = generate_topk_weights(num_tokens, top_k, device, generator)
    return topk_idx, topk_weights


def k_hot_routing(
    num_tokens: int,
    num_experts: int,
    top_k: int,
    device: torch.device | None = None,
    seed: int = 0,
    hot_experts: int = 4,
    hot_fraction: float = 0.9,
    **kwargs,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Incast / k-hot routing: *hot_fraction* of tokens pick ALL top-k from
    the first *hot_experts* set; the rest route uniformly.

    Raises:
        ValueError: if hot_experts < top_k (cannot pick top_k distinct experts
                    from a set smaller than top_k).
        ValueError: if hot_experts > num_experts.
        ValueError: if hot_fraction not in (0, 1).
    """
    if device is None:
        device = torch.device("cpu")
    if hot_experts > num_experts:
        raise ValueError(f"hot_experts ({hot_experts}) > num_experts ({num_experts})")
    if hot_experts < top_k:
        raise ValueError(
            f"hot_experts ({hot_experts}) < top_k ({top_k}); "
            f"cannot pick {top_k} distinct experts from {hot_experts} hot experts"
        )
    if not (0.0 < hot_fraction < 1.0):
        raise ValueError(f"hot_fraction must be in (0, 1), got {hot_fraction}")

    generator = torch.Generator(device=device).manual_seed(seed)

    num_hot = int(num_tokens * hot_fraction)
    num_cold = num_tokens - num_hot

    hot_scores = torch.randn(
        num_hot, hot_experts, dtype=torch.float32, device=device, generator=generator
    )
    hot_topk_idx = torch.topk(hot_scores, top_k, dim=-1)[1].to(torch.int64)

    cold_scores = torch.randn(
        num_cold,
        num_experts,
        dtype=torch.float32,
        device=device,
        generator=generator,
    )
    cold_topk_idx = torch.topk(cold_scores, top_k, dim=-1)[1].to(torch.int64)

    topk_idx = torch.cat([hot_topk_idx, cold_topk_idx], dim=0)
    perm = torch.randperm(num_tokens, generator=generator, device=device)
    topk_idx = topk_idx[perm]

    topk_weights = generate_topk_weights(num_tokens, top_k, device, generator)
    return topk_idx, topk_weights


def zipfian_routing(
    num_tokens: int,
    num_experts: int,
    top_k: int,
    device: torch.device | None = None,
    seed: int = 0,
    zipf_alpha: float = 1.0,
    **kwargs,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Zipfian routing: expert selection probability follows Zipf(alpha).

    P(expert_i) proportional to 1 / (i + 1)^alpha.

    Uses the Gumbel-max trick for top-k sampling without replacement:
    for each token, add Gumbel(0,1) noise to log-probabilities, then take top-k.
    """
    if device is None:
        device = torch.device("cpu")
    if zipf_alpha <= 0:
        raise ValueError(f"zipf_alpha must be > 0, got {zipf_alpha}")

    generator = torch.Generator(device=device).manual_seed(seed)

    expert_ids = torch.arange(1, num_experts + 1, dtype=torch.float32, device=device)
    log_probs = -zipf_alpha * torch.log(expert_ids)
    log_probs = log_probs - log_probs.logsumexp(dim=0)

    gumbel = -torch.log(
        -torch.log(
            torch.rand(
                num_tokens,
                num_experts,
                dtype=torch.float32,
                device=device,
                generator=generator,
            ).clamp(min=1e-20)
        )
    )
    noisy_scores = log_probs.unsqueeze(0) + gumbel
    topk_idx = torch.topk(noisy_scores, top_k, dim=-1)[1].to(torch.int64)

    topk_weights = generate_topk_weights(num_tokens, top_k, device, generator)
    return topk_idx, topk_weights


ROUTING_MODES = {
    "uniform": uniform_routing,
    "k_hot": k_hot_routing,
    "zipf": zipfian_routing,
}
