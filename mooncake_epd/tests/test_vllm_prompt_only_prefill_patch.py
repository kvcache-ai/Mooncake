from __future__ import annotations

import pytest

pytest.importorskip("vllm")

from vllm.sampling_params import SamplingParams  # noqa: E402


def test_sampling_params_accepts_prompt_only_prefill_budget_zero():
    params = SamplingParams(max_tokens=0, min_tokens=0, temperature=0.0, top_p=1.0)
    assert params.max_tokens == 0
    assert params.min_tokens == 0


def test_sampling_params_rejects_min_tokens_for_prompt_only_prefill():
    with pytest.raises(ValueError):
        SamplingParams(max_tokens=0, min_tokens=1, temperature=0.0, top_p=1.0)
