from __future__ import annotations

import pytest

pytest.importorskip("vllm")

import sitecustomize  # noqa: E402

sitecustomize._patch_vllm_prompt_only_prefill()

from vllm.sampling_params import SamplingParams  # noqa: E402


def test_sampling_params_accepts_prompt_only_prefill_budget_zero():
    params = SamplingParams(max_tokens=0, min_tokens=0, temperature=0.0, top_p=1.0)
    assert params.max_tokens == 0
    assert params.min_tokens == 0


def test_sampling_params_rejects_min_tokens_for_prompt_only_prefill():
    with pytest.raises(ValueError):
        SamplingParams(max_tokens=0, min_tokens=1, temperature=0.0, top_p=1.0)


def test_prompt_only_patch_removes_only_internal_sampled_output():
    output_type = type("Output", (), {})
    sampled = output_type()
    sampled.request_id = "prompt-only"
    sampled.new_token_ids = [42]
    terminal = output_type()
    terminal.request_id = "prompt-only"
    terminal.new_token_ids = []
    other = output_type()
    other.request_id = "decode-request"
    other.new_token_ids = [7]
    outputs = [sampled, terminal, other]

    removed = sitecustomize._remove_prompt_only_sample_outputs(
        outputs,
        "prompt-only",
    )

    assert removed == [sampled]
    assert outputs == [terminal, other]
    assert getattr(SamplingParams, "_mooncake_epd_prompt_only_protocol_version") == 2
