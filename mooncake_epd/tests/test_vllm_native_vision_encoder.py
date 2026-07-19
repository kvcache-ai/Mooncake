from __future__ import annotations

import pytest
import torch

from mooncake_epd.core.vllm_native_vision_encoder import (
    VLLMNativeVisionEncoderError,
    split_qwen3_vllm_packed_visual_output,
)


def test_split_qwen3_vllm_packed_visual_output_preserves_deepstack_order():
    packed = torch.arange(2 * 16, dtype=torch.float32).reshape(2, 16)

    main, deepstack = split_qwen3_vllm_packed_visual_output(
        packed,
        visual_dim=4,
        deepstack_layers=(8, 16, 24),
    )

    assert torch.equal(main, packed[:, :4])
    assert [layer for layer, _ in deepstack] == [8, 16, 24]
    assert torch.equal(deepstack[0][1], packed[:, 4:8])
    assert torch.equal(deepstack[1][1], packed[:, 8:12])
    assert torch.equal(deepstack[2][1], packed[:, 12:16])
    assert main.is_contiguous()
    assert all(tensor.is_contiguous() for _, tensor in deepstack)


@pytest.mark.parametrize(
    "packed, visual_dim, layers",
    [
        (torch.ones(16), 4, (8, 16, 24)),
        (torch.ones((2, 12)), 4, (8, 16, 24)),
    ],
)
def test_split_qwen3_vllm_packed_visual_output_fails_closed_on_abi_mismatch(
    packed: torch.Tensor,
    visual_dim: int,
    layers: tuple[int, ...],
):
    with pytest.raises(VLLMNativeVisionEncoderError):
        split_qwen3_vllm_packed_visual_output(
            packed,
            visual_dim=visual_dim,
            deepstack_layers=layers,
        )
