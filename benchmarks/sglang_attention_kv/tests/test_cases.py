import json

import pytest

from benchmarks.sglang_attention_kv.cases import (
    KernelCase,
    StepShape,
    build_cases,
    build_shapes,
    ragged_lengths,
    resolve_extend_branch,
    short_case,
)
from benchmarks.sglang_attention_kv.model_config import load_model_kv_config

QWEN3_LIKE = {
    "architectures": ["Qwen3ForCausalLM"],
    "model_type": "qwen3",
    "num_hidden_layers": 36,
    "num_attention_heads": 32,
    "num_key_value_heads": 8,
    "head_dim": 128,
    "hidden_size": 4096,
    "sliding_window": None,
    "use_sliding_window": False,
    "torch_dtype": "bfloat16",
    "vocab_size": 151936,
}


def write_config(tmp_path, body):
    path = tmp_path / "model"
    path.mkdir()
    (path / "config.json").write_text(json.dumps(body), encoding="utf-8")
    return str(path)


def model_config(tmp_path, **overrides):
    body = dict(QWEN3_LIKE)
    body.update(overrides)
    return load_model_kv_config(write_config(tmp_path, body))


def case(
    mode="extend",
    prefix_lens=(512,),
    new_lens=(128,),
    page_size=64,
    layout="random",
    num_qo_heads=8,
    num_kv_heads=2,
):
    return KernelCase(
        mode=mode,
        prefix_lens=prefix_lens,
        new_lens=new_lens,
        page_size=page_size,
        layout=layout,
        num_layers=36,
        num_qo_heads=num_qo_heads,
        num_kv_heads=num_kv_heads,
        head_dim=128,
        dtype="bfloat16",
    )


def test_ledger_counts_written_bytes_and_page_capacity():
    one = case()
    # 128 queries written, and the step's 640 tokens covered by 10 whole pages
    assert one.new_tokens == 128
    assert one.context_tokens == 640
    assert one.pages == 10
    assert one.padding_tokens == 0
    per_token = 36 * 2 * 2 * 128 * 2
    assert one.kv_bytes_written() == 128 * per_token
    assert one.kv_page_capacity_bytes() == 640 * per_token


def test_padding_is_counted_in_the_page_capacity():
    one = case(prefix_lens=(500,), new_lens=(100,))
    assert one.context_tokens == 600
    assert one.pages == 10  # 600 tokens over 64-token pages
    assert one.padding_tokens == 40
    assert one.kv_page_capacity_bytes() == one.kv_bytes(10 * 64)
    assert one.kv_page_capacity_bytes() > one.kv_bytes(one.context_tokens)


def test_attention_pairs_follow_the_causal_mask():
    # one query against a 512-token history costs 512 pairs, plus its own
    assert case(prefix_lens=(512,), new_lens=(1,)).attention_pairs == 513
    # a prefill of 128 tokens costs 128 * 129 / 2 pairs
    assert case(mode="prefill", prefix_lens=(0,), new_lens=(128,)).attention_pairs == (
        128 * 129 // 2
    )
    # a chunk of 128 behind a 512-token history costs 128 * 512 plus its own causal half
    assert case().attention_pairs == 128 * 512 + 128 * 129 // 2


def test_flops_scale_with_the_layer_count():
    one = case()
    per_layer = 4 * one.attention_pairs * one.num_qo_heads * one.head_dim
    assert one.attention_flops() == per_layer * one.num_layers


def test_labels_are_unique_across_a_matrix():
    shapes = build_shapes(
        modes=("prefill", "extend", "decode"),
        input_lens=(128, 512),
        chunk_lens=(64, 128),
        batch_sizes=(1, 4),
        page_sizes=(1, 64),
        layouts=("contiguous", "random"),
    )
    labels = [shape.label for shape in shapes]
    assert len(labels) == len(set(labels))


def test_query_lengths_are_an_axis_of_their_own():
    shapes = build_shapes(
        modes=("extend",),
        input_lens=(512,),
        chunk_lens=(128, 2048),
        batch_sizes=(1,),
        page_sizes=(64,),
        layouts=("contiguous",),
    )
    queries = sorted(shape.new_tokens for shape in shapes)
    assert queries == [128, 2048]
    assert all(shape.prefix_lens == (512,) for shape in shapes)


def test_a_chunk_longer_than_the_history_is_still_a_step():
    shape = StepShape(
        mode="extend",
        prefix_lens=(128,),
        new_lens=(512,),
        page_size=64,
        layout="random",
    )
    assert shape.context_lens == (640,)
    assert shape.attention_pairs == 512 * 128 + 512 * 513 // 2


def test_ragged_lengths_shrink_by_half_each_sequence():
    assert ragged_lengths(8192, 4) == (8192, 4096, 2048, 1024)
    assert ragged_lengths(16, 4) == (16, 8, 4, 2)
    assert ragged_lengths(1, 4) == (1, 1, 1, 1)


def test_invalid_shapes_are_rejected():
    with pytest.raises(ValueError, match="prefill step has no cached prefix"):
        case(mode="prefill", prefix_lens=(8,), new_lens=(128,))
    with pytest.raises(ValueError, match="decode step computes exactly one token"):
        case(mode="decode", prefix_lens=(512,), new_lens=(2,))
    with pytest.raises(ValueError, match="one entry per sequence"):
        StepShape(
            mode="extend",
            prefix_lens=(512,),
            new_lens=(128, 128),
            page_size=64,
            layout="random",
        )
    with pytest.raises(ValueError, match="unknown mode"):
        StepShape(
            mode="chunk", prefix_lens=(0,), new_lens=(8,), page_size=64, layout="random"
        )
    with pytest.raises(ValueError, match="unknown layout"):
        StepShape(
            mode="decode",
            prefix_lens=(512,),
            new_lens=(1,),
            page_size=64,
            layout="shuffled",
        )


def test_short_case_keeps_the_mode_and_caps_the_lengths(tmp_path):
    shapes = build_shapes(
        modes=("prefill", "extend", "decode"),
        input_lens=(8192,),
        chunk_lens=(2048,),
        batch_sizes=(1,),
        page_sizes=(64,),
        layouts=("random",),
    )
    bound = build_cases(shapes, model_config(tmp_path), tp_size=4)
    assert len(bound) == 3
    for one in bound:
        short = short_case(one)
        assert short.mode == one.mode
        assert short.page_size == one.page_size
        assert short.layout == one.layout
        assert short.num_layers == 1
        # the history cap plus the chunk cap, which is what the reference has to fit
        assert max(short.context_lens) <= 256 + 64


def test_build_cases_binds_the_model_shapes(tmp_path):
    config = model_config(tmp_path)
    shapes = build_shapes(
        modes=("extend",),
        input_lens=(512,),
        chunk_lens=(128,),
        batch_sizes=(1,),
        page_sizes=(64,),
        layouts=("random",),
    )
    cases = build_cases(shapes, config, tp_size=4)
    assert len(cases) == 1
    assert cases[0].num_layers == 36
    assert cases[0].num_qo_heads == 8  # 32 query heads over 4 ranks
    assert cases[0].num_kv_heads == 2  # 8 KV heads over 4 ranks
    assert cases[0].dtype == "bfloat16"


def test_build_cases_rejects_a_sharding_that_does_not_divide(tmp_path):
    config = model_config(tmp_path, num_key_value_heads=6, num_attention_heads=32)
    shapes = build_shapes(
        modes=("decode",),
        input_lens=(128,),
        chunk_lens=(1,),
        batch_sizes=(1,),
        page_sizes=(64,),
        layouts=("contiguous",),
    )
    with pytest.raises(ValueError, match="num_key_value_heads=6 is not divisible"):
        build_cases(shapes, config, tp_size=4)


def test_build_cases_rejects_a_dtype_the_kernels_cannot_run(tmp_path):
    config = model_config(tmp_path, torch_dtype="float8_e4m3fn")
    shapes = build_shapes(
        modes=("decode",),
        input_lens=(128,),
        chunk_lens=(1,),
        batch_sizes=(1,),
        page_sizes=(64,),
        layouts=("contiguous",),
    )
    with pytest.raises(ValueError, match="paged attention kernels"):
        build_cases(shapes, config, tp_size=4)


def test_a_case_refuses_a_head_count_that_is_not_a_whole_group():
    """The reference attention reads each KV head num_qo_heads // num_kv_heads
    times, so a ratio that is not whole would truncate query heads silently."""
    with pytest.raises(ValueError, match="not a whole number of groups"):
        case(num_qo_heads=8, num_kv_heads=3)
    assert case(num_qo_heads=8, num_kv_heads=8).group_ratio() == 1
    assert case(num_qo_heads=32, num_kv_heads=8).group_ratio() == 4


def test_a_model_whose_ranks_do_not_keep_whole_groups_is_refused(tmp_path):
    """The same check at the model level: 12 query heads over 8 KV heads at TP=1
    is a group ratio of one and a half."""
    config = model_config(tmp_path, num_attention_heads=12, num_key_value_heads=8)
    shapes = build_shapes(
        modes=("decode",),
        input_lens=(128,),
        chunk_lens=(1,),
        batch_sizes=(1,),
        page_sizes=(64,),
        layouts=("contiguous",),
    )
    with pytest.raises(ValueError, match="not a whole group ratio"):
        build_cases(shapes, config, tp_size=1)


def test_the_extend_branch_has_one_source(monkeypatch):
    """SGLANG_FLASHINFER_USE_PAGED is what a server sets; --extend-branch is what a
    run passes. With the variable set it decides, and a conflict is refused rather
    than silently overridden."""
    monkeypatch.delenv("SGLANG_FLASHINFER_USE_PAGED", raising=False)
    assert resolve_extend_branch(None) == "ragged_prefix_merge"
    assert resolve_extend_branch("paged_extend") == "paged_extend"
    monkeypatch.setenv("SGLANG_FLASHINFER_USE_PAGED", "1")
    assert resolve_extend_branch(None) == "paged_extend"
    assert resolve_extend_branch("paged_extend") == "paged_extend"
    with pytest.raises(ValueError, match="disagrees with SGLANG_FLASHINFER_USE_PAGED"):
        resolve_extend_branch("ragged_prefix_merge")
    monkeypatch.setenv("SGLANG_FLASHINFER_USE_PAGED", "0")
    assert resolve_extend_branch(None) == "ragged_prefix_merge"
    with pytest.raises(ValueError, match="disagrees with SGLANG_FLASHINFER_USE_PAGED"):
        resolve_extend_branch("paged_extend")
    with pytest.raises(ValueError, match="unknown extend branch"):
        monkeypatch.delenv("SGLANG_FLASHINFER_USE_PAGED", raising=False)
        resolve_extend_branch("paged_whatever")
