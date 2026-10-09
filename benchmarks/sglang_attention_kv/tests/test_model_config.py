import json

import pytest

from benchmarks.sglang_attention_kv.model_config import (
    canonical_torch_dtype,
    classify_attention_layout,
    load_model_kv_config,
)

DENSE_BODY = {
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


def body_with(**overrides):
    body = dict(DENSE_BODY)
    body.update(overrides)
    return body


def test_a_dense_model_is_accepted():
    layout, evidence = classify_attention_layout(body_with())
    assert layout == "dense"
    assert evidence == "every layer keeps a full-length KV"


def test_a_model_that_declares_a_window_without_disabling_it_is_hybrid():
    body = body_with(sliding_window=1024)
    body.pop("use_sliding_window")
    layout, evidence = classify_attention_layout(body)
    assert layout == "hybrid"
    assert "sliding_window=1024" in evidence


def test_a_window_that_is_explicitly_disabled_stays_dense():
    layout, _ = classify_attention_layout(
        body_with(sliding_window=1024, use_sliding_window=False)
    )
    assert layout == "dense"


def test_layer_types_with_one_sparse_layer_is_hybrid():
    layout, evidence = classify_attention_layout(
        body_with(
            layer_types=["sliding_attention", "full_attention"] * 18,
            sliding_window=1024,
        )
    )
    assert layout == "hybrid"
    assert "sliding_attention" in evidence


def test_layer_types_that_are_all_full_attention_stay_dense():
    layout, _ = classify_attention_layout(
        body_with(layer_types=["full_attention"] * 36)
    )
    assert layout == "dense"


def test_an_interleaved_linear_attention_model_is_hybrid():
    layout, evidence = classify_attention_layout(
        body_with(full_attention_interval=4, linear_attn_config={"k": 1})
    )
    assert layout == "hybrid"
    assert "full_attention_interval=4" in evidence


def test_an_encoder_decoder_model_is_hybrid():
    layout, evidence = classify_attention_layout(body_with(is_encoder_decoder=True))
    assert layout == "hybrid"
    assert "second cache" in evidence


def test_an_mla_model_is_classified_as_mla():
    layout, evidence = classify_attention_layout(
        body_with(kv_lora_rank=512, qk_rope_head_dim=64)
    )
    assert layout == "mla"
    assert "kv_lora_rank" in evidence


def test_a_non_dense_model_is_refused_with_its_evidence(tmp_path):
    path = write_config(
        tmp_path, body_with(layer_types=["sliding_attention"] * 36, sliding_window=512)
    )
    config = load_model_kv_config(path)
    assert config.attention_layout == "hybrid"
    with pytest.raises(ValueError, match="attention layout is 'hybrid'"):
        config.kv_bytes_per_token(1)


def test_kv_bytes_per_token_is_aggregate_and_per_rank(tmp_path):
    config = load_model_kv_config(write_config(tmp_path, body_with()))
    aggregate, per_rank = config.kv_bytes_per_token(4)
    assert aggregate == 36 * 2 * 8 * 128 * 2
    assert per_rank == aggregate // 4


def test_kv_bytes_per_page_multiplies_the_page_size(tmp_path):
    config = load_model_kv_config(write_config(tmp_path, body_with()))
    aggregate, per_rank = config.kv_bytes_per_page(64, 4)
    assert aggregate == 36 * 2 * 8 * 128 * 2 * 64
    assert per_rank == aggregate // 4


def test_an_indivisible_tensor_parallel_size_is_refused(tmp_path):
    config = load_model_kv_config(write_config(tmp_path, body_with()))
    with pytest.raises(ValueError, match="is not divisible by tp_size=64"):
        config.kv_bytes_per_token(64)


def test_head_dim_is_derived_when_the_config_omits_it(tmp_path):
    body = body_with()
    body.pop("head_dim")
    config = load_model_kv_config(write_config(tmp_path, body))
    assert config.head_dim == 4096 // 32
    assert "hidden_size // num_attention_heads" in config.config_source


def test_a_nested_language_config_is_used(tmp_path):
    nested = {key: value for key, value in DENSE_BODY.items() if key != "vocab_size"}
    nested["vocab_size"] = 151936
    body = {
        "architectures": ["SomeMultiModalForConditionalGeneration"],
        "text_config": nested,
        "vocab_size": 151936,
    }
    config = load_model_kv_config(write_config(tmp_path, body))
    assert config.num_layers == 36
    assert "text_config" in config.config_source


def test_a_config_without_a_depth_is_refused(tmp_path):
    path = write_config(tmp_path, {"num_attention_heads": 32})
    with pytest.raises(ValueError, match="no num_hidden_layers"):
        load_model_kv_config(path)


def test_a_missing_config_file_is_refused(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_model_kv_config(str(tmp_path / "absent"))


def test_dtype_spellings_are_normalised():
    assert canonical_torch_dtype("bf16") == "bfloat16"
    assert canonical_torch_dtype("FP16") == "float16"
    with pytest.raises(ValueError, match="unrecognised dtype"):
        canonical_torch_dtype("int4")


def test_a_model_that_lists_only_some_full_attention_layers_is_refused():
    """full_attn_idxs names the layers that attend fully, so the others are not
    attention or do not keep a full-length KV. LFM2 is written this way."""
    layout, evidence = classify_attention_layout({"full_attn_idxs": [0, 7, 15]})
    assert layout == "hybrid"
    assert "full_attn_idxs" in evidence


def test_a_model_whose_blocks_are_not_all_attention_is_refused():
    """layers_block_type mixes attention blocks with mamba or linear ones;
    NemotronH is written this way."""
    layout, evidence = classify_attention_layout(
        {"layers_block_type": ["mamba", "attention", "mamba"]}
    )
    assert layout == "hybrid"
    assert "layers_block_type" in evidence


def test_a_model_with_a_chunked_attention_window_is_refused():
    """attention_chunk_size bounds how far a layer's attention reaches, which is a
    window rather than the full context. Llama 4 carries it."""
    layout, evidence = classify_attention_layout({"attention_chunk_size": 8192})
    assert layout == "hybrid"
    assert "attention_chunk_size" in evidence


def test_a_dense_config_is_still_accepted():
    layout, evidence = classify_attention_layout(dict(DENSE_BODY))
    assert layout == "dense"
    assert evidence
