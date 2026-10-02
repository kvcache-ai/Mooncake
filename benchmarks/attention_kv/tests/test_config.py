import json

import pytest

from benchmarks.attention_kv.config import (
    PageTableSpec,
    build_e2e_configs,
    build_workload_cases,
    load_model_kv_config,
)

QWEN3_8B = {
    "architectures": ["Qwen3ForCausalLM"],
    "head_dim": 128,
    "hidden_size": 4096,
    "model_type": "qwen3",
    "num_attention_heads": 32,
    "num_hidden_layers": 36,
    "num_key_value_heads": 8,
    "torch_dtype": "bfloat16",
    "vocab_size": 151936,
}

MLA_LIKE = {
    "architectures": ["DeepseekV3ForCausalLM"],
    "hidden_size": 7168,
    "kv_lora_rank": 512,
    "qk_rope_head_dim": 64,
    "num_attention_heads": 128,
    "num_hidden_layers": 61,
    "num_key_value_heads": 128,
    "torch_dtype": "bfloat16",
    "vocab_size": 129280,
}


def write_config(tmp_path, body):
    directory = tmp_path / "model"
    directory.mkdir()
    (directory / "config.json").write_text(json.dumps(body), encoding="utf-8")
    return str(directory)


def test_qwen3_8b_kv_bytes_per_token(tmp_path):
    model = load_model_kv_config(write_config(tmp_path, QWEN3_8B))
    assert model.num_layers == 36
    assert model.num_key_value_heads == 8
    assert model.head_dim == 128
    assert model.dtype_bytes == 2
    assert model.is_mla is False

    aggregate, per_rank = model.kv_bytes_per_token(tp_size=1)
    assert aggregate == 36 * 2 * 8 * 128 * 2
    assert aggregate == 147456

    aggregate_tp8, per_rank_tp8 = model.kv_bytes_per_token(tp_size=8)
    assert aggregate_tp8 == 147456
    assert per_rank_tp8 == 147456 // 8 == 18432


def test_kv_bytes_per_page(tmp_path):
    model = load_model_kv_config(write_config(tmp_path, QWEN3_8B))
    aggregate, per_rank = model.kv_bytes_per_page(page_size=64, tp_size=8)
    assert aggregate == 147456 * 64
    assert per_rank == 18432 * 64


def test_tp_size_must_divide_kv_heads(tmp_path):
    model = load_model_kv_config(write_config(tmp_path, QWEN3_8B))
    with pytest.raises(ValueError):
        model.kv_bytes_per_token(tp_size=5)


def test_mla_is_rejected(tmp_path):
    model = load_model_kv_config(write_config(tmp_path, MLA_LIKE))
    assert model.is_mla is True
    with pytest.raises(ValueError):
        model.kv_bytes_per_token(tp_size=1)


def test_head_dim_falls_back_to_hidden_size(tmp_path):
    body = dict(QWEN3_8B)
    del body["head_dim"]
    model = load_model_kv_config(write_config(tmp_path, body))
    assert model.head_dim == 4096 // 32 == 128


def test_dtype_override(tmp_path):
    model = load_model_kv_config(
        write_config(tmp_path, QWEN3_8B), dtype_override="float8_e4m3fn"
    )
    assert model.dtype_bytes == 1
    aggregate, _ = model.kv_bytes_per_token(tp_size=1)
    assert aggregate == 36 * 2 * 8 * 128 * 1


def test_nested_text_config(tmp_path):
    body = {"text_config": dict(QWEN3_8B)}
    body["text_config"]["model_type"] = "gemma4_text"
    model = load_model_kv_config(write_config(tmp_path, body))
    assert model.num_layers == 36
    assert "text_config" in model.config_source


@pytest.mark.parametrize("key", ["text_config", "llm_config", "language_config"])
def test_vocab_size_follows_the_same_nested_lookup(tmp_path, key):
    """The vocabulary read and the KV shape read must land on the same config
    layer, otherwise a default would be used instead."""
    body = {key: dict(QWEN3_8B)}
    body[key]["vocab_size"] = 4242
    model = load_model_kv_config(write_config(tmp_path, body))
    assert model.vocab_size == 4242
    assert model.num_layers == 36


def test_missing_vocab_size_raises(tmp_path):
    body = dict(QWEN3_8B)
    del body["vocab_size"]
    with pytest.raises(ValueError):
        load_model_kv_config(write_config(tmp_path, body))


def test_dtype_aliases_normalise(tmp_path):
    body = dict(QWEN3_8B)
    body["torch_dtype"] = "bf16"
    model = load_model_kv_config(write_config(tmp_path, body))
    assert model.torch_dtype == "bfloat16"
    assert model.dtype_bytes == 2


def test_unknown_dtype_raises(tmp_path):
    body = dict(QWEN3_8B)
    body["torch_dtype"] = "int4_packed"
    with pytest.raises(ValueError):
        load_model_kv_config(write_config(tmp_path, body))


def test_page_table_spec():
    spec = PageTableSpec(seq_len=8192, page_size=64)
    assert spec.num_pages == 128
    assert spec.last_page_len == 64

    odd = PageTableSpec(seq_len=100, page_size=64)
    assert odd.num_pages == 2
    assert odd.last_page_len == 36


def test_workload_cases_cover_matrix():
    cases = build_workload_cases(
        input_lens=(128, 512),
        output_lens=(1, 128),
        num_requests=20,
        concurrencies=(1, 4),
    )
    assert len(cases) == 2 * 2 * 2
    identifiers = {case.case_id for case in cases}
    assert "in128-out1-n20-c1" in identifiers
    assert "in512-out128-n20-c4" in identifiers


def test_workload_case_dict_carries_case_id():
    """Records have to carry case_id; the report groups by it."""
    case = build_workload_cases(
        input_lens=(512,), output_lens=(1,), num_requests=4, concurrencies=(1,)
    )[0]
    body = case.as_dict()
    assert body["case_id"] == case.case_id == "in512-out1-n4-c1"
    assert body["input_len"] == 512
    assert body["num_requests"] == 4


def test_e2e_configs_cover_all_tiers_and_patterns():
    configs = build_e2e_configs()
    identifiers = {config.config_id for config in configs}
    for tier in ("gpu_only", "host", "mooncake"):
        for pattern in ("cold_miss", "full_hit", "partial_hit", "multiturn"):
            assert f"{tier}.{pattern}" in identifiers


def test_e2e_config_rejects_unknown_values():
    from benchmarks.attention_kv.config import E2EConfig

    with pytest.raises(ValueError):
        E2EConfig(cache_tier="disk", hit_pattern="cold_miss")
    with pytest.raises(ValueError):
        E2EConfig(cache_tier="host", hit_pattern="warm_hit")


class _Args:
    """The field set build_plan_from_args needs."""

    def __init__(self, **overrides):
        self.quick = True
        self.full = False
        self.model = "placeholder"
        self.backend = "sglang"
        self.tp_size = 1
        self.page_size = 64
        self.input_lens = None
        self.output_len = None
        self.requests = None
        self.concurrency = None
        self.repeats = None
        self.rounds = None
        self.seed = 42
        self.tiers = None
        self.patterns = None
        self.skip_kernel = False
        self.skip_e2e = False
        for key, value in overrides.items():
            setattr(self, key, value)


@pytest.mark.parametrize(
    "overrides",
    [
        {"tp_size": 0},
        {"tp_size": -1},
        {"page_size": 0},
        {"requests": 0},
        {"repeats": 0},
        {"rounds": 0},
        {"input_lens": [128, 0]},
        {"output_len": 0},
        {"concurrency": [0]},
    ],
)
def test_plan_rejects_non_positive_numbers(overrides):
    """A 0 or a negative value has to fail while the plan is built, not turn into
    a division by zero or an empty sample later."""
    from benchmarks.attention_kv.config import build_plan_from_args

    with pytest.raises(ValueError):
        build_plan_from_args(_Args(**overrides))


def test_plan_accepts_positive_numbers():
    from benchmarks.attention_kv.config import build_plan_from_args

    plan = build_plan_from_args(_Args(input_lens=[128, 512], requests=2))
    assert plan.num_requests == 2
    assert plan.input_lens == (128, 512)


def test_plan_accepts_an_unresolved_tp_size():
    """An empty --tp-size asks for the visible GPU count, which only a run on
    hardware resolves; building the plan must not reject it as a non-positive
    number, or --dry-run without --tp-size would fail."""
    from benchmarks.attention_kv.config import build_plan_from_args

    plan = build_plan_from_args(_Args(tp_size=None))
    assert plan.tp_size is None
    assert "auto (visible GPU count)" in plan.describe()


def test_plan_describes_a_resolved_tp_size():
    from benchmarks.attention_kv.config import build_plan_from_args

    plan = build_plan_from_args(_Args(tp_size=8))
    assert "auto" not in plan.describe()
