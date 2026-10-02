"""Construction of kernel measurement points and the byte accounting behind them.

These cases need torch, so the whole module is skipped when it is missing; on a
machine with a GPU they really execute.
"""

import json

import pytest

torch = pytest.importorskip("torch")

from benchmarks.attention_kv.config import load_model_kv_config  # noqa: E402
from benchmarks.attention_kv.kernel_bench import (  # noqa: E402
    assert_tp_sharding,
    build_kernel_cases,
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


def write_model(tmp_path, body):
    directory = tmp_path / "model"
    directory.mkdir()
    (directory / "config.json").write_text(json.dumps(body), encoding="utf-8")
    return str(directory)


def test_dtype_comes_from_the_model_config(tmp_path):
    model = load_model_kv_config(write_model(tmp_path, QWEN3_8B))
    cases = build_kernel_cases(
        model, tp_size=8, page_size=64, input_lens=[128], modes=["decode"]
    )
    assert cases[0].dtype == "bfloat16"
    assert cases[0].dtype_bytes == 2


def test_float16_model_is_honoured(tmp_path):
    body = dict(QWEN3_8B)
    body["torch_dtype"] = "float16"
    model = load_model_kv_config(write_model(tmp_path, body))
    cases = build_kernel_cases(
        model, tp_size=8, page_size=64, input_lens=[128], modes=["decode"]
    )
    assert cases[0].dtype == "float16"
    assert cases[0].dtype_bytes == 2


def test_fp8_model_is_rejected_before_measurement(tmp_path):
    body = dict(QWEN3_8B)
    body["torch_dtype"] = "float8_e4m3fn"
    model = load_model_kv_config(write_model(tmp_path, body))
    model = type(model)(**{**model.__dict__, "dtype_bytes": 1})
    with pytest.raises(ValueError):
        build_kernel_cases(
            model, tp_size=8, page_size=64, input_lens=[128], modes=["decode"]
        )


def test_query_heads_must_divide_by_tp(tmp_path):
    """Integer division truncates silently when the KV heads divide but the query heads do not."""
    body = dict(QWEN3_8B)
    # 6 % 3 == 0, so the check falls through to the query heads
    body["num_key_value_heads"] = 6
    model = load_model_kv_config(write_model(tmp_path, body))
    assert model.num_key_value_heads % 3 == 0
    assert model.num_attention_heads % 3 != 0
    with pytest.raises(ValueError, match="num_attention_heads"):
        assert_tp_sharding(model, tp_size=3)


def test_kv_heads_must_divide_by_tp(tmp_path):
    model = load_model_kv_config(write_model(tmp_path, QWEN3_8B))
    with pytest.raises(ValueError, match="num_key_value_heads"):
        assert_tp_sharding(model, tp_size=5)


def test_sharding_reports_per_rank_heads(tmp_path):
    model = load_model_kv_config(write_model(tmp_path, QWEN3_8B))
    qo_heads, kv_heads = assert_tp_sharding(model, tp_size=8)
    assert (qo_heads, kv_heads) == (4, 1)


def test_decode_read_bytes_count_full_pages(tmp_path):
    """A decode context of seq_len+1 leaves the last page partial, so reads count whole pages."""
    model = load_model_kv_config(write_model(tmp_path, QWEN3_8B))
    case = build_kernel_cases(
        model, tp_size=8, page_size=64, input_lens=[128], modes=["decode"]
    )[0]
    assert case.context_tokens == 129
    assert case.pages_read == 3
    assert case.kv_bytes_read_moved() > case.kv_bytes_read()
    assert case.padding_tokens_read == 3 * 64 - 129


def test_aligned_prefill_has_no_padding(tmp_path):
    model = load_model_kv_config(write_model(tmp_path, QWEN3_8B))
    case = build_kernel_cases(
        model, tp_size=8, page_size=64, input_lens=[512], modes=["prefill"]
    )[0]
    assert case.padding_tokens_read == 0
    assert case.kv_bytes_read_moved() == case.kv_bytes_read()


def test_case_payload_exposes_both_byte_counts(tmp_path):
    model = load_model_kv_config(write_model(tmp_path, QWEN3_8B))
    case = build_kernel_cases(
        model, tp_size=8, page_size=64, input_lens=[128], modes=["decode"]
    )[0]
    body = case.as_dict()
    assert body["kv_bytes_read_valid"] == case.kv_bytes_read()
    assert body["kv_bytes_read_moved"] == case.kv_bytes_read_moved()
    assert "kv_bytes_read" not in body


def test_the_read_only_gather_is_not_part_of_the_attention_step():
    """The gather copies every layer's KV through the page table to price the read
    path. A forward pass does not do that, so adding it to the step total would
    report a throughput no step reaches."""
    from benchmarks.attention_kv.kernel_bench import (
        DIAGNOSTIC_PHASES,
        MEASURED_PHASES,
        STEP_PHASES,
    )

    assert "kv_gather" in DIAGNOSTIC_PHASES
    assert "kv_gather" not in STEP_PHASES
    assert set(STEP_PHASES) | set(DIAGNOSTIC_PHASES) == set(MEASURED_PHASES)
    assert set(STEP_PHASES) == {
        "metadata",
        "kv_write",
        "attention_plan",
        "attention",
    }


def test_total_step_is_the_sum_of_the_step_phases(tmp_path):
    """A real run's total must equal the step phases, with the gather left out."""
    from benchmarks.attention_kv.kernel_bench import STEP_PHASES, run_case

    if not torch.cuda.is_available():
        pytest.skip("needs a GPU")
    model = load_model_kv_config(write_model(tmp_path, QWEN3_8B))
    case = build_kernel_cases(
        model, tp_size=1, page_size=64, input_lens=[128], modes=["prefill"]
    )[0]
    record = run_case(case, 0, warmup=2, timed=5, verify=False)
    phases = record["phases"]
    step_sum = sum(phases[name]["p50"] for name in STEP_PHASES)
    assert phases["total_step"]["p50"] == pytest.approx(step_sum, rel=0.1)
    assert phases["total_step"]["p50"] < step_sum + phases["kv_gather"]["p50"]
    assert "kv_gather_share_of_total" not in record["derived"]
    assert "kv_gather_share_of_attention_run" in record["derived"]


def test_metadata_breakdown_stays_inside_the_metadata_window(tmp_path):
    """The host to device copy happens inside the window that times the metadata
    phase, so the breakdown cannot be larger than the phase it belongs to."""
    from benchmarks.attention_kv.kernel_bench import run_case

    if not torch.cuda.is_available():
        pytest.skip("needs a GPU")
    model = load_model_kv_config(write_model(tmp_path, QWEN3_8B))
    case = build_kernel_cases(
        model, tp_size=1, page_size=64, input_lens=[128], modes=["prefill"]
    )[0]
    record = run_case(case, 0, warmup=2, timed=5, verify=False)
    phases = record["phases"]
    assert phases["metadata_h2d"]["p50"] <= phases["metadata"]["p50"]
    assert phases["metadata_cpu"]["p50"] <= phases["metadata"]["p50"]
    share = record["derived"]["metadata_h2d_share_of_metadata"]
    assert 0.0 <= share <= 1.0
