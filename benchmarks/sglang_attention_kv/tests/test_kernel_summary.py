# The summary table's contract, without a GPU: a record goes in and a row with the
# declared columns comes out. The README promises the four configuration fields in
# the CSV, so a record's configuration has to reach the row.

import csv

import pytest

from benchmarks.sglang_attention_kv import kernel_summary as summary_module

CONFIGURATION = {
    "branch": "ragged_prefix_merge",
    "branch_detail": "ragged suffix attention, paged history attention, merge_state, then the KV write",
    "reads_before_write": True,
    "attention_backend": "flashinfer-fa2",
    "wrapper_page_size": 1,
    "decode_use_tensor_cores": True,
    "kv_layout": "NHD",
    "kv_write_stream": "step",
    "slot_allocator": "paged",
    "flashinfer_use_paged_env": False,
}


# The figures derive() produces, which the schema has to name. The names are
# duplicated here on purpose: this is the contract a reader of the CSV sees, and a
# rename that reaches only one side fails in test_a_row_carries_every_declared_column.
DERIVED = {
    "attention_plan_us_per_step": 1.1,
    "attention_component_us_per_layer": 0.031,
    "kv_write_component_us_per_layer": 0.032,
    "indices_us_per_new_token": 0.0034,
    "attention_us_per_context_token": 0.0025,
    "kv_gather_unique_payload_gbps": 260.0,
    "attention_unique_payload_gbps": 4.2,
    "attention_tflops": 68.5,
    "attention_flops_per_unique_payload_byte": 16386.0,
    "indices_ratio_of_phase_sum": 0.05,
    "attention_plan_ratio_of_phase_sum": 0.05,
    "layer_loop_ratio_of_phase_sum": 0.9,
    "attention_component_ratio_of_phase_sum": 0.9,
    "kv_write_component_ratio_of_phase_sum": 0.14,
    "components_ratio_of_phase_sum": 1.04,
    "new_tokens_per_s": 1750.0,
}


def record():
    case = {
        "label": "extend_pref512_chunk128_bs1_ps64",
        "mode": "extend",
        "batch_size": 1,
        "prefix_lens": [512],
        "new_lens": [128],
        "context_lens": [640],
        "page_size": 64,
        "layout": "contiguous",
        "new_tokens": 128,
        "context_tokens": 640,
        "pages": 10,
        "padding_tokens": 0,
        "kv_bytes_written": 128 * 36864,
        "kv_page_capacity_bytes": 640 * 36864,
        "attention_pairs": 128 * 512 + 128 * 129 // 2,
        "attention_flops": 36 * 4 * (128 * 512 + 128 * 129 // 2) * 8 * 128,
    }
    phases = {
        name: {"min": 1.0, "p50": 1.1, "p95": 1.2, "p99": 1.3}
        for name in summary_module.REPORTED_PHASES
    }
    return {
        "kind": "kernel",
        "case": case,
        "configuration": CONFIGURATION,
        "read_bytes": {
            "paged": 512 * 36864,
            "ragged": 128 * 36864,
            "attention": 640 * 36864,
        },
        "gather_rows": 512,
        "gather_bytes": 512 * 36864,
        "kv_cache_bytes_per_rank": 1024 * 1024,
        "num_pages_allocated": 12,
        "warmup": 10,
        "timed": 100,
        "device_name": "NVIDIA H20",
        "phases": phases,
        "derived": dict(DERIVED),
        "correctness": {
            "indices": {"passed": True},
            "history": {"passed": True},
            "attention": {"passed": True},
        },
    }


def test_the_schema_covers_every_derived_figure():
    assert set(DERIVED) <= set(summary_module.CSV_COLUMNS)


def test_a_row_carries_every_declared_column():
    rows = summary_module.kernel_summary([record()])
    assert len(rows) == 1
    assert set(rows[0]) == set(summary_module.CSV_COLUMNS)


def test_the_configuration_reaches_the_row():
    row = summary_module.kernel_summary([record()])[0]
    assert row["attention_backend"] == "flashinfer-fa2"
    assert row["wrapper_page_size"] == 1
    assert row["decode_use_tensor_cores"] is True
    assert row["kv_write_stream"] == "step"
    assert row["slot_allocator"] == "paged"
    assert row["branch"] == "ragged_prefix_merge"
    assert row["reads_before_write"] is True


def test_the_ledger_and_the_windows_reach_the_row():
    row = summary_module.kernel_summary([record()])[0]
    assert row["kv_bytes_paged_read"] == 512 * 36864
    assert row["kv_bytes_ragged_read"] == 128 * 36864
    assert row["kv_bytes_attention_read"] == 640 * 36864
    assert row["kv_bytes_page_capacity"] == 640 * 36864
    assert row["phase_sum_p50_ms"] == 1.1
    assert row["step_window_p95_ms"] == 1.2
    assert row["indices_passed"] is True


def test_records_of_another_kind_are_left_out():
    other = record()
    other["kind"] = "something-else"
    assert summary_module.kernel_summary([other]) == []


def test_the_csv_header_is_the_declared_schema(tmp_path):
    rows = summary_module.kernel_summary([record()])
    path = tmp_path / "kernel_summary.csv"
    summary_module.write_csv(str(path), rows, summary_module.CSV_COLUMNS)
    with open(path, encoding="utf-8") as handle:
        header = next(csv.reader(handle))
    assert header == summary_module.CSV_COLUMNS


def test_a_row_that_lost_a_declared_column_fails_the_write(tmp_path):
    """The schema is the declared one, not whatever the first row holds: a row
    missing a configuration field fails here instead of writing a short line."""
    row = summary_module.kernel_summary([record()])[0]
    del row["decode_use_tensor_cores"]
    with pytest.raises(ValueError, match="missing the declared columns"):
        summary_module.write_csv(
            str(tmp_path / "short.csv"), [row], summary_module.CSV_COLUMNS
        )
