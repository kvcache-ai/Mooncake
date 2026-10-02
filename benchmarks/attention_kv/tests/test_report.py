import json

import pytest

from benchmarks.attention_kv.report import (
    DURATION_METRICS,
    _counter_or_zero,
    aggregate_e2e,
    build_markdown_summary,
    check_repeat_stability,
    collect_unavailable_notes,
    kernel_summary,
    load_records,
    write_csv,
)


def test_counter_or_zero_distinguishes_absent_from_zero():
    """An existing metric with no delta is 0; only an absent metric is
    unavailable."""
    base = "sglang:cached_tokens_total"
    assert _counter_or_zero({}, {base}, base) == 0.0
    assert _counter_or_zero({}, set(), base) is None
    delta = {f'{base}{{cache_source="device",tp_rank="0"}}': 128.0}
    assert _counter_or_zero(delta, {base}, base, ('cache_source="device"',)) == 128.0
    assert _counter_or_zero(delta, {base}, base, ('cache_source="host"',)) == 0.0


def make_e2e_record(
    ttft,
    e2e,
    itl,
    output_tokens,
    cached_tokens,
    prompt_tokens,
    config_id="host.full_hit",
    repeat_index=0,
    round_index=0,
    cached_tokens_details=None,
):
    tier, pattern = config_id.split(".")
    return {
        "kind": "e2e",
        "run_id": "run",
        "config": {
            "config_id": config_id,
            "cache_tier": tier,
            "cache_tier_description": "d",
            "hit_pattern": pattern,
            "rounds": 1,
            "repeats": 3,
            "warmups": 1,
            "seed": 42,
        },
        "case": {
            "input_len": 512,
            "output_len": 1,
            "num_requests": 20,
            "max_concurrency": 1,
            "case_id": "in512-out1-n20-c1",
        },
        "repeat_index": repeat_index,
        "round_index": round_index,
        "request_index": 0,
        "ttft_ms": ttft,
        "e2e_ms": e2e,
        "itl_ms": itl,
        "output_tokens": output_tokens,
        "meta_info": {
            "prompt_tokens": prompt_tokens,
            "cached_tokens": cached_tokens,
            "cached_tokens_details": cached_tokens_details,
        },
    }


def make_run_record(wall_ms, metrics_delta, repeat_index=0, metrics_present=()):
    return {
        "kind": "e2e_run_metrics",
        "run_id": "run",
        "config": {
            "config_id": "host.full_hit",
            "cache_tier": "host",
            "cache_tier_description": "d",
            "hit_pattern": "full_hit",
            "rounds": 1,
            "repeats": 3,
            "warmups": 1,
            "seed": 42,
        },
        "case": {
            "input_len": 512,
            "output_len": 1,
            "num_requests": 20,
            "max_concurrency": 1,
            "case_id": "in512-out1-n20-c1",
        },
        "repeat_index": repeat_index,
        "round_index": 0,
        "run_wall_ms": wall_ms,
        "metrics_delta": metrics_delta,
        "metrics_present": list(metrics_present),
    }


def test_aggregate_accepts_case_payload_from_config_module():
    """The report has to consume the case payload the config module produces."""
    from benchmarks.attention_kv.config import build_workload_cases

    case = build_workload_cases(
        input_lens=(512,), output_lens=(1,), num_requests=20, concurrencies=(1,)
    )[0]
    payload = dict(case.as_dict())
    payload["kv_bytes_per_token_aggregate"] = 147456
    payload["total_kv_bytes_prompt"] = 512 * 147456

    record = make_e2e_record(20.0, 40.0, [1.0], 2, 500, 512)
    record["case"] = payload
    run_record = make_run_record(1000.0, {})
    run_record["case"] = payload

    rows = aggregate_e2e([record, run_record], page_size=64)
    assert len(rows) == 1
    assert rows[0]["case_id"] == "in512-out1-n20-c1"
    assert rows[0]["kv_bytes_per_token"] == 147456


def test_aggregate_computes_hit_rate_from_meta_info():
    records = [
        make_e2e_record(20.0, 40.0, [1.0, 1.5], 3, 500, 512),
        make_e2e_record(30.0, 60.0, [2.0], 2, 480, 512),
        make_run_record(1000.0, {}),
    ]
    rows = aggregate_e2e(records, page_size=64)
    assert len(rows) == 1
    row = rows[0]
    assert row["hit_tokens"] == 980
    assert row["miss_tokens"] == 1024 - 980
    assert row["cache_hit_rate"] == 980 / 1024
    assert row["hit_source"] == "response meta_info cached_tokens"
    assert row["hit_pages"] == (980 + 63) // 64
    assert row["pages_source"] == "derived_from_tokens"


def test_aggregate_marks_missing_durations_unavailable():
    records = [
        make_e2e_record(20.0, 40.0, [1.0], 2, 500, 512),
        make_run_record(500.0, {}),
    ]
    row = aggregate_e2e(records, page_size=64)[0]
    assert row["l2_to_gpu_h2d_time_ms"] == "unavailable"
    assert row["device_to_l2_backup_time_ms"] == "unavailable"
    assert row["device_eviction_time_ms"] == "unavailable"
    assert row["l2_to_gpu_bandwidth_gbps"] is None


def test_aggregate_reads_durations_when_present():
    """An exact metric name hit converts seconds to milliseconds and sums the
    series across ranks."""
    spec = DURATION_METRICS["l2_to_gpu_h2d_time"]
    assert spec.full_name.endswith("_sum")
    records = [
        make_e2e_record(20.0, 40.0, [1.0], 2, 500, 512),
        make_run_record(
            500.0,
            {
                f'{spec.full_name}{{tp_rank="0"}}': 0.002,
                f'{spec.full_name}{{tp_rank="1"}}': 0.003,
            },
            metrics_present=[spec.base_name],
        ),
    ]
    row = aggregate_e2e(records, page_size=64)[0]
    # 0.002 s + 0.003 s = 5 ms
    assert row["l2_to_gpu_h2d_time_ms"]["value"] == pytest.approx(5.0)
    assert row["l2_to_gpu_h2d_time_ms"]["source_unit"] == "seconds"


def test_l3_write_back_is_not_read_from_the_device_to_host_backup():
    """The backup histogram times the GPU to host copy, which is the L2 path. Using
    it as the L3 write-back would report a duration for a transfer it never
    measured."""
    records = [
        make_e2e_record(20.0, 40.0, [1.0], 2, 500, 512),
        make_run_record(
            500.0,
            {
                "sglang:hicache_backup_duration_seconds_sum": 0.5,
                "sglang:hicache_backup_bytes_total": 1024.0,
            },
            metrics_present=["sglang:hicache_backup_duration_seconds"],
        ),
    ]
    row = aggregate_e2e(records, page_size=64)[0]
    assert row["device_to_l2_backup_time_ms"]["value"] == pytest.approx(500.0)
    # No L3 duration exists in this SGLang version, so none is reported
    assert row["l3_prefetch_hit_tokens"] is None


def test_l3_prefetch_hit_tokens_is_a_counter_not_a_duration():
    """A token counter must never be divided into the bytes of a transfer."""
    records = [
        make_e2e_record(20.0, 40.0, [1.0], 2, 500, 512),
        make_run_record(
            500.0,
            {'sglang:storage_prefetch_hit_tokens_total{tp_rank="0"}': 4096.0},
            metrics_present=["sglang:storage_prefetch_hit_tokens_total"],
        ),
    ]
    row = aggregate_e2e(records, page_size=64)[0]
    assert row["l3_prefetch_hit_tokens"] == 4096.0
    assert row["l2_to_gpu_bandwidth_gbps"] is None


def test_aggregate_percentiles_and_throughput():
    records = [
        make_e2e_record(ttft, ttft * 2, [1.0], 2, 0, 512)
        for ttft in (10.0, 20.0, 30.0, 40.0)
    ]
    records.append(make_run_record(2000.0, {}))
    row = aggregate_e2e(records, page_size=64)[0]
    assert row["ttft_p50_ms"] == 25.0
    assert row["ttft_p95_ms"] > row["ttft_p50_ms"]
    assert row["input_tokens_per_s"] == (512 * 4) / 2.0
    assert row["output_tokens_per_s"] == 8 / 2.0
    assert row["num_samples"] == 4


def with_tier_context(records, **context):
    for record in records:
        record["case"].update(context)
    return records


def test_tier_attribution_comes_from_the_per_request_details():
    """Which tier served a hit is reported by the server per request; without it a
    tier comparison can only say that some cache served the request."""
    records = [
        make_e2e_record(
            20.0,
            40.0,
            [1.0],
            2,
            500,
            512,
            cached_tokens_details={
                "device": 0,
                "host": 400,
                "storage": 100,
                "storage_backend": "MooncakeStore",
            },
        ),
        make_run_record(500.0, {}),
    ]
    records = with_tier_context(records, storage_backend="mooncake")
    row = aggregate_e2e(records, page_size=64)[0]
    assert row["device_hit_tokens"] == 0
    assert row["host_hit_tokens"] == 400
    assert row["storage_hit_tokens"] == 100
    assert row["hit_tier"] == "mixed"


def test_single_serving_tier_is_named():
    records = [
        make_e2e_record(
            20.0,
            40.0,
            [1.0],
            2,
            500,
            512,
            cached_tokens_details={"device": 0, "host": 500, "storage": 0},
        ),
        make_run_record(500.0, {}),
    ]
    row = aggregate_e2e(records, page_size=64)[0]
    assert row["hit_tier"] == "host"


def test_storage_hit_tokens_is_unknown_without_an_l3_backend():
    """Zero L3 tokens is a measurement; it cannot be claimed when no L3 backend was
    configured, because the response then carries no storage field at all."""
    without = [
        make_e2e_record(20.0, 40.0, [1.0], 2, 500, 512),
        make_run_record(500.0, {}),
    ]
    without = with_tier_context(without, storage_backend=None)
    assert aggregate_e2e(without, page_size=64)[0]["storage_hit_tokens"] is None

    with_backend = [
        make_e2e_record(20.0, 40.0, [1.0], 2, 500, 512),
        make_run_record(500.0, {}),
    ]
    with_backend = with_tier_context(
        with_backend,
        storage_backend="mooncake",
        l1_pool_tokens=1000,
        l2_pool_tokens=2000,
    )
    assert aggregate_e2e(with_backend, page_size=64)[0]["storage_hit_tokens"] == 0


def test_pool_capacity_flags_say_whether_a_tier_could_hold_the_working_set():
    records = [
        make_e2e_record(20.0, 40.0, [1.0], 2, 500, 512),
        make_run_record(500.0, {}),
    ]
    records = with_tier_context(records, l1_pool_tokens=10000, l2_pool_tokens=40000)
    row = aggregate_e2e(records, page_size=64)[0]
    assert row["working_set_tokens"] == 512
    assert row["l1_can_hold_working_set"] is True
    assert row["l2_can_hold_working_set"] is True

    beyond_l1 = [
        make_e2e_record(20.0, 40.0, [1.0], 2, 500, 512),
        make_run_record(500.0, {}),
    ]
    beyond_l1 = with_tier_context(
        beyond_l1,
        l1_pool_tokens=100,
        l2_pool_tokens=400,
        num_requests=20,
    )
    row = aggregate_e2e(beyond_l1, page_size=64)[0]
    assert row["l1_can_hold_working_set"] is False
    assert row["l2_can_hold_working_set"] is False


def test_local_transfer_bandwidths_use_bytes_and_duration_of_one_window():
    records = [
        make_e2e_record(20.0, 40.0, [1.0], 2, 500, 512),
        make_run_record(
            500.0,
            {
                "sglang:load_back_bytes_total": 2e9,
                "sglang:load_back_duration_seconds_sum": 0.5,
                "sglang:hicache_backup_bytes_total": 1e9,
                "sglang:hicache_backup_duration_seconds_sum": 0.25,
            },
            metrics_present=[
                "sglang:load_back_duration_seconds",
                "sglang:hicache_backup_duration_seconds",
            ],
        ),
    ]
    row = aggregate_e2e(records, page_size=64)[0]
    assert row["l2_to_gpu_bandwidth_gbps"] == pytest.approx(4.0)
    assert row["device_to_l2_bandwidth_gbps"] == pytest.approx(4.0)


def test_window_totals_are_not_divided_into_a_single_request_latency():
    """The load-back of a window can serve a later round, so the report keeps the
    window total and derives no per-request share from it."""
    records = [
        make_e2e_record(20.0, 40.0, [1.0], 2, 0, 512),
        make_e2e_record(40.0, 80.0, [1.0], 2, 0, 512),
        make_run_record(
            500.0,
            {"sglang:load_back_duration_seconds_sum": 0.04},
            metrics_present=["sglang:load_back_duration_seconds"],
        ),
    ]
    row = aggregate_e2e(records, page_size=64)[0]
    assert row["l2_to_gpu_h2d_time_ms"]["value"] == pytest.approx(40.0)
    assert row["mean_ttft_ms"] == pytest.approx(30.0)
    assert "l2_to_gpu_ms_per_request" not in row
    assert "l2_to_gpu_share_of_mean_ttft" not in row


def make_manifest():
    return {
        "generated_at": "2026-01-01T00:00:00+0000",
        "gpu": {"gpu_names": ["NVIDIA H20"], "gpu_count": 8},
        "model": {
            "path": "/models/Qwen3-8B",
            "dtype": "bfloat16",
            "tp_size": 8,
            "page_size": 64,
            "kv_config": {
                "num_layers": 36,
                "num_key_value_heads": 8,
                "head_dim": 128,
            },
            "kv_bytes_per_token_aggregate": 147456,
            "kv_bytes_per_token_per_rank": 18432,
        },
    }


def test_report_says_when_the_l3_tier_served_nothing():
    """A run whose storage backend never served a token cannot support an L3
    benefit, and the report has to say so instead of leaving it to the reader."""
    records = [
        make_e2e_record(
            20.0,
            40.0,
            [1.0],
            2,
            500,
            512,
            config_id="mooncake.full_hit",
            cached_tokens_details={"device": 0, "host": 500, "storage": 0},
        ),
        make_run_record(500.0, {}),
    ]
    records[1]["config"]["cache_tier"] = "mooncake"
    records[1]["config"]["config_id"] = "mooncake.full_hit"
    records = with_tier_context(
        records,
        storage_backend="mooncake",
        l1_pool_tokens=100,
        l2_pool_tokens=800,
    )
    rows = aggregate_e2e(records, page_size=64)
    markdown = build_markdown_summary(
        rows, [], make_manifest(), [], collect_unavailable_notes(rows)
    )
    assert "The storage backend served no token in this run" in markdown


def test_repeat_stability_flags_large_spread():
    records = []
    for repeat_index, ttft in enumerate((100.0, 108.0, 120.0)):
        records.append(
            make_e2e_record(ttft, ttft * 2, [1.0], 2, 0, 512, repeat_index=repeat_index)
        )
        records.append(make_run_record(100.0, {}, repeat_index=repeat_index))
    rows = aggregate_e2e(records, page_size=64)
    warnings = check_repeat_stability(rows, threshold=0.05)
    flagged = {warning["metric"] for warning in warnings}
    assert "ttft_p50_ms" in flagged
    ttft_warning = next(w for w in warnings if w["metric"] == "ttft_p50_ms")
    assert ttft_warning["relative_spread"] == pytest.approx(0.20)
    assert ttft_warning["repeats"] == 3


def test_repeat_stability_quiet_when_within_threshold():
    records = []
    for repeat_index, ttft in enumerate((100.0, 101.0, 102.0)):
        records.append(
            make_e2e_record(ttft, ttft * 2, [1.0], 2, 0, 512, repeat_index=repeat_index)
        )
        records.append(make_run_record(100.0, {}, repeat_index=repeat_index))
    rows = aggregate_e2e(records, page_size=64)
    assert check_repeat_stability(rows, threshold=0.05) == []


def test_every_aggregated_field_reaches_the_csv():
    """The CSV writer drops keys that are not listed, so a new row field has to be
    added to CSV_COLUMNS or it silently never reaches the summary."""
    from benchmarks.attention_kv.report import CSV_COLUMNS

    records = [
        make_e2e_record(20.0, 40.0, [1.0], 2, 500, 512),
        make_run_record(500.0, {}),
    ]
    row = aggregate_e2e(records, page_size=64)[0]
    assert [key for key in row if key not in CSV_COLUMNS] == []


def test_load_records_and_write_csv(tmp_path):
    path = tmp_path / "e2e.jsonl"
    records = [make_e2e_record(20.0, 40.0, [1.0], 2, 500, 512)]
    with open(path, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")
    loaded = load_records(str(path))
    assert len(loaded) == 1

    rows = aggregate_e2e(loaded, page_size=64)
    csv_path = tmp_path / "summary.csv"
    write_csv(str(csv_path), rows)
    text = csv_path.read_text(encoding="utf-8")
    assert "config_id" in text
    assert "host.full_hit" in text


def test_kernel_summary_extracts_phases():
    record = {
        "kind": "kernel",
        "case": {
            "label": "prefill_len128_bs1",
            "mode": "prefill",
            "seq_len": 128,
            "batch_size": 1,
            "page_size": 64,
            "num_layers": 36,
            "num_qo_heads": 4,
            "num_kv_heads": 1,
            "head_dim": 128,
            "kv_bytes_written": 100,
            "kv_bytes_read_valid": 200,
            "kv_bytes_read_moved": 256,
            "padding_tokens_read": 28,
            "kv_cache_bytes": 300,
        },
        "warmup": 10,
        "timed": 100,
        "device_name": "test-device",
        "correctness": {
            "scatter": {"passed": True},
            "attention": {"passed": True},
        },
        "phases": {
            name: {"p50": 1.0, "p95": 1.5, "p99": 2.0}
            for name in (
                "metadata",
                "kv_write",
                "kv_gather",
                "attention_plan",
                "attention",
                "total_step",
                "metadata_cpu",
                "metadata_h2d",
            )
        },
        "per_layer_p50_ms": {"total_step": 0.01},
        "derived": {"attention_effective_gbps": 12.0},
    }
    rows = kernel_summary([record])
    assert len(rows) == 1
    assert rows[0]["case_id"] == "prefill_len128_bs1"
    assert rows[0]["total_step_p50_ms"] == 1.0
    assert rows[0]["scatter_passed"] is True
    assert rows[0]["attention_effective_gbps"] == 12.0
    # Both byte counts, valid tokens and whole-page moves, reach the summary
    assert rows[0]["kv_bytes_read_valid"] == 200
    assert rows[0]["kv_bytes_read_moved"] == 256
    assert rows[0]["padding_tokens_read"] == 28
