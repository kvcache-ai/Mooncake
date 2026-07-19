from __future__ import annotations

import json
from pathlib import Path

from mooncake_epd.benchmarks.epd_benchmark_campaign import (
    _required_nvlink_pair_failures,
    load_and_run_campaign,
    run_campaign,
    validate_campaign_manifest,
)


def _raw_artifacts(tmp_path: Path, label: str) -> dict[str, str]:
    paths: dict[str, str] = {}
    for field in ("requests_jsonl", "responses_jsonl", "metrics", "environment"):
        path = tmp_path / f"{label}-{field}.json"
        path.write_text("{}", encoding="utf-8")
        paths[field] = str(path)
    logs = tmp_path / f"{label}-service-logs.json"
    logs.write_text(json.dumps({"logs": {}, "missing": []}), encoding="utf-8")
    paths["service_logs"] = str(logs)
    return paths


def _config(protocol: str) -> dict:
    return {
        "schema_version": 1,
        "request_fingerprint": "fixed-qwen3vl-workload",
        "request": {"model": "Qwen3-VL-8B-Instruct", "max_tokens": 96, "temperature": 0.0},
        "request_variation": {"mode": "none", "schema_version": 1},
        "vllm_prefix_cache": {"mode": "reuse", "schema_version": 1},
        "load": {
            "repeat_requests": 32,
            "concurrency": 4,
            "requested_warmup_requests": 16,
            "warmup_cover_dataset_cycle": False,
        },
        "serving": {"max_model_len": 1024, "max_num_batched_tokens": 4096, "max_num_seqs": 8},
        "topology": {"prefill_gpus": [1], "decode_gpus": [3], "total_gpus": 2},
        "epd": {"protocol": protocol, "layers_per_group": 32, "transfer_workers": 2, "prefill_decode_affinity": [["prefill-0", "decode-0"]]},
    }


def _serving_summary(tmp_path: Path, *, label: str, protocol: str, ttft: float, tps: float) -> Path:
    path = tmp_path / f"{label}.json"
    path.write_text(
        json.dumps(
            {
                "mooncake_protocol": protocol,
                "text_only": True,
                "benchmark_config": _config(protocol),
                "responses": [
                    {
                        "index": 0,
                        "status_code": 200,
                        "response_content_len": 9,
                        "response_text": "KV transfer separates prefill and decode.",
                        "routing_path": "PD",
                        "finish_reason": "length",
                        "usage": {"prompt_tokens": 42, "completion_tokens": 8},
                    }
                ],
                "avg_ttft_ms": ttft,
                "ttft_stats_ms": {"avg": ttft, "p95": ttft * 1.1},
                "request_throughput_rps": 2.0 if protocol == "tcp" else 2.3,
                "output_token_goodput_tps": tps,
                "avg_tpot_ms": 23.0,
                "decode_engine_first_token_latency_ms_avg": 80.0,
                "transport_runtime_evidence": {
                    "requested_transport": protocol,
                    "actual_transport": protocol,
                    "pass": True,
                },
                "metrics": {
                    "metrics": {
                        "requests_text": 1,
                        "layered_receive_failures": 0,
                        "layered_transfer_failed_batches": 0,
                        "fallback_batches": 0,
                        "peer_buffer_batches": 2,
                        "remote_transfer_backend_counts": {"peer_buffer_direct": 2},
                    }
                },
                "measured_online_direct_metric_summary": {
                    "peer_buffer_write_ms_avg": 7.0 if protocol == "tcp" else 0.6,
                    "peer_buffer_write_bandwidth_gbps": 2.5 if protocol == "tcp" else 35.0,
                    "layered_receive_kv_worker_ms_avg": 20.0 if protocol == "tcp" else 8.0,
                },
                "raw_artifacts": _raw_artifacts(tmp_path, label),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def _scenario(identifier: str, path: Path, protocol: str) -> dict:
    return {
        "id": identifier,
        "kind": "pd_transport",
        "required": True,
        "gate": "serving",
        "transport": protocol,
        "expected_routing_path": "PD",
        "summary": str(path),
        # Synthetic fixtures cannot provide a meaningful GPU topology; real
        # serving scenarios default to requiring it.
        "require_hardware_inventory": False,
    }


def test_campaign_reports_strong_same_resource_transport_evidence(tmp_path):
    scenarios = []
    baseline_ids = []
    candidate_ids = []
    for index, (tcp_ttft, nv_ttft) in enumerate(((210.0, 120.0), (205.0, 118.0), (215.0, 122.0)), start=1):
        tcp_id = f"tcp-r{index}"
        nv_id = f"nvlink-r{index}"
        baseline_ids.append(tcp_id)
        candidate_ids.append(nv_id)
        scenarios.append(
            _scenario(
                tcp_id,
                _serving_summary(tmp_path, label=tcp_id, protocol="tcp", ttft=tcp_ttft, tps=120.0),
                "tcp",
            )
        )
        scenarios.append(
            _scenario(
                nv_id,
                _serving_summary(tmp_path, label=nv_id, protocol="nvlink_intra", ttft=nv_ttft, tps=150.0),
                "nvlink_intra",
            )
        )
    manifest = {
        "schema_version": 1,
        "campaign_id": "transport-proof",
        "scenarios": scenarios,
        "comparisons": [
            {
                "id": "nvlink-vs-tcp",
                "kind": "transport_ab",
                "baseline": baseline_ids,
                "candidate": candidate_ids,
                "baseline_transport": "tcp",
                "candidate_transport": "nvlink_intra",
                "quality": "exact",
                "primary_metric": "avg_ttft_ms",
                "min_samples": 3,
            }
        ],
    }

    report = run_campaign(manifest, manifest_dir=tmp_path)

    assert report["coverage"]["complete"] is True
    comparison = report["comparisons"][0]
    assert comparison["status"] == "passed"
    assert comparison["same_resource"] is True
    assert comparison["quality"]["semantic_strength"] == "strong"
    assert comparison["strong_performance_claim_supported"] is True
    assert comparison["metrics"]["avg_ttft_ms"]["improvement"]["ci95_low_pct"] > 0.0
    assert report["bottleneck_rankings"][0]["component"] == "decode_first_token"


def test_campaign_keeps_blocked_hardware_work_out_of_passed_coverage(tmp_path):
    manifest = {
        "schema_version": 1,
        "campaign_id": "hardware-blocked",
        "scenarios": [
            {
                "id": "rdma-cross-node",
                "kind": "cross_node",
                "required": True,
                "gate": "serving",
                "transport": "rdma",
                "blocked_reason": "no active RDMA link",
            }
        ],
        "comparisons": [],
    }

    report = run_campaign(manifest, manifest_dir=tmp_path)

    assert report["scenarios"][0]["status"] == "blocked"
    assert report["coverage"]["complete"] is False
    assert report["coverage"]["blocked_scenarios"] == ["rdma-cross-node"]


def test_agent_clone_scenario_uses_clone_gate_without_fake_openai_responses(tmp_path):
    """Agent clone is a state benchmark, not a serving-response benchmark."""

    summary = tmp_path / "agent-clone.json"
    summary.write_text(
        json.dumps(
            {
                "zero_copy_clone_bytes": 0,
                "speedup_avg": 3.2,
                "store_stats_after_release": {"states": 0},
                "page_manager_stats_after_release": {"directory_orphans": 0},
            }
        ),
        encoding="utf-8",
    )
    manifest = {
        "schema_version": 1,
        "campaign_id": "agent-clone-only",
        "scenarios": [
            {
                "id": "clone",
                "kind": "agent_clone",
                "gate": "agent_clone",
                "require_raw_artifacts": False,
                "summary": str(summary),
            }
        ],
        "comparisons": [],
    }

    report = run_campaign(manifest, manifest_dir=tmp_path)

    assert report["coverage"]["complete"] is True
    assert report["scenarios"][0]["status"] == "passed"
    assert report["scenarios"][0]["response_count"] == 0


def test_scheduler_scenario_does_not_require_fake_openai_responses(tmp_path):
    summary = tmp_path / "scheduler.json"
    summary.write_text(
        json.dumps(
            {
                "count": 16,
                "route_correct_rate": 1.0,
                "sla_satisfied_rate": 1.0,
            }
        ),
        encoding="utf-8",
    )
    report = run_campaign(
        {
            "schema_version": 1,
            "campaign_id": "scheduler-only",
            "scenarios": [
                {
                    "id": "scheduler",
                    "kind": "scheduler",
                    "gate": "none",
                    "summary": str(summary),
                }
            ],
            "comparisons": [],
        },
        manifest_dir=tmp_path,
    )

    assert report["coverage"]["complete"] is True
    assert report["scenarios"][0]["status"] == "passed"
    assert report["scenarios"][0]["response_count"] == 0


def test_scheduler_status_only_ablation_compares_trace_metrics_without_responses(tmp_path):
    summaries = []
    for label, ttft, correct in (("least", 600.0, 0.7), ("aware", 500.0, 1.0)):
        path = tmp_path / f"{label}.json"
        path.write_text(
            json.dumps(
                {
                    "scheduler_policy": label,
                    "route_correct_rate": correct,
                    "sla_satisfied_rate": correct,
                    "ttft_ms": {"avg": ttft},
                }
            ),
            encoding="utf-8",
        )
        summaries.append(path)
    manifest = {
        "schema_version": 1,
        "campaign_id": "scheduler-ablation",
        "scenarios": [
            {"id": "least", "kind": "scheduler", "summary": str(summaries[0])},
            {"id": "aware", "kind": "scheduler", "summary": str(summaries[1])},
        ],
        "comparisons": [
            {
                "id": "aware-vs-least",
                "kind": "tuning_ab",
                "baseline": ["least"],
                "candidate": ["aware"],
                "quality": "status_only",
                "primary_metric": "route_correct_rate",
                "min_samples": 1,
            }
        ],
    }

    report = run_campaign(manifest, manifest_dir=tmp_path)

    comparison = report["comparisons"][0]
    assert comparison["status"] == "passed"
    assert comparison["quality"]["non_serving_trace_comparison"] is True
    assert comparison["metrics"]["avg_ttft_ms"]["candidate"]["mean"] == 500.0
    assert comparison["metrics"]["route_correct_rate"]["candidate"]["mean"] == 1.0


def test_campaign_cache_expectations_require_observed_hit_and_miss_evidence(tmp_path):
    summary = _serving_summary(
        tmp_path, label="cache", protocol="tcp", ttft=200.0, tps=100.0
    )
    payload = json.loads(summary.read_text(encoding="utf-8"))
    payload["direct_feature_cache_hits"] = 0
    payload["direct_feature_cache_hit_requests"] = 0
    payload["direct_buffer_stats_after_release"] = {
        "persistent_cache": False,
        "allocations": 0,
    }
    payload["measured_online_direct_metric_summary"].update(
        {"hidden_cache_hits": 0, "hidden_cache_misses": 8}
    )
    summary.write_text(json.dumps(payload), encoding="utf-8")
    scenario = _scenario("cache-negative", summary, "tcp")
    scenario.update(
        {
            "kind": "hidden_cache",
            "cache_expectations": {
                "direct_feature_cache_hits": {"max": 0},
                "direct_buffer_persistent_cache": {"equals": False},
                "hidden_cache_hits": {"max": 0},
            },
        }
    )

    report = run_campaign(
        {"schema_version": 1, "campaign_id": "cache", "scenarios": [scenario], "comparisons": []},
        manifest_dir=tmp_path,
    )

    assert report["scenarios"][0]["status"] == "passed"
    assert report["scenarios"][0]["cache_observations"]["hidden_cache_misses"] == 8.0

    scenario["cache_expectations"]["direct_feature_cache_hits"] = {"min": 1}
    failed = run_campaign(
        {"schema_version": 1, "campaign_id": "cache-fail", "scenarios": [scenario], "comparisons": []},
        manifest_dir=tmp_path,
    )
    assert failed["scenarios"][0]["status"] == "failed"
    assert any("direct_feature_cache_hits_below_min" in item for item in failed["scenarios"][0]["failures"])


def test_cache_ab_allows_only_declared_cache_controls_and_stays_diagnostic(tmp_path):
    cold = _serving_summary(
        tmp_path, label="cold", protocol="tcp", ttft=300.0, tps=100.0
    )
    hot = _serving_summary(
        tmp_path, label="hot", protocol="tcp", ttft=200.0, tps=120.0
    )
    for path, cache_enabled, variation in (
        (cold, False, "unique_prefix"),
        (hot, True, "none"),
    ):
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["benchmark_config"]["epd"].update(
            {
                "direct_persistent_cache": cache_enabled,
                "direct_proxy_handle_cache": cache_enabled,
                "vllm_mm_hidden_cache": cache_enabled,
                "rendered_prefill_cache": cache_enabled,
            }
        )
        payload["benchmark_config"]["request_variation"] = {
            "mode": variation,
            "schema_version": 1,
        }
        path.write_text(json.dumps(payload), encoding="utf-8")
    manifest = {
        "schema_version": 1,
        "campaign_id": "cache-ab",
        "scenarios": [
            _scenario("cold", cold, "tcp"),
            _scenario("hot", hot, "tcp"),
        ],
        "comparisons": [
            {
                "id": "hot-v-cold",
                "kind": "cache_ab",
                "baseline": ["cold"],
                "candidate": ["hot"],
                "baseline_transport": "tcp",
                "candidate_transport": "tcp",
                # Even exact output from a single cache A/B must remain a
                # cache-scoped diagnostic rather than a strong performance
                # claim.
                "quality": "exact",
                "primary_metric": "avg_ttft_ms",
                "min_samples": 1,
            }
        ],
    }

    report = run_campaign(manifest, manifest_dir=tmp_path)

    comparison = report["comparisons"][0]
    assert comparison["status"] == "passed"
    assert comparison["same_resource"] is True
    assert comparison["strong_performance_claim_supported"] is False
    assert comparison["claim_boundary"].startswith("cache-specific diagnostic")


def test_campaign_reports_rendered_prefill_cache_mode_and_measured_hits(tmp_path):
    summary = _serving_summary(
        tmp_path, label="rendered-cache", protocol="tcp", ttft=200.0, tps=100.0
    )
    payload = json.loads(summary.read_text(encoding="utf-8"))
    payload["benchmark_config"]["epd"]["rendered_prefill_cache"] = True
    payload["rendered_prefill_cache_hit_requests"] = 3
    payload["rendered_prefill_cache_miss_requests"] = 0
    payload["measured_online_direct_metric_summary"]["rendered_prefill_cache"] = {
        "hits": 4,
        "misses": 1,
    }
    summary.write_text(json.dumps(payload), encoding="utf-8")
    scenario = _scenario("rendered-cache", summary, "tcp")
    scenario["cache_expectations"] = {
        "rendered_prefill_cache_enabled": {"equals": True},
        "rendered_prefill_cache_hit_requests": {"min": 1},
        "rendered_prefill_cache_miss_requests": {"max": 0},
    }

    report = run_campaign(
        {
            "schema_version": 1,
            "campaign_id": "rendered-cache-observation",
            "scenarios": [scenario],
            "comparisons": [],
        },
        manifest_dir=tmp_path,
    )

    observed = report["scenarios"][0]
    assert observed["status"] == "passed"
    assert observed["cache_observations"]["rendered_prefill_cache_hits"] == 4.0
    assert observed["cache_observations"]["rendered_prefill_cache_hit_requests"] == 3.0


def test_tuning_ab_requires_explicit_and_narrow_config_difference(tmp_path):
    baseline = _serving_summary(
        tmp_path, label="agent-aware", protocol="tcp", ttft=320.0, tps=90.0
    )
    candidate = _serving_summary(
        tmp_path, label="least-loaded", protocol="tcp", ttft=260.0, tps=100.0
    )
    for path, policy in ((baseline, "agent_aware"), (candidate, "least_loaded")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["benchmark_config"]["epd"]["scheduler_policy"] = policy
        path.write_text(json.dumps(payload), encoding="utf-8")
    scenarios = [
        _scenario("baseline", baseline, "tcp"),
        _scenario("candidate", candidate, "tcp"),
    ]
    comparison = {
        "id": "least-loaded-vs-aware",
        "kind": "tuning_ab",
        "baseline": ["baseline"],
        "candidate": ["candidate"],
        "quality": "structural",
        "primary_metric": "avg_ttft_ms",
        "min_samples": 1,
    }

    undeclared = run_campaign(
        {
            "schema_version": 1,
            "campaign_id": "tuning-undeclared",
            "scenarios": scenarios,
            "comparisons": [comparison],
        },
        manifest_dir=tmp_path,
    )
    assert undeclared["comparisons"][0]["status"] == "incomplete"
    assert "benchmark_config_mismatch_after_allowed_protocol_difference" in undeclared[
        "comparisons"
    ][0]["failures"]

    comparison["allowed_config_differences"] = ["epd.scheduler_policy"]
    declared = run_campaign(
        {
            "schema_version": 1,
            "campaign_id": "tuning-declared",
            "scenarios": scenarios,
            "comparisons": [comparison],
        },
        manifest_dir=tmp_path,
    )
    result = declared["comparisons"][0]
    assert result["status"] == "passed"
    assert result["allowed_config_differences"] == ["epd.scheduler_policy"]

    invalid = validate_campaign_manifest(
        {
            "schema_version": 1,
            "campaign_id": "invalid-tuning-delta",
            "scenarios": scenarios,
            "comparisons": [
                {
                    **comparison,
                    "allowed_config_differences": ["request.model"],
                }
            ],
        }
    )
    assert any("unsupported allowed_config_difference: request.model" in item for item in invalid)


def test_capacity_scenario_requires_configured_workers_to_receive_measured_traffic(tmp_path):
    summary = _serving_summary(
        tmp_path, label="capacity", protocol="tcp", ttft=280.0, tps=110.0
    )
    payload = json.loads(summary.read_text(encoding="utf-8"))
    payload["benchmark_config"]["topology"] = {
        "prefill_gpus": [1, 5],
        "decode_gpus": [3, 6],
        "total_gpus": 4,
    }
    payload["prefill_worker_dispatches"] = {"prefill-0": 32}
    payload["decode_worker_dispatches"] = {"decode-0": 32}
    summary.write_text(json.dumps(payload), encoding="utf-8")
    scenario = _scenario("capacity", summary, "tcp")
    scenario.update(
        {
            "require_all_stage_workers_dispatched": True,
            "min_dispatch_entropy": 0.9,
        }
    )

    failed = run_campaign(
        {"schema_version": 1, "campaign_id": "capacity-fail", "scenarios": [scenario], "comparisons": []},
        manifest_dir=tmp_path,
    )
    failures = failed["scenarios"][0]["failures"]
    assert any("prefill_dispatch_coverage_incomplete" in item for item in failures)
    assert any("decode_dispatch_entropy_below_min" in item for item in failures)

    payload["prefill_worker_dispatches"] = {"prefill-0": 16, "prefill-1": 16}
    payload["decode_worker_dispatches"] = {"decode-0": 16, "decode-1": 16}
    summary.write_text(json.dumps(payload), encoding="utf-8")
    passed = run_campaign(
        {"schema_version": 1, "campaign_id": "capacity-pass", "scenarios": [scenario], "comparisons": []},
        manifest_dir=tmp_path,
    )

    observed = passed["scenarios"][0]
    assert observed["status"] == "passed"
    assert observed["worker_dispatch_balance"]["prefill"]["normalized_entropy"] == 1.0


def test_nvlink_topology_gate_rejects_non_nvlink_gpu_edge(tmp_path):
    hardware = tmp_path / "hardware.json"
    hardware.write_text(
        json.dumps(
            {
                "commands": {
                    "topology": {
                        "available": True,
                        "returncode": 0,
                        "stdout": "        GPU0 GPU1 GPU2\nGPU0     X   NV4  PIX\nGPU1   NV4    X  PIX\nGPU2   PIX  PIX    X\nLegend:\n",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    payload = {"raw_artifacts": {"hardware": str(hardware)}}

    failures, links = _required_nvlink_pair_failures(payload, [[0, 1]])
    assert failures == []
    assert links == [{"source_gpu": 0, "target_gpu": 1, "link": "NV4"}]

    failures, links = _required_nvlink_pair_failures(payload, [[0, 2]])
    assert failures == ["nvlink_topology_pair_0_2_is_PIX"]
    assert links[0]["link"] == "PIX"


def test_nvlink_topology_gate_parses_ansi_colored_nvidia_smi_header(tmp_path):
    hardware = tmp_path / "hardware-ansi.json"
    hardware.write_text(
        json.dumps(
            {
                "commands": {
                    "topology": {
                        "available": True,
                        "returncode": 0,
                        "stdout": (
                            "\t\x1b[4mGPU0\tGPU1\tGPU2\x1b[0m\n"
                            "GPU0\t X \tNV4\tPIX\n"
                            "GPU1\tNV4\t X \tPIX\n"
                            "GPU2\tPIX\tPIX\t X \n"
                            "Legend:\n"
                        ),
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    failures, links = _required_nvlink_pair_failures(
        {"raw_artifacts": {"hardware": str(hardware)}}, [[0, 1]]
    )

    assert failures == []
    assert links == [{"source_gpu": 0, "target_gpu": 1, "link": "NV4"}]


def test_campaign_manifest_validation_rejects_unknown_scenario_reference():
    failures = validate_campaign_manifest(
        {
            "schema_version": 1,
            "campaign_id": "bad",
            "scenarios": [
                {"id": "one", "kind": "pd_transport", "blocked_reason": "not run"}
            ],
            "comparisons": [
                {
                    "id": "bad-ref",
                    "kind": "transport_ab",
                    "baseline": ["one"],
                    "candidate": ["missing"],
                }
            ],
        }
    )

    assert "comparison[bad-ref] references unknown scenario: missing" in failures


def test_template_is_valid_and_cli_loader_supports_relative_summary(tmp_path):
    template = Path(__file__).resolve().parent.parent / "benchmarks" / "epd_benchmark_campaign.template.json"
    assert validate_campaign_manifest(json.loads(template.read_text(encoding="utf-8"))) == []

    summary = _serving_summary(tmp_path, label="relative", protocol="tcp", ttft=200.0, tps=100.0)
    manifest = {
        "schema_version": 1,
        "campaign_id": "relative",
        "scenarios": [
            {
                "id": "tcp",
                "kind": "pd_transport",
                "gate": "serving",
                "transport": "tcp",
                "summary": summary.name,
                "require_hardware_inventory": False,
            }
        ],
        "comparisons": [],
    }
    manifest_path = tmp_path / "campaign.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    report = load_and_run_campaign(manifest_path)

    assert report["coverage"]["complete"] is True
    assert report["scenarios"][0]["summary_path"] == str(summary)
