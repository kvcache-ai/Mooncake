# RFC Evaluation Report

## Main Results

| Metric | Value |
| --- | ---: |
| baseline_avg_ms | 100.00 |
| baseline_avg_tps | 10.00 |
| epd_ttft_avg_ms | 18.00 |
| epd_total_avg_ms | 35.00 |
| epd_decode_tps_avg | 31.00 |
| cross_step_reuse_ratio | 0.7500 |
| failures | 0 |
| serving_e2e_pass | true |
| serving_e2e_sanity_pass | true |
| serving_e2e_path_split_ok | true |
| serving_e2e_transfer_path_split_ok | true |
| serving_e2e_connector_path_totals_conserved | true |
| serving_e2e_connector_workers | 2 |
| serving_e2e_registry_events | 4 |
| serving_mm_prefetch_hot_path_ok | true |
| serving_mm_prefetch_completed | 0 |
| serving_mm_hidden_cache_hot_path_ok | false |
| serving_mm_hidden_cache_encoder_skip_ok | false |
| serving_mm_hidden_cache_hits | 0 |
| serving_mm_hidden_cache_hit_rate | 0.0000 |
| serving_cross_step_reuse_hot_path_ok | true |
| serving_cross_step_reuse_candidates | 0 |
| serving_cross_step_reused_tokens | 0 |
| serving_dataset_goodput_rps | 0.0000 |
| serving_dataset_deadline_miss_ratio | 0.0000 |
| feature_handle_e2e_available | false |
| feature_handle_e2e_pass | false |
| feature_handle_precomputed_hits | 0 |
| feature_handle_vision_compute_ms_avg | 0.0000 |
| feature_handle_peer_buffer_direct_batches | 0 |
| feature_handle_fallback_batches | 0 |
| feature_handle_layered_receive_failures | 0 |
| feature_handle_paired_available | false |
| feature_handle_paired_pass | false |
| feature_handle_paired_ttft_reduction_pct | 0.0000 |
| feature_handle_paired_goodput_gain_pct | 0.0000 |
| feature_handle_paired_feature_precomputed_hits | 0 |
| serving_correctness_available | false |
| serving_correctness_pass | false |
| serving_correctness_count | 0 |
| serving_correctness_avg_token_jaccard | 0.0000 |
| serving_correctness_same_finish_reason_rate | 0.0000 |
| ablation_ordering_pass | true |
| ttft_gain_b2_vs_b1_ms | 40.00 |
| ttft_gain_b7_vs_b3_ms | 30.00 |
| ttft_penalty_b8_vs_b7_ms | 90.00 |

## Stage Breakdown

| Stage | Avg ms |
| --- | ---: |
| baseline_total | 100.00 |
| epd_encode | 5.00 |
| epd_prefill | 6.00 |
| epd_ttft | 20.00 |
| epd_decode | 7.00 |
| epd_total | 40.00 |

## Cache Reuse

| Metric | Value |
| --- | ---: |
| cross_step_avg_reuse_ratio | 0.7500 |
| avg_matched_tokens | 75.00 |
| avg_delta_tokens | 25.00 |

### Reuse by Scenario

| Scenario | Avg reuse ratio |
| --- | ---: |
| multi_turn | 0.7500 |

## Reliability

| Metric | Value |
| --- | ---: |
| failures | 0 |
| samples | 2 |
| gpu_peak_enc_gb | 1.00 |
| gpu_peak_pre_gb | 2.00 |
| gpu_peak_dec_gb | 3.00 |
| gpu_peak_base_gb | 4.00 |

## Scheduler / Backpressure

| Metric | Value |
| --- | ---: |
| backpressure_events | 0 |
| reject_events | 0 |

## Transport / Prefetch

| Metric | Value |
| --- | ---: |
| kv_transfer_gbps | 0.00 |
| layered_transfer_batches_avg | 0.00 |
| mm_prefetch_hit_rate | 0.0000 |
| mm_recompute_fallbacks | 0 |

## Serving E2E

| Metric | Value |
| --- | ---: |
| available | true |
| pd_route_ok | true |
| epd_route_ok | true |
| path_split_ok | true |
| transfer_path_split_ok | true |
| workflow_registry_enabled | true |
| workflow_registry_events | 4 |
| connector_metric_workers | 2 |
| layered_transfer_grouped_batches | 2 |
| layered_receive_group_batches | 2 |
| peer_buffer_batches | 2 |
| pd_transfer_grouped_descriptors | 1 |
| epd_transfer_grouped_descriptors | 7 |
| pd_transfer_grouped_bytes | 64 |
| epd_transfer_grouped_bytes | 448 |
| e2e_backend:peer_buffer_direct | 2 |

## Serving E2E Sanity

| Metric | Value |
| --- | ---: |
| sanity_pass | true |
| connector_workers_ok | true |
| group_batch_parity_ok | true |
| finished_request_parity_ok | true |
| peer_backend_parity_ok | true |
| layer_wait_present | true |
| receive_failures_zero | true |
| group_batches_multiple_of_requests | true |
| wait_calls_multiple_of_receive_batches | true |
| descriptors_multiple_of_group_batches | true |
| wait_calls_per_receive_batch | 2.00 |
| descriptors_per_group_batch | 4.00 |
| group_batches_per_request | 1.00 |

## Serving Correctness vs Baseline

| Metric | Value |
| --- | ---: |
| available | false |
| pass_recommendation | false |
| pass_recommendation_label |  |
| count | 0 |
| exact_match_rate | 0.0000 |
| normalized_exact_match_rate | 0.0000 |
| same_finish_reason_rate | 0.0000 |
| normalized_exact_or_high_overlap_rate | 0.0000 |
| control_plane_equivalent_rate | 0.0000 |
| structural_completion_equivalent_rate | 0.0000 |
| avg_token_jaccard | 0.0000 |
| min_token_jaccard | 0.0000 |
| high_overlap_threshold | 0.0000 |
| min_pass_rate | 0.0000 |
| min_same_finish_reason_rate | 0.0000 |
| min_avg_token_jaccard_threshold | 0.0000 |
| min_token_jaccard_threshold | 0.0000 |
| baseline_elapsed_ms_avg | 0.00 |
| serving_elapsed_ms_avg | 0.00 |

## TTFT Gap Decomposition

| Comparison | Type | Base | Target | Base TTFT ms | Target TTFT ms | Effect ms | Effect % |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| Native PD → Prefix Cache | gain | B1 | B2 | 100.00 | 60.00 | 40.00 | 40.00 |
| Native PD → Feature Cache | gain | B1 | B3 | 100.00 | 80.00 | 20.00 | 20.00 |
| Feature Cache → Exact Reuse | gain | B3 | B7 | 80.00 | 50.00 | 30.00 | 37.50 |
| Exact Reuse → Approx Reuse | penalty | B7 | B8 | 50.00 | 140.00 | 90.00 | 180.00 |

## Ablation Ordering Sanity

| Metric | Value |
| --- | ---: |
| ordering_pass | true |

| Rule | LHS | RHS | Expect | Delta ms (lhs-rhs) | Pass |
| --- | --- | --- | --- | ---: | ---: |
| Prefix Cache should beat Native PD | B2 | B1 | lt | -40.00 | true |
| Feature Cache should beat Native PD | B3 | B1 | lt | -20.00 | true |
| Exact Reuse should beat Feature Cache | B7 | B3 | lt | -30.00 | true |
| Approx Reuse currently carries TTFT penalty | B8 | B7 | gt | 90.00 | true |

## B8 Approx Reuse Penalty

| Metric | Value |
| --- | ---: |
| penalty_ms | 90.00 |
| penalty_pct | 180.00 |
| exact_reuse_ratio | 0.0000 |
| approx_reuse_ratio | 0.0000 |
| delta_reuse_ratio_avg | 0.0000 |
| tier2_reused_tokens_avg | 0.00 |
| tier2_recomputed_tokens_avg | 0.00 |
| tier3_accepted_pages_avg | 0.00 |
| relay_segments_avg | 0.00 |
| relay_recompute_segments_avg | 0.00 |
| delta_prefill_ms_avg | 0.00 |
| pipeline_overhead_ms_avg | 0.00 |
| reuse_pipeline_ms_avg | 0.00 |
| dominant_cost_bucket | delta_prefill_ms |
| delta_prefill_share_of_b8_ttft | 0.0000 |
| pipeline_overhead_share_of_b8_ttft | 0.0000 |
| corr_penalty_reuse_ratio | 0.0000 |
| corr_penalty_delta_reuse_ratio | 0.0000 |
| corr_penalty_tier3_pages | 0.0000 |
| corr_penalty_tier2_recomputed_tokens | 0.0000 |
| corr_penalty_delta_prefill_ms | 0.0000 |
| corr_penalty_pipeline_overhead_ms | 0.0000 |
| corr_penalty_relay_segments | 0.0000 |
| corr_penalty_relay_recompute_segments | 0.0000 |
| tier2_reused_tokens_constant | false |
| max_penalty_scenario | None:None:0.00 |
| min_penalty_scenario | None:None:0.00 |

## Dataset

| Metric | Value |
| --- | ---: |
| workflow_traces | 1 |
| soak_examples | 8 |
| unique_images | 2 |
| scenario:multi_turn | 4 |
| scenario:tool_use_vqa | 4 |

## RFC Workloads (dev-small)

| Workload | Samples | Avg steps | Image reuse rate | Scenarios |
| --- | ---: | ---: | ---: | --- |
| W0 | 8 | 1.00 | 0.00 | a2a_handoff, multi_turn, tool_use_vqa, tree_of_thought |
| W1 | 2 | 4.00 | 1.00 | multi_turn |
| W2 | 2 | 4.00 | 1.00 | tool_use_vqa |
| W3 | 3 | 3.00 | 1.00 | tree_of_thought |
| W4 | 1 | 3.00 | 1.00 | a2a_handoff |
| W5 | 20 | 3.00 | 0.75 | a2a_handoff, multi_turn, tool_use_vqa, tree_of_thought |

## Baseline Matrix

| ID | Name | Available | Latency ms | Decode TPS | Purpose |
| --- | --- | ---: | ---: | ---: | --- |
| B0 | Single-node colocated | yes | 100.00 | 10.00 | 赛题要求的单机端到端基线 |
| B1 | Native PD | yes | 100.00 | - | 验证 EPD 相对传统 PD 的增益 |
| B2 | PD + Prefix Cache | yes | 60.00 | - | 隔离文本前缀缓存收益 |
| B3 | PD + Feature Cache | yes | 80.00 | - | 隔离视觉特征缓存收益 |
| B4 | Naive EPD | yes | 20.00 | 30.00 | 验证 TransferPolicy 与页级状态管理收益 |
| B5 | EPD + Deep-copy Fork | yes | 75.00 | - | 验证页级引用计数 + CoW 的收益 |
| B6 | EPD + CoW no cross-step | yes | 70.00 | - | 隔离跨步骤复用收益 |
| B7 | EPD + Exact Prefix | yes | 50.00 | - | 验证安全复用上界 |
| B8 | EPD + Approx Reuse | yes | 140.00 | - | 验证激进复用策略收益与风险 |
| B9 | Full System | yes | 35.00 | 31.00 | 最终方案 |
