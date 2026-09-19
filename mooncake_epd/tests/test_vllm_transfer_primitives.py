from __future__ import annotations

from mooncake_epd.core.control.vllm_transfer_primitives import (
    LayeredTransferWorkerMeta,
    chunk_transfer_descriptors,
    coalesce_transfer_descriptors,
    infer_descriptors_per_group,
    infer_group_count,
    is_retryable_transfer_failure,
)


def test_infer_group_count_and_descriptors():
    assert infer_group_count(total_regions=36, layers_per_group=4) == 9
    assert infer_descriptors_per_group(
        total_descriptors=90,
        total_regions=36,
        layers_per_group=4,
    ) == 10



def test_chunk_transfer_descriptors_by_descriptor_count():
    groups = chunk_transfer_descriptors(
        src_ptrs=list(range(10)),
        dst_ptrs=list(range(100, 110)),
        lengths=[8] * 10,
        descriptors_per_group=3,
    )
    assert [len(src) for src, _, _ in groups] == [3, 3, 3, 1]
    assert sum(sum(lengths) for _, _, lengths in groups) == 80



def test_chunk_transfer_descriptors_by_bytes_budget():
    groups = chunk_transfer_descriptors(
        src_ptrs=[1, 2, 3, 4],
        dst_ptrs=[11, 12, 13, 14],
        lengths=[16, 16, 64, 16],
        descriptors_per_group=10,
        max_group_bytes=32,
    )
    assert [sum(lengths) for _, _, lengths in groups] == [32, 64, 16]


def test_retryable_transfer_failure_is_conservative():
    assert is_retryable_transfer_failure(-110, None) is True
    assert is_retryable_transfer_failure(-1, "connection reset by peer") is True
    assert is_retryable_transfer_failure(-1, "connection reset during registration") is True
    assert is_retryable_transfer_failure(-1, "destination MR is out of bounds") is False
    assert is_retryable_transfer_failure(-22, "invalid buffer address") is False
    assert is_retryable_transfer_failure(-1, "unknown transfer failure") is False
    assert is_retryable_transfer_failure(0, "timeout") is False


def test_coalesce_transfer_descriptors_merges_interleaved_same_region_runs():
    result = coalesce_transfer_descriptors(
        src_ptrs=[100, 500, 132, 532],
        dst_ptrs=[1000, 5000, 1032, 5032],
        lengths=[32, 32, 32, 32],
        coalesce_keys=[("layer-0",), ("layer-1",), ("layer-0",), ("layer-1",)],
        descriptor_paths=["EPD", "EPD", "EPD", "EPD"],
    )

    assert result.src_ptrs == [100, 500]
    assert result.dst_ptrs == [1000, 5000]
    assert result.lengths == [64, 64]
    assert result.descriptor_paths == ["EPD", "EPD"]
    assert result.input_descriptors == 4
    assert result.output_descriptors == 2
    assert result.coalesced_descriptors == 2
    assert result.total_bytes == 128


def test_coalesce_transfer_descriptors_requires_same_provenance_and_path():
    result = coalesce_transfer_descriptors(
        src_ptrs=[100, 132, 164],
        dst_ptrs=[1000, 1032, 1064],
        lengths=[32, 32, 32],
        coalesce_keys=[("owner-a",), ("owner-b",), ("owner-b",)],
        descriptor_paths=["EPD", "EPD", "PD"],
    )

    assert result.src_ptrs == [100, 132, 164]
    assert result.dst_ptrs == [1000, 1032, 1064]
    assert result.lengths == [32, 32, 32]
    assert result.coalesced_descriptors == 0


def test_coalesce_transfer_descriptors_rejects_one_sided_contiguity():
    result = coalesce_transfer_descriptors(
        src_ptrs=[100, 132],
        dst_ptrs=[1000, 1064],
        lengths=[32, 32],
        coalesce_keys=[("same-region",), ("same-region",)],
        descriptor_paths=["EPD", "EPD"],
    )

    assert result.output_descriptors == 2
    assert result.coalesced_descriptors == 0


def test_coalesce_transfer_descriptors_without_explicit_keys_is_noop():
    result = coalesce_transfer_descriptors(
        src_ptrs=[100, 132],
        dst_ptrs=[1000, 1032],
        lengths=[32, 32],
        descriptor_paths=["EPD", "EPD"],
    )

    assert result.src_ptrs == [100, 132]
    assert result.dst_ptrs == [1000, 1032]
    assert result.lengths == [32, 32]
    assert result.coalesced_descriptors == 0


def test_coalesce_transfer_descriptors_validates_parallel_metadata():
    import pytest

    with pytest.raises(ValueError, match="identical lengths"):
        coalesce_transfer_descriptors(
            src_ptrs=[1, 2],
            dst_ptrs=[11],
            lengths=[8, 8],
        )
    with pytest.raises(ValueError, match="coalesce_keys"):
        coalesce_transfer_descriptors(
            src_ptrs=[1],
            dst_ptrs=[11],
            lengths=[8],
            coalesce_keys=[],
        )
    with pytest.raises(ValueError, match="positive"):
        coalesce_transfer_descriptors(
            src_ptrs=[1],
            dst_ptrs=[11],
            lengths=[0],
            coalesce_keys=[("region",)],
        )



def test_layered_transfer_worker_meta_aggregate():
    left = LayeredTransferWorkerMeta(
        grouped_batches=2,
        grouped_bytes=128,
        grouped_descriptors=4,
        peer_buffer_batches=2,
        peer_buffer_bytes=128,
        received_group_batches=1,
        received_finished_reqs=1,
        layer_wait_calls=2,
        layer_wait_ms=3.5,
        transfer_attempts=2,
        transfer_successes=2,
        transfer_bytes=128,
        transfer_elapsed_ms=2.0,
        transfer_attempt_elapsed_ms=2.0,
        descriptor_build_calls=2,
        descriptor_build_input_descriptors=8,
        descriptor_build_output_descriptors=4,
        coalesced_descriptors=4,
        descriptor_build_ms=1.5,
        topology_incarnation_observations=2,
        topology_incarnation_refreshes=1,
        topology_incarnation_pending_waits=1,
        topology_incarnation_wait_ms=0.5,
        backend_counts={"mooncake_engine_direct": 2},
        backend_bytes={"mooncake_engine_direct": 128},
        backend_elapsed_ms={"mooncake_engine_direct": 2.0},
    )
    right = LayeredTransferWorkerMeta(
        grouped_batches=1,
        grouped_bytes=64,
        grouped_descriptors=2,
        failed_batches=1,
        fallback_batches=1,
        fallback_bytes=64,
        received_group_batches=2,
        received_finished_reqs=3,
        layer_wait_calls=1,
        layer_wait_ms=1.5,
        receive_failures=1,
        transfer_attempts=2,
        transfer_successes=1,
        transfer_bytes=64,
        transfer_elapsed_ms=2.0,
        transfer_attempt_elapsed_ms=3.0,
        descriptor_build_calls=1,
        descriptor_build_input_descriptors=4,
        descriptor_build_output_descriptors=3,
        coalesced_descriptors=1,
        descriptor_build_ms=0.5,
        topology_incarnation_observations=3,
        topology_incarnation_refreshes=2,
        topology_incarnation_pending_waits=1,
        topology_incarnation_wait_ms=1.5,
        backend_counts={"mooncake_engine_direct": 1, "store": 1},
        backend_bytes={"mooncake_engine_direct": 64},
        backend_elapsed_ms={"mooncake_engine_direct": 2.0},
        backend_failures={"store": 1},
    )
    merged = left.aggregate(right)
    assert merged.grouped_batches == 3
    assert merged.grouped_bytes == 192
    assert merged.grouped_descriptors == 6
    assert merged.failed_batches == 1
    assert merged.peer_buffer_batches == 2
    assert merged.peer_buffer_bytes == 128
    assert merged.fallback_batches == 1
    assert merged.fallback_bytes == 64
    assert merged.received_group_batches == 3
    assert merged.received_finished_reqs == 4
    assert merged.layer_wait_calls == 3
    assert merged.layer_wait_ms == 5.0
    assert merged.receive_failures == 1
    assert merged.transfer_attempts == 4
    assert merged.transfer_successes == 3
    assert merged.transfer_bytes == 192
    assert merged.transfer_elapsed_ms == 4.0
    assert merged.transfer_attempt_elapsed_ms == 5.0
    assert merged.descriptor_build_calls == 3
    assert merged.descriptor_build_input_descriptors == 12
    assert merged.descriptor_build_output_descriptors == 7
    assert merged.coalesced_descriptors == 5
    assert merged.descriptor_build_ms == 2.0
    assert merged.topology_incarnation_observations == 5
    assert merged.topology_incarnation_refreshes == 3
    assert merged.topology_incarnation_pending_waits == 2
    assert merged.topology_incarnation_wait_ms == 2.0
    assert merged.backend_counts["mooncake_engine_direct"] == 3
    assert merged.backend_counts["store"] == 1
    assert merged.backend_bytes["mooncake_engine_direct"] == 192
    assert merged.backend_elapsed_ms["mooncake_engine_direct"] == 4.0
    assert merged.backend_failures["store"] == 1


def test_layered_transfer_worker_meta_roundtrip_and_empty_detection():
    meta = LayeredTransferWorkerMeta(
        grouped_batches=1,
        grouped_bytes=256,
        grouped_descriptors=3,
        failed_batches=0,
        peer_buffer_batches=1,
        peer_buffer_bytes=256,
        fallback_batches=0,
        fallback_bytes=0,
        accumulated_group_delay_ms=1.5,
        received_group_batches=2,
        received_finished_reqs=1,
        layer_wait_calls=3,
        layer_wait_ms=4.5,
        receive_failures=0,
        transfer_attempts=1,
        transfer_successes=1,
        transfer_bytes=256,
        transfer_elapsed_ms=2.0,
        transfer_attempt_elapsed_ms=2.0,
        descriptor_build_calls=1,
        descriptor_build_input_descriptors=6,
        descriptor_build_output_descriptors=3,
        coalesced_descriptors=3,
        descriptor_build_ms=1.25,
        topology_incarnation_observations=2,
        topology_incarnation_refreshes=1,
        topology_incarnation_pending_waits=1,
        topology_incarnation_wait_ms=0.25,
        backend_counts={"peer_buffer_direct": 1},
        backend_bytes={"peer_buffer_direct": 256},
        backend_elapsed_ms={"peer_buffer_direct": 2.0},
    )

    payload = meta.to_dict()
    restored = LayeredTransferWorkerMeta.from_dict(payload)

    assert payload["transfer_elapsed_ms_avg"] == 2.0
    assert payload["transfer_attempt_elapsed_ms_avg"] == 2.0
    assert payload["transfer_bandwidth_gbps"] == 0.001024
    assert payload["backend_bandwidth_gbps"]["peer_buffer_direct"] == 0.001024
    assert payload["descriptor_build_ms_avg"] == 1.25
    assert payload["descriptor_reduction_ratio"] == 0.5
    assert payload["descriptors_per_mb"] == 12288.0
    assert meta.is_empty() is False
    assert restored == meta
    assert LayeredTransferWorkerMeta().is_empty() is True


def test_layered_transfer_worker_meta_old_snapshot_reports_unavailable_bandwidth():
    restored = LayeredTransferWorkerMeta.from_dict(
        {
            "grouped_batches": 2,
            "grouped_bytes": 1024,
            "backend_counts": {"peer_buffer_direct": 2},
        }
    )

    payload = restored.to_dict()
    assert payload["transfer_attempts"] == 0
    assert payload["transfer_successes"] == 0
    assert payload["transfer_bandwidth_gbps"] is None
    assert payload["transfer_elapsed_ms_avg"] is None
