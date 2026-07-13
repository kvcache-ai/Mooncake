from __future__ import annotations

from mooncake_epd.core.control.vllm_transfer_primitives import (
    LayeredTransferWorkerMeta,
    chunk_transfer_descriptors,
    infer_descriptors_per_group,
    infer_group_count,
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
        receive_kv_response_messages=2,
        receive_kv_first_response_count=1,
        receive_kv_first_response_ms=10.0,
        receive_kv_last_response_count=1,
        receive_kv_last_response_ms=20.0,
        receive_kv_response_process_count=2,
        receive_kv_response_process_ms=0.5,
        backend_counts={"mooncake_engine_direct": 2},
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
        receive_kv_response_messages=3,
        receive_kv_first_response_count=1,
        receive_kv_first_response_ms=11.0,
        receive_kv_last_response_count=1,
        receive_kv_last_response_ms=21.0,
        receive_kv_response_process_count=3,
        receive_kv_response_process_ms=0.7,
        backend_counts={"mooncake_engine_direct": 1, "store": 1},
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
    assert merged.receive_kv_response_messages == 5
    assert merged.receive_kv_first_response_count == 2
    assert merged.receive_kv_first_response_ms == 21.0
    assert merged.receive_kv_last_response_count == 2
    assert merged.receive_kv_last_response_ms == 41.0
    assert merged.receive_kv_response_process_count == 5
    assert merged.receive_kv_response_process_ms == 1.2
    assert merged.backend_counts["mooncake_engine_direct"] == 3
    assert merged.backend_counts["store"] == 1


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
        receive_kv_response_messages=2,
        receive_kv_first_response_count=1,
        receive_kv_first_response_ms=2.5,
        receive_kv_last_response_count=1,
        receive_kv_last_response_ms=8.5,
        receive_kv_response_process_count=2,
        receive_kv_response_process_ms=0.25,
        backend_counts={"peer_buffer_direct": 1},
    )

    payload = meta.to_dict()
    restored = LayeredTransferWorkerMeta.from_dict(payload)

    assert meta.is_empty() is False
    assert restored == meta
    assert LayeredTransferWorkerMeta().is_empty() is True
