from __future__ import annotations

import asyncio
import threading
import time
from types import SimpleNamespace

import pytest

pytest.importorskip("vllm")
from vllm.v1.request import RequestStatus  # noqa: E402

from mooncake_epd.core.control.vllm_mooncake_connector import (  # noqa: E402
    LayeredMooncakeXferResponse,
    MooncakeConnector,
    MooncakeConnectorMetadata,
    MooncakeXferResponseStatus,
    EPDMooncakeConnectorScheduler,
    UpstreamMooncakeConnectorScheduler,
    UpstreamMooncakeConnectorWorker,
    _LayeredReceiveState,
    _LayeredSendState,
    EPDMooncakeConnectorWorker,
)
from mooncake_epd.core.control.connector_metrics import (  # noqa: E402
    ConnectorMetricsReader,
    ConnectorMetricsSink,
)
from mooncake_epd.core.control.vllm_transfer_primitives import LayeredTransferWorkerMeta  # noqa: E402


def _make_consumer_worker() -> EPDMooncakeConnectorWorker:
    worker = object.__new__(EPDMooncakeConnectorWorker)
    worker.layered_kv_transfer = True
    worker.trace_layered_kv = False
    worker.is_kv_producer = False
    worker.is_kv_consumer = True
    worker._worker_meta = LayeredTransferWorkerMeta()
    worker._layer_to_group = {"layer0": 0, "layer1": 0, "layer2": 1}
    worker.layer_load_timeout_seconds = 1.0
    worker._layered_recv_lock = threading.RLock()
    worker._layered_recv_states = {
        "req-0": _LayeredReceiveState.create(
            request_id="req-0",
            transfer_id="xfer-0",
            total_groups=2,
            expected_tasks=1,
        )
    }
    worker._current_recv_req_ids = ["req-0"]
    worker.finished_recving_reqs = set()
    worker._connector_metrics_pending_by_path = {}
    worker._recv_request_routing_paths = {}
    worker._recv_transfer_routing_paths = {}
    worker._send_request_routing_paths = {}
    worker._send_transfer_routing_paths = {}
    worker._remote_engine_incarnations = {}
    worker._remote_incarnation_lock = asyncio.Lock()
    worker._remote_agents = {}
    worker._tp_size = {"decode-engine": 1}
    worker._pending_bootstrap_queries = {}
    worker.async_zmq_ctx = type("Ctx", (), {"term": lambda self: None})()
    worker.receiver_loop = type(
        "Loop",
        (),
        {"is_running": lambda self: False, "call_soon_threadsafe": lambda *args, **kwargs: None},
    )()
    worker.shutdown = lambda: None
    return worker


def _make_producer_worker() -> EPDMooncakeConnectorWorker:
    worker = object.__new__(EPDMooncakeConnectorWorker)
    worker.layered_kv_transfer = True
    worker.trace_layered_kv = False
    worker.is_kv_consumer = False
    worker.is_kv_producer = True
    worker.layers_per_group = 2
    worker._sender_group_count = 2
    worker._layer_names = ["layer0", "layer1", "layer2"]
    worker._layer_to_group = {"layer0": 0, "layer1": 0, "layer2": 1}
    worker._layer_to_index = {"layer0": 0, "layer1": 1, "layer2": 2}
    worker._layered_send_lock = threading.RLock()
    worker._layered_send_states = {
        "xfer-0": _LayeredSendState.create("xfer-0", 2),
    }
    worker._current_send_transfer_ids = set()
    worker.reqs_need_send = {}
    worker.async_zmq_ctx = type("Ctx", (), {"term": lambda self: None})()
    worker.sender_loop = type(
        "Loop",
        (),
        {"is_running": lambda self: False, "call_soon_threadsafe": lambda *args, **kwargs: None},
    )()
    worker._sender_executor = type("Exec", (), {"shutdown": lambda *args, **kwargs: None})()
    worker.transport_backend = "mooncake_engine_direct"
    worker.group_delay_ms = 0.0
    worker.max_group_bytes = 0
    worker._registered_region_count = 4
    worker._worker_meta = LayeredTransferWorkerMeta()
    worker._connector_metrics_pending_by_path = {}
    worker._recv_request_routing_paths = {}
    worker._recv_transfer_routing_paths = {}
    worker._send_request_routing_paths = {}
    worker._send_transfer_routing_paths = {}
    worker.xfer_stats = type(
        "Stats",
        (),
        {
            "record_transfer": lambda *args, **kwargs: None,
            "record_failed_transfer": lambda *args, **kwargs: None,
        },
    )()
    worker._trace = lambda *args, **kwargs: None
    worker.shutdown = lambda: None
    return worker


def test_layered_connector_requires_piecewise_for_cudagraph():
    assert MooncakeConnector.requires_piecewise_for_cudagraph({}) is False
    assert MooncakeConnector.requires_piecewise_for_cudagraph(
        {"layered_kv_transfer": True}
    ) is True


def test_wait_for_layer_load_blocks_until_group_event_arrives():
    worker = _make_consumer_worker()
    state = worker._layered_recv_states["req-0"]  # noqa: SLF001

    def _ack():
        time.sleep(0.05)
        state.ack_group(0)

    thread = threading.Thread(target=_ack, daemon=True)
    thread.start()
    worker.wait_for_layer_load("layer0")
    thread.join(timeout=1.0)
    assert state.group_events[0].is_set()
    assert worker._worker_meta.layer_wait_calls == 1  # noqa: SLF001
    assert worker._worker_meta.layer_wait_ms > 0.0  # noqa: SLF001


def test_save_kv_layer_only_announces_group_tail():
    worker = _make_producer_worker()
    state = worker._layered_send_states["xfer-0"]  # noqa: SLF001

    worker.save_kv_layer("layer0", None, None)
    assert state.group_ready_events[0].is_set() is False

    worker.save_kv_layer("layer1", None, None)
    assert state.group_ready_events[0].is_set() is True

    worker.save_kv_layer("layer2", None, None)
    assert state.group_ready_events[1].is_set() is True


def test_layered_receive_state_can_resize_groups_without_losing_progress():
    state = _LayeredReceiveState.create(
        request_id="req-0",
        transfer_id="xfer-0",
        total_groups=2,
        expected_tasks=1,
    )
    state.ack_group(0)
    assert state.group_events[0].is_set() is True

    state.ensure_total_groups(3)

    assert state.total_groups == 3
    assert len(state.group_events) == 3
    assert state.group_events[0].is_set() is True
    assert state.group_events[1].is_set() is False
    assert state.group_events[2].is_set() is False


def test_process_pulling_result_aligns_total_groups_from_producer():
    worker = _make_consumer_worker()
    response = LayeredMooncakeXferResponse(
        status=MooncakeXferResponseStatus.CONTINUE,
        ok_reqs=["req-0"],
        group_index=0,
        total_groups=3,
    )

    worker.process_pulling_result(response, {"req-0": None})  # type: ignore[arg-type]

    state = worker._layered_recv_states["req-0"]  # noqa: SLF001
    assert state.total_groups == 3
    assert state.group_events[0].is_set() is True
    assert worker._worker_meta.received_group_batches == 1  # noqa: SLF001


def test_process_pulling_result_uses_decode_request_id_and_does_not_crash_on_trace():
    worker = _make_consumer_worker()
    worker._layered_recv_states = {
        "prefill-req-0": _LayeredReceiveState.create(
            request_id="prefill-req-0",
            transfer_id="xfer-0",
            total_groups=1,
            expected_tasks=1,
        )
    }
    worker._current_recv_req_ids = ["prefill-req-0"]
    worker.finished_recving_reqs = set()
    worker._recv_request_routing_paths = {"prefill-req-0": "EPD"}

    pull_meta = type("PullMeta", (), {"d_req_id": "decode-req-0"})()
    response = LayeredMooncakeXferResponse(
        status=MooncakeXferResponseStatus.FINISH,
        ok_reqs=["prefill-req-0"],
        group_index=0,
        total_groups=1,
    )

    worker.process_pulling_result(response, {"prefill-req-0": pull_meta})  # type: ignore[arg-type]

    assert "decode-req-0" in worker.finished_recving_reqs
    assert worker._worker_meta.received_finished_reqs == 1  # noqa: SLF001
    assert worker._connector_metrics_pending_by_path["EPD"].received_finished_reqs == 1  # noqa: SLF001
    assert "UNKNOWN" not in worker._connector_metrics_pending_by_path  # noqa: SLF001


def test_process_pulling_result_does_not_double_count_finished_request():
    worker = _make_consumer_worker()
    response0 = LayeredMooncakeXferResponse(
        status=MooncakeXferResponseStatus.CONTINUE,
        ok_reqs=["req-0"],
        group_index=0,
        total_groups=2,
    )
    response1 = LayeredMooncakeXferResponse(
        status=MooncakeXferResponseStatus.FINISH,
        ok_reqs=["req-0"],
        group_index=1,
        total_groups=2,
    )

    worker.process_pulling_result(response0, {"req-0": None})  # type: ignore[arg-type]
    worker.process_pulling_result(response1, {"req-0": None})  # type: ignore[arg-type]
    worker.process_pulling_result(response1, {"req-0": None})  # type: ignore[arg-type]

    assert worker._worker_meta.received_finished_reqs == 1  # noqa: SLF001


def test_record_send_reqs_creates_ready_placeholder_for_early_layered_send():
    worker = _make_producer_worker()
    metadata = MooncakeConnectorMetadata()
    metadata.reqs_to_send["req-0"] = ("xfer-ready", [[10, 11], [20]])

    asyncio.run(worker.record_send_reqs(metadata))

    send_meta = worker.reqs_need_send["xfer-ready"]
    assert send_meta.p_req_id == "req-0"
    assert send_meta.local_block_ids == [[10, 11], [20]]
    assert send_meta.ready.is_set() is True
    assert "xfer-ready" in worker._layered_send_states  # noqa: SLF001


def test_record_send_reqs_keeps_pending_placeholder_unready():
    worker = _make_producer_worker()
    metadata = MooncakeConnectorMetadata()
    metadata.reqs_to_send["req-0"] = ("xfer-pending", [])

    asyncio.run(worker.record_send_reqs(metadata))

    send_meta = worker.reqs_need_send["xfer-pending"]
    assert send_meta.p_req_id == "req-0"
    assert send_meta.local_block_ids == []
    assert send_meta.ready.is_set() is False


def test_record_send_reqs_keeps_all_empty_groups_unready():
    worker = _make_producer_worker()
    metadata = MooncakeConnectorMetadata()
    metadata.reqs_to_send["req-0"] = ("xfer-empty-groups", [[], []])

    asyncio.run(worker.record_send_reqs(metadata))

    send_meta = worker.reqs_need_send["xfer-empty-groups"]
    assert send_meta.local_block_ids == [[], []]
    assert send_meta.ready.is_set() is False


def test_record_send_reqs_does_not_drop_ready_transfer_without_matching_not_processed_flag():
    worker = _make_producer_worker()
    metadata = MooncakeConnectorMetadata()
    metadata.reqs_to_send["req-ready"] = ("xfer-ready", [[10, 11]])
    metadata.reqs_not_processed.add("xfer-other")

    asyncio.run(worker.record_send_reqs(metadata))

    assert "xfer-ready" in worker.reqs_need_send
    assert worker.reqs_need_send["xfer-ready"].ready.is_set() is True
    assert worker.reqs_need_send.get("xfer-other") is None


def test_scheduler_build_connector_meta_captures_routing_paths():
    scheduler = object.__new__(EPDMooncakeConnectorScheduler)
    scheduler.is_kv_producer = True
    scheduler.is_kv_consumer = False
    scheduler._reqs_need_recv = {}
    scheduler._reqs_need_send = {
        "req-epd": (
            type(
                "Req",
                (),
                {
                    "kv_transfer_params": {
                        "transfer_id": "xfer-epd",
                        "routing_path": "EPD",
                    }
                },
            )(),
            [[1, 2]],
        )
    }
    scheduler._reqs_not_processed = set()

    meta = scheduler.build_connector_meta(None)

    assert meta.request_routing_paths["req-epd"] == "EPD"
    assert meta.transfer_routing_paths["xfer-epd"] == "EPD"


def test_scheduler_request_finished_exports_prefill_process_incarnation(monkeypatch):
    scheduler = object.__new__(EPDMooncakeConnectorScheduler)
    scheduler.engine_id = "prefill-engine"
    scheduler.remote_bootstrap_addr = "http://prefill-bootstrap:8998"
    scheduler.tp_size = 1
    scheduler.get_sw_clipped_blocks = lambda block_ids: list(block_ids)
    monkeypatch.setattr(
        UpstreamMooncakeConnectorScheduler,
        "request_finished",
        lambda *_args, **_kwargs: (True, None),
    )
    request = SimpleNamespace(
        status=RequestStatus.FINISHED_LENGTH_CAPPED,
        kv_transfer_params={
            "transfer_id": "xfer-incarnation",
            "do_remote_decode": True,
            "do_remote_prefill": False,
        },
    )

    _, params = scheduler.request_finished(request, ([1, 2],))

    assert params is not None
    assert params["remote_engine_id"] == "prefill-engine"
    assert params["remote_engine_incarnation"]


def test_scheduler_build_connector_meta_captures_remote_engine_incarnation():
    scheduler = object.__new__(EPDMooncakeConnectorScheduler)
    scheduler.is_kv_producer = False
    scheduler.is_kv_consumer = True
    scheduler._reqs_need_send = {}
    scheduler._reqs_need_recv = {
        "req-incarnation": (
            SimpleNamespace(
                kv_transfer_params={
                    "transfer_id": "xfer-incarnation",
                    "remote_engine_id": "prefill-engine",
                    "remote_engine_incarnation": "prefill-token-a",
                    "remote_bootstrap_addr": "http://prefill-bootstrap:8998",
                    "routing_path": "EPD",
                }
            ),
            [[1, 2]],
        )
    }
    scheduler._reqs_not_processed = set()

    meta = scheduler.build_connector_meta(None)

    assert meta.remote_engine_incarnations == {
        "prefill-engine": "prefill-token-a"
    }


def test_consumer_prefill_incarnation_fence_invalidates_only_changed_engine():
    worker = _make_consumer_worker()
    worker._remote_engine_incarnations = {
        "prefill-a": "token-a-old",
        "prefill-b": "token-b",
    }
    worker._remote_agents = {
        "prefill-a": {0: {0: "tcp://old-a"}},
        "prefill-b": {0: {0: "tcp://stable-b"}},
    }
    worker._tp_size = {
        "decode-engine": 1,
        "prefill-a": 1,
        "prefill-b": 1,
    }

    refreshed = asyncio.run(
        worker._fence_remote_engine_incarnations(
            {
                "prefill-a": "token-a-new",
                "prefill-b": "token-b",
            },
            {
                "prefill-a": "http://prefill-a:8998",
                "prefill-b": "http://prefill-b:8998",
            },
        )
    )

    assert refreshed == ["prefill-a"]
    assert worker._remote_engine_incarnations["prefill-a"] == "token-a-new"
    assert "prefill-a" not in worker._remote_agents
    assert "prefill-a" not in worker._tp_size
    assert worker._remote_agents["prefill-b"][0][0] == "tcp://stable-b"
    assert worker._tp_size["prefill-b"] == 1
    assert worker._worker_meta.topology_incarnation_observations == 2
    assert worker._worker_meta.topology_incarnation_refreshes == 1


def test_consumer_prefill_incarnation_fence_waits_for_inflight_bootstrap():
    async def scenario():
        worker = _make_consumer_worker()
        worker._remote_engine_incarnations = {"prefill-a": "token-old"}
        worker._remote_agents = {"prefill-a": {0: {0: "tcp://stale"}}}
        worker._tp_size["prefill-a"] = 1
        pending = asyncio.Event()
        worker._pending_bootstrap_queries = {"http://prefill-a:8998": pending}

        task = asyncio.create_task(
            worker._fence_remote_engine_incarnations(
                {"prefill-a": "token-new"},
                {"prefill-a": "http://prefill-a:8998"},
            )
        )
        await asyncio.sleep(0)
        assert task.done() is False
        assert "prefill-a" in worker._remote_agents

        pending.set()
        refreshed = await task
        return worker, refreshed

    worker, refreshed = asyncio.run(scenario())

    assert refreshed == ["prefill-a"]
    assert "prefill-a" not in worker._remote_agents
    assert worker._worker_meta.topology_incarnation_pending_waits == 1


def test_layered_producer_sends_full_allocated_prefix_cache_blocks(monkeypatch):
    scheduler = object.__new__(EPDMooncakeConnectorScheduler)
    scheduler.layered_kv_transfer = True
    scheduler.is_kv_producer = True
    scheduler.is_kv_consumer = False
    scheduler._reqs_need_send = {}
    scheduler.get_sw_clipped_blocks = lambda block_ids: list(block_ids)
    monkeypatch.setattr(
        UpstreamMooncakeConnectorScheduler,
        "update_state_after_alloc",
        lambda *_args, **_kwargs: None,
    )

    full_blocks = list(range(64))
    blocks = SimpleNamespace(
        get_block_ids=lambda: (full_blocks,),
        get_unhashed_block_ids_all_groups=lambda: [[63]],
    )
    request = SimpleNamespace(
        request_id="req-prefix-hit",
        kv_transfer_params={
            "transfer_id": "xfer-prefix-hit",
            "do_remote_decode": True,
        },
    )

    scheduler.update_state_after_alloc(request, blocks, num_external_tokens=0)

    _, local_block_ids = scheduler._reqs_need_send["req-prefix-hit"]
    assert local_block_ids == [full_blocks]


def test_layered_producer_excludes_hybrid_cache_null_padding_blocks(monkeypatch):
    scheduler = object.__new__(EPDMooncakeConnectorScheduler)
    scheduler.layered_kv_transfer = True
    scheduler.is_kv_producer = True
    scheduler.is_kv_consumer = False
    scheduler._reqs_need_send = {}
    scheduler.get_sw_clipped_blocks = lambda block_ids: list(block_ids)
    monkeypatch.setattr(
        UpstreamMooncakeConnectorScheduler,
        "update_state_after_alloc",
        lambda *_args, **_kwargs: None,
    )
    blocks = SimpleNamespace(
        blocks=(
            [
                SimpleNamespace(block_id=10, is_null=False),
                SimpleNamespace(block_id=11, is_null=True),
                SimpleNamespace(block_id=12, is_null=False),
            ],
        ),
    )
    request = SimpleNamespace(
        request_id="req-hybrid-padding",
        kv_transfer_params={
            "transfer_id": "xfer-hybrid-padding",
            "do_remote_decode": True,
        },
    )

    scheduler.update_state_after_alloc(request, blocks, num_external_tokens=0)

    _, local_block_ids = scheduler._reqs_need_send["req-hybrid-padding"]
    assert local_block_ids == [[10, 12]]


def test_batched_transfer_regions_uses_peer_buffer_direct_path():
    worker = _make_producer_worker()
    worker._transfer_region_descriptors_via_peer_engine = lambda *args, **kwargs: 0
    worker.engine = type(
        "Engine",
        (),
        {"batch_transfer_sync_write": lambda *args, **kwargs: 0},
    )()

    result = worker._batched_transfer_regions(
        "peer-0",
        [1, 2],
        [11, 12],
        [16, 32],
    )

    assert result.ret_code == 0
    assert result.backend_label == "peer_buffer_direct"
    assert result.used_fallback is False


def test_batched_transfer_regions_falls_back_to_raw_batch_write_on_peer_failure():
    worker = _make_producer_worker()
    worker._transfer_region_descriptors_via_peer_engine = lambda *args, **kwargs: (_ for _ in ()).throw(
        RuntimeError("peer path failed")
    )
    calls = []
    worker.engine = type(
        "Engine",
        (),
        {
            "batch_transfer_sync_write": lambda self, remote, src, dst, lengths: calls.append(
                (remote, list(src), list(dst), list(lengths))
            )
            or 0
        },
    )()

    result = worker._batched_transfer_regions(
        "peer-1",
        [3, 4],
        [13, 14],
        [64, 128],
    )

    assert result.ret_code == 0
    assert result.backend_label == "batch_transfer_fallback"
    assert result.used_fallback is True
    assert calls == [("peer-1", [3, 4], [13, 14], [64, 128])]


def test_send_region_group_records_peer_buffer_path_metrics():
    worker = _make_producer_worker()
    worker._transfer_region_descriptors_via_peer_engine = lambda *args, **kwargs: 0
    worker.engine = type(
        "Engine",
        (),
        {"batch_transfer_sync_write": lambda *args, **kwargs: 0},
    )()

    ret = worker._send_region_group(
        "peer-2",
        [5, 6],
        [15, 16],
        [32, 32],
    )

    assert ret == 0
    assert worker._worker_meta.grouped_batches == 1  # noqa: SLF001
    assert worker._worker_meta.peer_buffer_batches == 1  # noqa: SLF001
    assert worker._worker_meta.peer_buffer_bytes == 64  # noqa: SLF001
    assert worker._worker_meta.backend_counts["peer_buffer_direct"] == 1  # noqa: SLF001
    assert worker._worker_meta.transfer_attempts == 1  # noqa: SLF001
    assert worker._worker_meta.transfer_successes == 1  # noqa: SLF001
    assert worker._worker_meta.transfer_bytes == 64  # noqa: SLF001
    assert worker._worker_meta.transfer_elapsed_ms > 0.0  # noqa: SLF001
    assert worker._worker_meta.to_dict()["transfer_bandwidth_gbps"] > 0.0  # noqa: SLF001
    assert worker._worker_meta.backend_bytes["peer_buffer_direct"] == 64  # noqa: SLF001


def test_send_blocks_uses_direct_dispatch_even_when_not_layered(monkeypatch):
    worker = _make_producer_worker()
    worker.layered_kv_transfer = False
    calls = []

    def _super_should_not_run(*args, **kwargs):
        raise AssertionError("unexpected upstream _send_blocks fallback")

    monkeypatch.setattr(UpstreamMooncakeConnectorWorker, "_send_blocks", _super_should_not_run)
    worker._send_region_group = lambda remote, src, dst, lengths, **kwargs: calls.append(  # type: ignore[method-assign]
        (remote, list(src), list(dst), list(lengths))
    ) or 0

    ret = worker._send_blocks("peer-3", [7], [17], [96])

    assert ret == 0
    assert calls == [("peer-3", [7], [17], [96])]


def test_send_blocks_with_descriptor_paths_preserves_path_totals(tmp_path):
    worker = _make_producer_worker()
    worker._connector_metrics_sink = ConnectorMetricsSink(  # noqa: SLF001
        tmp_path,
        engine_id="engine-path-aware",
        role="producer",
        hostname="host-4",
        rpc_port=9004,
        tp_rank=0,
    )
    worker.layers_per_group = 1
    worker._batched_transfer_regions = lambda *args, **kwargs: type(  # noqa: SLF001
        "Dispatch",
        (),
        {"ret_code": 0, "backend_label": "peer_buffer_direct"},
    )()

    ret = worker._send_blocks(
        "peer-path-aware",
        [1, 2, 3, 4, 5, 6],
        [11, 12, 13, 14, 15, 16],
        [32, 32, 64, 64, 96, 96],
        ["PD", "PD", "EPD", "EPD", "EPD", "EPD"],
    )

    assert ret == 0
    aggregate = ConnectorMetricsReader(tmp_path).aggregate()
    totals = aggregate.totals
    path_totals = aggregate.path_totals
    assert totals.grouped_batches == 1
    # One physical batch may carry descriptors from multiple routing paths;
    # path-local batch counters therefore count path touches, not disjoint
    # physical submissions.
    assert sum(meta.grouped_batches for meta in path_totals.values()) == 2
    assert sum(meta.grouped_bytes for meta in path_totals.values()) == totals.grouped_bytes
    assert sum(meta.grouped_descriptors for meta in path_totals.values()) == totals.grouped_descriptors
    assert sum(meta.peer_buffer_batches for meta in path_totals.values()) == 2
    assert sum(meta.peer_buffer_bytes for meta in path_totals.values()) == totals.peer_buffer_bytes
    assert path_totals["PD"].grouped_batches == 1
    assert path_totals["EPD"].grouped_batches == 1


def test_build_connector_worker_meta_flushes_shared_metrics(tmp_path):
    worker = _make_producer_worker()
    worker._connector_metrics_sink = ConnectorMetricsSink(  # noqa: SLF001
        tmp_path,
        engine_id="engine-test",
        role="producer",
        hostname="host-0",
        rpc_port=9000,
        tp_rank=0,
    )
    worker._worker_meta = LayeredTransferWorkerMeta(  # noqa: SLF001
        grouped_batches=2,
        grouped_bytes=128,
        grouped_descriptors=4,
        peer_buffer_batches=2,
        peer_buffer_bytes=128,
        backend_counts={"peer_buffer_direct": 2},
    )
    worker._connector_metrics_pending = worker._worker_meta  # noqa: SLF001
    worker._connector_metrics_pending_by_path = {  # noqa: SLF001
        "EPD": LayeredTransferWorkerMeta(
            grouped_batches=2,
            grouped_bytes=128,
            grouped_descriptors=4,
        )
    }

    meta = worker.build_connector_worker_meta()

    assert meta is not None
    assert "rank0" in worker._connector_metrics_sink.path.name  # noqa: SLF001
    payload = worker._connector_metrics_sink.path.read_text(encoding="utf-8")  # noqa: SLF001
    assert '"peer_buffer_direct": 2' in payload
    assert '"path_totals"' in payload
    assert '"EPD"' in payload


def test_build_connector_worker_meta_defers_batched_io_until_request_finishes(
    tmp_path,
    monkeypatch,
):
    worker = _make_producer_worker()
    worker.layered_kv_transfer = False
    worker._connector_metrics_sink = ConnectorMetricsSink(  # noqa: SLF001
        tmp_path,
        engine_id="engine-deferred-build",
        role="producer",
        hostname="host-deferred",
        rpc_port=9011,
        tp_rank=0,
        flush_interval_s=60.0,
        max_pending_records=100,
    )
    delta = LayeredTransferWorkerMeta(
        grouped_batches=1,
        grouped_bytes=64,
        grouped_descriptors=2,
    )
    worker._worker_meta = delta  # noqa: SLF001
    worker._connector_metrics_pending = delta  # noqa: SLF001
    worker._send_request_routing_paths = {"req-0": "EPD"}  # noqa: SLF001

    meta = worker.build_connector_worker_meta()

    assert meta is not None
    assert worker._connector_metrics_sink.path is not None  # noqa: SLF001
    assert worker._connector_metrics_sink.path.exists() is False  # noqa: SLF001
    assert worker._connector_metrics_sink.io_stats["flushes"] == 0  # noqa: SLF001

    monkeypatch.setattr(
        UpstreamMooncakeConnectorWorker,
        "get_finished",
        lambda self: ({"req-0"}, set()),
    )
    finished_sending, finished_recving = worker.get_finished()

    assert finished_sending == {"req-0"}
    assert finished_recving == set()
    assert worker._connector_metrics_sink.path.exists() is True  # noqa: SLF001
    aggregate = ConnectorMetricsReader(tmp_path).aggregate()
    assert aggregate.totals.grouped_batches == 1
    assert worker._connector_metrics_sink.io_stats["flushes"] == 1  # noqa: SLF001


def test_connector_metrics_sink_batches_atomic_file_updates(tmp_path):
    sink = ConnectorMetricsSink(
        tmp_path,
        engine_id="engine-batched",
        role="producer",
        hostname="host-batched",
        rpc_port=9010,
        tp_rank=0,
        flush_interval_s=60.0,
        max_pending_records=3,
    )
    delta = LayeredTransferWorkerMeta(
        grouped_batches=1,
        grouped_bytes=32,
        grouped_descriptors=1,
    )

    sink.record(delta)
    sink.record(delta)
    assert sink.path is not None
    assert sink.path.exists() is False

    sink.record(delta)
    assert sink.path.exists() is True
    aggregate = ConnectorMetricsReader(tmp_path).aggregate()
    assert aggregate.totals.grouped_batches == 3
    assert sink.io_stats["flushes"] == 1
    assert sink.io_stats["deferred_records"] == 2


def test_connector_metrics_sink_force_flushes_deferred_snapshot(tmp_path):
    sink = ConnectorMetricsSink(
        tmp_path,
        engine_id="engine-forced",
        role="producer",
        flush_interval_s=60.0,
        max_pending_records=100,
    )
    sink.record(LayeredTransferWorkerMeta(grouped_batches=1, grouped_bytes=16))

    assert sink.path is not None
    assert sink.path.exists() is False
    sink.flush()

    assert sink.path.exists() is True
    assert ConnectorMetricsReader(tmp_path).aggregate().totals.grouped_batches == 1
    assert sink.io_stats["flushes"] == 1


def test_consumer_build_connector_worker_meta_flushes_receive_metrics(tmp_path):
    worker = _make_consumer_worker()
    worker._connector_metrics_sink = ConnectorMetricsSink(  # noqa: SLF001
        tmp_path,
        engine_id="engine-consumer",
        role="consumer",
        hostname="host-1",
        rpc_port=9001,
        tp_rank=0,
    )
    worker._worker_meta = LayeredTransferWorkerMeta(  # noqa: SLF001
        received_group_batches=2,
        received_finished_reqs=1,
        layer_wait_calls=3,
        layer_wait_ms=4.5,
    )
    worker._connector_metrics_pending = worker._worker_meta  # noqa: SLF001

    meta = worker.build_connector_worker_meta()

    assert meta is not None
    payload = worker._connector_metrics_sink.path.read_text(encoding="utf-8")  # noqa: SLF001
    assert '"received_group_batches": 2' in payload


def test_process_pulling_result_publishes_shared_metrics_immediately(tmp_path):
    worker = _make_consumer_worker()
    worker._connector_metrics_sink = ConnectorMetricsSink(  # noqa: SLF001
        tmp_path,
        engine_id="engine-consumer-live",
        role="consumer",
        hostname="host-2",
        rpc_port=9002,
        tp_rank=0,
    )
    worker._recv_request_routing_paths = {"req-0": "EPD"}  # noqa: SLF001
    response = LayeredMooncakeXferResponse(
        status=MooncakeXferResponseStatus.FINISH,
        ok_reqs=["req-0"],
        group_index=0,
        total_groups=1,
    )

    worker.process_pulling_result(response, {"req-0": None})  # type: ignore[arg-type]

    payload = worker._connector_metrics_sink.path.read_text(encoding="utf-8")  # noqa: SLF001
    assert '"received_group_batches": 1' in payload
    assert '"received_finished_reqs": 1' in payload
    assert '"path_totals"' in payload
    assert '"EPD"' in payload


def test_send_region_group_publishes_shared_metrics_immediately(tmp_path):
    worker = _make_producer_worker()
    worker._connector_metrics_sink = ConnectorMetricsSink(  # noqa: SLF001
        tmp_path,
        engine_id="engine-producer-live",
        role="producer",
        hostname="host-3",
        rpc_port=9003,
        tp_rank=0,
    )
    worker._batched_transfer_regions = lambda *args, **kwargs: type(  # noqa: SLF001
        "Dispatch",
        (),
        {"ret_code": 0, "backend_label": "peer_buffer_direct"},
    )()

    ret = worker._send_region_group("peer-live", [1, 2], [3, 4], [32, 32])

    assert ret == 0
    payload = worker._connector_metrics_sink.path.read_text(encoding="utf-8")  # noqa: SLF001
    assert '"grouped_batches": 1' in payload
    assert '"peer_buffer_direct": 1' in payload


def test_send_blocks_applies_transport_safety_chunking_for_large_descriptor_burst():
    worker = _make_producer_worker()
    worker.layered_kv_transfer = True
    worker.layers_per_group = 128
    worker._registered_region_count = 128
    worker.max_group_bytes = 0
    worker.max_transfer_descriptors = 4
    worker.max_transfer_bytes = 64
    calls = []

    def _dispatch(remote, src, dst, lengths, path_stats=None, **kwargs):
        calls.append((list(src), list(dst), list(lengths), dict(path_stats or {})))
        return type("Dispatch", (), {"ret_code": 0, "backend_label": "peer_buffer_direct"})()

    worker._send_region_group_dispatch = _dispatch  # type: ignore[method-assign]

    ret = worker._send_blocks(
        "peer-chunked",
        list(range(10)),
        list(range(100, 110)),
        [16] * 10,
        ["EPD"] * 10,
    )

    assert ret == 0
    assert [len(src) for src, _, _, _ in calls] == [4, 4, 2]
    assert [sum(lengths) for _, _, lengths, _ in calls] == [64, 64, 32]


def test_send_blocks_applies_transport_safety_chunking_even_without_layered_mode():
    worker = _make_producer_worker()
    worker.layered_kv_transfer = False
    worker.max_transfer_descriptors = 2
    worker.max_transfer_bytes = 0
    calls = []

    def _dispatch(remote, src, dst, lengths, path_stats=None, **kwargs):
        calls.append((list(src), list(dst), list(lengths)))
        return type("Dispatch", (), {"ret_code": 0, "backend_label": "batch_transfer_native"})()

    worker._send_region_group_dispatch = _dispatch  # type: ignore[method-assign]

    ret = worker._send_blocks("peer-nonlayered", [1, 2, 3], [11, 12, 13], [8, 8, 8])

    assert ret == 0
    assert [len(src) for src, _, _ in calls] == [2, 1]


def test_send_blocks_does_not_resplit_already_layer_scoped_batch():
    worker = _make_producer_worker()
    worker.layered_kv_transfer = True
    worker.layers_per_group = 2
    worker._registered_region_count = 64
    worker.max_group_bytes = 0
    worker.max_transfer_descriptors = 32
    worker.max_transfer_bytes = 16 * 1024 * 1024
    calls = []

    def _dispatch(remote, src, dst, lengths, path_stats=None, descriptor_paths=None, **kwargs):
        calls.append((list(src), list(dst), list(lengths), list(descriptor_paths or [])))
        return SimpleNamespace(
            ret_code=0,
            backend_label="peer_buffer_direct",
            used_fallback=False,
            error_message=None,
        )

    worker._send_region_group_with_retry = _dispatch  # type: ignore[method-assign]

    ret = worker._send_blocks(
        "peer-layer-scoped",
        list(range(8)),
        list(range(100, 108)),
        [32] * 8,
        ["EPD"] * 8,
    )

    assert ret == 0
    assert [len(src) for src, _, _, _ in calls] == [8]


def test_send_blocks_applies_group_delay_only_between_transport_chunks(monkeypatch):
    worker = _make_producer_worker()
    worker.layered_kv_transfer = True
    worker.layers_per_group = 2
    worker._registered_region_count = 4
    worker.max_group_bytes = 0
    worker.max_transfer_descriptors = 2
    worker.max_transfer_bytes = 0
    worker.transfer_retry_attempts = 0
    worker.group_delay_ms = 1.5
    sleeps = []

    worker._batched_transfer_regions = lambda *args, **kwargs: SimpleNamespace(  # type: ignore[method-assign]
        ret_code=0,
        backend_label="peer_buffer_direct",
        used_fallback=False,
        error_message=None,
    )
    monkeypatch.setattr(
        "mooncake_epd.core.control.vllm_mooncake_connector.time.sleep",
        lambda seconds: sleeps.append(seconds),
    )

    ret = worker._send_blocks(
        "peer-delayed",
        [1, 2, 3, 4, 5],
        [11, 12, 13, 14, 15],
        [8, 8, 8, 8, 8],
    )

    assert ret == 0
    assert sleeps == [0.0015, 0.0015]
    assert worker._worker_meta.accumulated_group_delay_ms == 3.0  # noqa: SLF001


def test_send_region_group_does_not_retry_non_retryable_memory_error():
    worker = _make_producer_worker()
    worker.transfer_retry_attempts = 6
    worker.transfer_retry_backoff_ms = 0.0
    calls = []

    def _dispatch(*args, **kwargs):
        calls.append(len(args[1]))
        return SimpleNamespace(
            ret_code=-1,
            backend_label="peer_buffer_direct",
            used_fallback=False,
            error_message="destination MR is out of bounds",
        )

    worker._send_region_group_dispatch = _dispatch  # type: ignore[method-assign]

    result = worker._send_region_group_with_retry(
        "peer-invalid-mr",
        [1, 2, 3, 4],
        [11, 12, 13, 14],
        [8, 8, 8, 8],
    )

    assert result.ret_code == -1
    assert calls == [4]


def test_build_transfer_params_coalesces_adjacent_requests_within_same_registered_region():
    worker = _make_producer_worker()
    worker.enable_descriptor_coalescing = True
    worker._send_request_routing_paths = {"req-0": "EPD", "req-1": "EPD"}  # noqa: SLF001
    worker._get_sender_transfer_plan = lambda **_kwargs: (True, 0, 0, 32)  # type: ignore[method-assign]
    ready_reqs = [
        (
            "req-0",
            SimpleNamespace(transfer_id="xfer-0", local_block_ids=[[0]]),
        ),
        (
            "req-1",
            SimpleNamespace(transfer_id="xfer-1", local_block_ids=[[1]]),
        ),
    ]
    agent_meta = SimpleNamespace(
        req_blocks={
            "req-0": ("xfer-0", [[10]]),
            "req-1": ("xfer-1", [[11]]),
        },
        remote_tp_rank=0,
        remote_tp_size=1,
    )
    local_regions = [SimpleNamespace(base_addr=1000, block_len=32, kv_block_len=32)]
    remote_regions = [SimpleNamespace(base_addr=2000, block_len=32, kv_block_len=32)]

    src, dst, lengths, errors, message, path_stats, paths = asyncio.run(
        worker._build_transfer_params_with_path_stats(  # noqa: SLF001
            ready_reqs,
            agent_meta,
            local_regions,
            remote_regions,
        )
    )

    assert errors == []
    assert message is None
    assert src == [1000]
    assert dst == [2320]
    assert lengths == [64]
    assert paths == ["EPD"]
    assert path_stats == {"EPD": (1, 64)}
    assert worker._worker_meta.descriptor_build_calls == 1  # noqa: SLF001
    assert worker._worker_meta.descriptor_build_input_descriptors == 2  # noqa: SLF001
    assert worker._worker_meta.descriptor_build_output_descriptors == 1  # noqa: SLF001
    assert worker._worker_meta.coalesced_descriptors == 1  # noqa: SLF001


def test_build_transfer_params_can_disable_descriptor_coalescing():
    worker = _make_producer_worker()
    worker.enable_descriptor_coalescing = False
    worker._send_request_routing_paths = {"req-0": "EPD", "req-1": "EPD"}  # noqa: SLF001
    worker._get_sender_transfer_plan = lambda **_kwargs: (True, 0, 0, 32)  # type: ignore[method-assign]
    ready_reqs = [
        ("req-0", SimpleNamespace(transfer_id="xfer-0", local_block_ids=[[0]])),
        ("req-1", SimpleNamespace(transfer_id="xfer-1", local_block_ids=[[1]])),
    ]
    agent_meta = SimpleNamespace(
        req_blocks={
            "req-0": ("xfer-0", [[10]]),
            "req-1": ("xfer-1", [[11]]),
        },
        remote_tp_rank=0,
        remote_tp_size=1,
    )

    src, dst, lengths, *_ = asyncio.run(
        worker._build_transfer_params_with_path_stats(  # noqa: SLF001
            ready_reqs,
            agent_meta,
            [SimpleNamespace(base_addr=1000, block_len=32, kv_block_len=32)],
            [SimpleNamespace(base_addr=2000, block_len=32, kv_block_len=32)],
        )
    )

    assert src == [1000, 1032]
    assert dst == [2320, 2352]
    assert lengths == [32, 32]
    assert worker._worker_meta.descriptor_build_input_descriptors == 2  # noqa: SLF001
    assert worker._worker_meta.descriptor_build_output_descriptors == 2  # noqa: SLF001
    assert worker._worker_meta.coalesced_descriptors == 0  # noqa: SLF001


def test_direct_peer_failure_can_be_fail_fast_when_fallback_disabled():
    worker = _make_producer_worker()
    worker.allow_transfer_fallback = False
    worker._transfer_region_descriptors_via_peer_engine = lambda *args, **kwargs: (_ for _ in ()).throw(
        RuntimeError("direct path failed")
    )
    worker.engine = type(
        "Engine",
        (),
        {"batch_transfer_sync_write": lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("fallback should not run"))},
    )()

    result = worker._batched_transfer_regions("peer-strict", [1], [2], [3])

    assert result.ret_code != 0
    assert result.backend_label == "peer_buffer_direct"
    assert "direct path failed" in (result.error_message or "")


def test_send_region_group_records_failed_transfer_timing_without_fake_bandwidth():
    worker = _make_producer_worker()
    worker._batched_transfer_regions = lambda *args, **kwargs: type(  # noqa: SLF001
        "Dispatch",
        (),
        {"ret_code": -1, "backend_label": "peer_buffer_direct"},
    )()

    ret = worker._send_region_group("peer-fail", [1], [2], [64])

    assert ret == -1
    meta = worker._worker_meta  # noqa: SLF001
    payload = meta.to_dict()
    assert meta.failed_batches == 1
    assert meta.transfer_attempts == 1
    assert meta.transfer_successes == 0
    assert meta.transfer_bytes == 0
    assert meta.transfer_attempt_elapsed_ms > 0.0
    assert meta.backend_failures["peer_buffer_direct"] == 1
    assert payload["transfer_bandwidth_gbps"] is None


def test_save_kv_layer_only_marks_active_transfer_scope():
    worker = _make_producer_worker()
    worker._layered_send_states["xfer-1"] = _LayeredSendState.create("xfer-1", 2)  # noqa: SLF001
    worker._current_send_transfer_ids = {"xfer-1"}  # noqa: SLF001

    worker.save_kv_layer("layer1", None, None)

    assert worker._layered_send_states["xfer-0"].group_ready_events[0].is_set() is False  # noqa: SLF001
    assert worker._layered_send_states["xfer-1"].group_ready_events[0].is_set() is True  # noqa: SLF001


def test_save_kv_layer_marks_earlier_groups_when_late_tail_is_first():
    worker = _make_producer_worker()

    worker.save_kv_layer("layer2", None, None)

    state = worker._layered_send_states["xfer-0"]  # noqa: SLF001
    assert state.group_ready_events[0].is_set() is True
    assert state.group_ready_events[1].is_set() is True
