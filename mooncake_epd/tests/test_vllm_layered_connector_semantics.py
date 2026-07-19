from __future__ import annotations

import asyncio
import pickle
import threading
import time

import pytest

pytest.importorskip("vllm")

from mooncake_epd.core.control.vllm_mooncake_connector import (  # noqa: E402
    LayeredMooncakeXferMetadata,
    LayeredMooncakeXferResponse,
    MooncakeConnector,
    MooncakeConnectorMetadata,
    MooncakeXferResponseStatus,
    EPDMooncakeConnectorScheduler,
    UpstreamMooncakeConnectorWorker,
    _LayeredReceiveState,
    _LayeredSendState,
    EPDMooncakeConnectorWorker,
)
from vllm.distributed.kv_transfer.kv_connector.v1.mooncake.mooncake_connector import (  # noqa: E402
    SendBlockMeta,
)
from mooncake_epd.core.control.connector_metrics import (  # noqa: E402
    ConnectorMetricsReader,
    ConnectorMetricsSink,
)
from mooncake_epd.core.control.vllm_transfer_primitives import LayeredTransferWorkerMeta  # noqa: E402
from mooncake_epd.core.state.kv_transfer_manifest_v2 import (  # noqa: E402
    KVTransferLayoutV2,
    KVTransferManifestV2,
)


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
    worker._recv_kv_transfer_manifests_by_request = {}
    worker._send_request_routing_paths = {}
    worker._send_transfer_routing_paths = {}
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
    worker._layered_source_to_send_lock = threading.Lock()
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


def test_decode_scheduler_rejects_missing_or_wrong_target_kv_transfer_manifest():
    scheduler = object.__new__(EPDMooncakeConnectorScheduler)
    scheduler.require_kv_transfer_manifest_v2 = True
    scheduler.require_kv_transfer_manifest_generation = True
    scheduler.require_kv_transfer_manifest_compatibility = False
    scheduler.worker_id = "decode-0"
    scheduler.worker_generation = "decode-generation-1"
    scheduler.kv_transfer_layout_v2 = KVTransferLayoutV2(
        model_id="Qwen3-VL",
        model_revision="revision-a",
        vllm_adapter_version="adapter-1",
        dtype="float16",
        block_size=16,
        num_layers=36,
        tp_size=1,
        tp_rank=0,
    ).as_control_payload()
    manifest = KVTransferManifestV2.create(
        transfer_id="xfer-verified",
        workflow_id="wf-verified",
        source_engine_id="epd-prefill",
        source_generation="prefill-generation-1",
        destination_engine_id="decode-0",
        destination_generation="decode-generation-1",
        layout=KVTransferLayoutV2.from_control_payload(scheduler.kv_transfer_layout_v2),
        remote_block_ids=[[1, 2]],
        transport={"protocol": "tcp", "backend": "mooncake_engine_direct"},
        lease_seconds=30.0,
    )
    params = {
        "transfer_id": "xfer-verified",
        "workflow_id": "wf-verified",
        "remote_engine_id": "epd-prefill",
        "source_generation": "prefill-generation-1",
        "kv_transfer_manifest_v2": manifest.as_control_payload(),
    }

    scheduler._validate_kv_transfer_manifest_params(params)  # noqa: SLF001

    wrong_target = manifest.rebind_destination(
        destination_engine_id="decode-1",
        destination_generation="decode-generation-1",
    )
    params["kv_transfer_manifest_v2"] = wrong_target.as_control_payload()
    with pytest.raises(RuntimeError, match="destination_engine_id mismatch"):
        scheduler._validate_kv_transfer_manifest_params(params)  # noqa: SLF001

    wrong_generation = manifest.rebind_destination(
        destination_engine_id="decode-0",
        destination_generation="decode-generation-restarted",
    )
    params["kv_transfer_manifest_v2"] = wrong_generation.as_control_payload()
    with pytest.raises(RuntimeError, match="destination_generation mismatch"):
        scheduler._validate_kv_transfer_manifest_params(params)  # noqa: SLF001

    with pytest.raises(RuntimeError, match="required"):
        scheduler._validate_kv_transfer_manifest_params({})  # noqa: SLF001


def test_decode_scheduler_preserves_manifest_sidecar_for_worker_transport():
    scheduler = object.__new__(EPDMooncakeConnectorScheduler)
    scheduler.is_kv_producer = False
    scheduler.is_kv_consumer = True
    scheduler._reqs_need_send = {}
    scheduler._reqs_not_processed = set()
    scheduler._validate_kv_transfer_manifest_params = lambda _params: None  # noqa: SLF001
    manifest = KVTransferManifestV2.create(
        transfer_id="xfer-sidecar",
        workflow_id="wf-sidecar",
        source_engine_id="epd-prefill",
        source_generation="prefill-generation-1",
        destination_engine_id="decode-0",
        destination_generation="decode-generation-1",
        remote_block_ids=[[1]],
        transport={"protocol": "tcp", "backend": "mooncake_engine_direct"},
    )
    request = type(
        "Req",
        (),
        {
            "kv_transfer_params": {
                "do_remote_prefill": True,
                "transfer_id": "xfer-sidecar",
                "remote_engine_id": "epd-prefill",
                "remote_bootstrap_addr": "http://prefill-bootstrap",
                "kv_transfer_manifest_v2": manifest.as_control_payload(),
            }
        },
    )()
    scheduler._reqs_need_recv = {"decode-request": (request, [[9]])}

    metadata = scheduler.build_connector_meta(None)
    restored = pickle.loads(pickle.dumps(metadata))

    assert restored.epd_kv_transfer_manifests_v2 == {
        "decode-request": manifest.as_control_payload()
    }
    pull_meta = restored.reqs_to_recv["epd-prefill"]["decode-request"]
    assert pull_meta.epd_kv_transfer_manifest_v2 == manifest.as_control_payload()


def test_decode_scheduler_retains_manifest_after_upstream_consumes_pull_marker():
    """The upstream scheduler mutates do_remote_prefill after it records a pull."""

    scheduler = object.__new__(EPDMooncakeConnectorScheduler)
    scheduler.is_kv_producer = False
    scheduler.is_kv_consumer = True
    scheduler._reqs_need_send = {}
    scheduler._reqs_not_processed = set()
    scheduler._validate_kv_transfer_manifest_params = lambda _params: None  # noqa: SLF001
    manifest = KVTransferManifestV2.create(
        transfer_id="xfer-upstream-mutation",
        workflow_id="wf-upstream-mutation",
        source_engine_id="epd-prefill",
        source_generation="prefill-generation-1",
        destination_engine_id="decode-0",
        destination_generation="decode-generation-1",
        remote_block_ids=[[1]],
        transport={"protocol": "tcp", "backend": "mooncake_engine_direct"},
    )
    original_params = {
        "do_remote_prefill": True,
        "transfer_id": "xfer-upstream-mutation",
        "remote_engine_id": "epd-prefill",
        "remote_bootstrap_addr": "http://prefill-bootstrap",
        "kv_transfer_manifest_v2": manifest.as_control_payload(),
    }
    request = type(
        "Req",
        (),
        {
            # This is what the upstream connector leaves on the exact request
            # object it places in _reqs_need_recv.
            "kv_transfer_params": {**original_params, "do_remote_prefill": False}
        },
    )()
    scheduler._reqs_need_recv = {"decode-request": (request, [[9]])}
    scheduler._recv_kv_transfer_params_by_request = {"decode-request": original_params}

    metadata = scheduler.build_connector_meta(None)

    assert metadata.epd_kv_transfer_manifests_v2 == {
        "decode-request": manifest.as_control_payload()
    }
    assert (
        metadata.reqs_to_recv["epd-prefill"]["decode-request"].epd_kv_transfer_manifest_v2
        == manifest.as_control_payload()
    )
    assert scheduler._recv_kv_transfer_params_by_request == {}


def test_decode_worker_recovers_declared_pull_manifest_when_sidecar_is_stripped():
    scheduler = object.__new__(EPDMooncakeConnectorScheduler)
    scheduler.is_kv_producer = False
    scheduler.is_kv_consumer = True
    scheduler._reqs_need_send = {}
    scheduler._reqs_not_processed = set()
    scheduler._validate_kv_transfer_manifest_params = lambda _params: None  # noqa: SLF001
    manifest = KVTransferManifestV2.create(
        transfer_id="xfer-pull-meta",
        workflow_id="wf-pull-meta",
        source_engine_id="epd-prefill",
        source_generation="prefill-generation-1",
        destination_engine_id="decode-0",
        destination_generation="decode-generation-1",
        remote_block_ids=[[1]],
        transport={"protocol": "tcp", "backend": "mooncake_engine_direct"},
    )
    request = type(
        "Req",
        (),
        {
            "kv_transfer_params": {
                "do_remote_prefill": True,
                "transfer_id": "xfer-pull-meta",
                "remote_engine_id": "epd-prefill",
                "remote_bootstrap_addr": "http://prefill-bootstrap",
                "kv_transfer_manifest_v2": manifest.as_control_payload(),
            }
        },
    )()
    scheduler._reqs_need_recv = {"decode-request": (request, [[9]])}
    metadata = pickle.loads(pickle.dumps(scheduler.build_connector_meta(None)))
    del metadata.epd_kv_transfer_manifests_v2

    worker = _make_consumer_worker()
    worker.require_kv_transfer_manifest_v2 = True
    worker._record_recv_kv_transfer_manifests(metadata)  # noqa: SLF001

    assert worker._recv_kv_transfer_manifests_by_request == {  # noqa: SLF001
        "decode-request": manifest.as_control_payload()
    }


def test_prefill_worker_rejects_stale_source_generation_before_kv_send():
    worker = _make_producer_worker()
    worker.engine_id = "epd-prefill"
    worker.worker_generation = "prefill-generation-current"
    worker.require_kv_transfer_manifest_v2 = True
    worker.require_kv_transfer_manifest_generation = True
    worker.require_kv_transfer_manifest_compatibility = False

    stale_manifest = KVTransferManifestV2.create(
        transfer_id="xfer-stale-source",
        workflow_id="wf-stale-source",
        source_engine_id="epd-prefill",
        source_generation="prefill-generation-before-restart",
        destination_engine_id="decode-0",
        destination_generation="decode-generation-1",
        remote_block_ids=[[1]],
        transport={"protocol": "tcp", "backend": "mooncake_engine_direct"},
    )
    meta = LayeredMooncakeXferMetadata(
        remote_hostname="127.0.0.1",
        remote_port=9000,
        remote_tp_size=1,
        remote_tp_rank=0,
        req_blocks={"decode-request": ("xfer-stale-source", [[9]])},
        kv_caches_base_addr=[],
        block_lens=[],
        layered=True,
        total_groups=2,
        kv_transfer_manifests={
            "decode-request": stale_manifest.as_control_payload(),
        },
    )

    with pytest.raises(RuntimeError, match="source_generation mismatch"):
        worker._validate_kv_transfer_manifests_for_producer(meta)  # noqa: SLF001


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


def test_wait_for_layer_load_skips_terminal_receive_state():
    worker = _make_consumer_worker()
    state = worker._layered_recv_states["req-0"]  # noqa: SLF001
    state.ack_group(0)
    state.ack_group(1)
    assert state.mark_finished_if_complete() is True

    worker.wait_for_layer_load("layer0")

    # A completed remote KV handoff must leave the layer callback entirely;
    # continuing to poll it once per model layer slows Decode token throughput.
    assert worker._worker_meta.layer_wait_calls == 0  # noqa: SLF001


def test_connector_metrics_are_batched_until_terminal_boundary():
    worker = _make_producer_worker()

    class Sink:
        enabled = True

        def __init__(self):
            self.records = []
            self.flushes = 0

        def record(self, meta, *, path_totals=None, force=False):
            self.records.append((meta, dict(path_totals or {}), force))

        def flush(self, *, force=True):
            self.flushes += 1

    sink = Sink()
    worker._connector_metrics_sink = sink  # noqa: SLF001
    worker._connector_metrics_flush_interval_s = 60.0  # noqa: SLF001
    worker._connector_metrics_next_publish_monotonic = time.monotonic() + 60.0  # noqa: SLF001
    worker._connector_metrics_lock = threading.RLock()  # noqa: SLF001
    worker._connector_metrics_pending = LayeredTransferWorkerMeta()  # noqa: SLF001

    worker._accumulate_worker_meta(LayeredTransferWorkerMeta(grouped_batches=1))  # noqa: SLF001
    worker._publish_connector_metrics()  # noqa: SLF001
    assert sink.records == []

    worker._publish_connector_metrics(force=True)  # noqa: SLF001
    assert len(sink.records) == 1
    assert sink.records[0][0].grouped_batches == 1
    assert sink.records[0][2] is True


def test_save_kv_layer_only_announces_group_tail():
    worker = _make_producer_worker()
    state = worker._layered_send_states["xfer-0"]  # noqa: SLF001
    worker._current_send_transfer_ids = {"xfer-0"}  # noqa: SLF001

    worker.save_kv_layer("layer0", None, None)
    assert state.group_ready_events[0].is_set() is False

    worker.save_kv_layer("layer1", None, None)
    assert state.group_ready_events[0].is_set() is True

    worker.save_kv_layer("layer2", None, None)
    assert state.group_ready_events[1].is_set() is True


def test_save_kv_layer_without_forward_scope_does_not_unlock_outstanding_transfer():
    worker = _make_producer_worker()
    state = worker._layered_send_states["xfer-0"]  # noqa: SLF001

    worker.save_kv_layer("layer1", None, None)

    assert state.group_ready_events[0].is_set() is False
    assert state.group_ready_events[1].is_set() is False


def test_save_kv_layer_attaches_one_source_event_to_each_newly_ready_group():
    worker = _make_producer_worker()
    worker._current_send_transfer_ids = {"xfer-0"}  # noqa: SLF001
    state = worker._layered_send_states["xfer-0"]  # noqa: SLF001
    source_event = object()
    worker._record_source_ready_event = lambda _kv_layer: source_event  # type: ignore[method-assign]

    # A later first callback conservatively releases earlier groups, and the
    # same later CUDA fence proves that all of those earlier writes completed.
    worker.save_kv_layer("layer2", object(), None)

    assert state.source_ready_event(0) is source_event
    assert state.source_ready_event(1) is source_event


def test_wait_for_save_clears_only_the_completed_forward_scope():
    worker = _make_producer_worker()
    worker._current_send_transfer_ids = {"xfer-0"}  # noqa: SLF001

    worker.wait_for_save()

    assert worker._current_send_transfer_ids == set()  # noqa: SLF001
    assert "xfer-0" in worker._layered_send_states  # noqa: SLF001


def test_layered_group_wait_uses_sender_loop_event_not_executor_threads(monkeypatch):
    worker = _make_producer_worker()
    state = worker._layered_send_states["xfer-0"]  # noqa: SLF001
    send_meta = SendBlockMeta(
        p_req_id="prefill-0",
        transfer_id="xfer-0",
        local_block_ids=[],
        ready=asyncio.Event(),
    )

    async def _run() -> None:
        async def _unexpected_to_thread(*args, **kwargs):
            raise AssertionError("layered group wait must not consume asyncio.to_thread")

        monkeypatch.setattr(asyncio, "to_thread", _unexpected_to_thread)
        task = asyncio.create_task(worker._wait_group_ready((send_meta,), 0))
        await asyncio.sleep(0)
        state.mark_group_ready(0)
        await task

    asyncio.run(_run())


def test_source_ready_events_are_deduplicated_and_waited_before_send():
    worker = _make_producer_worker()
    calls: list[str] = []
    shared_event = type(
        "SourceEvent",
        (), {"synchronize": lambda self: calls.append("source-ready")},
    )()
    first = _LayeredSendState.create("xfer-first", 2)
    second = _LayeredSendState.create("xfer-second", 2)
    first.mark_group_ready(0, source_ready_event=shared_event)
    second.mark_group_ready(0, source_ready_event=shared_event)
    worker._layered_send_states = {  # noqa: SLF001
        "xfer-first": first,
        "xfer-second": second,
    }
    worker._send_blocks = lambda *args, **kwargs: calls.append("send") or 0  # type: ignore[method-assign]
    first_meta = SendBlockMeta(
        p_req_id="prefill-first",
        transfer_id="xfer-first",
        local_block_ids=[],
        ready=asyncio.Event(),
    )
    second_meta = SendBlockMeta(
        p_req_id="prefill-second",
        transfer_id="xfer-second",
        local_block_ids=[],
        ready=asyncio.Event(),
    )

    source_events = worker._source_ready_events_for_group(  # noqa: SLF001
        (("decode-first", first_meta), ("decode-second", second_meta)),
        0,
    )
    result = worker._wait_source_events_and_send_blocks(  # noqa: SLF001
        source_events,
        "peer-0",
        [1],
        [2],
        [64],
    )

    assert result == 0
    assert calls == ["source-ready", "send"]
    assert worker._worker_meta.source_ready_event_waits == 1  # noqa: SLF001
    assert worker._worker_meta.source_ready_event_wait_ms >= 0.0  # noqa: SLF001


def test_source_ready_fence_and_peer_send_are_atomic_across_sender_workers():
    """Do not let a completed source fence sit behind another transfer.

    The connector may use several sender executor threads.  The source event
    and its peer write must nevertheless execute as one transaction so vLLM
    cannot recycle a physical KV block between the two operations.
    """

    worker = _make_producer_worker()
    active_sends = 0
    max_active_sends = 0
    active_lock = threading.Lock()
    start = threading.Barrier(3)

    class _Event:
        def synchronize(self):
            # Release the GIL long enough that an implementation without the
            # connector dispatch gate deterministically overlaps both tasks.
            time.sleep(0.02)

    def _send_blocks(_remote, src, *_args):
        nonlocal active_sends, max_active_sends
        with active_lock:
            active_sends += 1
            max_active_sends = max(max_active_sends, active_sends)
        try:
            time.sleep(0.02)
            return 0
        finally:
            with active_lock:
                active_sends -= 1

    worker._send_blocks = _send_blocks  # type: ignore[method-assign]

    def _dispatch(index: int) -> None:
        start.wait(timeout=1.0)
        worker._wait_source_events_and_send_blocks(  # noqa: SLF001
            [_Event()],
            "peer-atomic",
            [index + 1],
            [index + 101],
            [64],
        )

    first = threading.Thread(target=_dispatch, args=(0,), daemon=True)
    second = threading.Thread(target=_dispatch, args=(1,), daemon=True)
    first.start()
    second.start()
    start.wait(timeout=1.0)
    first.join(timeout=2.0)
    second.join(timeout=2.0)

    assert first.is_alive() is False
    assert second.is_alive() is False
    assert max_active_sends == 1


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


def test_process_pulling_result_records_receive_kv_latency_breakdown():
    worker = _make_consumer_worker()
    state = worker._layered_recv_states["req-0"]  # noqa: SLF001
    state.mark_started(time.perf_counter() - 0.01)
    worker._recv_request_routing_paths = {"req-0": "EPD"}  # noqa: SLF001

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

    assert worker._worker_meta.receive_kv_first_group_count == 1  # noqa: SLF001
    assert worker._worker_meta.receive_kv_first_group_ms > 0  # noqa: SLF001
    assert worker._worker_meta.receive_kv_finished_count == 1  # noqa: SLF001
    assert worker._worker_meta.receive_kv_finished_ms >= worker._worker_meta.receive_kv_first_group_ms  # noqa: SLF001
    epd_meta = worker._connector_metrics_pending_by_path["EPD"]  # noqa: SLF001
    assert epd_meta.receive_kv_first_group_count == 1
    assert epd_meta.receive_kv_finished_count == 1


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


def test_record_send_reqs_does_not_replace_model_forward_send_scope():
    worker = _make_producer_worker()
    worker._current_send_transfer_ids = {"xfer-0"}  # noqa: SLF001
    metadata = MooncakeConnectorMetadata()
    metadata.reqs_to_send["req-later"] = ("xfer-later", [[10, 11]])

    asyncio.run(worker.record_send_reqs(metadata))

    assert worker._current_send_transfer_ids == {"xfer-0"}  # noqa: SLF001
    assert "xfer-later" in worker._layered_send_states  # noqa: SLF001


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


def test_layered_send_dispatches_ready_subset_without_waiting_for_slow_batch_peer():
    class _Sock:
        def __init__(self):
            self.responses = []

        async def send_multipart(self, parts):
            self.responses.append(parts[1])

    class _Loop:
        async def run_in_executor(self, _executor, fn, *args):
            return fn(*args)

    async def run_case():
        worker = _make_producer_worker()
        worker.sender_loop = _Loop()
        worker.transfer_topo = type("Topo", (), {"handshake_target_ranks": lambda self, _size: [0]})()
        worker.tp_size = 1
        worker.kv_caches_base_addr = []
        worker.block_len_per_layer = []
        worker._get_transfer_regions = lambda *args: []  # noqa: SLF001
        worker._validate_regions = lambda *args: None  # noqa: SLF001
        worker._encoder = type("Encoder", (), {"encode": lambda self, value: value})()
        worker.resolve_need_send = lambda send_meta, _ranks: setattr(send_meta, "need_send", 1)
        worker._publish_connector_metrics = lambda *args, **kwargs: None  # noqa: SLF001
        worker.finished_sending_reqs = set()

        async def build_params(*, ready_reqs, group_idx, **kwargs):
            return (
                [1000 + group_idx + idx for idx, _ in enumerate(ready_reqs)],
                [2000 + group_idx + idx for idx, _ in enumerate(ready_reqs)],
                [64 for _ in ready_reqs],
                [],
                None,
                {},
                ["EPD" for _ in ready_reqs],
            )

        worker._build_transfer_params_for_group = build_params  # type: ignore[method-assign]
        worker._send_blocks = lambda *args: 0  # noqa: SLF001

        first = SendBlockMeta(
            p_req_id="prefill-fast",
            transfer_id="xfer-fast",
            local_block_ids=[[1]],
            ready=asyncio.Event(),
        )
        slow = SendBlockMeta(
            p_req_id="prefill-slow",
            transfer_id="xfer-slow",
            local_block_ids=[[2]],
            ready=asyncio.Event(),
        )
        first.ready.set()
        worker.reqs_need_send = {"xfer-fast": first, "xfer-slow": slow}
        worker._layered_send_states = {  # noqa: SLF001
            "xfer-fast": _LayeredSendState.create("xfer-fast", 2),
            "xfer-slow": _LayeredSendState.create("xfer-slow", 2),
        }
        for event in worker._layered_send_states["xfer-fast"].group_ready_events:  # noqa: SLF001
            event.set()

        meta = LayeredMooncakeXferMetadata(
            remote_hostname="127.0.0.1",
            remote_port=9000,
            remote_tp_size=1,
            remote_tp_rank=0,
            req_blocks={
                "decode-fast": ("xfer-fast", [[11]]),
                "decode-slow": ("xfer-slow", [[12]]),
            },
            kv_caches_base_addr=[],
            block_lens=[],
            layered=True,
            total_groups=2,
        )
        sock = _Sock()
        task = asyncio.create_task(worker.send_kv_to_decode(b"peer", sock, meta))
        await asyncio.sleep(0.05)

        assert sock.responses
        assert all(response.ok_reqs == ["decode-fast"] for response in sock.responses)
        assert all(response.status is MooncakeXferResponseStatus.CONTINUE for response in sock.responses)

        slow.ready.set()
        for event in worker._layered_send_states["xfer-slow"].group_ready_events:  # noqa: SLF001
            event.set()
        await task

        assert sock.responses[-1].status is MooncakeXferResponseStatus.FINISH
        assert sock.responses[-1].ok_reqs == ["decode-slow"]

    asyncio.run(run_case())


def test_layered_send_batches_group_fences_and_peer_write():
    """Keep one native transaction and one response per ready layered group.

    A Decode pull treats a response as a group-level synchronization point for
    every request represented by that response.  The producer must fence each
    source event, but it must not split the wire protocol into independently
    acknowledged request transactions.
    """

    class _Sock:
        def __init__(self):
            self.responses = []

        async def send_multipart(self, parts):
            self.responses.append(parts[1])

    class _Loop:
        async def run_in_executor(self, _executor, fn, *args):
            return fn(*args)

    async def run_case() -> None:
        worker = _make_producer_worker()
        worker.sender_loop = _Loop()
        worker.transfer_topo = type("Topo", (), {"handshake_target_ranks": lambda self, _size: [0]})()
        worker.tp_size = 1
        worker.kv_caches_base_addr = []
        worker.block_len_per_layer = []
        worker._get_transfer_regions = lambda *args: []  # noqa: SLF001
        worker._validate_regions = lambda *args: None  # noqa: SLF001
        worker._encoder = type("Encoder", (), {"encode": lambda self, value: value})()
        worker.resolve_need_send = lambda send_meta, _ranks: setattr(send_meta, "need_send", 1)
        worker._publish_connector_metrics = lambda *args, **kwargs: None  # noqa: SLF001
        worker.finished_sending_reqs = set()

        build_calls: list[tuple[str, ...]] = []
        dispatches: list[tuple[tuple[object, ...], tuple[int, ...]]] = []

        async def build_params(*, ready_reqs, group_idx, **kwargs):
            request_ids = tuple(d_req_id for d_req_id, _ in ready_reqs)
            build_calls.append(request_ids)
            assert set(request_ids) == {"decode-first", "decode-second"}
            return (
                [1000 + group_idx, 1100 + group_idx],
                [2000 + group_idx, 2100 + group_idx],
                [64, 64],
                [],
                None,
                {},
                "EPD",
            )

        def dispatch(source_events, _remote, src_ptrs, *_args):
            dispatches.append((tuple(source_events), tuple(src_ptrs)))
            return 0

        worker._build_transfer_params_for_group = build_params  # type: ignore[method-assign]
        worker._wait_source_events_and_send_blocks = dispatch  # type: ignore[method-assign]

        first = SendBlockMeta(
            p_req_id="prefill-first",
            transfer_id="xfer-first",
            local_block_ids=[[1], [2]],
            ready=asyncio.Event(),
        )
        second = SendBlockMeta(
            p_req_id="prefill-second",
            transfer_id="xfer-second",
            local_block_ids=[[3], [4]],
            ready=asyncio.Event(),
        )
        first.ready.set()
        second.ready.set()
        first_event = object()
        second_event = object()
        first_state = _LayeredSendState.create("xfer-first", 2)
        second_state = _LayeredSendState.create("xfer-second", 2)
        for group_idx in range(2):
            first_state.mark_group_ready(group_idx, source_ready_event=first_event)
            second_state.mark_group_ready(group_idx, source_ready_event=second_event)
        worker.reqs_need_send = {"xfer-first": first, "xfer-second": second}
        worker._layered_send_states = {  # noqa: SLF001
            "xfer-first": first_state,
            "xfer-second": second_state,
        }

        meta = LayeredMooncakeXferMetadata(
            remote_hostname="127.0.0.1",
            remote_port=9000,
            remote_tp_size=1,
            remote_tp_rank=0,
            req_blocks={
                "decode-first": ("xfer-first", [[11], [12]]),
                "decode-second": ("xfer-second", [[13], [14]]),
            },
            kv_caches_base_addr=[],
            block_lens=[],
            layered=True,
            total_groups=2,
        )
        sock = _Sock()
        await worker.send_kv_to_decode(b"peer", sock, meta)

        assert len(build_calls) == 2
        assert all(
            set(request_ids) == {"decode-first", "decode-second"}
            for request_ids in build_calls
        )
        assert len(dispatches) == 2
        assert all(
            set(source_events) == {first_event, second_event}
            for source_events, _ in dispatches
        )
        assert all(
            src_ptrs in {(1000, 1100), (1001, 1101)}
            for _, src_ptrs in dispatches
        )
        assert all(
            set(response.ok_reqs or []) == {"decode-first", "decode-second"}
            for response in sock.responses
        )
        assert sock.responses[-1].status is MooncakeXferResponseStatus.FINISH

    asyncio.run(run_case())


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


def test_batched_transfer_regions_uses_peer_buffer_direct_path():
    worker = _make_producer_worker()
    worker._transfer_region_descriptors_via_peer_engine = lambda *args, **kwargs: type(  # noqa: SLF001
        "Dispatch",
        (),
        {
            "ret_code": 0,
            "backend_label": "peer_buffer_direct",
            "used_fallback": False,
            "dispatch_ms": 1.0,
            "prepare_ms": 0.1,
            "write_ms": 0.8,
        },
    )()
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
    worker._transfer_region_descriptors_via_peer_engine = lambda *args, **kwargs: type(  # noqa: SLF001
        "Dispatch",
        (),
        {
            "ret_code": 0,
            "backend_label": "peer_buffer_direct",
            "dispatch_ms": 1.0,
            "prepare_ms": 0.1,
            "write_ms": 0.8,
            "native_engine_lock_wait_ms": 0.05,
        },
    )()
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
    assert worker._worker_meta.peer_buffer_dispatch_ms == 1.0  # noqa: SLF001
    assert worker._worker_meta.peer_buffer_write_ms == 0.8  # noqa: SLF001
    assert worker._worker_meta.peer_buffer_native_engine_lock_wait_ms == 0.05  # noqa: SLF001
    assert worker._worker_meta.backend_counts["peer_buffer_direct"] == 1  # noqa: SLF001


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
    worker.max_transfer_descriptors = 2
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
    assert totals.grouped_batches == 3
    assert sum(meta.grouped_batches for meta in path_totals.values()) == totals.grouped_batches
    assert sum(meta.grouped_bytes for meta in path_totals.values()) == totals.grouped_bytes
    assert sum(meta.grouped_descriptors for meta in path_totals.values()) == totals.grouped_descriptors
    assert sum(meta.peer_buffer_batches for meta in path_totals.values()) == totals.peer_buffer_batches
    assert sum(meta.peer_buffer_bytes for meta in path_totals.values()) == totals.peer_buffer_bytes
    assert path_totals["PD"].grouped_batches == 1
    assert path_totals["EPD"].grouped_batches == 2


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



def test_send_blocks_does_not_overchunk_layered_group_that_fits_transport_limits():
    worker = _make_producer_worker()
    worker.layered_kv_transfer = True
    worker.layers_per_group = 1
    worker._registered_region_count = 128
    worker.max_group_bytes = 0
    worker.max_transfer_descriptors = 128
    worker.max_transfer_bytes = 0
    calls = []

    def _dispatch(remote, src, dst, lengths, path_stats=None, **kwargs):
        calls.append((list(src), list(dst), list(lengths), dict(path_stats or {})))
        return type("Dispatch", (), {"ret_code": 0, "backend_label": "peer_buffer_direct"})()

    worker._send_region_group_dispatch = _dispatch  # type: ignore[method-assign]

    ret = worker._send_blocks(
        "peer-no-overchunk",
        list(range(24)),
        list(range(100, 124)),
        [16] * 24,
        ["EPD"] * 24,
    )

    assert ret == 0
    assert [len(src) for src, _, _, _ in calls] == [24]


def test_send_blocks_uses_constant_path_label_without_per_descriptor_list():
    worker = _make_producer_worker()
    worker.max_transfer_descriptors = 2
    worker.max_transfer_bytes = 0
    calls = []

    def _dispatch(remote, src, dst, lengths, path_stats=None, **kwargs):
        calls.append(dict(path_stats or {}))
        return type("Dispatch", (), {"ret_code": 0, "backend_label": "peer_buffer_direct"})()

    worker._send_region_group_dispatch = _dispatch  # type: ignore[method-assign]

    ret = worker._send_blocks(
        "peer-constant-path",
        [1, 2, 3, 4, 5],
        [11, 12, 13, 14, 15],
        [8, 8, 8, 8, 8],
        "PD",
    )

    assert ret == 0
    assert calls == [
        {"PD": (2, 16)},
        {"PD": (2, 16)},
        {"PD": (1, 8)},
    ]

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


def test_save_kv_layer_only_marks_active_transfer_scope():
    worker = _make_producer_worker()
    worker._layered_send_states["xfer-1"] = _LayeredSendState.create("xfer-1", 2)  # noqa: SLF001
    worker._current_send_transfer_ids = {"xfer-1"}  # noqa: SLF001

    worker.save_kv_layer("layer1", None, None)

    assert worker._layered_send_states["xfer-0"].group_ready_events[0].is_set() is False  # noqa: SLF001
    assert worker._layered_send_states["xfer-1"].group_ready_events[0].is_set() is True  # noqa: SLF001


def test_save_kv_layer_marks_earlier_groups_when_late_tail_is_first():
    worker = _make_producer_worker()
    worker._current_send_transfer_ids = {"xfer-0"}  # noqa: SLF001

    worker.save_kv_layer("layer2", None, None)

    state = worker._layered_send_states["xfer-0"]  # noqa: SLF001
    assert state.group_ready_events[0].is_set() is True
    assert state.group_ready_events[1].is_set() is True
