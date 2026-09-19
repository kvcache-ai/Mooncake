from __future__ import annotations

import asyncio
import copy
import json
import time
from types import SimpleNamespace

import httpx
import mooncake_epd.scripts.vllm_disagg_proxy as proxy_module
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
from fastapi.testclient import TestClient

from mooncake_epd.core.control import ServingControlPlane, ServingControlPlaneConfig
from mooncake_epd.core.control.connector_metrics import ConnectorMetricsSink
from mooncake_epd.core.control.vllm_incarnation import (
    VLLM_EXPECTED_INCARNATION_HEADER,
    VLLM_INCARNATION_HEADER,
    VLLM_INCARNATION_MISMATCH_HEADER,
)
from mooncake_epd.core.control.vllm_transfer_primitives import LayeredTransferWorkerMeta
from mooncake_epd.core.state import WorkflowStateRegistry
from mooncake_epd.scripts.vllm_disagg_proxy import (
    ProxyConfig,
    _DirectFeatureHandleCache,
    _DecodeMMHashWarmCache,
    _IncrementalTokenDecoder,
    _copy_request_for_mm_url_rewrite,
    _fence_decode_mm_hash_reuse,
    _fence_prefill_direct_handle_reuse,
    _jittered_poll_delay_s,
    _load_mm_url_bytes,
    _make_client,
    _prefill_render_cache_digest,
    _probe_prefill_worker_incarnation,
    _probe_prefill_worker_incarnation_for_monitor,
    _probe_decode_worker_epoch,
    _set_image_url_on_item,
    _validate_remote_mm_url,
    create_app,
)



def _build_prefill_app(record: dict) -> FastAPI:
    app = FastAPI()

    @app.get("/health")
    async def health():
        status_code = 503 if record.get("prefill_unhealthy") else 200
        return JSONResponse({"status": "ok"}, status_code=status_code)

    @app.post("/v1/chat/completions/render")
    async def render_chat(request: Request):
        body = await request.json()
        record["prefill_render_calls"] = int(record.get("prefill_render_calls", 0)) + 1
        record["prefill_render_body"] = body
        payload = {
            "request_id": "rendered-prefill-0",
            "token_ids": [1, 2, 3, 4],
            "sampling_params": {
                "temperature": 0.0,
                "top_p": 1.0,
                "max_tokens": 16,
                "min_tokens": 0,
            },
            "model": "fake-model",
            "stream": bool(body.get("stream")),
            "priority": 0,
        }
        if record.get("render_features") is not None:
            payload["features"] = record["render_features"]
        return JSONResponse(payload)

    @app.post("/inference/v1/generate")
    async def generate(request: Request):
        body = await request.json()
        record["prefill_generate_calls"] = int(record.get("prefill_generate_calls", 0)) + 1
        record["prefill_generate_body"] = body
        sampling_params = dict(body.get("sampling_params") or {})
        extra_args = dict(sampling_params.get("extra_args") or {})
        kv = dict(extra_args.get("kv_transfer_params") or {})
        kv.update(
            {
                "transfer_id": kv.get("transfer_id") or request.headers.get("X-Request-Id"),
                "remote_engine_id": "prefill-engine-0",
                "remote_bootstrap_addr": "http://prefill-bootstrap:8998",
                "remote_block_ids": [[11, 12, 13]],
            }
        )
        return JSONResponse(
            {
                "request_id": "prefill-response",
                "choices": [{"index": 0, "finish_reason": "length", "token_ids": []}],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 0,
                    "total_tokens": 10,
                },
                "kv_transfer_params": kv,
            }
        )

    return app


def _build_decode_app(record: dict) -> FastAPI:
    app = FastAPI()

    @app.post("/v1/chat/completions")
    async def chat(request: Request):
        body = await request.json()
        record["decode_body"] = body
        if body.get("stream"):
            async def _gen():
                yield (
                    b'data: {"id":"chunk-0","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}\n\n'
                )
                yield (
                    b'data: {"id":"chunk-1","choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":null}]}\n\n'
                )
                yield (
                    b'data: {"id":"chunk-2","choices":[{"index":0,"delta":{"content":"world"},"finish_reason":"stop"}],'
                    b'"usage":{"prompt_tokens":12,"completion_tokens":2,"total_tokens":14}}\n\n'
                )
                yield b"data: [DONE]\n\n"

            return StreamingResponse(_gen(), media_type="text/event-stream")
        return JSONResponse(
            {
                "id": "decode-response",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "helloworld"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 12, "completion_tokens": 2, "total_tokens": 14},
            }
        )

    return app


class _FakeDecodeTokenizer:
    _pieces = {10: "hello", 11: " world", 99: "<eos>"}

    def decode(self, token_ids, *, skip_special_tokens=False):
        pieces = []
        for token_id in token_ids:
            if skip_special_tokens and int(token_id) == 99:
                continue
            pieces.append(self._pieces.get(int(token_id), ""))
        return "".join(pieces)


def _build_prerendered_decode_app(record: dict) -> FastAPI:
    app = FastAPI()

    @app.get("/health")
    async def health():
        status_code = 503 if record.get("decode_unhealthy") else 200
        return JSONResponse({"status": "ok"}, status_code=status_code)

    @app.get("/metrics")
    async def metrics():
        if record.get("decode_metrics_unavailable"):
            return PlainTextResponse("unavailable\n", status_code=503)
        epoch = str(record.get("decode_process_start_time", "1"))
        return PlainTextResponse(f"process_start_time_seconds {epoch}\n")

    @app.get("/mooncake_epd/incarnation")
    async def incarnation():
        epoch = str(record.get("decode_process_start_time", "1"))
        return PlainTextResponse(
            epoch,
            headers={"X-Mooncake-EPD-Incarnation": epoch},
        )

    @app.post("/v1/chat/completions")
    async def chat(request: Request):
        record["decode_openai_calls"] = int(record.get("decode_openai_calls", 0)) + 1
        body = await request.json()
        record["decode_openai_body"] = body
        return JSONResponse(
            {
                "id": "public-decode-response",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "public path"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 4,
                    "completion_tokens": 2,
                    "total_tokens": 6,
                },
            }
        )

    @app.post("/inference/v1/generate")
    async def generate(request: Request):
        body = await request.json()
        record["decode_started"] = True
        record["decode_generate_calls"] = int(
            record.get("decode_generate_calls", 0)
        ) + 1
        record["decode_generate_body"] = body
        record.setdefault("decode_generate_bodies", []).append(body)
        record.setdefault("decode_generate_headers", []).append(
            dict(request.headers)
        )
        expected_epoch = request.headers.get(
            VLLM_EXPECTED_INCARNATION_HEADER
        )
        actual_epoch = str(record.get("decode_process_start_time", "1"))
        if (
            record.get("enforce_incarnation_guard")
            and expected_epoch
            and expected_epoch != actual_epoch
        ):
            return JSONResponse(
                {"error": {"message": "stale Decode incarnation"}},
                status_code=409,
                headers={
                    "X-Mooncake-EPD-Incarnation": actual_epoch,
                    "X-Mooncake-EPD-Incarnation-Mismatch": "1",
                },
            )
        features = dict(body.get("features") or {})
        if features.get("kwargs_data") is None and record.get("reject_hash_only"):
            return JSONResponse(
                {"error": {"message": "simulated Decode receiver cache miss"}},
                status_code=500,
            )
        if body.get("stream"):

            async def _gen():
                if record.get("malformed_internal"):
                    yield (
                        b'data: {"request_id":"internal","choices":[{"index":0,'
                        b'"finish_reason":null,"delta":{"content":"bad"}}],'
                        b'"usage":null}\n\n'
                    )
                    yield b"data: [DONE]\n\n"
                    return
                yield (
                    b'data: {"request_id":"internal","choices":[{"index":0,'
                    b'"logprobs":null,"finish_reason":null,"token_ids":[10]}],'
                    b'"usage":null}\n\n'
                )
                if record.get("drop_before_done"):
                    return
                yield (
                    b'data: {"request_id":"internal","choices":[{"index":0,'
                    b'"logprobs":null,"finish_reason":null,"token_ids":[11]}],'
                    b'"usage":null}\n\n'
                )
                if record.get("combined_usage"):
                    yield (
                        b'data: {"request_id":"internal","choices":[{"index":0,'
                        b'"logprobs":null,"finish_reason":"stop","token_ids":[99]}],'
                        b'"usage":{"prompt_tokens":4,"completion_tokens":3,'
                        b'"total_tokens":7}}\n\n'
                    )
                else:
                    yield (
                        b'data: {"request_id":"internal","choices":[{"index":0,'
                        b'"logprobs":null,"finish_reason":"stop","token_ids":[99]}],'
                        b'"usage":null}\n\n'
                    )
                    yield (
                        b'data: {"request_id":"internal","choices":[],"usage":'
                        b'{"prompt_tokens":4,"completion_tokens":3,"total_tokens":7}}\n\n'
                    )
                yield b"data: [DONE]\n\n"

            return StreamingResponse(_gen(), media_type="text/event-stream")
        return JSONResponse(
            {
                "id": "internal",
                "created": 123,
                "model": "fake-model",
                "choices": [
                    {
                        "index": 0,
                        "logprobs": None,
                        "finish_reason": "stop",
                        "token_ids": [10, 11, 99],
                    }
                ],
                "usage": {
                    "prompt_tokens": 4,
                    "completion_tokens": 3,
                    "total_tokens": 7,
                },
            }
        )

    return app


def _build_empty_decode_app(record: dict) -> FastAPI:
    app = FastAPI()

    @app.post("/v1/chat/completions")
    async def chat(request: Request):
        body = await request.json()
        record["decode_body"] = body

        async def _gen():
            if False:
                yield b""

        return StreamingResponse(_gen(), media_type="text/event-stream")

    return app


def _build_pipeline_prefill_app(record: dict) -> FastAPI:
    app = FastAPI()

    @app.post("/v1/chat/completions/render")
    async def render_chat(request: Request):
        body = await request.json()
        return JSONResponse(
            {
                "request_id": "rendered-pipeline-prefill",
                "token_ids": [1, 2, 3, 4],
                "sampling_params": {
                    "temperature": 0.0,
                    "top_p": 1.0,
                    "max_tokens": 16,
                    "min_tokens": 0,
                },
                "model": "fake-model",
                "stream": bool(body.get("stream")),
                "priority": 0,
            }
        )

    @app.post("/inference/v1/generate")
    async def generate(request: Request):
        body = await request.json()
        record["prefill_generate_started"] = True
        for _ in range(100):
            if record.get("decode_started"):
                break
            await asyncio.sleep(0.001)
        record["decode_started_before_prefill_finished"] = bool(
            record.get("decode_started")
        )
        finish_delay_s = float(record.get("prefill_finish_delay_s", 0.0) or 0.0)
        if finish_delay_s > 0:
            await asyncio.sleep(finish_delay_s)
        kv = dict(body.get("kv_transfer_params") or {})
        kv.update(
            {
                "transfer_id": kv.get("transfer_id"),
                "remote_engine_id": "prefill-engine-pipeline",
                "remote_bootstrap_addr": "http://prefill-bootstrap:8998",
                "tp_size": 1,
                "remote_block_ids": [[11, 12]],
            }
        )
        return JSONResponse(
            {
                "request_id": "prefill-pipeline-response",
                "choices": [{"index": 0, "finish_reason": "length", "token_ids": []}],
                "usage": {"prompt_tokens": 4, "completion_tokens": 0, "total_tokens": 4},
                "kv_transfer_params": kv,
            }
        )

    return app


def _build_pipeline_decode_app(record: dict) -> FastAPI:
    app = FastAPI()

    @app.post("/v1/chat/completions")
    async def chat(request: Request):
        body = await request.json()
        record["decode_calls"] = int(record.get("decode_calls", 0)) + 1
        record["decode_started"] = True
        record["decode_kv"] = dict(body.get("kv_transfer_params") or {})

        async def _gen():
            yield b'data: {"choices":[{"index":0,"delta":{"content":"pipelined"},"finish_reason":null}]}\n\n'
            yield b'data: {"choices":[{"index":0,"delta":{"content":""},"finish_reason":"stop"}]}\n\n'
            yield b"data: [DONE]\n\n"

        return StreamingResponse(_gen(), media_type="text/event-stream")

    return app


def _build_failing_pipeline_decode_app(record: dict) -> FastAPI:
    app = FastAPI()

    @app.post("/v1/chat/completions")
    async def chat(request: Request):
        await request.json()
        record["decode_calls"] = int(record.get("decode_calls", 0)) + 1
        record["decode_started"] = True
        return JSONResponse(
            {"error": {"message": "decode startup rejected"}},
            status_code=503,
        )

    return app


def _client_override(app: FastAPI, worker_id: str, host: str, port: int) -> dict:
    transport = httpx.ASGITransport(app=app)
    return {
        "client": httpx.AsyncClient(base_url=f"http://{host}:{port}", transport=transport, timeout=None),
        "host": host,
        "port": port,
        "id": 0,
        "worker_id": worker_id,
    }


def test_upstream_client_uses_bounded_short_lived_keepalive_pool():
    client = _make_client(
        "http://127.0.0.1:8200",
        ProxyConfig(
            upstream_max_connections=12,
            upstream_max_keepalive_connections=5,
            upstream_keepalive_expiry_s=0.75,
        ),
    )
    try:
        pool = client._transport._pool
        assert pool._max_connections == 12
        assert pool._max_keepalive_connections == 5
        assert pool._keepalive_expiry == 0.75
    finally:
        asyncio.run(client.aclose())


def test_proxy_readiness_reports_real_upstream_state():
    record: dict = {}
    proxy_app = create_app(
        ProxyConfig(),
        prefill_clients=[
            _client_override(
                _build_prefill_app(record),
                "prefill-0",
                "prefill.local",
                8100,
            )
        ],
        decode_clients=[
            _client_override(
                _build_prerendered_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
    )

    with TestClient(proxy_app) as client:
        healthy = client.get("/ready")
        record["decode_unhealthy"] = True
        unhealthy = client.get("/ready")

    assert healthy.status_code == 200
    assert healthy.json()["status"] == "ready"
    assert unhealthy.status_code == 503
    assert unhealthy.json()["upstreams"]["decode:decode-0"] == {
        "ready": False,
        "status_code": 503,
    }


def test_proxy_reuses_semantic_prefill_render_but_not_request_kv_metadata():
    record: dict = {}
    proxy_app = create_app(
        ProxyConfig(
            enable_prefill_render_cache=True,
            prefill_render_cache_max_entries=8,
            prefill_render_cache_max_bytes=1024 * 1024,
            prefill_render_cache_ttl_s=60.0,
        ),
        prefill_clients=[
            _client_override(_build_prefill_app(record), "prefill-0", "prefill.local", 8100)
        ],
        decode_clients=[
            _client_override(_build_decode_app(record), "decode-0", "decode.local", 8200)
        ],
    )
    base = {
        "messages": [{"role": "user", "content": "cache this rendered prompt"}],
        "stream": False,
    }
    with TestClient(proxy_app) as client:
        first = client.post(
            "/v1/chat/completions",
            json=base | {"metadata": {"workflow_id": "wf-render-1"}},
        )
        second = client.post(
            "/v1/chat/completions",
            json=base | {"metadata": {"workflow_id": "wf-render-2"}},
        )
        stats = client.get("/metrics").json()["prefill_render_cache"]

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert record["prefill_render_calls"] == 1
    assert record["prefill_generate_calls"] == 2
    assert stats["entries"] == 1
    assert stats["hits"] == 1
    assert stats["misses"] == 1
    assert stats["inflight"] == 0


def test_prefill_render_cache_ignores_decode_worker_targeting_metadata():
    base = {
        "messages": [{"role": "user", "content": "same render semantics"}],
        "metadata": {
            "workflow_id": "wf-1",
            "mooncake_epd_decode_worker_id": "decode-0",
        },
    }
    alternate = copy.deepcopy(base)
    alternate["metadata"] = {
        "workflow_id": "wf-2",
        "mooncake_epd_decode_worker_id": "decode-1",
    }

    assert _prefill_render_cache_digest(base, mm_hashes=[]) == (
        _prefill_render_cache_digest(alternate, mm_hashes=[])
    )


def test_proxy_uuid_only_media_reuses_feature_and_render_caches_strictly():
    record: dict = {}
    image_item = {
        "type": "image_url",
        "image_url": {"url": "data:image/png;base64,ZmFrZS1pbWFnZQ=="},
        "uuid": "asset-uuid-1",
    }
    feature_handle = _feature_handle_payload_for_item(image_item)
    proxy_app = create_app(
        ProxyConfig(
            enable_client_mm_uuid_references=True,
            mm_prefetch_mode="feature_handle",
            prefill_supports_feature_handles=True,
            prefill_direct_buffer_service_url="http://buffer.local",
            release_direct_feature_buffers_after_prefill=False,
            enable_direct_feature_handle_cache=True,
            direct_feature_handle_cache_max_entries=8,
            direct_feature_handle_cache_max_bytes=1024 * 1024,
            direct_feature_handle_cache_ttl_s=60.0,
            enable_prefill_render_cache=True,
            prefill_render_cache_max_entries=8,
            prefill_render_cache_max_bytes=1024 * 1024,
            prefill_render_cache_ttl_s=60.0,
        ),
        prefill_clients=[
            _client_override(_build_prefill_app(record), "prefill-0", "prefill.local", 8100)
        ],
        decode_clients=[
            _client_override(_build_decode_app(record), "decode-0", "decode.local", 8200)
        ],
    )
    proxy_app.state.direct_feature_handle_cache.put_many(
        target_worker_id="prefill-0",
        handles=[feature_handle],
    )
    base = {
        "model": "fake-model",
        "messages": [
            {
                "role": "user",
                "content": [image_item, {"type": "text", "text": "describe"}],
            }
        ],
        "stream": False,
    }
    compact = json.loads(json.dumps(base))
    compact["messages"][0]["content"][0]["image_url"] = None

    with TestClient(proxy_app) as client:
        full_response = client.post("/v1/chat/completions", json=base)
        compact_response = client.post("/v1/chat/completions", json=compact)
        cold = json.loads(json.dumps(compact))
        cold["messages"][0]["content"][0]["uuid"] = "cold-asset"
        cold_response = client.post("/v1/chat/completions", json=cold)
        metrics = client.get("/metrics").json()

    assert full_response.status_code == 200, full_response.text
    assert compact_response.status_code == 200, compact_response.text
    assert full_response.headers["x-epd-client-mm-uuid-mode"] == "full"
    assert compact_response.headers["x-epd-client-mm-uuid-mode"] == "compact"
    assert record["prefill_render_calls"] == 1
    assert record["prefill_generate_calls"] == 2
    assert metrics["direct_feature_handle_cache"]["hits"] == 2
    assert metrics["prefill_render_cache"]["hits"] == 1
    assert metrics["client_mm_uuid_references"]["compact_requests"] == 2
    assert metrics["client_mm_uuid_references"]["cold_misses"] == 1
    assert metrics["client_mm_uuid_references"]["cold_misses_by_stage"] == {
        "direct_feature_handle": 1
    }
    assert cold_response.status_code == 409
    assert "resend the full media payload" in cold_response.json()["detail"]


def _direct_handle_for_incarnation(
    *, feature_id: str, source_mm_hash: str, incarnation: str
) -> dict:
    return {
        "handle_id": f"handle-{feature_id}",
        "feature_id": feature_id,
        "store_id": "prefill-direct",
        "uri": f"epd-direct://prefill-0/{feature_id}",
        "descriptor": {"nbytes": 4096},
        "metadata": {
            "source_mm_hash": source_mm_hash,
            "direct_remote_incarnation": incarnation,
        },
    }


def test_direct_feature_cache_invalidates_only_changed_prefill_incarnation():
    cache = _DirectFeatureHandleCache(
        enabled=True,
        max_entries=8,
        max_bytes=1024 * 1024,
        ttl_s=60.0,
    )
    changed, released = cache.put_many(
        target_worker_id="prefill-0",
        handles=[
            _direct_handle_for_incarnation(
                feature_id="old-0",
                source_mm_hash="asset-0",
                incarnation="prefill-a",
            )
        ],
    )
    cache.put_many(
        target_worker_id="prefill-1",
        handles=[
            _direct_handle_for_incarnation(
                feature_id="other-0",
                source_mm_hash="asset-1",
                incarnation="prefill-other",
            )
        ],
    )
    assert changed is False
    assert released == []

    changed, released = cache.observe_worker_incarnation(
        "prefill-0", "prefill-b"
    )

    assert changed is True
    assert released == ["old-0"]
    assert cache.has_worker_entries("prefill-0") is False
    assert cache.has_worker_entries("prefill-1") is True
    metrics = cache.stats()
    assert metrics["worker_incarnation_changes"] == 1
    assert metrics["worker_invalidations"] == 1
    assert metrics["worker_invalidated_entries"] == 1
    assert metrics["worker_incarnations"] == {
        "prefill-0": "prefill-b",
        "prefill-1": "prefill-other",
    }


def test_direct_feature_cache_rejects_late_store_from_stale_singleflight():
    cache = _DirectFeatureHandleCache(
        enabled=True,
        max_entries=8,
        max_bytes=1024 * 1024,
        ttl_s=60.0,
    )
    cache.observe_worker_incarnation("prefill-0", "prefill-a")
    cache.mark_worker_available("prefill-0")
    expected = cache.worker_incarnation("prefill-0")
    cache.observe_worker_incarnation("prefill-0", "prefill-b")

    with pytest.raises(RuntimeError, match="stale Prefill direct-handle"):
        cache.put_many(
            target_worker_id="prefill-0",
            handles=[
                _direct_handle_for_incarnation(
                    feature_id="late-a",
                    source_mm_hash="asset-0",
                    incarnation="prefill-a",
                )
            ],
            expected_worker_incarnation=expected,
            enforce_expected_incarnation=True,
        )

    assert cache.worker_incarnation("prefill-0") == "prefill-b"
    assert cache.has_worker_entries("prefill-0") is False
    assert cache.stats()["stale_singleflight_rejections"] == 1


def test_prefill_availability_metrics_separate_attempts_from_transitions():
    cache = _DirectFeatureHandleCache(
        enabled=True,
        max_entries=8,
        max_bytes=1024 * 1024,
        ttl_s=60.0,
    )
    cache.observe_worker_incarnation("prefill-0", "prefill-a")
    cache.mark_worker_available("prefill-0")

    for _ in range(3):
        cache.mark_worker_unavailable("prefill-0")
    unavailable = cache.stats()
    assert unavailable["worker_unavailable"] == 3
    assert unavailable["worker_unavailable_attempts"] == 3
    assert unavailable["worker_unavailable_transitions"] == 1
    assert unavailable["worker_recovery_transitions"] == 0
    assert unavailable["worker_availability"] == {"prefill-0": False}
    assert unavailable["unavailable_workers"] == ["prefill-0"]

    cache.observe_worker_incarnation("prefill-0", "prefill-a")
    cache.mark_worker_available("prefill-0")
    recovered = cache.stats()
    assert recovered["worker_recovery_transitions"] == 1
    assert recovered["worker_availability"] == {"prefill-0": True}
    assert recovered["unavailable_workers"] == []


@pytest.mark.parametrize(
    ("sample", "expected"),
    [(0.0, 0.8), (0.5, 1.0), (1.0, 1.2)],
)
def test_prefill_poll_jitter_is_symmetric_and_bounded(sample, expected):
    assert _jittered_poll_delay_s(
        interval_s=1.0,
        jitter_ratio=0.2,
        unit_sample=sample,
    ) == pytest.approx(expected)
    assert _jittered_poll_delay_s(
        interval_s=1.0,
        jitter_ratio=0.0,
        unit_sample=sample,
    ) == pytest.approx(1.0)


def test_prefill_incarnation_probe_invalidates_handles_and_cached_topology():
    cache = _DirectFeatureHandleCache(
        enabled=True,
        max_entries=8,
        max_bytes=1024 * 1024,
        ttl_s=60.0,
    )
    cache.put_many(
        target_worker_id="prefill-0",
        handles=[
            _direct_handle_for_incarnation(
                feature_id="old-0",
                source_mm_hash="asset-0",
                incarnation="prefill-a",
            )
        ],
    )

    class _Response:
        status_code = 200
        text = "prefill-b"
        content = b"prefill-b"
        headers = {VLLM_INCARNATION_HEADER: "prefill-b"}

        def raise_for_status(self):
            return None

        async def aclose(self):
            return None

    class _PrefillClient:
        async def get(self, path):
            assert path == "/mooncake_epd/incarnation"
            return _Response()

    class _DirectClient:
        released = []

        async def post(self, path, json):
            assert path == "release"
            self.released.extend(json["feature_ids"])
            return _Response()

    direct_client = _DirectClient()
    prefill_client = {
        "worker_id": "prefill-0",
        "client": _PrefillClient(),
        "remote_kv_topology": {
            "remote_engine_id": "prefill-engine-a",
            "remote_bootstrap_addr": "http://prefill-a:8998",
            "tp_size": 1,
        },
    }
    app = SimpleNamespace(
        state=SimpleNamespace(
            direct_feature_handle_cache=cache,
            prefill_direct_buffer_client=direct_client,
            proxy_config=SimpleNamespace(
                prefill_incarnation_probe_timeout_s=0.25,
                prefill_incarnation_endpoint="/mooncake_epd/incarnation",
            ),
        )
    )

    assert asyncio.run(
        _probe_prefill_worker_incarnation(
            app=app,
            prefill_client=prefill_client,
        )
    )

    assert direct_client.released == ["old-0"]
    assert "remote_kv_topology" not in prefill_client
    assert prefill_client["remote_api_incarnation"] == "prefill-b"
    metrics = cache.stats()
    assert metrics["worker_incarnation_changes"] == 1
    assert metrics["incarnation_probe_responses"] == 1
    assert metrics["incarnation_probe_failures"] == 0


def test_prefill_incarnation_monitor_debounces_only_consecutive_failures():
    cache = _DirectFeatureHandleCache(
        enabled=True,
        max_entries=8,
        max_bytes=1024 * 1024,
        ttl_s=60.0,
    )
    cache.observe_worker_incarnation("prefill-0", "prefill-a")
    cache.mark_worker_available("prefill-0")
    cache.put_many(
        target_worker_id="prefill-0",
        handles=[
            _direct_handle_for_incarnation(
                feature_id="monitor-cached-0",
                source_mm_hash="asset-0",
                incarnation="prefill-a",
            )
        ],
    )

    class _Response:
        status_code = 200
        text = "prefill-a"
        content = b"prefill-a"
        headers = {VLLM_INCARNATION_HEADER: "prefill-a"}

        async def aclose(self):
            return None

    class _SequencedPrefillClient:
        def __init__(self):
            self.outcomes = [False, False, True, False, False, False, False]

        async def get(self, path):
            assert path == "/mooncake_epd/incarnation"
            if not self.outcomes.pop(0):
                raise httpx.ReadTimeout("transient monitor timeout")
            return _Response()

    class _ControlPlane:
        def __init__(self):
            self.events = []

        def set_stage_worker_available(self, stage, worker_id, *, available):
            self.events.append((stage, worker_id, available))

    class _ReleaseResponse:
        def raise_for_status(self):
            return None

        async def aclose(self):
            return None

    class _DirectClient:
        def __init__(self):
            self.released = []

        async def post(self, path, json):
            assert path == "release"
            self.released.extend(json["feature_ids"])
            return _ReleaseResponse()

    control_plane = _ControlPlane()
    direct_client = _DirectClient()
    prefill_client = {
        "worker_id": "prefill-0",
        "client": _SequencedPrefillClient(),
        "remote_api_incarnation": "prefill-a",
        "remote_kv_topology": {"remote_engine_id": "prefill-engine-a"},
    }
    app = SimpleNamespace(
        state=SimpleNamespace(
            direct_feature_handle_cache=cache,
            proxy_config=SimpleNamespace(
                prefill_incarnation_probe_timeout_s=0.25,
                prefill_incarnation_endpoint="/mooncake_epd/incarnation",
                prefill_incarnation_failure_threshold=3,
            ),
            control_plane=control_plane,
            prefill_direct_buffer_client=direct_client,
        )
    )

    async def _run_sequence():
        assert not await _probe_prefill_worker_incarnation_for_monitor(
            app=app, prefill_client=prefill_client
        )
        assert not await _probe_prefill_worker_incarnation_for_monitor(
            app=app, prefill_client=prefill_client
        )
        before_reset = cache.stats()
        assert before_reset["incarnation_monitor_failure_streaks"] == {
            "prefill-0": 2
        }
        assert before_reset["incarnation_monitor_transient_failures"] == 2
        assert before_reset["unavailable_workers"] == []
        assert cache.has_worker_entries("prefill-0") is True
        assert prefill_client["remote_api_incarnation"] == "prefill-a"
        assert "remote_kv_topology" in prefill_client
        assert not any(not event[2] for event in control_plane.events)

        assert await _probe_prefill_worker_incarnation_for_monitor(
            app=app, prefill_client=prefill_client
        )
        assert cache.stats()["incarnation_monitor_failure_streaks"] == {}

        for _ in range(2):
            assert not await _probe_prefill_worker_incarnation_for_monitor(
                app=app, prefill_client=prefill_client
            )
        assert cache.stats()["unavailable_workers"] == []

        assert not await _probe_prefill_worker_incarnation_for_monitor(
            app=app, prefill_client=prefill_client
        )
        threshold_metrics = cache.stats()
        assert threshold_metrics["worker_unavailable_attempts"] == 1
        assert threshold_metrics["worker_unavailable_transitions"] == 1
        assert threshold_metrics["incarnation_monitor_threshold_reaches"] == 1
        assert threshold_metrics["unavailable_workers"] == ["prefill-0"]
        assert cache.has_worker_entries("prefill-0") is False
        assert direct_client.released == ["monitor-cached-0"]
        assert "remote_api_incarnation" not in prefill_client
        assert "remote_kv_topology" not in prefill_client

        # Further failures in the same streak must not repeat topology removal.
        assert not await _probe_prefill_worker_incarnation_for_monitor(
            app=app, prefill_client=prefill_client
        )
        assert cache.stats()["worker_unavailable_attempts"] == 1

    asyncio.run(_run_sequence())
    metrics = cache.stats()
    assert metrics["incarnation_monitor_streak_resets"] == 1
    assert metrics["incarnation_monitor_max_failure_streak"] == 4
    assert control_plane.events[-1] == ("prefill", "prefill-0", False)


def test_prefill_synchronous_incarnation_fence_remains_fail_closed():
    cache = _DirectFeatureHandleCache(
        enabled=True,
        max_entries=8,
        max_bytes=1024 * 1024,
        ttl_s=60.0,
    )
    cache.put_many(
        target_worker_id="prefill-0",
        handles=[
            _direct_handle_for_incarnation(
                feature_id="cached-0",
                source_mm_hash="asset-0",
                incarnation="prefill-a",
            )
        ],
    )
    cache.mark_worker_available("prefill-0")

    class _FailingPrefillClient:
        async def get(self, path):
            raise httpx.ReadTimeout("synchronous proof timeout")

    class _ReleaseResponse:
        def raise_for_status(self):
            return None

        async def aclose(self):
            return None

    class _DirectClient:
        def __init__(self):
            self.released = []

        async def post(self, path, json):
            assert path == "release"
            self.released.extend(json["feature_ids"])
            return _ReleaseResponse()

    direct_client = _DirectClient()
    prefill_client = {
        "worker_id": "prefill-0",
        "client": _FailingPrefillClient(),
        "remote_api_incarnation": "prefill-a",
        "remote_kv_topology": {"remote_engine_id": "prefill-engine-a"},
    }
    app = SimpleNamespace(
        state=SimpleNamespace(
            direct_feature_handle_cache=cache,
            prefill_direct_buffer_client=direct_client,
            proxy_config=SimpleNamespace(
                enable_prefill_incarnation_guard=True,
                enable_prefill_incarnation_probe_singleflight=False,
                prefill_incarnation_poll_s=1.0,
                prefill_incarnation_freshness_s=0.0,
                prefill_incarnation_probe_timeout_s=0.25,
                prefill_incarnation_endpoint="/mooncake_epd/incarnation",
                prefill_incarnation_failure_threshold=3,
            ),
            control_plane=None,
        )
    )

    with pytest.raises(proxy_module.HTTPException) as exc_info:
        asyncio.run(
            _fence_prefill_direct_handle_reuse(
                app=app,
                prefill_client=prefill_client,
            )
        )

    assert exc_info.value.status_code == 502
    assert direct_client.released == ["cached-0"]
    assert cache.has_worker_entries("prefill-0") is False
    metrics = cache.stats()
    assert metrics["synchronous_incarnation_probes"] == 1
    assert metrics["synchronous_incarnation_probe_dispatches"] == 1
    assert metrics["worker_unavailable_transitions"] == 1
    assert metrics["incarnation_monitor_failure_streaks"] == {}


def test_prefill_monitor_ignores_failure_superseded_by_newer_sync_success():
    cache = _DirectFeatureHandleCache(
        enabled=True,
        max_entries=8,
        max_bytes=1024 * 1024,
        ttl_s=60.0,
    )
    cache.observe_worker_incarnation("prefill-0", "prefill-a")
    cache.mark_worker_available("prefill-0")

    class _Response:
        status_code = 200
        text = "prefill-a"
        content = b"prefill-a"
        headers = {VLLM_INCARNATION_HEADER: "prefill-a"}

        async def aclose(self):
            return None

    class _BlockedFailureClient:
        def __init__(self):
            self.started = asyncio.Event()
            self.finish = asyncio.Event()

        async def get(self, path):
            self.started.set()
            await self.finish.wait()
            raise httpx.ReadTimeout("older background timeout")

    class _HealthyClient:
        async def get(self, path):
            return _Response()

    background_client = _BlockedFailureClient()
    monitor_info = {"worker_id": "prefill-0", "client": background_client}
    synchronous_info = {"worker_id": "prefill-0", "client": _HealthyClient()}
    app = SimpleNamespace(
        state=SimpleNamespace(
            direct_feature_handle_cache=cache,
            proxy_config=SimpleNamespace(
                prefill_incarnation_probe_timeout_s=0.25,
                prefill_incarnation_endpoint="/mooncake_epd/incarnation",
                prefill_incarnation_failure_threshold=3,
            ),
            control_plane=None,
        )
    )

    async def _run_race():
        monitor_task = asyncio.create_task(
            _probe_prefill_worker_incarnation_for_monitor(
                app=app,
                prefill_client=monitor_info,
            )
        )
        await background_client.started.wait()
        assert await _probe_prefill_worker_incarnation(
            app=app,
            prefill_client=synchronous_info,
        )
        background_client.finish.set()
        assert not await monitor_task

    asyncio.run(_run_race())
    metrics = cache.stats()
    assert metrics["incarnation_probe_failures"] == 1
    assert metrics["incarnation_monitor_superseded_failures"] == 1
    assert metrics["incarnation_monitor_failure_streaks"] == {}
    assert metrics["worker_unavailable_attempts"] == 0
    assert metrics["unavailable_workers"] == []


def test_prefill_incarnation_guard_learns_409_without_retrying_request():
    record = {"calls": 0, "incarnation": "prefill-b"}
    prefill_app = FastAPI()

    @prefill_app.post("/v1/chat/completions/render")
    async def guarded_render(request: Request):
        record["calls"] += 1
        record["expected"] = request.headers.get(
            VLLM_EXPECTED_INCARNATION_HEADER
        )
        actual = record["incarnation"]
        return PlainTextResponse(
            "prefill process incarnation changed",
            status_code=409,
            headers={
                VLLM_INCARNATION_HEADER: actual,
                VLLM_INCARNATION_MISMATCH_HEADER: "1",
            },
        )

    prefill_client = _client_override(
        prefill_app, "prefill-0", "prefill.local", 8100
    )
    prefill_client["remote_kv_topology"] = {
        "remote_engine_id": "prefill-engine-a",
        "remote_bootstrap_addr": "http://prefill-a:8998",
        "tp_size": 1,
    }
    proxy_app = create_app(
        ProxyConfig(
            prefill_direct_buffer_service_url="http://buffer.local",
            enable_direct_feature_handle_cache=True,
            enable_prefill_incarnation_guard=True,
            prefill_incarnation_poll_s=0.0,
            strict_no_fallback=True,
        ),
        prefill_clients=[prefill_client],
        decode_clients=[
            _client_override(
                _build_decode_app(record), "decode-0", "decode.local", 8200
            )
        ],
    )
    proxy_app.state.direct_feature_handle_cache.observe_worker_incarnation(
        "prefill-0", "prefill-a"
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "model": "fake-model",
                "messages": [{"role": "user", "content": "guarded"}],
                "stream": False,
            },
        )
        metrics = client.get("/metrics").json()["direct_feature_handle_cache"]

    assert response.status_code == 502
    assert record["calls"] == 1
    assert record["expected"] == "prefill-a"
    assert "remote_kv_topology" not in prefill_client
    assert metrics["worker_incarnations"] == {"prefill-0": "prefill-b"}
    assert metrics["incarnation_guard_rejections"] == 1


def test_proxy_rejects_uuid_only_media_when_protocol_is_disabled():
    record: dict = {}
    proxy_app = create_app(
        ProxyConfig(enable_client_mm_uuid_references=False),
        prefill_clients=[
            _client_override(_build_prefill_app(record), "prefill-0", "prefill.local", 8100)
        ],
        decode_clients=[
            _client_override(_build_decode_app(record), "decode-0", "decode.local", 8200)
        ],
    )
    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": None,
                                "uuid": "asset-disabled",
                            },
                            {"type": "text", "text": "describe"},
                        ],
                    }
                ]
            },
        )
        metrics = client.get("/metrics").json()

    assert response.status_code == 400
    assert "--enable-client-mm-uuid-references" in response.json()["detail"]
    assert metrics["metrics"]["requests_total"] == 0


def test_prerendered_decode_stream_reuses_render_payload_and_adapts_openai_sse():
    record: dict = {}
    proxy_app = create_app(
        ProxyConfig(
            enable_prerendered_decode=True,
            prerendered_decode_model="fake-model",
        ),
        prefill_clients=[
            _client_override(
                _build_prefill_app(record),
                "prefill-0",
                "prefill.local",
                8100,
            )
        ],
        decode_clients=[
            _client_override(
                _build_prerendered_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
        decode_tokenizer=_FakeDecodeTokenizer(),
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "model": "fake-model",
                "messages": [{"role": "user", "content": "use one render"}],
                "max_tokens": 16,
                "stream": True,
                "stream_options": {"include_usage": True},
            },
        )
        metrics = client.get("/metrics").json()["prerendered_decode"]

    assert response.status_code == 200, response.text
    assert response.headers["X-EPD-Decode-Protocol"] == "prerendered-generate"
    assert '"delta":{"role":"assistant","content":""}' in response.text
    assert '"delta":{"content":"hello"}' in response.text
    assert '"delta":{"content":" world"}' in response.text
    assert '"finish_reason":"stop"' in response.text
    assert '"completion_tokens":3' in response.text
    assert response.text.rstrip().endswith("data: [DONE]")
    assert record.get("decode_openai_calls", 0) == 0
    assert record["decode_generate_calls"] == 1
    decode_body = record["decode_generate_body"]
    assert decode_body["token_ids"] == [1, 2, 3, 4]
    assert decode_body["sampling_params"]["max_tokens"] == 16
    assert decode_body["stream_options"] == {"include_usage": True}
    assert decode_body["kv_transfer_params"]["do_remote_prefill"] is True
    assert (
        decode_body["sampling_params"]["extra_args"]["kv_transfer_params"]
        == decode_body["kv_transfer_params"]
    )
    assert metrics["selected"] == 1
    assert metrics["streaming"] == 1
    assert metrics["bypassed"] == 0


def test_prerendered_decode_non_stream_adapts_internal_token_response():
    record: dict = {}
    proxy_app = create_app(
        ProxyConfig(
            enable_prerendered_decode=True,
            prerendered_decode_model="fake-model",
        ),
        prefill_clients=[
            _client_override(
                _build_prefill_app(record),
                "prefill-0",
                "prefill.local",
                8100,
            )
        ],
        decode_clients=[
            _client_override(
                _build_prerendered_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
        decode_tokenizer=_FakeDecodeTokenizer(),
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "model": "fake-model",
                "messages": [{"role": "user", "content": "decode token ids"}],
                "max_tokens": 16,
                "stream": False,
            },
        )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["object"] == "chat.completion"
    assert payload["choices"][0]["message"] == {
        "role": "assistant",
        "content": "hello world",
    }
    assert payload["choices"][0]["finish_reason"] == "stop"
    assert payload["usage"]["completion_tokens"] == 3
    assert record.get("decode_openai_calls", 0) == 0
    assert record["decode_generate_calls"] == 1


def test_decode_mm_hash_cache_promotes_only_after_success_and_strips_hot_payload():
    serialized_feature = "base64-tensor-payload"
    record: dict = {
        "render_features": {
            "mm_hashes": {"image": ["image-hash-a"]},
            "mm_placeholders": {"image": [{"offset": 1, "length": 2}]},
            "kwargs_data": {"image": [serialized_feature]},
        }
    }
    proxy_app = create_app(
        ProxyConfig(
            enable_prerendered_decode=True,
            prerendered_decode_model="fake-model",
            enable_decode_mm_hash_cache=True,
            decode_mm_hash_cache_max_entries=4,
            decode_mm_hash_cache_ttl_s=60.0,
            strict_no_fallback=True,
        ),
        prefill_clients=[
            _client_override(
                _build_prefill_app(record),
                "prefill-0",
                "prefill.local",
                8100,
            )
        ],
        decode_clients=[
            _client_override(
                _build_prerendered_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
        decode_tokenizer=_FakeDecodeTokenizer(),
    )
    request_body = {
        "model": "fake-model",
        "messages": [{"role": "user", "content": "reuse image features"}],
        "stream": False,
    }

    with TestClient(proxy_app) as client:
        first = client.post("/v1/chat/completions", json=request_body)
        second = client.post("/v1/chat/completions", json=request_body)
        metrics = client.get("/metrics").json()["decode_mm_hash_cache"]

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert first.headers["X-EPD-Decode-MM-Features"] == "full"
    assert first.headers["X-EPD-Decode-Worker"] == "decode-0"
    assert second.headers["X-EPD-Decode-MM-Features"] == "hash-only"
    bodies = record["decode_generate_bodies"]
    assert bodies[0]["features"]["kwargs_data"] == {
        "image": [serialized_feature]
    }
    assert bodies[1]["features"]["kwargs_data"] is None
    assert metrics["entries"] == 1
    assert metrics["full_requests"] == 1
    assert metrics["hash_only_requests"] == 1
    assert metrics["promotions"] == 1
    assert metrics["invalidations"] == 0
    assert metrics["avoided_serialized_bytes"] == len(serialized_feature)


def test_decode_hash_only_request_carries_confirmed_incarnation_guard():
    record: dict = {
        "decode_process_start_time": "boot-token",
        "render_features": {
            "mm_hashes": {"image": ["image-hash-guard"]},
            "mm_placeholders": {"image": [{"offset": 1, "length": 2}]},
            "kwargs_data": {"image": ["serialized-guard-feature"]},
        },
    }
    proxy_app = create_app(
        ProxyConfig(
            enable_prerendered_decode=True,
            prerendered_decode_model="fake-model",
            enable_decode_mm_hash_cache=True,
            decode_mm_hash_epoch_endpoint="/mooncake_epd/incarnation",
            enable_decode_mm_hash_epoch_guard=True,
            strict_no_fallback=True,
        ),
        prefill_clients=[
            _client_override(
                _build_prefill_app(record),
                "prefill-0",
                "prefill.local",
                8100,
            )
        ],
        decode_clients=[
            _client_override(
                _build_prerendered_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
        decode_tokenizer=_FakeDecodeTokenizer(),
    )
    request_body = {
        "model": "fake-model",
        "messages": [{"role": "user", "content": "guard hash reuse"}],
        "stream": False,
    }

    with TestClient(proxy_app) as client:
        first = client.post("/v1/chat/completions", json=request_body)
        second = client.post("/v1/chat/completions", json=request_body)
        metrics = client.get("/metrics").json()["decode_mm_hash_cache"]

    assert first.status_code == 200
    assert second.status_code == 200
    headers = record["decode_generate_headers"]
    assert VLLM_EXPECTED_INCARNATION_HEADER.lower() not in headers[0]
    assert headers[1][VLLM_EXPECTED_INCARNATION_HEADER.lower()] == "boot-token"
    assert metrics["epoch_guarded_hash_only_requests"] == 1
    assert metrics["epoch_guard_rejections"] == 0
    assert metrics["epoch_guard_enabled"] is True


def test_decode_epoch_guard_learns_restart_epoch_before_full_rewarm():
    record: dict = {
        "decode_process_start_time": "boot-token-old",
        "enforce_incarnation_guard": True,
        "render_features": {
            "mm_hashes": {"image": ["image-hash-guard-restart"]},
            "mm_placeholders": {"image": [{"offset": 1, "length": 2}]},
            "kwargs_data": {"image": ["serialized-guard-restart-feature"]},
        },
    }
    proxy_app = create_app(
        ProxyConfig(
            enable_prerendered_decode=True,
            prerendered_decode_model="fake-model",
            enable_decode_mm_hash_cache=True,
            decode_mm_hash_epoch_poll_s=0.0,
            decode_mm_hash_epoch_freshness_s=300.0,
            decode_mm_hash_epoch_endpoint="/mooncake_epd/incarnation",
            enable_decode_mm_hash_epoch_guard=True,
            strict_no_fallback=True,
        ),
        prefill_clients=[
            _client_override(
                _build_prefill_app(record),
                "prefill-0",
                "prefill.local",
                8100,
            )
        ],
        decode_clients=[
            _client_override(
                _build_prerendered_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
        decode_tokenizer=_FakeDecodeTokenizer(),
    )
    request_body = {
        "model": "fake-model",
        "messages": [{"role": "user", "content": "restart epoch learning"}],
        "stream": False,
    }

    with TestClient(proxy_app) as client:
        warm = client.post("/v1/chat/completions", json=request_body)
        confirmed = client.post("/v1/chat/completions", json=request_body)
        record["decode_process_start_time"] = "boot-token-new"
        rejected = client.post("/v1/chat/completions", json=request_body)
        rewarm = client.post("/v1/chat/completions", json=request_body)
        recovered = client.post("/v1/chat/completions", json=request_body)
        metrics = client.get("/metrics").json()["decode_mm_hash_cache"]

    assert [
        warm.status_code,
        confirmed.status_code,
        rejected.status_code,
        rewarm.status_code,
        recovered.status_code,
    ] == [200, 200, 502, 200, 200]
    modes = [
        body["features"]["kwargs_data"] is None
        for body in record["decode_generate_bodies"]
    ]
    assert modes == [False, True, True, False, True]
    headers = record["decode_generate_headers"]
    assert headers[1][VLLM_EXPECTED_INCARNATION_HEADER.lower()] == (
        "boot-token-old"
    )
    assert headers[2][VLLM_EXPECTED_INCARNATION_HEADER.lower()] == (
        "boot-token-old"
    )
    assert headers[4][VLLM_EXPECTED_INCARNATION_HEADER.lower()] == (
        "boot-token-new"
    )
    assert metrics["worker_epochs"] == {"decode-0": "boot-token-new"}
    assert metrics["epoch_changes"] == 1
    assert metrics["epoch_guard_rejections"] == 1
    assert metrics["epoch_guard_epoch_learns"] == 1
    assert metrics["worker_invalidations"] == 1


def test_decode_mm_hash_cache_invalidates_failed_assumption_without_retry():
    record: dict = {
        "render_features": {
            "mm_hashes": {"image": ["image-hash-restart"]},
            "mm_placeholders": {"image": [{"offset": 1, "length": 2}]},
            "kwargs_data": {"image": ["full-feature"]},
        }
    }
    proxy_app = create_app(
        ProxyConfig(
            enable_prerendered_decode=True,
            prerendered_decode_model="fake-model",
            enable_decode_mm_hash_cache=True,
            strict_no_fallback=True,
        ),
        prefill_clients=[
            _client_override(
                _build_prefill_app(record),
                "prefill-0",
                "prefill.local",
                8100,
            )
        ],
        decode_clients=[
            _client_override(
                _build_prerendered_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
        decode_tokenizer=_FakeDecodeTokenizer(),
    )
    request_body = {
        "model": "fake-model",
        "messages": [{"role": "user", "content": "strict restart behavior"}],
        "stream": False,
    }

    with TestClient(proxy_app) as client:
        warm = client.post("/v1/chat/completions", json=request_body)
        record["reject_hash_only"] = True
        failed = client.post("/v1/chat/completions", json=request_body)
        record["reject_hash_only"] = False
        recovered = client.post("/v1/chat/completions", json=request_body)
        metrics = client.get("/metrics").json()["decode_mm_hash_cache"]

    assert warm.status_code == 200, warm.text
    assert failed.status_code == 502, failed.text
    assert recovered.status_code == 200, recovered.text
    assert record["decode_generate_calls"] == 3
    modes = [
        body["features"]["kwargs_data"] is None
        for body in record["decode_generate_bodies"]
    ]
    assert modes == [False, True, False]
    assert metrics["full_requests"] == 2
    assert metrics["hash_only_requests"] == 1
    assert metrics["invalidations"] == 1
    assert metrics["promotions"] == 2


def test_decode_mm_hash_cache_failure_invalidates_entire_worker_generation():
    cache = _DecodeMMHashWarmCache(
        enabled=True,
        max_entries=8,
        ttl_s=60.0,
    )

    def payload(mm_hash: str) -> dict:
        return {
            "features": {
                "mm_hashes": {"image": [mm_hash]},
                "kwargs_data": {"image": [f"serialized-{mm_hash}"]},
            }
        }

    for mm_hash in ("image-hash-a", "image-hash-b"):
        _prepared, lease = cache.prepare(worker_id="decode-0", payload=payload(mm_hash))
        assert lease is not None
        lease.succeed()
    cache.observe_worker_epoch("decode-0", "1")

    hot_payload, failed_lease = cache.prepare(
        worker_id="decode-0",
        payload=payload("image-hash-a"),
    )
    assert failed_lease is not None
    assert hot_payload["features"]["kwargs_data"] is None
    failed_lease.fail()

    recovered_payload, recovered_lease = cache.prepare(
        worker_id="decode-0",
        payload=payload("image-hash-b"),
    )
    assert recovered_lease is not None
    assert recovered_payload["features"]["kwargs_data"] is not None
    metrics = cache.stats()
    assert metrics["entries"] == 0
    assert metrics["invalidations"] == 2
    assert metrics["worker_invalidations"] == 1
    assert metrics["worker_invalidated_entries"] == 2


def test_decode_epoch_probe_updates_admission_worker_health():
    cache = _DecodeMMHashWarmCache(enabled=True, max_entries=8, ttl_s=60.0)
    control_plane = ServingControlPlane(
        ServingControlPlaneConfig(node_id="proxy-epoch-health")
    )
    control_plane.register_stage_workers("decode", ["decode-0", "decode-1"])

    class _Response:
        def __init__(self, status_code: int):
            self.status_code = status_code
            self.text = "process_start_time_seconds 100\n"

        async def aclose(self):
            return None

    class _Client:
        status_code = 503

        async def get(self, _path):
            return _Response(self.status_code)

    client = _Client()
    client_info = {"worker_id": "decode-1", "client": client}

    async def _run():
        assert not await _probe_decode_worker_epoch(
            cache=cache,
            client_info=client_info,
            timeout_s=0.25,
            control_plane=control_plane,
        )
        assert control_plane.stage_worker_availability("decode")["decode-1"] is False
        client.status_code = 200
        assert await _probe_decode_worker_epoch(
            cache=cache,
            client_info=client_info,
            timeout_s=0.25,
            control_plane=control_plane,
        )
        assert control_plane.stage_worker_availability("decode")["decode-1"] is True

    asyncio.run(_run())


def test_decode_epoch_probe_prefers_lightweight_incarnation_header():
    cache = _DecodeMMHashWarmCache(enabled=True, max_entries=8, ttl_s=60.0)

    class _Response:
        status_code = 200
        text = "body-token"
        content = b"body-token"
        headers = {"X-Mooncake-EPD-Incarnation": "boot-token"}

        async def aclose(self):
            return None

    class _Client:
        paths = []

        async def get(self, path):
            self.paths.append(path)
            return _Response()

    client = _Client()
    assert asyncio.run(
        _probe_decode_worker_epoch(
            cache=cache,
            client_info={"worker_id": "decode-0", "client": client},
            timeout_s=0.25,
            epoch_endpoint="/mooncake_epd/incarnation",
        )
    )

    metrics = cache.stats()
    assert client.paths == ["/mooncake_epd/incarnation"]
    assert metrics["worker_epochs"] == {"decode-0": "boot-token"}
    assert metrics["epoch_probe_responses"] == 1
    assert metrics["epoch_probe_response_bytes"] == len(b"body-token")
    assert metrics["epoch_probe_source_counts"] == {"incarnation-header": 1}


def test_decode_mm_hash_cache_renews_ttl_only_after_successful_hot_terminal(
    monkeypatch,
):
    now = [0.0]
    monkeypatch.setattr(proxy_module.time, "monotonic", lambda: now[0])
    cache = _DecodeMMHashWarmCache(enabled=True, max_entries=8, ttl_s=10.0)
    payload = {
        "features": {
            "mm_hashes": {"image": ["image-hash"]},
            "kwargs_data": {"image": ["serialized-feature"]},
        }
    }
    _cold_payload, cold_lease = cache.prepare(
        worker_id="decode-0",
        payload=payload,
    )
    assert cold_lease is not None
    cold_lease.succeed()
    cache.observe_worker_epoch("decode-0", "100")

    now[0] = 9.0
    hot_payload, hot_lease = cache.prepare(worker_id="decode-0", payload=payload)
    assert hot_lease is not None
    assert hot_payload["features"]["kwargs_data"] is None
    hot_lease.succeed()

    now[0] = 11.0
    renewed_payload, renewed_lease = cache.prepare(
        worker_id="decode-0",
        payload=payload,
    )
    assert renewed_lease is not None
    assert renewed_payload["features"]["kwargs_data"] is None
    metrics = cache.stats()
    assert metrics["renewals"] == 1
    assert metrics["expired"] == 0


def test_hot_terminal_cannot_resurrect_entry_invalidated_by_worker_epoch():
    cache = _DecodeMMHashWarmCache(enabled=True, max_entries=8, ttl_s=60.0)
    payload = {
        "features": {
            "mm_hashes": {"image": ["image-hash"]},
            "kwargs_data": {"image": ["serialized-feature"]},
        }
    }
    _cold_payload, cold_lease = cache.prepare(
        worker_id="decode-0",
        payload=payload,
    )
    assert cold_lease is not None
    cold_lease.succeed()
    cache.observe_worker_epoch("decode-0", "100")
    _hot_payload, hot_lease = cache.prepare(worker_id="decode-0", payload=payload)
    assert hot_lease is not None
    assert hot_lease.mode == "hash-only"

    cache.observe_worker_epoch("decode-0", "200")
    hot_lease.succeed()

    metrics = cache.stats()
    assert metrics["entries"] == 0
    assert metrics["renewals"] == 0
    assert metrics["epoch_changes"] == 1


def test_concurrent_hot_requests_collapse_worker_epoch_probe():
    cache = _DecodeMMHashWarmCache(enabled=True, max_entries=8, ttl_s=60.0)
    payload = {
        "features": {
            "mm_hashes": {"image": ["image-hash"]},
            "kwargs_data": {"image": ["serialized-feature"]},
        }
    }
    _prepared, lease = cache.prepare(worker_id="decode-0", payload=payload)
    assert lease is not None
    lease.succeed()
    cache.observe_worker_epoch("decode-0", "100")

    class _Response:
        status_code = 200
        text = "process_start_time_seconds 100\n"

        async def aclose(self):
            return None

    class _Client:
        calls = 0

        async def get(self, _path):
            self.calls += 1
            await asyncio.sleep(0.02)
            return _Response()

    client = _Client()
    app = SimpleNamespace(
        state=SimpleNamespace(
            decode_mm_hash_cache=cache,
            decode_epoch_probe_inflight={},
            proxy_config=SimpleNamespace(
                decode_mm_hash_epoch_probe_timeout_s=0.25,
                enable_decode_mm_hash_epoch_probe_singleflight=True,
            ),
        )
    )
    decode_client = {"worker_id": "decode-0", "client": client}

    async def _run():
        await asyncio.gather(
            _fence_decode_mm_hash_reuse(
                app=app,
                decode_client=decode_client,
            ),
            _fence_decode_mm_hash_reuse(
                app=app,
                decode_client=decode_client,
            ),
        )

    asyncio.run(_run())
    metrics = cache.stats()
    assert client.calls == 1
    assert metrics["synchronous_epoch_probes"] == 2
    assert metrics["synchronous_epoch_probe_dispatches"] == 1
    assert metrics["synchronous_epoch_probe_collapsed"] == 1
    assert app.state.decode_epoch_probe_inflight == {}


def test_fresh_epoch_lease_skips_redundant_synchronous_probe(monkeypatch):
    now = [100.0]
    monkeypatch.setattr(proxy_module.time, "monotonic", lambda: now[0])
    cache = _DecodeMMHashWarmCache(enabled=True, max_entries=8, ttl_s=60.0)
    payload = {
        "features": {
            "mm_hashes": {"image": ["image-hash"]},
            "kwargs_data": {"image": ["serialized-feature"]},
        }
    }
    _prepared, lease = cache.prepare(worker_id="decode-0", payload=payload)
    assert lease is not None
    lease.succeed()
    cache.observe_worker_epoch("decode-0", "100")

    class _Client:
        calls = 0

        async def get(self, _path):
            self.calls += 1
            raise AssertionError("fresh epoch lease should avoid a redundant probe")

    client = _Client()
    app = SimpleNamespace(
        state=SimpleNamespace(
            decode_mm_hash_cache=cache,
            decode_epoch_probe_inflight={},
            proxy_config=SimpleNamespace(
                decode_mm_hash_epoch_probe_timeout_s=0.25,
                decode_mm_hash_epoch_freshness_s=0.5,
                enable_decode_mm_hash_epoch_probe_singleflight=True,
            ),
        )
    )
    asyncio.run(
        _fence_decode_mm_hash_reuse(
            app=app,
            decode_client={"worker_id": "decode-0", "client": client},
        )
    )

    metrics = cache.stats()
    assert client.calls == 0
    assert metrics["epoch_freshness_skips"] == 1
    assert metrics["synchronous_epoch_probes"] == 0

    now[0] += 0.51
    assert cache.use_fresh_worker_epoch("decode-0", 0.5) is False
    cache.mark_worker_unavailable("decode-0")
    assert cache.use_fresh_worker_epoch("decode-0", 1.0) is False


def test_epoch_probe_singleflight_can_be_disabled_for_control_runs():
    cache = _DecodeMMHashWarmCache(enabled=True, max_entries=8, ttl_s=60.0)
    payload = {
        "features": {
            "mm_hashes": {"image": ["image-hash"]},
            "kwargs_data": {"image": ["serialized-feature"]},
        }
    }
    _prepared, lease = cache.prepare(worker_id="decode-0", payload=payload)
    assert lease is not None
    lease.succeed()
    cache.observe_worker_epoch("decode-0", "100")

    class _Response:
        status_code = 200
        text = "process_start_time_seconds 100\n"

        async def aclose(self):
            return None

    class _Client:
        calls = 0

        async def get(self, _path):
            self.calls += 1
            await asyncio.sleep(0.02)
            return _Response()

    client = _Client()
    app = SimpleNamespace(
        state=SimpleNamespace(
            decode_mm_hash_cache=cache,
            decode_epoch_probe_inflight={},
            proxy_config=SimpleNamespace(
                decode_mm_hash_epoch_probe_timeout_s=0.25,
                enable_decode_mm_hash_epoch_probe_singleflight=False,
            ),
        )
    )
    decode_client = {"worker_id": "decode-0", "client": client}

    async def _run():
        await asyncio.gather(
            _fence_decode_mm_hash_reuse(app=app, decode_client=decode_client),
            _fence_decode_mm_hash_reuse(app=app, decode_client=decode_client),
        )

    asyncio.run(_run())
    metrics = cache.stats()
    assert client.calls == 2
    assert metrics["synchronous_epoch_probes"] == 2
    assert metrics["synchronous_epoch_probe_dispatches"] == 2
    assert metrics["synchronous_epoch_probe_collapsed"] == 0


def test_decode_mm_hash_cache_stream_promotes_only_after_terminal_done():
    record: dict = {
        "render_features": {
            "mm_hashes": {"image": ["image-hash-stream"]},
            "mm_placeholders": {"image": [{"offset": 1, "length": 2}]},
            "kwargs_data": {"image": ["serialized-stream-feature"]},
        }
    }
    proxy_app = create_app(
        ProxyConfig(
            enable_prerendered_decode=True,
            prerendered_decode_model="fake-model",
            enable_decode_mm_hash_cache=True,
            strict_no_fallback=True,
        ),
        prefill_clients=[
            _client_override(
                _build_prefill_app(record),
                "prefill-0",
                "prefill.local",
                8100,
            )
        ],
        decode_clients=[
            _client_override(
                _build_prerendered_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
        decode_tokenizer=_FakeDecodeTokenizer(),
    )
    request_body = {
        "model": "fake-model",
        "messages": [{"role": "user", "content": "stream lifecycle"}],
        "stream": True,
    }

    with TestClient(proxy_app) as client:
        warm = client.post("/v1/chat/completions", json=request_body)
        record["drop_before_done"] = True
        with pytest.raises(Exception, match="closed without \\[DONE\\]"):
            client.post("/v1/chat/completions", json=request_body)
        record["drop_before_done"] = False
        recovered = client.post("/v1/chat/completions", json=request_body)
        metrics = client.get("/metrics").json()["decode_mm_hash_cache"]

    assert warm.status_code == 200, warm.text
    assert warm.headers["X-EPD-Decode-MM-Features"] == "full"
    assert recovered.status_code == 200, recovered.text
    assert recovered.headers["X-EPD-Decode-MM-Features"] == "full"
    modes = [
        body["features"]["kwargs_data"] is None
        for body in record["decode_generate_bodies"]
    ]
    assert modes == [False, True, False]
    assert metrics["promotions"] == 2
    assert metrics["invalidations"] == 1
    assert metrics["worker_invalidations"] == 1


def test_decode_mm_hash_cache_epoch_fence_cools_worker_before_reuse():
    record: dict = {
        "decode_process_start_time": "100",
        "render_features": {
            "mm_hashes": {"image": ["image-hash-epoch"]},
            "mm_placeholders": {"image": [{"offset": 1, "length": 2}]},
            "kwargs_data": {"image": ["serialized-epoch-feature"]},
        },
    }
    proxy_app = create_app(
        ProxyConfig(
            enable_prerendered_decode=True,
            prerendered_decode_model="fake-model",
            enable_decode_mm_hash_cache=True,
            decode_mm_hash_epoch_poll_s=0.01,
            decode_mm_hash_epoch_probe_timeout_s=0.25,
            strict_no_fallback=True,
        ),
        prefill_clients=[
            _client_override(
                _build_prefill_app(record),
                "prefill-0",
                "prefill.local",
                8100,
            )
        ],
        decode_clients=[
            _client_override(
                _build_prerendered_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
        decode_tokenizer=_FakeDecodeTokenizer(),
    )
    request_body = {
        "model": "fake-model",
        "messages": [{"role": "user", "content": "epoch fence"}],
        "stream": False,
    }

    with TestClient(proxy_app) as client:
        first = client.post("/v1/chat/completions", json=request_body)
        second = client.post("/v1/chat/completions", json=request_body)
        record["decode_process_start_time"] = "200"
        time.sleep(0.05)
        after_restart = client.post("/v1/chat/completions", json=request_body)
        metrics = client.get("/metrics").json()["decode_mm_hash_cache"]

    assert first.headers["X-EPD-Decode-MM-Features"] == "full"
    assert second.headers["X-EPD-Decode-MM-Features"] == "hash-only"
    assert after_restart.status_code == 200, after_restart.text
    assert after_restart.headers["X-EPD-Decode-MM-Features"] == "full"
    assert metrics["epoch_monitor_enabled"] is True
    assert metrics["epoch_changes"] == 1
    assert metrics["worker_invalidations"] == 1


def test_prerendered_render_cache_never_reuses_per_request_decode_kv():
    record: dict = {}
    proxy_app = create_app(
        ProxyConfig(
            enable_prerendered_decode=True,
            prerendered_decode_model="fake-model",
            enable_prefill_render_cache=True,
            prefill_render_cache_max_entries=4,
            prefill_render_cache_max_bytes=1024 * 1024,
        ),
        prefill_clients=[
            _client_override(
                _build_prefill_app(record),
                "prefill-0",
                "prefill.local",
                8100,
            )
        ],
        decode_clients=[
            _client_override(
                _build_prerendered_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
        decode_tokenizer=_FakeDecodeTokenizer(),
    )
    request_body = {
        "model": "fake-model",
        "messages": [{"role": "user", "content": "same semantic render"}],
        "stream": False,
    }

    with TestClient(proxy_app) as client:
        first = client.post(
            "/v1/chat/completions",
            json=request_body | {"metadata": {"workflow_id": "render-kv-1"}},
        )
        second = client.post(
            "/v1/chat/completions",
            json=request_body | {"metadata": {"workflow_id": "render-kv-2"}},
        )

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert record["prefill_render_calls"] == 1
    bodies = record["decode_generate_bodies"]
    assert len(bodies) == 2
    first_kv = bodies[0]["kv_transfer_params"]
    second_kv = bodies[1]["kv_transfer_params"]
    assert first_kv["transfer_id"] != second_kv["transfer_id"]
    assert first_kv["handoff_id"] != second_kv["handoff_id"]
    assert (
        bodies[0]["sampling_params"]["extra_args"]["kv_transfer_params"]
        == first_kv
    )
    assert (
        bodies[1]["sampling_params"]["extra_args"]["kv_transfer_params"]
        == second_kv
    )


def test_prerendered_decode_bypasses_tool_response_without_silent_failure_fallback():
    record: dict = {}
    proxy_app = create_app(
        ProxyConfig(
            enable_prerendered_decode=True,
            prerendered_decode_model="fake-model",
            strict_no_fallback=True,
        ),
        prefill_clients=[
            _client_override(
                _build_prefill_app(record),
                "prefill-0",
                "prefill.local",
                8100,
            )
        ],
        decode_clients=[
            _client_override(
                _build_prerendered_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
        decode_tokenizer=_FakeDecodeTokenizer(),
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "model": "fake-model",
                "messages": [{"role": "user", "content": "call a tool"}],
                "tools": [
                    {
                        "type": "function",
                        "function": {"name": "lookup", "parameters": {}},
                    }
                ],
                "stream": False,
            },
        )
        metrics = client.get("/metrics").json()["prerendered_decode"]

    assert response.status_code == 200, response.text
    assert response.headers["X-EPD-Decode-Protocol"] == "openai"
    assert response.json()["choices"][0]["message"]["content"] == "public path"
    assert record["decode_openai_calls"] == 1
    assert record.get("decode_generate_calls", 0) == 0
    assert metrics["selected"] == 0
    assert metrics["bypassed"] == 1
    assert metrics["bypass_reasons"] == {"tool_response_parsing": 1}


def test_prerendered_decode_preserves_early_pipeline_overlap(monkeypatch):
    record: dict = {"prefill_finish_delay_s": 0.02}
    inject_calls = 0
    original_inject = proxy_module._inject_decode_kv_into_rendered_request

    def counted_inject(*args, **kwargs):
        nonlocal inject_calls
        inject_calls += 1
        return original_inject(*args, **kwargs)

    monkeypatch.setattr(
        proxy_module,
        "_inject_decode_kv_into_rendered_request",
        counted_inject,
    )
    prefill_client = _client_override(
        _build_pipeline_prefill_app(record),
        "prefill-0",
        "prefill.local",
        8100,
    )
    prefill_client["remote_kv_topology"] = {
        "remote_engine_id": "prefill-engine-pipeline",
        "remote_bootstrap_addr": "http://prefill-bootstrap:8998",
        "tp_size": 1,
    }
    proxy_app = create_app(
        ProxyConfig(
            enable_decode_pipeline=True,
            enable_prerendered_decode=True,
            prerendered_decode_model="fake-model",
        ),
        prefill_clients=[prefill_client],
        decode_clients=[
            _client_override(
                _build_prerendered_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
        decode_tokenizer=_FakeDecodeTokenizer(),
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "model": "fake-model",
                "messages": [{"role": "user", "content": "pipeline rendered"}],
                "stream": True,
            },
        )
        metrics = client.get("/metrics").json()["metrics"]

    assert response.status_code == 200, response.text
    assert response.headers["X-EPD-Decode-Pipeline"] == "active"
    assert response.headers["X-EPD-Decode-Protocol"] == "prerendered-generate"
    assert record["decode_started_before_prefill_finished"] is True
    assert record["decode_generate_calls"] == 1
    assert record.get("decode_openai_calls", 0) == 0
    assert record["decode_generate_body"]["kv_transfer_params"]["handoff_id"]
    assert inject_calls == 1
    assert metrics["handoff_prepared"] == 1
    assert metrics["handoff_committed"] == 1
    assert metrics["handoff_rolled_back"] == 0


def test_prerendered_decode_rolls_back_before_committing_malformed_first_packet():
    record: dict = {"malformed_internal": True}
    proxy_app = create_app(
        ProxyConfig(
            enable_prerendered_decode=True,
            prerendered_decode_model="fake-model",
            strict_no_fallback=True,
        ),
        prefill_clients=[
            _client_override(
                _build_prefill_app(record),
                "prefill-0",
                "prefill.local",
                8100,
            )
        ],
        decode_clients=[
            _client_override(
                _build_prerendered_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
        decode_tokenizer=_FakeDecodeTokenizer(),
    )

    with TestClient(proxy_app, raise_server_exceptions=False) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "model": "fake-model",
                "messages": [{"role": "user", "content": "reject bad packet"}],
                "stream": True,
            },
        )
        metrics = client.get("/metrics").json()["metrics"]

    assert response.status_code == 200
    assert metrics["handoff_prepared"] == 1
    assert metrics["handoff_committed"] == 0
    assert metrics["handoff_rolled_back"] == 1


def test_prerendered_decode_emits_usage_only_chunk_for_combined_internal_packet():
    record: dict = {"combined_usage": True}
    proxy_app = create_app(
        ProxyConfig(
            enable_prerendered_decode=True,
            prerendered_decode_model="fake-model",
        ),
        prefill_clients=[
            _client_override(
                _build_prefill_app(record),
                "prefill-0",
                "prefill.local",
                8100,
            )
        ],
        decode_clients=[
            _client_override(
                _build_prerendered_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
        decode_tokenizer=_FakeDecodeTokenizer(),
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "model": "fake-model",
                "messages": [{"role": "user", "content": "separate usage"}],
                "stream": True,
                "stream_options": {"include_usage": True},
            },
        )

    usage_packets = []
    for line in response.text.splitlines():
        if not line.startswith("data: {"):
            continue
        packet = json.loads(line[6:])
        if packet.get("usage") is not None:
            usage_packets.append(packet)
    assert len(usage_packets) == 1
    assert usage_packets[0]["choices"] == []
    assert usage_packets[0]["usage"]["completion_tokens"] == 3


def test_proxy_pipelines_decode_stream_open_with_prompt_only_prefill():
    record: dict = {}
    prefill_client = _client_override(
        _build_pipeline_prefill_app(record),
        "prefill-0",
        "prefill.local",
        8100,
    )
    prefill_client["remote_kv_topology"] = {
        "remote_engine_id": "prefill-engine-pipeline",
        "remote_bootstrap_addr": "http://prefill-bootstrap:8998",
        "tp_size": 1,
    }
    proxy_app = create_app(
        ProxyConfig(enable_decode_pipeline=True),
        prefill_clients=[prefill_client],
        decode_clients=[
            _client_override(
                _build_pipeline_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [
                    {
                        "role": "user",
                        "content": [{"type": "text", "text": "pipeline this request"}],
                    }
                ],
                "stream": True,
            },
        )

    assert response.status_code == 200
    assert b"pipelined" in response.content
    assert record["decode_started_before_prefill_finished"] is True
    assert record["decode_kv"]["do_remote_prefill"] is True
    assert record["decode_kv"]["remote_engine_id"] == "prefill-engine-pipeline"
    assert record["decode_kv"]["transfer_id"]


def test_decode_pipeline_inflight_gate_preserves_serial_path_under_pressure():
    record: dict = {}
    prefill_client = _client_override(
        _build_pipeline_prefill_app(record),
        "prefill-0",
        "prefill.local",
        8100,
    )
    prefill_client["remote_kv_topology"] = {
        "remote_engine_id": "prefill-engine-pipeline",
        "remote_bootstrap_addr": "http://prefill-bootstrap:8998",
        "tp_size": 1,
    }
    proxy_app = create_app(
        ProxyConfig(
            enable_decode_pipeline=True,
            decode_pipeline_max_inflight=1,
        ),
        prefill_clients=[prefill_client],
        decode_clients=[
            _client_override(
                _build_pipeline_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
    )

    with TestClient(proxy_app) as client:
        decode_worker = proxy_app.state.control_plane.stage_workers("decode")[0]
        decode_worker.current_load = 1
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [{"role": "user", "content": "gate early open"}],
                "stream": True,
            },
        )
        stats = client.get("/metrics").json()["decode_pipeline"]

    assert response.status_code == 200, response.text
    assert response.headers["X-EPD-Decode-Pipeline"] == "serial"
    assert record["decode_started_before_prefill_finished"] is False
    assert stats["eligible"] == 1
    assert stats["active"] == 0
    assert stats["suppressed_inflight"] == 1
    assert stats["max_inflight"] == 1


def test_decode_pipeline_warms_topology_serially_before_early_open():
    record: dict = {}
    prefill_client = _client_override(
        _build_pipeline_prefill_app(record),
        "prefill-0",
        "prefill.local",
        8100,
    )
    proxy_app = create_app(
        ProxyConfig(enable_decode_pipeline=True),
        prefill_clients=[prefill_client],
        decode_clients=[
            _client_override(
                _build_pipeline_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
    )
    request_body = {
        "messages": [{"role": "user", "content": "warm the topology"}],
        "stream": True,
    }

    with TestClient(proxy_app) as client:
        first = client.post("/v1/chat/completions", json=request_body)
        assert first.status_code == 200
        assert first.headers["X-EPD-Decode-Pipeline"] == "serial"
        assert record["decode_started_before_prefill_finished"] is False
        assert prefill_client["remote_kv_topology"] == {
            "remote_engine_id": "prefill-engine-pipeline",
            "remote_bootstrap_addr": "http://prefill-bootstrap:8998",
            "tp_size": 1,
        }

        record.pop("decode_started", None)
        record.pop("decode_started_before_prefill_finished", None)
        second = client.post("/v1/chat/completions", json=request_body)

    assert second.status_code == 200
    assert second.headers["X-EPD-Decode-Pipeline"] == "active"
    assert record["decode_started_before_prefill_finished"] is True
    assert record["decode_calls"] == 2


def test_prefill_topology_cache_preserves_process_incarnation():
    topology = proxy_module._normalize_remote_kv_topology(
        {
            "remote_engine_id": "prefill-engine",
            "remote_bootstrap_addr": "http://prefill-bootstrap:8998/",
            "remote_engine_incarnation": "prefill-token-a",
            "tp_size": 1,
        }
    )

    assert topology == {
        "remote_engine_id": "prefill-engine",
        "remote_bootstrap_addr": "http://prefill-bootstrap:8998",
        "remote_engine_incarnation": "prefill-token-a",
        "tp_size": 1,
    }


def test_decode_pipeline_rejects_topology_drift_without_retry_or_fallback():
    record: dict = {}
    prefill_client = _client_override(
        _build_pipeline_prefill_app(record),
        "prefill-0",
        "prefill.local",
        8100,
    )
    prefill_client["remote_kv_topology"] = {
        "remote_engine_id": "stale-prefill-engine",
        "remote_bootstrap_addr": "http://stale-prefill-bootstrap:8998",
        "tp_size": 1,
    }
    proxy_app = create_app(
        ProxyConfig(enable_decode_pipeline=True, strict_no_fallback=True),
        prefill_clients=[prefill_client],
        decode_clients=[
            _client_override(
                _build_pipeline_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [{"role": "user", "content": "reject stale topology"}],
                "stream": True,
            },
        )
        metrics = client.get("/metrics").json()

    assert response.status_code == 502
    assert "will not retry or fall back" in response.json()["detail"]
    assert record["decode_calls"] == 1
    assert record["decode_kv"]["remote_engine_id"] == "stale-prefill-engine"
    assert prefill_client["remote_kv_topology"]["remote_engine_id"] == (
        "prefill-engine-pipeline"
    )
    assert metrics["active_requests"] == []


def test_decode_pipeline_startup_failure_cancels_prefill_and_releases_admission():
    record: dict = {"prefill_finish_delay_s": 0.05}
    prefill_client = _client_override(
        _build_pipeline_prefill_app(record),
        "prefill-0",
        "prefill.local",
        8100,
    )
    prefill_client["remote_kv_topology"] = {
        "remote_engine_id": "prefill-engine-pipeline",
        "remote_bootstrap_addr": "http://prefill-bootstrap:8998",
        "tp_size": 1,
    }
    proxy_app = create_app(
        ProxyConfig(enable_decode_pipeline=True, strict_no_fallback=True),
        prefill_clients=[prefill_client],
        decode_clients=[
            _client_override(
                _build_failing_pipeline_decode_app(record),
                "decode-0",
                "decode.local",
                8200,
            )
        ],
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [{"role": "user", "content": "fail one early open"}],
                "stream": True,
            },
        )
        metrics = client.get("/metrics").json()

    assert response.status_code == 502
    assert "early pipeline startup" in response.json()["detail"]
    assert record["decode_calls"] == 1
    assert metrics["active_requests"] == []
    assert metrics["workers"]["prefill"][0]["current_load"] == 0
    assert metrics["workers"]["decode"][0]["current_load"] == 0


def test_structural_multimodal_copy_preserves_large_payload_and_input_immutability():
    large_data_url = "data:image/png;base64," + ("A" * (1024 * 1024))
    original = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": large_data_url, "detail": "high"},
                    },
                    {"type": "text", "text": "describe"},
                ],
            }
        ],
        "metadata": {"workflow_id": "wf-copy"},
    }

    rewritten = _copy_request_for_mm_url_rewrite(original)
    rewritten_item = rewritten["messages"][0]["content"][0]
    original_item = original["messages"][0]["content"][0]

    assert rewritten is not original
    assert rewritten["messages"] is not original["messages"]
    assert rewritten["messages"][0] is not original["messages"][0]
    assert rewritten_item is not original_item
    assert rewritten_item["image_url"] is not original_item["image_url"]
    assert rewritten_item["image_url"]["url"] is large_data_url

    _set_image_url_on_item(rewritten_item, "data:image/png;base64,optimized")

    assert rewritten_item["image_url"]["url"] == "data:image/png;base64,optimized"
    assert original_item["image_url"]["url"] == large_data_url


def test_multimodal_remote_url_rejects_private_and_metadata_networks():
    for url in (
        "http://127.0.0.1/image.png",
        "http://10.0.0.1/image.png",
        "http://169.254.169.254/latest/meta-data",
        "http://[::1]/image.png",
    ):
        with pytest.raises(ValueError, match="non-public"):
            asyncio.run(_validate_remote_mm_url(url, allow_private=False))

    asyncio.run(
        _validate_remote_mm_url(
            "https://8.8.8.8/image.png",
            allow_private=False,
        )
    )


def test_multimodal_remote_fetch_stops_when_stream_exceeds_byte_limit():
    upstream = FastAPI()

    @upstream.get("/large")
    async def large():
        async def _chunks():
            yield b"A" * 8
            yield b"B" * 8

        return StreamingResponse(_chunks(), media_type="image/png")

    async def _run() -> None:
        client = httpx.AsyncClient(
            base_url="http://127.0.0.1",
            transport=httpx.ASGITransport(app=upstream),
        )
        app = SimpleNamespace(
            state=SimpleNamespace(
                proxy_config=ProxyConfig(allow_private_mm_urls=True),
                mm_fetch_client=client,
            )
        )
        try:
            with pytest.raises(ValueError, match="too large"):
                await _load_mm_url_bytes(
                    app,
                    "http://127.0.0.1/large",
                    max_bytes=10,
                )
        finally:
            await client.aclose()

    asyncio.run(_run())


def test_cumulative_detokenizer_fallback_preserves_prompt_boundary():
    class _BoundaryTokenizer:
        def decode(self, token_ids, *, skip_special_tokens=False):
            values = list(token_ids)
            if values == [1, 2]:
                return "prompt"
            if values == [1, 2, 10]:
                return "prompt hello"
            if values == [1, 2, 10, 11]:
                return "prompt hello world"
            if values == [10]:
                return "WRONG-WITHOUT-PROMPT"
            return ""

    decoder = _IncrementalTokenDecoder(
        _BoundaryTokenizer(),
        prompt_token_ids=[1, 2],
        skip_special_tokens=True,
    )

    assert decoder.push([10]) == " hello"
    assert decoder.push([11]) == " world"



def test_proxy_propagates_control_plane_metadata_and_metrics(tmp_path):
    record: dict = {}
    prefill_app = _build_prefill_app(record)
    decode_app = _build_decode_app(record)
    sink = ConnectorMetricsSink(
        tmp_path,
        engine_id="prefill-engine-0",
        role="producer",
        hostname="prefill.local",
        rpc_port=8998,
        tp_rank=0,
    )
    sink.record(
        LayeredTransferWorkerMeta(
            grouped_batches=1,
            grouped_bytes=96,
            grouped_descriptors=3,
            peer_buffer_batches=1,
            peer_buffer_bytes=96,
            backend_counts={"peer_buffer_direct": 1},
        ),
        path_totals={
            "EPD": LayeredTransferWorkerMeta(
                grouped_batches=1,
                grouped_bytes=96,
                grouped_descriptors=3,
            )
        },
    )
    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            node_id="proxy-it",
            layers_per_group=6,
            group_delay_ms=2.5,
            connector_metrics_dir=str(tmp_path),
            enable_agent_state_clone=True,
        )
    )
    proxy_app = create_app(
        ProxyConfig(
            layers_per_group=6,
            group_delay_ms=2.5,
            transport_backend="mooncake_engine_direct",
            connector_metrics_dir=str(tmp_path),
        ),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
        control_plane=cp,
    )

    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": "https://example.com/cat.png"}},
                    {"type": "text", "text": "What is shown here?"},
                ],
            }
        ],
        "max_tokens": 16,
        "stream": True,
        "metadata": {"workflow_id": "wf-proxy-mm"},
    }

    with TestClient(proxy_app) as client:
        response = client.post("/v1/chat/completions", json=body)
        assert response.status_code == 200
        assert response.headers["x-epd-routing-path"] == "EPD"
        assert response.headers["x-epd-admission"] in {"ADMIT", "BACKPRESSURE"}
        assert b"data:" in response.content

        prefill_kv = record["prefill_generate_body"]["sampling_params"]["extra_args"]["kv_transfer_params"]
        decode_kv = record["decode_body"]["kv_transfer_params"]
        assert record["prefill_render_body"]["messages"] == body["messages"]
        assert record["prefill_generate_body"]["sampling_params"]["max_tokens"] == 0
        assert record["prefill_generate_body"]["sampling_params"]["min_tokens"] == 0
        assert prefill_kv["transfer_id"] == decode_kv["transfer_id"]
        assert prefill_kv["layered_kv_transfer"] is True
        assert prefill_kv["layers_per_group"] == 6
        assert prefill_kv["mm_prefetch_policy"] == "event_driven"
        assert decode_kv["do_remote_prefill"] is True
        assert decode_kv["do_remote_decode"] is False
        assert decode_kv["handoff_id"]
        assert decode_kv["workflow_id"] == "wf-proxy-mm"
        assert decode_kv["transport_backend"] == "mooncake_engine_direct"
        assert decode_kv["a2a_source_node"] == "prefill-0"
        assert decode_kv["a2a_target_node"] == "decode-0"

        metrics = client.get("/metrics")
        assert metrics.status_code == 200
        payload = metrics.json()
        assert payload["metrics"]["handoff_prepared"] == 1
        assert payload["metrics"]["handoff_committed"] == 1
        assert payload["metrics"]["requests_multimodal"] == 1
        assert payload["metrics"]["peer_buffer_batches"] == 1
        assert payload["metrics"]["path_stats"]["EPD"]["requests_total"] == 1
        assert payload["metrics"]["path_stats"]["EPD"]["handoff_committed"] == 1
        assert payload["metrics"]["path_stats"]["EPD"]["stage_dispatches"] == {
            "prefill": 1,
            "decode": 1,
        }
        assert payload["metrics"]["connector_path_stats"]["EPD"]["grouped_bytes"] == 96
        assert payload["metrics"]["remote_transfer_backend_counts"] == {"peer_buffer_direct": 1}

        fork = client.post(
            "/mooncake_epd/agent/fork",
            json={
                "workflow_id": "wf-proxy-mm",
                "parent_request_id": response.headers["x-request-id"],
                "branch_count": 2,
                "target_node_id": "decode-0",
            },
        )
        assert fork.status_code == 200
        fork_payload = fork.json()
        assert fork_payload["zero_copy_branches"] == 2
        assert fork_payload["copied_bytes"] == 0
        assert fork_payload["kv_block_ids"] == [
            "prefill-engine-0:11",
            "prefill-engine-0:12",
            "prefill-engine-0:13",
        ]

        metrics_after_fork = client.get("/metrics").json()
        assert metrics_after_fork["metrics"]["agent_state_clone_requests"] == 1
        assert metrics_after_fork["metrics"]["agent_state_clone_branches"] == 2



def test_proxy_rejects_when_decode_stage_is_exhausted():
    record: dict = {}
    prefill_app = _build_prefill_app(record)
    decode_app = _build_decode_app(record)
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-it-2"))
    proxy_app = create_app(
        ProxyConfig(),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
        control_plane=cp,
    )

    with TestClient(proxy_app) as client:
        cp.update_worker_load(
            "decode",
            "decode-0",
            current_load=64,
            max_capacity=64,
            queue_size=64,
            queue_capacity=64,
            service_rate=10.0,
            arrival_rate=20.0,
        )
        response = client.post(
            "/v1/chat/completions",
            json={"messages": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}]},
        )
        assert response.status_code == 503
        assert "decode" in response.text or "rejected" in response.text


def test_proxy_rolls_back_handoff_when_decode_stream_never_yields():
    record: dict = {}
    prefill_app = _build_prefill_app(record)
    decode_app = _build_empty_decode_app(record)
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-empty-stream"))
    proxy_app = create_app(
        ProxyConfig(),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
        control_plane=cp,
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [
                    {
                        "role": "user",
                        "content": [{"type": "text", "text": "hello"}],
                    }
                ],
                "stream": True,
                "metadata": {"workflow_id": "wf-empty-stream"},
            },
        )
        assert response.status_code == 200
        assert response.content == b""

        metrics = client.get("/metrics")
        assert metrics.status_code == 200
        payload = metrics.json()
        assert payload["metrics"]["handoff_prepared"] == 1
        assert payload["metrics"]["handoff_committed"] == 0
        assert payload["metrics"]["handoff_rolled_back"] == 1
        assert payload["metrics"]["path_stats"]["PD"]["requests_total"] == 1
        assert payload["metrics"]["path_stats"]["PD"]["handoff_rolled_back"] == 1


def test_proxy_updates_workflow_registry_across_request_lifecycle(tmp_path):
    record: dict = {}
    registry = WorkflowStateRegistry(str(tmp_path / "proxy-registry.jsonl"))
    prefill_app = _build_prefill_app(record)
    decode_app = _build_decode_app(record)
    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            node_id="proxy-registry-it",
            workflow_registry_wal_path=str(tmp_path / "serving-registry-shadow.jsonl"),
        ),
        workflow_registry=registry,
    )
    proxy_app = create_app(
        ProxyConfig(
            node_id="proxy-registry-it",
            workflow_registry_wal_path=str(tmp_path / "serving-registry-shadow.jsonl"),
        ),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
        control_plane=cp,
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [
                    {
                        "role": "user",
                        "content": [{"type": "text", "text": "hello registry"}],
                    }
                ],
                "stream": True,
                "metadata": {"workflow_id": "wf-proxy-registry"},
            },
        )
        assert response.status_code == 200
        request_id = response.headers["x-request-id"]

        registry_record = registry.get_record(request_id)
        assert registry_record is not None
        assert registry_record.workflow_id == "wf-proxy-registry"
        assert registry_record.status == "RELEASED"
        assert registry_record.agent_id == "decode-0"
        assert registry_record.released_at is not None

        metrics = client.get("/metrics").json()
        reg_snapshot = metrics["workflow_registry"]
        assert reg_snapshot["enabled"] is True
        assert request_id not in reg_snapshot["active_state_ids"]
        assert reg_snapshot["status_counts"]["RELEASED"] >= 1


def test_proxy_uses_prompt_only_prefill_and_keeps_decode_payload_unpatched():
    record: dict = {}
    prefill_app = _build_prefill_app(record)
    decode_app = _build_decode_app(record)
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-prompt-only"))
    proxy_app = create_app(
        ProxyConfig(),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
        control_plane=cp,
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [
                    {
                        "role": "user",
                        "content": [{"type": "text", "text": "What is the answer?"}],
                    }
                ],
                "max_tokens": 18,
                "stream": True,
                "metadata": {"workflow_id": "wf-prompt-only"},
            },
        )
        assert response.status_code == 200
        assert record["prefill_generate_body"]["request_id"] == response.headers["x-request-id"]
        assert record["prefill_generate_body"]["sampling_params"]["max_tokens"] == 0
        assert record["prefill_generate_body"]["sampling_params"]["min_tokens"] == 0
        assert "continue_final_message" not in record["decode_body"]
        assert "add_generation_prompt" not in record["decode_body"]
        assert record["decode_body"]["max_tokens"] == 18
        assert record["decode_body"]["messages"] == [
            {
                "role": "user",
                "content": [{"type": "text", "text": "What is the answer?"}],
            }
        ]
        text = response.text
        chunks = []
        for line in text.splitlines():
            if not line.startswith("data: ") or line.strip() == "data: [DONE]":
                continue
            packet = json.loads(line[6:])
            choices = list(packet.get("choices") or [])
            if not choices:
                continue
            delta = dict(choices[0].get("delta") or {})
            content = delta.get("content")
            if content:
                chunks.append(str(content))
        assert "".join(chunks) == "helloworld"
        usage_line = next(
            line for line in text.splitlines() if '"usage"' in line and '"completion_tokens"' in line
        )
        assert '"prompt_tokens": 12' in usage_line
        assert '"completion_tokens": 2' in usage_line
        assert '"total_tokens": 14' in usage_line


def test_prompt_only_prefill_does_not_short_circuit_when_user_budget_is_one():
    record: dict = {}
    prefill_app = _build_prefill_app(record)
    decode_app = _build_decode_app(record)
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-budget-one"))
    proxy_app = create_app(
        ProxyConfig(),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
        control_plane=cp,
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [
                    {
                        "role": "user",
                        "content": [{"type": "text", "text": "Say hi"}],
                    }
                ],
                "max_tokens": 1,
                "stream": False,
            },
        )
        assert response.status_code == 200
        assert record["decode_body"]["max_tokens"] == 1
        payload = response.json()
        assert payload["id"] == "decode-response"


def test_proxy_mm_store_prefetches_data_url_on_serving_hot_path(tmp_path):
    import base64

    record: dict = {}
    prefill_app = _build_prefill_app(record)
    decode_app = _build_decode_app(record)
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-mm-prefetch"))
    proxy_app = create_app(
        ProxyConfig(mm_prefetch_wait_ms=500.0),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
        control_plane=cp,
    )
    data_url = "data:image/png;base64," + base64.b64encode(b"not-a-real-png-but-real-bytes").decode("ascii")

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "image_url", "image_url": {"url": data_url}},
                            {"type": "text", "text": "describe"},
                        ],
                    }
                ],
                "metadata": {"workflow_id": "wf-mm-prefetch"},
            },
        )
        assert response.status_code == 200
        metrics = client.get("/metrics").json()
        assert metrics["metrics"]["mm_prefetch_attempted"] == 1
        assert metrics["metrics"]["mm_prefetch_completed"] == 1
        assert metrics["metrics"]["mm_prefetch_failed"] == 0
        assert metrics["metrics"]["path_stats"]["EPD"]["mm_prefetch_completed"] == 1
        assert metrics["metrics"]["path_stats"]["EPD"]["mm_prefetch_wait_ms_count"] == 1
        assert metrics["metrics"]["path_stats"]["EPD"]["mm_prefetch_wait_ms_avg"] >= 0.0
        assert metrics["metrics"]["first_token_ms"]["count"] == 1
        assert metrics["metrics"]["first_token_ms"]["p95"] is not None
        assert metrics["metrics"]["request_stage_timing_ms"]["proxy_parse"]["count"] == 1
        assert metrics["metrics"]["request_stage_timing_ms"]["mm_prepare"]["count"] == 1
        assert metrics["metrics"]["request_stage_timing_ms"]["prefill_dispatch"]["count"] == 1
        assert metrics["metrics"]["request_stage_timing_ms"]["decode_first_response"]["count"] == 1
        assert metrics["metrics"]["stage_conservation"]["count"] == 1
        assert metrics["mm_store"]["completed"] >= 1
        rendered_url = record["prefill_render_body"]["messages"][0]["content"][0]["image_url"]["url"]
        decode_url = record["decode_body"]["messages"][0]["content"][0]["image_url"]["url"]
        assert rendered_url.startswith("data:image/png;base64,")
        assert decode_url == rendered_url


def _feature_handle_payload_for_item(item: dict, *, feature_id: str = "feature-hidden-0") -> dict:
    from mooncake_epd.core.control import ServingControlPlane
    from mooncake_epd.core.state import FeatureBundle, FeatureHandle

    bundle = FeatureBundle(
        image_hash=feature_id,
        last_hidden=__import__("torch").randn(2, 4),
        intermediates=[],
        metadata={"model_fingerprint": "model-x", "processor_fingerprint": "processor-x"},
    )
    source_mm_hash = ServingControlPlane._stable_mm_hash(item)
    handle = FeatureHandle(
        handle_id="handle-0",
        feature_id=feature_id,
        store_id="external-encoder-store",
        uri=f"mmstore://external-encoder-store/{feature_id}",
        descriptor=bundle.descriptor(checksum=False),
        metadata={"source_mm_hash": source_mm_hash},
    )
    return handle.as_control_payload()


def test_proxy_feature_handle_mode_fails_fast_without_prefill_support():
    record: dict = {}
    prefill_app = _build_prefill_app(record)
    decode_app = _build_decode_app(record)
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-feature-handle-unsupported"))
    proxy_app = create_app(
        ProxyConfig(mm_prefetch_mode="feature_handle", prefill_supports_feature_handles=False),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
        control_plane=cp,
    )
    image_item = {"type": "image_url", "image_url": {"url": "https://example.com/hidden.png"}}

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [
                    {"role": "user", "content": [image_item, {"type": "text", "text": "describe"}]}
                ],
                "metadata": {
                    "workflow_id": "wf-feature-handle-unsupported",
                    "mooncake_epd_feature_handles": [_feature_handle_payload_for_item(image_item)],
                },
            },
        )
        assert response.status_code == 501
        assert "external multimodal hidden-state handles" in response.json()["detail"]
        metrics = client.get("/metrics").json()
        assert metrics["metrics"]["path_stats"]["EPD"]["requests_active"] == 0
        assert "prefill_generate_body" not in record


def test_proxy_feature_handle_mode_forwards_handles_when_prefill_supports_them():
    record: dict = {}
    prefill_app = _build_prefill_app(record)
    decode_app = _build_decode_app(record)
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-feature-handle"))
    proxy_app = create_app(
        ProxyConfig(mm_prefetch_mode="feature_handle", prefill_supports_feature_handles=True),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
        control_plane=cp,
    )
    image_url = "https://example.com/hidden.png"
    image_item = {"type": "image_url", "image_url": {"url": image_url}}
    feature_handle = _feature_handle_payload_for_item(image_item)

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [
                    {"role": "user", "content": [image_item, {"type": "text", "text": "describe"}]}
                ],
                "metadata": {
                    "workflow_id": "wf-feature-handle",
                    "mooncake_epd_feature_handles": [feature_handle],
                },
            },
        )
        assert response.status_code == 200
        prefill_kv = record["prefill_generate_body"]["sampling_params"]["extra_args"]["kv_transfer_params"]
        assert prefill_kv["mm_prefetch_policy"] == "feature_handle"
        assert prefill_kv["mm_feature_handles"][0]["handle_id"] == "handle-0"
        assert prefill_kv["mm_feature_handle_target_worker"] == "prefill-0"
        assert record["prefill_render_body"]["messages"][0]["content"][0]["image_url"]["url"] == image_url
        metrics = client.get("/metrics").json()
        assert metrics["metrics"]["path_stats"]["EPD"]["requests_active"] == 0
        assert metrics["metrics"]["path_stats"]["EPD"]["requests_total"] == 1
