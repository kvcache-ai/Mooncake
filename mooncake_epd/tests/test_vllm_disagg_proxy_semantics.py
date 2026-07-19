from __future__ import annotations

import json
import sys
from collections import OrderedDict
from copy import deepcopy

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.testclient import TestClient

from mooncake_epd.core.control import ServingControlPlane, ServingControlPlaneConfig
from mooncake_epd.core.control.connector_metrics import ConnectorMetricsSink
from mooncake_epd.core.control.vllm_transfer_primitives import LayeredTransferWorkerMeta
from mooncake_epd.core.state import WorkflowStateRegistry
from mooncake_epd.scripts.vllm_disagg_proxy import (
    ProxyConfig,
    _make_client,
    _prune_proxy_direct_feature_handle_cache,
    _store_proxy_direct_feature_handles,
    _upstream_exception_summary,
    create_app,
)



def _build_prefill_app(record: dict) -> FastAPI:
    app = FastAPI()

    @app.post("/v1/chat/completions/render")
    async def render_chat(request: Request):
        body = await request.json()
        record["prefill_render_body"] = body
        record.setdefault("prefill_render_bodies", []).append(body)
        return JSONResponse(
            {
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
        )

    @app.post("/inference/v1/generate")
    async def generate(request: Request):
        body = await request.json()
        record["prefill_generate_body"] = body
        record.setdefault("prefill_generate_bodies", []).append(body)
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
        record.setdefault("decode_bodies", []).append(body)
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


def _build_openai_prompt_only_prefill_app(
    record: dict,
    *,
    verified_capability: bool = False,
    fail_first_capability_probe: bool = False,
) -> FastAPI:
    app = FastAPI()

    if verified_capability:
        @app.get("/mooncake_epd/capabilities")
        async def capabilities():
            record["capability_calls"] = int(
                record.get("capability_calls", 0) or 0
            ) + 1
            if fail_first_capability_probe and record["capability_calls"] == 1:
                return JSONResponse(
                    {"detail": "worker still compiling"},
                    status_code=503,
                )
            return {
                "installed": True,
                "version_supported": True,
                "prompt_only_ready": True,
                "prompt_only_patch_installed": True,
                "prompt_envelope_metadata_patch_installed": True,
                "prompt_only_verified": True,
                "prompt_only_protocol_version": 2,
                "role": "prefill",
                "kv_role": "kv_producer",
                "worker_id": "prefill-0",
                "model_id_sha256": "",
                "model_family": "fake-vl",
                "model_config_sha256": "model-config",
                "tokenizer_id_sha256": "tokenizer",
                "chat_template_sha256": "chat-template",
                "kv_manifest_schema_version": (
                    "mooncake.epd.kv_transfer_manifest.v2"
                ),
            }

    @app.post("/v1/chat/completions")
    async def chat(request: Request):
        body = await request.json()
        record["prefill_openai_body"] = body
        kv = dict(body.get("kv_transfer_params") or {})
        kv.update(
            {
                "transfer_id": kv.get("transfer_id") or request.headers.get("X-Request-Id"),
                "remote_engine_id": "prefill-engine-openai",
                "remote_bootstrap_addr": "http://prefill-bootstrap-openai:8998",
                "remote_block_ids": [[21, 22, 23]],
            }
        )
        return JSONResponse(
            {
                "id": "prefill-openai-response",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": ""},
                        "finish_reason": "length",
                    }
                ],
                "usage": {"prompt_tokens": 10, "completion_tokens": 0, "total_tokens": 10},
                "prompt_token_ids": list(range(10)),
                "mooncake_epd_prompt_envelope": {
                    "version": 1,
                    "prompt_token_ids": list(range(10)),
                    "mm_placeholders": {
                        "image": [{"offset": 2, "length": 4}],
                    },
                    "mm_hashes": {"image": ["renderer-image-0"]},
                    "mm_metadata": {
                        "image": [{"image_grid_thw": [1, 2, 2]}],
                    },
                },
                "kv_transfer_params": kv,
            }
        )

    return app


def _build_token_decode_app(record: dict) -> FastAPI:
    app = FastAPI()

    @app.get("/mooncake_epd/capabilities")
    async def capabilities():
        return {
            "installed": True,
            "version_supported": True,
            "role": "decode",
            "kv_role": "kv_consumer",
            "worker_id": "decode-0",
            "model_id_sha256": "",
            "decode_token_prompt_ready": True,
            "decode_token_envelope_protocol_version": 1,
            "decode_token_endpoint": "/v1/completions",
            "model_family": "fake-vl",
            "model_config_sha256": "model-config",
            "tokenizer_id_sha256": "tokenizer",
            "chat_template_sha256": "chat-template",
            "kv_manifest_schema_version": (
                "mooncake.epd.kv_transfer_manifest.v2"
            ),
        }

    @app.post("/v1/completions")
    async def completion(request: Request):
        body = await request.json()
        record["decode_token_body"] = body
        if body.get("stream"):
            async def _gen():
                yield (
                    b'data: {"id":"cmpl-0","object":"text_completion",'
                    b'"choices":[{"index":0,"text":"hello","finish_reason":null}]}\n\n'
                )
                yield (
                    b'data: {"id":"cmpl-0","object":"text_completion",'
                    b'"choices":[{"index":0,"text":"world","finish_reason":"stop"}],'
                    b'"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}\n\n'
                )
                yield b"data: [DONE]\n\n"

            return StreamingResponse(_gen(), media_type="text/event-stream")
        return JSONResponse(
            {
                "id": "cmpl-0",
                "object": "text_completion",
                "model": "fake-model",
                "choices": [
                    {
                        "index": 0,
                        "text": "helloworld",
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 2,
                    "total_tokens": 12,
                },
            }
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


def _failing_client_override(worker_id: str, host: str, port: int) -> dict:
    def _fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadError("", request=request)

    return {
        "client": httpx.AsyncClient(
            base_url=f"http://{host}:{port}",
            transport=httpx.MockTransport(_fail),
            timeout=None,
        ),
        "host": host,
        "port": port,
        "id": 0,
        "worker_id": worker_id,
    }


def test_prefill_transport_error_reports_exception_class_without_payload():
    """An empty-string httpx error must remain diagnosable at the proxy edge."""

    record: dict = {}
    decode_app = _build_decode_app(record)
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-prefill-read-error"))
    proxy_app = create_app(
        ProxyConfig(),
        prefill_clients=[_failing_client_override("prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
        control_plane=cp,
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [{"role": "user", "content": [{"type": "text", "text": "hello"}]}],
                "max_tokens": 1,
            },
        )

    assert response.status_code == 502
    assert "prefill request failed: ReadError" in response.json()["detail"]
    assert _upstream_exception_summary(httpx.ReadError("")).startswith("ReadError:")


def test_prefill_http_client_disables_idle_connection_reuse_by_default():
    """A stale Prefill keep-alive socket is not safe to blindly retry."""

    proxy_app = create_app(ProxyConfig())
    with TestClient(proxy_app):
        prefill_client = proxy_app.state.prefill_clients[0]["client"]
        decode_client = proxy_app.state.decode_clients[0]["client"]

        assert (
            prefill_client._transport._pool._max_keepalive_connections  # noqa: SLF001
            == 0
        )
        assert (
            decode_client._transport._pool._max_keepalive_connections  # noqa: SLF001
            == sys.maxsize
        )


def test_prefill_http_client_can_explicitly_reuse_idle_connections():
    client = _make_client("http://prefill.local", keepalive=True)
    try:
        assert client._transport._pool._max_keepalive_connections == sys.maxsize  # noqa: SLF001
    finally:
        # The client has no open sockets, but close it to preserve async-client
        # lifecycle discipline without depending on a test server.
        import asyncio

        asyncio.run(client.aclose())



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
        assert response.headers["x-epd-prefill-worker"] == "prefill-0"
        assert response.headers["x-epd-decode-worker"] == "decode-0"
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
        usage_packet = json.loads(usage_line[len("data: "):])
        assert usage_packet["usage"]["prompt_tokens"] == 12
        assert usage_packet["usage"]["completion_tokens"] == 2
        assert usage_packet["usage"]["total_tokens"] == 14


def test_rendered_prefill_cache_reuses_render_only_with_fresh_kv_metadata():
    """Repeated immutable requests skip /render, never the per-request KV handoff."""

    record: dict = {}
    prefill_app = _build_prefill_app(record)
    decode_app = _build_decode_app(record)
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-render-cache"))
    proxy_app = create_app(
        ProxyConfig(
            enable_rendered_prefill_cache=True,
            rendered_prefill_cache_max_entries=4,
            rendered_prefill_cache_max_bytes=1024 * 1024,
            worker_generations=[("prefill-0", "generation-render-cache")],
        ),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
        control_plane=cp,
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": "reuse this rendered prompt"}],
            }
        ],
        "max_tokens": 8,
        "stream": False,
        "metadata": {
            "workflow_id": "wf-render-cache",
            # These replay-artifact fields deliberately vary for repeated
            # dataset requests.  They must not affect a rendered OpenAI
            # prompt, while real routing controls remain part of the key.
            "benchmark_source_workflow_id": "source-wf",
            "benchmark_dataset_cycle_index": 0,
            "benchmark_dataset_source_index": 3,
            # Simulate the real E->P direct-buffer path: repeated requests
            # may receive a new control-plane handle id/timing for the same
            # visible image. Those values must not defeat a /render cache hit.
            "mooncake_epd_feature_handles": [
                {
                    "handle_id": "cold-handle",
                    "feature_id": "same-feature",
                    "metadata": {"proxy_direct_timings_ms": {"direct_publish_ms": 8.0}},
                }
            ],
        },
        "kv_transfer_params": {
            "mm_feature_handles": [{"handle_id": "cold-handle"}],
            "mm_feature_handle_target_worker": "prefill-0",
        },
    }
    second_body = deepcopy(body)
    second_body["metadata"]["workflow_id"] = "wf-render-cache-retry"
    second_body["metadata"]["benchmark_dataset_cycle_index"] = 1
    second_body["metadata"]["benchmark_dataset_source_index"] = 4
    second_body["metadata"]["mooncake_epd_feature_handles"][0]["handle_id"] = "hot-handle"
    second_body["metadata"]["mooncake_epd_feature_handles"][0]["metadata"] = {
        "proxy_direct_timings_ms": {"direct_cache_lookup_ms": 0.1}
    }
    second_body["kv_transfer_params"]["mm_feature_handles"] = [{"handle_id": "hot-handle"}]

    with TestClient(proxy_app) as client:
        first = client.post("/v1/chat/completions", json=body)
        second = client.post("/v1/chat/completions", json=second_body)
        assert first.status_code == 200
        assert second.status_code == 200
        assert first.json()["choices"][0]["message"]["content"] == "helloworld"
        assert second.json()["choices"][0]["message"]["content"] == "helloworld"

        # ``_set_proxy_timing`` makes every forwarded body differ. A hit here
        # proves the cache removes only proxy-observability noise, not request
        # semantics. Prefill still receives a distinct request/KV handoff.
        assert len(record["prefill_render_bodies"]) == 1
        assert len(record["prefill_generate_bodies"]) == 2
        generated = record["prefill_generate_bodies"]
        assert generated[0]["request_id"] != generated[1]["request_id"]
        first_kv = generated[0]["sampling_params"]["extra_args"]["kv_transfer_params"]
        second_kv = generated[1]["sampling_params"]["extra_args"]["kv_transfer_params"]
        assert first_kv["transfer_id"] != second_kv["transfer_id"]
        decode_bodies = record["decode_bodies"]
        assert decode_bodies[0]["kv_transfer_params"]["handoff_id"] != decode_bodies[1]["kv_transfer_params"]["handoff_id"]

        first_timing = json.loads(first.headers["x-epd-timing-ms"])
        second_timing = json.loads(second.headers["x-epd-timing-ms"])
        assert first_timing["prefill_render_cache_hit"] == 0.0
        assert second_timing["prefill_render_cache_hit"] == 1.0
        assert second_timing["prefill_render_ms"] >= 0.0
        metrics = client.get("/metrics").json()["rendered_prefill_cache"]
        assert metrics["lookups"] == 2
        assert metrics["misses"] == 1
        assert metrics["hits"] == 1
        assert metrics["stores"] == 1
        assert metrics["entries"] == 1


def test_rendered_prefill_cache_preserves_fresh_vllm_cache_salt():
    """Prefix-cache isolation must not turn into a stale /generate salt."""

    record: dict = {}
    prefill_app = _build_prefill_app(record)
    decode_app = _build_decode_app(record)
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-cache-salt"))
    proxy_app = create_app(
        ProxyConfig(
            enable_rendered_prefill_cache=True,
            rendered_prefill_cache_max_entries=4,
            rendered_prefill_cache_max_bytes=1024 * 1024,
            worker_generations=[("prefill-0", "generation-cache-salt")],
        ),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
        control_plane=cp,
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": "same model-visible prompt"}],
            }
        ],
        "max_tokens": 8,
        "stream": False,
        "cache_salt": "isolation-a",
        "metadata": {"workflow_id": "wf-cache-salt-a"},
    }
    second_body = deepcopy(body)
    second_body["cache_salt"] = "isolation-b"
    second_body["metadata"]["workflow_id"] = "wf-cache-salt-b"

    with TestClient(proxy_app) as client:
        first = client.post("/v1/chat/completions", json=body)
        second = client.post("/v1/chat/completions", json=second_body)

        assert first.status_code == 200
        assert second.status_code == 200
        # cache_salt does not affect rendering, so only /generate must see the
        # per-request value while the rendered prompt stays cached.
        assert len(record["prefill_render_bodies"]) == 1
        generated = record["prefill_generate_bodies"]
        assert [item["cache_salt"] for item in generated] == [
            "isolation-a",
            "isolation-b",
        ]
        second_timing = json.loads(second.headers["x-epd-timing-ms"])
        assert second_timing["prefill_render_cache_hit"] == 1.0


def test_rendered_prefill_cache_bypasses_mutable_external_media():
    record: dict = {}
    prefill_app = _build_prefill_app(record)
    decode_app = _build_decode_app(record)
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-render-cache-url"))
    proxy_app = create_app(
        ProxyConfig(
            enable_mm_prefetch=False,
            enable_rendered_prefill_cache=True,
            worker_generations=[("prefill-0", "generation-render-cache-url")],
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
                    {"type": "image_url", "image_url": {"url": "https://example.invalid/mutable.png"}},
                    {"type": "text", "text": "describe"},
                ],
            }
        ],
        "max_tokens": 8,
        "stream": False,
        "metadata": {"workflow_id": "wf-render-cache-url"},
    }

    with TestClient(proxy_app) as client:
        assert client.post("/v1/chat/completions", json=body).status_code == 200
        assert client.post("/v1/chat/completions", json=body).status_code == 200
        assert len(record["prefill_render_bodies"]) == 2
        metrics = client.get("/metrics").json()["rendered_prefill_cache"]
        assert metrics["hits"] == 0
        assert metrics["mutable_media_bypasses"] == 2


def test_proxy_can_use_single_openai_prompt_only_prefill_call():
    record: dict = {}
    prefill_app = _build_openai_prompt_only_prefill_app(record)
    decode_app = _build_decode_app(record)
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-openai-prompt-only"))
    proxy_app = create_app(
        ProxyConfig(prefill_dispatch_mode="openai_prompt_only"),
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
                "metadata": {"workflow_id": "wf-openai-prompt-only"},
            },
        )
        assert response.status_code == 200
        assert record["prefill_openai_body"]["stream"] is False
        assert record["prefill_openai_body"]["max_tokens"] == 0
        assert record["prefill_openai_body"]["return_token_ids"] is True
        assert "stream_options" not in record["prefill_openai_body"]
        assert record["prefill_openai_body"]["kv_transfer_params"]["do_remote_prefill"] is False
        decode_kv = record["decode_body"]["kv_transfer_params"]
        assert decode_kv["remote_engine_id"] == "prefill-engine-openai"
        assert decode_kv["remote_block_ids"] == [[21, 22, 23]]
        timings = json.loads(response.headers["x-epd-timing-ms"])
        assert timings["proxy_request_body_bytes"] > 0.0
        assert timings["proxy_request_body_read_ms"] >= 0.0
        assert timings["proxy_request_json_decode_ms"] >= 0.0
        assert timings["prefill_openai_prompt_only_request_bytes"] > 0.0
        assert timings["prefill_openai_prompt_only_json_encode_ms"] >= 0.0
        assert timings["prefill_openai_prompt_only_response_bytes"] > 0.0
        assert timings["decode_request_bytes"] > 0.0
        assert timings["decode_json_encode_ms"] >= 0.0
        assert timings["prefill_openai_prompt_only_ms"] >= 0.0
        assert timings["prefill_render_ms"] == 0.0
        assert timings["prefill_generate_ms"] == 0.0
        assert timings["prefill_sampled_token_count"] == 0.0
        assert timings["decode_stream_open_ms"] >= 0.0
        assert timings["proxy_request_to_decode_stream_open_ms"] >= 0.0
        stream_timings = []
        for line in response.text.splitlines():
            if not line.startswith("data: ") or line.strip() == "data: [DONE]":
                continue
            packet = json.loads(line[6:])
            raw = packet.get("_mooncake_epd_proxy_timings_ms")
            if isinstance(raw, dict):
                stream_timings.append(raw)
        assert any("decode_first_event_ms" in item for item in stream_timings)
        assert any("decode_first_content_ms" in item for item in stream_timings)
        assert any("proxy_request_to_first_event_ms" in item for item in stream_timings)
        assert any("proxy_request_to_first_content_ms" in item for item in stream_timings)


def test_strict_proxy_rejects_unverified_openai_prompt_only_prefill():
    record: dict = {}
    prefill_app = _build_openai_prompt_only_prefill_app(record)
    decode_app = _build_decode_app(record)
    cp = ServingControlPlane(ServingControlPlaneConfig(node_id="proxy-openai-prompt-only-strict"))
    proxy_app = create_app(
        ProxyConfig(
            prefill_dispatch_mode="openai_prompt_only",
            strict_no_fallback=True,
        ),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
        control_plane=cp,
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [{"role": "user", "content": "What is the answer?"}],
                "max_tokens": 18,
            },
        )

    assert response.status_code == 502
    assert "openai_prompt_only capability is not verified" in response.json()["detail"]
    assert "prefill_openai_body" not in record


def test_strict_proxy_accepts_verified_prompt_only_protocol():
    record: dict = {}
    prefill_app = _build_openai_prompt_only_prefill_app(
        record,
        verified_capability=True,
    )
    decode_app = _build_decode_app(record)
    cp = ServingControlPlane(
        ServingControlPlaneConfig(node_id="proxy-openai-prompt-only-verified")
    )
    proxy_app = create_app(
        ProxyConfig(
            prefill_dispatch_mode="openai_prompt_only",
            strict_no_fallback=True,
        ),
        prefill_clients=[
            _client_override(prefill_app, "prefill-0", "prefill.local", 8100)
        ],
        decode_clients=[
            _client_override(decode_app, "decode-0", "decode.local", 8200)
        ],
        control_plane=cp,
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [{"role": "user", "content": "What is the answer?"}],
                "max_tokens": 18,
                "stream": True,
            },
        )
    assert response.status_code == 200
    timings = json.loads(response.headers["x-epd-timing-ms"])
    assert timings["prefill_prompt_only_capability_verified"] == 1.0
    assert timings["prefill_prompt_only_unverified_override"] == 0.0


def test_prompt_only_capability_refreshes_after_startup_probe_race():
    record: dict = {}
    prefill_app = _build_openai_prompt_only_prefill_app(
        record,
        verified_capability=True,
        fail_first_capability_probe=True,
    )
    decode_app = _build_decode_app(record)
    cp = ServingControlPlane(
        ServingControlPlaneConfig(node_id="proxy-prompt-only-capability-refresh")
    )
    proxy_app = create_app(
        ProxyConfig(
            prefill_dispatch_mode="openai_prompt_only",
            strict_no_fallback=True,
        ),
        prefill_clients=[
            _client_override(prefill_app, "prefill-0", "prefill.local", 8100)
        ],
        decode_clients=[
            _client_override(decode_app, "decode-0", "decode.local", 8200)
        ],
        control_plane=cp,
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [{"role": "user", "content": "Answer."}],
                "max_tokens": 4,
            },
        )

    assert response.status_code == 200
    assert record["capability_calls"] == 2
    timings = json.loads(response.headers["x-epd-timing-ms"])
    assert timings["prefill_prompt_only_capability_verified"] == 1.0


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


def test_decode_token_fast_path_strips_media_and_adapts_stream_to_chat():
    record: dict = {}
    prefill_app = _build_openai_prompt_only_prefill_app(
        record,
        verified_capability=True,
    )
    decode_app = _build_token_decode_app(record)
    cp = ServingControlPlane(
        ServingControlPlaneConfig(node_id="proxy-token-decode")
    )
    proxy_app = create_app(
        ProxyConfig(
            enable_mm_prefetch=False,
            prefill_dispatch_mode="openai_prompt_only",
            decode_dispatch_mode="token_ids",
            strict_no_fallback=True,
        ),
        prefill_clients=[
            _client_override(prefill_app, "prefill-0", "prefill.local", 8100)
        ],
        decode_clients=[
            _client_override(decode_app, "decode-0", "decode.local", 8200)
        ],
        control_plane=cp,
    )
    large_media = "data:image/png;base64," + ("A" * 100_000)

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
                                "image_url": {"url": large_media},
                            },
                            {"type": "text", "text": "Describe."},
                        ],
                    }
                ],
                "max_tokens": 8,
                "temperature": 0.0,
                "stream": True,
            },
        )
        lifecycle_metrics = client.get("/metrics").json()

    assert response.status_code == 200
    token_body = record["decode_token_body"]
    assert token_body["prompt"] == list(range(10))
    assert token_body["add_special_tokens"] is False
    assert "messages" not in token_body
    assert large_media not in json.dumps(token_body)
    envelope = token_body["metadata"]["mooncake_epd_decode_token_envelope"]
    assert envelope["version"] == 1
    assert envelope["prompt_token_count"] == 10
    assert envelope["prefill_worker_id"] == "prefill-0"
    assert envelope["decode_worker_id"] == "decode-0"
    assert envelope["multimodal_placeholder_spans"] == {
        "image": [{"offset": 2, "length": 4}]
    }
    assert envelope["render_multimodal_hashes"] == {
        "image": ["renderer-image-0"]
    }

    packets = [
        json.loads(line[6:])
        for line in response.text.splitlines()
        if line.startswith("data: ") and line.strip() != "data: [DONE]"
    ]
    assert packets[0]["object"] == "chat.completion.chunk"
    assert packets[0]["choices"][0]["delta"] == {
        "role": "assistant",
        "content": "hello",
    }
    assert packets[1]["choices"][0]["delta"]["content"] == "world"
    timings = json.loads(response.headers["x-epd-timing-ms"])
    assert timings["decode_token_capability_verified"] == 1.0
    assert timings["decode_token_envelope_validated"] == 1.0
    assert timings["decode_token_fast_path"] == 1.0
    assert timings["decode_token_media_stripped"] == 1.0
    assert timings["decode_request_bytes"] < 20_000
    decode_worker = lifecycle_metrics["workers"]["decode"][0]
    assert decode_worker["queued_requests"] == 0
    assert decode_worker["running_requests"] == 0
    assert decode_worker["first_token_pending"] == 0
    assert decode_worker["active_decode_sequences"] == 0
    assert decode_worker["avg_first_token_ms"] > 0.0


def test_decode_token_fast_path_fails_closed_for_chat_tools():
    record: dict = {}
    prefill_app = _build_openai_prompt_only_prefill_app(
        record,
        verified_capability=True,
    )
    decode_app = _build_token_decode_app(record)
    cp = ServingControlPlane(
        ServingControlPlaneConfig(node_id="proxy-token-decode-tools")
    )
    proxy_app = create_app(
        ProxyConfig(
            prefill_dispatch_mode="openai_prompt_only",
            decode_dispatch_mode="token_ids",
            strict_no_fallback=True,
        ),
        prefill_clients=[
            _client_override(prefill_app, "prefill-0", "prefill.local", 8100)
        ],
        decode_clients=[
            _client_override(decode_app, "decode-0", "decode.local", 8200)
        ],
        control_plane=cp,
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [{"role": "user", "content": "Call a tool."}],
                "max_tokens": 8,
                "tools": [
                    {
                        "type": "function",
                        "function": {
                            "name": "lookup",
                            "parameters": {"type": "object"},
                        },
                    }
                ],
                "tool_choice": "auto",
            },
        )

    assert response.status_code == 502
    assert "tool parsing" in response.json()["detail"]
    assert "decode_token_body" not in record


def test_decode_token_shadow_validates_without_changing_decode_api():
    record: dict = {}
    prefill_app = _build_openai_prompt_only_prefill_app(
        record,
        verified_capability=True,
    )
    decode_app = _build_decode_app(record)

    @decode_app.get("/mooncake_epd/capabilities")
    async def capabilities():
        return {
            "installed": True,
            "version_supported": True,
            "role": "decode",
            "kv_role": "kv_consumer",
            "worker_id": "decode-0",
            "model_id_sha256": "",
            "decode_token_prompt_ready": True,
            "decode_token_envelope_protocol_version": 1,
            "decode_token_endpoint": "/v1/completions",
            "model_family": "fake-vl",
            "model_config_sha256": "model-config",
            "tokenizer_id_sha256": "tokenizer",
            "chat_template_sha256": "chat-template",
            "kv_manifest_schema_version": (
                "mooncake.epd.kv_transfer_manifest.v2"
            ),
        }

    cp = ServingControlPlane(
        ServingControlPlaneConfig(node_id="proxy-token-shadow")
    )
    proxy_app = create_app(
        ProxyConfig(
            prefill_dispatch_mode="openai_prompt_only",
            decode_dispatch_mode="shadow",
            strict_no_fallback=True,
        ),
        prefill_clients=[
            _client_override(prefill_app, "prefill-0", "prefill.local", 8100)
        ],
        decode_clients=[
            _client_override(decode_app, "decode-0", "decode.local", 8200)
        ],
        control_plane=cp,
    )

    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [{"role": "user", "content": "Answer."}],
                "max_tokens": 4,
            },
        )

    assert response.status_code == 200
    assert "messages" in record["decode_body"]
    timings = json.loads(response.headers["x-epd-timing-ms"])
    assert timings["decode_token_envelope_validated"] == 1.0
    assert timings["decode_token_shadow_only"] == 1.0
    assert "decode_token_fast_path" not in timings


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


def test_proxy_consumes_agent_state_decode_only_hot_path(tmp_path):
    record: dict = {}
    prefill_app = _build_prefill_app(record)
    decode_app = _build_decode_app(record)
    cp = ServingControlPlane(
        ServingControlPlaneConfig(
            node_id="proxy-agent-consume",
            enable_agent_state_clone=True,
            workflow_registry_wal_path=str(tmp_path / "agent-consume.jsonl"),
        ),
        workflow_registry=WorkflowStateRegistry(str(tmp_path / "agent-consume.jsonl")),
    )
    cp.kv_directory.ensure_block_record(
        "prefill-engine-0:11",
        workflow_id="wf-agent-consume",
        owner_shard="prefill-0",
        physical_node_id="prefill-0",
        external_placeholder=True,
    )
    cp.register_agent_state(
        workflow_id="wf-agent-consume",
        state_id="branch-consume-0",
        kv_block_ids=["prefill-engine-0:11"],
        token_ids=[101, 102, 103],
        target_node_id="decode-0",
        kv_transfer_params={
            "remote_engine_id": "prefill-engine-0",
            "remote_bootstrap_addr": "http://prefill-bootstrap:8998",
            "remote_block_ids": [[11]],
            "a2a_source_node": "prefill-0",
            "remote_prefill_prompt_tokens": 3,
        },
    )
    proxy_app = create_app(
        ProxyConfig(enable_mm_prefetch=False),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
        control_plane=cp,
    )
    with TestClient(proxy_app) as client:
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [{"role": "user", "content": "continue branch"}],
                "metadata": {
                    "workflow_id": "wf-agent-consume",
                    "mooncake_epd_agent_state_id": "branch-consume-0",
                },
                "max_tokens": 4,
            },
        )
        assert response.status_code == 200
        assert response.headers["x-epd-routing-path"] == "AGENT_STATE"
        assert "prefill_generate_body" not in record
        decode_kv = record["decode_body"]["kv_transfer_params"]
        assert decode_kv["epd_agent_state_consume"] is True
        assert decode_kv["do_remote_prefill"] is True
        assert decode_kv["remote_engine_id"] == "prefill-engine-0"
        assert decode_kv["remote_block_ids"] == [[11]]
        assert decode_kv["agent_state_id"] == "branch-consume-0"
        metrics = client.get("/metrics").json()["metrics"]
        assert metrics["agent_state_consume_requests"] == 1
        assert metrics["agent_state_consume_success"] == 1


def test_direct_feature_handle_cache_sweeps_ttl_once_per_bounded_interval():
    app = FastAPI()
    app.state.proxy_config = ProxyConfig(
        enable_direct_feature_handle_cache=True,
        direct_feature_handle_cache_max_entries=8,
        direct_feature_handle_cache_ttl_s=60.0,
    )
    app.state.direct_feature_handle_cache = OrderedDict()
    app.state.direct_feature_handle_cache_stats = {
        "hits": 0,
        "misses": 0,
        "stores": 0,
        "evictions": 0,
        "expired": 0,
        "ttl_sweeps": 0,
        "ttl_sweep_entries_scanned": 0,
        "entries": 0,
    }
    app.state.direct_feature_handle_cache_next_ttl_sweep_at = 0.0
    handles = [
        {
            "feature_id": f"feature-{index}",
            "uri": f"epd-direct://feature-{index}",
            "metadata": {
                "direct_plan": {"targets": []},
                "direct_remote_session": "prefill-session",
            },
        }
        for index in range(2)
    ]

    _store_proxy_direct_feature_handles(app, handles, target_worker_id="prefill-0")
    stats = app.state.direct_feature_handle_cache_stats
    assert stats["ttl_sweeps"] == 1
    assert stats["ttl_sweep_entries_scanned"] == 2

    # The next sweep is deliberately deferred; capacity enforcement remains
    # cheap and immediate even while expiry reclamation is throttled.
    app.state.direct_feature_handle_cache_next_ttl_sweep_at = float("inf")
    _store_proxy_direct_feature_handles(app, handles[:1], target_worker_id="prefill-0")
    assert stats["ttl_sweeps"] == 1
    _prune_proxy_direct_feature_handle_cache(app, force_ttl_sweep=True)
    assert stats["ttl_sweeps"] == 2
