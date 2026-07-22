from __future__ import annotations

import copy
import json
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx

EXAMPLE_DIR = Path(__file__).resolve().parents[2] / "example"
if str(EXAMPLE_DIR) not in sys.path:
    sys.path.insert(0, str(EXAMPLE_DIR))

import cache_aware_disagg_proxy as proxy  # noqa: E402


def valid_config_dict() -> dict[str, Any]:
    return {
        "conductor": {
            "address": "http://conductor.test:13333",
            "query_timeout_seconds": 0.25,
            "registration_timeout_seconds": 2.0,
        },
        "prefill": {
            "config": {
                "modelname": "test-model",
                "block_size": 16,
                "tenant_id": "tenant-a",
                "lora_name": "adapter-a",
                "hash_profile": {
                    "strategy": "vllm_v1",
                    "algorithm": "sha256_cbor",
                    "python_hash_seed": "0",
                    "index_projection": "low64_be",
                },
            },
            "instances": [
                {
                    "instance_id": "prefill-a",
                    "http_endpoint": "http://prefill-a.test:8100",
                    "registrations": [
                        {"endpoint": "tcp://prefill-a.test:5557", "dp_rank": 0},
                        {"endpoint": "tcp://prefill-a.test:5558", "dp_rank": 1},
                    ],
                },
                {
                    "instance_id": "prefill-b",
                    "http_endpoint": "http://prefill-b.test:8101",
                    "registrations": [
                        {"endpoint": "tcp://prefill-b.test:5557", "dp_rank": 0}
                    ],
                },
            ],
        },
        "decode": {
            "instances": [
                {"http_endpoint": "http://decode-a.test:8200"},
                {"http_endpoint": "http://decode-b.test:8201"},
            ]
        },
    }


def valid_config() -> proxy.ProxyConfig:
    return proxy.parse_config(valid_config_dict())


def cloned_config_dict() -> dict[str, Any]:
    return copy.deepcopy(valid_config_dict())


def request_json(request: httpx.Request) -> dict[str, Any]:
    body = json.loads(request.content)
    assert isinstance(body, dict)
    return body


Handler = Callable[[httpx.Request], Awaitable[httpx.Response]]


class RecordingClientFactory:
    def __init__(self, handler: Handler) -> None:
        self.requests: list[httpx.Request] = []
        self.clients: list[tuple[str, float | None, httpx.AsyncClient]] = []
        self._handler = handler

    def __call__(
        self, base_url: str, timeout_seconds: float | None
    ) -> httpx.AsyncClient:
        async def record(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            return await self._handler(request)

        timeout = None if timeout_seconds is None else httpx.Timeout(timeout_seconds)
        client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            transport=httpx.MockTransport(record),
        )
        self.clients.append((base_url, timeout_seconds, client))
        return client


async def registration_success_handler(request: httpx.Request) -> httpx.Response:
    if request.url.path != "/register":
        raise AssertionError(f"unexpected request: {request.method} {request.url}")
    payload = request_json(request)
    return httpx.Response(
        200,
        json={
            "status": "registered successfully",
            "instance_id": payload["instance_id"],
        },
    )
