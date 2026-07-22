from __future__ import annotations

import unittest

import httpx

from _support import (
    RecordingClientFactory,
    proxy,
    registration_success_handler,
    request_json,
    valid_config,
)


class ConductorClientTest(unittest.IsolatedAsyncioTestCase):
    async def test_registration_payloads_follow_current_contract(self) -> None:
        config = valid_config()
        factory = RecordingClientFactory(registration_success_handler)
        client = factory(
            config.conductor.address, config.conductor.query_timeout_seconds
        )
        conductor = proxy.ConductorClient(
            config.conductor, config.prefill.config, client
        )
        self.addAsyncCleanup(client.aclose)

        payloads = conductor.registration_payloads(config.prefill.instances)

        self.assertEqual(3, len(payloads))
        self.assertEqual(
            {
                "endpoint": "tcp://prefill-a.test:5557",
                "type": "vLLM",
                "modelname": "test-model",
                "instance_id": "prefill-a",
                "block_size": 16,
                "dp_rank": 0,
                "tenant_id": "tenant-a",
                "lora_name": "adapter-a",
                "hash_profile": {
                    "strategy": "vllm_v1",
                    "algorithm": "sha256_cbor",
                    "python_hash_seed": "0",
                    "index_projection": "low64_be",
                },
            },
            payloads[0],
        )
        for payload in payloads:
            self.assertNotIn("replay_endpoint", payload)
            self.assertNotIn("cache_salt", payload)

    async def test_runtime_registers_all_ranks_and_does_not_unregister(self) -> None:
        config = valid_config()
        factory = RecordingClientFactory(registration_success_handler)
        runtime = proxy.ProxyRuntime(config, factory)

        await runtime.start()
        self.assertTrue(runtime.started)
        self.assertEqual(3, len(factory.requests))
        self.assertEqual(
            {"/register"}, {request.url.path for request in factory.requests}
        )

        await runtime.close()
        self.assertTrue(all(client.is_closed for _, _, client in factory.clients))
        self.assertNotIn(
            "/unregister", [request.url.path for request in factory.requests]
        )

    async def test_registration_failure_aborts_startup_and_closes_clients(self) -> None:
        calls = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            if calls == 2:
                return httpx.Response(400, json={"reason": "invalid_registration"})
            return await registration_success_handler(request)

        factory = RecordingClientFactory(handler)
        runtime = proxy.ProxyRuntime(valid_config(), factory)

        with self.assertRaises(httpx.HTTPStatusError):
            await runtime.start()

        self.assertFalse(runtime.started)
        self.assertEqual(2, calls)
        self.assertTrue(all(client.is_closed for _, _, client in factory.clients))

    async def test_client_creation_failure_closes_earlier_clients(self) -> None:
        base_factory = RecordingClientFactory(registration_success_handler)
        calls = 0

        def failing_factory(
            base_url: str, timeout_seconds: float | None
        ) -> httpx.AsyncClient:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("client creation failed")
            return base_factory(base_url, timeout_seconds)

        runtime = proxy.ProxyRuntime(valid_config(), failing_factory)

        with self.assertRaisesRegex(RuntimeError, "client creation failed"):
            await runtime.start()

        self.assertEqual(1, len(base_factory.clients))
        self.assertTrue(base_factory.clients[0][2].is_closed)

    async def test_registration_rejects_unexpected_success_body(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            payload = request_json(request)
            return httpx.Response(
                200,
                json={"status": "ok", "instance_id": payload["instance_id"]},
            )

        factory = RecordingClientFactory(handler)
        runtime = proxy.ProxyRuntime(valid_config(), factory)

        with self.assertRaises(proxy.ConductorProtocolError):
            await runtime.start()
        self.assertTrue(all(client.is_closed for _, _, client in factory.clients))

    async def test_query_payload_forwards_optional_cache_salt_and_timeout(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"instances": {}})

        config = valid_config()
        factory = RecordingClientFactory(handler)
        client = factory(
            config.conductor.address, config.conductor.query_timeout_seconds
        )
        conductor = proxy.ConductorClient(
            config.conductor, config.prefill.config, client
        )
        self.addAsyncCleanup(client.aclose)

        await conductor.query(
            {"model": "test-model", "prompt": "hello", "cache_salt": "salt-a"},
            [1, 2, 3],
            "request-1",
        )
        await conductor.query(
            {"model": "test-model", "prompt": "hello"},
            [4, 5],
            "request-2",
        )

        salted = request_json(factory.requests[0])
        unsalted = request_json(factory.requests[1])
        self.assertEqual(
            {
                "model": "test-model",
                "block_size": 16,
                "token_ids": [1, 2, 3],
                "tenant_id": "tenant-a",
                "lora_name": "adapter-a",
                "cache_salt": "salt-a",
            },
            salted,
        )
        self.assertNotIn("cache_salt", unsalted)
        self.assertEqual("request-1", factory.requests[0].headers["x-request-id"])
        timeout = factory.requests[0].extensions["timeout"]
        self.assertTrue(all(value == 0.25 for value in timeout.values()))

    async def test_query_rejects_invalid_json_success(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b"not-json")

        config = valid_config()
        factory = RecordingClientFactory(handler)
        client = factory(
            config.conductor.address, config.conductor.query_timeout_seconds
        )
        conductor = proxy.ConductorClient(
            config.conductor, config.prefill.config, client
        )
        self.addAsyncCleanup(client.aclose)

        with self.assertRaises(proxy.ConductorProtocolError):
            await conductor.query({"model": "test-model"}, [1], "request-1")


if __name__ == "__main__":
    unittest.main()
