from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

import httpx

from _support import RecordingClientFactory, proxy, request_json, valid_config


class EndpointTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        asyncio.get_running_loop().slow_callback_duration = 1.0

    async def test_completion_preserves_prefill_decode_contract(self) -> None:
        outbound: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            outbound.append(request)
            path = request.url.path
            if request.url.host == "conductor.test" and path == "/register":
                payload = request_json(request)
                return httpx.Response(
                    200,
                    json={
                        "status": "registered successfully",
                        "instance_id": payload["instance_id"],
                    },
                )
            if request.url.host == "conductor.test" and path == "/query":
                return httpx.Response(
                    200,
                    json={
                        "instances": {
                            "prefill-a": {"longest_matched": 16},
                            "prefill-b": {"longest_matched": 32},
                        }
                    },
                )
            if path == "/tokenize":
                return httpx.Response(200, json={"tokens": [1, 2, 3]})
            if request.url.host == "prefill-b.test" and path == "/v1/completions":
                return httpx.Response(
                    200,
                    json={
                        "kv_transfer_params": {
                            "remote_engine_id": "engine-b",
                            "remote_block_ids": [7, 8],
                        }
                    },
                )
            if request.url.host == "decode-a.test" and path == "/v1/completions":
                return httpx.Response(200, content=b"decode-completion")
            raise AssertionError(f"unexpected request: {request.method} {request.url}")

        factory = RecordingClientFactory(handler)
        app = proxy.create_app(valid_config(), factory)
        request_body = {
            "model": "test-model",
            "prompt": "hello",
            "max_tokens": 9,
            "stream": True,
            "stream_options": {"include_usage": True},
            "cache_salt": "salt-a",
        }

        with (
            patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"}),
            patch.object(proxy.uuid, "uuid4", return_value="fixed-request-id"),
        ):
            async with app.router.lifespan_context(app):
                transport = httpx.ASGITransport(app=app)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://proxy.test"
                ) as client:
                    health = await client.get("/healthcheck")
                    response = await client.post("/v1/completions", json=request_body)

        self.assertEqual(
            {"status": "ok", "prefill_instances": 2, "decode_instances": 2},
            health.json(),
        )
        self.assertEqual(b"decode-completion", response.content)
        self.assertTrue(response.headers["content-type"].startswith("application/json"))

        tokenize = next(
            request for request in outbound if request.url.path == "/tokenize"
        )
        query = next(request for request in outbound if request.url.path == "/query")
        prefill = next(
            request
            for request in outbound
            if request.url.host == "prefill-b.test"
            and request.url.path == "/v1/completions"
        )
        decode = next(
            request
            for request in outbound
            if request.url.host == "decode-a.test"
            and request.url.path == "/v1/completions"
        )

        tokenize_body = request_json(tokenize)
        self.assertFalse(tokenize_body["stream"])
        self.assertEqual(1, tokenize_body["max_tokens"])
        self.assertNotIn("stream_options", tokenize_body)

        self.assertEqual("salt-a", request_json(query)["cache_salt"])

        prefill_body = request_json(prefill)
        self.assertFalse(prefill_body["stream"])
        self.assertEqual(1, prefill_body["max_tokens"])
        self.assertNotIn("stream_options", prefill_body)
        self.assertEqual(True, prefill_body["kv_transfer_params"]["do_remote_decode"])

        decode_body = request_json(decode)
        self.assertTrue(decode_body["stream"])
        self.assertEqual(9, decode_body["max_tokens"])
        self.assertEqual({"include_usage": True}, decode_body["stream_options"])
        self.assertEqual(
            {"remote_engine_id": "engine-b", "remote_block_ids": [7, 8]},
            decode_body["kv_transfer_params"],
        )

        for request in (tokenize, query, prefill, decode):
            self.assertEqual("fixed-request-id", request.headers["x-request-id"])
        for request in (tokenize, prefill, decode):
            self.assertEqual("Bearer test-key", request.headers["authorization"])
        self.assertTrue(all(client.is_closed for _, _, client in factory.clients))
        self.assertNotIn("/unregister", [request.url.path for request in outbound])

    async def test_chat_completion_fallback_and_decode_round_robin(self) -> None:
        outbound: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            outbound.append(request)
            path = request.url.path
            if request.url.host == "conductor.test" and path == "/register":
                payload = request_json(request)
                return httpx.Response(
                    200,
                    json={
                        "status": "registered successfully",
                        "instance_id": payload["instance_id"],
                    },
                )
            if request.url.host == "conductor.test" and path == "/query":
                return httpx.Response(
                    200,
                    json={
                        "instances": {
                            "prefill-a": {"longest_matched": 0},
                            "prefill-b": {"longest_matched": 0},
                        }
                    },
                )
            if path == "/tokenize":
                return httpx.Response(200, json={"tokens": [1]})
            if (
                request.url.host.startswith("prefill-")
                and path == "/v1/chat/completions"
            ):
                return httpx.Response(
                    200,
                    json={"kv_transfer_params": {"remote_engine_id": request.url.host}},
                )
            if (
                request.url.host.startswith("decode-")
                and path == "/v1/chat/completions"
            ):
                return httpx.Response(200, content=request.url.host.encode())
            raise AssertionError(f"unexpected request: {request.method} {request.url}")

        factory = RecordingClientFactory(handler)
        app = proxy.create_app(valid_config(), factory)
        body = {
            "model": "test-model",
            "messages": [{"role": "user", "content": "hello"}],
            "max_completion_tokens": 12,
            "stream": True,
            "stream_options": {"include_usage": True},
        }

        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://proxy.test"
            ) as client:
                first = await client.post("/v1/chat/completions", json=body)
                second = await client.post("/v1/chat/completions", json=body)

        self.assertEqual(b"decode-a.test", first.content)
        self.assertEqual(b"decode-b.test", second.content)
        prefills = [
            request
            for request in outbound
            if request.url.path == "/v1/chat/completions"
            and request.url.host.startswith("prefill-")
        ]
        self.assertEqual(
            ["prefill-a.test", "prefill-b.test"], [r.url.host for r in prefills]
        )
        for request in prefills:
            payload = request_json(request)
            self.assertEqual(1, payload["max_completion_tokens"])
            self.assertNotIn("stream_options", payload)

        decodes = [
            request
            for request in outbound
            if request.url.path == "/v1/chat/completions"
            and request.url.host.startswith("decode-")
        ]
        for request in decodes:
            payload = request_json(request)
            self.assertEqual(12, payload["max_completion_tokens"])
            self.assertEqual({"include_usage": True}, payload["stream_options"])


if __name__ == "__main__":
    unittest.main()
