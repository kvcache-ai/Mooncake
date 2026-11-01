#!/usr/bin/env python3
import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

from aiohttp.test_utils import TestClient, TestServer
from mooncake.http_metadata_server import KVBootstrapServer


class FakeRequest:
    def __init__(self, method, key=None, body=b"", headers=None):
        self.method = method
        self.query = {} if key is None else {"key": key}
        self.body = body
        self.content_length = len(body)
        self.headers = {} if headers is None else headers

    async def read(self):
        return self.body


class HttpMetadataServerTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = KVBootstrapServer(port=0)

    async def test_missing_metadata_key_is_rejected_for_all_methods(self):
        for method in ("GET", "PUT", "DELETE"):
            with self.subTest(method=method):
                response = await self.server._handle_metadata(
                    FakeRequest(method, body=b"value")
                )

                self.assertEqual(response.status, 400)
                self.assertEqual(response.content_type, "application/json")
                self.assertNotIn("", self.server.store)

    async def test_empty_metadata_key_is_rejected(self):
        response = await self.server._handle_metadata(
            FakeRequest("PUT", key="", body=b"value")
        )

        self.assertEqual(response.status, 400)
        self.assertEqual(response.content_type, "application/json")
        self.assertNotIn("", self.server.store)

    async def test_blank_metadata_key_is_rejected(self):
        response = await self.server._handle_metadata(
            FakeRequest("PUT", key="   ", body=b"value")
        )

        self.assertEqual(response.status, 400)
        self.assertEqual(response.content_type, "application/json")
        self.assertNotIn("   ", self.server.store)

    async def test_metadata_key_is_stripped_before_operations(self):
        put_response = await self.server._handle_metadata(
            FakeRequest("PUT", key="  valid  ", body=b"value")
        )
        get_response = await self.server._handle_metadata(
            FakeRequest("GET", key="  valid  ")
        )

        self.assertEqual(put_response.status, 200)
        self.assertEqual(get_response.status, 200)
        self.assertEqual(get_response.body, b"value")
        self.assertIn("valid", self.server.store)
        self.assertNotIn("  valid  ", self.server.store)

    async def test_valid_metadata_key_still_round_trips(self):
        put_response = await self.server._handle_metadata(
            FakeRequest("PUT", key="valid", body=b"value")
        )
        get_response = await self.server._handle_metadata(
            FakeRequest("GET", key="valid")
        )

        self.assertEqual(put_response.status, 200)
        self.assertEqual(get_response.status, 200)
        self.assertEqual(get_response.body, b"value")


RPC_META_KEY = "mooncake/rpc_meta/10.0.0.1:12384"


def _request(data: bytes) -> AsyncMock:
    request = AsyncMock()
    request.read = AsyncMock(return_value=data)
    return request


class TestHttpMetadataServerPut(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.server = KVBootstrapServer(port=0)

    async def test_first_put_stores_value(self) -> None:
        resp = await self.server._handle_put(RPC_META_KEY, _request(b'{"rpc_port": 1}'))
        self.assertEqual(resp.status, 200)

    async def test_republish_same_rpc_meta_is_idempotent(self) -> None:
        payload = b'{"rpc_port": 1}'
        await self.server._handle_put(RPC_META_KEY, _request(payload))
        resp = await self.server._handle_put(RPC_META_KEY, _request(payload))
        # Same value must be accepted, not rejected as a duplicate.
        self.assertEqual(resp.status, 200)

    async def test_conflicting_rpc_meta_is_rejected(self) -> None:
        await self.server._handle_put(RPC_META_KEY, _request(b'{"rpc_port": 1}'))
        resp = await self.server._handle_put(RPC_META_KEY, _request(b'{"rpc_port": 2}'))
        self.assertEqual(resp.status, 400)

    async def test_non_rpc_meta_key_can_be_overwritten(self) -> None:
        key = "mooncake/segment/abc"
        await self.server._handle_put(key, _request(b"v1"))
        resp = await self.server._handle_put(key, _request(b"v2"))
        self.assertEqual(resp.status, 200)


TOKEN = "s3cret-token"
GOOD = {"Authorization": f"Bearer {TOKEN}"}


class TestHttpMetadataServerAuth(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.env = patch.dict(os.environ, {"MC_METADATA_HTTP_TOKEN": TOKEN})
        self.env.start()
        self.server = KVBootstrapServer(port=0)

    async def asyncTearDown(self) -> None:
        self.env.stop()

    async def test_missing_token_is_rejected(self) -> None:
        resp = await self.server._handle_metadata(
            FakeRequest("PUT", key="k", body=b"v")
        )
        self.assertEqual(resp.status, 401)
        self.assertNotIn("k", self.server.store)

    async def test_wrong_token_is_rejected(self) -> None:
        resp = await self.server._handle_metadata(
            FakeRequest("GET", key="k", headers={"Authorization": "Bearer nope"})
        )
        self.assertEqual(resp.status, 401)

    async def test_auth_runs_before_key_validation(self) -> None:
        resp = await self.server._handle_metadata(FakeRequest("PUT", body=b"v"))
        self.assertEqual(resp.status, 401)

    async def test_correct_token_round_trips(self) -> None:
        put = await self.server._handle_metadata(
            FakeRequest("PUT", key="k", body=b"v", headers=GOOD)
        )
        get = await self.server._handle_metadata(
            FakeRequest("GET", key="k", headers=GOOD)
        )
        delete = await self.server._handle_metadata(
            FakeRequest("DELETE", key="k", headers=GOOD)
        )
        self.assertEqual((put.status, get.status, delete.status), (200, 200, 200))
        self.assertEqual(get.body, b"v")

    async def test_rpc_meta_rules_still_apply_behind_auth(self) -> None:
        key = "mooncake/rpc_meta/10.0.0.1:12384"
        await self.server._handle_metadata(
            FakeRequest("PUT", key=key, body=b'{"rpc_port": 1}', headers=GOOD)
        )
        resp = await self.server._handle_metadata(
            FakeRequest("PUT", key=key, body=b'{"rpc_port": 2}', headers=GOOD)
        )
        self.assertEqual(resp.status, 400)


class TestHttpMetadataServerBounds(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.env = patch.dict(
            os.environ,
            {"MC_METADATA_MAX_KEYS": "2", "MC_METADATA_MAX_TOTAL_BYTES": "16"},
        )
        self.env.start()
        self.server = KVBootstrapServer(port=0)

    async def asyncTearDown(self) -> None:
        self.env.stop()

    async def test_third_distinct_key_is_rejected(self) -> None:
        await self.server._handle_put("k1", _request(b"v1"))
        await self.server._handle_put("k2", _request(b"v2"))
        resp = await self.server._handle_put("k3", _request(b"v3"))
        self.assertEqual(resp.status, 507)
        self.assertNotIn("k3", self.server.store)

    async def test_overwrite_within_key_budget_is_allowed(self) -> None:
        await self.server._handle_put("k1", _request(b"v1"))
        await self.server._handle_put("k2", _request(b"v2"))
        resp = await self.server._handle_put("k1", _request(b"v9"))
        self.assertEqual(resp.status, 200)

    async def test_size_budget_recovers_after_delete(self) -> None:
        await self.server._handle_put("k1", _request(b"0123456789"))
        resp = await self.server._handle_put("k2", _request(b"0123456789"))
        self.assertEqual(resp.status, 507)
        await self.server._handle_delete("k1")
        resp = await self.server._handle_put("k2", _request(b"0123456789"))
        self.assertEqual(resp.status, 200)

    async def test_size_budget_counts_key_bytes(self) -> None:
        await self.server._handle_put("12345678", _request(b"12345678"))
        resp = await self.server._handle_put("x", _request(b""))
        self.assertEqual(resp.status, 507)
        self.assertNotIn("x", self.server.store)

    async def test_concurrent_put_body_reads_are_bounded(self) -> None:
        with patch.dict(os.environ, {"MC_METADATA_MAX_CONCURRENT_PUTS": "1"}):
            server = KVBootstrapServer(port=0)

        first_started = asyncio.Event()
        release_first = asyncio.Event()

        class BlockingRequest:
            async def read(self):
                first_started.set()
                await release_first.wait()
                return b"v1"

        second = _request(b"v2")
        first_task = asyncio.create_task(server._handle_put("k1", BlockingRequest()))
        await first_started.wait()
        second_task = asyncio.create_task(server._handle_put("k2", second))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(second.read.await_count, 0)

        release_first.set()
        first_response, second_response = await asyncio.gather(first_task, second_task)
        self.assertEqual((first_response.status, second_response.status), (200, 200))

    async def test_body_limit_is_passed_to_aiohttp(self) -> None:
        with patch.dict(os.environ, {"MC_METADATA_MAX_BODY_BYTES": "123"}):
            server = KVBootstrapServer(port=0)
        self.assertEqual(server.max_body_bytes, 123)
        # aiohttp may or may not expose client_max_size as a public attribute
        # depending on version; when available, verify the app received it.
        app_limit = getattr(server.app, "client_max_size", None)
        if app_limit is not None:
            self.assertEqual(app_limit, 123)

    async def test_aiohttp_rejects_oversized_put_before_handler(self) -> None:
        with patch.dict(os.environ, {"MC_METADATA_MAX_BODY_BYTES": "4"}):
            server = KVBootstrapServer(port=0)

        with patch.object(
            server, "_handle_put", AsyncMock(wraps=server._handle_put)
        ) as handle_put:
            async with TestClient(TestServer(server.app)) as client:
                response = await client.put("/metadata?key=too-big", data=b"12345")

            self.assertEqual(response.status, 413)
            handle_put.assert_not_awaited()
        self.assertEqual(server.store, {})

    async def test_invalid_budget_env_is_rejected_at_startup(self) -> None:
        names = (
            "MC_METADATA_MAX_BODY_BYTES",
            "MC_METADATA_MAX_CONCURRENT_PUTS",
            "MC_METADATA_MAX_KEYS",
            "MC_METADATA_MAX_TOTAL_BYTES",
        )
        for name in names:
            for value in ("0", "-1", "abc"):
                with self.subTest(name=name, value=value):
                    with patch.dict(os.environ, {name: value}):
                        with self.assertRaisesRegex(
                            ValueError, f"{name}.*positive integer"
                        ):
                            KVBootstrapServer(port=0)


class TestHttpMetadataServerBindGate(unittest.TestCase):
    """run() must refuse a non-loopback bind with no token unless the
    operator explicitly opts into the legacy open behavior."""

    def _start(self, host: str, env: dict) -> unittest.mock.Mock:
        with patch.dict(os.environ, env):
            server = KVBootstrapServer(port=0, host=host)
            with patch("mooncake.http_metadata_server.threading.Thread") as thread:
                server.run()
        return thread

    def test_non_loopback_without_token_refuses_to_start(self) -> None:
        env = {"MC_METADATA_HTTP_TOKEN": "", "MC_METADATA_HTTP_ALLOW_INSECURE": ""}
        with self.assertRaisesRegex(RuntimeError, "refuses to bind"):
            self._start("0.0.0.0", env)

    def test_loopback_bind_without_token_starts(self) -> None:
        env = {"MC_METADATA_HTTP_TOKEN": "", "MC_METADATA_HTTP_ALLOW_INSECURE": ""}
        for host in ("127.0.0.1", "localhost", "::1"):
            with self.subTest(host=host):
                thread = self._start(host, env)
                thread.return_value.start.assert_called_once()

    def test_non_loopback_with_insecure_opt_in_starts(self) -> None:
        env = {"MC_METADATA_HTTP_TOKEN": "", "MC_METADATA_HTTP_ALLOW_INSECURE": "1"}
        thread = self._start("0.0.0.0", env)
        thread.return_value.start.assert_called_once()

    def test_opt_in_value_must_be_truthy(self) -> None:
        env = {"MC_METADATA_HTTP_TOKEN": "", "MC_METADATA_HTTP_ALLOW_INSECURE": "0"}
        with self.assertRaisesRegex(RuntimeError, "refuses to bind"):
            self._start("0.0.0.0", env)

    def test_non_loopback_with_token_starts(self) -> None:
        env = {"MC_METADATA_HTTP_TOKEN": TOKEN, "MC_METADATA_HTTP_ALLOW_INSECURE": ""}
        thread = self._start("0.0.0.0", env)
        thread.return_value.start.assert_called_once()


if __name__ == "__main__":
    unittest.main()
