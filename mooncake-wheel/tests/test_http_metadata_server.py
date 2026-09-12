#!/usr/bin/env python3
import sys
import unittest
from unittest.mock import AsyncMock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mooncake.http_metadata_server import KVBootstrapServer


class FakeRequest:
    def __init__(self, method, key=None, body=b""):
        self.method = method
        self.query = {} if key is None else {"key": key}
        self.body = body

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


if __name__ == "__main__":
    unittest.main()
