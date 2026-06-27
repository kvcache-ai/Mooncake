"""Unit tests for the Python HTTP metadata server.

These exercise KVBootstrapServer._handle_put directly and need neither the
compiled engine nor a running server, so they import the pure-Python module
only.
"""

import unittest
from unittest.mock import AsyncMock

from mooncake.http_metadata_server import KVBootstrapServer

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
