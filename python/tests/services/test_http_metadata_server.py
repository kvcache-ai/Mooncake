#!/usr/bin/env python3
import os
import unittest
from unittest.mock import AsyncMock, patch

from mooncake.http_metadata_server import KVBootstrapServer


class FakeRequest:
    def __init__(self, method, key=None, body=b"", headers=None):
        self.method = method
        self.query = {} if key is None else {"key": key}
        self.body = body
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

    async def test_invalid_budget_env_falls_back_to_default(self) -> None:
        with patch.dict(os.environ, {"MC_METADATA_MAX_KEYS": "abc"}):
            server = KVBootstrapServer(port=0)
        self.assertEqual(server.max_keys, 65536)


if __name__ == "__main__":
    unittest.main()
