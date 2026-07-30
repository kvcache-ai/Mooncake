#!/usr/bin/env python3
import sys
import unittest
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


if __name__ == "__main__":
    unittest.main()
