#!/usr/bin/env python3
"""Unit tests for the metadata query path of mc_meta_cli."""

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, HTTPServer
import os
from pathlib import Path
import sys
import threading
import unittest
from unittest.mock import patch
import urllib.parse

sys.path.insert(0, str(Path(__file__).resolve().parent))
import mc_meta_cli as cli  # noqa: E402

TOKEN = "s3cret-token"


class _MetadataHandler(BaseHTTPRequestHandler):
    """Mimics the GET path of mooncake.http_metadata_server."""

    def do_GET(self) -> None:
        token = self.server.auth_token
        if token is not None:
            if self.headers.get("Authorization") != f"Bearer {token}":
                self.send_response(401)
                self.end_headers()
                return
        query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        key = query.get("key", [""])[0]
        self.send_response(200 if key in self.server.keys else 404)
        self.end_headers()

    def log_message(self, *args) -> None:
        pass


class QueryKeyByHttpTest(unittest.TestCase):
    def _start_server(self, auth_token: str | None) -> str:
        server = HTTPServer(("127.0.0.1", 0), _MetadataHandler)
        server.auth_token = auth_token
        server.keys = {"present"}
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_port}/metadata"

    def test_token_from_env_is_sent(self) -> None:
        url = self._start_server(TOKEN)
        with patch.dict(os.environ, {"MC_METADATA_HTTP_TOKEN": TOKEN}):
            self.assertTrue(cli.query_key_by_http(url, "present", 5.0))
            self.assertFalse(cli.query_key_by_http(url, "absent", 5.0))

    def test_missing_token_reports_auth_hint(self) -> None:
        url = self._start_server(TOKEN)
        with patch.dict(os.environ, {"MC_METADATA_HTTP_TOKEN": ""}):
            with self.assertRaises(RuntimeError) as ctx:
                cli.query_key_by_http(url, "present", 5.0)
        self.assertIn("HTTP error 401", str(ctx.exception))
        self.assertIn("MC_METADATA_HTTP_TOKEN", str(ctx.exception))

    def test_wrong_token_is_still_rejected(self) -> None:
        url = self._start_server(TOKEN)
        with patch.dict(os.environ, {"MC_METADATA_HTTP_TOKEN": "wrong"}):
            with self.assertRaises(RuntimeError) as ctx:
                cli.query_key_by_http(url, "present", 5.0)
        self.assertIn("HTTP error 401", str(ctx.exception))

    def test_open_server_works_without_token(self) -> None:
        url = self._start_server(None)
        with patch.dict(os.environ, {"MC_METADATA_HTTP_TOKEN": ""}):
            self.assertTrue(cli.query_key_by_http(url, "present", 5.0))
            self.assertFalse(cli.query_key_by_http(url, "absent", 5.0))


if __name__ == "__main__":
    unittest.main()
