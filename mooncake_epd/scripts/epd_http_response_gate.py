#!/usr/bin/env python3
"""Transparent real-upstream response gate for lifecycle fault injection.

The gate never fabricates an upstream response.  It forwards the request,
fully receives the real response, persists a marker, and delays delivery until
the release file appears.  This makes process-incarnation races reproducible
without mocking Encoder work or the Mooncake data plane.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response


_HOP_BY_HOP_HEADERS = {
    "connection",
    "content-length",
    "content-encoding",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


@dataclass(frozen=True)
class ResponseGateConfig:
    upstream: str
    gated_path: str
    marker_file: Path
    release_file: Path
    gate_timeout_s: float = 300.0
    poll_interval_s: float = 0.01


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def create_app(
    config: ResponseGateConfig,
    *,
    upstream_client: Optional[httpx.AsyncClient] = None,
) -> FastAPI:
    owns_client = upstream_client is None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if app.state.upstream_client is None:
            app.state.upstream_client = httpx.AsyncClient(
                base_url=config.upstream.rstrip("/"),
                timeout=None,
                trust_env=False,
            )
        try:
            yield
        finally:
            if owns_client and app.state.upstream_client is not None:
                await app.state.upstream_client.aclose()

    app = FastAPI(lifespan=lifespan)
    app.state.upstream_client = upstream_client
    app.state.gate_lock = asyncio.Lock()
    app.state.gate_reserved = False
    app.state.gate_state = "idle"
    app.state.gated_responses = 0
    app.state.forwarded_requests = 0

    @app.get("/__gate/health")
    async def gate_health() -> dict:
        return {
            "status": "ok",
            "state": app.state.gate_state,
            "reserved": app.state.gate_reserved,
        }

    @app.get("/__gate/state")
    async def gate_state() -> dict:
        return {
            "state": app.state.gate_state,
            "reserved": app.state.gate_reserved,
            "gated_responses": app.state.gated_responses,
            "forwarded_requests": app.state.forwarded_requests,
            "marker_file": str(config.marker_file),
            "release_file": str(config.release_file),
        }

    @app.api_route(
        "/{path:path}",
        methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    )
    async def forward(path: str, request: Request) -> Response:
        request_path = f"/{path}"
        should_gate = False
        if request_path.rstrip("/") == config.gated_path.rstrip("/"):
            async with app.state.gate_lock:
                if not app.state.gate_reserved:
                    app.state.gate_reserved = True
                    app.state.gate_state = "reserved"
                    should_gate = True

        headers = {
            key: value
            for key, value in request.headers.items()
            if key.lower() not in _HOP_BY_HOP_HEADERS and key.lower() != "host"
        }
        try:
            upstream_response = await app.state.upstream_client.request(
                request.method,
                request_path,
                params=request.query_params,
                content=await request.body(),
                headers=headers,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            app.state.gate_state = "upstream-error"
            return JSONResponse(
                {"detail": f"upstream request failed: {type(exc).__name__}: {exc}"},
                status_code=502,
            )

        app.state.forwarded_requests += 1
        response_body = bytes(upstream_response.content)
        response_headers = {
            key: value
            for key, value in upstream_response.headers.items()
            if key.lower() not in _HOP_BY_HOP_HEADERS
        }
        if should_gate:
            app.state.gate_state = "holding"
            app.state.gated_responses += 1
            _atomic_write_json(
                config.marker_file,
                {
                    "path": request_path,
                    "status_code": upstream_response.status_code,
                    "response_bytes": len(response_body),
                    "held_at_unix_s": time.time(),
                },
            )
            deadline = time.monotonic() + max(0.01, config.gate_timeout_s)
            while not config.release_file.exists():
                if time.monotonic() >= deadline:
                    app.state.gate_state = "gate-timeout"
                    return JSONResponse(
                        {"detail": "response gate timed out before release"},
                        status_code=504,
                    )
                await asyncio.sleep(max(0.001, config.poll_interval_s))
            app.state.gate_state = "released"

        return Response(
            content=response_body,
            status_code=upstream_response.status_code,
            headers=response_headers,
            media_type=None,
        )

    return app


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18330)
    parser.add_argument("--upstream", default="http://127.0.0.1:8330")
    parser.add_argument("--gated-path", default="/publish_direct")
    parser.add_argument("--marker-file", type=Path, required=True)
    parser.add_argument("--release-file", type=Path, required=True)
    parser.add_argument("--gate-timeout-s", type=float, default=300.0)
    parser.add_argument("--poll-interval-s", type=float, default=0.01)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.port < 1 or args.port > 65535:
        raise ValueError("port must be between 1 and 65535")
    if not str(args.gated_path).startswith("/"):
        raise ValueError("gated path must be absolute")
    if args.gate_timeout_s <= 0 or args.poll_interval_s <= 0:
        raise ValueError("gate timeout and poll interval must be positive")
    app = create_app(
        ResponseGateConfig(
            upstream=str(args.upstream),
            gated_path=str(args.gated_path),
            marker_file=args.marker_file,
            release_file=args.release_file,
            gate_timeout_s=float(args.gate_timeout_s),
            poll_interval_s=float(args.poll_interval_s),
        )
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
