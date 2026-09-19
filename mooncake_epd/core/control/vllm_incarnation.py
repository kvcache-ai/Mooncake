"""Lightweight vLLM process-incarnation endpoint for Decode cache fencing.

The Decode-side multimodal receiver cache lives for the lifetime of the vLLM
API-server/engine deployment.  Pulling the complete Prometheus payload before
every hash-only request is correct but unnecessarily expensive.  vLLM's
official ``--middleware`` extension point lets us expose the same opaque
process-lifetime evidence as a tiny, dependency-free response without patching
vLLM itself.
"""

from __future__ import annotations

import os
import hmac
import time
import uuid
from typing import Awaitable, Callable

from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response


VLLM_INCARNATION_ENDPOINT = "/mooncake_epd/incarnation"
VLLM_INCARNATION_HEADER = "X-Mooncake-EPD-Incarnation"
VLLM_EXPECTED_INCARNATION_HEADER = "X-Mooncake-EPD-Expected-Incarnation"
VLLM_INCARNATION_MISMATCH_HEADER = "X-Mooncake-EPD-Incarnation-Mismatch"
VLLM_INCARNATION_MIDDLEWARE = (
    "mooncake_epd.core.control.vllm_incarnation.epd_incarnation_middleware"
)

# Materialized once when the vLLM API process imports this middleware.  The
# random suffix prevents false equality even when a supervisor restarts a
# process within the same clock tick or reuses a PID.
VLLM_PROCESS_INCARNATION = (
    f"{os.getpid()}-{time.time_ns()}-{uuid.uuid4().hex}"
)


async def epd_incarnation_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Return an opaque process-lifetime token on the dedicated probe path."""

    if request.url.path.rstrip("/") == VLLM_INCARNATION_ENDPOINT:
        if request.method not in {"GET", "HEAD"}:
            return PlainTextResponse(
                "method not allowed",
                status_code=405,
                headers={"Allow": "GET, HEAD"},
            )
        return PlainTextResponse(
            "" if request.method == "HEAD" else VLLM_PROCESS_INCARNATION,
            headers={
                VLLM_INCARNATION_HEADER: VLLM_PROCESS_INCARNATION,
                "Cache-Control": "no-store",
            },
        )
    expected_incarnation = request.headers.get(
        VLLM_EXPECTED_INCARNATION_HEADER
    )
    if expected_incarnation and not hmac.compare_digest(
        expected_incarnation,
        VLLM_PROCESS_INCARNATION,
    ):
        return PlainTextResponse(
            "decode process incarnation changed",
            status_code=409,
            headers={
                VLLM_INCARNATION_HEADER: VLLM_PROCESS_INCARNATION,
                VLLM_INCARNATION_MISMATCH_HEADER: "1",
                "Cache-Control": "no-store",
            },
        )
    return await call_next(request)


__all__ = [
    "VLLM_INCARNATION_ENDPOINT",
    "VLLM_INCARNATION_HEADER",
    "VLLM_EXPECTED_INCARNATION_HEADER",
    "VLLM_INCARNATION_MISMATCH_HEADER",
    "VLLM_INCARNATION_MIDDLEWARE",
    "VLLM_PROCESS_INCARNATION",
    "epd_incarnation_middleware",
]
