from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from mooncake_epd.core.control.vllm_incarnation import (
    VLLM_EXPECTED_INCARNATION_HEADER,
    VLLM_INCARNATION_ENDPOINT,
    VLLM_INCARNATION_HEADER,
    VLLM_INCARNATION_MISMATCH_HEADER,
    VLLM_PROCESS_INCARNATION,
    epd_incarnation_middleware,
)


def _app() -> FastAPI:
    app = FastAPI()
    app.middleware("http")(epd_incarnation_middleware)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


def test_incarnation_endpoint_is_small_stable_and_not_cacheable():
    with TestClient(_app()) as client:
        first = client.get(VLLM_INCARNATION_ENDPOINT)
        second = client.get(f"{VLLM_INCARNATION_ENDPOINT}/")

    assert first.status_code == 200
    assert first.text == VLLM_PROCESS_INCARNATION
    assert second.text == first.text
    assert first.headers[VLLM_INCARNATION_HEADER] == first.text
    assert first.headers["cache-control"] == "no-store"
    assert len(first.content) < 128


def test_incarnation_middleware_preserves_other_vllm_routes():
    with TestClient(_app()) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_incarnation_guard_rejects_stale_epoch_before_route_dispatch():
    with TestClient(_app()) as client:
        stale = client.get(
            "/health",
            headers={VLLM_EXPECTED_INCARNATION_HEADER: "stale-token"},
        )
        current = client.get(
            "/health",
            headers={
                VLLM_EXPECTED_INCARNATION_HEADER: VLLM_PROCESS_INCARNATION
            },
        )

    assert stale.status_code == 409
    assert stale.headers[VLLM_INCARNATION_MISMATCH_HEADER] == "1"
    assert stale.headers[VLLM_INCARNATION_HEADER] == VLLM_PROCESS_INCARNATION
    assert current.status_code == 200
