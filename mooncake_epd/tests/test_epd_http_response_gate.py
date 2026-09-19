import asyncio

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from mooncake_epd.scripts.epd_http_response_gate import (
    ResponseGateConfig,
    create_app,
)


def test_response_gate_holds_real_upstream_response_until_release(tmp_path):
    upstream = FastAPI()

    @upstream.post("/publish_direct")
    async def publish_direct():
        return JSONResponse(
            {"handles": [{"feature_id": "real-handle"}]},
            headers={"X-Upstream-Evidence": "real"},
        )

    marker = tmp_path / "held.json"
    release = tmp_path / "release"
    upstream_client = httpx.AsyncClient(
        base_url="http://encoder.local",
        transport=httpx.ASGITransport(app=upstream),
    )
    gate = create_app(
        ResponseGateConfig(
            upstream="http://encoder.local",
            gated_path="/publish_direct",
            marker_file=marker,
            release_file=release,
            gate_timeout_s=2.0,
            poll_interval_s=0.001,
        ),
        upstream_client=upstream_client,
    )

    async def run():
        async with httpx.AsyncClient(
            base_url="http://gate.local",
            transport=httpx.ASGITransport(app=gate),
        ) as client:
            request_task = asyncio.create_task(client.post("/publish_direct", json={}))
            for _ in range(1000):
                if marker.exists():
                    break
                await asyncio.sleep(0.001)
            assert marker.exists()
            assert request_task.done() is False
            release.touch()
            response = await request_task
            state = (await client.get("/__gate/state")).json()
        await upstream_client.aclose()
        return response, state

    response, state = asyncio.run(run())
    assert response.status_code == 200
    assert response.json() == {"handles": [{"feature_id": "real-handle"}]}
    assert response.headers["X-Upstream-Evidence"] == "real"
    assert state["state"] == "released"
    assert state["gated_responses"] == 1
    assert state["forwarded_requests"] == 1
