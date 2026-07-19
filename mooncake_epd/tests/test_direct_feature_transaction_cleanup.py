from __future__ import annotations

import asyncio

import httpx
import pytest
import torch
from fastapi import FastAPI, HTTPException

from mooncake_epd.core.state import FeatureBundle
from mooncake_epd.scripts.vllm_disagg_proxy import (
    ProxyConfig,
    _collect_direct_feature_release_ids,
    _request_direct_feature_handles_from_encoder_service,
    create_app as create_proxy_app,
)


def test_direct_feature_transaction_releases_ticket_and_allocations_after_publish_failure():
    encoder = FastAPI()
    direct = FastAPI()
    calls = {"released_ticket": [], "released_features": []}
    bundle = FeatureBundle("image-1", torch.ones((2, 3), dtype=torch.float32))

    @encoder.post("/describe")
    async def describe():
        return {"ticket": "ticket-1", "descriptors": [bundle.descriptor().to_dict()]}

    @encoder.post("/publish_direct")
    async def publish_direct():
        raise HTTPException(status_code=500, detail="synthetic publish failure")

    @encoder.post("/release_direct_ticket")
    async def release_ticket(payload: dict):
        calls["released_ticket"].append(payload["ticket"])
        return {"released": True}

    @direct.post("/mooncake_epd/direct_feature_buffer/allocate")
    async def allocate():
        return {"targets": [{"feature_id": "image-1", "publish_required": True}]}

    @direct.post("/mooncake_epd/direct_feature_buffer/release")
    async def release_features(payload: dict):
        calls["released_features"].extend(payload["feature_ids"])
        return {"released": len(payload["feature_ids"])}

    proxy = create_proxy_app(
        ProxyConfig(
            mm_prefetch_mode="feature_handle",
            prefill_supports_feature_handles=True,
            prefill_direct_buffer_service_url="http://direct.local",
        )
    )

    async def run() -> None:
        proxy.state.encoder_client = httpx.AsyncClient(
            base_url="http://encoder.local", transport=httpx.ASGITransport(app=encoder)
        )
        proxy.state.prefill_direct_buffer_client = httpx.AsyncClient(
            base_url="http://direct.local", transport=httpx.ASGITransport(app=direct)
        )
        with pytest.raises(HTTPException, match="direct FeatureHandle transaction failed"):
            await _request_direct_feature_handles_from_encoder_service(
                app=proxy,
                payload={"messages": []},
                target_worker_id="prefill-0",
            )
        await proxy.state.encoder_client.aclose()
        await proxy.state.prefill_direct_buffer_client.aclose()

    asyncio.run(run())
    assert calls["released_ticket"] == ["ticket-1"]
    assert calls["released_features"] == ["image-1"]


@pytest.mark.parametrize("failure_endpoint", ["mark_ready", "wait_ready"])
def test_direct_feature_transaction_releases_allocations_until_readiness_is_committed(
    failure_endpoint: str,
):
    encoder = FastAPI()
    direct = FastAPI()
    calls = {"released_ticket": [], "released_features": []}
    bundles = [
        FeatureBundle("image-published", torch.ones((2, 3), dtype=torch.float32)),
        FeatureBundle("image-coalesced", torch.full((2, 3), 2.0, dtype=torch.float32)),
    ]

    @encoder.post("/describe")
    async def describe():
        return {
            "ticket": "ticket-ready",
            "descriptors": [bundle.descriptor().to_dict() for bundle in bundles],
        }

    @encoder.post("/publish_direct")
    async def publish_direct():
        return {
            "handles": [
                {"feature_id": "image-published", "uri": "epd-direct://published"},
                {"feature_id": "image-coalesced", "uri": "epd-direct://coalesced"},
            ]
        }

    @encoder.post("/release_direct_ticket")
    async def release_ticket(payload: dict):
        calls["released_ticket"].append(payload["ticket"])
        return {"released": True}

    @direct.post("/mooncake_epd/direct_feature_buffer/allocate")
    async def allocate():
        return {
            "targets": [
                {"feature_id": "image-published", "publish_required": True},
                {"feature_id": "image-coalesced", "publish_required": False},
            ]
        }

    @direct.post("/mooncake_epd/direct_feature_buffer/mark_ready")
    async def mark_ready():
        if failure_endpoint == "mark_ready":
            raise HTTPException(status_code=500, detail="synthetic ready failure")
        return {"ready": 1}

    @direct.post("/mooncake_epd/direct_feature_buffer/wait_ready")
    async def wait_ready():
        if failure_endpoint == "wait_ready":
            raise HTTPException(status_code=504, detail="synthetic wait timeout")
        return {"ready": 1}

    @direct.post("/mooncake_epd/direct_feature_buffer/release")
    async def release_features(payload: dict):
        calls["released_features"].extend(payload["feature_ids"])
        return {"released": len(payload["feature_ids"])}

    proxy = create_proxy_app(
        ProxyConfig(
            mm_prefetch_mode="feature_handle",
            prefill_supports_feature_handles=True,
            prefill_direct_buffer_service_url="http://direct.local",
        )
    )

    async def run() -> None:
        proxy.state.encoder_client = httpx.AsyncClient(
            base_url="http://encoder.local", transport=httpx.ASGITransport(app=encoder)
        )
        proxy.state.prefill_direct_buffer_client = httpx.AsyncClient(
            base_url="http://direct.local", transport=httpx.ASGITransport(app=direct)
        )
        with pytest.raises(HTTPException, match="direct FeatureHandle transaction failed"):
            await _request_direct_feature_handles_from_encoder_service(
                app=proxy,
                payload={"messages": []},
                target_worker_id="prefill-0",
            )
        await proxy.state.encoder_client.aclose()
        await proxy.state.prefill_direct_buffer_client.aclose()

    asyncio.run(run())
    # /publish_direct consumes the encoder ticket. Releasing it remains
    # idempotent and the important ownership compensation is the Prefill set.
    assert calls["released_ticket"] == ["ticket-ready"]
    assert sorted(calls["released_features"]) == ["image-coalesced", "image-published"]


def test_direct_feature_transaction_overlaps_independent_ready_and_wait_rpcs():
    """Mixed cache misses/hits must not pay two serial readiness RTTs."""

    encoder = FastAPI()
    direct = FastAPI()
    bundles = [
        FeatureBundle("image-published", torch.ones((2, 3), dtype=torch.float32)),
        FeatureBundle("image-coalesced", torch.full((2, 3), 2.0, dtype=torch.float32)),
    ]
    mark_started = asyncio.Event()
    wait_started = asyncio.Event()

    @encoder.post("/describe")
    async def describe():
        return {
            "ticket": "ticket-overlap",
            "descriptors": [bundle.descriptor().to_dict() for bundle in bundles],
        }

    @encoder.post("/publish_direct")
    async def publish_direct():
        return {
            "handles": [
                {"feature_id": "image-published", "uri": "epd-direct://published"},
                {"feature_id": "image-coalesced", "uri": "epd-direct://coalesced"},
            ]
        }

    @direct.post("/mooncake_epd/direct_feature_buffer/allocate")
    async def allocate():
        return {
            "targets": [
                {"feature_id": "image-published", "publish_required": True},
                {"feature_id": "image-coalesced", "publish_required": False},
            ]
        }

    @direct.post("/mooncake_epd/direct_feature_buffer/mark_ready")
    async def mark_ready():
        mark_started.set()
        await asyncio.wait_for(wait_started.wait(), timeout=0.5)
        return {"ready": 1}

    @direct.post("/mooncake_epd/direct_feature_buffer/wait_ready")
    async def wait_ready():
        wait_started.set()
        await asyncio.wait_for(mark_started.wait(), timeout=0.5)
        return {"ready": 1}

    proxy = create_proxy_app(
        ProxyConfig(
            mm_prefetch_mode="feature_handle",
            prefill_supports_feature_handles=True,
            prefill_direct_buffer_service_url="http://direct.local",
        )
    )

    async def run() -> list[dict]:
        proxy.state.encoder_client = httpx.AsyncClient(
            base_url="http://encoder.local", transport=httpx.ASGITransport(app=encoder)
        )
        proxy.state.prefill_direct_buffer_client = httpx.AsyncClient(
            base_url="http://direct.local", transport=httpx.ASGITransport(app=direct)
        )
        try:
            return await _request_direct_feature_handles_from_encoder_service(
                app=proxy,
                payload={"messages": []},
                target_worker_id="prefill-0",
            )
        finally:
            await proxy.state.encoder_client.aclose()
            await proxy.state.prefill_direct_buffer_client.aclose()

    handles = asyncio.run(run())
    assert [item["feature_id"] for item in handles] == [
        "image-published",
        "image-coalesced",
    ]
    assert mark_started.is_set()
    assert wait_started.is_set()


def test_direct_feature_transaction_counts_shared_multimodal_batch_once():
    """Per-handle batch timing must not inflate request-level TTFT telemetry."""

    encoder = FastAPI()
    direct = FastAPI()
    bundles = [
        FeatureBundle("image-first", torch.ones((2, 3), dtype=torch.float32)),
        FeatureBundle("image-second", torch.full((2, 3), 2.0, dtype=torch.float32)),
    ]

    @encoder.post("/describe")
    async def describe():
        return {
            "ticket": "ticket-batch-telemetry",
            "descriptors": [bundle.descriptor().to_dict() for bundle in bundles],
        }

    @encoder.post("/publish_direct")
    async def publish_direct():
        handles = []
        for index, bundle in enumerate(bundles):
            handles.append(
                {
                    "feature_id": bundle.image_hash,
                    "uri": f"epd-direct://prefill/{bundle.image_hash}",
                    "metadata": {
                        "direct_transfer_timings_ms": {
                            "total_ms": 12.5,
                            "write_ms": 4.0,
                            "nbytes": 600.0,
                            "batch_feature_count": 2.0,
                            "batch_index": float(index),
                        }
                    },
                }
            )
        return {"handles": handles}

    @direct.post("/mooncake_epd/direct_feature_buffer/allocate")
    async def allocate():
        return {
            "targets": [
                {"feature_id": "image-first", "publish_required": True},
                {"feature_id": "image-second", "publish_required": True},
            ]
        }

    @direct.post("/mooncake_epd/direct_feature_buffer/mark_ready")
    async def mark_ready():
        return {"ready": 2}

    proxy = create_proxy_app(
        ProxyConfig(
            mm_prefetch_mode="feature_handle",
            prefill_supports_feature_handles=True,
            prefill_direct_buffer_service_url="http://direct.local",
        )
    )

    async def run() -> list[dict]:
        proxy.state.encoder_client = httpx.AsyncClient(
            base_url="http://encoder.local", transport=httpx.ASGITransport(app=encoder)
        )
        proxy.state.prefill_direct_buffer_client = httpx.AsyncClient(
            base_url="http://direct.local", transport=httpx.ASGITransport(app=direct)
        )
        try:
            return await _request_direct_feature_handles_from_encoder_service(
                app=proxy,
                payload={"messages": []},
                target_worker_id="prefill-0",
            )
        finally:
            await proxy.state.encoder_client.aclose()
            await proxy.state.prefill_direct_buffer_client.aclose()

    handles = asyncio.run(run())
    timings = handles[0]["metadata"]["proxy_direct_timings_ms"]
    assert timings["direct_publish_engine_total_ms"] == 12.5
    assert timings["direct_publish_engine_write_ms"] == 4.0
    assert timings["direct_publish_engine_nbytes"] == 600.0
    assert timings["direct_publish_engine_batch_feature_count"] == 2.0
    assert handles[1]["metadata"]["proxy_direct_timings_ms"] == timings


def test_direct_feature_transaction_releases_allocations_after_handle_validation_failure():
    encoder = FastAPI()
    direct = FastAPI()
    calls = {"released_ticket": [], "released_features": []}
    bundle = FeatureBundle("image-invalid", torch.ones((2, 3), dtype=torch.float32))

    @encoder.post("/describe")
    async def describe():
        return {"ticket": "ticket-invalid", "descriptors": [bundle.descriptor().to_dict()]}

    @encoder.post("/publish_direct")
    async def publish_direct():
        return {"handles": [{"feature_id": "image-invalid", "uri": "file:///not-direct"}]}

    @encoder.post("/release_direct_ticket")
    async def release_ticket(payload: dict):
        calls["released_ticket"].append(payload["ticket"])
        return {"released": True}

    @direct.post("/mooncake_epd/direct_feature_buffer/allocate")
    async def allocate():
        return {"targets": [{"feature_id": "image-invalid", "publish_required": True}]}

    @direct.post("/mooncake_epd/direct_feature_buffer/mark_ready")
    async def mark_ready():
        return {"ready": 1}

    @direct.post("/mooncake_epd/direct_feature_buffer/release")
    async def release_features(payload: dict):
        calls["released_features"].extend(payload["feature_ids"])
        return {"released": len(payload["feature_ids"])}

    proxy = create_proxy_app(
        ProxyConfig(
            mm_prefetch_mode="feature_handle",
            prefill_supports_feature_handles=True,
            prefill_direct_buffer_service_url="http://direct.local",
        )
    )

    async def run() -> None:
        proxy.state.encoder_client = httpx.AsyncClient(
            base_url="http://encoder.local", transport=httpx.ASGITransport(app=encoder)
        )
        proxy.state.prefill_direct_buffer_client = httpx.AsyncClient(
            base_url="http://direct.local", transport=httpx.ASGITransport(app=direct)
        )
        with pytest.raises(HTTPException, match="non-direct handle"):
            await _request_direct_feature_handles_from_encoder_service(
                app=proxy,
                payload={"messages": []},
                target_worker_id="prefill-0",
            )
        await proxy.state.encoder_client.aclose()
        await proxy.state.prefill_direct_buffer_client.aclose()

    asyncio.run(run())
    assert calls["released_ticket"] == ["ticket-invalid"]
    assert calls["released_features"] == ["image-invalid"]


def test_direct_feature_transaction_releases_each_duplicate_allocation_reference():
    encoder = FastAPI()
    direct = FastAPI()
    calls = {"released_ticket": [], "released_features": []}
    bundles = [
        FeatureBundle("image-shared", torch.ones((2, 3), dtype=torch.float32)),
        FeatureBundle("image-shared", torch.full((2, 3), 2.0, dtype=torch.float32)),
    ]

    @encoder.post("/describe")
    async def describe():
        return {
            "ticket": "ticket-duplicate",
            "descriptors": [bundle.descriptor().to_dict() for bundle in bundles],
        }

    @encoder.post("/publish_direct")
    async def publish_direct():
        raise HTTPException(status_code=500, detail="synthetic publish failure")

    @encoder.post("/release_direct_ticket")
    async def release_ticket(payload: dict):
        calls["released_ticket"].append(payload["ticket"])
        return {"released": True}

    @direct.post("/mooncake_epd/direct_feature_buffer/allocate")
    async def allocate():
        return {
            "targets": [
                {"feature_id": "image-shared", "publish_required": True},
                {"feature_id": "image-shared", "publish_required": False},
            ]
        }

    @direct.post("/mooncake_epd/direct_feature_buffer/release")
    async def release_features(payload: dict):
        calls["released_features"].extend(payload["feature_ids"])
        return {"released": len(payload["feature_ids"])}

    proxy = create_proxy_app(
        ProxyConfig(
            mm_prefetch_mode="feature_handle",
            prefill_supports_feature_handles=True,
            prefill_direct_buffer_service_url="http://direct.local",
        )
    )

    async def run() -> None:
        proxy.state.encoder_client = httpx.AsyncClient(
            base_url="http://encoder.local", transport=httpx.ASGITransport(app=encoder)
        )
        proxy.state.prefill_direct_buffer_client = httpx.AsyncClient(
            base_url="http://direct.local", transport=httpx.ASGITransport(app=direct)
        )
        with pytest.raises(HTTPException, match="direct FeatureHandle transaction failed"):
            await _request_direct_feature_handles_from_encoder_service(
                app=proxy,
                payload={"messages": []},
                target_worker_id="prefill-0",
            )
        await proxy.state.encoder_client.aclose()
        await proxy.state.prefill_direct_buffer_client.aclose()

    asyncio.run(run())
    assert calls["released_ticket"] == ["ticket-duplicate"]
    assert calls["released_features"] == ["image-shared", "image-shared"]


def test_post_prefill_release_preserves_duplicate_handle_references():
    req_data = {
        "kv_transfer_params": {
            "mm_feature_handle_target_worker": "prefill-0",
            "mm_feature_handles": [
                {"feature_id": "image-shared", "uri": "epd-direct://direct/image-shared"},
                {"feature_id": "image-shared", "uri": "epd-direct://direct/image-shared"},
            ],
        }
    }

    assert _collect_direct_feature_release_ids(req_data) == {
        "prefill-0": ["image-shared", "image-shared"]
    }


@pytest.mark.parametrize(
    ("published_handles", "error_fragment"),
    [
        ([{"feature_id": "image-a", "uri": "epd-direct://direct/image-a"}], "handle count mismatch"),
        (
            [
                {"feature_id": "image-b", "uri": "epd-direct://direct/image-b"},
                {"feature_id": "image-a", "uri": "epd-direct://direct/image-a"},
            ],
            "feature id mismatch",
        ),
    ],
)
def test_direct_feature_transaction_releases_allocation_on_handle_target_mismatch(
    published_handles: list[dict],
    error_fragment: str,
):
    encoder = FastAPI()
    direct = FastAPI()
    calls = {"released_ticket": [], "released_features": []}
    bundles = [
        FeatureBundle("image-a", torch.ones((2, 3), dtype=torch.float32)),
        FeatureBundle("image-b", torch.full((2, 3), 2.0, dtype=torch.float32)),
    ]

    @encoder.post("/describe")
    async def describe():
        return {
            "ticket": "ticket-mismatch",
            "descriptors": [bundle.descriptor().to_dict() for bundle in bundles],
        }

    @encoder.post("/publish_direct")
    async def publish_direct():
        return {"handles": published_handles}

    @encoder.post("/release_direct_ticket")
    async def release_ticket(payload: dict):
        calls["released_ticket"].append(payload["ticket"])
        return {"released": True}

    @direct.post("/mooncake_epd/direct_feature_buffer/allocate")
    async def allocate():
        return {
            "targets": [
                {"feature_id": "image-a", "publish_required": True},
                {"feature_id": "image-b", "publish_required": True},
            ]
        }

    @direct.post("/mooncake_epd/direct_feature_buffer/release")
    async def release_features(payload: dict):
        calls["released_features"].extend(payload["feature_ids"])
        return {"released": len(payload["feature_ids"])}

    proxy = create_proxy_app(
        ProxyConfig(
            mm_prefetch_mode="feature_handle",
            prefill_supports_feature_handles=True,
            prefill_direct_buffer_service_url="http://direct.local",
        )
    )

    async def run() -> None:
        proxy.state.encoder_client = httpx.AsyncClient(
            base_url="http://encoder.local", transport=httpx.ASGITransport(app=encoder)
        )
        proxy.state.prefill_direct_buffer_client = httpx.AsyncClient(
            base_url="http://direct.local", transport=httpx.ASGITransport(app=direct)
        )
        with pytest.raises(HTTPException, match=error_fragment):
            await _request_direct_feature_handles_from_encoder_service(
                app=proxy,
                payload={"messages": []},
                target_worker_id="prefill-0",
            )
        await proxy.state.encoder_client.aclose()
        await proxy.state.prefill_direct_buffer_client.aclose()

    asyncio.run(run())
    assert calls["released_ticket"] == ["ticket-mismatch"]
    assert sorted(calls["released_features"]) == ["image-a", "image-b"]
