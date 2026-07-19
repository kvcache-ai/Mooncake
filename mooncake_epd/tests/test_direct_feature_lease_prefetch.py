from __future__ import annotations

import asyncio

import httpx
import torch
from fastapi import FastAPI, Request

from mooncake_epd.core.state import FeatureBundle
from mooncake_epd.scripts import vllm_disagg_proxy as proxy_mod


def _direct_lookup_payload(
    bundle: FeatureBundle,
    *,
    lease_count: int,
    server_lookup_ms: float | None = None,
) -> dict:
    nbytes = int(bundle.last_hidden.nelement() * bundle.last_hidden.element_size())
    payload = {
        "hits": [
            {
                "feature_id": bundle.image_hash,
                "descriptor": bundle.descriptor().to_dict(),
                "target": {
                    "store_id": "direct",
                    "remote_session": "prefill-session",
                    "remote_pointers": {
                        "last_hidden": 0x1234,
                        "last_hidden:nbytes": nbytes,
                    },
                    "lease_count": lease_count,
                },
            }
        ],
        "misses": [],
        "count": 1,
        "all_hit": True,
        "worker_id": "prefill-0",
        "lease_count": lease_count,
    }
    if server_lookup_ms is not None:
        payload["server_lookup_ms"] = server_lookup_ms
    return payload


def test_generation_fenced_lease_prefetch_reuses_one_lookup_and_flushes_spares():
    bundle = FeatureBundle(
        image_hash="lease-image",
        last_hidden=torch.ones((2, 3), dtype=torch.float32),
    )
    direct = FastAPI()
    calls = {"lookup": 0, "leases": [], "released": []}
    refs = {"value": 0}

    @direct.post("/mooncake_epd/direct_feature_buffer/lookup")
    async def lookup(request: Request):
        payload = await request.json()
        lease_count = int(payload["lease_count"])
        calls["lookup"] += 1
        calls["leases"].append(lease_count)
        refs["value"] += lease_count
        return _direct_lookup_payload(
            bundle,
            lease_count=lease_count,
            server_lookup_ms=1.25,
        )

    @direct.post("/mooncake_epd/direct_feature_buffer/release")
    async def release(request: Request):
        payload = await request.json()
        feature_ids = list(payload["feature_ids"])
        calls["released"].extend(feature_ids)
        refs["value"] -= len(feature_ids)
        return {"released": len(feature_ids)}

    proxy = proxy_mod.create_app(
        proxy_mod.ProxyConfig(
            prefill_direct_buffer_service_url="http://direct.local",
            release_direct_feature_buffers_after_prefill=True,
            direct_feature_lease_prefetch=4,
            direct_feature_lease_prefetch_max_entries=8,
            direct_feature_lease_prefetch_ttl_s=30.0,
            worker_generations=[("prefill-0", "generation-a")],
        )
    )

    async def run() -> None:
        client = httpx.AsyncClient(
            base_url="http://direct.local",
            transport=httpx.ASGITransport(app=direct),
        )
        proxy.state.prefill_direct_buffer_client = client
        try:
            first = await proxy_mod._lookup_prefill_cached_direct_feature_handles(
                app=proxy,
                feature_ids=[bundle.image_hash],
                target_worker_id="prefill-0",
            )
            assert first and first[0]["metadata"]["proxy_direct_timings_ms"]["direct_cache_lookup_ms"] >= 0.0
            assert (
                first[0]["metadata"]["proxy_direct_timings_ms"]
                ["direct_cache_server_lookup_ms"]
                == 1.25
            )
            # Simulate the normal post-Prefill cleanup for the first request.
            assert await proxy_mod._release_direct_feature_buffer_ids(
                proxy, {"prefill-0": [bundle.image_hash]}
            ) == 0

            second = await proxy_mod._lookup_prefill_cached_direct_feature_handles(
                app=proxy,
                feature_ids=[bundle.image_hash],
                target_worker_id="prefill-0",
            )
            assert second
            assert second[0]["metadata"]["direct_backend"] == "prefill_persistent_cache_lease_prefetch"
            assert second[0]["metadata"]["proxy_direct_timings_ms"]["direct_cache_lookup_ms"] == 0.0
            assert await proxy_mod._release_direct_feature_buffer_ids(
                proxy, {"prefill-0": [bundle.image_hash]}
            ) == 0

            released, failures = await proxy_mod._flush_direct_feature_lease_prefetch_pool(
                proxy,
                reason="test",
                wait_for_release=True,
            )
            assert released == 2
            assert failures == 0
        finally:
            await client.aclose()

    asyncio.run(run())
    assert calls["lookup"] == 1
    assert calls["leases"] == [4]
    # Two request-owned references plus two returned by terminal flush.
    assert calls["released"] == [bundle.image_hash] * 4
    assert refs["value"] == 0
    stats = proxy.state.direct_feature_lease_prefetch_stats
    assert stats["pool_hits"] == 1
    assert stats["reservations"] == 1
    assert stats["held_references"] == 0


def test_adaptive_lease_prefetch_coalesces_only_concurrent_waiters():
    """A fixed-one deployment must not retain speculative serial leases."""

    bundle = FeatureBundle(
        image_hash="adaptive-image",
        last_hidden=torch.ones((2, 3), dtype=torch.float32),
    )
    direct = FastAPI()
    calls = {"leases": [], "released": []}
    refs = {"value": 0}

    @direct.post("/mooncake_epd/direct_feature_buffer/lookup")
    async def lookup(request: Request):
        payload = await request.json()
        lease_count = int(payload["lease_count"])
        calls["leases"].append(lease_count)
        refs["value"] += lease_count
        return _direct_lookup_payload(bundle, lease_count=lease_count)

    @direct.post("/mooncake_epd/direct_feature_buffer/release")
    async def release(request: Request):
        feature_ids = list((await request.json())["feature_ids"])
        calls["released"].extend(feature_ids)
        refs["value"] -= len(feature_ids)
        return {"released": len(feature_ids)}

    proxy = proxy_mod.create_app(
        proxy_mod.ProxyConfig(
            prefill_direct_buffer_service_url="http://direct.local",
            release_direct_feature_buffers_after_prefill=True,
            direct_feature_lease_prefetch=1,
            direct_feature_adaptive_lease_prefetch=True,
            direct_feature_adaptive_lease_prefetch_max=8,
            direct_feature_lease_prefetch_max_entries=8,
            direct_feature_lease_prefetch_ttl_s=30.0,
            worker_generations=[("prefill-0", "generation-a")],
        )
    )

    async def lookup_once() -> list[dict]:
        return await proxy_mod._lookup_prefill_cached_direct_feature_handles(
            app=proxy,
            feature_ids=[bundle.image_hash],
            target_worker_id="prefill-0",
        )

    async def release_one() -> None:
        assert await proxy_mod._release_direct_feature_buffer_ids(
            proxy, {"prefill-0": [bundle.image_hash]}
        ) == 0

    async def run() -> None:
        client = httpx.AsyncClient(
            base_url="http://direct.local",
            transport=httpx.ASGITransport(app=direct),
        )
        proxy.state.prefill_direct_buffer_client = client
        try:
            concurrent = await asyncio.gather(*(lookup_once() for _ in range(3)))
            assert all(handles for handles in concurrent)
            assert calls["leases"] == [3]
            assert [
                handles[0]["metadata"]["proxy_direct_timings_ms"]
                ["direct_cache_lease_prefetch"]
                for handles in concurrent
            ] == [3.0, 3.0, 3.0]
            await asyncio.gather(*(release_one() for _ in concurrent))

            # No queued peer can join this later serial lookup, so it uses the
            # fixed one-reference path and leaves no extra pooled lifetime.
            serial = await lookup_once()
            assert serial
            assert calls["leases"] == [3, 1]
            await release_one()
        finally:
            await client.aclose()

    asyncio.run(run())
    assert calls["released"] == [bundle.image_hash] * 4
    assert refs["value"] == 0
    assert proxy.state.direct_feature_lease_prefetch_waiters == {}
    stats = proxy.state.direct_feature_lease_prefetch_stats
    assert stats["adaptive_reservations"] == 1
    assert stats["adaptive_reserved_references"] == 2
    assert stats["adaptive_waiter_peak"] == 3
    assert stats["pool_hits"] == 2
    assert stats["held_references"] == 0


def test_lease_prefetch_releases_every_partial_lookup_reservation():
    bundle = FeatureBundle(
        image_hash="partial-image",
        last_hidden=torch.ones((1, 2), dtype=torch.float32),
    )
    direct = FastAPI()
    calls = {"lookup_payloads": [], "released": []}

    @direct.post("/mooncake_epd/direct_feature_buffer/lookup")
    async def lookup(request: Request):
        payload = await request.json()
        calls["lookup_payloads"].append(payload)
        partial = _direct_lookup_payload(bundle, lease_count=int(payload["lease_count"]))
        partial["all_hit"] = False
        partial["misses"] = ["missing-image"]
        return partial

    @direct.post("/mooncake_epd/direct_feature_buffer/release")
    async def release(request: Request):
        payload = await request.json()
        calls["released"].extend(payload["feature_ids"])
        return {"released": len(payload["feature_ids"])}

    proxy = proxy_mod.create_app(
        proxy_mod.ProxyConfig(
            prefill_direct_buffer_service_url="http://direct.local",
            release_direct_feature_buffers_after_prefill=True,
            direct_feature_lease_prefetch=3,
            worker_generations=[("prefill-0", "generation-a")],
        )
    )

    async def run() -> None:
        client = httpx.AsyncClient(
            base_url="http://direct.local",
            transport=httpx.ASGITransport(app=direct),
        )
        proxy.state.prefill_direct_buffer_client = client
        try:
            handles = await proxy_mod._lookup_prefill_cached_direct_feature_handles(
                app=proxy,
                feature_ids=[bundle.image_hash, "missing-image"],
                target_worker_id="prefill-0",
            )
            assert handles == []
        finally:
            await client.aclose()

    asyncio.run(run())
    assert calls["lookup_payloads"] == [
        {
            "feature_ids": [bundle.image_hash, "missing-image"],
            "target_worker_id": "prefill-0",
            "lease_count": 3,
        }
    ]
    assert calls["released"] == [bundle.image_hash] * 3


def test_background_direct_release_batches_jobs_without_dropping_references():
    """Release batching may reduce RPCs, never logical reference releases."""

    direct = FastAPI()
    calls: list[dict] = []

    @direct.post("/mooncake_epd/direct_feature_buffer/release")
    async def release(request: Request):
        payload = await request.json()
        calls.append(payload)
        return {"released": len(payload["feature_ids"])}

    proxy = proxy_mod.create_app(
        proxy_mod.ProxyConfig(
            prefill_direct_buffer_service_url="http://direct.local",
            direct_feature_release_batch_max_jobs=8,
        )
    )

    async def run() -> None:
        client = httpx.AsyncClient(
            base_url="http://direct.local",
            transport=httpx.ASGITransport(app=direct),
        )
        proxy.state.prefill_direct_buffer_client = client
        try:
            # Enqueue synchronously in one event-loop turn so the worker's
            # explicit yield can merge the independent ownership decrements.
            proxy_mod._enqueue_direct_feature_buffer_release_job(
                proxy, {"prefill-0": ["image-a"]}
            )
            proxy_mod._enqueue_direct_feature_buffer_release_job(
                proxy, {"prefill-0": ["image-b"]}
            )
            proxy_mod._enqueue_direct_feature_buffer_release_job(
                proxy, {"prefill-0": ["image-c"]}
            )
            await proxy_mod._shutdown_direct_feature_release_dispatcher(proxy)
        finally:
            await client.aclose()

    asyncio.run(run())
    assert calls == [
        {
            "feature_ids": ["image-a", "image-b", "image-c"],
            "target_worker_id": "prefill-0",
        }
    ]
    stats = proxy.state.direct_feature_release_stats
    assert stats["scheduled"] == 3
    assert stats["completed"] == 3
    assert stats["batches"] == 1
    assert stats["batched_jobs"] == 2
    assert stats["released_feature_references"] == 3
    assert stats["failed"] == 0
