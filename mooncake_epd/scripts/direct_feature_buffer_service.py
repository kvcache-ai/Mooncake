#!/usr/bin/env python3
"""Embeddable Prefill direct FeatureBundle buffer allocation API.

Production invariant: the registry served by this API must live in the same
Prefill/vLLM worker process that later resolves ``epd-direct://`` handles.  The
standalone ``main`` entry point is useful for diagnostics and process-manager
experiments, but a separate process cannot expose pointers owned by vLLM.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.state import (  # noqa: E402
    DirectFeatureBufferRegistry,
    FeatureBundleDescriptor,
    FeatureHandleError,
    register_direct_feature_buffer_registry,
    unregister_direct_feature_buffer_registry,
)
from mooncake_epd.core.transfer import TransferEngine  # noqa: E402

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DirectFeatureBufferServiceConfig:
    host: str = "127.0.0.1"
    port: int = 8331
    worker_id: str = "prefill-0"
    device: str = "cpu"
    mooncake_protocol: str = "tcp"
    mooncake_local_hostname: str = "localhost"
    mooncake_metadata_server: str = "P2PHANDSHAKE"
    mooncake_device_name: str = ""
    remote_session: Optional[str] = None
    register_memory: bool = True
    # Every direct publish covers the full descriptor before ``mark_ready``.
    # Clearing the destination first is therefore pure write amplification.
    zero_fill: bool = False
    target_memory_mode: str = "registered_tensor"
    persistent_cache: bool = False
    max_cache_entries: int = 64
    max_cache_bytes: int = 2 * 1024 * 1024 * 1024


def create_app(
    config: Optional[DirectFeatureBufferServiceConfig] = None,
    *,
    registry: Optional[DirectFeatureBufferRegistry] = None,
    transfer_engine: Optional[TransferEngine] = None,
) -> FastAPI:
    config = config or DirectFeatureBufferServiceConfig()
    if registry is None:
        engine = transfer_engine or TransferEngine(
            protocol=config.mooncake_protocol,
            local_hostname=config.mooncake_local_hostname,
            metadata_server=config.mooncake_metadata_server,
            device_name=config.mooncake_device_name,
        )
        registry = DirectFeatureBufferRegistry(
            worker_id=config.worker_id,
            device=config.device,
            transfer_engine=engine if config.register_memory or config.remote_session is None else transfer_engine,
            remote_session=config.remote_session,
            register_memory=bool(config.register_memory),
            target_memory_mode=config.target_memory_mode,
            persistent_cache=bool(config.persistent_cache),
            max_cache_entries=int(config.max_cache_entries),
            max_cache_bytes=int(config.max_cache_bytes),
        )
    register_direct_feature_buffer_registry(registry)

    @asynccontextmanager
    async def _lifespan(app: FastAPI):
        try:
            yield
        finally:
            unregister_direct_feature_buffer_registry(registry.worker_id)

    app = FastAPI(
        title="Mooncake EPD Prefill Direct Feature Buffer Service",
        lifespan=_lifespan,
    )
    app.state.config = config
    app.state.registry = registry

    def _allocate_targets(
        raw_descriptors: List[Any],
        *,
        zero_fill: bool,
        reuse_ready: bool,
    ) -> List[Dict[str, Any]]:
        targets: List[Dict[str, Any]] = []
        for raw in raw_descriptors:
            descriptor = FeatureBundleDescriptor.from_dict(dict(raw or {}))
            if not descriptor.feature_id:
                raise FeatureHandleError("descriptor.feature_id is required")
            allocation = registry.allocate_for_descriptor(
                descriptor,
                zero_fill=zero_fill,
                reuse_ready=reuse_ready,
            )
            should_publish = False
            with registry._lock:  # noqa: SLF001 - atomic publish reservation
                if not allocation.ready and not allocation.publish_started:
                    allocation.publish_started = True
                    should_publish = True
            target = allocation.as_direct_target()
            target["cache_hit"] = bool(allocation.ready)
            target["publish_required"] = bool(should_publish)
            target["publish_pending"] = bool(not allocation.ready and not should_publish)
            target["ref_count"] = int(allocation.ref_count)
            targets.append(target)
        return targets

    def _wait_for_ready(feature_ids: List[str], timeout_s: float) -> None:
        for feature_id in feature_ids:
            registry.wait_ready(feature_id, timeout_s=timeout_s)

    def _release_features(feature_ids: List[str]) -> None:
        for feature_id in feature_ids:
            registry.release(feature_id)

    def _lookup_ready_targets(
        feature_ids: List[Any],
        *,
        lease_count: int,
    ) -> tuple[List[Dict[str, Any]], List[str], float]:
        """Look up and serialize direct targets outside the ASGI event loop.

        The embedded production route and this diagnostic service must share
        the same concurrency contract.  Registry access is protected by its
        own thread lock, while target export can touch CUDA-backed tensors; do
        not let that synchronous work delay unrelated API requests.
        """

        started = time.perf_counter()
        hits: List[Dict[str, Any]] = []
        misses: List[str] = []
        for feature_id in feature_ids:
            allocation = registry.lookup_ready(str(feature_id), lease_count=lease_count)
            if allocation is None:
                misses.append(str(feature_id))
                continue
            target = allocation.as_direct_target()
            target["cache_hit"] = True
            target["publish_required"] = False
            target["ref_count"] = int(allocation.ref_count)
            target["lease_count"] = lease_count
            hits.append(
                {
                    "feature_id": str(feature_id),
                    "descriptor": allocation.descriptor.to_dict(),
                    "target": target,
                }
            )
        return hits, misses, max(0.0, (time.perf_counter() - started) * 1000.0)

    @app.get("/health")
    async def health() -> Dict[str, Any]:
        return {
            "status": "ok",
            "worker_id": registry.worker_id,
            "device": str(registry.device),
            "embedded_prefill_process_required": True,
        }

    @app.get("/stats")
    async def stats() -> Dict[str, Any]:
        return dict(registry.stats())

    @app.post("/allocate")
    async def allocate(payload: Dict[str, Any]) -> Dict[str, Any]:
        raw_descriptors = payload.get("descriptors")
        if raw_descriptors is None and isinstance(payload.get("descriptor"), dict):
            raw_descriptors = [payload.get("descriptor")]
        if not isinstance(raw_descriptors, list) or not raw_descriptors:
            raise HTTPException(status_code=400, detail="allocate requires descriptors[]")
        try:
            # CUDA allocation and Mooncake registration may block, so keep them
            # off the API loop. The registry single-flights identical feature
            # ids and serializes only engine registration, allowing unrelated
            # FeatureBundles to allocate concurrently.
            targets = await asyncio.to_thread(
                _allocate_targets,
                list(raw_descriptors),
                zero_fill=bool(payload.get("zero_fill", config.zero_fill)),
                reuse_ready=bool(payload.get("reuse_ready", True)),
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"direct buffer allocation failed: {exc}") from exc
        return {"targets": targets, "count": len(targets), "worker_id": registry.worker_id}

    @app.post("/lookup")
    async def lookup(payload: Dict[str, Any]) -> Dict[str, Any]:
        raw = payload.get("feature_ids") or payload.get("features") or []
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list) or not raw:
            raise HTTPException(status_code=400, detail="lookup requires feature_ids[]")
        try:
            lease_count = int(payload.get("lease_count", 1) or 1)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="lookup lease_count must be an integer") from exc
        # This is a bounded control-plane reference reservation, not an
        # unbounded cache pin.  The Proxy releases every unused lease on TTL,
        # eviction, explicit flush, and shutdown.
        if lease_count <= 0 or lease_count > 1024:
            raise HTTPException(status_code=400, detail="lookup lease_count must be in [1, 1024]")
        try:
            hits, misses, server_lookup_ms = await asyncio.to_thread(
                _lookup_ready_targets,
                list(raw),
                lease_count=lease_count,
            )
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"direct buffer lookup failed: {exc}") from exc
        return {
            "hits": hits,
            "misses": misses,
            "count": len(hits),
            "all_hit": len(hits) == len(raw),
            "worker_id": registry.worker_id,
            "lease_count": lease_count,
            "server_lookup_ms": server_lookup_ms,
        }

    @app.post("/mark_ready")
    async def mark_ready(payload: Dict[str, Any]) -> Dict[str, Any]:
        raw = payload.get("feature_ids") or payload.get("features") or []
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list) or not raw:
            raise HTTPException(status_code=400, detail="mark_ready requires feature_ids[]")
        for feature_id in raw:
            registry.mark_ready(str(feature_id))
        return {"ready": len(raw), "stats": dict(registry.stats())}

    @app.post("/wait_ready")
    async def wait_ready(payload: Dict[str, Any]) -> Dict[str, Any]:
        raw = payload.get("feature_ids") or payload.get("features") or []
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list) or not raw:
            raise HTTPException(status_code=400, detail="wait_ready requires feature_ids[]")
        timeout_s = float(payload.get("timeout_s", 30.0) or 30.0)
        try:
            await asyncio.to_thread(
                _wait_for_ready,
                [str(feature_id) for feature_id in raw],
                timeout_s,
            )
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"ready": len(raw), "stats": dict(registry.stats())}

    @app.post("/release")
    async def release(payload: Dict[str, Any]) -> Dict[str, Any]:
        raw = payload.get("feature_ids") or payload.get("features") or []
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list):
            raise HTTPException(status_code=400, detail="release requires feature_ids[]")
        try:
            await asyncio.to_thread(_release_features, [str(feature_id) for feature_id in raw])
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"direct buffer release failed: {exc}") from exc
        return {"released": len(raw), "stats": dict(registry.stats())}

    # Match the embedded vLLM route namespace used by production sitecustomize.
    # The root routes remain for standalone diagnostics and older tests.
    prefix = "/mooncake_epd/direct_feature_buffer"
    app.get(f"{prefix}/stats")(stats)
    app.post(f"{prefix}/allocate")(allocate)
    app.post(f"{prefix}/lookup")(lookup)
    app.post(f"{prefix}/mark_ready")(mark_ready)
    app.post(f"{prefix}/wait_ready")(wait_ready)
    app.post(f"{prefix}/release")(release)

    return app


def parse_args() -> DirectFeatureBufferServiceConfig:
    ap = argparse.ArgumentParser(description="Mooncake EPD prefill direct feature buffer allocation API")
    ap.add_argument("--host", default=os.getenv("MOONCAKE_EPD_DIRECT_BUFFER_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.getenv("MOONCAKE_EPD_DIRECT_BUFFER_PORT", "8331")))
    ap.add_argument("--worker-id", default=os.getenv("MOONCAKE_EPD_DIRECT_BUFFER_WORKER_ID", "prefill-0"))
    ap.add_argument("--device", default=os.getenv("MOONCAKE_EPD_DIRECT_BUFFER_DEVICE", "cpu"))
    ap.add_argument("--mooncake-protocol", default=os.getenv("MOONCAKE_PROTOCOL", "tcp"))
    ap.add_argument("--mooncake-local-hostname", default=os.getenv("MOONCAKE_LOCAL_HOSTNAME", "localhost"))
    ap.add_argument("--mooncake-metadata-server", default=os.getenv("MOONCAKE_TE_META_DATA_SERVER", "P2PHANDSHAKE"))
    ap.add_argument("--mooncake-device-name", default=os.getenv("MOONCAKE_DEVICE_NAME", ""))
    ap.add_argument("--remote-session", default=os.getenv("MOONCAKE_EPD_DIRECT_REMOTE_SESSION"))
    ap.add_argument("--register-memory", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--zero-fill", action=argparse.BooleanOptionalAction, default=False)
    ap.add_argument("--target-memory-mode", choices=["registered_tensor", "managed_buffer", "auto"], default=os.getenv("MOONCAKE_EPD_DIRECT_TARGET_MODE", "registered_tensor"))
    ap.add_argument("--persistent-cache", action=argparse.BooleanOptionalAction, default=os.getenv("MOONCAKE_EPD_DIRECT_BUFFER_PERSISTENT_CACHE", "0").lower() in {"1", "true", "yes", "on"})
    ap.add_argument("--max-cache-entries", type=int, default=int(os.getenv("MOONCAKE_EPD_DIRECT_BUFFER_CACHE_MAX_ENTRIES", "64")))
    ap.add_argument("--max-cache-bytes", type=int, default=int(os.getenv("MOONCAKE_EPD_DIRECT_BUFFER_CACHE_MAX_BYTES", str(2 * 1024 * 1024 * 1024))))
    args = ap.parse_args()
    return DirectFeatureBufferServiceConfig(**vars(args))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    config = parse_args()
    app = create_app(config)
    import uvicorn

    uvicorn.run(app, host=config.host, port=config.port)


if __name__ == "__main__":
    main()
