#!/usr/bin/env python3
"""Embeddable Prefill direct FeatureBundle buffer allocation API.

Production invariant: the registry served by this API must live in the same
Prefill/vLLM worker process that later resolves ``epd-direct://`` handles.  The
standalone ``main`` entry point is useful for diagnostics and process-manager
experiments, but a separate process cannot expose pointers owned by vLLM.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
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
from mooncake_epd.core.control.vllm_incarnation import (  # noqa: E402
    VLLM_PROCESS_INCARNATION,
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
    zero_fill: bool = True
    target_memory_mode: str = "registered_tensor"


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
            remote_incarnation=VLLM_PROCESS_INCARNATION,
            register_memory=bool(config.register_memory),
            target_memory_mode=config.target_memory_mode,
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
        targets: List[Dict[str, Any]] = []
        allocated_feature_ids: List[str] = []
        try:
            for raw in raw_descriptors:
                descriptor = FeatureBundleDescriptor.from_dict(dict(raw or {}))
                if not descriptor.feature_id:
                    raise FeatureHandleError("descriptor.feature_id is required")
                allocation = registry.allocate_for_descriptor(
                    descriptor,
                    zero_fill=bool(payload.get("zero_fill", config.zero_fill)),
                )
                allocated_feature_ids.append(allocation.feature_id)
                targets.append(allocation.as_direct_target())
        except Exception as exc:
            for feature_id in allocated_feature_ids:
                registry.release(feature_id)
            raise HTTPException(status_code=400, detail=f"direct buffer allocation failed: {exc}") from exc
        return {"targets": targets, "count": len(targets), "worker_id": registry.worker_id}

    @app.post("/release")
    async def release(payload: Dict[str, Any]) -> Dict[str, Any]:
        raw = payload.get("feature_ids") or payload.get("features") or []
        if isinstance(raw, str):
            raw = [raw]
        if not isinstance(raw, list):
            raise HTTPException(status_code=400, detail="release requires feature_ids[]")
        for feature_id in raw:
            registry.release(str(feature_id))
        return {"released": len(raw), "stats": dict(registry.stats())}

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
    ap.add_argument("--zero-fill", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--target-memory-mode", choices=["registered_tensor", "managed_buffer", "auto"], default=os.getenv("MOONCAKE_EPD_DIRECT_TARGET_MODE", "registered_tensor"))
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
