"""Control-plane handles for E→P multimodal hidden-state transfer.

A FeatureHandle is the lightweight object that should travel in serving/A2A
metadata instead of tensor bytes.  It names a FeatureBundle in an MMStore and
carries the descriptor needed by Prefill to validate the hidden states before
skipping the vision encoder.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, Optional

import torch

from ..transfer import Channel, Mode, TransferPolicy
from .feature_store import FeatureBundle, FeatureBundleDescriptor
from .mm_store import MMStore, MMStoreHandle


@dataclass(frozen=True)
class FeatureHandle:
    handle_id: str
    feature_id: str
    store_id: str
    uri: str
    descriptor: FeatureBundleDescriptor
    created_at: float = field(default_factory=time.monotonic)
    ttl_deadline: float = float("inf")
    metadata: Dict = field(default_factory=dict)

    @property
    def expired(self) -> bool:
        return self.ttl_deadline != float("inf") and time.monotonic() > self.ttl_deadline

    def as_control_payload(self) -> Dict:
        return {
            "handle_id": self.handle_id,
            "feature_id": self.feature_id,
            "store_id": self.store_id,
            "uri": self.uri,
            "created_at": self.created_at,
            "ttl_deadline": (None if self.ttl_deadline == float("inf") else self.ttl_deadline),
            "descriptor": self.descriptor.to_dict(),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_control_payload(cls, payload: Dict) -> "FeatureHandle":
        return cls(
            handle_id=str(payload.get("handle_id") or ""),
            feature_id=str(payload.get("feature_id") or ""),
            store_id=str(payload.get("store_id") or ""),
            uri=str(payload.get("uri") or ""),
            descriptor=FeatureBundleDescriptor.from_dict(dict(payload.get("descriptor") or {})),
            created_at=float(payload.get("created_at", time.monotonic()) or time.monotonic()),
            ttl_deadline=(
                float("inf")
                if payload.get("ttl_deadline") is None
                else float(payload.get("ttl_deadline"))
            ),
            metadata=dict(payload.get("metadata") or {}),
        )


class FeatureHandleError(RuntimeError):
    pass


class FeatureHandleExpired(FeatureHandleError):
    pass


class FeatureHandleRegistry:
    """MMStore-backed feature handle registry.

    The registry is intentionally small and explicit: it does not compute model
    features by itself.  Encoder workers publish real FeatureBundles; Prefill
    workers resolve handles into MMStore prefetches and validate descriptors.
    """

    def __init__(self, mm_store: MMStore, *, store_id: str = "local-mm-store"):
        self.mm_store = mm_store
        self.store_id = str(store_id)

    def publish_bundle(
        self,
        bundle: FeatureBundle,
        *,
        ttl_seconds: Optional[float] = None,
        metadata: Optional[Dict] = None,
        checksum: bool = False,
        incref: bool = False,
    ) -> FeatureHandle:
        descriptor = bundle.descriptor(checksum=checksum)
        feature_id = self.mm_store.publish(bundle, incref=incref)
        ttl_deadline = (
            float("inf")
            if ttl_seconds is None
            else time.monotonic() + max(0.0, float(ttl_seconds))
        )
        if ttl_seconds is not None:
            try:
                self.mm_store.shared_store.refresh_ttl(feature_id, ttl_seconds=ttl_seconds)
            except KeyError:
                # publish() succeeded but the backing store was concurrently cleared.
                raise FeatureHandleError(f"published feature disappeared: {feature_id}")
        return FeatureHandle(
            handle_id=uuid.uuid4().hex,
            feature_id=feature_id,
            store_id=self.store_id,
            uri=f"mmstore://{self.store_id}/{feature_id}",
            descriptor=descriptor,
            ttl_deadline=ttl_deadline,
            metadata=dict(metadata or {}),
        )

    def prefetch(
        self,
        handle: FeatureHandle,
        *,
        target_worker_id: str,
        target_device: torch.device,
        policy: Optional[TransferPolicy] = None,
    ) -> MMStoreHandle:
        self._validate_handle(handle)
        policy = policy or TransferPolicy(
            mode=Mode.PULL,
            channel=Channel.ENCODER_TO_PREFILL,
            extra={"force_copy": True},
        )
        return self.mm_store.prefetch(
            handle.feature_id,
            target_worker_id=target_worker_id,
            target_device=target_device,
            policy=policy,
        )

    def wait_and_validate(
        self,
        handle: FeatureHandle,
        prefetch_handle: MMStoreHandle,
        *,
        timeout: Optional[float] = None,
        expected_model_fingerprint: Optional[str] = None,
        expected_processor_fingerprint: Optional[str] = None,
        require_checksum: bool = False,
    ) -> FeatureBundle:
        self._validate_handle(handle)
        bundle = self.mm_store.wait(prefetch_handle, timeout=timeout)
        if bundle is None:
            raise TimeoutError(f"feature prefetch did not complete: {handle.uri}")
        handle.descriptor.validate_bundle(
            bundle,
            expected_model_fingerprint=expected_model_fingerprint,
            expected_processor_fingerprint=expected_processor_fingerprint,
            require_checksum=require_checksum,
        )
        return bundle

    def resolve_local(self, handle: FeatureHandle, *, worker_id: str) -> Optional[FeatureBundle]:
        self._validate_handle(handle)
        bundle = self.mm_store.lookup(worker_id, handle.feature_id)
        if bundle is not None:
            handle.descriptor.validate_bundle(bundle)
        return bundle

    def _validate_handle(self, handle: FeatureHandle) -> None:
        if handle.store_id != self.store_id:
            raise FeatureHandleError(
                f"feature handle targets store={handle.store_id}, registry store={self.store_id}"
            )
        if handle.feature_id != handle.descriptor.feature_id:
            raise FeatureHandleError(
                "feature handle id does not match descriptor: "
                f"handle={handle.feature_id} descriptor={handle.descriptor.feature_id}"
            )
        if handle.expired:
            raise FeatureHandleExpired(f"feature handle expired: {handle.uri}")
