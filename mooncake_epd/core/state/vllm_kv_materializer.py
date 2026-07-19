"""Explicit adapter from Agent KV manifests to a vLLM-compatible block owner.

The adapter deliberately does not accept client-provided physical ids.  It
materializes only through ``MooncakeKVStateStore`` (which performs destination
allocation plus Store/Transfer data movement) and produces a checked connector
metadata envelope for the next Decode request.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

from .kv_state_store import KVStateDescriptor, MooncakeKVStateStore


class VLLMKVMaterializationError(RuntimeError):
    """Raised when a real target allocation/import/commit is unavailable."""


@dataclass(frozen=True)
class VLLMKVMaterializationResult:
    state_id: str
    workflow_id: str
    owner_node_id: str
    target_node_id: str
    block_ids: tuple[str, ...]
    manifest_checksums: tuple[str, ...]
    materialized_bytes: int
    backend: str
    committed: bool
    elapsed_ms: float
    connector_metadata: Mapping[str, Any]


ConnectorCommitter = Callable[[Mapping[str, Any]], Optional[Mapping[str, Any]]]


class VLLMKVMaterializer:
    """Strict materializer with an optional vLLM connector commit callback.

    ``MooncakeKVStateStore`` is the authoritative allocator abstraction in the
    optional package.  Production integrations wire ``commit_to_connector`` to
    the version-locked vLLM adapter after it has accepted the newly allocated
    target block ids.  Without an explicit committer, callers can still obtain
    verified metadata but cannot claim a Decode-side connector commit.
    """

    def __init__(
        self,
        state_store: MooncakeKVStateStore,
        *,
        commit_to_connector: Optional[ConnectorCommitter] = None,
    ) -> None:
        self.state_store = state_store
        self.commit_to_connector = commit_to_connector

    def materialize_read(
        self,
        state_id: str,
        *,
        target_node_id: str,
        require_connector_commit: bool = False,
    ) -> VLLMKVMaterializationResult:
        """Resolve a read-only state or materialize a remote descriptor safely."""

        descriptor = self.state_store.get_state(state_id)
        if descriptor is None:
            raise KeyError(f"unknown KV state: {state_id}")
        if descriptor.owner_node_id != str(target_node_id) or bool(
            descriptor.metadata.get("remote_descriptor_shared")
        ):
            return self.materialize_write(
                state_id,
                target_node_id=target_node_id,
                require_connector_commit=require_connector_commit,
            )
        return self._commit_result(
            descriptor,
            target_node_id=target_node_id,
            materialized_bytes=0,
            backend="same_node_block_ref",
            require_connector_commit=require_connector_commit,
            started_at=time.perf_counter(),
        )

    def materialize_write(
        self,
        state_id: str,
        *,
        target_node_id: str,
        require_connector_commit: bool = False,
    ) -> VLLMKVMaterializationResult:
        """Allocate target blocks, import pages, validate and optionally commit."""

        started = time.perf_counter()
        before = self._import_bytes()
        try:
            descriptor = self.state_store.materialize_for_write(
                state_id,
                target_node_id=str(target_node_id),
            )
        except Exception as exc:
            raise VLLMKVMaterializationError(
                f"KV materialization failed for state={state_id} target={target_node_id}: {exc}"
            ) from exc
        bytes_delta = max(0, self._import_bytes() - before)
        backend = (
            "mooncake_store_pages"
            if self.state_store.kv_page_store is not None and bytes_delta > 0
            else "mooncake_transfer_engine"
            if self.state_store.remote_materializer is not None
            else "same_node_block_ref"
        )
        return self._commit_result(
            descriptor,
            target_node_id=target_node_id,
            materialized_bytes=bytes_delta,
            backend=backend,
            require_connector_commit=require_connector_commit,
            started_at=started,
        )

    def _commit_result(
        self,
        descriptor: KVStateDescriptor,
        *,
        target_node_id: str,
        materialized_bytes: int,
        backend: str,
        require_connector_commit: bool,
        started_at: float,
    ) -> VLLMKVMaterializationResult:
        if descriptor.owner_node_id != str(target_node_id):
            raise VLLMKVMaterializationError(
                "materialized descriptor owner does not match target: "
                f"owner={descriptor.owner_node_id} target={target_node_id}"
            )
        manifests = tuple(descriptor.manifests)
        for manifest in manifests:
            manifest.validate(allow_expired=False)
            if manifest.manifest_checksum() != manifest.checksum:
                raise VLLMKVMaterializationError(
                    f"manifest checksum mismatch: {manifest.transfer_key}"
                )
        envelope: dict[str, Any] = {
            "epd_agent_state_id": descriptor.state_id,
            "workflow_id": descriptor.workflow_id,
            "vllm_kv_block_ids": list(descriptor.block_ids),
            "kv_state_manifests": [manifest.as_control_payload() for manifest in manifests],
            "target_node_id": str(target_node_id),
            "materialized_bytes": int(materialized_bytes),
            "backend": str(backend),
        }
        committed = False
        if self.commit_to_connector is not None:
            response = self.commit_to_connector(envelope)
            if response is not None:
                envelope["connector_commit"] = dict(response)
            committed = True
        elif require_connector_commit:
            raise VLLMKVMaterializationError(
                "vLLM connector commit callback is required for this materialization"
            )
        return VLLMKVMaterializationResult(
            state_id=descriptor.state_id,
            workflow_id=descriptor.workflow_id,
            owner_node_id=descriptor.owner_node_id,
            target_node_id=str(target_node_id),
            block_ids=tuple(descriptor.block_ids),
            manifest_checksums=tuple(manifest.checksum for manifest in manifests),
            materialized_bytes=int(materialized_bytes),
            backend=str(backend),
            committed=committed,
            elapsed_ms=(time.perf_counter() - started_at) * 1000.0,
            connector_metadata=envelope,
        )

    def _import_bytes(self) -> int:
        page_store = self.state_store.kv_page_store
        return int(page_store.stats().get("import_bytes", 0)) if page_store is not None else 0
