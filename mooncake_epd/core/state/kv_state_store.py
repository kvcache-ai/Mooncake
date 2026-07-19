"""KV state descriptor store for zero-copy Agent state cloning.

This module is intentionally metadata-first.  It never serializes KV tensors for
clone; it shares authoritative BlockRef/GID descriptors and relies on
``PagedKVManager`` + KV Directory refcounts for local zero-copy semantics.  A
cross-node clone requires a real remote materializer/transfer implementation;
without one the API fails fast instead of pretending that local pointers are
valid on another node.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

import torch

from .feature_store import FeatureStore
from .kv_manifest import KVStateManifest, manifest_checksum
from .mooncake_kv_page_store import MooncakeKVPageStore
from .page_manager import BlockRef, PagedKVManager
from ..transfer import Channel, Mode, TransferEngine, TransferPolicy


@dataclass(frozen=True)
class KVBlockDescriptor:
    global_block_id: str
    physical_id: int
    filled: int
    physical_node_id: str
    logical_index: int = 0
    virtual_offset: int = 0

    @classmethod
    def from_ref(cls, ref: BlockRef) -> "KVBlockDescriptor":
        return cls(
            global_block_id=str(ref.global_block_id or f"local:{ref.physical_id}"),
            physical_id=int(ref.physical_id),
            filled=int(ref.filled),
            physical_node_id=str(ref.physical_node_id),
            logical_index=int(ref.logical_index),
            virtual_offset=int(ref.virtual_offset),
        )

    def to_ref(self) -> BlockRef:
        return BlockRef(
            physical_id=self.physical_id,
            filled=self.filled,
            global_block_id=self.global_block_id,
            physical_node_id=self.physical_node_id,
            logical_index=self.logical_index,
            virtual_offset=self.virtual_offset,
        )


@dataclass(frozen=True)
class KVStateDescriptor:
    state_id: str
    workflow_id: str
    version_id: str
    parent_version_id: Optional[str]
    snapshot_epoch: int
    owner_node_id: str
    block_descriptors: tuple[KVBlockDescriptor, ...]
    feature_hashes: tuple[str, ...] = field(default_factory=tuple)
    created_at: float = field(default_factory=time.monotonic)
    ttl_deadline: float = float("inf")
    metadata: Dict = field(default_factory=dict)
    manifests: tuple[KVStateManifest, ...] = field(default_factory=tuple)

    @property
    def num_kv_tokens(self) -> int:
        return sum(block.filled for block in self.block_descriptors)

    @property
    def block_ids(self) -> List[str]:
        return [block.global_block_id for block in self.block_descriptors]

    def to_refs(self) -> List[BlockRef]:
        return [block.to_ref() for block in self.block_descriptors]


RemoteMaterializer = Callable[[KVStateDescriptor, str], Sequence[BlockRef]]


class MooncakeKVStateStore:
    """Descriptor-backed store for Agent KV state fork/release.

    Same-node clone is zero-copy: it calls ``PagedKVManager.fork_refs`` to bump
    authoritative KV Directory refcounts and creates a new immutable descriptor.
    Cross-node clone needs a real materializer that installs/pulls the referenced
    pages on the target node; this class will not fake that by reusing local
    physical ids on a remote node.
    """

    def __init__(
        self,
        page_manager: PagedKVManager,
        *,
        feature_store: Optional[FeatureStore] = None,
        node_id: Optional[str] = None,
        remote_materializer: Optional[RemoteMaterializer] = None,
        page_managers_by_node: Optional[Dict[str, PagedKVManager]] = None,
        allow_remote_descriptor_sharing: bool = False,
        model_id: str = "unknown",
        model_revision: str = "unknown",
        tp_rank: int = 0,
        tp_size: int = 1,
        source_generation: str = "local",
        target_generation: str = "local",
        kv_page_store: Optional[MooncakeKVPageStore] = None,
    ):
        self.pm = page_manager
        self.feature_store = feature_store
        self.node_id = str(node_id or page_manager.node_id)
        self.remote_materializer = remote_materializer
        self.allow_remote_descriptor_sharing = bool(allow_remote_descriptor_sharing)
        self.model_id = str(model_id)
        self.model_revision = str(model_revision)
        self.tp_rank = int(tp_rank)
        self.tp_size = int(tp_size)
        self.source_generation = str(source_generation)
        self.target_generation = str(target_generation)
        self.kv_page_store = kv_page_store
        self._page_managers: Dict[str, PagedKVManager] = {self.node_id: self.pm}
        for key, manager in dict(page_managers_by_node or {}).items():
            self._page_managers[str(key)] = manager
        self._states: Dict[str, KVStateDescriptor] = {}
        self._state_refs: Dict[str, List[BlockRef]] = {}
        self._pending_state_ids: set[str] = set()
        self._lock = threading.RLock()

    def register_page_manager(self, node_id: str, page_manager: PagedKVManager) -> None:
        """Register the authoritative page manager for a remote materialized node."""
        self._page_managers[str(node_id)] = page_manager

    def register_state(
        self,
        refs: Sequence[BlockRef],
        *,
        workflow_id: str,
        version_id: Optional[str] = None,
        parent_version_id: Optional[str] = None,
        snapshot_epoch: int = 0,
        state_id: Optional[str] = None,
        feature_hashes: Optional[Iterable[str]] = None,
        ttl_deadline: float = float("inf"),
        metadata: Optional[Dict] = None,
        model_id: Optional[str] = None,
        model_revision: Optional[str] = None,
        tp_rank: Optional[int] = None,
        tp_size: Optional[int] = None,
        source_generation: Optional[str] = None,
        target_generation: Optional[str] = None,
    ) -> KVStateDescriptor:
        indexed_refs = [self._normalise_ref(ref, idx, page_manager=self.pm) for idx, ref in enumerate(refs)]
        state_id = state_id or uuid.uuid4().hex
        version_id = version_id or uuid.uuid4().hex
        # Reserve the identity before exporting payloads.  Store page keys are
        # derived from state/version IDs, so detecting a duplicate only after
        # export can overwrite another live state or leave orphaned objects.
        with self._lock:
            if state_id in self._states or state_id in self._pending_state_ids:
                raise ValueError(f"state already registered: {state_id}")
            self._pending_state_ids.add(state_id)
        try:
            manifests = self._build_manifests(
                indexed_refs,
                workflow_id=str(workflow_id),
                state_id=state_id,
                version_id=version_id,
                snapshot_epoch=int(snapshot_epoch),
                owner_node_id=self.node_id,
                target_node_id=self.node_id,
                ttl_deadline=ttl_deadline,
                model_id=model_id or self.model_id,
                model_revision=model_revision or self.model_revision,
                tp_rank=self.tp_rank if tp_rank is None else int(tp_rank),
                tp_size=self.tp_size if tp_size is None else int(tp_size),
                source_generation=source_generation or self.source_generation,
                target_generation=target_generation or self.target_generation,
            )
            if self.kv_page_store is not None and manifests:
                manifests = self.kv_page_store.export_pages(self.pm, indexed_refs, manifests)
            descriptor = KVStateDescriptor(
                state_id=state_id,
                workflow_id=str(workflow_id),
                version_id=version_id,
                parent_version_id=parent_version_id,
                snapshot_epoch=int(snapshot_epoch),
                owner_node_id=self.node_id,
                block_descriptors=tuple(KVBlockDescriptor.from_ref(ref) for ref in indexed_refs),
                feature_hashes=tuple(dict.fromkeys(str(item) for item in (feature_hashes or []))),
                ttl_deadline=ttl_deadline,
                metadata=dict(metadata or {}),
                manifests=tuple(manifests),
            )
            with self._lock:
                if state_id not in self._pending_state_ids or state_id in self._states:
                    raise RuntimeError(f"state reservation was lost: {state_id}")
                self._states[state_id] = descriptor
                self._state_refs[state_id] = indexed_refs
                self._pending_state_ids.discard(state_id)
        except Exception:
            with self._lock:
                self._pending_state_ids.discard(state_id)
            raise
        self._acquire_leases(indexed_refs, self.pm)
        for feature_hash in descriptor.feature_hashes:
            if self.feature_store is not None and self.feature_store.has(feature_hash):
                self.feature_store.incref(feature_hash)
        return descriptor

    def export_state_to_store(self, state_id: str) -> KVStateDescriptor:
        """Export a registered state as immutable Mooncake Store page objects.

        The operation is explicit because exporting real vLLM blocks is a
        data-plane action.  A failure leaves the previous descriptor live and
        unmodified; callers can retry with the same state/lease.
        """

        if self.kv_page_store is None:
            raise RuntimeError("export_state_to_store requires a MooncakeKVPageStore")
        with self._lock:
            descriptor = self._states.get(state_id)
            refs = list(self._state_refs.get(state_id, []))
        if descriptor is None:
            raise KeyError(f"unknown state: {state_id}")
        manager = self._manager_for_node(descriptor.owner_node_id)
        if all(manifest.has_store_payload for manifest in descriptor.manifests):
            return descriptor
        manifests = self.kv_page_store.export_pages(manager, refs, descriptor.manifests)
        exported = KVStateDescriptor(
            state_id=descriptor.state_id,
            workflow_id=descriptor.workflow_id,
            version_id=descriptor.version_id,
            parent_version_id=descriptor.parent_version_id,
            snapshot_epoch=descriptor.snapshot_epoch,
            owner_node_id=descriptor.owner_node_id,
            block_descriptors=descriptor.block_descriptors,
            feature_hashes=descriptor.feature_hashes,
            created_at=descriptor.created_at,
            ttl_deadline=descriptor.ttl_deadline,
            metadata={**descriptor.metadata, "store_exported": True},
            manifests=tuple(manifests),
        )
        with self._lock:
            if self._states.get(state_id) is not descriptor:
                raise RuntimeError(f"state changed while exporting to Store: {state_id}")
            self._states[state_id] = exported
        return exported

    def clone_state(
        self,
        source_state_id: str,
        *,
        child_state_id: Optional[str] = None,
        child_version_id: Optional[str] = None,
        target_node_id: Optional[str] = None,
        share_remote_descriptor: Optional[bool] = None,
        metadata: Optional[Dict] = None,
    ) -> KVStateDescriptor:
        with self._lock:
            parent = self._states[source_state_id]
            parent_refs = list(self._state_refs[source_state_id])
        parent_manager = self._manager_for_node(parent.owner_node_id)
        target_node = str(target_node_id or parent.owner_node_id)
        # A descriptor-share clone keeps the exact immutable physical pages of
        # its parent.  Its captured page checksums are therefore still valid;
        # recalculating them would turn a metadata/refcount operation into a
        # GPU-to-host copy for every KV page.  Materialized clones receive new
        # pages and must validate their content below.
        reuse_captured_page_checksums = False
        if target_node != parent.owner_node_id:
            use_descriptor_share = (
                self.allow_remote_descriptor_sharing
                if share_remote_descriptor is None
                else bool(share_remote_descriptor)
            )
            if use_descriptor_share:
                # Cross-node "zero-copy clone" means the child descriptor points
                # at the source owner blocks and increments the owner refcounts.
                # The target node must materialize/CoW explicitly before writing.
                child_refs = parent_manager.fork_refs(parent_refs)
                owner_node = parent.owner_node_id
                reuse_captured_page_checksums = True
            elif self.remote_materializer is None:
                if self.kv_page_store is None or not parent.manifests or not all(
                    manifest.has_store_payload for manifest in parent.manifests
                ):
                    raise NotImplementedError(
                        "cross-node KVState clone requires a real Mooncake remote_materializer "
                        "or Store-backed page manifests"
                    )
                target_pm = self._page_managers.get(target_node)
                if target_pm is None:
                    raise RuntimeError(f"no page manager registered for target node: {target_node}")
                materialized_refs = list(self.kv_page_store.materialize_pages(target_pm, parent.manifests))
                child_refs = [
                    self._normalise_ref(ref, idx, page_manager=target_pm)
                    for idx, ref in enumerate(materialized_refs)
                ]
                owner_node = target_node
            else:
                materialized_refs = list(self.remote_materializer(parent, target_node))
                if not materialized_refs:
                    raise RuntimeError("remote_materializer returned no KV refs")
                target_pm = self._page_managers.get(target_node)
                if target_pm is None:
                    raise RuntimeError(
                        f"remote materializer installed refs for node={target_node}, but no page manager was registered"
                    )
                try:
                    child_refs = [
                        self._normalise_ref(ref, idx, page_manager=target_pm)
                        for idx, ref in enumerate(materialized_refs)
                    ]
                except Exception:
                    target_pm.release_refs(materialized_refs)
                    raise
                owner_node = target_node
        else:
            child_refs = parent_manager.fork_refs(parent_refs)
            owner_node = parent.owner_node_id
            reuse_captured_page_checksums = True

        child_meta = dict(parent.metadata)
        child_meta.update(metadata or {})
        if target_node != owner_node:
            child_meta.update(
                {
                    "remote_descriptor_shared": True,
                    "target_node_id": target_node,
                    "owner_node_id": owner_node,
                    "requires_materialize_before_write": True,
                }
            )
        child_state_id = child_state_id or uuid.uuid4().hex
        child_version_id = child_version_id or uuid.uuid4().hex
        owner_manager = self._manager_for_node(owner_node)
        try:
            child_manifests = self._rebind_manifests(
                parent.manifests,
                refs=child_refs,
                state_id=child_state_id,
                version_id=child_version_id,
                snapshot_epoch=parent.snapshot_epoch,
                owner_node_id=owner_node,
                target_node_id=target_node,
                ttl_deadline=parent.ttl_deadline,
                data_manager=owner_manager,
                source_generation=(
                    parent.manifests[0].source_generation
                    if parent.manifests
                    else self.source_generation
                ),
                target_generation=(
                    parent.manifests[0].target_generation
                    if parent.manifests
                    else self.target_generation
                ),
                workflow_id=parent.workflow_id,
                reuse_captured_page_checksums=reuse_captured_page_checksums,
            )
            child = KVStateDescriptor(
                state_id=child_state_id,
                workflow_id=parent.workflow_id,
                version_id=child_version_id,
                parent_version_id=parent.version_id,
                snapshot_epoch=parent.snapshot_epoch,
                owner_node_id=owner_node,
                block_descriptors=tuple(KVBlockDescriptor.from_ref(ref) for ref in child_refs),
                feature_hashes=parent.feature_hashes,
                ttl_deadline=parent.ttl_deadline,
                metadata=child_meta,
                manifests=tuple(child_manifests),
            )
        except Exception:
            owner_manager.release_refs(child_refs)
            raise
        with self._lock:
            if child.state_id in self._states:
                owner_manager.release_refs(child_refs)
                raise ValueError(f"state already registered: {child.state_id}")
            self._states[child.state_id] = child
            self._state_refs[child.state_id] = child_refs
        self._acquire_leases(child_refs, owner_manager)
        for feature_hash in child.feature_hashes:
            if self.feature_store is not None and self.feature_store.has(feature_hash):
                self.feature_store.incref(feature_hash)
        return child


    def commit_state_version(
        self,
        previous_state_id: str,
        refs: Sequence[BlockRef],
        *,
        state_id: Optional[str] = None,
        version_id: Optional[str] = None,
        consumed_old_block_ids: Optional[Iterable[str]] = None,
        immutable_reused_block_ids: Optional[Iterable[str]] = None,
        metadata: Optional[Dict] = None,
        owner_node_id: Optional[str] = None,
        target_node_id: Optional[str] = None,
    ) -> KVStateDescriptor:
        """Replace a live state with an append-only child version.

        ``PagedKVManager.cow_page`` decrements the old shared page immediately.
        Passing that old GID in ``consumed_old_block_ids`` prevents this method
        from decrementing it a second time while retiring the previous
        descriptor. Reused refs are carried forward without incref because the
        logical state reference is transferred from the previous descriptor to
        the new descriptor.

        ``immutable_reused_block_ids`` is an explicit hot-path optimization
        for callers that know a carried-forward page was not written between
        the prior capture and this commit.  Such pages retain their captured
        checksum instead of forcing a GPU-to-host rehash.  The method accepts
        only IDs carried forward at the same logical page position and fails
        closed otherwise; callers must not mark a page immutable after writing
        to it.
        """

        with self._lock:
            previous = self._states.get(previous_state_id)
            previous_refs = list(self._state_refs.get(previous_state_id, []))
        if previous is None:
            raise KeyError(f"unknown state: {previous_state_id}")
        consumed = {str(item) for item in (consumed_old_block_ids or [])}
        immutable_reused = {
            str(item) for item in (immutable_reused_block_ids or []) if str(item)
        }
        previous_block_ids = {str(ref.global_block_id) for ref in previous_refs if ref.global_block_id}
        target_owner = str(owner_node_id or target_node_id or previous.owner_node_id)
        target_manager = self._manager_for_node(target_owner)
        new_refs: List[BlockRef] = []
        try:
            new_refs = [
                self._normalise_ref(ref, idx, page_manager=target_manager)
                for idx, ref in enumerate(refs)
            ]
            new_block_ids = {ref.global_block_id for ref in new_refs}
            new_meta = dict(previous.metadata)
            new_meta.update(metadata or {})
            next_state_id = state_id or uuid.uuid4().hex
            next_version_id = version_id or uuid.uuid4().hex
            manifests = self._rebind_manifests(
                previous.manifests,
                refs=new_refs,
                state_id=next_state_id,
                version_id=next_version_id,
                snapshot_epoch=previous.snapshot_epoch + 1,
                owner_node_id=target_owner,
                target_node_id=str(target_node_id or target_owner),
                ttl_deadline=previous.ttl_deadline,
                data_manager=target_manager,
                source_generation=(previous.manifests[0].source_generation if previous.manifests else self.source_generation),
                target_generation=(previous.manifests[0].target_generation if previous.manifests else self.target_generation),
                workflow_id=previous.workflow_id,
                immutable_reused_block_ids=immutable_reused,
            )
            descriptor = KVStateDescriptor(
                state_id=next_state_id,
                workflow_id=previous.workflow_id,
                version_id=next_version_id,
                parent_version_id=previous.version_id,
                snapshot_epoch=previous.snapshot_epoch + 1,
                owner_node_id=target_owner,
                block_descriptors=tuple(KVBlockDescriptor.from_ref(ref) for ref in new_refs),
                feature_hashes=previous.feature_hashes,
                ttl_deadline=previous.ttl_deadline,
                metadata=new_meta,
                manifests=tuple(manifests),
            )
            with self._lock:
                if self._states.get(previous_state_id) is not previous:
                    raise RuntimeError(f"state changed while committing version: {previous_state_id}")
                # ``state_id`` is the stable logical serving handle whereas
                # ``version_id`` identifies its immutable snapshot.  A CoW
                # commit may intentionally retain the same handle so callers
                # (and AgentStateCloner's branch mapping) continue to resolve
                # the latest version.  It must still reject collisions with a
                # *different* live state.
                if (
                    descriptor.state_id in self._states
                    and descriptor.state_id != previous_state_id
                ):
                    raise ValueError(f"state already registered: {descriptor.state_id}")
                self._states.pop(previous_state_id)
                self._state_refs.pop(previous_state_id)
                self._states[descriptor.state_id] = descriptor
                self._state_refs[descriptor.state_id] = new_refs
        except Exception:
            # Only pages introduced by the failed version are ours to release;
            # reused refs still belong to the live previous descriptor.
            newly_owned = [ref for ref in new_refs if str(ref.global_block_id) not in previous_block_ids]
            if newly_owned:
                target_manager.release_refs(newly_owned)
            raise
        old_manager = self._manager_for_node(previous.owner_node_id)
        for old_ref in previous_refs:
            old_gid = old_ref.global_block_id
            if old_gid in consumed or old_gid in new_block_ids:
                continue
            old_manager.release_refs([old_ref])
        self._release_leases(previous_refs, old_manager)
        self._acquire_leases(new_refs, target_manager)
        return descriptor

    def get_state(self, state_id: str) -> Optional[KVStateDescriptor]:
        with self._lock:
            return self._states.get(state_id)

    def get_refs(self, state_id: str) -> List[BlockRef]:
        with self._lock:
            if state_id not in self._state_refs:
                raise KeyError(f"unknown state: {state_id}")
            return list(self._state_refs[state_id])

    def resolve_remote_refs(
        self,
        state_id: str,
        *,
        target_node_id: Optional[str] = None,
        for_write: bool = False,
    ) -> List[BlockRef]:
        """Resolve descriptor-shared refs for a target execution node.

        Descriptor-share clones intentionally keep the authoritative owner page
        refs instead of copying KV during Agent fork.  This method makes that
        boundary explicit for serving/runtime code:

        - read-only consumers receive the owner refs plus metadata validation;
        - writers must call ``materialize_for_write`` first, or pass
          ``for_write=True`` to fail fast before mutating a remote-owned page.
        """

        with self._lock:
            descriptor = self._states.get(state_id)
            refs = list(self._state_refs.get(state_id, []))
        if descriptor is None:
            raise KeyError(f"unknown state: {state_id}")
        target = str(target_node_id or descriptor.metadata.get("target_node_id") or self.node_id)
        meta_target = descriptor.metadata.get("target_node_id")
        if meta_target is not None and str(meta_target) != target:
            raise ValueError(
                f"state={state_id} is shared for target={meta_target}, not target={target}"
            )
        remote_shared = bool(descriptor.metadata.get("remote_descriptor_shared"))
        if for_write and remote_shared and descriptor.owner_node_id != target:
            raise RuntimeError(
                "remote descriptor-shared KV state must be materialized before write: "
                f"state={state_id} owner={descriptor.owner_node_id} target={target}"
            )
        return refs

    def materialize_for_write(
        self,
        state_id: str,
        *,
        target_node_id: Optional[str] = None,
        version_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> KVStateDescriptor:
        """Install a descriptor-shared remote clone on the target node.

        The method uses the configured ``remote_materializer`` (TransferEngine +
        target PagedKVManager) to create target-local pages, then releases the
        old owner refs that were held by the descriptor-share clone.  After it
        returns, ``release_state`` and subsequent CoW/write operations use the
        target page manager, not the original owner manager.
        """

        with self._lock:
            descriptor = self._states.get(state_id)
            old_refs = list(self._state_refs.get(state_id, []))
        if descriptor is None:
            raise KeyError(f"unknown state: {state_id}")
        target = str(target_node_id or descriptor.metadata.get("target_node_id") or self.node_id)
        if descriptor.owner_node_id == target and not descriptor.metadata.get("remote_descriptor_shared"):
            return descriptor
        target_pm = self._page_managers.get(target)
        if target_pm is None:
            raise RuntimeError(f"no page manager registered for target node: {target}")
        if self.kv_page_store is not None and descriptor.manifests and all(
            manifest.has_store_payload for manifest in descriptor.manifests
        ):
            materialized_refs = list(self.kv_page_store.materialize_pages(target_pm, descriptor.manifests))
        elif self.remote_materializer is not None:
            materialized_refs = list(self.remote_materializer(descriptor, target))
        else:
            raise NotImplementedError(
                "materialize_for_write requires a real Mooncake remote_materializer "
                "or Store-backed page manifests"
            )
        if not materialized_refs:
            raise RuntimeError("remote_materializer returned no KV refs")
        try:
            new_refs = [
                self._normalise_ref(ref, idx, page_manager=target_pm)
                for idx, ref in enumerate(materialized_refs)
            ]
        except Exception:
            target_pm.release_refs(materialized_refs)
            raise
        owner_pm = self._manager_for_node(descriptor.owner_node_id)
        new_meta = dict(descriptor.metadata)
        new_meta.pop("remote_descriptor_shared", None)
        new_meta.pop("requires_materialize_before_write", None)
        new_meta["materialized_from_owner_node_id"] = descriptor.owner_node_id
        new_meta["materialized_target_node_id"] = target
        new_meta["materialized_at"] = time.monotonic()
        new_meta.update(metadata or {})
        new_version_id = version_id or uuid.uuid4().hex
        try:
            manifests = self._rebind_manifests(
                descriptor.manifests,
                refs=new_refs,
                state_id=descriptor.state_id,
                version_id=new_version_id,
                snapshot_epoch=descriptor.snapshot_epoch + 1,
                owner_node_id=target,
                target_node_id=target,
                ttl_deadline=descriptor.ttl_deadline,
                data_manager=target_pm,
                source_generation=(descriptor.manifests[0].source_generation if descriptor.manifests else self.source_generation),
                target_generation=(descriptor.manifests[0].target_generation if descriptor.manifests else self.target_generation),
                workflow_id=descriptor.workflow_id,
            )
        except Exception:
            target_pm.release_refs(new_refs)
            raise
        new_descriptor = KVStateDescriptor(
            state_id=descriptor.state_id,
            workflow_id=descriptor.workflow_id,
            version_id=new_version_id,
            parent_version_id=descriptor.version_id,
            snapshot_epoch=descriptor.snapshot_epoch + 1,
            owner_node_id=target,
            block_descriptors=tuple(KVBlockDescriptor.from_ref(ref) for ref in new_refs),
            feature_hashes=descriptor.feature_hashes,
            ttl_deadline=descriptor.ttl_deadline,
            metadata=new_meta,
            manifests=tuple(manifests),
        )
        with self._lock:
            current = self._states.get(state_id)
            if current is not descriptor:
                target_pm.release_refs(new_refs)
                raise RuntimeError(f"state changed while materializing: {state_id}")
            self._states[state_id] = new_descriptor
            self._state_refs[state_id] = new_refs
        owner_pm.release_refs(old_refs)
        self._release_leases(old_refs, owner_pm)
        self._acquire_leases(new_refs, target_pm)
        return new_descriptor

    def restore_state_from_store(
        self,
        state_id: str,
        manifests: Sequence[KVStateManifest | Mapping[str, Any]],
        *,
        target_node_id: Optional[str] = None,
        version_id: Optional[str] = None,
        ttl_deadline: Optional[float] = None,
        metadata: Optional[Dict] = None,
        feature_hashes: Optional[Iterable[str]] = None,
    ) -> KVStateDescriptor:
        """Rehydrate a catalogued Store-backed state after process restart.

        This is an explicit materialization, not a pointer resurrection: the
        target ``PagedKVManager`` allocates fresh block ids, ``batch_get_into``
        imports immutable page objects, and checksums are validated by
        ``MooncakeKVPageStore`` before the descriptor becomes visible.  The
        method exists so a WAL-recovered Agent catalog can resume a branch
        without ever treating an old physical id as valid on a new process.
        """

        if self.kv_page_store is None:
            raise RuntimeError("restore_state_from_store requires a MooncakeKVPageStore")
        state_id = str(state_id or "").strip()
        if not state_id:
            raise ValueError("state_id is required")
        normalized: List[KVStateManifest] = []
        for raw in manifests:
            manifest = (
                raw
                if isinstance(raw, KVStateManifest)
                else KVStateManifest.from_control_payload(raw)
            )
            manifest.validate(allow_expired=False)
            if manifest.manifest_checksum() != manifest.checksum:
                raise KVManifestError(
                    f"manifest checksum mismatch during restore: {manifest.transfer_key}"
                )
            if manifest.state_id != state_id:
                raise KVManifestError(
                    f"restore state id mismatch: {manifest.state_id} != {state_id}"
                )
            if not manifest.has_store_payload:
                raise RuntimeError(
                    f"manifest {manifest.transfer_key} has no Store payload for recovery"
                )
            normalized.append(manifest)
        if not normalized:
            raise ValueError("restore_state_from_store requires at least one manifest")
        workflow_id = normalized[0].workflow_id
        if any(manifest.workflow_id != workflow_id for manifest in normalized):
            raise KVManifestError("all restored manifests must belong to one workflow")
        target = str(target_node_id or self.node_id)
        target_pm = self._manager_for_node(target)
        with self._lock:
            if state_id in self._states or state_id in self._pending_state_ids:
                raise ValueError(f"state already registered: {state_id}")
            self._pending_state_ids.add(state_id)
        refs: List[BlockRef] = []
        try:
            refs = list(self.kv_page_store.materialize_pages(target_pm, normalized))
            refs = [
                self._normalise_ref(ref, index, page_manager=target_pm)
                for index, ref in enumerate(refs)
            ]
            source = normalized[0]
            effective_deadline = (
                float(source.lease_deadline)
                if ttl_deadline is None
                else float(ttl_deadline)
            )
            restored_version = version_id or uuid.uuid4().hex
            rebound = self._rebind_manifests(
                normalized,
                refs=refs,
                state_id=state_id,
                version_id=restored_version,
                snapshot_epoch=max(manifest.snapshot_epoch for manifest in normalized) + 1,
                owner_node_id=target,
                target_node_id=target,
                ttl_deadline=effective_deadline,
                data_manager=target_pm,
                source_generation=source.source_generation,
                target_generation=source.target_generation,
                workflow_id=workflow_id,
            )
            descriptor = KVStateDescriptor(
                state_id=state_id,
                workflow_id=workflow_id,
                version_id=restored_version,
                parent_version_id=source.version_id,
                snapshot_epoch=max(manifest.snapshot_epoch for manifest in normalized) + 1,
                owner_node_id=target,
                block_descriptors=tuple(KVBlockDescriptor.from_ref(ref) for ref in refs),
                feature_hashes=tuple(
                    dict.fromkeys(str(item) for item in (feature_hashes or ()))
                ),
                ttl_deadline=effective_deadline,
                metadata={
                    "restored_from_store": True,
                    "restored_source_owner_node_id": source.owner_node_id,
                    **dict(metadata or {}),
                },
                manifests=tuple(rebound),
            )
            with self._lock:
                if state_id not in self._pending_state_ids or state_id in self._states:
                    raise RuntimeError(f"state reservation was lost during restore: {state_id}")
                self._states[state_id] = descriptor
                self._state_refs[state_id] = refs
                self._pending_state_ids.discard(state_id)
        except Exception:
            with self._lock:
                self._pending_state_ids.discard(state_id)
            if refs:
                target_pm.release_refs(refs)
            raise
        self._acquire_leases(refs, target_pm)
        for feature_hash in descriptor.feature_hashes:
            if self.feature_store is not None and self.feature_store.has(feature_hash):
                self.feature_store.incref(feature_hash)
        return descriptor

    def sweep_expired_states(self, *, now: Optional[float] = None) -> int:
        """Release expired manifest leases and their page references.

        The state catalog invokes this from its periodic TTL path.  It is safe
        to call repeatedly: entries removed by a prior sweep are simply absent.
        """

        now_f = time.monotonic() if now is None else float(now)
        with self._lock:
            expired = [
                state_id
                for state_id, descriptor in self._states.items()
                if descriptor.ttl_deadline != float("inf")
                and float(descriptor.ttl_deadline) <= now_f
            ]
        for state_id in expired:
            self.release_state(state_id)
        return len(expired)

    def release_state(self, state_id: str) -> int:
        with self._lock:
            descriptor = self._states.pop(state_id, None)
            refs = self._state_refs.pop(state_id, None)
        if descriptor is None or refs is None:
            return 0
        manager = self._manager_for_node(descriptor.owner_node_id)
        tracked_gids = {
            str(ref.global_block_id)
            for ref in refs
            if ref.global_block_id and manager.kv_directory.get_record(ref.global_block_id) is not None
        }
        manager.release_refs(refs)
        self._release_leases(refs, manager)
        freed = sum(
            1
            for gid in tracked_gids
            if manager.kv_directory.get_record(gid) is None
        )
        for feature_hash in descriptor.feature_hashes:
            if self.feature_store is not None:
                self.feature_store.release(feature_hash)
        return freed

    def active_state_ids(self) -> List[str]:
        with self._lock:
            return list(self._states)

    def stats(self) -> Dict[str, int]:
        with self._lock:
            states = list(self._states.values())
        return {
            "states": len(states),
            "blocks": sum(len(state.block_descriptors) for state in states),
            "tokens": sum(state.num_kv_tokens for state in states),
        }

    @staticmethod
    def _acquire_leases(refs: Sequence[BlockRef], manager: PagedKVManager) -> None:
        for ref in refs:
            manager.kv_directory.lease(ref.global_block_id)

    @staticmethod
    def _release_leases(refs: Sequence[BlockRef], manager: PagedKVManager) -> None:
        for ref in refs:
            manager.release_lease(ref.global_block_id)

    def _build_manifests(
        self,
        refs: Sequence[BlockRef],
        *,
        workflow_id: str,
        state_id: str,
        version_id: str,
        snapshot_epoch: int,
        owner_node_id: str,
        target_node_id: Optional[str],
        ttl_deadline: float,
        model_id: str,
        model_revision: str,
        tp_rank: int,
        tp_size: int,
        source_generation: str,
        target_generation: str,
        data_manager: Optional[PagedKVManager] = None,
    ) -> List[KVStateManifest]:
        """Create content-bound manifests at an explicit state boundary."""

        manager = data_manager or self._manager_for_node(owner_node_id)
        result: List[KVStateManifest] = []
        token_cursor = 0
        for index, ref in enumerate(refs):
            filled = int(ref.filled)
            if filled <= 0:
                continue
            payload: Dict[str, object] = {
                "model_id": str(model_id),
                "model_revision": str(model_revision),
                "workflow_id": str(workflow_id),
                "state_id": str(state_id),
                "version_id": str(version_id),
                "snapshot_epoch": int(snapshot_epoch),
                "token_start": token_cursor,
                "token_end": token_cursor + filled,
                "layer_group": 0,
                "layer_start": 0,
                "layer_end": int(manager.num_layers),
                "tp_rank": int(tp_rank),
                "tp_size": int(tp_size),
                "owner_node_id": str(owner_node_id),
                "target_node_id": target_node_id,
                "block_id": str(ref.global_block_id),
                "checksum": "",
                "lease_id": f"{state_id}:lease:{index}",
                "lease_deadline": float(ttl_deadline),
                "source_generation": str(source_generation),
                "target_generation": str(target_generation),
                "dtype": str(manager.dtype),
                "page_size": int(manager.page_size),
                "num_kv_heads": int(manager.num_kv_heads),
                "head_dim": int(manager.head_dim),
                "data_checksum": manager.page_checksum(ref),
                # Include optional Store fields before calculating the
                # manifest checksum.  Dataclass construction supplies these
                # defaults too; omitting them here made freshly built
                # non-Store manifests fail their own canonical checksum.
                "key_store_key": None,
                "value_store_key": None,
                "key_nbytes": None,
                "value_nbytes": None,
            }
            payload["checksum"] = manifest_checksum(payload)
            result.append(KVStateManifest(**payload))  # type: ignore[arg-type]
            token_cursor += filled
        return result

    def _rebind_manifests(
        self,
        previous: Sequence[KVStateManifest],
        *,
        refs: Sequence[BlockRef],
        state_id: str,
        version_id: str,
        snapshot_epoch: int,
        owner_node_id: str,
        target_node_id: Optional[str],
        ttl_deadline: float,
        data_manager: PagedKVManager,
        source_generation: str,
        target_generation: str,
        workflow_id: Optional[str] = None,
        reuse_captured_page_checksums: bool = False,
        immutable_reused_block_ids: Optional[Iterable[str]] = None,
    ) -> List[KVStateManifest]:
        nonempty_refs = [ref for ref in refs if int(ref.filled) > 0]
        immutable_reused = {
            str(item) for item in (immutable_reused_block_ids or []) if str(item)
        }
        if len(previous) != len(nonempty_refs):
            if reuse_captured_page_checksums:
                raise RuntimeError(
                    "descriptor-share clone changed its nonempty KV-page layout; "
                    "cannot reuse captured page checksums"
                )
            seed = previous[0] if previous else None
            return self._build_manifests(
                refs,
                workflow_id=workflow_id or (seed.workflow_id if seed else "unknown"),
                state_id=state_id,
                version_id=version_id,
                snapshot_epoch=snapshot_epoch,
                owner_node_id=owner_node_id,
                target_node_id=target_node_id,
                ttl_deadline=ttl_deadline,
                model_id=seed.model_id if seed else self.model_id,
                model_revision=seed.model_revision if seed else self.model_revision,
                tp_rank=seed.tp_rank if seed else self.tp_rank,
                tp_size=seed.tp_size if seed else self.tp_size,
                source_generation=source_generation,
                target_generation=target_generation,
                data_manager=data_manager,
            )

        result: List[KVStateManifest] = []
        for index, (old, ref) in enumerate(zip(previous, nonempty_refs)):
            block_id = str(ref.global_block_id)
            if reuse_captured_page_checksums:
                # ``fork_refs`` only increments refcounts; the GID identifies
                # the same immutable physical page.  Fail closed if a caller
                # ever tries to opt in for materialized or otherwise changed
                # data, rather than silently accepting a stale checksum.
                if str(old.block_id) != block_id:
                    raise RuntimeError(
                        "descriptor-share clone changed a KV page identity; "
                        "cannot reuse captured page checksums"
                    )
                if not old.data_checksum:
                    raise RuntimeError(
                        "descriptor-share clone has no captured KV page checksum"
                    )
                preserve_store_payload = old.has_store_payload
                data_checksum = old.data_checksum
            elif block_id in immutable_reused:
                # Commit-time reuse is deliberately opt-in: a private page can
                # be mutated in place, unlike a forked descriptor-share page.
                # Verify identity and a prior checksum before skipping the
                # expensive content audit.
                if str(old.block_id) != block_id:
                    raise RuntimeError(
                        "immutable reused KV page changed identity; "
                        "cannot reuse captured page checksum"
                    )
                if not old.data_checksum:
                    raise RuntimeError(
                        "immutable reused KV page has no captured checksum"
                    )
                immutable_reused.remove(block_id)
                preserve_store_payload = old.has_store_payload
                data_checksum = old.data_checksum
            else:
                if old.has_store_payload:
                    # Store-backed payloads encode full fixed-size pages, so
                    # retain Store object references only after validating the
                    # full page is unchanged.
                    full_checksum = data_manager.page_checksum(ref, filled_only=False)
                    preserve_store_payload = old.data_checksum == full_checksum
                    data_checksum = (
                        full_checksum
                        if preserve_store_payload
                        else data_manager.page_checksum(ref)
                    )
                else:
                    # No Store object can be retained, so an additional full
                    # page checksum is pure overhead.  Capture the normal
                    # logical filled-page checksum exactly once.
                    preserve_store_payload = False
                    data_checksum = data_manager.page_checksum(ref)
            payload = {
                **old.canonical_payload(),
                "state_id": state_id,
                "version_id": version_id,
                "snapshot_epoch": int(snapshot_epoch),
                "owner_node_id": owner_node_id,
                "target_node_id": target_node_id,
                "block_id": str(ref.global_block_id),
                "lease_id": f"{state_id}:lease:{index}",
                "lease_deadline": float(ttl_deadline),
                "source_generation": source_generation,
                "target_generation": target_generation,
                "data_checksum": data_checksum,
                "checksum": "",
            }
            if not preserve_store_payload:
                payload.update(
                    {
                        "key_store_key": None,
                        "value_store_key": None,
                        "key_nbytes": None,
                        "value_nbytes": None,
                    }
                )
            payload["checksum"] = manifest_checksum(payload)
            result.append(KVStateManifest(**payload))  # type: ignore[arg-type]
        if immutable_reused:
            raise RuntimeError(
                "immutable reused KV block ids were not carried forward unchanged: "
                + ", ".join(sorted(immutable_reused))
            )
        return result

    def _manager_for_node(self, node_id: str) -> PagedKVManager:
        manager = self._page_managers.get(str(node_id))
        if manager is None:
            raise KeyError(f"no PagedKVManager registered for node: {node_id}")
        return manager

    def _normalise_ref(
        self,
        ref: BlockRef,
        logical_index: int,
        *,
        page_manager: PagedKVManager,
    ) -> BlockRef:
        gid = ref.global_block_id or page_manager.kv_directory.gid_for_physical_id(ref.physical_id)
        if not gid:
            raise KeyError(f"block ref has no directory GID: physical_id={ref.physical_id}")
        record = page_manager.kv_directory.get_record(gid)
        if record is None:
            raise KeyError(f"block GID is missing from KV directory: {gid}")
        return BlockRef(
            physical_id=int(ref.physical_id),
            filled=int(ref.filled),
            global_block_id=str(gid),
            physical_node_id=str(ref.physical_node_id or record.physical_node_id),
            logical_index=int(logical_index),
            virtual_offset=int(ref.virtual_offset),
        )


class MooncakeRemoteKVMaterializer:
    """Real cross-node KV materializer over TransferEngine + PagedKVManager.

    It reads authoritative pages from the source manager, transfers every K/V
    tensor through the configured TransferEngine, and installs new physical
    pages in the target manager.  The returned refs are valid only in the target
    manager's KV directory; MooncakeKVStateStore therefore tracks page managers
    per owner node and releases remote clones against the correct directory.
    """

    def __init__(
        self,
        source_page_manager: PagedKVManager,
        target_page_manager: PagedKVManager,
        *,
        transfer_engine: Optional[TransferEngine] = None,
        policy: Optional[TransferPolicy] = None,
    ):
        self.source_pm = source_page_manager
        self.target_pm = target_page_manager
        self.transfer = transfer_engine or TransferEngine(protocol="local")
        self.policy = policy or TransferPolicy(
            mode=Mode.PULL,
            channel=Channel.AGENT_TO_AGENT,
            extra={"force_copy": True},
        )

    @property
    def target_node_id(self) -> str:
        return self.target_pm.node_id

    def __call__(self, descriptor: KVStateDescriptor, target_node_id: str) -> Sequence[BlockRef]:
        if str(target_node_id) != str(self.target_pm.node_id):
            raise ValueError(
                f"materializer target mismatch: requested={target_node_id}, configured={self.target_pm.node_id}"
            )
        refs: List[BlockRef] = []
        try:
            for logical_index, block in enumerate(descriptor.block_descriptors):
                src_ref = block.to_ref()
                key, value = self.source_pm.get_page(src_ref)
                moved_key = self.transfer.transfer_tensor(key, self.target_pm.device, self.policy)
                moved_value = self.transfer.transfer_tensor(value, self.target_pm.device, self.policy)
                target_ref = self.target_pm.allocate_page(filled=src_ref.filled)
                refs.append(target_ref)
                self.target_pm.write_page_slots(target_ref, moved_key, moved_value, offset=0)
                refs[-1] = BlockRef(
                    physical_id=target_ref.physical_id,
                    filled=target_ref.filled,
                    global_block_id=target_ref.global_block_id,
                    physical_node_id=self.target_pm.node_id,
                    logical_index=logical_index,
                    virtual_offset=src_ref.virtual_offset,
                )
            return refs
        except Exception:
            if refs:
                self.target_pm.release_refs(refs)
            raise
