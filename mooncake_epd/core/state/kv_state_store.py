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
from typing import Callable, Dict, Iterable, List, Optional, Sequence

import torch

from .feature_store import FeatureStore
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
    ):
        self.pm = page_manager
        self.feature_store = feature_store
        self.node_id = str(node_id or page_manager.node_id)
        self.remote_materializer = remote_materializer
        self.allow_remote_descriptor_sharing = bool(allow_remote_descriptor_sharing)
        self._page_managers: Dict[str, PagedKVManager] = {self.node_id: self.pm}
        for key, manager in dict(page_managers_by_node or {}).items():
            self._page_managers[str(key)] = manager
        self._states: Dict[str, KVStateDescriptor] = {}
        self._state_refs: Dict[str, List[BlockRef]] = {}
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
    ) -> KVStateDescriptor:
        indexed_refs = [self._normalise_ref(ref, idx, page_manager=self.pm) for idx, ref in enumerate(refs)]
        descriptor = KVStateDescriptor(
            state_id=state_id or uuid.uuid4().hex,
            workflow_id=str(workflow_id),
            version_id=version_id or uuid.uuid4().hex,
            parent_version_id=parent_version_id,
            snapshot_epoch=int(snapshot_epoch),
            owner_node_id=self.node_id,
            block_descriptors=tuple(KVBlockDescriptor.from_ref(ref) for ref in indexed_refs),
            feature_hashes=tuple(dict.fromkeys(str(item) for item in (feature_hashes or []))),
            ttl_deadline=ttl_deadline,
            metadata=dict(metadata or {}),
        )
        with self._lock:
            if descriptor.state_id in self._states:
                raise ValueError(f"state already registered: {descriptor.state_id}")
            self._states[descriptor.state_id] = descriptor
            self._state_refs[descriptor.state_id] = indexed_refs
        for feature_hash in descriptor.feature_hashes:
            if self.feature_store is not None and self.feature_store.has(feature_hash):
                self.feature_store.incref(feature_hash)
        return descriptor

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
        target_node = str(target_node_id or self.node_id)
        if target_node != self.node_id:
            use_descriptor_share = (
                self.allow_remote_descriptor_sharing
                if share_remote_descriptor is None
                else bool(share_remote_descriptor)
            )
            if use_descriptor_share:
                # Cross-node "zero-copy clone" means the child descriptor points
                # at the source owner blocks and increments the owner refcounts.
                # The target node must materialize/CoW explicitly before writing.
                child_refs = self.pm.fork_refs(parent_refs)
                owner_node = parent.owner_node_id
            elif self.remote_materializer is None:
                raise NotImplementedError(
                    "cross-node KVState clone requires a real Mooncake remote_materializer"
                )
            else:
                materialized_refs = list(self.remote_materializer(parent, target_node))
                if not materialized_refs:
                    raise RuntimeError("remote_materializer returned no KV refs")
                target_pm = self._page_managers.get(target_node)
                if target_pm is None:
                    raise RuntimeError(
                        f"remote materializer installed refs for node={target_node}, but no page manager was registered"
                    )
                child_refs = [
                    self._normalise_ref(ref, idx, page_manager=target_pm)
                    for idx, ref in enumerate(materialized_refs)
                ]
                owner_node = target_node
        else:
            child_refs = self.pm.fork_refs(parent_refs)
            owner_node = self.node_id

        child_meta = dict(parent.metadata)
        child_meta.update(metadata or {})
        if target_node != self.node_id and owner_node != target_node:
            child_meta.update(
                {
                    "remote_descriptor_shared": True,
                    "target_node_id": target_node,
                    "owner_node_id": owner_node,
                    "requires_materialize_before_write": True,
                }
            )
        child = KVStateDescriptor(
            state_id=child_state_id or uuid.uuid4().hex,
            workflow_id=parent.workflow_id,
            version_id=child_version_id or uuid.uuid4().hex,
            parent_version_id=parent.version_id,
            snapshot_epoch=parent.snapshot_epoch,
            owner_node_id=owner_node,
            block_descriptors=tuple(KVBlockDescriptor.from_ref(ref) for ref in child_refs),
            feature_hashes=parent.feature_hashes,
            ttl_deadline=parent.ttl_deadline,
            metadata=child_meta,
        )
        with self._lock:
            if child.state_id in self._states:
                self._manager_for_node(owner_node).release_refs(child_refs)
                raise ValueError(f"state already registered: {child.state_id}")
            self._states[child.state_id] = child
            self._state_refs[child.state_id] = child_refs
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
        metadata: Optional[Dict] = None,
    ) -> KVStateDescriptor:
        """Replace a live state with an append-only child version.

        ``PagedKVManager.cow_page`` decrements the old shared page immediately.
        Passing that old GID in ``consumed_old_block_ids`` prevents this method
        from decrementing it a second time while retiring the previous
        descriptor. Reused refs are carried forward without incref because the
        logical state reference is transferred from the previous descriptor to
        the new descriptor.
        """

        with self._lock:
            previous = self._states.pop(previous_state_id)
            previous_refs = self._state_refs.pop(previous_state_id)
        consumed = {str(item) for item in (consumed_old_block_ids or [])}
        new_refs = [self._normalise_ref(ref, idx, page_manager=self.pm) for idx, ref in enumerate(refs)]
        new_block_ids = {ref.global_block_id for ref in new_refs}
        for old_ref in previous_refs:
            old_gid = old_ref.global_block_id
            if old_gid in consumed or old_gid in new_block_ids:
                continue
            self.pm.release_refs([old_ref])
        new_meta = dict(previous.metadata)
        new_meta.update(metadata or {})
        descriptor = KVStateDescriptor(
            state_id=state_id or uuid.uuid4().hex,
            workflow_id=previous.workflow_id,
            version_id=version_id or uuid.uuid4().hex,
            parent_version_id=previous.version_id,
            snapshot_epoch=previous.snapshot_epoch + 1,
            owner_node_id=self.node_id,
            block_descriptors=tuple(KVBlockDescriptor.from_ref(ref) for ref in new_refs),
            feature_hashes=previous.feature_hashes,
            ttl_deadline=previous.ttl_deadline,
            metadata=new_meta,
        )
        with self._lock:
            if descriptor.state_id in self._states:
                raise ValueError(f"state already registered: {descriptor.state_id}")
            self._states[descriptor.state_id] = descriptor
            self._state_refs[descriptor.state_id] = new_refs
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
        if self.remote_materializer is None:
            raise NotImplementedError(
                "materialize_for_write requires a real Mooncake remote_materializer"
            )
        target_pm = self._page_managers.get(target)
        if target_pm is None:
            raise RuntimeError(f"no page manager registered for target node: {target}")

        materialized_refs = list(self.remote_materializer(descriptor, target))
        if not materialized_refs:
            raise RuntimeError("remote_materializer returned no KV refs")
        new_refs = [
            self._normalise_ref(ref, idx, page_manager=target_pm)
            for idx, ref in enumerate(materialized_refs)
        ]
        owner_pm = self._manager_for_node(descriptor.owner_node_id)
        new_meta = dict(descriptor.metadata)
        new_meta.pop("remote_descriptor_shared", None)
        new_meta.pop("requires_materialize_before_write", None)
        new_meta["materialized_from_owner_node_id"] = descriptor.owner_node_id
        new_meta["materialized_target_node_id"] = target
        new_meta["materialized_at"] = time.monotonic()
        new_meta.update(metadata or {})
        new_descriptor = KVStateDescriptor(
            state_id=descriptor.state_id,
            workflow_id=descriptor.workflow_id,
            version_id=version_id or uuid.uuid4().hex,
            parent_version_id=descriptor.version_id,
            snapshot_epoch=descriptor.snapshot_epoch + 1,
            owner_node_id=target,
            block_descriptors=tuple(KVBlockDescriptor.from_ref(ref) for ref in new_refs),
            feature_hashes=descriptor.feature_hashes,
            ttl_deadline=descriptor.ttl_deadline,
            metadata=new_meta,
        )
        with self._lock:
            current = self._states.get(state_id)
            if current is not descriptor:
                target_pm.release_refs(new_refs)
                raise RuntimeError(f"state changed while materializing: {state_id}")
            self._states[state_id] = new_descriptor
            self._state_refs[state_id] = new_refs
        owner_pm.release_refs(old_refs)
        return new_descriptor

    def release_state(self, state_id: str) -> int:
        with self._lock:
            descriptor = self._states.pop(state_id, None)
            refs = self._state_refs.pop(state_id, None)
        if descriptor is None or refs is None:
            return 0
        freed = self._manager_for_node(descriptor.owner_node_id).release_refs(refs)
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
        for logical_index, block in enumerate(descriptor.block_descriptors):
            src_ref = block.to_ref()
            key, value = self.source_pm.get_page(src_ref)
            moved_key = self.transfer.transfer_tensor(key, self.target_pm.device, self.policy)
            moved_value = self.transfer.transfer_tensor(value, self.target_pm.device, self.policy)
            target_ref = self.target_pm.allocate_page(filled=src_ref.filled)
            self.target_pm.write_page_slots(target_ref, moved_key, moved_value, offset=0)
            refs.append(
                BlockRef(
                    physical_id=target_ref.physical_id,
                    filled=target_ref.filled,
                    global_block_id=target_ref.global_block_id,
                    physical_node_id=self.target_pm.node_id,
                    logical_index=logical_index,
                    virtual_offset=src_ref.virtual_offset,
                )
            )
        return refs
