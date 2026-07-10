"""MultimodalState: versioned state object plus lifecycle operations."""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from ..control.write_ahead_log import JsonLineWAL, merge_wal_rows
from .feature_store import FeatureStore
from .page_manager import BlockRef, PagedKVManager
from .radix_tree import RadixTree
from .workflow_registry import (
    WorkflowStateRecord,
    WorkflowStateRegistry,
    normalize_reuse_telemetry,
)


@dataclass
class StateMeta:
    token_ids: List[int] = field(default_factory=list)
    image_ids: List[str] = field(default_factory=list)
    agent_id: str = ""
    workflow_id: str = ""
    step: int = 0
    turn: int = 0
    ttl_deadline: float = float("inf")
    priority: int = 0


@dataclass
class MultimodalState:
    state_id: str
    kv_refs: List[BlockRef] = field(default_factory=list)
    feature_hash: Optional[str] = None
    feature_hashes: List[str] = field(default_factory=list)
    reuse_telemetry: Dict[str, Any] = field(default_factory=dict)
    meta: StateMeta = field(default_factory=StateMeta)
    workflow_id: str = ""
    step_index: int = 0
    version_id: str = ""
    parent_version_id: Optional[str] = None
    snapshot_epoch: int = 0
    approximate: bool = False
    status: str = "ACTIVE"
    ttl_deadline: float = float("inf")

    def __post_init__(self) -> None:
        if not self.workflow_id:
            self.workflow_id = self.meta.workflow_id
        if self.step_index == 0 and self.meta.step:
            self.step_index = self.meta.step
        if not self.version_id:
            self.version_id = self.state_id
        if self.ttl_deadline == float("inf"):
            self.ttl_deadline = self.meta.ttl_deadline
        self.meta.workflow_id = self.workflow_id
        self.meta.step = self.step_index
        self.meta.ttl_deadline = self.ttl_deadline
        if self.feature_hash and self.feature_hash not in self.feature_hashes:
            self.feature_hashes = [self.feature_hash] + list(self.feature_hashes)
        # Preserve insertion order while deduplicating feature hashes.
        self.feature_hashes = list(dict.fromkeys(self.feature_hashes))
        self.reuse_telemetry = normalize_reuse_telemetry(self.reuse_telemetry)

    @property
    def num_kv_tokens(self) -> int:
        return sum(r.filled for r in self.kv_refs)


@dataclass
class HandoffTransaction:
    handoff_id: str
    state_id: str
    workflow_id: str
    source_agent_id: str
    target_agent_id: str
    source_node_id: str
    target_node_id: str
    staged_refs: List[BlockRef] = field(default_factory=list)
    staged_feature_hashes: List[str] = field(default_factory=list)
    prepared_at: float = field(default_factory=time.monotonic)
    status: str = "PREPARED"


class StateLayer:
    """Lifecycle operations on versioned ``MultimodalState`` objects."""

    _MUTATION_BLOCKED_STATUSES = {"OFFLOADING", "RESTORING", "HANDING_OVER"}

    def __init__(
        self,
        page_manager: PagedKVManager,
        radix_tree: RadixTree,
        feature_store: FeatureStore,
        default_ttl_seconds: float = 3600.0,
        *,
        handoff_wal_path: Optional[str] = None,
        recover_handoffs_on_start: bool = False,
        workflow_registry: Optional[WorkflowStateRegistry] = None,
    ):
        self.pm = page_manager
        self.radix = radix_tree
        self.features = feature_store
        self.default_ttl_seconds = default_ttl_seconds
        self._lock = threading.RLock()
        self._states: Dict[str, MultimodalState] = {}
        self._workflow_chain: Dict[str, List[str]] = {}
        self._workflow_epoch: Dict[str, int] = {}
        self._workflow_snapshot_index: Dict[str, Dict[int, List[str]]] = {}
        self._workflow_version_index: Dict[str, Dict[str, str]] = {}
        self._handoff_pending: Dict[str, HandoffTransaction] = {}
        self._reaper_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._handoff_wal = JsonLineWAL(handoff_wal_path) if handoff_wal_path else None
        self.workflow_registry = workflow_registry
        if recover_handoffs_on_start:
            self.recover_handoffs()

    def register(
        self,
        kv_refs: Sequence[BlockRef],
        feature_hash: Optional[str],
        meta: StateMeta,
        register_in_radix: bool = True,
        *,
        version_id: Optional[str] = None,
        parent_version_id: Optional[str] = None,
        snapshot_epoch: Optional[int] = None,
        approximate: bool = False,
        status: str = "ACTIVE",
        reuse_telemetry: Optional[Dict[str, Any]] = None,
    ) -> MultimodalState:
        workflow_id = meta.workflow_id
        if meta.ttl_deadline == float("inf"):
            meta.ttl_deadline = time.monotonic() + self.default_ttl_seconds
        if snapshot_epoch is None:
            snapshot_epoch = self._workflow_epoch.get(workflow_id, -1) + 1

        indexed_refs: List[BlockRef] = []
        for idx, ref in enumerate(kv_refs):
            indexed_refs.append(BlockRef(
                physical_id=ref.physical_id,
                filled=ref.filled,
                global_block_id=ref.global_block_id or f"local:{ref.physical_id}",
                physical_node_id=ref.physical_node_id,
                logical_index=idx,
                virtual_offset=ref.virtual_offset,
            ))

        feature_hashes = list(meta.image_ids)
        if feature_hash and feature_hash not in feature_hashes:
            feature_hashes.append(feature_hash)
        state = MultimodalState(
            state_id=uuid.uuid4().hex,
            kv_refs=indexed_refs,
            feature_hash=feature_hash,
            feature_hashes=feature_hashes,
            reuse_telemetry=normalize_reuse_telemetry(reuse_telemetry),
            meta=meta,
            workflow_id=workflow_id,
            step_index=meta.step,
            version_id=version_id or uuid.uuid4().hex,
            parent_version_id=parent_version_id,
            snapshot_epoch=snapshot_epoch,
            approximate=approximate,
            status=status,
            ttl_deadline=meta.ttl_deadline,
        )

        with self._lock:
            self._states[state.state_id] = state
            self._workflow_chain.setdefault(workflow_id, []).append(state.state_id)
            self._workflow_epoch[workflow_id] = max(
                snapshot_epoch,
                self._workflow_epoch.get(workflow_id, -1),
            )
            self._workflow_snapshot_index.setdefault(workflow_id, {}).setdefault(
                snapshot_epoch, []
            ).append(state.state_id)
            self._workflow_version_index.setdefault(workflow_id, {})[
                state.version_id
            ] = state.state_id
            if workflow_id:
                self.pm.kv_directory.commit_epoch(
                    workflow_id=workflow_id,
                    version_id=state.version_id,
                    snapshot_epoch=snapshot_epoch,
                )

        for h in state.feature_hashes:
            if self.features.has(h):
                self.features.incref(h)

        if register_in_radix and state.meta.token_ids and state.kv_refs:
            self._insert_pagewise(state.meta.token_ids, state.kv_refs, scope=workflow_id)
        if self.workflow_registry is not None:
            self.workflow_registry.upsert_from_state(state, event="REGISTERED")
        return state

    def _insert_pagewise(
        self, token_ids: Sequence[int], refs: Sequence[BlockRef], scope: Optional[str] = None
    ) -> None:
        page_size = self.pm.page_size
        offset = 0
        prefix: List[int] = []
        for idx, ref in enumerate(refs):
            chunk = list(token_ids[offset : offset + ref.filled])
            if len(chunk) < ref.filled:
                break
            prefix.extend(chunk)
            scoped_ref = BlockRef(
                physical_id=ref.physical_id,
                filled=ref.filled,
                global_block_id=ref.global_block_id or f"local:{ref.physical_id}",
                physical_node_id=ref.physical_node_id,
                logical_index=idx,
                virtual_offset=ref.virtual_offset,
            )
            self.radix.insert(prefix, [scoped_ref], scope=scope)
            offset += ref.filled

    def fork(
        self,
        parent: MultimodalState,
        child_agent_id: Optional[str] = None,
    ) -> MultimodalState:
        self._ensure_state_mutable(parent, operation="fork")
        child_meta = StateMeta(
            token_ids=list(parent.meta.token_ids),
            image_ids=list(parent.feature_hashes),
            agent_id=child_agent_id or parent.meta.agent_id,
            workflow_id=parent.workflow_id,
            step=parent.step_index,
            turn=parent.meta.turn,
            ttl_deadline=parent.ttl_deadline,
            priority=parent.meta.priority,
        )
        child_refs = self.pm.fork_refs(parent.kv_refs)
        for h in parent.feature_hashes:
            if self.features.has(h):
                self.features.incref(h)
        child = MultimodalState(
            state_id=uuid.uuid4().hex,
            kv_refs=child_refs,
            feature_hash=parent.feature_hash,
            feature_hashes=list(parent.feature_hashes),
            reuse_telemetry=dict(parent.reuse_telemetry),
            meta=child_meta,
            workflow_id=parent.workflow_id,
            step_index=parent.step_index,
            version_id=uuid.uuid4().hex,
            parent_version_id=parent.version_id,
            snapshot_epoch=parent.snapshot_epoch,
            approximate=parent.approximate,
            status="ACTIVE",
            ttl_deadline=parent.ttl_deadline,
        )
        with self._lock:
            self._states[child.state_id] = child
            self._workflow_chain.setdefault(parent.workflow_id, []).append(child.state_id)
            self._workflow_snapshot_index.setdefault(parent.workflow_id, {}).setdefault(
                child.snapshot_epoch, []
            ).append(child.state_id)
            self._workflow_version_index.setdefault(parent.workflow_id, {})[
                child.version_id
            ] = child.state_id
        if self.workflow_registry is not None:
            self.workflow_registry.upsert_from_state(child, event="FORKED")
        return child

    def release(self, state: MultimodalState) -> None:
        with self._lock:
            if state.state_id not in self._states:
                return
            del self._states[state.state_id]
            chain = self._workflow_chain.get(state.workflow_id)
            if chain and state.state_id in chain:
                chain.remove(state.state_id)
            versions = self._workflow_version_index.get(state.workflow_id)
            if versions is not None:
                versions.pop(state.version_id, None)
            snapshots = self._workflow_snapshot_index.get(state.workflow_id, {})
            members = snapshots.get(state.snapshot_epoch, [])
            if state.state_id in members:
                members.remove(state.state_id)
            if not members and state.snapshot_epoch in snapshots:
                snapshots.pop(state.snapshot_epoch, None)
        self.pm.release_refs(state.kv_refs)
        for h in state.feature_hashes:
            self.features.release(h)
        state.kv_refs = []
        state.status = "RELEASED"
        if self.workflow_registry is not None:
            self.workflow_registry.mark_released(state.state_id)

    def advance_step_prepare(
        self,
        prev: MultimodalState,
        new_token_ids: Sequence[int],
        new_image_hashes: Sequence[str],
    ) -> Tuple[List[BlockRef], List[int], List[str]]:
        self._ensure_state_mutable(prev, operation="advance_step_prepare")
        full_tokens = list(prev.meta.token_ids) + list(new_token_ids)
        requested_hashes = list(dict.fromkeys(str(h) for h in new_image_hashes if h))
        # Multimodal KV is conditioned on the image FeatureBundle. If a new
        # step changes the image set, do not reuse old image-conditioned KV
        # pages by token prefix alone. Cached features may still be passed to
        # prefill below, but KV must be recomputed for the changed context.
        image_context_changed = any(h not in prev.feature_hashes for h in requested_hashes)
        if image_context_changed:
            matched_pages, delta_tokens = [], full_tokens
        else:
            matched_pages, delta_tokens = self.borrow_prefix_refs(
                full_tokens, workflow_id=prev.workflow_id
            )
        missing_hashes = [h for h in requested_hashes if not self.features.has(h)]
        return matched_pages, list(delta_tokens), missing_hashes

    def borrow_prefix_refs(
        self,
        token_ids: Sequence[int],
        workflow_id: Optional[str] = None,
    ) -> Tuple[List[BlockRef], List[int]]:
        matched_pages, delta_tokens = self.radix.match_longest_prefix(
            token_ids, scope=workflow_id
        )
        for ref in matched_pages:
            self.pm.incref(ref.global_block_id or ref.physical_id)
        return matched_pages, list(delta_tokens)

    def advance_step_commit(
        self,
        prev: MultimodalState,
        new_token_ids: Sequence[int],
        new_image_hashes: Sequence[str],
        new_kv_refs: Sequence[BlockRef],
        matched_pages: Sequence[BlockRef],
        *,
        approximate: bool = False,
        reuse_telemetry: Optional[Dict[str, Any]] = None,
    ) -> MultimodalState:
        self._ensure_state_mutable(prev, operation="advance_step_commit")
        full_tokens = list(prev.meta.token_ids) + list(new_token_ids)
        full_hashes = list(dict.fromkeys(list(prev.feature_hashes) + list(new_image_hashes)))
        meta = StateMeta(
            token_ids=full_tokens,
            image_ids=full_hashes,
            agent_id=prev.meta.agent_id,
            workflow_id=prev.workflow_id,
            step=prev.step_index + 1,
            turn=prev.meta.turn,
            ttl_deadline=time.monotonic() + self.default_ttl_seconds,
            priority=prev.meta.priority,
        )
        committed_refs = self._normalize_committed_refs(
            matched_pages=matched_pages,
            committed_refs=new_kv_refs,
        )
        new_state = self.register(
            kv_refs=committed_refs,
            feature_hash=new_image_hashes[-1] if new_image_hashes else prev.feature_hash,
            meta=meta,
            register_in_radix=True,
            version_id=uuid.uuid4().hex,
            parent_version_id=prev.version_id,
            snapshot_epoch=prev.snapshot_epoch + 1,
            approximate=approximate,
            status="ACTIVE",
            reuse_telemetry=reuse_telemetry,
        )
        self.refresh_ttl(prev)
        return new_state

    def _ensure_state_mutable(self, state: MultimodalState, *, operation: str) -> None:
        if state.status in self._MUTATION_BLOCKED_STATUSES:
            raise RuntimeError(f"cannot {operation} state while {state.status.lower()}")

    @staticmethod
    def _normalize_committed_refs(
        *,
        matched_pages: Sequence[BlockRef],
        committed_refs: Sequence[BlockRef],
    ) -> List[BlockRef]:
        """Normalize commit refs for step advancement.

        Callers may return either:
        - full refs: ``matched_pages + delta_refs``
        - delta-only refs: ``delta_refs``

        This helper guarantees that the committed state always retains the
        matched prefix pages exactly once.
        """
        matched = [
            BlockRef(
                physical_id=ref.physical_id,
                filled=ref.filled,
                global_block_id=ref.global_block_id or f"local:{ref.physical_id}",
                physical_node_id=ref.physical_node_id,
                logical_index=ref.logical_index,
                virtual_offset=ref.virtual_offset,
            )
            for ref in matched_pages
        ]
        all_refs = list(committed_refs)
        if not matched:
            normalized = all_refs
        else:
            def _same_ref(a: BlockRef, b: BlockRef) -> bool:
                return (
                    (a.global_block_id or f"local:{a.physical_id}")
                    == (b.global_block_id or f"local:{b.physical_id}")
                    and a.physical_id == b.physical_id
                    and a.filled == b.filled
                    and a.virtual_offset == b.virtual_offset
                )

            starts_with_matched = (
                len(all_refs) >= len(matched)
                and all(_same_ref(all_refs[idx], matched[idx]) for idx in range(len(matched)))
            )
            # Callers either return the full ref stream (already beginning
            # with the matched prefix) or return only the delta refs.
            # Preserve later re-occurrences of the same physical page: those
            # are valid for approximate / substring reuse and must not be
            # collapsed away.
            normalized = list(all_refs) if starts_with_matched else (matched + list(all_refs))

        return [
            BlockRef(
                physical_id=ref.physical_id,
                filled=ref.filled,
                global_block_id=ref.global_block_id or f"local:{ref.physical_id}",
                physical_node_id=ref.physical_node_id,
                logical_index=idx,
                virtual_offset=ref.virtual_offset,
            )
            for idx, ref in enumerate(normalized)
        ]

    def refresh_ttl(
        self,
        state: MultimodalState,
        extra_seconds: Optional[float] = None,
        step_window: Optional[int] = None,
    ) -> None:
        if step_window is not None:
            extension = step_window * self.default_ttl_seconds
        elif extra_seconds is not None:
            extension = extra_seconds
        else:
            extension = self.default_ttl_seconds
        state.ttl_deadline = time.monotonic() + extension
        state.meta.ttl_deadline = state.ttl_deadline
        if self.workflow_registry is not None:
            self.workflow_registry.transition(
                state.state_id,
                ttl_deadline=state.ttl_deadline,
                event="TTL_REFRESHED",
            )

    def get_state(self, state_id: str) -> Optional[MultimodalState]:
        with self._lock:
            return self._states.get(state_id)

    def get_state_version(
        self,
        workflow_id: str,
        *,
        version_id: Optional[str] = None,
        snapshot_epoch: Optional[int] = None,
    ) -> Optional[MultimodalState]:
        with self._lock:
            if version_id is not None:
                state_id = self._workflow_version_index.get(workflow_id, {}).get(version_id)
                return None if state_id is None else self._states.get(state_id)
            if snapshot_epoch is not None:
                members = self._workflow_snapshot_index.get(workflow_id, {}).get(snapshot_epoch, [])
                for state_id in reversed(members):
                    state = self._states.get(state_id)
                    if state is not None:
                        return state
                return None
            chain = self._workflow_chain.get(workflow_id, [])
            for state_id in reversed(chain):
                state = self._states.get(state_id)
                if state is not None:
                    return state
            return None

    def states_at_snapshot(self, workflow_id: str, snapshot_epoch: int) -> List[MultimodalState]:
        with self._lock:
            members = list(
                self._workflow_snapshot_index.get(workflow_id, {}).get(snapshot_epoch, [])
            )
        return [self._states[state_id] for state_id in members if state_id in self._states]

    def latest_workflow_version(self, workflow_id: str) -> Optional[MultimodalState]:
        with self._lock:
            snapshots = self._workflow_snapshot_index.get(workflow_id, {})
            if not snapshots:
                return None
            latest_epoch = max(snapshots)
        return self.get_state_version(workflow_id, snapshot_epoch=latest_epoch)

    def fork_at_snapshot(
        self,
        workflow_id: str,
        snapshot_epoch: int,
        *,
        child_agent_id: Optional[str] = None,
    ) -> MultimodalState:
        parent = self.get_state_version(workflow_id, snapshot_epoch=snapshot_epoch)
        if parent is None:
            raise KeyError(
                f"no committed state for workflow_id={workflow_id} snapshot_epoch={snapshot_epoch}"
            )
        return self.fork(parent, child_agent_id=child_agent_id)

    def get_chain(self, workflow_id: str) -> List[MultimodalState]:
        with self._lock:
            ids = list(self._workflow_chain.get(workflow_id, []))
        return [self._states[sid] for sid in ids if sid in self._states]

    def release_chain(self, workflow_id: str) -> int:
        with self._lock:
            ids = list(self._workflow_chain.pop(workflow_id, []))
        released = 0
        for sid in ids:
            state = self._states.get(sid)
            if state is not None:
                self.release(state)
                released += 1
        return released

    def restore(self, handle: str, offload_manager=None) -> Optional[MultimodalState]:
        if offload_manager is None:
            return None
        return offload_manager.restore(handle)

    def advance_step(
        self,
        prev_state: MultimodalState,
        new_tokens: Sequence[int],
        new_images: Sequence[str],
        prefill_fn=None,
    ) -> MultimodalState:
        matched, delta, missing_hashes = self.advance_step_prepare(prev_state, new_tokens, new_images)
        if missing_hashes:
            raise KeyError(
                "missing FeatureBundle(s) for new multimodal step: "
                + ", ".join(missing_hashes)
            )
        delta_features = [
            bundle
            for bundle in (self.features.get(h) for h in dict.fromkeys(new_images))
            if bundle is not None
        ]
        if prefill_fn is not None:
            new_kv_refs, _ = prefill_fn(
                delta,
                prefix_kv_refs=matched,
                delta_features=delta_features,
            )
        else:
            new_kv_refs = list(matched)
        return self.advance_step_commit(prev_state, new_tokens, new_images, new_kv_refs, matched)

    def handoff_prepare(
        self,
        state: MultimodalState,
        to_agent_id: str,
        *,
        target_node_id: Optional[str] = None,
        staged_refs: Optional[Sequence[BlockRef]] = None,
        staged_feature_hashes: Optional[Sequence[str]] = None,
    ) -> MultimodalState:
        if state.status in {"OFFLOADING", "RESTORING"}:
            raise RuntimeError(f"cannot handoff state while {state.status.lower()}")
        target_node = str(target_node_id or self.pm.node_id)
        handoff_id = uuid.uuid4().hex
        txn = HandoffTransaction(
            handoff_id=handoff_id,
            state_id=state.state_id,
            workflow_id=state.workflow_id,
            source_agent_id=state.meta.agent_id,
            target_agent_id=to_agent_id,
            source_node_id=self.pm.node_id,
            target_node_id=target_node,
            staged_refs=list(staged_refs or state.kv_refs),
            staged_feature_hashes=list(staged_feature_hashes or state.feature_hashes),
        )
        with self._lock:
            self._handoff_pending[state.state_id] = txn
        self.pm.kv_directory.begin_handoff(
            handoff_id=handoff_id,
            state_id=state.state_id,
            workflow_id=state.workflow_id,
            source_agent_id=state.meta.agent_id,
            target_agent_id=to_agent_id,
            source_node_id=self.pm.node_id,
            target_node_id=target_node,
            block_ids=[ref.global_block_id or f"local:{ref.physical_id}" for ref in txn.staged_refs],
            feature_hashes=list(txn.staged_feature_hashes),
        )
        state.status = "HANDING_OVER"
        if self.workflow_registry is not None:
            self.workflow_registry.transition(
                state.state_id,
                status="HANDING_OVER",
                handoff_id=handoff_id,
                target_agent_id=to_agent_id,
                target_node_id=target_node,
                event="HANDOFF_PREPARED",
            )
        if self._handoff_wal is not None:
            self._handoff_wal.append(
                {
                    "kind": "handoff",
                    "action": "PREPARED",
                    "handoff_id": handoff_id,
                    "state_id": state.state_id,
                    "workflow_id": state.workflow_id,
                    "source_agent_id": txn.source_agent_id,
                    "target_agent_id": txn.target_agent_id,
                    "source_node_id": txn.source_node_id,
                    "target_node_id": txn.target_node_id,
                    "block_ids": [
                        ref.global_block_id or f"local:{ref.physical_id}"
                        for ref in txn.staged_refs
                    ],
                    "feature_hashes": list(txn.staged_feature_hashes),
                }
            )
        return state

    def handoff_commit(self, state: MultimodalState) -> MultimodalState:
        with self._lock:
            txn = self._handoff_pending.pop(state.state_id, None)
        if txn is None:
            raise RuntimeError("handoff commit requested without pending prepare")
        state.meta.agent_id = txn.target_agent_id
        state.workflow_id = txn.workflow_id
        state.meta.workflow_id = txn.workflow_id
        state.kv_refs = [
            BlockRef(
                physical_id=ref.physical_id,
                filled=ref.filled,
                global_block_id=ref.global_block_id or f"local:{ref.physical_id}",
                physical_node_id=txn.target_node_id,
                logical_index=idx,
                virtual_offset=ref.virtual_offset,
            )
            for idx, ref in enumerate(txn.staged_refs)
        ]
        self.pm.kv_directory.commit_handoff(txn.handoff_id)
        if self.workflow_registry is not None:
            self.workflow_registry.transition(
                state.state_id,
                status="ACTIVE",
                agent_id=txn.target_agent_id,
                clear_handoff=True,
                event="HANDOFF_COMMITTED",
            )
        if self._handoff_wal is not None:
            self._handoff_wal.append(
                {
                    "kind": "handoff",
                    "action": "COMMITTED",
                    "handoff_id": txn.handoff_id,
                    "state_id": state.state_id,
                    "workflow_id": state.workflow_id,
                    "target_agent_id": txn.target_agent_id,
                    "target_node_id": txn.target_node_id,
                }
            )
        self.pm.kv_directory.clear_handoff(txn.handoff_id)
        if self._handoff_wal is not None:
            self._handoff_wal.append(
                {
                    "kind": "handoff",
                    "action": "CLEARED",
                    "handoff_id": txn.handoff_id,
                    "state_id": state.state_id,
                }
            )
        state.status = "ACTIVE"
        return state

    def handoff_rollback(self, state: MultimodalState) -> MultimodalState:
        with self._lock:
            txn = self._handoff_pending.pop(state.state_id, None)
        if txn is not None:
            self.pm.kv_directory.rollback_handoff(txn.handoff_id)
            if self.workflow_registry is not None:
                self.workflow_registry.transition(
                    state.state_id,
                    status="ACTIVE",
                    clear_handoff=True,
                    event="HANDOFF_ROLLED_BACK",
                )
            if self._handoff_wal is not None:
                self._handoff_wal.append(
                    {
                        "kind": "handoff",
                        "action": "ROLLED_BACK",
                        "handoff_id": txn.handoff_id,
                        "state_id": state.state_id,
                    }
                )
            self.pm.kv_directory.clear_handoff(txn.handoff_id)
            if self._handoff_wal is not None:
                self._handoff_wal.append(
                    {
                        "kind": "handoff",
                        "action": "CLEARED",
                        "handoff_id": txn.handoff_id,
                        "state_id": state.state_id,
                    }
                )
        state.status = "ACTIVE"
        return state

    def handoff(self, state: MultimodalState, to_agent_id: str) -> MultimodalState:
        self.handoff_prepare(state, to_agent_id=to_agent_id)
        return self.handoff_commit(state)

    def get_handoff(self, state_id: str) -> Optional[HandoffTransaction]:
        with self._lock:
            txn = self._handoff_pending.get(state_id)
            if txn is None:
                return None
            return HandoffTransaction(
                handoff_id=txn.handoff_id,
                state_id=txn.state_id,
                workflow_id=txn.workflow_id,
                source_agent_id=txn.source_agent_id,
                target_agent_id=txn.target_agent_id,
                source_node_id=txn.source_node_id,
                target_node_id=txn.target_node_id,
                staged_refs=list(txn.staged_refs),
                staged_feature_hashes=list(txn.staged_feature_hashes),
                prepared_at=txn.prepared_at,
                status=txn.status,
            )

    def offload(self, state: MultimodalState) -> None:
        self.pm.release_refs(state.kv_refs)
        state.kv_refs = []
        state.status = "OFFLOADED"
        if self.workflow_registry is not None:
            self.workflow_registry.transition(
                state.state_id,
                status="OFFLOADED",
                event="OFFLOADED",
            )

    def recreate_state_from_record(self, record: WorkflowStateRecord) -> MultimodalState:
        """Recreate a lightweight state shell from persisted workflow metadata."""

        with self._lock:
            existing = self._states.get(record.state_id)
            if existing is not None:
                return existing
            meta = StateMeta(
                token_ids=list(record.token_ids),
                image_ids=list(record.image_ids or record.feature_hashes),
                agent_id=record.agent_id,
                workflow_id=record.workflow_id,
                step=record.step_index,
                ttl_deadline=record.ttl_deadline,
            )
            state = MultimodalState(
                state_id=record.state_id,
                kv_refs=[],
                feature_hash=record.feature_hashes[-1] if record.feature_hashes else None,
                feature_hashes=list(record.feature_hashes),
                reuse_telemetry=dict(record.reuse_telemetry),
                meta=meta,
                workflow_id=record.workflow_id,
                step_index=record.step_index,
                version_id=record.version_id,
                parent_version_id=record.parent_version_id,
                snapshot_epoch=record.snapshot_epoch,
                approximate=record.approximate,
                status=record.status,
                ttl_deadline=record.ttl_deadline,
            )
            self._states[state.state_id] = state
            self._workflow_chain.setdefault(record.workflow_id, []).append(state.state_id)
            self._workflow_snapshot_index.setdefault(record.workflow_id, {}).setdefault(
                record.snapshot_epoch, []
            ).append(state.state_id)
            self._workflow_version_index.setdefault(record.workflow_id, {})[
                record.version_id
            ] = state.state_id
            self._workflow_epoch[record.workflow_id] = max(
                record.snapshot_epoch,
                self._workflow_epoch.get(record.workflow_id, -1),
            )
            return state

    def recover_handoffs(self) -> Dict[str, str]:
        """Recover dangling 2PC handoffs from WAL + directory state."""

        recovered: Dict[str, str] = {}
        if self._handoff_wal is None:
            return recovered
        with self._lock:
            self._handoff_pending.clear()

        wal_rows = [
            row
            for row in self._handoff_wal.read_all()
            if row.get("kind") == "handoff"
        ]
        merged = merge_wal_rows(wal_rows, group_key="handoff_id")
        pending = {}
        if hasattr(self.pm.kv_directory, "handoff_states"):
            pending = self.pm.kv_directory.handoff_states()

        handoff_ids = set(merged) | set(pending)
        for handoff_id in sorted(handoff_ids):
            row = merged.get(handoff_id, {})
            action = str(row.get("action", "")).upper()
            state_id = row.get("state_id")
            state = self._states.get(str(state_id)) if state_id is not None else None
            if state is None and self.workflow_registry is not None and state_id is not None:
                record = self.workflow_registry.get_record(str(state_id))
                if record is not None:
                    state = self.recreate_state_from_record(record)

            if action == "PREPARED":
                self.pm.kv_directory.rollback_handoff(handoff_id)
                self.pm.kv_directory.clear_handoff(handoff_id)
                if state is not None:
                    state.status = "ACTIVE"
                if self.workflow_registry is not None and state_id is not None:
                    self.workflow_registry.transition(
                        str(state_id),
                        status="ACTIVE",
                        clear_handoff=True,
                        event="HANDOFF_RECOVERED_ROLLBACK",
                    )
                recovered[handoff_id] = "ROLLED_BACK"
                self._handoff_wal.append(
                    {
                        "kind": "handoff",
                        "action": "RECOVERED_ROLLBACK",
                        "handoff_id": handoff_id,
                        "state_id": state_id,
                    }
                )
            elif action == "COMMITTED":
                self.pm.kv_directory.clear_handoff(handoff_id)
                if state is not None:
                    target_agent_id = row.get("target_agent_id")
                    if target_agent_id:
                        state.meta.agent_id = str(target_agent_id)
                    state.status = "ACTIVE"
                if self.workflow_registry is not None and state_id is not None:
                    self.workflow_registry.transition(
                        str(state_id),
                        status="ACTIVE",
                        agent_id=None if row.get("target_agent_id") is None else str(row.get("target_agent_id")),
                        clear_handoff=True,
                        event="HANDOFF_RECOVERED_CLEAR",
                    )
                recovered[handoff_id] = "COMMITTED_CLEARED"
                self._handoff_wal.append(
                    {
                        "kind": "handoff",
                        "action": "RECOVERED_CLEAR",
                        "handoff_id": handoff_id,
                        "state_id": state_id,
                    }
                )
            elif action in {"ROLLED_BACK", "CLEARED", "RECOVERED_ROLLBACK", "RECOVERED_CLEAR"}:
                if action != "CLEARED":
                    self.pm.kv_directory.clear_handoff(handoff_id)
                if state is not None:
                    state.status = "ACTIVE"
                recovered[handoff_id] = action
            elif handoff_id in pending:
                # Defensive fallback for directory-only leftovers.
                self.pm.kv_directory.rollback_handoff(handoff_id)
                self.pm.kv_directory.clear_handoff(handoff_id)
                if state is not None:
                    state.status = "ACTIVE"
                if self.workflow_registry is not None and state_id is not None:
                    self.workflow_registry.transition(
                        str(state_id),
                        status="ACTIVE",
                        clear_handoff=True,
                        event="HANDOFF_RECOVERED_DIRECTORY_ONLY",
                    )
                recovered[handoff_id] = "ROLLED_BACK_DIRECTORY_ONLY"
                self._handoff_wal.append(
                    {
                        "kind": "handoff",
                        "action": "RECOVERED_ROLLBACK",
                        "handoff_id": handoff_id,
                        "state_id": state_id,
                    }
                )
        return recovered

    def start_reaper(self, interval_seconds: float = 5.0) -> None:
        if self._reaper_thread is not None:
            return

        def _run():
            while not self._stop.wait(interval_seconds):
                self._reap_once()

        self._reaper_thread = threading.Thread(target=_run, daemon=True)
        self._reaper_thread.start()

    def stop_reaper(self) -> None:
        self._stop.set()
        if self._reaper_thread is not None:
            self._reaper_thread.join(timeout=2.0)
            self._reaper_thread = None

    def _reap_once(self) -> None:
        now = time.monotonic()
        with self._lock:
            expired = [
                sid
                for sid, s in self._states.items()
                if s.ttl_deadline <= now
                and s.status not in {"OFFLOADING", "RESTORING", "HANDING_OVER"}
            ]
        for sid in expired:
            state = self._states.get(sid)
            if state is not None:
                self.release(state)

    def stats(self) -> Dict[str, float]:
        with self._lock:
            return {
                "active_states": len(self._states),
                "workflows": len(self._workflow_chain),
                "workflow_epochs": len(self._workflow_epoch),
                "workflow_versions": sum(
                    len(versions) for versions in self._workflow_version_index.values()
                ),
                "pending_handoffs": len(self._handoff_pending),
            }
