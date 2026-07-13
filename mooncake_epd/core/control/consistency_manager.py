"""Distributed owner-shard consistency helpers.

The repo already had a correct single-process `LocalKVDirectory`. This module
layers a small but real cross-shard coordinator on top:

- source-shard prepare / target-shard commit / rollback state machine;
- cross-shard metadata move while preserving global block ids;
- workflow-epoch forwarding for version/snapshot commits.

The implementation is intentionally transport-agnostic so it can be embedded
in-process for tests or wrapped by an RPC service later.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field, replace
from typing import Dict, Iterable, Mapping, Optional

from .kv_directory import HandoffRecord, KVBlockRecord, LocalKVDirectory


@dataclass
class DistributedHandoffState:
    handoff_id: str
    source_shard: str
    target_shard: str
    record: HandoffRecord
    committed: bool = False
    phase: str = "PREPARED"
    prepared_target_placeholders: list[str] = field(default_factory=list)
    prepared_target_records: Dict[str, KVBlockRecord] = field(default_factory=dict)
    source_records: Dict[str, KVBlockRecord] = field(default_factory=dict)


class ConsistencyManager:
    """Cross-shard owner metadata coordinator.

    Hot-path incref/decref/CAS stay inside each shard-local `LocalKVDirectory`.
    This coordinator only handles the slower cross-shard transitions that need
    multi-object atomicity from the control plane perspective.
    """

    def __init__(self, shards: Mapping[str, LocalKVDirectory]):
        self._shards = dict(shards)
        self._lock = threading.RLock()
        self._handoffs: Dict[str, DistributedHandoffState] = {}

    def begin_handoff(
        self,
        *,
        source_shard: str,
        target_shard: str,
        handoff_id: str,
        state_id: str,
        workflow_id: str,
        source_agent_id: str,
        target_agent_id: str,
        source_node_id: str,
        target_node_id: str,
        block_ids: list[str],
        feature_hashes: Optional[list[str]] = None,
    ) -> HandoffRecord:
        with self._lock:
            existing = self._handoffs.get(handoff_id)
            if existing is not None:
                same_request = (
                    existing.source_shard == source_shard
                    and existing.target_shard == target_shard
                    and existing.record.state_id == state_id
                    and existing.record.workflow_id == workflow_id
                    and existing.record.block_ids == list(block_ids)
                )
                if not same_request:
                    raise ValueError(
                        f"handoff_id={handoff_id} already exists with different contents"
                    )
                return replace(existing.record)
            source = self._shards[source_shard]
            target = self._shards[target_shard]
            record = source.begin_handoff(
                handoff_id=handoff_id,
                state_id=state_id,
                workflow_id=workflow_id,
                source_agent_id=source_agent_id,
                target_agent_id=target_agent_id,
                source_node_id=source_node_id,
                target_node_id=target_node_id,
                block_ids=block_ids,
                feature_hashes=feature_hashes,
            )

            prepared_placeholders: list[str] = []
            prepared_target_records: Dict[str, KVBlockRecord] = {}
            source_records: Dict[str, KVBlockRecord] = {}
            try:
                if source_shard != target_shard:
                    for gid in block_ids:
                        source_record = source.get_record(gid)
                        if source_record is not None:
                            source_records[gid] = source_record
                        placeholder = target.ensure_block_record(
                            gid,
                            workflow_id=workflow_id,
                            owner_shard=target_shard,
                            physical_node_id=target_node_id,
                            local_pid=(
                                None if source_record is None else source_record.local_physical_id
                            ),
                            refcount=0 if source_record is None else source_record.refcount,
                            external_placeholder=True,
                        )
                        prepared_placeholders.append(gid)
                        prepared_target_records[gid] = placeholder
                self._handoffs[handoff_id] = DistributedHandoffState(
                    handoff_id=handoff_id,
                    source_shard=source_shard,
                    target_shard=target_shard,
                    record=record,
                    phase="PREPARED",
                    prepared_target_placeholders=prepared_placeholders,
                    prepared_target_records=prepared_target_records,
                    source_records=source_records,
                )
                return record
            except Exception:
                source.rollback_handoff(handoff_id)
                if source_shard != target_shard:
                    for gid in prepared_placeholders:
                        target.drop_block_record(gid, only_placeholder=True)
                source.clear_handoff(handoff_id)
                raise

    def get_handoff(self, handoff_id: str) -> Optional[DistributedHandoffState]:
        with self._lock:
            state = self._handoffs.get(handoff_id)
            if state is None:
                return None
            return DistributedHandoffState(
                handoff_id=state.handoff_id,
                source_shard=state.source_shard,
                target_shard=state.target_shard,
                record=replace(state.record),
                committed=state.committed,
                phase=state.phase,
                prepared_target_placeholders=list(state.prepared_target_placeholders),
                prepared_target_records={
                    gid: replace(record)
                    for gid, record in state.prepared_target_records.items()
                },
                source_records={
                    gid: replace(record)
                    for gid, record in state.source_records.items()
                },
            )

    def commit_handoff(self, handoff_id: str) -> Optional[HandoffRecord]:
        with self._lock:
            state = self._handoffs.get(handoff_id)
            if state is None:
                return None
            if state.phase == "COMMITTED" or state.record.status == "COMMITTED":
                return replace(state.record)
            if state.phase == "ROLLED_BACK" or state.record.status == "ROLLED_BACK":
                return replace(state.record)
            state.phase = "COMMITTING"
            source = self._shards[state.source_shard]
            target = self._shards[state.target_shard]
            applied_target_gids: list[str] = []
            try:
                committed_record = source.commit_handoff(handoff_id) or replace(
                    state.record,
                    status="COMMITTED",
                )
                if state.source_shard != state.target_shard:
                    for gid in state.record.block_ids:
                        source_record = source.get_record(gid) or state.source_records.get(gid)
                        if source_record is None:
                            continue
                        target.ensure_block_record(
                            gid,
                            workflow_id=source_record.workflow_id,
                            owner_shard=state.target_shard,
                            physical_node_id=state.record.target_node_id,
                            local_pid=source_record.local_physical_id,
                            refcount=source_record.refcount,
                            external_placeholder=False,
                        )
                        applied_target_gids.append(gid)
                    for gid in state.record.block_ids:
                        source.drop_block_record(gid)
                state.committed = True
                state.phase = "COMMITTED"
                state.record = replace(committed_record, status="COMMITTED")
                return replace(state.record)
            except Exception:
                self._restore_after_failed_commit(
                    state,
                    applied_target_gids=applied_target_gids,
                )
                state.phase = "PREPARED"
                raise

    def rollback_handoff(self, handoff_id: str) -> Optional[HandoffRecord]:
        with self._lock:
            state = self._handoffs.get(handoff_id)
            if state is None:
                return None
            if state.phase == "COMMITTED" or state.record.status == "COMMITTED":
                return replace(state.record)
            if state.phase == "ROLLED_BACK" or state.record.status == "ROLLED_BACK":
                return replace(state.record)
            state.phase = "ROLLING_BACK"
            source = self._shards[state.source_shard]
            target = self._shards[state.target_shard]
            record = source.rollback_handoff(handoff_id)
            if record is None:
                record = target.rollback_handoff(handoff_id)
            if record is None:
                record = replace(state.record, status="ROLLED_BACK")

            if state.source_shard != state.target_shard:
                self._drop_target_placeholders(state)
            state.committed = False
            state.phase = "ROLLED_BACK"
            state.record = replace(record, status="ROLLED_BACK")
            return replace(state.record)

    def clear_handoff(self, handoff_id: str) -> Optional[HandoffRecord]:
        with self._lock:
            state = self._handoffs.get(handoff_id)
            if state is None:
                return None
            source = self._shards[state.source_shard]
            target = self._shards[state.target_shard]
            was_prepared = state.phase == "PREPARED" or state.record.status == "PREPARED"
            if was_prepared:
                source.rollback_handoff(handoff_id)
                if state.source_shard != state.target_shard:
                    self._drop_target_placeholders(state)
            record = source.clear_handoff(handoff_id)
            if record is None:
                record = target.clear_handoff(handoff_id)
            if record is None:
                record = replace(state.record)
            self._handoffs.pop(handoff_id, None)
            if was_prepared:
                return replace(record, status="ROLLED_BACK")
            return record

    def workflow_epoch(self, workflow_id: str, shard_id: str) -> int:
        return self._shards[shard_id].workflow_epoch(workflow_id)

    def commit_epoch(
        self,
        *,
        workflow_id: str,
        version_id: str,
        shard_id: str,
        snapshot_epoch: Optional[int] = None,
    ) -> int:
        return self._shards[shard_id].commit_epoch(
            workflow_id=workflow_id,
            version_id=version_id,
            snapshot_epoch=snapshot_epoch,
        )

    def shards(self) -> Iterable[str]:
        return tuple(self._shards)

    def _restore_target_placeholders(self, state: DistributedHandoffState) -> None:
        target = self._shards[state.target_shard]
        for gid in state.prepared_target_placeholders:
            placeholder = state.prepared_target_records.get(gid)
            if placeholder is None:
                target.drop_block_record(gid)
                continue
            target.ensure_block_record(
                gid,
                workflow_id=placeholder.workflow_id,
                owner_shard=placeholder.owner_shard,
                physical_node_id=placeholder.physical_node_id,
                local_pid=placeholder.local_physical_id,
                refcount=placeholder.refcount,
                external_placeholder=True,
            )

    def _restore_after_failed_commit(
        self,
        state: DistributedHandoffState,
        *,
        applied_target_gids: list[str],
    ) -> None:
        source = self._shards[state.source_shard]
        target = self._shards[state.target_shard]
        for gid in state.record.block_ids:
            original = state.source_records.get(gid)
            if original is None:
                continue
            source.ensure_block_record(
                gid,
                workflow_id=original.workflow_id,
                owner_shard=original.owner_shard,
                physical_node_id=original.physical_node_id,
                local_pid=original.local_physical_id,
                refcount=original.refcount,
                external_placeholder=original.external_placeholder,
            )
        for gid in applied_target_gids:
            placeholder = state.prepared_target_records.get(gid)
            if placeholder is None:
                target.drop_block_record(gid)
                continue
            target.ensure_block_record(
                gid,
                workflow_id=placeholder.workflow_id,
                owner_shard=placeholder.owner_shard,
                physical_node_id=placeholder.physical_node_id,
                local_pid=placeholder.local_physical_id,
                refcount=placeholder.refcount,
                external_placeholder=True,
            )
        source.clear_handoff(state.handoff_id)
        source.begin_handoff(
            handoff_id=state.handoff_id,
            state_id=state.record.state_id,
            workflow_id=state.record.workflow_id,
            source_agent_id=state.record.source_agent_id,
            target_agent_id=state.record.target_agent_id,
            source_node_id=state.record.source_node_id,
            target_node_id=state.record.target_node_id,
            block_ids=list(state.record.block_ids),
            feature_hashes=list(state.record.feature_hashes),
        )

    def _drop_target_placeholders(self, state: DistributedHandoffState) -> None:
        target = self._shards[state.target_shard]
        for gid in state.prepared_target_placeholders:
            target.drop_block_record(gid, only_placeholder=True)
