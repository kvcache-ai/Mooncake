"""Distributed owner-shard KV directory.

This is the repo's first non-trivial replacement for `LocalKVDirectory`.
It keeps the same method surface so `PagedKVManager`, `StateLayer`, serving,
and tests can inject it without rewriting callers, while adding:

- workflow-based shard routing;
- global gid -> shard resolution;
- cross-shard handoff coordination via `ConsistencyManager`;
- aggregated introspection across shards.
"""

from __future__ import annotations

import hashlib
import threading
from dataclasses import replace
from typing import Dict, Mapping, Optional, Sequence

from .consistency_manager import ConsistencyManager
from .kv_directory import HandoffRecord, KVBlockRecord, LocalKVDirectory, PromoteResult


def _stable_hash(text: str) -> int:
    digest = hashlib.sha256(str(text).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False)


class DistributedKVDirectory:
    """Workflow-sharded directory with a LocalKVDirectory per owner shard."""

    def __init__(
        self,
        shards: int | Mapping[str, LocalKVDirectory] = 2,
        *,
        default_node_id: str = "dist-kv",
    ):
        if isinstance(shards, int):
            if shards <= 0:
                raise ValueError("shards must be positive")
            shard_map = {
                f"shard-{idx}": LocalKVDirectory(node_id=f"shard-{idx}")
                for idx in range(shards)
            }
        else:
            shard_map = dict(shards)
            if not shard_map:
                raise ValueError("shards mapping must not be empty")

        self.node_id = default_node_id
        self._shards = shard_map
        self._ordered_shards = tuple(sorted(shard_map))
        self._gid_to_shard: Dict[str, str] = {}
        self._physical_to_shard: Dict[int, str] = {}
        self._lock = threading.RLock()
        self._consistency = ConsistencyManager(self._shards)

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------
    def _route_workflow(self, workflow_id: str) -> str:
        if not workflow_id:
            return self._ordered_shards[0]
        return self._ordered_shards[_stable_hash(workflow_id) % len(self._ordered_shards)]

    def _route_target_node(self, node_id: str) -> str:
        if not node_id:
            return self._ordered_shards[0]
        return self._ordered_shards[_stable_hash(node_id) % len(self._ordered_shards)]

    def _register_gid(self, gid: str, shard_id: str, local_pid: Optional[int] = None) -> None:
        with self._lock:
            self._gid_to_shard[gid] = shard_id
            if local_pid is not None:
                self._physical_to_shard[int(local_pid)] = shard_id

    def _unregister_gid(self, gid: str, local_pid: Optional[int] = None) -> None:
        with self._lock:
            self._gid_to_shard.pop(gid, None)
            if local_pid is not None:
                self._physical_to_shard.pop(int(local_pid), None)

    def _shard_for_gid(self, gid: str) -> str:
        with self._lock:
            shard_id = self._gid_to_shard.get(gid)
        if shard_id is not None:
            directory = self._shards[shard_id]
            if directory.get_record(gid) is not None:
                return shard_id
            self._unregister_gid(gid)
        for candidate, directory in self._shards.items():
            if directory.get_record(gid) is not None:
                self._register_gid(gid, candidate)
                return candidate
        raise KeyError(f"unknown global block id: {gid}")

    def _directory_for_gid(self, gid: str) -> LocalKVDirectory:
        return self._shards[self._shard_for_gid(gid)]

    def _directory_for_workflow(self, workflow_id: str) -> tuple[str, LocalKVDirectory]:
        shard_id = self._route_workflow(workflow_id)
        return shard_id, self._shards[shard_id]

    def _normalize_shard_hint(
        self,
        shard_hint: Optional[str],
        *,
        workflow_id: str = "",
    ) -> str:
        if shard_hint:
            normalized = str(shard_hint)
            if normalized in self._shards:
                return normalized
            return self._route_target_node(normalized)
        if workflow_id:
            return self._route_workflow(workflow_id)
        return self._ordered_shards[0]

    # ------------------------------------------------------------------
    # Block lifecycle
    # ------------------------------------------------------------------
    def register_block(
        self,
        local_pid: int,
        workflow_id: str = "",
        node_id: Optional[str] = None,
    ) -> str:
        shard_id, directory = self._directory_for_workflow(workflow_id)
        gid = directory.register_block(local_pid=local_pid, workflow_id=workflow_id, node_id=node_id)
        self._register_gid(gid, shard_id, local_pid=local_pid)
        return gid

    def ensure_block_record(
        self,
        global_block_id: str,
        *,
        workflow_id: str = "",
        owner_shard: Optional[str] = None,
        physical_node_id: Optional[str] = None,
        local_pid: Optional[int] = None,
        refcount: int = 1,
        external_placeholder: bool = False,
    ) -> KVBlockRecord:
        shard_id = self._normalize_shard_hint(
            owner_shard or self._gid_to_shard.get(global_block_id),
            workflow_id=workflow_id,
        )
        record = self._shards[shard_id].ensure_block_record(
            global_block_id,
            workflow_id=workflow_id,
            owner_shard=shard_id,
            physical_node_id=physical_node_id or owner_shard or shard_id,
            local_pid=local_pid,
            refcount=refcount,
            external_placeholder=external_placeholder,
        )
        self._register_gid(global_block_id, shard_id, local_pid=record.local_physical_id)
        return record

    def gid_for_physical_id(self, local_pid: int) -> Optional[str]:
        with self._lock:
            shard_id = self._physical_to_shard.get(int(local_pid))
        if shard_id is not None:
            gid = self._shards[shard_id].gid_for_physical_id(local_pid)
            if gid is not None:
                return gid
            with self._lock:
                self._physical_to_shard.pop(int(local_pid), None)
        for candidate, directory in self._shards.items():
            gid = directory.gid_for_physical_id(local_pid)
            if gid is not None:
                self._register_gid(gid, candidate, local_pid=local_pid)
                return gid
        return None

    def resolve_physical_id(self, global_block_id: str) -> Optional[int]:
        return self._directory_for_gid(global_block_id).resolve_physical_id(global_block_id)

    def get_record(self, global_block_id: str) -> Optional[KVBlockRecord]:
        try:
            record = self._directory_for_gid(global_block_id).get_record(global_block_id)
        except KeyError:
            return None
        return None if record is None else replace(record)

    def drop_block_record(
        self,
        global_block_id: str,
        *,
        only_placeholder: bool = False,
    ) -> bool:
        try:
            shard_id = self._shard_for_gid(global_block_id)
        except KeyError:
            return False
        record = self._shards[shard_id].get_record(global_block_id)
        ok = self._shards[shard_id].drop_block_record(
            global_block_id,
            only_placeholder=only_placeholder,
        )
        if ok:
            self._unregister_gid(
                global_block_id,
                local_pid=None if record is None else record.local_physical_id,
            )
        return ok

    def incref(self, global_block_id: str) -> int:
        return self._directory_for_gid(global_block_id).incref(global_block_id)

    def decref(self, global_block_id: str) -> int:
        return self._directory_for_gid(global_block_id).decref(global_block_id)

    def refcount(self, global_block_id: str) -> int:
        try:
            return self._directory_for_gid(global_block_id).refcount(global_block_id)
        except KeyError:
            return 0

    def lease(self, global_block_id: str) -> int:
        return self._directory_for_gid(global_block_id).lease(global_block_id)

    def release_lease(self, global_block_id: str) -> int:
        return self._directory_for_gid(global_block_id).release_lease(global_block_id)

    def release_physical(self, global_block_id: str) -> bool:
        try:
            return self._directory_for_gid(global_block_id).release_physical(global_block_id)
        except KeyError:
            return False

    def promote_block(self, global_block_id: str, node_id: str) -> PromoteResult:
        shard_id = self._shard_for_gid(global_block_id)
        result = self._shards[shard_id].promote_block(global_block_id, node_id)
        return PromoteResult(
            global_block_id=result.global_block_id,
            old_owner=result.old_owner,
            new_owner=result.new_owner,
            physical_node_id=result.physical_node_id,
        )

    # ------------------------------------------------------------------
    # Handoff / 2PC coordination
    # ------------------------------------------------------------------
    def begin_handoff(
        self,
        *,
        handoff_id: str,
        state_id: str,
        workflow_id: str,
        source_agent_id: str,
        target_agent_id: str,
        source_node_id: str,
        target_node_id: str,
        block_ids: Sequence[str],
        feature_hashes: Optional[Sequence[str]] = None,
    ) -> HandoffRecord:
        if block_ids:
            try:
                source_shard = self._shard_for_gid(block_ids[0])
            except KeyError:
                source_shard = self._route_workflow(workflow_id)
        else:
            source_shard = self._route_workflow(workflow_id)
        target_shard = self._route_target_node(target_node_id)
        record = self._consistency.begin_handoff(
            source_shard=source_shard,
            target_shard=target_shard,
            handoff_id=handoff_id,
            state_id=state_id,
            workflow_id=workflow_id,
            source_agent_id=source_agent_id,
            target_agent_id=target_agent_id,
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            block_ids=list(block_ids),
            feature_hashes=None if feature_hashes is None else list(feature_hashes),
        )
        for gid in block_ids:
            self._register_gid(gid, source_shard)
        return record

    def get_handoff(self, handoff_id: str) -> Optional[HandoffRecord]:
        state = self._consistency.get_handoff(handoff_id)
        return None if state is None else replace(state.record)

    def commit_handoff(self, handoff_id: str) -> Optional[HandoffRecord]:
        record = self._consistency.commit_handoff(handoff_id)
        if record is None:
            return None
        target_shard = self._route_target_node(record.target_node_id)
        for gid in record.block_ids:
            target_record = self._shards[target_shard].get_record(gid)
            self._register_gid(
                gid,
                target_shard,
                local_pid=None if target_record is None else target_record.local_physical_id,
            )
        return record

    def rollback_handoff(self, handoff_id: str) -> Optional[HandoffRecord]:
        return self._consistency.rollback_handoff(handoff_id)

    def clear_handoff(self, handoff_id: str) -> Optional[HandoffRecord]:
        return self._consistency.clear_handoff(handoff_id)

    def handoff_states(self) -> Dict[str, HandoffRecord]:
        out: Dict[str, HandoffRecord] = {}
        for handoff_id in tuple(self._consistency._handoffs.keys()):  # noqa: SLF001
            record = self.get_handoff(handoff_id)
            if record is not None:
                out[handoff_id] = record
        return out

    # ------------------------------------------------------------------
    # Epoch / versions
    # ------------------------------------------------------------------
    def commit_epoch(
        self,
        workflow_id: str,
        version_id: str,
        snapshot_epoch: Optional[int] = None,
    ) -> int:
        shard_id = self._route_workflow(workflow_id)
        return self._consistency.commit_epoch(
            workflow_id=workflow_id,
            version_id=version_id,
            shard_id=shard_id,
            snapshot_epoch=snapshot_epoch,
        )

    def workflow_epoch(self, workflow_id: str) -> int:
        shard_id = self._route_workflow(workflow_id)
        return self._consistency.workflow_epoch(workflow_id, shard_id)

    def workflow_versions(self, workflow_id: str):
        shard_id = self._route_workflow(workflow_id)
        return self._shards[shard_id].workflow_versions(workflow_id)

    # ------------------------------------------------------------------
    # Introspection / GC
    # ------------------------------------------------------------------
    def orphan_block_ids(self) -> list[str]:
        out: list[str] = []
        for directory in self._shards.values():
            out.extend(directory.orphan_block_ids())
        return out

    def sweep_orphans(self) -> int:
        swept = 0
        for directory in self._shards.values():
            swept += directory.sweep_orphans()
        return swept

    def stats(self) -> Dict[str, int]:
        shard_stats = {name: directory.stats() for name, directory in self._shards.items()}
        return {
            "blocks": sum(item["blocks"] for item in shard_stats.values()),
            "leased_blocks": sum(item["leased_blocks"] for item in shard_stats.values()),
            "orphan_blocks": sum(item["orphan_blocks"] for item in shard_stats.values()),
            "workflows": sum(item["workflows"] for item in shard_stats.values()),
            "handoffs": sum(item["handoffs"] for item in shard_stats.values()),
            "shards": len(self._shards),
        }
