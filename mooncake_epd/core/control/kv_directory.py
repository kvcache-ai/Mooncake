"""Local in-memory KV Directory / owner-shard abstraction.

This implements the RFC's control-plane notion that KV page ownership and
reference counts are authoritative in a directory, rather than inside the
tensor allocator alone. The current backend is intentionally single-process
and in-memory, but the API is shaped so it can later be replaced by a
distributed owner-shard service.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field, replace
from typing import Dict, List, Optional, Tuple


@dataclass
class KVBlockRecord:
    global_block_id: str
    local_physical_id: int
    workflow_id: str = ""
    owner_shard: str = "local"
    physical_node_id: str = "local"
    refcount: int = 1
    lease_count: int = 0
    physical_released: bool = False
    external_placeholder: bool = False
    created_at: float = field(default_factory=time.monotonic)
    last_updated_at: float = field(default_factory=time.monotonic)


@dataclass
class PromoteResult:
    global_block_id: str
    old_owner: str
    new_owner: str
    physical_node_id: str


@dataclass
class HandoffRecord:
    handoff_id: str
    state_id: str
    workflow_id: str
    source_agent_id: str
    target_agent_id: str
    source_node_id: str
    target_node_id: str
    prepared_at: float = field(default_factory=time.monotonic)
    status: str = "PREPARED"
    block_ids: List[str] = field(default_factory=list)
    feature_hashes: List[str] = field(default_factory=list)


class LocalKVDirectory:
    """Authoritative local owner-shard for KV block lifecycle."""

    def __init__(self, node_id: str = "local"):
        self.node_id = str(node_id)
        self._lock = threading.RLock()
        self._records: Dict[str, KVBlockRecord] = {}
        self._pid_to_gid: Dict[int, str] = {}
        self._next_gid = 0
        self._next_placeholder_pid = -1
        self._workflow_epochs: Dict[str, int] = {}
        self._workflow_versions: Dict[str, List[Tuple[int, str]]] = {}
        self._handoffs: Dict[str, HandoffRecord] = {}

    # ------------------------------------------------------------------
    # Block lifecycle
    # ------------------------------------------------------------------
    def register_block(
        self,
        local_pid: int,
        workflow_id: str = "",
        node_id: Optional[str] = None,
    ) -> str:
        with self._lock:
            gid = f"{self.node_id}-block-{self._next_gid}"
            self._next_gid += 1
            node = str(node_id or self.node_id)
            record = KVBlockRecord(
                global_block_id=gid,
                local_physical_id=local_pid,
                workflow_id=workflow_id,
                owner_shard=node,
                physical_node_id=node,
            )
            self._records[gid] = record
            self._pid_to_gid[local_pid] = gid
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
        with self._lock:
            existing = self._records.get(global_block_id)
            if existing is not None:
                if workflow_id and not existing.workflow_id:
                    existing.workflow_id = workflow_id
                if owner_shard:
                    existing.owner_shard = str(owner_shard)
                if physical_node_id:
                    existing.physical_node_id = str(physical_node_id)
                if local_pid is not None and int(local_pid) != existing.local_physical_id:
                    mapped_gid = self._pid_to_gid.get(int(local_pid))
                    if mapped_gid is not None and mapped_gid != global_block_id:
                        raise ValueError(
                            f"local_pid={local_pid} already mapped to {mapped_gid}, "
                            f"cannot remap to {global_block_id}"
                        )
                    self._pid_to_gid.pop(existing.local_physical_id, None)
                    existing.local_physical_id = int(local_pid)
                    self._pid_to_gid[int(local_pid)] = global_block_id
                if refcount > existing.refcount:
                    existing.refcount = refcount
                existing.external_placeholder = bool(external_placeholder)
                existing.last_updated_at = time.monotonic()
                return replace(existing)

            if local_pid is None:
                local_pid = self._next_placeholder_pid
                self._next_placeholder_pid -= 1
            elif local_pid in self._pid_to_gid:
                mapped_gid = self._pid_to_gid[local_pid]
                if mapped_gid != global_block_id:
                    raise ValueError(
                        f"local_pid={local_pid} already mapped to {mapped_gid}, "
                        f"cannot remap to {global_block_id}"
                    )

            owner = str(owner_shard or self.node_id)
            physical_node = str(physical_node_id or owner)
            record = KVBlockRecord(
                global_block_id=global_block_id,
                local_physical_id=int(local_pid),
                workflow_id=workflow_id,
                owner_shard=owner,
                physical_node_id=physical_node,
                refcount=max(0, int(refcount)),
                external_placeholder=external_placeholder,
            )
            self._records[global_block_id] = record
            self._pid_to_gid[int(local_pid)] = global_block_id
            return replace(record)

    def gid_for_physical_id(self, local_pid: int) -> Optional[str]:
        with self._lock:
            return self._pid_to_gid.get(local_pid)

    def resolve_physical_id(self, global_block_id: str) -> Optional[int]:
        with self._lock:
            record = self._records.get(global_block_id)
            return None if record is None else record.local_physical_id

    def get_record(self, global_block_id: str) -> Optional[KVBlockRecord]:
        with self._lock:
            record = self._records.get(global_block_id)
            return None if record is None else replace(record)

    def drop_block_record(
        self,
        global_block_id: str,
        *,
        only_placeholder: bool = False,
    ) -> bool:
        with self._lock:
            record = self._records.get(global_block_id)
            if record is None:
                return False
            if only_placeholder and not record.external_placeholder:
                return False
            self._drop_record(global_block_id, record.local_physical_id)
            return True

    def incref(self, global_block_id: str) -> int:
        with self._lock:
            record = self._records[global_block_id]
            record.refcount += 1
            record.last_updated_at = time.monotonic()
            return record.refcount

    def decref(self, global_block_id: str) -> int:
        with self._lock:
            record = self._records.get(global_block_id)
            if record is None:
                return 0
            record.refcount = max(0, record.refcount - 1)
            record.last_updated_at = time.monotonic()
            if (
                record.refcount <= 0
                and record.lease_count <= 0
                and record.physical_released
            ):
                self._drop_record(global_block_id, record.local_physical_id)
                return 0
            return record.refcount

    def refcount(self, global_block_id: str) -> int:
        with self._lock:
            record = self._records.get(global_block_id)
            return 0 if record is None else record.refcount

    def lease(self, global_block_id: str) -> int:
        with self._lock:
            record = self._records[global_block_id]
            record.lease_count += 1
            record.last_updated_at = time.monotonic()
            return record.lease_count

    def release_lease(self, global_block_id: str) -> int:
        with self._lock:
            record = self._records.get(global_block_id)
            if record is None:
                return 0
            record.lease_count = max(0, record.lease_count - 1)
            record.last_updated_at = time.monotonic()
            if (
                record.refcount <= 0
                and record.lease_count <= 0
                and record.physical_released
            ):
                self._drop_record(global_block_id, record.local_physical_id)
                return 0
            return record.lease_count

    def release_physical(self, global_block_id: str) -> bool:
        with self._lock:
            record = self._records.get(global_block_id)
            if record is None:
                return False
            record.physical_released = True
            record.last_updated_at = time.monotonic()
            if record.refcount <= 0 and record.lease_count <= 0:
                self._drop_record(global_block_id, record.local_physical_id)
            return True

    def promote_block(self, global_block_id: str, node_id: str) -> PromoteResult:
        with self._lock:
            record = self._records[global_block_id]
            old_owner = record.owner_shard
            new_owner = str(node_id)
            record.owner_shard = new_owner
            record.physical_node_id = new_owner
            record.last_updated_at = time.monotonic()
            return PromoteResult(
                global_block_id=global_block_id,
                old_owner=old_owner,
                new_owner=new_owner,
                physical_node_id=record.physical_node_id,
            )

    # ------------------------------------------------------------------
    # A2A 2PC handoff bookkeeping
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
        block_ids: List[str],
        feature_hashes: Optional[List[str]] = None,
    ) -> HandoffRecord:
        with self._lock:
            record = HandoffRecord(
                handoff_id=handoff_id,
                state_id=state_id,
                workflow_id=workflow_id,
                source_agent_id=source_agent_id,
                target_agent_id=target_agent_id,
                source_node_id=source_node_id,
                target_node_id=target_node_id,
                block_ids=list(block_ids),
                feature_hashes=list(feature_hashes or []),
            )
            self._handoffs[handoff_id] = record
            return replace(record)

    def get_handoff(self, handoff_id: str) -> Optional[HandoffRecord]:
        with self._lock:
            record = self._handoffs.get(handoff_id)
            return None if record is None else replace(record)

    def commit_handoff(self, handoff_id: str) -> Optional[HandoffRecord]:
        with self._lock:
            record = self._handoffs.get(handoff_id)
            if record is None:
                return None
            for gid in record.block_ids:
                if gid in self._records:
                    self._records[gid].owner_shard = record.target_node_id
                    self._records[gid].physical_node_id = record.target_node_id
                    self._records[gid].last_updated_at = time.monotonic()
            record.status = "COMMITTED"
            return replace(record)

    def rollback_handoff(self, handoff_id: str) -> Optional[HandoffRecord]:
        with self._lock:
            record = self._handoffs.get(handoff_id)
            if record is None:
                return None
            record.status = "ROLLED_BACK"
            return replace(record)

    def clear_handoff(self, handoff_id: str) -> Optional[HandoffRecord]:
        with self._lock:
            record = self._handoffs.pop(handoff_id, None)
            return None if record is None else replace(record)

    def handoff_states(self) -> Dict[str, HandoffRecord]:
        with self._lock:
            return {
                handoff_id: replace(record)
                for handoff_id, record in self._handoffs.items()
            }

    # ------------------------------------------------------------------
    # Epoch / version bookkeeping
    # ------------------------------------------------------------------
    def commit_epoch(
        self,
        workflow_id: str,
        version_id: str,
        snapshot_epoch: Optional[int] = None,
    ) -> int:
        if workflow_id == "":
            return -1 if snapshot_epoch is None else snapshot_epoch
        with self._lock:
            if snapshot_epoch is None:
                snapshot_epoch = self._workflow_epochs.get(workflow_id, -1) + 1
            self._workflow_epochs[workflow_id] = max(
                snapshot_epoch, self._workflow_epochs.get(workflow_id, -1)
            )
            self._workflow_versions.setdefault(workflow_id, []).append(
                (snapshot_epoch, version_id)
            )
            return snapshot_epoch

    def workflow_epoch(self, workflow_id: str) -> int:
        with self._lock:
            return self._workflow_epochs.get(workflow_id, -1)

    def workflow_versions(self, workflow_id: str) -> List[Tuple[int, str]]:
        with self._lock:
            return list(self._workflow_versions.get(workflow_id, []))

    # ------------------------------------------------------------------
    # GC / introspection
    # ------------------------------------------------------------------
    def orphan_block_ids(self) -> List[str]:
        with self._lock:
            return [
                gid
                for gid, record in self._records.items()
                if record.physical_released
                and record.refcount <= 0
                and record.lease_count <= 0
            ]

    def sweep_orphans(self) -> int:
        with self._lock:
            orphaned = [
                (gid, record.local_physical_id)
                for gid, record in self._records.items()
                if record.physical_released
                and record.refcount <= 0
                and record.lease_count <= 0
            ]
            for gid, pid in orphaned:
                self._drop_record(gid, pid)
            return len(orphaned)

    def stats(self) -> Dict[str, int]:
        with self._lock:
            return {
                "blocks": len(self._records),
                "leased_blocks": sum(1 for record in self._records.values() if record.lease_count > 0),
                "orphan_blocks": len(self.orphan_block_ids()),
                "workflows": len(self._workflow_epochs),
                "handoffs": len(self._handoffs),
            }

    def _drop_record(self, global_block_id: str, local_pid: int) -> None:
        self._records.pop(global_block_id, None)
        if self._pid_to_gid.get(local_pid) == global_block_id:
            self._pid_to_gid.pop(local_pid, None)
