"""Persistent workflow/state registry for state-layer recovery."""

from __future__ import annotations

import threading
import time
from dataclasses import asdict, dataclass, field, replace
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Sequence

from ..control.write_ahead_log import JsonLineWAL, merge_wal_rows

if TYPE_CHECKING:  # pragma: no cover
    from .state import MultimodalState


_REUSE_FLOAT_KEYS = {
    "reuse_ratio",
    "prefill_time_ms",
    "delta_prefill_ms",
    "tier3_mean_similarity",
}
_REUSE_INT_KEYS = {
    "reused_tokens",
    "total_tokens",
    "delta_tokens",
    "tier1_matched_tokens",
    "tier2_reused_tokens",
    "tier2_recomputed_tokens",
    "tier3_candidate_pages",
    "tier3_accepted_pages",
    "tier3_rejected_pages",
    "tier3_reused_tokens",
    "relay_segments",
    "relay_reusable_segments",
    "relay_recompute_segments",
    "relay_substring_hits",
    "relay_substring_misses",
    "delta_prefill_calls",
    "delta_prefill_tokens",
}
_REUSE_BOOL_KEYS = {
    "approximate",
    "cross_step",
}
_RELAY_FLOAT_KEYS = {
    "reuse_ratio",
    "prefill_ms",
}
_RELAY_INT_KEYS = {
    "segments",
    "reusable_segments",
    "recompute_segments",
    "reused_tokens",
    "recomputed_tokens",
    "substring_hits",
    "substring_misses",
    "prefill_calls",
    "prefill_tokens",
}


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_reuse_telemetry(telemetry: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    if not isinstance(telemetry, Mapping):
        return {}

    normalized: Dict[str, Any] = {}
    for key in _REUSE_FLOAT_KEYS:
        if key in telemetry:
            normalized[key] = _coerce_float(telemetry.get(key))
    for key in _REUSE_INT_KEYS:
        if key in telemetry:
            normalized[key] = _coerce_int(telemetry.get(key))
    for key in _REUSE_BOOL_KEYS:
        if key in telemetry:
            normalized[key] = bool(telemetry.get(key))

    relay_stats = telemetry.get("relay_stats")
    if isinstance(relay_stats, Mapping):
        relay: Dict[str, Any] = dict(relay_stats)
        for key in _RELAY_FLOAT_KEYS:
            if key in relay:
                relay[key] = _coerce_float(relay.get(key))
        for key in _RELAY_INT_KEYS:
            if key in relay:
                relay[key] = _coerce_int(relay.get(key))
        normalized["relay_stats"] = relay

    for key, value in telemetry.items():
        if key == "relay_stats":
            continue
        if key not in normalized:
            normalized[key] = value
    return normalized


@dataclass
class WorkflowStateRecord:
    state_id: str
    workflow_id: str
    version_id: str
    parent_version_id: Optional[str]
    snapshot_epoch: int
    step_index: int
    agent_id: str
    status: str
    approximate: bool = False
    reuse_telemetry: Dict[str, Any] = field(default_factory=dict)
    ttl_deadline: float = float("inf")
    token_ids: List[int] = field(default_factory=list)
    image_ids: List[str] = field(default_factory=list)
    feature_hashes: List[str] = field(default_factory=list)
    kv_block_ids: List[str] = field(default_factory=list)
    kv_logical_filled: List[int] = field(default_factory=list)
    # Persisted metadata-only Store/physical-page manifests.  Payload bytes
    # never enter the WAL; this allows recovery to validate lease/fence/layout
    # before asking a worker to materialize a branch.
    kv_manifests: List[Dict[str, Any]] = field(default_factory=list)
    offload_handle: Optional[str] = None
    offload_checkpoint_path: Optional[str] = None
    handoff_id: Optional[str] = None
    target_agent_id: Optional[str] = None
    target_node_id: Optional[str] = None
    updated_at: float = field(default_factory=time.monotonic)
    released_at: Optional[float] = None


class WorkflowStateRegistry:
    """Persistent state catalog keyed by `state_id`."""

    def __init__(self, wal_path: Optional[str] = None):
        self._lock = threading.RLock()
        self._records: Dict[str, WorkflowStateRecord] = {}
        self._wal = JsonLineWAL(wal_path) if wal_path else None
        if self._wal is not None:
            self.recover()

    # ------------------------------------------------------------------
    def upsert_from_state(
        self,
        state: "MultimodalState",
        *,
        event: str = "UPSERT",
        offload_handle: Optional[str] = None,
        offload_checkpoint_path: Optional[str] = None,
        handoff_id: Optional[str] = None,
        target_agent_id: Optional[str] = None,
        target_node_id: Optional[str] = None,
        reuse_telemetry: Optional[Mapping[str, Any]] = None,
    ) -> WorkflowStateRecord:
        record = WorkflowStateRecord(
            state_id=state.state_id,
            workflow_id=state.workflow_id,
            version_id=state.version_id,
            parent_version_id=state.parent_version_id,
            snapshot_epoch=state.snapshot_epoch,
            step_index=state.step_index,
            agent_id=state.meta.agent_id,
            status=state.status,
            approximate=state.approximate,
            reuse_telemetry=normalize_reuse_telemetry(
                reuse_telemetry
                if reuse_telemetry is not None
                else getattr(state, "reuse_telemetry", None)
            ),
            ttl_deadline=state.ttl_deadline,
            token_ids=list(state.meta.token_ids),
            image_ids=list(state.meta.image_ids),
            feature_hashes=list(state.feature_hashes),
            kv_block_ids=[
                ref.global_block_id or f"local:{ref.physical_id}"
                for ref in state.kv_refs
            ],
            kv_logical_filled=[int(ref.filled) for ref in state.kv_refs],
            offload_handle=offload_handle,
            offload_checkpoint_path=offload_checkpoint_path,
            handoff_id=handoff_id,
            target_agent_id=target_agent_id,
            target_node_id=target_node_id,
            updated_at=time.monotonic(),
        )
        return self.upsert_record(record, event=event)

    def upsert_record(
        self,
        record: WorkflowStateRecord,
        *,
        event: str = "UPSERT",
        preserve_existing_offload: bool = True,
        preserve_existing_handoff: bool = True,
        preserve_existing_reuse: bool = True,
    ) -> WorkflowStateRecord:
        record.reuse_telemetry = normalize_reuse_telemetry(record.reuse_telemetry)
        with self._lock:
            previous = self._records.get(record.state_id)
            if previous is not None:
                if preserve_existing_offload and record.offload_handle is None:
                    record.offload_handle = previous.offload_handle
                if preserve_existing_offload and record.offload_checkpoint_path is None:
                    record.offload_checkpoint_path = previous.offload_checkpoint_path
                if preserve_existing_handoff and record.handoff_id is None:
                    record.handoff_id = previous.handoff_id
                if preserve_existing_handoff and record.target_agent_id is None:
                    record.target_agent_id = previous.target_agent_id
                if preserve_existing_handoff and record.target_node_id is None:
                    record.target_node_id = previous.target_node_id
                if preserve_existing_reuse and not record.reuse_telemetry and previous.reuse_telemetry:
                    record.reuse_telemetry = dict(previous.reuse_telemetry)
                record.released_at = previous.released_at
            self._records[record.state_id] = record
        self._append(event, record)
        return replace(record)

    def transition(
        self,
        state_id: str,
        *,
        status: Optional[str] = None,
        agent_id: Optional[str] = None,
        ttl_deadline: Optional[float] = None,
        offload_handle: Optional[str] = None,
        offload_checkpoint_path: Optional[str] = None,
        handoff_id: Optional[str] = None,
        target_agent_id: Optional[str] = None,
        target_node_id: Optional[str] = None,
        clear_offload: bool = False,
        clear_handoff: bool = False,
        event: str = "TRANSITION",
    ) -> Optional[WorkflowStateRecord]:
        with self._lock:
            record = self._records.get(state_id)
            if record is None:
                return None
            if status is not None:
                record.status = status
            if agent_id is not None:
                record.agent_id = agent_id
            if ttl_deadline is not None:
                record.ttl_deadline = ttl_deadline
            if offload_handle is not None:
                record.offload_handle = offload_handle
            if offload_checkpoint_path is not None:
                record.offload_checkpoint_path = offload_checkpoint_path
            if handoff_id is not None:
                record.handoff_id = handoff_id
            if target_agent_id is not None:
                record.target_agent_id = target_agent_id
            if target_node_id is not None:
                record.target_node_id = target_node_id
            if clear_offload:
                record.offload_handle = None
                record.offload_checkpoint_path = None
            if clear_handoff:
                record.handoff_id = None
                record.target_agent_id = None
                record.target_node_id = None
            record.updated_at = time.monotonic()
            snapshot = replace(record)
        self._append(event, snapshot)
        return snapshot

    def mark_released(self, state_id: str, *, event: str = "RELEASED") -> Optional[WorkflowStateRecord]:
        with self._lock:
            record = self._records.get(state_id)
            if record is None:
                return None
            record.status = "RELEASED"
            record.released_at = time.monotonic()
            record.updated_at = record.released_at
            snapshot = replace(record)
        self._append(event, snapshot)
        return snapshot

    def get_record(self, state_id: str) -> Optional[WorkflowStateRecord]:
        with self._lock:
            record = self._records.get(state_id)
            return None if record is None else replace(record)

    def workflow_records(self, workflow_id: str) -> List[WorkflowStateRecord]:
        with self._lock:
            rows = [
                replace(record)
                for record in self._records.values()
                if record.workflow_id == workflow_id
            ]
        rows.sort(key=lambda row: (row.snapshot_epoch, row.step_index, row.updated_at))
        return rows

    def all_records(self) -> List[WorkflowStateRecord]:
        with self._lock:
            rows = [replace(record) for record in self._records.values()]
        rows.sort(key=lambda row: (row.workflow_id, row.snapshot_epoch, row.step_index, row.updated_at))
        return rows

    def latest_workflow_record(self, workflow_id: str) -> Optional[WorkflowStateRecord]:
        rows = self.workflow_records(workflow_id)
        return rows[-1] if rows else None

    def reuse_telemetry_summary(
        self,
        records: Optional[Sequence[WorkflowStateRecord]] = None,
    ) -> Dict[str, Any]:
        rows = list(self.all_records() if records is None else records)
        telemetry_rows: List[tuple[WorkflowStateRecord, Dict[str, Any]]] = []
        cross_step_rows: List[tuple[WorkflowStateRecord, Dict[str, Any]]] = []
        approximate_state_ids: List[str] = []
        active_approximate_state_ids: List[str] = []
        latest_by_workflow: Dict[str, tuple[WorkflowStateRecord, Dict[str, Any]]] = {}

        for row in rows:
            telemetry = normalize_reuse_telemetry(row.reuse_telemetry)
            if not telemetry:
                continue
            telemetry_rows.append((row, telemetry))
            if bool(telemetry.get("approximate", row.approximate)):
                approximate_state_ids.append(row.state_id)
                if row.status != "RELEASED":
                    active_approximate_state_ids.append(row.state_id)
            if bool(telemetry.get("cross_step")):
                cross_step_rows.append((row, telemetry))
                previous = latest_by_workflow.get(row.workflow_id)
                if previous is None or (
                    row.snapshot_epoch,
                    row.step_index,
                    row.updated_at,
                ) >= (
                    previous[0].snapshot_epoch,
                    previous[0].step_index,
                    previous[0].updated_at,
                ):
                    latest_by_workflow[row.workflow_id] = (row, telemetry)

        aggregate_rows = cross_step_rows or telemetry_rows
        ratios = [_coerce_float(telemetry.get("reuse_ratio")) for _, telemetry in aggregate_rows]
        delta_prefill_values = [
            _coerce_float(telemetry.get("delta_prefill_ms"))
            for _, telemetry in aggregate_rows
        ]
        total_reused_tokens = sum(
            _coerce_int(telemetry.get("reused_tokens"))
            for _, telemetry in aggregate_rows
        )
        total_context_tokens = sum(
            _coerce_int(telemetry.get("total_tokens"))
            for _, telemetry in aggregate_rows
        )
        total_delta_tokens = sum(
            _coerce_int(telemetry.get("delta_tokens"))
            for _, telemetry in aggregate_rows
        )
        total_tier3_reused_tokens = sum(
            _coerce_int(telemetry.get("tier3_reused_tokens"))
            for _, telemetry in aggregate_rows
        )
        total_tier3_accepted_pages = sum(
            _coerce_int(telemetry.get("tier3_accepted_pages"))
            for _, telemetry in aggregate_rows
        )
        tier3_mean_similarity_values = [
            _coerce_float(telemetry.get("tier3_mean_similarity"))
            for _, telemetry in aggregate_rows
            if _coerce_float(telemetry.get("tier3_mean_similarity")) > 0.0
        ]
        relay_substring_hits = 0
        relay_substring_misses = 0
        for _, telemetry in aggregate_rows:
            relay = telemetry.get("relay_stats") if isinstance(telemetry.get("relay_stats"), Mapping) else {}
            relay_substring_hits += _coerce_int(
                telemetry.get("relay_substring_hits", relay.get("substring_hits", 0))
            )
            relay_substring_misses += _coerce_int(
                telemetry.get("relay_substring_misses", relay.get("substring_misses", 0))
            )

        latest_reuse_by_workflow: Dict[str, Dict[str, Any]] = {}
        for workflow_id, (row, telemetry) in latest_by_workflow.items():
            relay = telemetry.get("relay_stats") if isinstance(telemetry.get("relay_stats"), Mapping) else {}
            latest_reuse_by_workflow[workflow_id] = {
                "state_id": row.state_id,
                "status": row.status,
                "step_index": row.step_index,
                "reuse_ratio": _coerce_float(telemetry.get("reuse_ratio")),
                "reused_tokens": _coerce_int(telemetry.get("reused_tokens")),
                "total_tokens": _coerce_int(telemetry.get("total_tokens")),
                "delta_tokens": _coerce_int(telemetry.get("delta_tokens")),
                "approximate": bool(telemetry.get("approximate", row.approximate)),
                "tier3_accepted_pages": _coerce_int(telemetry.get("tier3_accepted_pages")),
                "tier3_reused_tokens": _coerce_int(telemetry.get("tier3_reused_tokens")),
                "tier3_mean_similarity": _coerce_float(telemetry.get("tier3_mean_similarity")),
                "relay_substring_hits": _coerce_int(
                    telemetry.get("relay_substring_hits", relay.get("substring_hits", 0))
                ),
                "relay_substring_misses": _coerce_int(
                    telemetry.get("relay_substring_misses", relay.get("substring_misses", 0))
                ),
                "delta_prefill_ms": _coerce_float(telemetry.get("delta_prefill_ms")),
            }

        return {
            "states_with_reuse_telemetry": len(telemetry_rows),
            "cross_step_records": len(cross_step_rows),
            "approximate_states": len(approximate_state_ids),
            "active_approximate_state_ids": sorted(set(active_approximate_state_ids)),
            "approximate_state_ids": sorted(set(approximate_state_ids)),
            "approximate_workflows": len(
                {
                    row.workflow_id
                    for row, telemetry in telemetry_rows
                    if bool(telemetry.get("approximate", row.approximate))
                }
            ),
            "avg_reuse_ratio": (
                sum(ratios) / len(ratios)
                if ratios
                else 0.0
            ),
            "max_reuse_ratio": max(ratios) if ratios else 0.0,
            "total_reused_tokens": total_reused_tokens,
            "total_context_tokens": total_context_tokens,
            "total_delta_tokens": total_delta_tokens,
            "total_tier3_reused_tokens": total_tier3_reused_tokens,
            "total_tier3_accepted_pages": total_tier3_accepted_pages,
            "avg_tier3_mean_similarity": (
                float(sum(tier3_mean_similarity_values) / len(tier3_mean_similarity_values))
                if tier3_mean_similarity_values
                else 0.0
            ),
            "relay_substring_hits": relay_substring_hits,
            "relay_substring_misses": relay_substring_misses,
            "delta_prefill_ms_total": float(sum(delta_prefill_values)),
            "delta_prefill_ms_avg": (
                float(sum(delta_prefill_values) / len(delta_prefill_values))
                if delta_prefill_values
                else 0.0
            ),
            "latest_reuse_by_workflow": latest_reuse_by_workflow,
        }

    def active_state_ids(self) -> List[str]:
        with self._lock:
            return sorted(
                state_id
                for state_id, record in self._records.items()
                if record.status != "RELEASED"
            )

    def recover(self) -> Dict[str, WorkflowStateRecord]:
        if self._wal is None:
            return {}
        rows = [
            row
            for row in self._wal.read_all()
            if row.get("kind") == "workflow_registry"
        ]
        merged = merge_wal_rows(rows, group_key="state_id")
        rebuilt: Dict[str, WorkflowStateRecord] = {}
        for state_id, row in merged.items():
            payload = dict(row)
            payload.pop("kind", None)
            payload.pop("event", None)
            payload.pop("ts_unix", None)
            payload.pop("ts_mono", None)
            payload["reuse_telemetry"] = normalize_reuse_telemetry(payload.get("reuse_telemetry"))
            rebuilt[state_id] = WorkflowStateRecord(**payload)
        with self._lock:
            self._records = rebuilt
            return {state_id: replace(record) for state_id, record in rebuilt.items()}

    # ------------------------------------------------------------------
    def _append(self, event: str, record: WorkflowStateRecord) -> None:
        if self._wal is None:
            return
        payload = asdict(record)
        payload.update({"kind": "workflow_registry", "event": event})
        self._wal.append(payload)
