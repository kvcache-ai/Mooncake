"""Offload manager: KV eviction during tool-call blocking (RFC §6.3)."""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import torch

from ...core.control.write_ahead_log import JsonLineWAL, merge_wal_rows
from ...core.state import MultimodalState, StateLayer


@dataclass
class OffloadedState:
    handle: str
    state_id: str
    kv_refs_cpu: List = field(default_factory=list)  # list[(k_cpu, v_cpu)]
    blocked_at: float = field(default_factory=time.monotonic)
    expected_return: float = 0.0
    leased_block_ids: List[str] = field(default_factory=list)
    done: threading.Event = field(default_factory=threading.Event, repr=False)
    abort_requested: bool = False
    completed: bool = False
    phase: str = "COPYING"
    checkpoint_path: Optional[str] = None
    bytes_copied: int = 0
    metadata: Dict = field(default_factory=dict)


class OffloadManager:
    """Moves blocked states to host memory and restores them on demand."""

    def __init__(
        self,
        state_layer: StateLayer,
        device: torch.device,
        default_ttl_seconds: float = 60.0,
        *,
        wal_path: Optional[str] = None,
        spill_dir: Optional[str] = None,
        recover_on_start: bool = False,
    ):
        self.sl = state_layer
        self.device = device
        self.default_ttl_seconds = default_ttl_seconds
        self._offloaded: Dict[str, OffloadedState] = {}
        self._lock = threading.RLock()
        self._stats = {
            "offloaded": 0,
            "restored": 0,
            "aborted": 0,
            "expired": 0,
            "bytes_freed": 0,
            "recoveries": 0,
        }
        self._wal = JsonLineWAL(wal_path) if wal_path else None
        self._spill_dir = Path(spill_dir) if spill_dir else None
        if self._spill_dir is not None:
            self._spill_dir.mkdir(parents=True, exist_ok=True)
        if recover_on_start:
            self.recover()

    # ------------------------------------------------------------------
    # Offload lifecycle
    # ------------------------------------------------------------------
    def start_offload(
        self,
        state: MultimodalState,
        expected_return_seconds: Optional[float] = None,
        *,
        copy_delay_seconds: float = 0.0,
    ) -> str:
        handle = state.state_id
        expected_return = time.monotonic() + (
            expected_return_seconds or self.default_ttl_seconds
        )
        entry = OffloadedState(
            handle=handle,
            state_id=state.state_id,
            leased_block_ids=[
                ref.global_block_id or f"local:{ref.physical_id}"
                for ref in state.kv_refs
            ],
            expected_return=expected_return,
            metadata={
                "workflow_id": state.workflow_id,
                "version_id": state.version_id,
                "snapshot_epoch": state.snapshot_epoch,
            },
        )
        with self._lock:
            self._offloaded[handle] = entry
        for block_id in entry.leased_block_ids:
            self.sl.pm.kv_directory.lease(block_id)
        state.status = "OFFLOADING"
        if self.sl.workflow_registry is not None:
            self.sl.workflow_registry.transition(
                state.state_id,
                status="OFFLOADING",
                offload_handle=handle,
                event="OFFLOAD_STARTED",
            )
        self._log(
            "STARTED",
            handle=handle,
            state_id=state.state_id,
            leased_block_ids=list(entry.leased_block_ids),
            expected_return=expected_return,
        )

        worker = threading.Thread(
            target=self._finish_offload,
            args=(state, entry, copy_delay_seconds),
            daemon=True,
        )
        worker.start()
        return handle

    def _finish_offload(
        self,
        state: MultimodalState,
        entry: OffloadedState,
        copy_delay_seconds: float,
    ) -> None:
        pm = self.sl.pm
        cpu_pages = []
        total_bytes = 0
        refs_snapshot = list(state.kv_refs)
        try:
            for ref in refs_snapshot:
                k, v = pm.get_page(ref)
                k_cpu = k.detach().to("cpu", copy=True)
                v_cpu = v.detach().to("cpu", copy=True)
                cpu_pages.append((k_cpu, v_cpu))
                total_bytes += k_cpu.nelement() * k_cpu.element_size()
                total_bytes += v_cpu.nelement() * v_cpu.element_size()
                if copy_delay_seconds > 0:
                    time.sleep(copy_delay_seconds)

            with self._lock:
                latest = self._offloaded.get(entry.handle)
                abort_requested = bool(latest and latest.abort_requested)
            if abort_requested:
                state.status = "ACTIVE"
                entry.phase = "ABORTED"
                if self.sl.workflow_registry is not None:
                    self.sl.workflow_registry.transition(
                        state.state_id,
                        status="ACTIVE",
                        clear_offload=True,
                        event="OFFLOAD_ABORTED",
                    )
                self._log("ABORTED", handle=entry.handle, state_id=state.state_id)
                with self._lock:
                    latest = self._offloaded.get(entry.handle)
                    if latest is not None:
                        latest.phase = "ABORTED"
                        latest.completed = False
                        latest.done.set()
                        self._stats["aborted"] += 1
                return

            checkpoint_path = self._persist_checkpoint(entry.handle, cpu_pages)
            self.sl.offload(state)
            state.status = "OFFLOADED"
            entry.kv_refs_cpu = cpu_pages
            entry.bytes_copied = total_bytes
            entry.checkpoint_path = checkpoint_path
            entry.phase = "OFFLOADED"
            if self.sl.workflow_registry is not None:
                self.sl.workflow_registry.transition(
                    state.state_id,
                    status="OFFLOADED",
                    offload_handle=entry.handle,
                    offload_checkpoint_path=checkpoint_path,
                    event="OFFLOAD_COMPLETED",
                )
            self._log(
                "COMPLETED",
                handle=entry.handle,
                state_id=state.state_id,
                bytes_copied=total_bytes,
                checkpoint_path=checkpoint_path,
                leased_block_ids=list(entry.leased_block_ids),
                expected_return=entry.expected_return,
            )
            with self._lock:
                latest = self._offloaded.get(entry.handle)
                if latest is not None:
                    latest.kv_refs_cpu = cpu_pages
                    latest.bytes_copied = total_bytes
                    latest.checkpoint_path = checkpoint_path
                    latest.phase = "OFFLOADED"
                    latest.completed = True
                    latest.done.set()
                    self._stats["offloaded"] += 1
                    self._stats["bytes_freed"] += total_bytes
        finally:
            self._release_leases(entry)
            with self._lock:
                latest = self._offloaded.get(entry.handle)
                if latest is not None and not latest.done.is_set():
                    latest.done.set()

    def await_offload(self, handle: str, timeout: Optional[float] = None) -> bool:
        with self._lock:
            entry = self._offloaded.get(handle)
        if entry is None:
            return False
        return entry.done.wait(timeout=timeout)

    def offload(
        self,
        state: MultimodalState,
        expected_return_seconds: Optional[float] = None,
    ) -> str:
        handle = self.start_offload(
            state,
            expected_return_seconds=expected_return_seconds,
        )
        self.await_offload(handle, timeout=60.0)
        return handle

    def restore(self, handle: str) -> Optional[MultimodalState]:
        with self._lock:
            entry = self._offloaded.get(handle)
        if entry is None:
            return None

        if not entry.completed:
            with self._lock:
                entry = self._offloaded.get(handle)
                if entry is None:
                    return None
                entry.abort_requested = True
            entry.done.wait(timeout=60.0)
            with self._lock:
                self._offloaded.pop(handle, None)
            state = self.sl.get_state(entry.state_id)
            if state is None and self.sl.workflow_registry is not None:
                record = self.sl.workflow_registry.get_record(entry.state_id)
                if record is not None:
                    state = self.sl.recreate_state_from_record(record)
            if state is not None:
                state.status = "ACTIVE"
                if self.sl.workflow_registry is not None:
                    self.sl.workflow_registry.transition(
                        state.state_id,
                        status="ACTIVE",
                        clear_offload=True,
                        event="OFFLOAD_ABORTED_RESTORE",
                    )
            self._cleanup_checkpoint(entry)
            return state

        pages = list(entry.kv_refs_cpu)
        if not pages and entry.checkpoint_path:
            payload = torch.load(entry.checkpoint_path, map_location="cpu")
            pages = list(payload.get("kv_refs_cpu", []))
            entry.kv_refs_cpu = pages
        if not pages:
            state = self.sl.get_state(entry.state_id)
            if state is not None:
                state.status = "ABORTED"
            return None

        state = self.sl.get_state(entry.state_id)
        if state is None:
            if self.sl.workflow_registry is not None:
                record = self.sl.workflow_registry.get_record(entry.state_id)
                if record is not None:
                    state = self.sl.recreate_state_from_record(record)
        if state is None:
            self._cleanup_checkpoint(entry)
            with self._lock:
                self._offloaded.pop(handle, None)
            return None

        state.status = "RESTORING"
        pm = self.sl.pm
        new_refs = []
        for k_cpu, v_cpu in pages:
            k = k_cpu.to(self.device)
            v = v_cpu.to(self.device)
            filled = k.shape[-2]
            ref = pm.allocate_page(filled=filled)
            pm.write_page_slots(ref, k, v, offset=0)
            new_refs.append(ref)

        state.kv_refs = new_refs
        self.sl.refresh_ttl(state)
        state.status = "ACTIVE"
        if self.sl.workflow_registry is not None:
            self.sl.workflow_registry.transition(
                state.state_id,
                status="ACTIVE",
                clear_offload=True,
                event="OFFLOAD_RESTORED",
            )
        with self._lock:
            self._offloaded.pop(handle, None)
            self._stats["restored"] += 1
        self._cleanup_checkpoint(entry)
        self._log("RESTORED", handle=handle, state_id=entry.state_id)
        return state

    # ------------------------------------------------------------------
    # Agent integration
    # ------------------------------------------------------------------
    def on_tool_call(
        self,
        state: MultimodalState,
        expected_duration: Optional[float] = None,
    ) -> str:
        return self.offload(state, expected_return_seconds=expected_duration)

    def on_tool_return(self, handle: str) -> Optional[MultimodalState]:
        return self.restore(handle)

    # ------------------------------------------------------------------
    # Expiry / recovery / stats
    # ------------------------------------------------------------------
    def sweep_expired(self) -> int:
        now = time.monotonic()
        with self._lock:
            expired = [
                h
                for h, e in self._offloaded.items()
                if e.expected_return <= now and e.completed
            ]
        freed = 0
        for h in expired:
            with self._lock:
                entry = self._offloaded.pop(h, None)
            if entry is None:
                continue
            state = self.sl.get_state(entry.state_id)
            if state is None and self.sl.workflow_registry is not None:
                record = self.sl.workflow_registry.get_record(entry.state_id)
                if record is not None:
                    state = self.sl.recreate_state_from_record(record)
            if state is not None:
                state.status = "ABORTED"
                self.sl.release(state)
            elif self.sl.workflow_registry is not None:
                self.sl.workflow_registry.transition(
                    entry.state_id,
                    status="ABORTED",
                    clear_offload=True,
                    event="OFFLOAD_EXPIRED",
                )
            self._cleanup_checkpoint(entry)
            self._stats["expired"] += 1
            self._log("EXPIRED", handle=h, state_id=entry.state_id)
            freed += 1
        return freed

    def recover(self) -> Dict[str, str]:
        if self._wal is None:
            return {}
        merged = merge_wal_rows(
            [
                row
                for row in self._wal.read_all()
                if row.get("kind") == "offload"
            ],
            group_key="handle",
        )
        recovered: Dict[str, str] = {}
        for handle, row in merged.items():
            action = str(row.get("action", "")).upper()
            state_id = str(row.get("state_id", handle))
            state = self.sl.get_state(state_id)
            if state is None and self.sl.workflow_registry is not None:
                record = self.sl.workflow_registry.get_record(state_id)
                if record is not None:
                    state = self.sl.recreate_state_from_record(record)
            leased_block_ids = list(row.get("leased_block_ids", []))
            checkpoint_path = row.get("checkpoint_path")
            if action == "COMPLETED":
                entry = OffloadedState(
                    handle=handle,
                    state_id=state_id,
                    leased_block_ids=leased_block_ids,
                    expected_return=float(
                        row.get(
                            "expected_return",
                            time.monotonic() + self.default_ttl_seconds,
                        )
                    ),
                    completed=True,
                    phase="OFFLOADED",
                    checkpoint_path=checkpoint_path,
                    bytes_copied=int(row.get("bytes_copied", 0) or 0),
                )
                entry.done.set()
                with self._lock:
                    self._offloaded[handle] = entry
                    self._stats["recoveries"] += 1
                if state is not None:
                    state.status = "OFFLOADED"
                if self.sl.workflow_registry is not None:
                    self.sl.workflow_registry.transition(
                        state_id,
                        status="OFFLOADED",
                        offload_handle=handle,
                        offload_checkpoint_path=checkpoint_path,
                        event="OFFLOAD_RECOVERED",
                    )
                recovered[handle] = "RECOVERED_OFFLOADED"
            elif action == "STARTED":
                for block_id in leased_block_ids:
                    self.sl.pm.kv_directory.release_lease(block_id)
                if state is not None:
                    state.status = "ACTIVE"
                if self.sl.workflow_registry is not None:
                    self.sl.workflow_registry.transition(
                        state_id,
                        status="ACTIVE",
                        clear_offload=True,
                        event="OFFLOAD_RECOVERED_ABORT",
                    )
                recovered[handle] = "RECOVERED_ABORTED_INFLIGHT"
                self._log("RECOVERED_ABORT", handle=handle, state_id=state_id)
            elif action in {"ABORTED", "RESTORED", "EXPIRED", "RECOVERED_ABORT"}:
                if checkpoint_path:
                    self._safe_unlink(checkpoint_path)
                if state is not None and state.status != "RELEASED":
                    state.status = "ACTIVE"
                if self.sl.workflow_registry is not None:
                    self.sl.workflow_registry.transition(
                        state_id,
                        status="ACTIVE",
                        clear_offload=True,
                        event="OFFLOAD_RECOVERED_CLEAN",
                    )
                recovered[handle] = action
        return recovered

    def stats(self) -> Dict:
        with self._lock:
            return dict(self._stats)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _persist_checkpoint(self, handle: str, cpu_pages: List) -> Optional[str]:
        if self._spill_dir is None:
            return None
        path = self._spill_dir / f"{handle}.pt"
        torch.save({"kv_refs_cpu": cpu_pages}, path)
        return str(path)

    def _cleanup_checkpoint(self, entry: OffloadedState) -> None:
        if entry.checkpoint_path:
            self._safe_unlink(entry.checkpoint_path)
            entry.checkpoint_path = None

    @staticmethod
    def _safe_unlink(path: str) -> None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    def _release_leases(self, entry: OffloadedState) -> None:
        for block_id in entry.leased_block_ids:
            self.sl.pm.kv_directory.release_lease(block_id)

    def _log(self, action: str, **payload) -> None:
        if self._wal is None:
            return
        self._wal.append({"kind": "offload", "action": action, **payload})
