"""Durable, fenced catalog for Store-backed Agent KV state.

``MooncakeKVStateStore`` owns live allocator references while this module owns
the durable *metadata* truth: which workflow owns a state, which immutable
Store pages it references, its lease, and the generation/fencing value that
guards updates.  The split is deliberate: a client-supplied physical block id
is never treated as proof that KV payload is still live.

The catalog is intentionally dependency-free.  A JSON-lines WAL makes the
control-plane state recoverable in a local deployment and gives production
deployments a small interface to replace with a distributed CAS store.  Page
objects are reference-counted by their Store key pair; callers may provide a
reclaimer, but the catalog never guesses how an arbitrary Store client deletes
payloads.
"""

from __future__ import annotations

import copy
import time
from dataclasses import asdict, dataclass, field
from functools import wraps
from threading import RLock
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

from ..control.write_ahead_log import JsonLineWAL, merge_wal_rows
from .kv_manifest import KVManifestError, KVStateManifest


class AgentStateCatalogError(RuntimeError):
    """Raised when a durable Agent-state transition cannot be applied."""


class AgentStateCatalogConflict(AgentStateCatalogError):
    """Raised when a CAS version or workflow fence does not match."""


@dataclass(frozen=True)
class CatalogPageRef:
    """A payload-free identity for one immutable K/V Store page pair."""

    key_store_key: str
    value_store_key: str
    data_checksum: str
    key_nbytes: int
    value_nbytes: int

    @property
    def identity(self) -> str:
        # Store keys are canonical, collision-resistant identities in this
        # adapter.  Include a separator that cannot be introduced by the key
        # generator to avoid ambiguous concatenation.
        return f"{self.key_store_key}\x1f{self.value_store_key}"

    @classmethod
    def from_manifest(cls, manifest: KVStateManifest) -> Optional["CatalogPageRef"]:
        if not manifest.has_store_payload:
            return None
        return cls(
            key_store_key=str(manifest.key_store_key),
            value_store_key=str(manifest.value_store_key),
            data_checksum=str(manifest.data_checksum or ""),
            key_nbytes=int(manifest.key_nbytes or 0),
            value_nbytes=int(manifest.value_nbytes or 0),
        )


@dataclass
class AgentStateCatalogRecord:
    """Durable state alias and immutable-page ownership metadata."""

    state_id: str
    workflow_id: str
    state_store_id: str
    parent_state_id: Optional[str]
    owner_node_id: str
    target_node_id: Optional[str]
    status: str
    catalog_version: int
    fence_token: int
    lease_deadline_unix: float
    manifests: list[dict[str, Any]] = field(default_factory=list)
    clone_payload_bytes: int = 0
    materialized_bytes: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at_unix: float = field(default_factory=time.time)
    updated_at_unix: float = field(default_factory=time.time)
    released_at_unix: Optional[float] = None
    reclaim_page_refs: list[dict[str, Any]] = field(default_factory=list)
    gc_completed: bool = True


PageReclaimer = Callable[[Sequence[CatalogPageRef]], None]


def _synchronized(method):
    """Serialize catalog transitions without burdening every call site."""

    @wraps(method)
    def wrapped(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return wrapped


class AgentStateCatalog:
    """Single-workflow-owner catalog with CAS, lease, WAL and page refcounts.

    The API is deliberately state-store agnostic.  Callers first perform the
    real allocator/Store operation, then record the completed descriptor here.
    If a process restarts, ``recover`` preserves the manifest/page ownership
    facts; a serving worker can rematerialize from those manifests or safely
    release them through the catalog's pending-GC path.
    """

    _ACTIVE_STATUSES = frozenset(
        {
            "ACTIVE",
            "MATERIALIZE_REQUIRED",
            "MATERIALIZED",
            "RECOVERY_PENDING",
        }
    )

    def __init__(
        self,
        wal_path: Optional[str] = None,
        *,
        page_reclaimer: Optional[PageReclaimer] = None,
    ) -> None:
        self._lock = RLock()
        self._records: Dict[str, AgentStateCatalogRecord] = {}
        self._page_refcounts: Dict[str, int] = {}
        self._page_refs: Dict[str, CatalogPageRef] = {}
        self._workflow_fences: Dict[str, int] = {}
        self._page_reclaimer = page_reclaimer
        self._wal = JsonLineWAL(wal_path) if wal_path else None
        if self._wal is not None:
            self.recover()

    # ------------------------------------------------------------------
    # Public state transitions
    # ------------------------------------------------------------------
    @_synchronized
    def register_state(
        self,
        *,
        state_id: str,
        workflow_id: str,
        state_store_id: str,
        manifests: Sequence[KVStateManifest | Mapping[str, Any]],
        owner_node_id: str,
        target_node_id: Optional[str],
        lease_deadline_unix: float,
        status: str = "ACTIVE",
        metadata: Optional[Mapping[str, Any]] = None,
        expected_version: Optional[int] = None,
    ) -> AgentStateCatalogRecord:
        """Register an allocator-owned state after Store export succeeds.

        Repeating the same registration is idempotent.  A different payload
        under the same active state id is a conflict rather than an overwrite.
        """

        normalized = self._validate_manifests(
            manifests,
            state_store_id=str(state_store_id),
            workflow_id=str(workflow_id),
        )
        state_id = self._required_text("state_id", state_id)
        workflow_id = self._required_text("workflow_id", workflow_id)
        state_store_id = self._required_text("state_store_id", state_store_id)
        owner_node_id = self._required_text("owner_node_id", owner_node_id)
        deadline = self._validate_deadline(lease_deadline_unix)
        status = self._normalize_active_status(status)
        existing = self._records.get(state_id)
        if existing is not None and existing.status != "RELEASED":
            self._check_expected_version(existing, expected_version)
            proposed = self._manifest_payloads(normalized)
            if (
                existing.workflow_id == workflow_id
                and existing.state_store_id == state_store_id
                and existing.manifests == proposed
                and existing.owner_node_id == owner_node_id
            ):
                return self._copy_record(existing)
            raise AgentStateCatalogConflict(
                f"state_id already has a live catalog record: {state_id}"
            )
        if existing is not None:
            self._check_expected_version(existing, expected_version)
            if not existing.gc_completed:
                # Reusing an alias whose old immutable objects are still
                # pending deletion can overwrite deterministic Store keys.
                # Drain the old GC first or require the caller to choose a new
                # state id; never silently discard cleanup evidence.
                self.collect_garbage()
                existing = self._records[state_id]
                if not existing.gc_completed:
                    raise AgentStateCatalogConflict(
                        f"released state_id still has pending page GC: {state_id}"
                    )

        now = time.time()
        record = AgentStateCatalogRecord(
            state_id=state_id,
            workflow_id=workflow_id,
            state_store_id=state_store_id,
            parent_state_id=None,
            owner_node_id=owner_node_id,
            target_node_id=str(target_node_id) if target_node_id is not None else None,
            status=status,
            catalog_version=1 if existing is None else existing.catalog_version + 1,
            fence_token=self._next_fence(workflow_id),
            lease_deadline_unix=deadline,
            manifests=self._manifest_payloads(normalized),
            metadata=dict(metadata or {}),
            created_at_unix=now,
            updated_at_unix=now,
        )
        self._records[state_id] = record
        self._retain_pages(record)
        self._append("REGISTERED", record)
        return self._copy_record(record)

    @_synchronized
    def fork_state(
        self,
        *,
        parent_state_id: str,
        child_state_id: str,
        child_state_store_id: str,
        manifests: Sequence[KVStateManifest | Mapping[str, Any]],
        owner_node_id: str,
        target_node_id: Optional[str],
        lease_deadline_unix: Optional[float] = None,
        status: str = "MATERIALIZE_REQUIRED",
        metadata: Optional[Mapping[str, Any]] = None,
        expected_parent_version: Optional[int] = None,
    ) -> AgentStateCatalogRecord:
        """Create a payload-free child manifest alias.

        A fork may change state/version/lease metadata, but its immutable Store
        page identities must exactly match the parent's.  That is the durable
        enforcement behind ``clone_payload_bytes == 0``.
        """

        parent = self._require_live(parent_state_id, expected_parent_version)
        child_state_id = self._required_text("child_state_id", child_state_id)
        child_state_store_id = self._required_text("child_state_store_id", child_state_store_id)
        if child_state_id in self._records and self._records[child_state_id].status != "RELEASED":
            raise AgentStateCatalogConflict(
                f"child_state_id already has a live catalog record: {child_state_id}"
            )
        normalized = self._validate_manifests(
            manifests,
            state_store_id=child_state_store_id,
            workflow_id=parent.workflow_id,
        )
        parent_pages = self._page_identity_set(self._manifests_from_record(parent))
        child_pages = self._page_identity_set(normalized)
        if parent_pages != child_pages:
            raise AgentStateCatalogError(
                "metadata-only Agent fork must retain exactly the parent Store page references"
            )
        deadline = self._validate_deadline(
            parent.lease_deadline_unix if lease_deadline_unix is None else lease_deadline_unix
        )
        now = time.time()
        record = AgentStateCatalogRecord(
            state_id=child_state_id,
            workflow_id=parent.workflow_id,
            state_store_id=child_state_store_id,
            parent_state_id=parent.state_id,
            owner_node_id=self._required_text("owner_node_id", owner_node_id),
            target_node_id=str(target_node_id) if target_node_id is not None else None,
            status=self._normalize_active_status(status),
            catalog_version=1,
            fence_token=self._next_fence(parent.workflow_id),
            lease_deadline_unix=deadline,
            manifests=self._manifest_payloads(normalized),
            clone_payload_bytes=0,
            metadata={**dict(parent.metadata), **dict(metadata or {})},
            created_at_unix=now,
            updated_at_unix=now,
        )
        self._records[child_state_id] = record
        self._retain_pages(record)
        self._append("FORKED", record)
        return self._copy_record(record)

    @_synchronized
    def update_materialized(
        self,
        state_id: str,
        *,
        manifests: Sequence[KVStateManifest | Mapping[str, Any]],
        owner_node_id: str,
        target_node_id: Optional[str],
        materialized_bytes: int,
        metadata: Optional[Mapping[str, Any]] = None,
        expected_version: Optional[int] = None,
    ) -> AgentStateCatalogRecord:
        """CAS-update a state after a real target allocation/import completes."""

        current = self._require_live(state_id, expected_version)
        normalized = self._validate_manifests(
            manifests,
            state_store_id=current.state_store_id,
            workflow_id=current.workflow_id,
        )
        new_payloads = self._manifest_payloads(normalized)
        old_pages = self._page_refs_for_manifests(self._manifests_from_record(current))
        new_pages = self._page_refs_for_manifests(normalized)
        retired = self._replace_pages(old_pages, new_pages)
        current.manifests = new_payloads
        current.owner_node_id = self._required_text("owner_node_id", owner_node_id)
        current.target_node_id = str(target_node_id) if target_node_id is not None else None
        current.status = "MATERIALIZED"
        current.materialized_bytes += max(0, int(materialized_bytes))
        current.catalog_version += 1
        current.fence_token = self._next_fence(current.workflow_id)
        current.updated_at_unix = time.time()
        if metadata:
            current.metadata.update(dict(metadata))
        if retired:
            current.reclaim_page_refs = self._merge_reclaim_refs(
                current.reclaim_page_refs,
                retired,
            )
            current.gc_completed = False
        self._append("MATERIALIZED", current)
        return self._copy_record(current)

    @_synchronized
    def release_state(
        self,
        state_id: str,
        *,
        expected_version: Optional[int] = None,
        reclaim: bool = True,
    ) -> AgentStateCatalogRecord:
        """Idempotently release a state and queue unreferenced page objects.

        The record is journaled as released before the optional data-plane
        reclaimer runs.  If a Store delete fails, the pending keys remain in
        the WAL and can be retried by ``collect_garbage`` after restart.
        """

        record = self._records.get(str(state_id))
        if record is None:
            raise KeyError(f"unknown catalog state_id={state_id}")
        self._check_expected_version(record, expected_version)
        if record.status == "RELEASED":
            if reclaim:
                self.collect_garbage()
            return self._copy_record(self._records[str(state_id)])

        reclaim_refs = self._release_pages(record)
        record.status = "RELEASED"
        record.catalog_version += 1
        record.fence_token = self._next_fence(record.workflow_id)
        record.released_at_unix = time.time()
        record.updated_at_unix = record.released_at_unix
        record.reclaim_page_refs = self._merge_reclaim_refs(
            record.reclaim_page_refs,
            reclaim_refs,
        )
        record.gc_completed = not bool(record.reclaim_page_refs)
        self._append("RELEASED", record)
        if reclaim and record.reclaim_page_refs:
            self.collect_garbage()
        return self._copy_record(self._records[str(state_id)])

    @_synchronized
    def sweep_expired(self, *, now: Optional[float] = None, reclaim: bool = True) -> int:
        """Release active records whose wall-clock lease has expired."""

        now_f = time.time() if now is None else float(now)
        expired = [
            record.state_id
            for record in self._records.values()
            if record.status in self._ACTIVE_STATUSES and record.lease_deadline_unix <= now_f
        ]
        for state_id in expired:
            self.release_state(state_id, reclaim=False)
        if reclaim and expired:
            self.collect_garbage()
        return len(expired)

    @_synchronized
    def collect_garbage(self) -> int:
        """Delete page objects queued by released records when a reclaimer exists."""

        if self._page_reclaimer is None:
            return 0
        candidates: Dict[str, CatalogPageRef] = {}
        for record in self._records.values():
            if record.gc_completed:
                continue
            for payload in record.reclaim_page_refs:
                ref = CatalogPageRef(**dict(payload))
                # A later retry may have revived the same immutable object;
                # never delete a page currently held by an active manifest.
                if self._page_refcounts.get(ref.identity, 0) <= 0:
                    candidates[ref.identity] = ref
        if not candidates:
            return 0
        try:
            self._page_reclaimer(list(candidates.values()))
        except Exception as exc:
            raise AgentStateCatalogError(f"page garbage collection failed: {exc}") from exc

        completed = 0
        candidate_ids = set(candidates)
        for record in self._records.values():
            if record.gc_completed:
                continue
            refs = [CatalogPageRef(**dict(payload)) for payload in record.reclaim_page_refs]
            remaining = [
                ref
                for ref in refs
                if ref.identity not in candidate_ids
            ]
            if len(remaining) == len(refs):
                continue
            completed += len(refs) - len(remaining)
            record.reclaim_page_refs = [asdict(ref) for ref in remaining]
            record.gc_completed = not bool(remaining)
            record.updated_at_unix = time.time()
            self._append("GC_COMPLETED", record)
        return completed

    # ------------------------------------------------------------------
    # Query / recovery
    # ------------------------------------------------------------------
    @_synchronized
    def get_record(self, state_id: str) -> Optional[AgentStateCatalogRecord]:
        record = self._records.get(str(state_id))
        return None if record is None else self._copy_record(record)

    @_synchronized
    def active_records(self) -> list[AgentStateCatalogRecord]:
        return [
            self._copy_record(record)
            for record in sorted(self._records.values(), key=lambda item: item.state_id)
            if record.status in self._ACTIVE_STATUSES
        ]

    @_synchronized
    def stats(self) -> dict[str, Any]:
        records = list(self._records.values())
        return {
            "tracked_states": len(records),
            "active_states": sum(record.status in self._ACTIVE_STATUSES for record in records),
            "released_states": sum(record.status == "RELEASED" for record in records),
            "workflows": len({record.workflow_id for record in records}),
            "active_store_pages": len(self._page_refcounts),
            "active_store_page_references": sum(self._page_refcounts.values()),
            "pending_gc_states": sum(
                not record.gc_completed
                for record in records
            ),
        }

    @_synchronized
    def recover(self) -> dict[str, AgentStateCatalogRecord]:
        """Rebuild records, fences and active page refs from the WAL."""

        if self._wal is None:
            return {}
        rows = [
            row
            for row in self._wal.read_all()
            if row.get("kind") == "agent_state_catalog"
        ]
        merged = merge_wal_rows(rows, group_key="state_id")
        records: Dict[str, AgentStateCatalogRecord] = {}
        for state_id, row in merged.items():
            payload = dict(row)
            for key in ("kind", "event", "ts_unix", "ts_mono"):
                payload.pop(key, None)
            # Older WAL rows cannot create catalog entries; strict construction
            # here deliberately rejects corruption instead of silently making
            # Store pages collectible.
            try:
                record = AgentStateCatalogRecord(**payload)
            except TypeError as exc:
                raise AgentStateCatalogError(
                    f"invalid catalog WAL row for state_id={state_id}: {exc}"
                ) from exc
            records[state_id] = record
        self._records = records
        self._page_refcounts = {}
        self._page_refs = {}
        self._workflow_fences = {}
        for record in records.values():
            self._workflow_fences[record.workflow_id] = max(
                self._workflow_fences.get(record.workflow_id, 0),
                int(record.fence_token),
            )
            if record.status in self._ACTIVE_STATUSES:
                self._retain_pages(record)
        return {state_id: self._copy_record(record) for state_id, record in records.items()}

    # ------------------------------------------------------------------
    # Internal integrity helpers
    # ------------------------------------------------------------------
    def _require_live(
        self,
        state_id: str,
        expected_version: Optional[int],
    ) -> AgentStateCatalogRecord:
        record = self._records.get(str(state_id))
        if record is None:
            raise KeyError(f"unknown catalog state_id={state_id}")
        self._check_expected_version(record, expected_version)
        if record.status not in self._ACTIVE_STATUSES:
            raise AgentStateCatalogError(
                f"catalog state is not live: state_id={state_id} status={record.status}"
            )
        if record.lease_deadline_unix <= time.time():
            raise AgentStateCatalogError(f"catalog state lease expired: {state_id}")
        return record

    @staticmethod
    def _required_text(name: str, value: object) -> str:
        text = str(value or "").strip()
        if not text:
            raise AgentStateCatalogError(f"{name} is required")
        return text

    @staticmethod
    def _validate_deadline(value: float) -> float:
        deadline = float(value)
        if deadline <= 0.0:
            raise AgentStateCatalogError("lease_deadline_unix must be positive")
        return deadline

    @classmethod
    def _normalize_active_status(cls, status: str) -> str:
        value = str(status or "ACTIVE").strip().upper()
        if value not in cls._ACTIVE_STATUSES:
            raise AgentStateCatalogError(f"invalid active catalog status: {value}")
        return value

    @staticmethod
    def _check_expected_version(
        record: AgentStateCatalogRecord,
        expected_version: Optional[int],
    ) -> None:
        if expected_version is not None and int(expected_version) != int(record.catalog_version):
            raise AgentStateCatalogConflict(
                "catalog version conflict for "
                f"state_id={record.state_id}: expected={expected_version} actual={record.catalog_version}"
            )

    def _next_fence(self, workflow_id: str) -> int:
        next_token = int(self._workflow_fences.get(workflow_id, 0)) + 1
        self._workflow_fences[workflow_id] = next_token
        return next_token

    def _validate_manifests(
        self,
        manifests: Sequence[KVStateManifest | Mapping[str, Any]],
        *,
        state_store_id: str,
        workflow_id: str,
    ) -> list[KVStateManifest]:
        if not manifests:
            raise AgentStateCatalogError("Agent state catalog requires at least one KV manifest")
        normalized: list[KVStateManifest] = []
        for raw in manifests:
            try:
                manifest = (
                    raw
                    if isinstance(raw, KVStateManifest)
                    else KVStateManifest.from_control_payload(raw)
                )
                manifest.validate(allow_expired=True)
            except (KVManifestError, TypeError, ValueError) as exc:
                raise AgentStateCatalogError(f"invalid KV state manifest: {exc}") from exc
            if manifest.manifest_checksum() != manifest.checksum:
                raise AgentStateCatalogError(
                    f"manifest checksum mismatch for {manifest.transfer_key}"
                )
            if manifest.state_id != str(state_store_id):
                raise AgentStateCatalogError(
                    "manifest state_id does not bind to state_store_id: "
                    f"{manifest.state_id} != {state_store_id}"
                )
            if manifest.workflow_id != str(workflow_id):
                raise AgentStateCatalogError(
                    "manifest workflow_id does not bind to catalog workflow: "
                    f"{manifest.workflow_id} != {workflow_id}"
                )
            normalized.append(manifest)
        return normalized

    @staticmethod
    def _manifest_payloads(manifests: Sequence[KVStateManifest]) -> list[dict[str, Any]]:
        return [manifest.as_control_payload() for manifest in manifests]

    @staticmethod
    def _manifests_from_record(record: AgentStateCatalogRecord) -> list[KVStateManifest]:
        return [KVStateManifest.from_control_payload(payload) for payload in record.manifests]

    @staticmethod
    def _page_refs_for_manifests(manifests: Sequence[KVStateManifest]) -> dict[str, CatalogPageRef]:
        refs: dict[str, CatalogPageRef] = {}
        for manifest in manifests:
            ref = CatalogPageRef.from_manifest(manifest)
            if ref is not None:
                existing = refs.get(ref.identity)
                if existing is not None and existing != ref:
                    raise AgentStateCatalogError(
                        "Store page identity has inconsistent checksum or size metadata: "
                        f"{ref.key_store_key}"
                    )
                refs[ref.identity] = ref
        return refs

    @classmethod
    def _page_identity_set(cls, manifests: Sequence[KVStateManifest]) -> set[str]:
        return set(cls._page_refs_for_manifests(manifests))

    def _retain_pages(self, record: AgentStateCatalogRecord) -> None:
        for identity, ref in self._page_refs_for_manifests(self._manifests_from_record(record)).items():
            existing = self._page_refs.get(identity)
            if existing is not None and existing != ref:
                raise AgentStateCatalogError(
                    f"Store page metadata conflict for {ref.key_store_key}"
                )
            self._page_refs[identity] = ref
            self._page_refcounts[identity] = self._page_refcounts.get(identity, 0) + 1

    def _release_pages(self, record: AgentStateCatalogRecord) -> list[CatalogPageRef]:
        reclaim: list[CatalogPageRef] = []
        for identity, ref in self._page_refs_for_manifests(self._manifests_from_record(record)).items():
            count = self._page_refcounts.get(identity, 0) - 1
            if count > 0:
                self._page_refcounts[identity] = count
                continue
            self._page_refcounts.pop(identity, None)
            self._page_refs.pop(identity, None)
            reclaim.append(ref)
        return reclaim

    def _replace_pages(
        self,
        old_pages: Mapping[str, CatalogPageRef],
        new_pages: Mapping[str, CatalogPageRef],
    ) -> list[CatalogPageRef]:
        retired: list[CatalogPageRef] = []
        for identity in set(old_pages) - set(new_pages):
            count = self._page_refcounts.get(identity, 0) - 1
            if count > 0:
                self._page_refcounts[identity] = count
            else:
                # The old object becomes unreachable.  It will be reclaimed
                # through the normal release record rather than deleted during
                # an in-place materialization transition.
                self._page_refcounts.pop(identity, None)
                self._page_refs.pop(identity, None)
                retired.append(old_pages[identity])
        for identity in set(new_pages) - set(old_pages):
            ref = new_pages[identity]
            existing = self._page_refs.get(identity)
            if existing is not None and existing != ref:
                raise AgentStateCatalogError(
                    f"Store page metadata conflict for {ref.key_store_key}"
                )
            self._page_refs[identity] = ref
            self._page_refcounts[identity] = self._page_refcounts.get(identity, 0) + 1
        return retired

    @staticmethod
    def _merge_reclaim_refs(
        existing_payloads: Sequence[Mapping[str, Any]],
        added_refs: Sequence[CatalogPageRef],
    ) -> list[dict[str, Any]]:
        merged: dict[str, CatalogPageRef] = {}
        for payload in existing_payloads:
            ref = CatalogPageRef(**dict(payload))
            merged[ref.identity] = ref
        for ref in added_refs:
            merged[ref.identity] = ref
        return [asdict(merged[key]) for key in sorted(merged)]

    def _append(self, event: str, record: AgentStateCatalogRecord) -> None:
        if self._wal is None:
            return
        payload = asdict(record)
        payload.update({"kind": "agent_state_catalog", "event": str(event)})
        self._wal.append(payload)

    @staticmethod
    def _copy_record(record: AgentStateCatalogRecord) -> AgentStateCatalogRecord:
        return copy.deepcopy(record)
