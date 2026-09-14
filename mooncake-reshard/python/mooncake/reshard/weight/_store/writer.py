"""Stateful writer for one manifest-backed model-weight snapshot."""

from __future__ import annotations

from types import TracebackType
from typing import TYPE_CHECKING

from ...contracts import ParticipantId, PlacementFragmentId
from ..storage_manifest import StoredWeightManifest
from .contracts import UploadReceipt, WeightUploadPlan
from .errors import WeightStoreError
from .snapshot import (
    WeightSnapshotAdapter,
    WeightSnapshotDescriptor,
    _require_nonempty_string,
)

if TYPE_CHECKING:
    from .store import WeightStore


class WeightStoreWriter:
    """Accumulate canonical tensor fragments into one immutable model snapshot."""

    def __init__(
        self,
        weight_store: WeightStore,
        snapshot: WeightSnapshotDescriptor,
        adapter: WeightSnapshotAdapter,
    ) -> None:
        self._weight_store = weight_store
        self.snapshot = snapshot
        self._adapter = adapter
        self._source = adapter.export_source(snapshot)
        self._placement = self._source.placement
        self._bindings = tuple(self._source.bindings)
        self._validate_source_identity()
        self._plan = weight_store.plan_upload(
            self._placement,
            self._bindings,
            namespace=snapshot.namespace,
        )
        self._binding_by_participant = {
            binding.participant_id: binding for binding in self._bindings
        }
        self._operation_by_placement_id = {
            operation.source_placement.placement_fragment_id: operation
            for operation in self._plan.operations
        }
        if len(self._operation_by_placement_id) != len(self._plan.operations):
            raise WeightStoreError("snapshot plan has duplicate placement fragments")
        self._required_by_participant: dict[
            ParticipantId, set[PlacementFragmentId]
        ] = {}
        for operation in self._plan.operations:
            self._required_by_participant.setdefault(
                operation.source_participant_id, set()
            ).add(operation.source_placement.placement_fragment_id)
        self._seen_by_participant: dict[ParticipantId, set[PlacementFragmentId]] = {
            participant_id: set() for participant_id in self._required_by_participant
        }
        self._flushed_participants: set[ParticipantId] = set()
        self._receipts: list[UploadReceipt] = []
        self._closed = False
        self._committed = False
        self._commit_decision_may_exist = False

    @property
    def plan(self) -> WeightUploadPlan:
        """Return the immutable upload plan owned by this writer."""

        return self._plan

    def write_tensor(
        self,
        tensor_id: str,
        tensor: object,
    ) -> tuple[UploadReceipt, ...]:
        """Validate and write a framework-owned tensor fragment."""

        self._require_open()
        _require_nonempty_string(tensor_id, "tensor_id")
        resolved = tuple(
            self._adapter.resolve_fragment_ids(
                tensor_id=tensor_id,
                tensor=tensor,
                source=self._source,
            )
        )
        if not resolved:
            raise WeightStoreError("weight snapshot tensor resolved no fragments")
        if len(set(resolved)) != len(resolved):
            raise WeightStoreError(
                "weight snapshot tensor resolved duplicate fragments"
            )

        flushed: list[UploadReceipt] = []
        try:
            for placement_fragment_id in resolved:
                operation = self._operation_by_placement_id.get(placement_fragment_id)
                if operation is None:
                    raise WeightStoreError(
                        "weight snapshot tensor resolved a fragment outside this snapshot: "
                        f"{placement_fragment_id}"
                    )
                if operation.source_placement.tensor_id != tensor_id:
                    raise WeightStoreError(
                        "weight snapshot tensor resolved a fragment for another tensor: "
                        f"{placement_fragment_id}"
                    )
                participant_id = operation.source_participant_id
                seen = self._seen_by_participant[participant_id]
                if placement_fragment_id in seen:
                    raise WeightStoreError(
                        "weight snapshot tensor submitted a duplicate fragment: "
                        f"{placement_fragment_id}"
                    )
                seen.add(placement_fragment_id)
                if (
                    seen == self._required_by_participant[participant_id]
                    and participant_id not in self._flushed_participants
                ):
                    flushed.extend(self._flush_participant(participant_id))
        except BaseException:
            self.abort()
            raise
        return tuple(flushed)

    def commit(self) -> StoredWeightManifest:
        """Publish the manifest only after every selected fragment is written."""

        self._require_open()
        missing = tuple(
            sorted(
                placement_fragment_id
                for participant_id, required in self._required_by_participant.items()
                for placement_fragment_id in required
                if placement_fragment_id
                not in self._seen_by_participant[participant_id]
            )
        )
        if missing:
            self.abort()
            raise WeightStoreError(
                "Weight snapshot is missing required fragments: " + ", ".join(missing)
            )
        if set(self._required_by_participant) != self._flushed_participants:
            self.abort()
            raise WeightStoreError("Weight snapshot has unflushed participants")
        manifest = self._weight_store._commit_upload_from_writer(
            self._plan,
            self._receipts,
            on_commit_decision_may_exist=self._mark_commit_decision_may_exist,
        )
        self._closed = True
        self._committed = True
        return manifest

    def abort(self) -> None:
        """Remove uploaded payloads unless this writer has already committed."""

        if self._closed:
            return
        if self._commit_decision_may_exist:
            raise WeightStoreError(
                "Weight snapshot commit decision may exist; retry commit instead"
            )
        self._closed = True
        if self._receipts:
            self._weight_store.abort_upload(self._plan, self._receipts)

    def _mark_commit_decision_may_exist(self) -> None:
        self._commit_decision_may_exist = True

    def _validate_source_identity(self) -> None:
        placement = self._placement
        if (
            placement.resource_id != self.snapshot.resource_id
            or placement.revision != self.snapshot.revision
            or placement.weight_generation != self.snapshot.weight_generation
        ):
            raise WeightStoreError("snapshot identity differs from source placement")

    def _flush_participant(
        self,
        participant_id: ParticipantId,
    ) -> tuple[UploadReceipt, ...]:
        binding = self._binding_by_participant.get(participant_id)
        if binding is None:
            raise WeightStoreError(
                f"snapshot source binding is missing: {participant_id}"
            )
        receipts = self._weight_store.upload(
            self._plan,
            self._placement,
            binding,
            source_allocation_guards=self._adapter.source_allocation_guards(binding),
        )
        self._receipts.extend(receipts)
        self._flushed_participants.add(participant_id)
        return receipts

    def _require_open(self) -> None:
        if self._committed:
            raise WeightStoreError("Weight snapshot has already committed")
        if self._closed:
            raise WeightStoreError("Weight snapshot is closed")

    def __enter__(self) -> WeightStoreWriter:
        self._require_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if exc_type is None:
            if not self._committed:
                self.commit()
        elif not self._commit_decision_may_exist:
            self.abort()
        return False


__all__ = [
    "WeightStoreWriter",
]
