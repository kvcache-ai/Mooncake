from __future__ import annotations

from collections.abc import Sequence
from typing import Callable, Optional
from uuid import uuid4

from ..manifest import (
    WeightPlacementManifest,
    WeightRuntimeBindingManifest,
)
from ...contracts import RuntimeFragmentId
from ..storage_manifest import StoredWeightManifest
from ...lifetime import TerminalTransferState
from ..lifetime import (
    WeightAllocationGuardProviders,
    acquire_weight_binding_token,
)
from .contracts import UploadReceipt, WeightLoadPlan, WeightUploadPlan
from .errors import WeightStoreError
from .backend import (
    StoreBackend,
    StoreConfigFactory,
    default_config_factory,
)
from .load import WeightLoadService
from .payload import PayloadStoreOperations
from .registration import StoreBufferRegistration, StoreRegistrationLease
from .transaction import WeightUploadTransaction
from .snapshot import (
    WeightSnapshotAdapter,
    WeightSnapshotDescriptor,
)
from .writer import (
    WeightStoreWriter,
)
from .upload import WeightUploadService


def _require_upload_plan(plan: WeightUploadPlan) -> None:
    if not isinstance(plan, WeightUploadPlan):
        raise WeightStoreError("plan must be a WeightUploadPlan")


def _require_load_plan(plan: WeightLoadPlan) -> None:
    if not isinstance(plan, WeightLoadPlan):
        raise WeightStoreError("plan must be a WeightLoadPlan")


class WeightStore:
    def __init__(
        self,
        store: object,
        *,
        key_prefix: str = "weights",
        config_factory: Optional[StoreConfigFactory] = None,
        max_range_bytes: int = 64 * 1024 * 1024,
        max_ranges_per_request: int = 1024,
        max_region_segments: int = 1_000_000,
    ) -> None:
        if (
            max_range_bytes <= 0
            or max_ranges_per_request <= 0
            or max_region_segments <= 0
        ):
            raise ValueError("range limits must be positive")
        self.store = StoreBackend(store)
        self.key_prefix = key_prefix.strip("/")
        self.config_factory = config_factory or default_config_factory
        self.max_range_bytes = max_range_bytes
        self.max_ranges_per_request = max_ranges_per_request
        self.max_region_segments = max_region_segments
        self.registration = StoreBufferRegistration(self.store)
        self._payloads = PayloadStoreOperations(self)
        self._transaction = WeightUploadTransaction(self, self._payloads)
        self._upload = WeightUploadService(self, self._payloads, self._transaction)
        self._load = WeightLoadService(self)

    def plan_upload(
        self,
        source_placement: WeightPlacementManifest,
        source_bindings: Sequence[WeightRuntimeBindingManifest],
        *,
        namespace: str = "default",
    ) -> WeightUploadPlan:
        return self._upload.plan_upload(
            source_placement,
            source_bindings,
            namespace=namespace,
        )

    def begin_weight_snapshot(
        self,
        snapshot: WeightSnapshotDescriptor,
        adapter: WeightSnapshotAdapter,
    ) -> WeightStoreWriter:
        """Create a manifest-backed writer for one immutable model snapshot."""

        return WeightStoreWriter(self, snapshot, adapter)

    def upload(
        self,
        plan: WeightUploadPlan,
        source_placement: WeightPlacementManifest,
        source_binding: WeightRuntimeBindingManifest,
        *,
        source_worker_id: Optional[str] = None,
        source_allocation_guards: Optional[WeightAllocationGuardProviders] = None,
        registration_lease: Optional[StoreRegistrationLease] = None,
        transfer_id: Optional[str] = None,
    ) -> tuple[UploadReceipt, ...]:
        _require_upload_plan(plan)
        return self._upload.upload(
            plan,
            source_placement,
            source_binding,
            source_worker_id=source_worker_id,
            source_allocation_guards=source_allocation_guards,
            registration_lease=registration_lease,
            transfer_id=transfer_id,
        )

    def abort_upload(
        self,
        plan: WeightUploadPlan,
        receipts: Sequence[UploadReceipt],
    ) -> None:
        _require_upload_plan(plan)
        self._transaction.abort_upload(plan, receipts)

    def finalize_upload_transaction(self, plan: WeightUploadPlan) -> None:
        _require_upload_plan(plan)
        self._transaction.finalize_upload_transaction(plan)

    def commit_upload(
        self,
        plan: WeightUploadPlan,
        receipts: Sequence[UploadReceipt],
    ) -> StoredWeightManifest:
        _require_upload_plan(plan)
        return self._transaction.commit(plan, receipts)

    def _commit_upload_from_writer(
        self,
        plan: WeightUploadPlan,
        receipts: Sequence[UploadReceipt],
        *,
        on_commit_decision_may_exist: Callable[[], None],
    ) -> StoredWeightManifest:
        _require_upload_plan(plan)
        return self._transaction.commit(
            plan,
            receipts,
            on_commit_decision_may_exist=on_commit_decision_may_exist,
        )

    def load_manifest(self, manifest_key: str) -> StoredWeightManifest:
        return self._load.load_manifest(manifest_key)

    def plan_load(
        self,
        manifest: StoredWeightManifest,
        target_placement: WeightPlacementManifest,
        target_bindings: Sequence[WeightRuntimeBindingManifest],
    ) -> WeightLoadPlan:
        return self._load.plan_load(manifest, target_placement, target_bindings)

    def load(
        self,
        plan: WeightLoadPlan,
        target_placement: WeightPlacementManifest,
        target_binding: WeightRuntimeBindingManifest,
        *,
        target_worker_id: Optional[str] = None,
        target_allocation_guards: Optional[WeightAllocationGuardProviders] = None,
        registration_lease: Optional[StoreRegistrationLease] = None,
        transfer_id: Optional[str] = None,
    ) -> None:
        _require_load_plan(plan)
        self._load.load(
            plan,
            target_placement,
            target_binding,
            target_worker_id=target_worker_id,
            target_allocation_guards=target_allocation_guards,
            registration_lease=registration_lease,
            transfer_id=transfer_id,
        )

    def register_weight_buffers(
        self,
        binding: WeightRuntimeBindingManifest,
        *,
        fragment_ids: Sequence[RuntimeFragmentId],
        allocation_guards: Optional[WeightAllocationGuardProviders],
        side: str,
        transfer_id: Optional[str] = None,
    ) -> StoreRegistrationLease:
        """Create a typed long-lived Store registration under a framework pin."""

        if not isinstance(binding, WeightRuntimeBindingManifest):
            raise WeightStoreError("binding must be a WeightRuntimeBindingManifest")
        requested_ids = tuple(sorted(set(fragment_ids)))
        if not requested_ids:
            raise WeightStoreError("Store registration lease requires fragments")
        try:
            fresh_binding, tokens = acquire_weight_binding_token(
                transfer_id=transfer_id or uuid4().hex,
                expected_binding=binding,
                required_fragment_ids=requested_ids,
                side=side,
                providers=allocation_guards,
            )
        except ValueError as error:
            raise WeightStoreError(str(error)) from error
        fragments_by_id = {
            fragment.fragment_id: fragment for fragment in fresh_binding.fragments
        }
        try:
            fragments = tuple(
                fragments_by_id[fragment_id] for fragment_id in requested_ids
            )
        except KeyError as error:
            tokens.release_after_terminal(TerminalTransferState.ABORTED)
            raise WeightStoreError(
                f"Store registration fragment is missing: {error.args[0]}"
            ) from error
        return self.registration.acquire_lease(fresh_binding, fragments, tokens)

    def pending_registration_ids(self) -> tuple[str, ...]:
        return self.registration.pending_registration_ids()

    def drain_pending_registration(self, pending_registration_id: str) -> None:
        self.registration.drain_pending_registration(pending_registration_id)


__all__ = ["WeightStore", "WeightStoreError"]
