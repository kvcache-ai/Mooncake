from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional, Sequence

from ..._typing import TypeAlias
from urllib.parse import quote
from uuid import uuid4

from .._planner.ownership import (
    complete_parallel_source_replicas,
    has_dp_ownership,
    parallel_tensor_owner,
)
from ...contracts import (
    ResourceId,
    RevisionId,
    RuntimeFragmentId,
    StoredFragmentSnapshotId,
    TensorId,
)
from ..manifest import (
    PlacementFragment,
    RuntimeBindingFragment,
    TensorDescriptor,
    WeightPlacementManifest,
    WeightRuntimeBindingManifest,
)
from ..storage_manifest import StoredFragmentSnapshot, StoredWeightManifest
from ..planner import RuntimeFragmentSnapshot
from ...lifetime import AllocationTokenSet, TerminalTransferState
from ..lifetime import (
    WeightAllocationGuardProviders,
    acquire_weight_binding_token,
)
from .contracts import UploadOperation, UploadReceipt, WeightUploadPlan
from .errors import WeightStoreError
from .payload import PayloadStoreOperations
from .transaction import WeightUploadTransaction
from .registration import StoreRegistrationLease
from .validation import (
    pair_manifests,
    same_runtime_snapshot,
    validate_manifest_pair,
)

if TYPE_CHECKING:
    from .store import WeightStore


UploadSource = tuple[
    PlacementFragment,
    RuntimeBindingFragment,
    WeightRuntimeBindingManifest,
]
UploadGeometryKey: TypeAlias = tuple[TensorId, tuple[int, ...], tuple[int, ...]]
UploadSortKey: TypeAlias = tuple[int, int, int, int, str, str]


def _runtime_sort_key(source: UploadSource) -> UploadSortKey:
    placement, binding, _ = source
    return (
        placement.rank.dp,
        placement.rank.pp,
        placement.rank.ep,
        placement.rank.tp,
        binding.worker_id,
        binding.fragment_id,
    )


def _geometry_key(fragment: PlacementFragment) -> UploadGeometryKey:
    return fragment.tensor_id, fragment.global_offset, fragment.local_shape


def _collect_upload_sources(
    placement: WeightPlacementManifest,
    bindings: Sequence[WeightRuntimeBindingManifest],
) -> tuple[
    ResourceId,
    RevisionId,
    int,
    tuple[TensorDescriptor, ...],
    list[UploadSource],
]:
    pairs = pair_manifests(placement, tuple(bindings), "source")
    resource_id = pairs[0][0].resource_id
    revision = pairs[0][0].revision
    weight_generation = pairs[0][0].weight_generation
    tensor_by_id: dict[TensorId, TensorDescriptor] = {
        tensor.tensor_id: tensor for tensor in placement.tensors
    }
    if any(has_dp_ownership(tensor) for tensor in tensor_by_id.values()):
        raise ValueError(
            "Weight Store upload requires replicated DP source tensors; "
            "DP-owned tensor snapshots are not supported"
        )
    sources: list[UploadSource] = []
    runtime_fragment_ids: set[RuntimeFragmentId] = set()
    for placement, binding in pairs:
        part = next(
            item
            for item in placement.parts
            if item.participant_id == binding.participant_id
        )
        for tensor in part.tensors:
            previous = tensor_by_id.setdefault(tensor.tensor_id, tensor)
            if previous != tensor:
                raise ValueError(f"tensor descriptor mismatch: {tensor.tensor_id}")
        runtime_by_placement_id = {
            fragment.placement_fragment_id: fragment for fragment in binding.fragments
        }
        for fragment in part.fragments:
            runtime_fragment = runtime_by_placement_id[fragment.placement_fragment_id]
            if runtime_fragment.fragment_id in runtime_fragment_ids:
                raise ValueError(
                    f"duplicate source fragment_id: {runtime_fragment.fragment_id}"
                )
            runtime_fragment_ids.add(runtime_fragment.fragment_id)
            sources.append((fragment, runtime_fragment, binding))

    tensors = tuple(sorted(tensor_by_id.values(), key=lambda item: item.tensor_id))
    try:
        complete_replicas = complete_parallel_source_replicas(
            tensor_by_id,
            [placement_fragment for placement_fragment, _, _ in sources],
        )
    except ValueError as error:
        if str(error) == (
            "source manifests have no complete DP replica; "
            "tensors are not fully covered"
        ):
            raise ValueError(
                "source manifests have no complete generation-consistent DP replica"
            ) from error
        raise

    generations_by_dp: dict[int, int] = {}
    for dp_rank, replica_owners in complete_replicas.items():
        generations = {
            binding_manifest.generation
            for placement_fragment, _, binding_manifest in sources
            if placement_fragment.rank.dp == dp_rank
            and parallel_tensor_owner(
                tensor_by_id[placement_fragment.tensor_id], placement_fragment
            )
            == replica_owners[placement_fragment.tensor_id]
        }
        if len(generations) == 1:
            generations_by_dp[dp_rank] = next(iter(generations))
    if not generations_by_dp:
        raise ValueError(
            "source manifests have no complete generation-consistent DP replica"
        )
    if len(set(generations_by_dp.values())) != 1:
        raise ValueError(
            "complete source DP replicas have inconsistent lease generations"
        )

    selected_dp = min(generations_by_dp)
    owner_by_tensor = complete_replicas[selected_dp]
    candidates: dict[UploadGeometryKey, list[UploadSource]] = {}
    for source in sources:
        placement_fragment, _, binding_manifest = source
        tensor = tensor_by_id[placement_fragment.tensor_id]
        if (
            placement_fragment.rank.dp != selected_dp
            or binding_manifest.generation != generations_by_dp[selected_dp]
            or parallel_tensor_owner(tensor, placement_fragment)
            != owner_by_tensor[placement_fragment.tensor_id]
        ):
            continue
        candidates.setdefault(_geometry_key(placement_fragment), []).append(source)

    selected: list[UploadSource] = []
    for group in candidates.values():
        group.sort(key=_runtime_sort_key)
        selected.append(group[0])
    selected.sort(
        key=lambda source: (
            source[0].tensor_id,
            source[0].global_offset,
            source[0].local_shape,
        )
    )
    return resource_id, revision, weight_generation, tensors, selected


def _safe_segment(value: str) -> str:
    return quote(value, safe="._-")


def _fragment_digest(fragment: PlacementFragment) -> StoredFragmentSnapshotId:
    value = (
        f"{fragment.tensor_id}|{fragment.global_offset}|{fragment.local_shape}"
    ).encode()
    return StoredFragmentSnapshotId(hashlib.sha256(value).hexdigest()[:24])


def plan_weight_upload(
    source_placement: WeightPlacementManifest,
    source_bindings: Sequence[WeightRuntimeBindingManifest],
    *,
    namespace: str = "default",
    key_prefix: str = "weights",
) -> WeightUploadPlan:
    """Build an address-free Store upload plan from complete source manifests."""

    if type(namespace) is not str or not namespace:
        raise ValueError("namespace must be a non-empty string")
    if type(key_prefix) is not str or not key_prefix.strip("/"):
        raise ValueError("key_prefix must contain a non-empty path segment")
    resource_id, revision, weight_generation, tensors, sources = (
        _collect_upload_sources(
            source_placement,
            source_bindings,
        )
    )
    base_key = "/".join(
        (
            key_prefix.strip("/"),
            _safe_segment(namespace),
            _safe_segment(resource_id),
            _safe_segment(revision),
            str(weight_generation),
        )
    )
    transaction_id = uuid4().hex
    stored_fragments: list[StoredFragmentSnapshot] = []
    operations: list[UploadOperation] = []
    for placement_fragment, source_binding, binding_manifest in sources:
        fragment_id = _fragment_digest(placement_fragment)
        target = StoredFragmentSnapshot(
            fragment_id=fragment_id,
            tensor_id=placement_fragment.tensor_id,
            global_offset=placement_fragment.global_offset,
            local_shape=placement_fragment.local_shape,
            object_key=f"{base_key}/payload/{transaction_id}/{fragment_id}",
            object_offset=0,
            nbytes=placement_fragment.nbytes,
            aliases=placement_fragment.aliases,
        )
        stored_fragments.append(target)
        operations.append(
            UploadOperation(
                source_placement=placement_fragment,
                source_snapshot=RuntimeFragmentSnapshot.from_attested_pair(
                    placement_fragment,
                    source_binding,
                    lease_generation=binding_manifest.generation,
                ),
                source_participant_id=binding_manifest.participant_id,
                source_instance_id=binding_manifest.instance_id,
                target=target,
                source_generation=binding_manifest.generation,
                source_lease_id=binding_manifest.lease_id,
            )
        )
    manifest = StoredWeightManifest(
        namespace=namespace,
        resource_id=resource_id,
        revision=revision,
        weight_generation=weight_generation,
        group_id=base_key,
        manifest_key=f"{base_key}/manifest",
        tensors=tensors,
        fragments=tuple(stored_fragments),
        created_at=datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
    )
    return WeightUploadPlan(
        manifest=manifest,
        source_placement_id=source_placement.placement_id,
        source_placement_digest=source_placement.digest,
        transaction_group_id=f"{base_key}/transactions/{transaction_id}",
        control_key=f"{base_key}/transactions/{transaction_id}/decision",
        operations=tuple(operations),
    )


class WeightUploadService:
    def __init__(
        self,
        client: WeightStore,
        payloads: PayloadStoreOperations,
        transaction: WeightUploadTransaction,
    ) -> None:
        self.client = client
        self.payloads = payloads
        self.transaction = transaction

    def plan_upload(
        self,
        source_placement: WeightPlacementManifest,
        source_bindings: Sequence[WeightRuntimeBindingManifest],
        *,
        namespace: str = "default",
    ) -> WeightUploadPlan:
        return plan_weight_upload(
            source_placement,
            source_bindings,
            namespace=namespace,
            key_prefix=self.client.key_prefix,
        )

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
        validate_manifest_pair(source_placement, source_binding, "source")
        if source_placement.resource_id != plan.manifest.resource_id or (
            source_placement.revision != plan.manifest.revision
        ):
            raise WeightStoreError("source placement revision mismatch")
        if source_placement.weight_generation != plan.manifest.weight_generation:
            raise WeightStoreError("source placement weight generation mismatch")
        if (
            source_placement.placement_id != plan.source_placement_id
            or source_placement.digest != plan.source_placement_digest
        ):
            raise WeightStoreError("source placement identity mismatch")
        local = {
            fragment.fragment_id: fragment for fragment in source_binding.fragments
        }
        source_part = next(
            part
            for part in source_placement.parts
            if part.participant_id == source_binding.participant_id
        )
        local_placements = {
            fragment.placement_fragment_id: fragment
            for fragment in source_part.fragments
        }
        expected_operations = [
            operation
            for operation in plan.operations
            if operation.source_participant_id == source_binding.participant_id
        ]
        if not expected_operations:
            return ()
        available_workers = sorted(
            {operation.source_snapshot.worker_id for operation in expected_operations}
        )
        if source_worker_id is None:
            if len(available_workers) != 1:
                raise WeightStoreError(
                    "source worker selector is required for a multi-worker binding"
                )
            source_worker_id = available_workers[0]
        elif type(source_worker_id) is not str or not source_worker_id:
            raise WeightStoreError("source_worker_id must be a non-empty string")
        elif source_worker_id not in available_workers:
            raise WeightStoreError(f"unknown source worker: {source_worker_id}")
        expected_operations = [
            operation
            for operation in expected_operations
            if operation.source_snapshot.worker_id == source_worker_id
        ]
        if any(
            operation.source_instance_id != source_binding.instance_id
            for operation in expected_operations
        ):
            raise WeightStoreError(
                f"stale source instance: {source_binding.participant_id}"
            )
        missing = {
            operation.source_snapshot.fragment_id
            for operation in expected_operations
            if operation.source_snapshot.fragment_id not in local
        }
        if missing:
            raise WeightStoreError(
                f"missing planned source fragment: {', '.join(sorted(missing))}"
            )
        local_operations: list[tuple[UploadOperation, RuntimeBindingFragment]] = []
        for operation in expected_operations:
            current = local.get(operation.source_snapshot.fragment_id)
            if current is None:
                raise AssertionError("planned source fragment was not resolved")
            current_placement = local_placements.get(
                operation.source_placement.placement_fragment_id
            )
            if current_placement != operation.source_placement:
                raise WeightStoreError(
                    "stale source placement: "
                    f"{operation.source_placement.placement_fragment_id}"
                )
            if (
                source_binding.lease_id != operation.source_lease_id
                or source_binding.generation != operation.source_generation
            ):
                raise WeightStoreError(
                    f"stale source lease: {operation.source_snapshot.fragment_id}"
                )
            if not same_runtime_snapshot(current, operation.source_snapshot):
                raise WeightStoreError(
                    f"stale source fragment: {operation.source_snapshot.fragment_id}"
                )
            local_operations.append((operation, current))
        self.transaction.require_writable(plan)
        required_fragment_ids = tuple(
            sorted(
                {
                    operation.source_snapshot.fragment_id
                    for operation, _ in local_operations
                }
            )
        )
        lifetime_tokens: Optional[AllocationTokenSet] = None
        if registration_lease is None:
            try:
                fresh_binding, lifetime_tokens = acquire_weight_binding_token(
                    transfer_id=transfer_id or uuid4().hex,
                    expected_binding=source_binding,
                    required_fragment_ids=required_fragment_ids,
                    side="source",
                    providers=source_allocation_guards,
                )
            except ValueError as error:
                raise WeightStoreError(str(error)) from error
        else:
            fresh_binding = registration_lease.binding
            registration_lease.validate(
                source_binding,
                tuple(current for _, current in local_operations),
            )

        fresh_local = {
            fragment.fragment_id: fragment for fragment in fresh_binding.fragments
        }
        refreshed_operations: list[tuple[UploadOperation, RuntimeBindingFragment]] = []
        for operation, _ in local_operations:
            current = fresh_local.get(operation.source_snapshot.fragment_id)
            if current is None or not same_runtime_snapshot(
                current,
                operation.source_snapshot,
            ):
                raise WeightStoreError(
                    f"stale source fragment: {operation.source_snapshot.fragment_id}"
                )
            refreshed_operations.append((operation, current))

        sources = [current for _, current in refreshed_operations]
        object_keys = [
            operation.target.object_key for operation, _ in refreshed_operations
        ]
        store_io_started = False
        terminal_state = TerminalTransferState.ABORTED
        try:
            with self.client.registration.registered(
                sources,
                pre_registered_lease=registration_lease,
                lifetime_tokens=lifetime_tokens,
            ):
                for begin in range(
                    0, len(refreshed_operations), self.client.max_ranges_per_request
                ):
                    batch = refreshed_operations[
                        begin : begin + self.client.max_ranges_per_request
                    ]
                    store_io_started = True
                    results = self.client.store.batch_put_from(
                        [operation.target.object_key for operation, _ in batch],
                        [current.address for _, current in batch],
                        [current.nbytes for _, current in batch],
                        self.client.config_factory(
                            [plan.manifest.group_id] * len(batch), "payload"
                        ),
                    )
                    if len(results) != len(batch) or any(
                        result != 0 for result in results
                    ):
                        raise WeightStoreError(f"batch_put_from failed: {results}")
                self.payloads.require_complete_payloads(object_keys)
            terminal_state = TerminalTransferState.COMPLETED
        except BaseException:
            if store_io_started:
                terminal_state = TerminalTransferState.FAILED_DRAINED
            raise
        finally:
            if lifetime_tokens is not None:
                lifetime_tokens.release_after_terminal(terminal_state)
        self.transaction.require_writable(plan, cleanup_keys=object_keys)
        return tuple(
            UploadReceipt(
                fragment_id=operation.target.fragment_id,
                object_key=operation.target.object_key,
                worker_id=current.worker_id,
            )
            for operation, current in refreshed_operations
        )
