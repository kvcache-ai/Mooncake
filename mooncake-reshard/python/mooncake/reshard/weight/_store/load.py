from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence
from uuid import uuid4

from ..._typing import TypeAlias

from ...contracts import RuntimeFragmentId
from ...lifetime import AllocationTokenSet, TerminalTransferState
from ..lifetime import (
    WeightAllocationGuardProviders,
    acquire_weight_binding_token,
)
from ..manifest import (
    RuntimeBindingFragment,
    WeightPlacementManifest,
    WeightRuntimeBindingManifest,
)
from ..planner import (
    StoredLoadOperation,
    bind_logical_transfer_plan,
    plan_stored_transfer_to_target_placement,
    resolve_executor_plans,
)
from ..storage_manifest import StoredWeightManifest
from .backend import RangeResults
from .contracts import WeightLoadPlan, _require_stored_load_operation
from .errors import WeightStoreError
from .registration import StoreRegistrationLease
from .validation import (
    pair_manifests,
    runtime_binding_fragment,
    same_runtime_snapshot,
    validate_manifest_pair,
)

if TYPE_CHECKING:
    from .store import WeightStore


RangeRequest: TypeAlias = tuple[RuntimeBindingFragment, str, int, int, int]


class WeightLoadService:
    def __init__(self, client: WeightStore) -> None:
        self.client = client

    def load_manifest(self, manifest_key: str) -> StoredWeightManifest:
        try:
            raw = self.client.store.get(manifest_key)
        except Exception as error:
            raise WeightStoreError(f"manifest get failed: {manifest_key}") from error
        try:
            manifest = StoredWeightManifest.from_json(raw)
        except Exception as error:
            raise WeightStoreError(
                f"invalid weight manifest: {manifest_key}"
            ) from error
        if manifest.manifest_key != manifest_key:
            raise WeightStoreError(
                f"manifest key mismatch: expected {manifest_key}, "
                f"got {manifest.manifest_key}"
            )
        return manifest

    def plan_load(
        self,
        manifest: StoredWeightManifest,
        target_placement: WeightPlacementManifest,
        target_bindings: Sequence[WeightRuntimeBindingManifest],
    ) -> WeightLoadPlan:
        target_pairs = pair_manifests(
            target_placement,
            target_bindings,
            "target",
        )
        if any(
            placement.resource_id != manifest.resource_id
            or placement.revision != manifest.revision
            or placement.weight_generation != manifest.weight_generation
            for placement, _ in target_pairs
        ):
            raise WeightStoreError("target placement revision mismatch")

        logical_plan = plan_stored_transfer_to_target_placement(
            manifest,
            target_placement,
        )
        return WeightLoadPlan(
            manifest=manifest,
            transfer=bind_logical_transfer_plan(
                logical_plan,
                target_bindings=target_bindings,
                source_manifest=manifest,
            ),
        )

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
        validate_manifest_pair(target_placement, target_binding, "target")
        if (
            target_placement.resource_id != plan.manifest.resource_id
            or target_placement.revision != plan.manifest.revision
            or target_placement.weight_generation != plan.manifest.weight_generation
        ):
            raise WeightStoreError("target placement revision mismatch")
        try:
            executors = resolve_executor_plans(
                plan.transfer,
                target_placement,
                target_binding,
                "target",
            )
        except ValueError as error:
            raise WeightStoreError(str(error)) from error
        available_workers = sorted({executor.worker_id for executor in executors})
        if target_worker_id is None:
            if len(available_workers) != 1:
                raise WeightStoreError(
                    "target worker selector is required for a multi-worker binding"
                )
            target_worker_id = available_workers[0]
        elif type(target_worker_id) is not str or not target_worker_id:
            raise WeightStoreError("target_worker_id must be a non-empty string")
        elif target_worker_id not in available_workers:
            raise WeightStoreError(f"unknown target worker: {target_worker_id}")
        executors = tuple(
            executor for executor in executors if executor.worker_id == target_worker_id
        )

        try:
            committed_manifest = self.load_manifest(plan.manifest.manifest_key)
        except WeightStoreError as error:
            raise WeightStoreError("weight manifest is not committed") from error
        if committed_manifest != plan.manifest:
            raise WeightStoreError("committed weight manifest differs from load plan")

        local = {
            fragment.fragment_id: fragment for fragment in target_binding.fragments
        }
        operations_by_target: dict[RuntimeFragmentId, list[StoredLoadOperation]] = {}
        operation_indices = sorted(
            index
            for executor in executors
            for index in plan.transfer.operation_indices_for_executor(
                executor, "target"
            )
        )
        for index in operation_indices:
            operation = _require_stored_load_operation(plan.transfer.operations[index])
            planned_target = runtime_binding_fragment(operation.target)
            current = local.get(planned_target.fragment_id)
            if current is None or not same_runtime_snapshot(current, planned_target):
                raise WeightStoreError(
                    f"stale target fragment: {planned_target.fragment_id}"
                )
            try:
                operation.validate_bounds()
            except ValueError as error:
                raise WeightStoreError(
                    f"invalid transfer region for {operation.tensor_id}: {error}"
                ) from error
            if operation.repeat > self.client.max_region_segments:
                raise WeightStoreError(
                    f"transfer region exceeds max_region_segments: "
                    f"{operation.tensor_id}: {operation.repeat} > "
                    f"{self.client.max_region_segments}"
                )
            operations_by_target.setdefault(planned_target.fragment_id, []).append(
                operation
            )
        if not operations_by_target:
            return

        planned_targets = [
            local[fragment_id] for fragment_id in sorted(operations_by_target)
        ]
        required_fragment_ids = tuple(
            sorted(fragment.fragment_id for fragment in planned_targets)
        )
        lifetime_tokens: Optional[AllocationTokenSet] = None
        if registration_lease is None:
            try:
                fresh_binding, lifetime_tokens = acquire_weight_binding_token(
                    transfer_id=transfer_id or uuid4().hex,
                    expected_binding=target_binding,
                    required_fragment_ids=required_fragment_ids,
                    side="target",
                    providers=target_allocation_guards,
                )
            except ValueError as error:
                raise WeightStoreError(str(error)) from error
        else:
            fresh_binding = registration_lease.binding
            registration_lease.validate(target_binding, planned_targets)

        fresh_targets = {
            fragment.fragment_id: fragment for fragment in fresh_binding.fragments
        }
        targets: list[RuntimeBindingFragment] = []
        for planned_target in planned_targets:
            current = fresh_targets.get(planned_target.fragment_id)
            if current is None or not same_runtime_snapshot(current, planned_target):
                raise WeightStoreError(
                    f"stale target fragment: {planned_target.fragment_id}"
                )
            targets.append(current)

        store_io_started = False
        terminal_state = TerminalTransferState.ABORTED
        try:
            with self.client.registration.registered(
                targets,
                pre_registered_lease=registration_lease,
                lifetime_tokens=lifetime_tokens,
            ):
                batch: list[RangeRequest] = []
                for target in targets:
                    operations = sorted(
                        operations_by_target[target.fragment_id],
                        key=lambda item: (
                            item.target_offset,
                            item.source.object_key,
                            item.source_offset,
                        ),
                    )
                    for operation in operations:
                        for (
                            source_offset,
                            target_offset,
                            nbytes,
                        ) in operation.iter_segments(
                            max_segments=self.client.max_region_segments
                        ):
                            for chunk_offset in range(
                                0, nbytes, self.client.max_range_bytes
                            ):
                                chunk_size = min(
                                    self.client.max_range_bytes,
                                    nbytes - chunk_offset,
                                )
                                batch.append(
                                    (
                                        target,
                                        operation.source.object_key,
                                        target_offset + chunk_offset,
                                        operation.source.object_offset
                                        + source_offset
                                        + chunk_offset,
                                        chunk_size,
                                    )
                                )
                                if len(batch) == self.client.max_ranges_per_request:
                                    store_io_started = True
                                    self._load_range_batch(batch)
                                    batch = []
                if batch:
                    store_io_started = True
                    self._load_range_batch(batch)
            terminal_state = TerminalTransferState.COMPLETED
        except BaseException:
            if store_io_started:
                terminal_state = TerminalTransferState.FAILED_DRAINED
            raise
        finally:
            if lifetime_tokens is not None:
                lifetime_tokens.release_after_terminal(terminal_state)

    def _load_range_batch(
        self,
        ranges: Sequence[tuple[RuntimeBindingFragment, str, int, int, int]],
    ) -> None:
        grouped: dict[
            str,
            tuple[
                RuntimeBindingFragment,
                dict[str, tuple[list[int], list[int], list[int]]],
            ],
        ] = {}
        for target, key, target_offset, source_offset, nbytes in ranges:
            current, object_ranges = grouped.setdefault(
                target.fragment_id, (target, {})
            )
            if current != target:
                raise WeightStoreError(
                    f"target fragment changed in range batch: {target.fragment_id}"
                )
            target_offsets, source_offsets, sizes = object_ranges.setdefault(
                key, ([], [], [])
            )
            target_offsets.append(target_offset)
            source_offsets.append(source_offset)
            sizes.append(nbytes)

        addresses: list[int] = []
        all_keys: list[list[str]] = []
        all_target_offsets: list[list[list[int]]] = []
        all_source_offsets: list[list[list[int]]] = []
        all_sizes: list[list[list[int]]] = []
        for target, object_ranges in grouped.values():
            addresses.append(target.address)
            all_keys.append(list(object_ranges))
            all_target_offsets.append([group[0] for group in object_ranges.values()])
            all_source_offsets.append([group[1] for group in object_ranges.values()])
            all_sizes.append([group[2] for group in object_ranges.values()])
        results = self.client.store.get_into_ranges(
            addresses,
            all_keys,
            all_target_offsets,
            all_source_offsets,
            all_sizes,
        )
        self._validate_range_results(all_keys, all_sizes, results)

    @staticmethod
    def _validate_range_results(
        all_keys: Sequence[Sequence[str]],
        all_sizes: Sequence[Sequence[Sequence[int]]],
        results: RangeResults,
    ) -> None:
        if len(results) != len(all_keys):
            raise WeightStoreError("get_into_ranges returned invalid buffer count")
        for keys, expected_groups, actual_groups in zip(all_keys, all_sizes, results):
            if len(actual_groups) != len(keys):
                raise WeightStoreError("get_into_ranges returned invalid object count")
            for key, expected, actual in zip(keys, expected_groups, actual_groups):
                if list(actual) != list(expected):
                    raise WeightStoreError(
                        f"get_into_ranges failed for {key}: "
                        f"expected {list(expected)}, got {list(actual)}"
                    )
