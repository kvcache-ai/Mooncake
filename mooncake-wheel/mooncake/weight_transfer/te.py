from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Sequence

from .manifest import RuntimeFragment, RuntimeManifest
from .planner import CopyRange, TransferPlan, resolve_executor_plans


class TransferEngineError(RuntimeError):
    pass


@dataclass(frozen=True)
class DirectTransferReceipt:
    source_worker_id: str
    target_endpoint: str
    operation_count: int
    nbytes: int


@dataclass(frozen=True)
class MemoryRegistrationLease:
    fragment_id: str
    worker_id: str
    address: int
    nbytes: int
    lease_generation: int

    def __post_init__(self) -> None:
        if not self.fragment_id or not self.worker_id:
            raise ValueError("registration lease identifiers must not be empty")
        for name in ("address", "nbytes", "lease_generation"):
            value = getattr(self, name)
            if type(value) is not int:
                raise ValueError(f"registration lease {name} must be an integer")
        if self.address <= 0 or self.nbytes <= 0 or self.lease_generation < 0:
            raise ValueError("registration lease values are invalid")

    @classmethod
    def from_fragment(cls, fragment: RuntimeFragment) -> MemoryRegistrationLease:
        return cls(
            fragment_id=fragment.fragment_id,
            worker_id=fragment.worker_id,
            address=fragment.address,
            nbytes=fragment.nbytes,
            lease_generation=fragment.lease_generation,
        )


def _same_snapshot(current: RuntimeFragment, planned: RuntimeFragment) -> bool:
    return (
        current.tensor_id == planned.tensor_id
        and current.global_offset == planned.global_offset
        and current.local_shape == planned.local_shape
        and current.address == planned.address
        and current.nbytes == planned.nbytes
        and current.worker_id == planned.worker_id
        and current.endpoint == planned.endpoint
        and current.lease_generation == planned.lease_generation
    )


class MooncakeTransferEngineSink:
    def __init__(self, engine: Any, *, max_batch_operations: int = 1024) -> None:
        if max_batch_operations <= 0:
            raise ValueError("max_batch_operations must be positive")
        self.engine = engine
        self.max_batch_operations = max_batch_operations

    def execute(
        self,
        plan: TransferPlan,
        source_manifest: RuntimeManifest,
        target_manifests: Sequence[RuntimeManifest],
        *,
        target_registrations: Sequence[MemoryRegistrationLease] | None = None,
        source_pre_registered: bool = False,
        source_registrations: Sequence[MemoryRegistrationLease] | None = None,
    ) -> tuple[DirectTransferReceipt, ...]:
        self._validate_plan_identity(plan, source_manifest, "source")
        if not target_manifests:
            raise TransferEngineError("target manifests must not be empty")
        try:
            source_executors = resolve_executor_plans(plan, source_manifest, "source")
        except ValueError as error:
            raise TransferEngineError(str(error)) from error
        source_workers = {executor.worker_id for executor in source_executors}
        if len(source_workers) != 1:
            raise TransferEngineError("source manifest spans multiple workers")
        source_worker_id = next(iter(source_workers))

        targets: dict[str, RuntimeFragment] = {}
        target_ranks = set()
        for manifest in target_manifests:
            self._validate_plan_identity(plan, manifest, "target")
            try:
                executors = resolve_executor_plans(plan, manifest, "target")
            except ValueError as error:
                raise TransferEngineError(str(error)) from error
            for executor in executors:
                if executor.rank in target_ranks:
                    raise TransferEngineError(
                        f"duplicate target executor rank: {executor.rank}"
                    )
                target_ranks.add(executor.rank)
            for fragment in manifest.fragments:
                if fragment.fragment_id in targets:
                    raise TransferEngineError(
                        f"duplicate target fragment: {fragment.fragment_id}"
                    )
                targets[fragment.fragment_id] = fragment
        expected_target_ranks = {executor.rank for executor in plan.target_executors}
        if target_ranks != expected_target_ranks:
            raise TransferEngineError("target executor set is incomplete")

        local_operations = [
            plan.operations[index]
            for executor in source_executors
            for index in executor.operation_indices
        ]
        if not local_operations:
            return ()

        local = {
            fragment.fragment_id: fragment for fragment in source_manifest.fragments
        }
        target_registration_by_id = self._registration_map(
            target_registrations, "target"
        )

        operations_by_endpoint: dict[
            str, list[tuple[CopyRange, RuntimeFragment, RuntimeFragment]]
        ] = {}
        used_sources: dict[str, RuntimeFragment] = {}
        for operation in local_operations:
            if not isinstance(operation.source, RuntimeFragment):
                raise TransferEngineError(
                    "MooncakeTransferEngineSink requires runtime sources"
                )
            current = local[operation.source.fragment_id]
            if not _same_snapshot(current, operation.source):
                raise TransferEngineError(
                    f"stale source fragment: {operation.source.fragment_id}"
                )
            target = targets.get(operation.target.fragment_id)
            if target is None:
                raise TransferEngineError(
                    f"missing planned target fragment: {operation.target.fragment_id}"
                )
            if not _same_snapshot(target, operation.target):
                raise TransferEngineError(
                    f"stale target fragment: {operation.target.fragment_id}"
                )
            if not target.endpoint:
                raise TransferEngineError(
                    f"target endpoint is empty: {operation.target.fragment_id}"
                )
            try:
                operation.validate_bounds()
            except ValueError as error:
                raise TransferEngineError(
                    f"invalid copy range for {operation.tensor_id}: {error}"
                ) from error
            self._validate_registration(target, target_registration_by_id, "target")
            used_sources[current.fragment_id] = current
            operations_by_endpoint.setdefault(target.endpoint, []).append(
                (operation, current, target)
            )

        with self._registered_sources(
            tuple(used_sources.values()),
            pre_registered=source_pre_registered,
            registrations=source_registrations,
        ):
            receipts = []
            for endpoint in sorted(operations_by_endpoint):
                operations = sorted(
                    operations_by_endpoint[endpoint],
                    key=lambda item: (
                        item[2].address + item[0].target_offset,
                        item[1].address + item[0].source_offset,
                    ),
                )
                source_addresses = []
                target_addresses = []
                sizes = []
                operation_count = 0
                total_bytes = 0
                for operation, source, target in operations:
                    for (
                        source_offset,
                        target_offset,
                        nbytes,
                    ) in operation.iter_segments():
                        source_addresses.append(source.address + source_offset)
                        target_addresses.append(target.address + target_offset)
                        sizes.append(nbytes)
                        operation_count += 1
                        total_bytes += nbytes
                        if len(sizes) == self.max_batch_operations:
                            self._transfer_batch(
                                endpoint,
                                source_addresses,
                                target_addresses,
                                sizes,
                            )
                            source_addresses = []
                            target_addresses = []
                            sizes = []
                if sizes:
                    self._transfer_batch(
                        endpoint, source_addresses, target_addresses, sizes
                    )
                receipts.append(
                    DirectTransferReceipt(
                        source_worker_id=source_worker_id,
                        target_endpoint=endpoint,
                        operation_count=operation_count,
                        nbytes=total_bytes,
                    )
                )
        return tuple(receipts)

    @staticmethod
    def _validate_plan_identity(
        plan: TransferPlan, manifest: RuntimeManifest, label: str
    ) -> None:
        if manifest.model_id != plan.model_id:
            raise TransferEngineError(f"{label} model_id mismatch")
        if manifest.revision != plan.revision:
            raise TransferEngineError(f"{label} revision mismatch")

    @staticmethod
    def _registration_map(
        registrations: Sequence[MemoryRegistrationLease] | None,
        label: str,
    ) -> dict[str, MemoryRegistrationLease]:
        if registrations is None:
            raise TransferEngineError(f"{label} registration leases are required")
        result = {}
        for registration in registrations:
            if registration.fragment_id in result:
                raise TransferEngineError(
                    f"duplicate {label} registration lease: {registration.fragment_id}"
                )
            result[registration.fragment_id] = registration
        return result

    @staticmethod
    def _validate_registration(
        fragment: RuntimeFragment,
        registrations: dict[str, MemoryRegistrationLease],
        label: str,
    ) -> None:
        registration = registrations.get(fragment.fragment_id)
        if registration is None or (
            registration.worker_id != fragment.worker_id
            or registration.address != fragment.address
            or registration.nbytes != fragment.nbytes
            or registration.lease_generation != fragment.lease_generation
        ):
            raise TransferEngineError(
                f"{label} registration lease mismatch: {fragment.fragment_id}"
            )

    @contextmanager
    def _registered_sources(
        self,
        fragments: Sequence[RuntimeFragment],
        *,
        pre_registered: bool,
        registrations: Sequence[MemoryRegistrationLease] | None,
    ) -> Iterator[None]:
        if pre_registered:
            registration_by_id = self._registration_map(registrations, "source")
            for fragment in fragments:
                self._validate_registration(fragment, registration_by_id, "source")
            yield
            return
        if registrations is not None:
            raise TransferEngineError(
                "source registration leases require source_pre_registered=True"
            )

        sizes_by_address: dict[int, int] = {}
        for fragment in fragments:
            sizes_by_address[fragment.address] = max(
                sizes_by_address.get(fragment.address, 0), fragment.nbytes
            )
        owned = []
        primary_error: BaseException | None = None
        try:
            for address, nbytes in sizes_by_address.items():
                try:
                    result = self.engine.register_memory(address, nbytes)
                except Exception as error:
                    raise TransferEngineError(
                        f"source register_memory failed for {address}: {error}"
                    ) from error
                if result != 0:
                    raise TransferEngineError(
                        f"source register_memory failed for {address}: {result}"
                    )
                owned.append(address)
            yield
        except BaseException as error:
            primary_error = error

        failures = []
        for address in reversed(owned):
            try:
                result = self.engine.unregister_memory(address)
            except Exception as error:
                failures.append((address, repr(error)))
                continue
            if result != 0:
                failures.append((address, result))
        if failures:
            detail = f"source unregister_memory failed: {failures}"
            if primary_error is not None:
                raise TransferEngineError(
                    f"{primary_error}; {detail}"
                ) from primary_error
            raise TransferEngineError(detail)
        if primary_error is not None:
            raise primary_error

    def _transfer_batch(
        self,
        endpoint: str,
        source_addresses: list[int],
        target_addresses: list[int],
        sizes: list[int],
    ) -> None:
        result = self.engine.batch_transfer_sync_write(
            endpoint,
            source_addresses,
            target_addresses,
            sizes,
        )
        if result != 0:
            raise TransferEngineError(f"batch transfer to {endpoint} failed: {result}")
