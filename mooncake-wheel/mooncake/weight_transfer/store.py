from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterator, Literal, Mapping, Sequence
from urllib.parse import quote
from uuid import uuid4

from .manifest import (
    RuntimeFragment,
    RuntimeManifest,
    StoredFragment,
    TensorDescriptor,
    WeightManifest,
)
from .planner import TransferPlan, plan_stored_transfer, resolve_executor_plans


OBJECT_NOT_FOUND = -704


class WeightStoreError(RuntimeError):
    pass


@dataclass(frozen=True)
class UploadOperation:
    source: RuntimeFragment
    target: StoredFragment


@dataclass(frozen=True)
class WeightUploadPlan:
    manifest: WeightManifest
    session_group_id: str
    control_key: str
    operations: tuple[UploadOperation, ...]


@dataclass(frozen=True)
class UploadReceipt:
    fragment_id: str
    object_key: str
    worker_id: str


@dataclass(frozen=True)
class WeightLoadPlan:
    manifest: WeightManifest
    transfer: TransferPlan


@dataclass(frozen=True)
class _UploadDecision:
    plan_digest: str
    decision: str


StoreRecordType = Literal["payload", "metadata"]


def _default_config_factory(
    group_ids: Sequence[str], record_type: StoreRecordType
) -> Any:
    from mooncake.store import ObjectDataType, ReplicateConfig

    config = ReplicateConfig()
    config.group_ids = list(group_ids)
    config.with_hard_pin = True
    if record_type == "payload":
        config.data_type = ObjectDataType.WEIGHT
    elif record_type == "metadata":
        config.data_type = ObjectDataType.METADATA
    else:
        raise ValueError(f"invalid weight Store record type: {record_type}")
    return config


def _runtime_sort_key(fragment: RuntimeFragment) -> tuple:
    return (
        fragment.rank.dp,
        fragment.rank.pp,
        fragment.rank.ep,
        fragment.rank.tp,
        fragment.worker_id,
        fragment.fragment_id,
    )


def _geometry_key(fragment: RuntimeFragment) -> tuple:
    return fragment.tensor_id, fragment.global_offset, fragment.local_shape


def _collect_upload_sources(
    manifests: Sequence[RuntimeManifest],
) -> tuple[str, str, tuple[TensorDescriptor, ...], list[RuntimeFragment]]:
    if not manifests:
        raise ValueError("source manifests must not be empty")
    model_id = manifests[0].model_id
    revision = manifests[0].revision
    tensor_by_id: dict[str, TensorDescriptor] = {}
    fragments_by_dp: dict[int, list[RuntimeFragment]] = {}
    fragment_ids: set[str] = set()
    for manifest in manifests:
        if manifest.model_id != model_id or manifest.revision != revision:
            raise ValueError("source manifests describe different revisions")
        for tensor in manifest.tensors:
            previous = tensor_by_id.setdefault(tensor.tensor_id, tensor)
            if previous != tensor:
                raise ValueError(f"tensor descriptor mismatch: {tensor.tensor_id}")
        for fragment in manifest.fragments:
            if fragment.fragment_id in fragment_ids:
                raise ValueError(
                    f"duplicate source fragment_id: {fragment.fragment_id}"
                )
            fragment_ids.add(fragment.fragment_id)
            fragments_by_dp.setdefault(fragment.rank.dp, []).append(fragment)

    tensors = tuple(sorted(tensor_by_id.values(), key=lambda item: item.tensor_id))
    complete_replicas: list[tuple[int, int, list[RuntimeFragment]]] = []
    for dp_rank, fragments in sorted(fragments_by_dp.items()):
        generations = {fragment.lease_generation for fragment in fragments}
        if len(generations) != 1:
            continue
        candidates: dict[tuple, list[RuntimeFragment]] = {}
        for fragment in fragments:
            candidates.setdefault(_geometry_key(fragment), []).append(fragment)
        selected = []
        for group in candidates.values():
            group.sort(key=_runtime_sort_key)
            selected.append(group[0])
        selected.sort(
            key=lambda fragment: (
                fragment.tensor_id,
                fragment.global_offset,
                fragment.local_shape,
            )
        )
        try:
            _validate_upload_coverage(tensors, selected)
        except ValueError:
            continue
        complete_replicas.append((dp_rank, next(iter(generations)), selected))

    if not complete_replicas:
        raise ValueError(
            "source manifests have no complete generation-consistent DP replica"
        )
    generations = {generation for _, generation, _ in complete_replicas}
    if len(generations) != 1:
        raise ValueError(
            "complete source DP replicas have inconsistent lease generations"
        )
    _, _, selected = complete_replicas[0]
    return model_id, revision, tensors, selected


def _validate_upload_coverage(
    tensors: Sequence[TensorDescriptor], fragments: Sequence[RuntimeFragment]
) -> None:
    fragments_by_tensor: dict[str, list[RuntimeFragment]] = {}
    for fragment in fragments:
        fragments_by_tensor.setdefault(fragment.tensor_id, []).append(fragment)

    for tensor in tensors:
        tensor_fragments = fragments_by_tensor.get(tensor.tensor_id, [])
        if tensor.partition_dim is None:
            if len(tensor_fragments) != 1:
                raise ValueError(f"tensor is not fully covered: {tensor.tensor_id}")
            continue
        dim = tensor.partition_dim
        intervals = sorted(
            (
                fragment.global_offset[dim],
                fragment.global_offset[dim] + fragment.local_shape[dim],
            )
            for fragment in tensor_fragments
        )
        cursor = 0
        for begin, end in intervals:
            if begin != cursor:
                raise ValueError(f"tensor is not fully covered: {tensor.tensor_id}")
            cursor = end
        if cursor != tensor.global_shape[dim]:
            raise ValueError(f"tensor is not fully covered: {tensor.tensor_id}")


def _safe_segment(value: str) -> str:
    return quote(value, safe="._-")


def _fragment_digest(fragment: RuntimeFragment) -> str:
    value = (
        f"{fragment.tensor_id}|{fragment.global_offset}|{fragment.local_shape}"
    ).encode()
    return hashlib.sha256(value).hexdigest()[:24]


def _plan_digest(manifest: WeightManifest) -> str:
    return hashlib.sha256(manifest.to_json().encode()).hexdigest()


def _decision_payload(plan: WeightUploadPlan, decision: str) -> bytes:
    return json.dumps(
        {
            "decision": decision,
            "format_version": 1,
            "plan_digest": _plan_digest(plan.manifest),
            "record_type": "weight-upload-decision",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _decode_decision(value: bytes | bytearray | str) -> _UploadDecision:
    def reject_constant(constant: str) -> None:
        raise ValueError(f"non-finite JSON number is unsupported: {constant}")

    raw = json.loads(value, parse_constant=reject_constant)
    expected = {
        "decision",
        "format_version",
        "plan_digest",
        "record_type",
    }
    if not isinstance(raw, Mapping) or set(raw) != expected:
        raise ValueError("upload decision schema fields do not match format_version")
    if raw["format_version"] != 1 or type(raw["format_version"]) is not int:
        raise ValueError("unsupported upload decision format_version")
    if raw["record_type"] != "weight-upload-decision":
        raise ValueError("invalid upload decision record_type")
    if raw["decision"] not in ("abort", "commit"):
        raise ValueError("invalid upload decision")
    digest = raw["plan_digest"]
    if (
        type(digest) is not str
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise ValueError("invalid upload decision plan_digest")
    return _UploadDecision(plan_digest=digest, decision=raw["decision"])


def _same_runtime_snapshot(current: RuntimeFragment, planned: RuntimeFragment) -> bool:
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


class WeightStore:
    def __init__(
        self,
        store: Any,
        *,
        key_prefix: str = "weights",
        config_factory: Callable[[Sequence[str], StoreRecordType], Any] | None = None,
        max_range_bytes: int = 64 * 1024 * 1024,
        max_ranges_per_request: int = 1024,
    ) -> None:
        if max_range_bytes <= 0 or max_ranges_per_request <= 0:
            raise ValueError("range limits must be positive")
        self.store = store
        self.key_prefix = key_prefix.strip("/")
        self.config_factory = config_factory or _default_config_factory
        self.max_range_bytes = max_range_bytes
        self.max_ranges_per_request = max_ranges_per_request

    def prepare_upload(
        self,
        source_manifests: Sequence[RuntimeManifest],
        *,
        namespace: str = "default",
    ) -> WeightUploadPlan:
        model_id, revision, tensors, sources = _collect_upload_sources(source_manifests)
        base_key = "/".join(
            (
                self.key_prefix,
                _safe_segment(namespace),
                _safe_segment(model_id),
                _safe_segment(revision),
            )
        )
        group_id = base_key
        upload_id = uuid4().hex
        stored_fragments = []
        operations = []
        for source in sources:
            fragment_id = _fragment_digest(source)
            target = StoredFragment(
                fragment_id=fragment_id,
                tensor_id=source.tensor_id,
                global_offset=source.global_offset,
                local_shape=source.local_shape,
                object_key=(f"{base_key}/payload/{upload_id}/{fragment_id}"),
                object_offset=0,
                nbytes=source.nbytes,
            )
            stored_fragments.append(target)
            operations.append(UploadOperation(source=source, target=target))

        manifest = WeightManifest(
            namespace=namespace,
            model_id=model_id,
            revision=revision,
            group_id=group_id,
            manifest_key=f"{base_key}/manifest",
            tensors=tensors,
            fragments=tuple(stored_fragments),
            created_at=datetime.now(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z"),
        )
        return WeightUploadPlan(
            manifest=manifest,
            session_group_id=f"{base_key}/sessions/{upload_id}",
            control_key=f"{base_key}/sessions/{upload_id}/decision",
            operations=tuple(operations),
        )

    def upload(
        self,
        plan: WeightUploadPlan,
        runtime_manifest: RuntimeManifest,
        *,
        pre_registered: bool = False,
    ) -> tuple[UploadReceipt, ...]:
        if (
            runtime_manifest.model_id != plan.manifest.model_id
            or runtime_manifest.revision != plan.manifest.revision
        ):
            raise WeightStoreError("runtime manifest revision mismatch")
        self._require_upload_writable(plan)
        local = {
            fragment.fragment_id: fragment for fragment in runtime_manifest.fragments
        }
        local_workers = {
            runtime_manifest.instance_id,
            *(fragment.worker_id for fragment in runtime_manifest.fragments),
        }
        expected_operations = [
            operation
            for operation in plan.operations
            if operation.source.worker_id in local_workers
        ]
        missing = {
            operation.source.fragment_id
            for operation in expected_operations
            if operation.source.fragment_id not in local
        }
        if missing:
            raise WeightStoreError(
                f"missing planned source fragment: {', '.join(sorted(missing))}"
            )
        local_operations = []
        for operation in expected_operations:
            current = local.get(operation.source.fragment_id)
            if current is None:
                raise AssertionError("planned source fragment was not resolved")
            if not _same_runtime_snapshot(current, operation.source):
                raise WeightStoreError(
                    f"stale source fragment: {operation.source.fragment_id}"
                )
            local_operations.append((operation, current))
        if not local_operations:
            return ()

        sources = [current for _, current in local_operations]
        object_keys = [operation.target.object_key for operation, _ in local_operations]
        with self._registered(sources, pre_registered=pre_registered):
            results = self.store.batch_put_from(
                object_keys,
                [current.address for _, current in local_operations],
                [current.nbytes for _, current in local_operations],
                self.config_factory(
                    [plan.manifest.group_id] * len(local_operations), "payload"
                ),
            )
            if len(results) != len(local_operations) or any(
                result != 0 for result in results
            ):
                raise WeightStoreError(f"batch_put_from failed: {results}")
            self._require_complete_payloads(object_keys)
        self._require_upload_writable(plan, cleanup_keys=object_keys)
        return tuple(
            UploadReceipt(
                fragment_id=operation.target.fragment_id,
                object_key=operation.target.object_key,
                worker_id=current.worker_id,
            )
            for operation, current in local_operations
        )

    def abort_upload(
        self,
        plan: WeightUploadPlan,
        receipts: Sequence[UploadReceipt],
    ) -> None:
        self._validate_receipts(plan, receipts, require_complete=False)
        persisted = self._load_manifest_if_present(plan.manifest.manifest_key)
        if persisted == plan.manifest:
            raise WeightStoreError("cannot abort a published weight revision")
        self._claim_upload(plan, "abort")
        persisted = self._load_manifest_if_present(plan.manifest.manifest_key)
        if persisted == plan.manifest:
            raise WeightStoreError("cannot abort a published weight revision")
        keys = (
            [operation.target.object_key for operation in plan.operations]
            if persisted is None
            else self._unreferenced_plan_payload_keys(plan, persisted)
        )
        failures = self._remove_keys(keys)
        if failures:
            raise WeightStoreError(f"upload cleanup failed: {failures}")

    def finalize_upload_session(self, plan: WeightUploadPlan) -> None:
        decision = self._load_decision_if_present(plan.control_key)
        if decision is None:
            persisted = self._load_manifest_if_present(plan.manifest.manifest_key)
            if persisted == plan.manifest:
                self._claim_upload(plan, "commit")
                decision = self._load_decision_if_present(plan.control_key)
            else:
                raise WeightStoreError("upload session has no terminal decision")
        if decision is None:
            raise WeightStoreError("upload session terminal decision is incomplete")
        self._validate_decision_owner(plan, decision)
        if decision.decision == "commit":
            persisted = self._load_manifest_if_present(plan.manifest.manifest_key)
            if persisted is None:
                raise WeightStoreError("committed upload session has no manifest")
            if persisted != plan.manifest:
                failures = self._remove_keys(
                    self._unreferenced_plan_payload_keys(plan, persisted)
                )
                if failures:
                    raise WeightStoreError(
                        f"conflicting upload cleanup failed: {failures}"
                    )
        else:
            persisted = self._load_manifest_if_present(plan.manifest.manifest_key)
            if persisted == plan.manifest:
                raise WeightStoreError("aborted upload has a published manifest")
            keys = (
                [operation.target.object_key for operation in plan.operations]
                if persisted is None
                else self._unreferenced_plan_payload_keys(plan, persisted)
            )
            failures = self._remove_keys(keys)
            if failures:
                raise WeightStoreError(f"upload cleanup failed: {failures}")

    def commit(
        self,
        plan: WeightUploadPlan,
        receipts: Sequence[UploadReceipt],
    ) -> WeightManifest:
        self._validate_receipts(plan, receipts, require_complete=True)
        payload_keys = [receipt.object_key for receipt in receipts]
        if self._current_upload_decision(plan) is None:
            self._require_complete_payloads(payload_keys)
        self._claim_upload(plan, "commit")
        self._require_complete_payloads(payload_keys)
        existing = self._load_manifest_if_present(plan.manifest.manifest_key)
        if existing is not None:
            return self._resolve_manifest_commit(plan, existing, payload_keys)

        put_error: Exception | None = None
        result: int | None = None
        try:
            result = self.store.put(
                plan.manifest.manifest_key,
                plan.manifest.to_json().encode(),
                self.config_factory([plan.manifest.group_id], "metadata"),
            )
        except Exception as error:
            put_error = error

        persisted = self._load_manifest_if_present(plan.manifest.manifest_key)
        if persisted is not None:
            return self._resolve_manifest_commit(plan, persisted, payload_keys)

        detail = f"manifest put failed: {result}"
        if put_error is not None:
            detail = f"manifest put failed: {put_error}"
        raise WeightStoreError(detail) from put_error

    def _claim_upload(self, plan: WeightUploadPlan, decision: str) -> None:
        put_error: Exception | None = None
        result: int | None = None
        try:
            result = self.store.put(
                plan.control_key,
                _decision_payload(plan, decision),
                self.config_factory([plan.session_group_id], "metadata"),
            )
        except Exception as error:
            put_error = error

        persisted = self._load_decision_if_present(plan.control_key)
        if persisted is None:
            detail = f"upload decision is not complete; retry: {result}"
            if put_error is not None:
                detail = f"upload decision is not complete; retry: {put_error}"
            raise WeightStoreError(detail) from put_error
        self._validate_decision_owner(plan, persisted)
        if persisted.decision != decision:
            raise WeightStoreError(f"weight upload already chose {persisted.decision}")

    def _current_upload_decision(self, plan: WeightUploadPlan) -> str | None:
        persisted = self._load_decision_if_present(plan.control_key)
        if persisted is None:
            return None
        self._validate_decision_owner(plan, persisted)
        return persisted.decision

    @staticmethod
    def _validate_decision_owner(
        plan: WeightUploadPlan, persisted: _UploadDecision
    ) -> None:
        if persisted.plan_digest != _plan_digest(plan.manifest):
            raise WeightStoreError("upload decision belongs to another plan")

    def _require_upload_writable(
        self,
        plan: WeightUploadPlan,
        *,
        cleanup_keys: Sequence[str] = (),
    ) -> None:
        decision = self._current_upload_decision(plan)
        if decision is None:
            return
        persisted = self._load_manifest_if_present(plan.manifest.manifest_key)
        if decision == "commit" and (persisted is None or persisted == plan.manifest):
            return
        protected_keys = (
            set()
            if persisted is None
            else {fragment.object_key for fragment in persisted.fragments}
        )
        failures = self._remove_keys(
            [key for key in cleanup_keys if key not in protected_keys]
        )
        detail = (
            "weight upload already chose abort"
            if decision == "abort"
            else "weight upload lost to a conflicting revision"
        )
        if failures:
            detail += f"; cleanup failed: {failures}"
        raise WeightStoreError(detail)

    def _load_decision_if_present(self, control_key: str) -> _UploadDecision | None:
        exists = self.store.is_exist(control_key)
        if exists == 0:
            return None
        if exists != 1:
            raise WeightStoreError(
                f"upload decision existence check failed: {control_key}: {exists}"
            )
        try:
            return _decode_decision(self.store.get(control_key))
        except Exception as error:
            raise WeightStoreError(f"invalid upload decision: {control_key}") from error

    def _load_manifest_if_present(self, manifest_key: str) -> WeightManifest | None:
        exists = self.store.is_exist(manifest_key)
        if exists == 0:
            return None
        if exists != 1:
            raise WeightStoreError(
                f"manifest existence check failed: {manifest_key}: {exists}"
            )
        return self.load_manifest(manifest_key)

    def _resolve_manifest_commit(
        self,
        plan: WeightUploadPlan,
        persisted: WeightManifest,
        payload_keys: Sequence[str],
    ) -> WeightManifest:
        if persisted == plan.manifest:
            return persisted
        persisted_keys = {fragment.object_key for fragment in persisted.fragments}
        cleanup_failures = self._remove_keys(
            [key for key in payload_keys if key not in persisted_keys]
        )
        detail = f"conflicting weight revision: {plan.manifest.manifest_key}"
        if cleanup_failures:
            detail += f"; cleanup failed: {cleanup_failures}"
        raise WeightStoreError(detail)

    @staticmethod
    def _unreferenced_plan_payload_keys(
        plan: WeightUploadPlan,
        persisted: WeightManifest,
    ) -> list[str]:
        persisted_keys = {fragment.object_key for fragment in persisted.fragments}
        return [
            operation.target.object_key
            for operation in plan.operations
            if operation.target.object_key not in persisted_keys
        ]

    def load_manifest(self, manifest_key: str) -> WeightManifest:
        try:
            raw = self.store.get(manifest_key)
        except Exception as error:
            raise WeightStoreError(f"manifest get failed: {manifest_key}") from error
        try:
            manifest = WeightManifest.from_json(raw)
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
        manifest: WeightManifest,
        target_manifests: Sequence[RuntimeManifest],
    ) -> WeightLoadPlan:
        return WeightLoadPlan(
            manifest=manifest,
            transfer=plan_stored_transfer(manifest, target_manifests),
        )

    def load(
        self,
        plan: WeightLoadPlan,
        runtime_manifest: RuntimeManifest,
        *,
        pre_registered: bool = False,
    ) -> None:
        if (
            runtime_manifest.model_id != plan.manifest.model_id
            or runtime_manifest.revision != plan.manifest.revision
        ):
            raise WeightStoreError("runtime manifest revision mismatch")

        try:
            executors = resolve_executor_plans(
                plan.transfer, runtime_manifest, "target"
            )
        except ValueError as error:
            raise WeightStoreError(str(error)) from error
        local = {
            fragment.fragment_id: fragment for fragment in runtime_manifest.fragments
        }
        operations_by_target: dict[str, list] = {}
        operation_indices = sorted(
            index for executor in executors for index in executor.operation_indices
        )
        for index in operation_indices:
            operation = plan.transfer.operations[index]
            current = local[operation.target.fragment_id]
            if not _same_runtime_snapshot(current, operation.target):
                raise WeightStoreError(
                    f"stale target fragment: {operation.target.fragment_id}"
                )
            operations_by_target.setdefault(operation.target.fragment_id, []).append(
                operation
            )
        if not operations_by_target:
            return

        targets = [local[fragment_id] for fragment_id in sorted(operations_by_target)]
        with self._registered(targets, pre_registered=pre_registered):
            batch = []
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
                    ) in operation.iter_segments():
                        for chunk_offset in range(0, nbytes, self.max_range_bytes):
                            chunk_size = min(
                                self.max_range_bytes, nbytes - chunk_offset
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
                            if len(batch) == self.max_ranges_per_request:
                                self._load_range_batch(batch)
                                batch = []
            if batch:
                self._load_range_batch(batch)

    def _load_range_batch(
        self,
        ranges: Sequence[tuple[RuntimeFragment, str, int, int, int]],
    ) -> None:
        grouped: dict[
            str,
            tuple[
                RuntimeFragment,
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

        addresses = []
        all_keys = []
        all_target_offsets = []
        all_source_offsets = []
        all_sizes = []
        for target, object_ranges in grouped.values():
            addresses.append(target.address)
            all_keys.append(list(object_ranges))
            all_target_offsets.append([group[0] for group in object_ranges.values()])
            all_source_offsets.append([group[1] for group in object_ranges.values()])
            all_sizes.append([group[2] for group in object_ranges.values()])
        results = self.store.get_into_ranges(
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
        results: Any,
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

    def _require_complete_payloads(self, keys: Sequence[str]) -> None:
        if not keys:
            return
        batch_is_exist = getattr(self.store, "batch_is_exist", None)
        incomplete = []
        for begin in range(0, len(keys), self.max_ranges_per_request):
            chunk = list(keys[begin : begin + self.max_ranges_per_request])
            if callable(batch_is_exist):
                results = batch_is_exist(chunk)
            else:
                results = [self.store.is_exist(key) for key in chunk]
            if len(results) != len(chunk):
                raise WeightStoreError("payload existence check returned invalid count")
            for key, result in zip(chunk, results):
                if result == 1:
                    continue
                if result == 0:
                    incomplete.append(key)
                    continue
                raise WeightStoreError(
                    f"payload existence check failed for {key}: {result}"
                )
        if incomplete:
            raise WeightStoreError(
                f"payload is not complete: {', '.join(sorted(incomplete))}"
            )

    @staticmethod
    def _validate_receipts(
        plan: WeightUploadPlan,
        receipts: Sequence[UploadReceipt],
        *,
        require_complete: bool,
    ) -> None:
        receipt_ids = [receipt.fragment_id for receipt in receipts]
        if len(receipt_ids) != len(set(receipt_ids)):
            raise WeightStoreError("duplicate upload receipt")

        expected = {
            operation.target.fragment_id: operation for operation in plan.operations
        }
        completed = set(receipt_ids)
        unexpected = completed - set(expected)
        if unexpected:
            raise WeightStoreError(
                f"unexpected upload receipts: {', '.join(sorted(unexpected))}"
            )
        for receipt in receipts:
            operation = expected[receipt.fragment_id]
            if (
                receipt.object_key != operation.target.object_key
                or receipt.worker_id != operation.source.worker_id
            ):
                raise WeightStoreError(f"invalid upload receipt: {receipt.fragment_id}")
        if require_complete:
            missing = set(expected) - completed
            if missing:
                raise WeightStoreError(
                    f"missing upload receipts: {', '.join(sorted(missing))}"
                )

    def _remove_keys(self, keys: Sequence[str]) -> list[tuple[str, Any]]:
        failures = []
        for key in keys:
            try:
                result = self.store.remove(key, force=True)
            except Exception as error:
                failures.append((key, repr(error)))
                continue
            if result not in (0, OBJECT_NOT_FOUND):
                failures.append((key, result))
        return failures

    @contextmanager
    def _registered(
        self,
        fragments: Sequence[RuntimeFragment],
        *,
        pre_registered: bool,
    ) -> Iterator[None]:
        if pre_registered:
            yield
            return

        requests: dict[int, tuple[int, str]] = {}
        for fragment in fragments:
            previous = requests.get(fragment.address)
            if previous is None or fragment.nbytes > previous[0]:
                requests[fragment.address] = (fragment.nbytes, fragment.fragment_id)

        owned: list[int] = []
        primary_error: BaseException | None = None
        try:
            for address, (nbytes, fragment_id) in requests.items():
                result = self.store.register_buffer(address, nbytes)
                if result == 0:
                    owned.append(address)
                else:
                    raise WeightStoreError(
                        f"register_buffer failed for {fragment_id}: {result}"
                    )
            yield
        except BaseException as error:
            primary_error = error

        failures = []
        for address in reversed(owned):
            try:
                result = self.store.unregister_buffer(address)
            except Exception as error:
                failures.append((address, repr(error)))
                continue
            if result != 0:
                failures.append((address, result))
        if failures:
            detail = f"unregister_buffer failed: {failures}"
            if primary_error is not None:
                raise WeightStoreError(f"{primary_error}; {detail}") from primary_error
            raise WeightStoreError(detail)
        if primary_error is not None:
            raise primary_error
