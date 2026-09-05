from __future__ import annotations

from typing import TYPE_CHECKING, Sequence, Union

from ..storage_manifest import StoredWeightManifest
from .contracts import UploadReceipt, WeightUploadPlan
from .errors import WeightStoreError

if TYPE_CHECKING:
    from .store import WeightStore


OBJECT_NOT_FOUND = -704


class PayloadStoreOperations:
    def __init__(self, client: WeightStore) -> None:
        self.client = client

    def require_complete_payloads(self, keys: Sequence[str]) -> None:
        if not keys:
            return
        batch_is_exist = self.client.store.batch_is_exist
        incomplete: list[str] = []
        for begin in range(0, len(keys), self.client.max_ranges_per_request):
            chunk = list(keys[begin : begin + self.client.max_ranges_per_request])
            results = batch_is_exist(chunk)
            if results is None:
                results = [self.client.store.is_exist(key) for key in chunk]
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
    def validate_receipts(
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
                or receipt.worker_id != operation.source_snapshot.worker_id
            ):
                raise WeightStoreError(f"invalid upload receipt: {receipt.fragment_id}")
        if require_complete:
            missing = set(expected) - completed
            if missing:
                raise WeightStoreError(
                    f"missing upload receipts: {', '.join(sorted(missing))}"
                )

    @staticmethod
    def unreferenced_plan_payload_keys(
        plan: WeightUploadPlan,
        persisted: StoredWeightManifest,
    ) -> list[str]:
        persisted_keys = {fragment.object_key for fragment in persisted.fragments}
        return [
            operation.target.object_key
            for operation in plan.operations
            if operation.target.object_key not in persisted_keys
        ]

    def remove_keys(self, keys: Sequence[str]) -> list[tuple[str, Union[str, int]]]:
        failures: list[tuple[str, Union[str, int]]] = []
        for key in keys:
            try:
                result = self.client.store.remove(key, force=True)
            except Exception as error:
                failures.append((key, repr(error)))
                continue
            if result not in (0, OBJECT_NOT_FOUND):
                failures.append((key, result))
        return failures
