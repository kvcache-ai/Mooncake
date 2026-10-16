"""Operation-bound receipts and a fail-closed all-participant barrier."""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass

from .transfer import KVCacheRuntimeTransferPlan
from .types import require_integer, require_nonempty_string, require_sha256


@dataclass(frozen=True)
class KVCacheWriterReceipt:
    operation_id: str
    transfer_digest: str
    writer_id: str
    target_id: str
    success: bool
    completed_bytes: int
    quiesced: bool
    error: str | None = None

    def __post_init__(self) -> None:
        for name in ("operation_id", "writer_id", "target_id"):
            require_nonempty_string(getattr(self, name), name)
        require_sha256(self.transfer_digest, "transfer_digest")
        require_integer(self.completed_bytes, "completed_bytes")
        if type(self.success) is not bool or type(self.quiesced) is not bool:
            raise ValueError("receipt success and quiesced must be booleans")
        if self.success:
            if self.error is not None or not self.quiesced:
                raise ValueError("success requires quiescence and no error")
        else:
            require_nonempty_string(self.error, "failure error")


@dataclass(frozen=True)
class KVCacheTargetReceipt:
    operation_id: str
    transfer_digest: str
    target_id: str
    success: bool
    error: str | None = None

    def __post_init__(self) -> None:
        for name in ("operation_id", "target_id"):
            require_nonempty_string(getattr(self, name), name)
        require_sha256(self.transfer_digest, "transfer_digest")
        if type(self.success) is not bool:
            raise ValueError("receipt success must be a boolean")
        if self.success and self.error is not None:
            raise ValueError("success cannot contain an error")
        if not self.success:
            require_nonempty_string(self.error, "failure error")


class KVCacheCompletion:
    """Collect authenticated control-plane receipts before runtime activation.

    Frameworks transport receipts and validate target content. Mooncake never
    activates traffic or releases pins. A failed/expired operation is terminal;
    retry with a new operation ID after outstanding native work is quiescent.
    """

    def __init__(
        self, plan: KVCacheRuntimeTransferPlan, *, timeout_seconds: float = 60.0
    ) -> None:
        if not isinstance(plan, KVCacheRuntimeTransferPlan):
            raise TypeError("plan must be a KVCacheRuntimeTransferPlan")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be positive and finite")
        self.plan = plan
        self._deadline = time.monotonic() + timeout_seconds
        self._writers: dict[tuple[str, str], KVCacheWriterReceipt] = {}
        self._targets: dict[str, KVCacheTargetReceipt] = {}
        self._error: str | None = None
        self._lock = threading.RLock()

    def _expire(self) -> None:
        if (
            not self._complete()
            and self._error is None
            and time.monotonic() >= self._deadline
        ):
            self._error = "runtime operation completion deadline expired"

    def _complete(self) -> bool:
        return (
            self._error is None
            and self._writers_ready()
            and len(self._targets) == len(self.plan.target_bindings)
        )

    def _writers_ready(self) -> bool:
        return len(self._writers) == len(self.plan.expected_writers) and all(
            r.success for r in self._writers.values()
        )

    @property
    def state(self) -> str:
        with self._lock:
            self._expire()
            if self._error is not None:
                return "failed"
            if self._complete():
                return "complete"
            return "writers_done" if self._writers_ready() else "pending"

    @property
    def error(self) -> str | None:
        with self._lock:
            self._expire()
            return self._error

    @property
    def can_activate(self) -> bool:
        return self.state == "complete"

    def fail(self, reason: str) -> None:
        require_nonempty_string(reason, "failure reason")
        with self._lock:
            if self._complete():
                raise ValueError("completed operation is terminal")
            self._error = self._error or reason

    def _identity(self, operation_id: str, digest: str) -> None:
        if operation_id != self.plan.operation_id or digest != self.plan.digest:
            raise ValueError("receipt belongs to a different operation or transfer")
        self._expire()

    def record_writer(self, receipt: KVCacheWriterReceipt) -> None:
        if not isinstance(receipt, KVCacheWriterReceipt):
            raise TypeError("receipt must be a KVCacheWriterReceipt")
        with self._lock:
            self._identity(receipt.operation_id, receipt.transfer_digest)
            key = (receipt.writer_id, receipt.target_id)
            if key not in self.plan.expected_writers:
                raise ValueError("receipt writer/target is not expected")
            if key in self._writers:
                if self._writers[key] != receipt:
                    raise ValueError("conflicting duplicate writer receipt")
                return
            if self._error is not None:
                raise ValueError("failed operation is terminal")
            expected_bytes = self.plan.writer_bytes(*key)
            if receipt.completed_bytes > expected_bytes or (
                receipt.success and receipt.completed_bytes != expected_bytes
            ):
                raise ValueError("writer receipt byte count differs from plan")
            self._writers[key] = receipt
            if not receipt.success:
                self._error = receipt.error

    def record_target(self, receipt: KVCacheTargetReceipt) -> None:
        if not isinstance(receipt, KVCacheTargetReceipt):
            raise TypeError("receipt must be a KVCacheTargetReceipt")
        with self._lock:
            self._identity(receipt.operation_id, receipt.transfer_digest)
            if receipt.target_id not in {
                b.participant_id for b in self.plan.target_bindings
            }:
                raise ValueError("target participant is not expected")
            if receipt.target_id in self._targets:
                if self._targets[receipt.target_id] != receipt:
                    raise ValueError("conflicting duplicate target receipt")
                return
            if self._error is not None:
                raise ValueError("failed operation is terminal")
            if not self._writers_ready():
                raise ValueError(
                    "all expected writers must finish before target validation"
                )
            self._targets[receipt.target_id] = receipt
            if not receipt.success:
                self._error = receipt.error
