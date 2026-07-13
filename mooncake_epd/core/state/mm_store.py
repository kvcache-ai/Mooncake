"""Event-driven multimodal store and asynchronous feature prefetch."""

from __future__ import annotations

import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional

import torch

from ..control.write_ahead_log import JsonLineWAL
from ..strict_mode import strict_no_fallback_enabled
from ..transfer import Channel, Mode, TransferEngine, TransferPolicy
from .feature_store import FeatureBundle, FeatureStore


class MMStoreBackpressureError(RuntimeError):
    """Raised when MMStore prefetch admission is rejected by bounded capacity."""


@dataclass
class MMStoreEvent:
    event_id: str
    image_hash: str
    target_worker_id: str
    target_device: torch.device
    created_at: float = field(default_factory=time.monotonic)
    policy: Optional[TransferPolicy] = None
    dedup_key: str = ""
    status: str = "PENDING"
    waiter_count: int = 1
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    shared_store_hit: bool = False
    worker_cache_hit: bool = False
    recomputed: bool = False


@dataclass
class MMStoreHandle:
    event: MMStoreEvent
    done: threading.Event = field(default_factory=threading.Event, repr=False)
    completed_at: Optional[float] = None
    result: Optional[FeatureBundle] = None
    error: Optional[BaseException] = None
    recomputed: bool = False
    deduplicated: bool = False
    worker_cache_hit: bool = False
    shared_store_hit: bool = False

    def wait(self, timeout: Optional[float] = None) -> Optional[FeatureBundle]:
        self.done.wait(timeout=timeout)
        if self.error is not None:
            raise self.error
        return self.result


class MMStore:
    """Shared MM store with deduplicated event-driven prefetch."""

    def __init__(
        self,
        shared_store: Optional[FeatureStore] = None,
        transfer_engine: Optional[TransferEngine] = None,
        *,
        worker_cache_bytes: int = 512 * 1024 * 1024,
        worker_cache_ttl_seconds: Optional[float] = 600.0,
        journal_path: Optional[str] = None,
        max_queue_size: int = 256,
        dispatcher_workers: int = 2,
        queue_put_timeout_s: float = 0.0,
        inline_fallback_on_queue_full: bool = True,
    ):
        self.shared_store = shared_store or FeatureStore(max_bytes=4 * 1024**3)
        self.transfer = transfer_engine or TransferEngine(protocol="local")
        self.worker_cache_bytes = worker_cache_bytes
        self.worker_cache_ttl_seconds = worker_cache_ttl_seconds
        self.max_queue_size = max(0, int(max_queue_size))
        self.dispatcher_workers = max(1, int(dispatcher_workers))
        self.queue_put_timeout_s = max(0.0, float(queue_put_timeout_s))
        self.inline_fallback_on_queue_full = (
            bool(inline_fallback_on_queue_full)
            and not strict_no_fallback_enabled()
        )
        self._worker_caches: Dict[str, FeatureStore] = {}
        self._recompute_hooks: Dict[str, Callable[[str], FeatureBundle]] = {}
        self._events: "queue.Queue[MMStoreEvent]" = queue.Queue(maxsize=self.max_queue_size)
        self._handles: Dict[str, MMStoreHandle] = {}
        self._waiters_by_event: Dict[str, list[str]] = {}
        self._event_by_key: Dict[str, str] = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._wal = JsonLineWAL(journal_path) if journal_path else None
        self._stats = {
            "published": 0,
            "queued": 0,
            "completed": 0,
            "failed": 0,
            "deduplicated_events": 0,
            "collapsed_waiters": 0,
            "worker_cache_hits": 0,
            "shared_store_hits": 0,
            "shared_store_misses": 0,
            "recompute_fallbacks": 0,
            "queue_full": 0,
            "backpressure_rejects": 0,
            "inline_fallbacks": 0,
            "sync_prefetches": 0,
            "inflight_peak": 0,
            "queue_wait_ms": [],
        }
        self._dispatchers = [
            threading.Thread(target=self._dispatch_loop, daemon=True, name=f"mm-store-{idx}")
            for idx in range(self.dispatcher_workers)
        ]
        for dispatcher in self._dispatchers:
            dispatcher.start()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def ensure_worker_cache(self, worker_id: str) -> FeatureStore:
        with self._lock:
            cache = self._worker_caches.get(worker_id)
            if cache is None:
                cache = FeatureStore(
                    max_bytes=self.worker_cache_bytes,
                    ttl_seconds=self.worker_cache_ttl_seconds,
                    node_id=f"worker:{worker_id}",
                )
                self._worker_caches[worker_id] = cache
            return cache

    def register_recompute_hook(
        self,
        worker_id: str,
        hook: Callable[[str], FeatureBundle],
    ) -> None:
        with self._lock:
            self._recompute_hooks[worker_id] = hook

    def publish(self, bundle: FeatureBundle, *, incref: bool = False) -> str:
        self.shared_store.put(bundle.image_hash, bundle)
        if incref:
            self.shared_store.incref(bundle.image_hash)
        with self._lock:
            self._stats["published"] += 1
        self._log("PUBLISHED", image_hash=bundle.image_hash)
        return bundle.image_hash

    # ------------------------------------------------------------------
    # Prefetch
    # ------------------------------------------------------------------
    def prefetch(
        self,
        image_hash: str,
        *,
        target_worker_id: str,
        target_device: torch.device,
        policy: Optional[TransferPolicy] = None,
    ) -> MMStoreHandle:
        policy = policy or TransferPolicy(
            mode=Mode.PULL,
            channel=Channel.ENCODER_TO_PREFILL,
            extra={"force_copy": True},
        )
        cache = self.ensure_worker_cache(target_worker_id)
        cached = cache.get(image_hash)
        if cached is not None:
            event = MMStoreEvent(
                event_id=uuid.uuid4().hex,
                image_hash=image_hash,
                target_worker_id=target_worker_id,
                target_device=target_device,
                policy=policy,
                dedup_key=f"{target_worker_id}:{target_device}:{image_hash}",
                status="COMPLETED",
                worker_cache_hit=True,
            )
            handle = MMStoreHandle(
                event=event,
                result=cached,
                completed_at=time.monotonic(),
                worker_cache_hit=True,
            )
            handle.done.set()
            with self._lock:
                self._handles[event.event_id] = handle
                self._stats["worker_cache_hits"] += 1
            self._log(
                "WORKER_CACHE_HIT",
                event_id=event.event_id,
                image_hash=image_hash,
                target_worker_id=target_worker_id,
            )
            return handle

        dedup_key = f"{target_worker_id}:{target_device}:{image_hash}"
        with self._lock:
            existing_event_id = self._event_by_key.get(dedup_key)
            if existing_event_id is not None:
                existing = self._handles.get(existing_event_id)
                if existing is not None and not existing.done.is_set():
                    cloned_event = MMStoreEvent(
                        event_id=existing_event_id,
                        image_hash=image_hash,
                        target_worker_id=target_worker_id,
                        target_device=target_device,
                        policy=policy,
                        dedup_key=dedup_key,
                    )
                    handle = MMStoreHandle(event=cloned_event, deduplicated=True)
                    handle_id = uuid.uuid4().hex
                    self._handles[handle_id] = handle
                    self._waiters_by_event.setdefault(existing_event_id, []).append(handle_id)
                    self._stats["deduplicated_events"] += 1
                    self._stats["collapsed_waiters"] += 1
                    self._log(
                        "DEDUPLICATED",
                        event_id=existing_event_id,
                        image_hash=image_hash,
                        target_worker_id=target_worker_id,
                    )
                    return handle

            event = MMStoreEvent(
                event_id=uuid.uuid4().hex,
                image_hash=image_hash,
                target_worker_id=target_worker_id,
                target_device=target_device,
                policy=policy,
                dedup_key=dedup_key,
            )
            handle = MMStoreHandle(event=event)
            self._handles[event.event_id] = handle
            self._waiters_by_event[event.event_id] = [event.event_id]
            self._event_by_key[dedup_key] = event.event_id
            self._stats["queued"] += 1
            self._stats["inflight_peak"] = max(
                int(self._stats.get("inflight_peak", 0) or 0),
                len(self._event_by_key),
            )

        try:
            self._events.put(event, timeout=self.queue_put_timeout_s)
        except queue.Full as exc:
            with self._lock:
                self._stats["queue_full"] += 1
            self._log(
                "QUEUE_FULL",
                event_id=event.event_id,
                image_hash=image_hash,
                target_worker_id=target_worker_id,
            )
            if not self.inline_fallback_on_queue_full:
                with self._lock:
                    self._stats["backpressure_rejects"] += 1
                    self._waiters_by_event.pop(event.event_id, None)
                    self._event_by_key.pop(dedup_key, None)
                handle.error = MMStoreBackpressureError(
                    f"MMStore prefetch queue full for worker={target_worker_id} image_hash={image_hash}"
                )
                handle.done.set()
                raise handle.error from exc
            with self._lock:
                self._stats["inline_fallbacks"] += 1
                self._stats["sync_prefetches"] += 1
            self._process_event(event)
        self._log(
            "ENQUEUED",
            event_id=event.event_id,
            image_hash=image_hash,
            target_worker_id=target_worker_id,
        )
        return handle

    def wait(self, handle: MMStoreHandle, timeout: Optional[float] = None) -> Optional[FeatureBundle]:
        return handle.wait(timeout=timeout)

    def lookup(self, worker_id: str, image_hash: str) -> Optional[FeatureBundle]:
        cache = self.ensure_worker_cache(worker_id)
        return cache.get(image_hash)

    def stop(self) -> None:
        self._stop.set()
        for _ in range(self.dispatcher_workers):
            self._events.put(
                MMStoreEvent(
                    event_id="__stop__",
                    image_hash="__stop__",
                    target_worker_id="__stop__",
                    target_device=torch.device("cpu"),
                )
            )
        for dispatcher in self._dispatchers:
            dispatcher.join(timeout=2.0)

    # ------------------------------------------------------------------
    # Stats / internal execution
    # ------------------------------------------------------------------
    def stats(self) -> Dict[str, int]:
        with self._lock:
            ready = sum(
                1
                for handle in self._handles.values()
                if handle.done.is_set() and handle.error is None
            )
            return {
                "shared_entries": int(self.shared_store.stats()["entries"]),
                "worker_caches": len(self._worker_caches),
                "inflight_events": len(self._event_by_key),
                "completed_events": ready,
                "queue_capacity": self.max_queue_size,
                "queue_size": self._events.qsize(),
                "dispatcher_workers": self.dispatcher_workers,
                "published": self._stats["published"],
                "queued": self._stats["queued"],
                "completed": self._stats["completed"],
                "failed": self._stats["failed"],
                "deduplicated_events": self._stats["deduplicated_events"],
                "collapsed_waiters": self._stats["collapsed_waiters"],
                "worker_cache_hits": self._stats["worker_cache_hits"],
                "shared_store_hits": self._stats["shared_store_hits"],
                "shared_store_misses": self._stats["shared_store_misses"],
                "recompute_fallbacks": self._stats["recompute_fallbacks"],
                "queue_full": self._stats["queue_full"],
                "backpressure_rejects": self._stats["backpressure_rejects"],
                "inline_fallbacks": self._stats["inline_fallbacks"],
                "sync_prefetches": self._stats["sync_prefetches"],
                "inflight_peak": self._stats["inflight_peak"],
                "queue_wait_ms_avg": (
                    float(sum(self._stats["queue_wait_ms"]) / len(self._stats["queue_wait_ms"]))
                    if self._stats["queue_wait_ms"]
                    else 0.0
                ),
            }

    def _dispatch_loop(self) -> None:
        while not self._stop.is_set():
            event = self._events.get()
            if event.event_id == "__stop__":
                return
            self._process_event(event)

    def _process_event(self, event: MMStoreEvent) -> None:
        event.started_at = time.monotonic()
        with self._lock:
            queue_wait_ms = max(0.0, (event.started_at - event.created_at) * 1000.0)
            self._stats["queue_wait_ms"].append(queue_wait_ms)
        try:
            bundle = self.shared_store.get(event.image_hash)
            if bundle is None:
                with self._lock:
                    self._stats["shared_store_misses"] += 1
                hook = self._recompute_hooks.get(event.target_worker_id)
                if hook is None:
                    raise KeyError(f"feature bundle not found: {event.image_hash}")
                bundle = hook(event.image_hash)
                self.publish(bundle)
                event.recomputed = True
                with self._lock:
                    self._stats["recompute_fallbacks"] += 1
                self._log(
                    "RECOMPUTED",
                    event_id=event.event_id,
                    image_hash=event.image_hash,
                    target_worker_id=event.target_worker_id,
                )
            else:
                event.shared_store_hit = True
                with self._lock:
                    self._stats["shared_store_hits"] += 1

            moved = self.transfer.transfer_feature_bundle(
                bundle,
                event.target_device,
                event.policy,
            )
            cache = self.ensure_worker_cache(event.target_worker_id)
            cache.put(event.image_hash, moved)
            event.status = "COMPLETED"
            event.completed_at = time.monotonic()
            self._complete_event(event, result=moved, error=None)
        except BaseException as exc:  # pragma: no cover - exercised by waiters
            event.status = "FAILED"
            event.completed_at = time.monotonic()
            self._complete_event(event, result=None, error=exc)

    def _complete_event(
        self,
        event: MMStoreEvent,
        *,
        result: Optional[FeatureBundle],
        error: Optional[BaseException],
    ) -> None:
        with self._lock:
            waiter_ids = list(self._waiters_by_event.pop(event.event_id, []))
            self._event_by_key.pop(event.dedup_key, None)
            if error is None:
                self._stats["completed"] += 1
            else:
                self._stats["failed"] += 1
        for waiter_id in waiter_ids:
            handle = self._handles.get(waiter_id)
            if handle is None:
                continue
            handle.result = result
            handle.error = error
            handle.recomputed = event.recomputed
            handle.shared_store_hit = event.shared_store_hit
            handle.completed_at = event.completed_at
            handle.done.set()
        if error is None:
            self._log(
                "COMPLETED",
                event_id=event.event_id,
                image_hash=event.image_hash,
                target_worker_id=event.target_worker_id,
                recomputed=event.recomputed,
                shared_store_hit=event.shared_store_hit,
                elapsed_ms=(
                    0.0
                    if event.started_at is None or event.completed_at is None
                    else (event.completed_at - event.started_at) * 1000.0
                ),
            )
        else:
            self._log(
                "FAILED",
                event_id=event.event_id,
                image_hash=event.image_hash,
                target_worker_id=event.target_worker_id,
                error=repr(error),
            )

    def _log(self, action: str, **payload) -> None:
        if self._wal is None:
            return
        self._wal.append({"kind": "mm_store", "action": action, **payload})
