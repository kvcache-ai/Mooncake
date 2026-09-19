#!/usr/bin/env python3
"""Online E-stage service for Mooncake EPD FeatureHandle generation.

The service is intentionally thin: it owns the real vision encoder process,
publishes hidden-state FeatureBundles, and returns lightweight FeatureHandle
control payloads for the vLLM prefill hot path.  It can publish either to the
local file transport used by same-node development or to a real Mooncake Store
(``--publish-backend mooncake``).
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import concurrent.futures
import copy
import functools
import hashlib
import inspect
import io
import json
import logging
import os
import queue
import stat
import sys
import threading
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote, urlsplit
import torch
from fastapi import FastAPI, HTTPException
from PIL import Image

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT.parent))

from mooncake_epd.core.epd_workers import EncoderWorker  # noqa: E402
from mooncake_epd.core.omni_encoder_worker import Qwen25OmniImageEncoderWorker  # noqa: E402
from mooncake_epd.core.state import (  # noqa: E402
    FeatureBundle,
    FeatureBundleDescriptor,
    FeatureHandle,
    FeatureStore,
    MooncakeFeatureBundleStore,
    MooncakeFeatureBundleStoreConfig,
    TensorSpec,
    publish_feature_bundle_to_dir,
)
from mooncake_epd.core.transfer import TransferEngine  # noqa: E402
from mooncake_epd.multimodal_identity import stable_multimodal_identity_hash  # noqa: E402

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _accepts_keyword(fn: Any, keyword: str) -> bool:
    """Return whether a worker callable supports an optional capability."""

    try:
        parameters = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False
    parameter = parameters.get(keyword)
    if parameter is not None and parameter.kind is not inspect.Parameter.POSITIONAL_ONLY:
        return True
    return any(item.kind is inspect.Parameter.VAR_KEYWORD for item in parameters.values())


@dataclass(frozen=True)
class EncoderServiceConfig:
    model: str = os.getenv("MOONCAKE_EPD_MODEL_PATH", "/data01/LWX/Qwen3-VL-8B-Instruct")
    device: str = "cuda:0"
    dtype: str = "bfloat16"
    encoder_family: str = "auto"  # auto | qwen3_vl | qwen2_5_omni
    publish_backend: str = "file"  # file | mooncake | direct_engine
    store_dir: str = "/tmp/mooncake_epd_feature_handle_store"
    mooncake_store_url: Optional[str] = None
    mooncake_store_id: str = "mooncake-mm-store"
    mooncake_config: Optional[str] = None
    mooncake_timeout_s: float = 30.0
    mooncake_protocol: str = "tcp"
    mooncake_local_hostname: str = "localhost"
    mooncake_metadata_server: str = "P2PHANDSHAKE"
    mooncake_device_name: str = ""
    direct_source_mode: str = "registered_tensor"
    checksum: bool = False
    max_image_bytes: int = 32 * 1024 * 1024
    local_image_root: Optional[str] = None
    allow_http_image_urls: bool = False
    http_image_host_allowlist: str = ""
    request_timeout_s: float = 15.0
    direct_ticket_ttl_s: float = 60.0
    direct_ticket_max_entries: int = 32
    direct_ticket_max_bytes: int = 2 * 1024 * 1024 * 1024
    enable_omni_hidden_prefix_cache: bool = True
    omni_hidden_prefix_cache_metrics: Optional[str] = None
    omni_allow_partial_prefix: bool = False
    enable_feature_bundle_cache: bool = True
    feature_bundle_cache_max_entries: int = 32
    feature_bundle_cache_max_bytes: int = 2 * 1024 * 1024 * 1024
    feature_bundle_cache_admission_policy: str = "lru"
    qwen3_content_first_decode: bool = False
    enable_qwen3_preprocess_executors: bool = False
    qwen3_media_workers: int = 4
    qwen3_media_max_pending: int = 16
    qwen3_processor_workers: int = 4
    qwen3_processor_max_pending: int = 16
    enable_qwen3_vision_only_processor: bool = False
    enable_qwen3_predicted_descriptor_overlap: bool = False
    enable_qwen3_dynamic_batching: bool = False
    qwen3_vision_only: bool = False
    qwen3_dynamic_batch_max_size: int = 4
    qwen3_dynamic_batch_wait_ms: float = 2.0
    qwen3_dynamic_batch_min_inflight: int = 2
    qwen3_dynamic_batch_direct_credits: int = 0
    qwen3_dynamic_batch_max_queue: int = 64
    qwen3_dynamic_batch_max_patches: int = 65536
    qwen3_dynamic_batch_policy: str = "fifo"
    qwen3_dynamic_batch_reorder_window: int = 8
    qwen3_dynamic_batch_starvation_ms: float = 2000.0


class _PredictedDescriptorValidationError(ValueError):
    """Actual Qwen3 output violated its pre-compute descriptor contract."""


class _BoundedThreadExecutor:
    """Dedicated CPU stage executor with async backpressure and telemetry.

    ``ThreadPoolExecutor`` itself has an unbounded submission queue.  The
    semaphore bounds running plus queued work before submission, while the
    completion callback keeps the slot occupied if the awaiting HTTP task is
    cancelled but the underlying native/PIL/HF call cannot be interrupted.
    """

    def __init__(
        self,
        *,
        name: str,
        max_workers: int,
        max_pending: int,
        submit_timeout_s: float,
    ) -> None:
        self.name = str(name)
        self.max_workers = max(1, int(max_workers))
        self.max_pending = max(self.max_workers, int(max_pending))
        self.submit_timeout_s = max(0.001, float(submit_timeout_s))
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix=f"epd-{self.name}",
        )
        self._slots = asyncio.Semaphore(self.max_pending)
        self._closed = False
        self._metrics: Dict[str, Any] = {
            "submitted": 0,
            "completed": 0,
            "failures": 0,
            "caller_cancellations": 0,
            "queue_waits": 0,
            "admission_timeouts": 0,
            "queue_wait_ms_total": 0.0,
            "queue_wait_ms_max": 0.0,
            "execute_ms_total": 0.0,
            "execute_ms_max": 0.0,
            "inflight": 0,
            "peak_inflight": 0,
        }

    async def run(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        if self._closed:
            raise RuntimeError(f"{self.name} executor is closed")
        wait_started = time.perf_counter()
        try:
            if self._slots.locked():
                self._metrics["queue_waits"] += 1
                await asyncio.wait_for(
                    self._slots.acquire(),
                    timeout=self.submit_timeout_s,
                )
            else:
                # The uncontended Semaphore.acquire path completes without an
                # event-loop yield.  Avoid wrapping it in wait_for, which would
                # schedule all same-turn callers before any slot decrement and
                # make queue telemetry inaccurate.
                await self._slots.acquire()
        except asyncio.TimeoutError as exc:
            self._metrics["admission_timeouts"] += 1
            raise RuntimeError(
                f"{self.name} executor admission timed out"
            ) from exc

        queue_wait_ms = (time.perf_counter() - wait_started) * 1000.0
        self._metrics["queue_wait_ms_total"] += queue_wait_ms
        self._metrics["queue_wait_ms_max"] = max(
            float(self._metrics["queue_wait_ms_max"]),
            queue_wait_ms,
        )
        if self._closed:
            self._slots.release()
            raise RuntimeError(f"{self.name} executor is closed")

        loop = asyncio.get_running_loop()
        execute_started = time.perf_counter()
        try:
            future = loop.run_in_executor(
                self._executor,
                functools.partial(fn, *args, **kwargs),
            )
        except BaseException:
            self._slots.release()
            raise
        self._metrics["submitted"] += 1
        self._metrics["inflight"] += 1
        self._metrics["peak_inflight"] = max(
            int(self._metrics["peak_inflight"]),
            int(self._metrics["inflight"]),
        )

        def _complete(done: asyncio.Future) -> None:
            execute_ms = (time.perf_counter() - execute_started) * 1000.0
            self._metrics["completed"] += 1
            self._metrics["execute_ms_total"] += execute_ms
            self._metrics["execute_ms_max"] = max(
                float(self._metrics["execute_ms_max"]),
                execute_ms,
            )
            self._metrics["inflight"] = max(
                0,
                int(self._metrics["inflight"]) - 1,
            )
            try:
                if done.cancelled() or done.exception() is not None:
                    self._metrics["failures"] += 1
            except BaseException:
                self._metrics["failures"] += 1
            finally:
                self._slots.release()

        future.add_done_callback(_complete)
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            self._metrics["caller_cancellations"] += 1
            raise

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await asyncio.to_thread(
            self._executor.shutdown,
            wait=True,
            cancel_futures=False,
        )
        await asyncio.sleep(0)

    def stats(self) -> Dict[str, Any]:
        completed = int(self._metrics["completed"])
        return {
            "enabled": True,
            "name": self.name,
            "max_workers": self.max_workers,
            "max_pending": self.max_pending,
            "submit_timeout_s": self.submit_timeout_s,
            **dict(self._metrics),
            "queue_wait_ms_avg": (
                float(self._metrics["queue_wait_ms_total"])
                / int(self._metrics["submitted"])
                if int(self._metrics["submitted"])
                else 0.0
            ),
            "execute_ms_avg": (
                float(self._metrics["execute_ms_total"]) / completed
                if completed
                else 0.0
            ),
            "closed": self._closed,
        }


class _ExclusiveProcessorPool:
    """One independently cloned processor per concurrent processor call.

    Hugging Face processors are composite Python/Rust objects and do not expose
    a cross-version thread-safety contract.  A worker pool must therefore not
    call the same instance concurrently.  Replicas are built once after the
    real processor is loaded, then checked out exclusively inside the bounded
    processor executor.  Clone failure is fail-closed rather than silently
    falling back to shared mutable state.
    """

    def __init__(self, processor: Any, replicas: int) -> None:
        started = time.perf_counter()
        size = max(1, int(replicas))
        processors = [processor]
        for _ in range(1, size):
            processors.append(copy.deepcopy(processor))
        self._available: queue.LifoQueue[Any] = queue.LifoQueue(maxsize=size)
        for item in processors:
            self._available.put_nowait(item)
        self._lock = threading.Lock()
        self._metrics: Dict[str, Any] = {
            "replicas": size,
            "clone_ms": (time.perf_counter() - started) * 1000.0,
            "checkouts": 0,
            "failures": 0,
            "in_use": 0,
            "peak_in_use": 0,
        }

    def _process(
        self,
        fn: Callable[..., Dict[str, torch.Tensor]],
        /,
        *args: Any,
    ) -> Dict[str, torch.Tensor]:
        processor = self._available.get()
        with self._lock:
            self._metrics["checkouts"] += 1
            self._metrics["in_use"] += 1
            self._metrics["peak_in_use"] = max(
                int(self._metrics["peak_in_use"]),
                int(self._metrics["in_use"]),
            )
        try:
            return fn(processor, *args)
        except BaseException:
            with self._lock:
                self._metrics["failures"] += 1
            raise
        finally:
            with self._lock:
                self._metrics["in_use"] = max(
                    0,
                    int(self._metrics["in_use"]) - 1,
                )
            self._available.put_nowait(processor)

    def process(self, image: Image.Image, prompt: str) -> Dict[str, torch.Tensor]:
        return self._process(_processor_inputs, image, prompt)

    def process_vision_only(self, image: Image.Image) -> Dict[str, torch.Tensor]:
        return self._process(_processor_vision_inputs, image)

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "enabled": True,
                **dict(self._metrics),
                "available": self._available.qsize(),
            }


@dataclass
class _QueuedQwen3Encode:
    worker: Any
    pixel_values: torch.Tensor
    image_grid_thw: torch.Tensor
    image_id: str
    enqueued_at: float
    future: asyncio.Future

    @property
    def patch_count(self) -> int:
        return int(self.pixel_values.shape[0]) if self.pixel_values.ndim else 1

    @property
    def compatibility_key(self) -> Tuple[Any, ...]:
        return (
            id(self.worker),
            str(self.pixel_values.dtype),
            tuple(int(dim) for dim in self.pixel_values.shape[1:]),
        )


class _Qwen3DynamicBatcher:
    """Bounded deadline-aware microbatcher for independent Qwen3-VL images."""

    def __init__(
        self,
        *,
        max_batch_size: int,
        max_wait_ms: float,
        max_queue: int,
        max_patches: int,
        submit_timeout_s: float,
        policy: str = "fifo",
        reorder_window: int = 8,
        starvation_ms: float = 2000.0,
        clock: Callable[[], float] = time.perf_counter,
    ):
        self.max_batch_size = max(1, int(max_batch_size))
        self.max_wait_s = max(0.0, float(max_wait_ms)) / 1000.0
        self.max_patches = max(1, int(max_patches))
        self.submit_timeout_s = max(0.001, float(submit_timeout_s))
        self.policy = str(policy).strip().lower()
        if self.policy not in {"fifo", "patch_aging"}:
            raise ValueError(f"unsupported Qwen3 dynamic batch policy: {policy}")
        if self.policy == "patch_aging" and self.max_batch_size != 1:
            raise ValueError("Qwen3 patch_aging policy requires max_batch_size=1")
        self.reorder_window = max(1, int(reorder_window))
        self.starvation_s = max(0.0, float(starvation_ms)) / 1000.0
        self._clock = clock
        self._queue: asyncio.Queue[_QueuedQwen3Encode] = asyncio.Queue(
            maxsize=max(1, int(max_queue))
        )
        self._runner: Optional[asyncio.Task] = None
        self._closed = False
        self._close_event = asyncio.Event()
        self._put_tasks: set[asyncio.Task] = set()
        self._active_futures: Dict[asyncio.Future, bool] = {}
        self._native_encode_tasks: set[asyncio.Task] = set()
        self._queue_wait_samples_ms = deque(maxlen=2048)
        self._batch_encode_samples_ms = deque(maxlen=2048)
        self._metrics: Dict[str, Any] = {
            "submitted": 0,
            "completed": 0,
            "skipped_cancelled": 0,
            "batches": 0,
            "singleton_batches": 0,
            "multi_item_batches": 0,
            "max_observed_batch_size": 0,
            "max_observed_batch_patches": 0,
            "queue_full_waits": 0,
            "failures": 0,
            "queue_wait_ms_total": 0.0,
            "batch_encode_ms_total": 0.0,
            "batch_size_counts": {},
            "queue_high_watermark": 0,
            "selection_scans": 0,
            "selection_items_total": 0,
            "max_selection_width": 0,
            "reordered_selections": 0,
            "starvation_overrides": 0,
            "selected_patch_bucket_counts": {},
            "max_observed_queue_wait_ms": 0.0,
        }

    @staticmethod
    def _patch_bucket(patch_count: int) -> str:
        patches = max(0, int(patch_count))
        if patches <= 512:
            return "small_le_512"
        if patches <= 4096:
            return "medium_513_4096"
        if patches <= 16384:
            return "large_4097_16384"
        return "xlarge_gt_16384"

    @staticmethod
    def _sample_stats(samples: Iterable[float]) -> Dict[str, Any]:
        values = sorted(float(value) for value in samples)
        if not values:
            return {"count": 0, "p50": None, "p95": None, "p99": None, "max": None}

        def percentile(q: float) -> float:
            position = (len(values) - 1) * q
            lower = int(position)
            upper = min(len(values) - 1, lower + 1)
            weight = position - lower
            return values[lower] * (1.0 - weight) + values[upper] * weight

        return {
            "count": len(values),
            "p50": percentile(0.50),
            "p95": percentile(0.95),
            "p99": percentile(0.99),
            "max": values[-1],
        }

    def _select_patch_aging(
        self,
        first: _QueuedQwen3Encode,
    ) -> Optional[_QueuedQwen3Encode]:
        ready = [first]
        while len(ready) < self.reorder_window:
            try:
                ready.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        live: List[_QueuedQwen3Encode] = []
        for item in ready:
            if item.future.cancelled():
                self._metrics["skipped_cancelled"] += 1
                self._queue.task_done()
            else:
                live.append(item)
        if not live:
            return None

        now = self._clock()
        fifo = live[0]
        oldest = min(live, key=lambda item: item.enqueued_at)
        shortest = min(live, key=lambda item: (item.patch_count, item.enqueued_at))
        oldest_age_s = max(0.0, now - oldest.enqueued_at)
        starvation_override = (
            self.starvation_s > 0.0
            and oldest_age_s >= self.starvation_s
            and oldest is not shortest
        )
        selected = oldest if starvation_override else shortest
        self._metrics["selection_scans"] += 1
        self._metrics["selection_items_total"] += len(live)
        self._metrics["max_selection_width"] = max(
            int(self._metrics["max_selection_width"]),
            len(live),
        )
        self._metrics["reordered_selections"] += int(selected is not fifo)
        self._metrics["starvation_overrides"] += int(starvation_override)
        bucket = self._patch_bucket(selected.patch_count)
        buckets = self._metrics["selected_patch_bucket_counts"]
        buckets[bucket] = int(buckets.get(bucket, 0)) + 1

        # get_nowait() wakes blocked putters, but this method does not await.
        # Return every unselected item synchronously, preserving their order
        # and net unfinished-task count before another producer can run.
        for item in live:
            if item is selected:
                continue
            self._queue.task_done()
            self._queue.put_nowait(item)
        return selected

    def _ensure_runner(self) -> None:
        if self._closed:
            raise RuntimeError("Qwen3 dynamic batcher is closed")
        if self._runner is None or self._runner.done():
            self._runner = asyncio.create_task(
                self._run(),
                name="epd-qwen3-dynamic-batcher",
            )

    async def submit(
        self,
        *,
        worker: Any,
        pixel_values: torch.Tensor,
        image_grid_thw: torch.Tensor,
        image_id: str,
    ) -> Any:
        self._ensure_runner()
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        item = _QueuedQwen3Encode(
            worker=worker,
            pixel_values=pixel_values,
            image_grid_thw=image_grid_thw,
            image_id=str(image_id),
            enqueued_at=self._clock(),
            future=future,
        )
        if self._queue.full():
            self._metrics["queue_full_waits"] += 1
        self._active_futures[future] = False
        put_task = asyncio.create_task(
            self._queue.put(item),
            name="epd-qwen3-dynamic-batcher-put",
        )
        close_task = asyncio.create_task(
            self._close_event.wait(),
            name="epd-qwen3-dynamic-batcher-close-wait",
        )
        self._put_tasks.add(put_task)
        try:
            done, _pending = await asyncio.wait(
                {put_task, close_task},
                timeout=self.submit_timeout_s,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                put_task.cancel()
                future.cancel()
                raise RuntimeError("Qwen3 dynamic batch queue admission timed out")
            if close_task in done or self._closed:
                put_task.cancel()
                if not future.done():
                    future.set_exception(RuntimeError("Qwen3 dynamic batcher is closed"))
                return await future
            await put_task
            self._active_futures[future] = True
            self._metrics["submitted"] += 1
            self._metrics["queue_high_watermark"] = max(
                int(self._metrics["queue_high_watermark"]),
                self._queue.qsize(),
            )
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            future.cancel()
            raise
        finally:
            if not put_task.done():
                put_task.cancel()
            if not close_task.done():
                close_task.cancel()
            await asyncio.gather(put_task, close_task, return_exceptions=True)
            self._put_tasks.discard(put_task)
            self._active_futures.pop(future, None)

    @staticmethod
    def _compatible(first: _QueuedQwen3Encode, candidate: _QueuedQwen3Encode) -> bool:
        return first.compatibility_key == candidate.compatibility_key

    async def _run(self) -> None:
        pending: Optional[_QueuedQwen3Encode] = None
        try:
            while True:
                first = pending if pending is not None else await self._queue.get()
                pending = None
                if self.policy == "patch_aging":
                    first = self._select_patch_aging(first)
                    if first is None:
                        continue
                batch = [first]
                patch_count = first.patch_count
                deadline = asyncio.get_running_loop().time() + self.max_wait_s
                while len(batch) < self.max_batch_size:
                    candidate: Optional[_QueuedQwen3Encode] = None
                    try:
                        candidate = self._queue.get_nowait()
                    except asyncio.QueueEmpty:
                        remaining = deadline - asyncio.get_running_loop().time()
                        if remaining <= 0.0:
                            break
                        try:
                            candidate = await asyncio.wait_for(
                                self._queue.get(),
                                timeout=remaining,
                            )
                        except asyncio.TimeoutError:
                            break
                    if candidate is None:
                        break
                    if (
                        not self._compatible(first, candidate)
                        or patch_count + candidate.patch_count > self.max_patches
                    ):
                        pending = candidate
                        break
                    batch.append(candidate)
                    patch_count += candidate.patch_count

                live_batch = [item for item in batch if not item.future.cancelled()]
                skipped_cancelled = len(batch) - len(live_batch)
                if skipped_cancelled:
                    self._metrics["skipped_cancelled"] += skipped_cancelled
                    for item in batch:
                        if item.future.cancelled():
                            self._queue.task_done()
                batch = live_batch
                if not batch:
                    continue
                first = batch[0]
                patch_count = sum(item.patch_count for item in batch)

                started = self._clock()
                for item in batch:
                    queue_wait_ms = max(0.0, started - item.enqueued_at) * 1000.0
                    self._metrics["queue_wait_ms_total"] += queue_wait_ms
                    self._queue_wait_samples_ms.append(queue_wait_ms)
                    self._metrics["max_observed_queue_wait_ms"] = max(
                        float(self._metrics["max_observed_queue_wait_ms"]),
                        queue_wait_ms,
                    )
                try:
                    if not hasattr(first.worker, "encode_many"):
                        raise RuntimeError(
                            "Qwen3 dynamic batching requires EncoderWorker.encode_many"
                        )
                    native_task = asyncio.create_task(
                        asyncio.to_thread(
                            first.worker.encode_many,
                            [
                                (item.pixel_values, item.image_grid_thw, item.image_id)
                                for item in batch
                            ],
                        ),
                        name="epd-qwen3-dynamic-batch-native-encode",
                    )
                    self._native_encode_tasks.add(native_task)
                    native_task.add_done_callback(self._native_encode_tasks.discard)
                    # Cancelling the async runner cannot interrupt an already
                    # executing native/CUDA call.  Shield it so close() can
                    # track and await the real completion before model and
                    # transfer resources are torn down.
                    outputs = await asyncio.shield(native_task)
                    if len(outputs) != len(batch):
                        raise RuntimeError(
                            "Qwen3 dynamic batch output count mismatch: "
                            f"outputs={len(outputs)} batch={len(batch)}"
                        )
                except asyncio.CancelledError:
                    stopped = RuntimeError(
                        "Qwen3 dynamic batcher stopped during execution"
                    )
                    for item in batch:
                        if not item.future.done():
                            item.future.set_exception(stopped)
                    raise
                except Exception as exc:
                    self._metrics["failures"] += len(batch)
                    for item in batch:
                        if not item.future.done():
                            item.future.set_exception(exc)
                else:
                    for item, output in zip(batch, outputs):
                        if not item.future.done():
                            item.future.set_result(output)
                    self._metrics["completed"] += len(batch)
                finally:
                    elapsed_ms = max(0.0, self._clock() - started) * 1000.0
                    size = len(batch)
                    self._metrics["batches"] += 1
                    self._metrics["batch_encode_ms_total"] += elapsed_ms
                    self._batch_encode_samples_ms.append(elapsed_ms)
                    self._metrics["singleton_batches"] += int(size == 1)
                    self._metrics["multi_item_batches"] += int(size > 1)
                    self._metrics["max_observed_batch_size"] = max(
                        int(self._metrics["max_observed_batch_size"]),
                        size,
                    )
                    self._metrics["max_observed_batch_patches"] = max(
                        int(self._metrics["max_observed_batch_patches"]),
                        patch_count,
                    )
                    counts = self._metrics["batch_size_counts"]
                    counts[str(size)] = int(counts.get(str(size), 0)) + 1
                    for _item in batch:
                        self._queue.task_done()
        except asyncio.CancelledError:
            raise
        finally:
            abandoned: List[_QueuedQwen3Encode] = []
            if pending is not None:
                abandoned.append(pending)
            while True:
                try:
                    abandoned.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            for item in abandoned:
                if item.future.cancelled():
                    self._metrics["skipped_cancelled"] += 1
                elif not item.future.done():
                    item.future.set_exception(
                        RuntimeError("Qwen3 dynamic batcher stopped before execution")
                    )
                    self._metrics["failures"] += 1
                self._queue.task_done()

    def stats(self) -> Dict[str, Any]:
        submitted = int(self._metrics["submitted"])
        completed = int(self._metrics["completed"])
        batches = int(self._metrics["batches"])
        return {
            "enabled": True,
            "max_batch_size": self.max_batch_size,
            "max_wait_ms": self.max_wait_s * 1000.0,
            "max_queue": self._queue.maxsize,
            "max_patches": self.max_patches,
            "scheduler_policy": self.policy,
            "reorder_window": self.reorder_window,
            "starvation_ms": self.starvation_s * 1000.0,
            "queue_size": self._queue.qsize(),
            "native_encode_inflight": len(self._native_encode_tasks),
            **dict(self._metrics),
            "avg_completed_batch_size": completed / batches if batches else 0.0,
            "avg_queue_wait_ms": (
                float(self._metrics["queue_wait_ms_total"]) / completed
                if completed
                else 0.0
            ),
            "avg_batch_encode_ms": (
                float(self._metrics["batch_encode_ms_total"]) / batches
                if batches
                else 0.0
            ),
            "queue_wait_ms": self._sample_stats(self._queue_wait_samples_ms),
            "batch_encode_ms": self._sample_stats(self._batch_encode_samples_ms),
            "pending": max(
                0,
                submitted
                - completed
                - int(self._metrics["failures"])
                - int(self._metrics["skipped_cancelled"]),
            ),
        }

    async def close(self) -> None:
        if self._closed and not self._put_tasks and not self._native_encode_tasks:
            return
        self._closed = True
        self._close_event.set()
        put_tasks = list(self._put_tasks)
        for task in put_tasks:
            if not task.done():
                task.cancel()
        if put_tasks:
            await asyncio.gather(*put_tasks, return_exceptions=True)

        failed_admitted = 0
        for future, admitted in list(self._active_futures.items()):
            if future.done():
                continue
            future.set_exception(
                RuntimeError(
                    "Qwen3 dynamic batcher stopped during execution"
                    if admitted
                    else "Qwen3 dynamic batcher closed before queue admission"
                )
            )
            failed_admitted += int(admitted)
        self._metrics["failures"] += failed_admitted

        runner = self._runner
        if runner is not None and not runner.done():
            runner.cancel()
            await asyncio.gather(runner, return_exceptions=True)
        native_tasks = list(self._native_encode_tasks)
        if native_tasks:
            await asyncio.gather(*native_tasks, return_exceptions=True)
        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if item.future.cancelled():
                self._metrics["skipped_cancelled"] += 1
            elif not item.future.done():
                item.future.set_exception(
                    RuntimeError("Qwen3 dynamic batcher stopped before execution")
                )
                self._metrics["failures"] += 1
            self._queue.task_done()


class _Qwen3BatchAdmission:
    """Burst-scoped concurrency gate for latency-safe dynamic batching.

    HTTP ingress is counted before request-body preprocessing. Small bursts
    bypass the microbatch queue entirely. Once request pressure crosses
    ``min_inflight``, the whole overlapping burst is latched into batch mode
    until it drains, preventing individual late arrivals from oscillating
    between direct and batched execution.
    """

    def __init__(self, min_inflight: int, direct_credits_per_burst: int = 0):
        self.min_inflight = max(1, int(min_inflight))
        self.direct_credits_per_burst = max(0, int(direct_credits_per_burst))
        self.direct_credits_remaining = 0
        self.active = 0
        self.peak_active = 0
        self.batch_burst = False
        self.burst_activations = 0
        self.direct_decisions = 0
        self.direct_credit_decisions = 0
        self.batched_decisions = 0

    def enter(self) -> None:
        self.active += 1
        self.peak_active = max(self.peak_active, self.active)
        if not self.batch_burst and self.active >= self.min_inflight:
            self.batch_burst = True
            self.burst_activations += 1
            self.direct_credits_remaining = self.direct_credits_per_burst

    def leave(self) -> None:
        if self.active <= 0:
            raise RuntimeError("Qwen3 batch admission active count underflow")
        self.active -= 1
        if self.active == 0:
            self.batch_burst = False
            self.direct_credits_remaining = 0

    def should_batch(self) -> bool:
        # Credits are deliberately consumed at the ViT admission point, not
        # by HTTP arrival order.  Requests that already reached ViT before the
        # pressure latch remain direct; the first ready work after latching
        # seeds the GPU before the remaining burst is serialized.
        decision = bool(self.batch_burst)
        if decision and self.direct_credits_remaining > 0:
            self.direct_credits_remaining -= 1
            self.direct_credit_decisions += 1
            decision = False
        if decision:
            self.batched_decisions += 1
        else:
            self.direct_decisions += 1
        return decision

    def stats(self) -> Dict[str, Any]:
        return {
            "enabled": True,
            "min_inflight": self.min_inflight,
            "direct_credits_per_burst": self.direct_credits_per_burst,
            "direct_credits_remaining": self.direct_credits_remaining,
            "active": self.active,
            "peak_active": self.peak_active,
            "batch_burst": self.batch_burst,
            "burst_activations": self.burst_activations,
            "direct_decisions": self.direct_decisions,
            "direct_credit_decisions": self.direct_credit_decisions,
            "batched_decisions": self.batched_decisions,
        }


class _Qwen3VLVisionOnlyModel:
    """Minimal feature API over the checkpoint's standalone Qwen3-VL tower."""

    def __init__(self, visual: Any, config: Any, model_path: str):
        self.visual = visual
        self.config = config
        self.name_or_path = str(model_path)

    def eval(self) -> "_Qwen3VLVisionOnlyModel":
        self.visual.eval()
        return self

    def get_image_features(
        self,
        pixel_values: torch.Tensor,
        image_grid_thw: torch.Tensor,
    ) -> Any:
        pixel_values = pixel_values.to(dtype=self.visual.dtype)
        vision_output = self.visual(
            pixel_values,
            grid_thw=image_grid_thw,
            return_dict=True,
        )
        split_sizes = (
            image_grid_thw.prod(-1) // self.visual.spatial_merge_size**2
        ).tolist()
        vision_output.pooler_output = torch.split(
            vision_output.pooler_output,
            split_sizes,
        )
        return vision_output


def _load_qwen3_vision_only_model(
    model_path: str,
    *,
    dtype: torch.dtype,
    device: torch.device,
) -> Tuple[_Qwen3VLVisionOnlyModel, Dict[str, Any]]:
    """Load only ``model.visual.*`` tensors from a sharded Qwen3-VL checkpoint."""

    from safetensors import safe_open
    from transformers import Qwen3VLConfig
    from transformers.models.qwen3_vl.modeling_qwen3_vl import Qwen3VLVisionModel

    started = time.perf_counter()
    root = Path(model_path).resolve()
    index_path = root / "model.safetensors.index.json"
    if not index_path.is_file():
        raise FileNotFoundError(
            "Qwen3-VL vision-only loading requires model.safetensors.index.json: "
            f"{index_path}"
        )
    index = json.loads(index_path.read_text(encoding="utf-8"))
    weight_map = dict(index.get("weight_map") or {})
    prefix = "model.visual."
    selected = {
        str(key): str(shard)
        for key, shard in weight_map.items()
        if str(key).startswith(prefix)
    }
    if not selected:
        raise RuntimeError(f"checkpoint index contains no {prefix} weights")

    config = Qwen3VLConfig.from_pretrained(root, local_files_only=True)
    with torch.device("meta"):
        visual = Qwen3VLVisionModel(config.vision_config)

    state_dict: Dict[str, torch.Tensor] = {}
    shards = sorted(set(selected.values()))
    for shard in shards:
        shard_path = root / shard
        if not shard_path.is_file():
            raise FileNotFoundError(f"vision checkpoint shard is missing: {shard_path}")
        with safe_open(str(shard_path), framework="pt", device="cpu") as reader:
            for full_key, mapped_shard in selected.items():
                if mapped_shard == shard:
                    state_dict[full_key.removeprefix(prefix)] = reader.get_tensor(full_key)

    load_result = visual.load_state_dict(state_dict, strict=True, assign=True)
    if load_result.missing_keys or load_result.unexpected_keys:
        raise RuntimeError(
            "Qwen3-VL vision-only checkpoint mismatch: "
            f"missing={load_result.missing_keys} unexpected={load_result.unexpected_keys}"
        )
    # ``inv_freq`` is a non-persistent buffer, so it is intentionally absent
    # from the checkpoint state dict. Materialize the value that Transformers'
    # normal Qwen3-VL initializer creates; all persistent parameters above must
    # still match strictly.
    rotary = visual.rotary_pos_emb
    if rotary.inv_freq.is_meta:
        rotary.inv_freq = 1.0 / (
            rotary.theta
            ** (
                torch.arange(0, rotary.dim, 2, dtype=torch.float32)
                / float(rotary.dim)
            )
        )
    remaining_meta = [
        name
        for name, tensor in list(visual.named_parameters()) + list(visual.named_buffers())
        if tensor.is_meta
    ]
    if remaining_meta:
        raise RuntimeError(f"vision-only model retains meta tensors: {remaining_meta}")
    checkpoint_bytes = sum(int(tensor.nbytes) for tensor in state_dict.values())
    del state_dict
    # Cast checkpoint parameters explicitly, then move the module without a
    # module-wide dtype conversion. Qwen3-VL intentionally keeps rotary
    # ``inv_freq`` in fp32 even when weights are bf16; casting that buffer
    # causes a measurable feature drift through all 27 vision blocks.
    for parameter in visual.parameters():
        if parameter.is_floating_point() and parameter.dtype != dtype:
            parameter.data = parameter.data.to(dtype=dtype)
    visual.to(device=device)
    visual.eval()
    model = _Qwen3VLVisionOnlyModel(visual, config, str(root)).eval()
    stats = {
        "mode": "vision_only",
        "attention_implementation": str(
            getattr(visual.config, "_attn_implementation", "") or ""
        ),
        "checkpoint_prefix": prefix,
        "checkpoint_shards": shards,
        "checkpoint_tensors": len(selected),
        "parameter_bytes": int(
            sum(parameter.numel() * parameter.element_size() for parameter in visual.parameters())
        ),
        "checkpoint_bytes": int(checkpoint_bytes),
        "load_ms": (time.perf_counter() - started) * 1000.0,
    }
    return model, stats


@dataclass
class _DirectEngineSlot:
    engine: TransferEngine
    generation: int
    owned: bool
    in_flight: int = 0


class _DirectEngineLease:
    """Reference-counted access to one quiescence-safe engine generation."""

    def __init__(
        self,
        coordinator: "_DirectTransferEngineCoordinator",
        slot: _DirectEngineSlot,
        recovery_reason: str,
    ):
        self._coordinator = coordinator
        self._slot = slot
        self.recovery_reason = str(recovery_reason or "none")
        self._released = False

    @property
    def engine(self) -> TransferEngine:
        return self._slot.engine

    @property
    def generation(self) -> int:
        return int(self._slot.generation)

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._coordinator.release(self._slot)


class _DirectTransferEngineCoordinator:
    """Keep steady-state direct transfers concurrent while making resets safe.

    Mooncake's current Python binding does not expose remote segment-cache
    invalidation. Recreating an engine while another native transfer is using
    it is unsafe, and running two locally-owned engines concurrently can also
    collide in metadata. This coordinator therefore only serializes the rare
    generation transition: new leases pause, existing leases drain, the old
    owned engine shuts down, and a fresh engine is installed. Normal transfers
    hold no global execution lock.
    """

    def __init__(
        self,
        factory: Callable[[], TransferEngine],
        *,
        initial_engine: Optional[TransferEngine] = None,
        owns_initial_engine: bool = False,
    ):
        self._factory = factory
        self._condition = threading.Condition(threading.RLock())
        self._generation = 0
        self._current: Optional[_DirectEngineSlot] = None
        if initial_engine is not None:
            self._generation = 1
            self._current = _DirectEngineSlot(
                engine=initial_engine,
                generation=self._generation,
                owned=bool(owns_initial_engine),
            )
        self._rebuilding = False
        self._closed = False
        self._remote_incarnations: Dict[str, str] = {}
        self._metrics: Dict[str, Any] = {
            "engine_creations": 0,
            "engine_resets": 0,
            "proactive_resets": 0,
            "reactive_resets": 0,
            "publish_retries": 0,
            "recovery_successes": 0,
            "recovery_failures": 0,
            "engine_rebuild_failures": 0,
            "reset_wait_ms": 0.0,
            "reset_initialize_ms": 0.0,
            "last_reset_reason": "none",
            "last_reset_at_unix_s": 0.0,
        }

    def _wait_until_available_locked(self) -> None:
        while self._rebuilding and not self._closed:
            self._condition.wait()
        if self._closed:
            raise RuntimeError("direct TransferEngine coordinator is closed")

    def _create_engine_locked(self) -> _DirectEngineSlot:
        started = time.perf_counter()
        engine: Optional[TransferEngine] = None
        try:
            engine = self._factory()
            engine.initialize()
        except Exception:
            if engine is not None:
                try:
                    engine.shutdown()
                except Exception:
                    logger.exception("failed to close partially initialized direct TransferEngine")
            raise
        finally:
            self._metrics["reset_initialize_ms"] += (
                time.perf_counter() - started
            ) * 1000.0
        self._generation += 1
        self._metrics["engine_creations"] += 1
        return _DirectEngineSlot(
            engine=engine,
            generation=self._generation,
            owned=True,
        )

    def _ensure_current_locked(self) -> _DirectEngineSlot:
        if self._current is not None:
            return self._current
        self._rebuilding = True
        try:
            self._current = self._create_engine_locked()
            return self._current
        finally:
            self._rebuilding = False
            self._condition.notify_all()

    def _rebuild_locked(self, *, reason: str) -> _DirectEngineSlot:
        self._rebuilding = True
        old = self._current
        wait_started = time.perf_counter()
        try:
            while old is not None and old.in_flight > 0:
                self._condition.wait()
            self._metrics["reset_wait_ms"] += (
                time.perf_counter() - wait_started
            ) * 1000.0
            self._current = None
            if old is not None and old.owned:
                old.engine.shutdown()
            try:
                fresh = self._create_engine_locked()
            except Exception:
                self._metrics["engine_rebuild_failures"] += 1
                raise
            self._current = fresh
            self._metrics["engine_resets"] += 1
            if reason == "proactive_incarnation":
                self._metrics["proactive_resets"] += 1
            elif reason == "reactive_transfer_failure":
                self._metrics["reactive_resets"] += 1
            self._metrics["last_reset_reason"] = reason
            self._metrics["last_reset_at_unix_s"] = time.time()
            logger.warning(
                "rebuilt direct TransferEngine generation=%s reason=%s",
                fresh.generation,
                reason,
            )
            return fresh
        finally:
            self._rebuilding = False
            self._condition.notify_all()

    @staticmethod
    def _lease(
        coordinator: "_DirectTransferEngineCoordinator",
        slot: _DirectEngineSlot,
        recovery_reason: str,
    ) -> _DirectEngineLease:
        slot.in_flight += 1
        return _DirectEngineLease(coordinator, slot, recovery_reason)

    def acquire(
        self,
        *,
        remote_session: str,
        remote_incarnation: str,
    ) -> _DirectEngineLease:
        with self._condition:
            self._wait_until_available_locked()
            recovery_reason = "none"
            session = str(remote_session or "")
            incarnation = str(remote_incarnation or "")
            previous = self._remote_incarnations.get(session) if incarnation else None
            if (
                incarnation
                and previous is not None
                and previous != incarnation
                and self._current is not None
            ):
                self._rebuild_locked(reason="proactive_incarnation")
                recovery_reason = "proactive_incarnation"
            slot = self._ensure_current_locked()
            if incarnation:
                self._remote_incarnations[session] = incarnation
            return self._lease(self, slot, recovery_reason)

    def recover_after_transfer_failure(
        self,
        *,
        failed_generation: int,
    ) -> _DirectEngineLease:
        with self._condition:
            self._metrics["publish_retries"] += 1
            self._wait_until_available_locked()
            if (
                self._current is None
                or int(self._current.generation) == int(failed_generation)
            ):
                slot = self._rebuild_locked(reason="reactive_transfer_failure")
                reason = "reactive_transfer_failure"
            else:
                # A concurrent failed publisher already rebuilt this generation.
                slot = self._current
                reason = "reactive_transfer_failure_coalesced"
            return self._lease(self, slot, reason)

    def release(self, slot: _DirectEngineSlot) -> None:
        with self._condition:
            if slot.in_flight <= 0:
                raise RuntimeError("direct TransferEngine lease released more than once")
            slot.in_flight -= 1
            self._condition.notify_all()

    def record_recovery(self, *, success: bool) -> None:
        with self._condition:
            key = "recovery_successes" if success else "recovery_failures"
            self._metrics[key] += 1

    def stats(self) -> Dict[str, Any]:
        with self._condition:
            slot = self._current
            return {
                "enabled": True,
                "initialized": slot is not None,
                "generation": 0 if slot is None else int(slot.generation),
                "inflight": 0 if slot is None else int(slot.in_flight),
                "owns_current": False if slot is None else bool(slot.owned),
                "rebuilding": bool(self._rebuilding),
                "tracked_remote_sessions": len(self._remote_incarnations),
                "remote_incarnation_fingerprints": {
                    session: hashlib.sha256(incarnation.encode("utf-8")).hexdigest()[:16]
                    for session, incarnation in self._remote_incarnations.items()
                },
                **dict(self._metrics),
            }

    def shutdown(self) -> None:
        with self._condition:
            if self._closed:
                return
            self._rebuilding = True
            slot = self._current
            while slot is not None and slot.in_flight > 0:
                self._condition.wait()
            self._current = None
            self._closed = True
            try:
                if slot is not None and slot.owned:
                    slot.engine.shutdown()
            finally:
                self._rebuilding = False
                self._condition.notify_all()


def _is_recoverable_direct_transfer_failure(exc: BaseException) -> bool:
    return isinstance(exc, RuntimeError) and str(exc).startswith(
        "peer-buffer transfer failed: rc="
    )


class _LazyEncoder:
    def __init__(self, config: EncoderServiceConfig):
        self.config = config
        self._lock = asyncio.Lock()
        self._worker: Optional[Any] = None
        self._processor = None
        self._model = None
        self._load_stats: Dict[str, Any] = {
            "mode": "unloaded",
            "load_ms": 0.0,
            "parameter_bytes": 0,
        }

    async def worker(self) -> Any:
        if self._worker is not None:
            return self._worker
        async with self._lock:
            if self._worker is None:
                self._worker = await asyncio.to_thread(self._load_sync)
            return self._worker

    def _load_sync(self) -> Any:
        from transformers import AutoConfig, AutoProcessor

        dtype = _torch_dtype(self.config.dtype)
        device = torch.device(self.config.device)
        family = str(self.config.encoder_family or "auto").lower()
        if family == "auto":
            cfg = AutoConfig.from_pretrained(self.config.model, trust_remote_code=True)
            model_type = str(getattr(cfg, "model_type", "") or "").lower()
            family = "qwen2_5_omni" if model_type == "qwen2_5_omni" else "qwen3_vl"
        processor = AutoProcessor.from_pretrained(self.config.model, trust_remote_code=True)
        if family in {"qwen2_5_omni", "qwen25_omni", "omni"}:
            from transformers import Qwen2_5OmniThinkerForConditionalGeneration

            model = Qwen2_5OmniThinkerForConditionalGeneration.from_pretrained(
                self.config.model,
                dtype=dtype,
                device_map={"": device},
                low_cpu_mem_usage=True,
                local_files_only=True,
                trust_remote_code=True,
            )
            model.eval()
            self._processor = processor
            self._model = model
            return Qwen25OmniImageEncoderWorker(
                model,
                processor,
                device=device,
                enable_hidden_prefix_cache=bool(self.config.enable_omni_hidden_prefix_cache),
                allow_partial_prefix_reuse=bool(self.config.omni_allow_partial_prefix),
                cache_metrics_path=self.config.omni_hidden_prefix_cache_metrics,
            )
        if family in {"qwen3_vl", "qwen_vl", "qwen3vl"}:
            if self.config.qwen3_vision_only:
                model, load_stats = _load_qwen3_vision_only_model(
                    self.config.model,
                    dtype=dtype,
                    device=device,
                )
            else:
                from transformers import Qwen3VLForConditionalGeneration

                load_started = time.perf_counter()
                model = Qwen3VLForConditionalGeneration.from_pretrained(
                    self.config.model,
                    dtype=dtype,
                    device_map={"": device},
                    low_cpu_mem_usage=True,
                    local_files_only=True,
                )
                load_stats = {
                    "mode": "full_model",
                    "attention_implementation": str(
                        getattr(
                            model.model.visual.config,
                            "_attn_implementation",
                            "",
                        )
                        or ""
                    ),
                    "parameter_bytes": int(
                        sum(
                            parameter.numel() * parameter.element_size()
                            for parameter in model.parameters()
                        )
                    ),
                    "load_ms": (time.perf_counter() - load_started) * 1000.0,
                }
            model.eval()
            self._processor = processor
            self._model = model
            self._load_stats = load_stats
            return EncoderWorker(model, processor, device=device)
        raise ValueError(f"unsupported encoder_family: {self.config.encoder_family}")

    @property
    def processor(self):
        if self._processor is None:
            raise RuntimeError("encoder is not loaded yet")
        return self._processor

    def stats(self) -> Dict[str, Any]:
        return {
            **dict(self._load_stats),
            "loaded": self._worker is not None,
            "configured_vision_only": bool(self.config.qwen3_vision_only),
        }


def create_app(
    config: Optional[EncoderServiceConfig] = None,
    *,
    encoder: Optional[Any] = None,
    direct_transfer_engine: Optional[TransferEngine] = None,
    direct_transfer_engine_factory: Optional[Callable[[], TransferEngine]] = None,
) -> FastAPI:
    """Create the online encoder FastAPI app.

    ``encoder`` is an optional dependency injection point for local tests and
    process managers that already own a loaded EncoderWorker-compatible object.
    Production code should leave it unset so the real Qwen-VL model is loaded
    lazily on the configured GPU.
    """

    config = config or EncoderServiceConfig()
    if config.enable_qwen3_preprocess_executors:
        if config.qwen3_media_workers < 1 or config.qwen3_processor_workers < 1:
            raise ValueError("Qwen3 preprocess executor workers must be positive")
        if config.qwen3_media_max_pending < config.qwen3_media_workers:
            raise ValueError(
                "Qwen3 media max pending must be at least the media worker count"
            )
        if config.qwen3_processor_max_pending < config.qwen3_processor_workers:
            raise ValueError(
                "Qwen3 processor max pending must be at least the processor worker count"
            )
    if config.qwen3_dynamic_batch_direct_credits < 0:
        raise ValueError("Qwen3 dynamic batch direct credits must be non-negative")
    dynamic_batch_policy = str(config.qwen3_dynamic_batch_policy).strip().lower()
    if dynamic_batch_policy not in {"fifo", "patch_aging"}:
        raise ValueError(
            f"unsupported Qwen3 dynamic batch policy: {config.qwen3_dynamic_batch_policy}"
        )
    if config.qwen3_dynamic_batch_reorder_window < 1:
        raise ValueError("Qwen3 dynamic batch reorder window must be positive")
    if config.qwen3_dynamic_batch_starvation_ms < 0.0:
        raise ValueError("Qwen3 dynamic batch starvation must be non-negative")
    if (
        config.enable_qwen3_dynamic_batching
        and dynamic_batch_policy == "patch_aging"
        and config.qwen3_dynamic_batch_max_size != 1
    ):
        raise ValueError("Qwen3 patch_aging policy requires max batch size 1")

    def _new_direct_transfer_engine() -> TransferEngine:
        return TransferEngine(
            protocol=config.mooncake_protocol,
            local_hostname=config.mooncake_local_hostname,
            metadata_server=config.mooncake_metadata_server,
            device_name=config.mooncake_device_name,
        )

    direct_engine_coordinator = (
        _DirectTransferEngineCoordinator(
            direct_transfer_engine_factory or _new_direct_transfer_engine,
            initial_engine=direct_transfer_engine,
            owns_initial_engine=False,
        )
        if config.publish_backend == "direct_engine"
        else None
    )

    @asynccontextmanager
    async def _lifespan(app: FastAPI):
        ticket_sweeper = None
        if config.publish_backend == "direct_engine":
            ticket_sweeper = asyncio.create_task(
                _run_direct_ticket_sweeper(),
                name="epd-direct-ticket-sweeper",
            )
        try:
            yield
        finally:
            if ticket_sweeper is not None:
                ticket_sweeper.cancel()
                await asyncio.gather(ticket_sweeper, return_exceptions=True)
            await _drain_pending_direct_bundles()
            batcher = getattr(app.state, "qwen3_dynamic_batcher", None)
            if batcher is not None:
                await batcher.close()
            for executor_name in (
                "qwen3_media_executor",
                "qwen3_processor_executor",
            ):
                executor = getattr(app.state, executor_name, None)
                if executor is not None:
                    await executor.close()
            coordinator = getattr(
                app.state,
                "direct_transfer_engine_coordinator",
                None,
            )
            if coordinator is not None:
                await asyncio.to_thread(coordinator.shutdown)

    app = FastAPI(title="Mooncake EPD Encoder Service", lifespan=_lifespan)
    app.state.config = config
    app.state.lazy_encoder = None if encoder is not None else _LazyEncoder(config)
    app.state.encoder = encoder
    app.state.direct_transfer_engine = direct_transfer_engine
    app.state.owns_direct_transfer_engine = False
    app.state.direct_transfer_engine_coordinator = direct_engine_coordinator
    app.state.pending_direct_bundles = {}
    app.state.pending_direct_created = 0
    app.state.pending_direct_published = 0
    app.state.pending_direct_publish_failures = 0
    app.state.pending_direct_publish_failure_records = 0
    app.state.pending_direct_publish_failure_bytes = 0
    app.state.pending_direct_publish_failure_visibility_sync_ms = 0.0
    app.state.pending_direct_publish_cancellations = 0
    app.state.pending_direct_discarded = 0
    app.state.pending_direct_expired = 0
    app.state.pending_direct_retained_bytes = 0
    app.state.pending_direct_high_watermark_tickets = 0
    app.state.pending_direct_high_watermark_bytes = 0
    app.state.pending_direct_capacity_rejections = 0
    app.state.pending_direct_entry_limit_rejections = 0
    app.state.pending_direct_byte_limit_rejections = 0
    app.state.pending_direct_rejected_records = 0
    app.state.pending_direct_rejected_bytes = 0
    app.state.pending_direct_rejection_visibility_sync_ms = 0.0
    app.state.predicted_descriptor_created = 0
    app.state.predicted_descriptor_published = 0
    app.state.predicted_descriptor_validation_failures = 0
    app.state.predicted_descriptor_compute_failures = 0
    app.state.predicted_descriptor_fallbacks = 0
    app.state.predicted_descriptor_deferred_cleanups = 0
    app.state.predicted_descriptor_cleanup_tasks = set()
    app.state.feature_bundle_cache = (
        FeatureStore(
            max_bytes=max(1, int(config.feature_bundle_cache_max_bytes)),
            max_entries=max(1, int(config.feature_bundle_cache_max_entries)),
            node_id="online-encoder",
            admission_policy=config.feature_bundle_cache_admission_policy,
        )
        if config.enable_feature_bundle_cache
        else None
    )
    app.state.feature_bundle_inflight = {}
    app.state.qwen3_prediction_prepare_inflight = {}
    app.state.qwen3_prediction_prepare_singleflight_waiters = 0
    app.state.feature_bundle_cache_computes = 0
    app.state.feature_bundle_cache_singleflight_waiters = 0
    app.state.qwen3_content_first_raw_sources = 0
    app.state.qwen3_content_first_raw_bytes = 0
    app.state.qwen3_content_first_image_decodes = 0
    app.state.qwen3_content_first_image_decode_ms = 0.0
    app.state.qwen3_content_first_cache_decode_skips = 0
    app.state.qwen3_content_first_singleflight_decode_skips = 0
    app.state.qwen3_media_source_loads = 0
    app.state.qwen3_media_source_failures = 0
    app.state.qwen3_media_source_ms = 0.0
    app.state.qwen3_processor_calls = 0
    app.state.qwen3_processor_failures = 0
    app.state.qwen3_processor_ms = 0.0
    app.state.qwen3_vision_only_processor_calls = 0
    app.state.qwen3_vision_only_processor_fallbacks = 0
    app.state.qwen3_processor_pool = None
    app.state.qwen3_processor_pool_lock = asyncio.Lock()
    app.state.qwen3_media_executor = (
        _BoundedThreadExecutor(
            name="qwen3-media",
            max_workers=config.qwen3_media_workers,
            max_pending=config.qwen3_media_max_pending,
            submit_timeout_s=config.request_timeout_s,
        )
        if config.enable_qwen3_preprocess_executors
        else None
    )
    app.state.qwen3_processor_executor = (
        _BoundedThreadExecutor(
            name="qwen3-processor",
            max_workers=config.qwen3_processor_workers,
            max_pending=config.qwen3_processor_max_pending,
            submit_timeout_s=config.request_timeout_s,
        )
        if config.enable_qwen3_preprocess_executors
        else None
    )
    app.state.qwen3_dynamic_batcher = (
        _Qwen3DynamicBatcher(
            max_batch_size=config.qwen3_dynamic_batch_max_size,
            max_wait_ms=config.qwen3_dynamic_batch_wait_ms,
            max_queue=config.qwen3_dynamic_batch_max_queue,
            max_patches=config.qwen3_dynamic_batch_max_patches,
            submit_timeout_s=config.request_timeout_s,
            policy=dynamic_batch_policy,
            reorder_window=config.qwen3_dynamic_batch_reorder_window,
            starvation_ms=config.qwen3_dynamic_batch_starvation_ms,
        )
        if config.enable_qwen3_dynamic_batching
        else None
    )
    app.state.qwen3_batch_admission = (
        _Qwen3BatchAdmission(
            config.qwen3_dynamic_batch_min_inflight,
            config.qwen3_dynamic_batch_direct_credits,
        )
        if config.enable_qwen3_dynamic_batching
        else None
    )
    @app.middleware("http")
    async def _track_qwen3_ingress_pressure(request, call_next):
        admission: Optional[_Qwen3BatchAdmission] = app.state.qwen3_batch_admission
        tracked = admission is not None and request.url.path in {"/describe", "/encode"}
        if tracked:
            assert admission is not None
            admission.enter()
        try:
            return await call_next(request)
        finally:
            if tracked:
                assert admission is not None
                admission.leave()

    def _publish_bundle_from_app(
        bundle: Any,
        metadata: Dict[str, Any],
        direct_target: Optional[Dict[str, Any]],
    ) -> FeatureHandle:
        if config.publish_backend != "direct_engine":
            return _publish_bundle(
                config,
                bundle,
                metadata,
                direct_target,
                app.state.direct_transfer_engine,
            )
        if not isinstance(direct_target, dict) or not direct_target:
            raise ValueError(
                "publish_backend=direct_engine requires metadata.mooncake_epd_direct_feature_targets"
            )
        remote_session = str(direct_target.get("remote_session") or "")
        if not remote_session:
            raise ValueError("direct feature target requires remote_session")
        remote_incarnation = str(
            direct_target.get("remote_incarnation")
            or direct_target.get("prefill_incarnation")
            or ""
        )
        coordinator: _DirectTransferEngineCoordinator = (
            app.state.direct_transfer_engine_coordinator
        )
        lease = coordinator.acquire(
            remote_session=remote_session,
            remote_incarnation=remote_incarnation,
        )
        try:
            return _publish_bundle(
                config,
                bundle,
                metadata,
                direct_target,
                lease.engine,
                direct_transfer_context={
                    "direct_engine_generation": lease.generation,
                    "direct_engine_recovery": lease.recovery_reason,
                    "direct_engine_recovered": False,
                    "direct_remote_incarnation": remote_incarnation,
                },
            )
        except Exception as exc:
            if not _is_recoverable_direct_transfer_failure(exc):
                raise
            failed_generation = lease.generation
            lease.release()
            try:
                lease = coordinator.recover_after_transfer_failure(
                    failed_generation=failed_generation,
                )
            except Exception:
                coordinator.record_recovery(success=False)
                raise
            try:
                handle = _publish_bundle(
                    config,
                    bundle,
                    metadata,
                    direct_target,
                    lease.engine,
                    direct_transfer_context={
                        "direct_engine_generation": lease.generation,
                        "direct_engine_recovery": lease.recovery_reason,
                        "direct_engine_recovered": True,
                        "direct_remote_incarnation": remote_incarnation,
                    },
                )
            except Exception:
                coordinator.record_recovery(success=False)
                raise
            coordinator.record_recovery(success=True)
            return handle
        finally:
            lease.release()

    @app.get("/health")
    async def health() -> Dict[str, Any]:
        await _sweep_expired_direct_bundles()
        return {
            "status": "ok",
            "model": config.model,
            "device": config.device,
            "encoder_family": config.encoder_family,
            "publish_backend": config.publish_backend,
            "http_image_urls": {
                "enabled": False,
                "legacy_enable_requested": bool(config.allow_http_image_urls),
                "reason": "pinned-IP transport is not implemented",
            },
            "omni_hidden_prefix_cache": bool(config.enable_omni_hidden_prefix_cache),
            "feature_bundle_cache": _feature_bundle_cache_stats(),
            "encoder_runtime": (
                app.state.lazy_encoder.stats()
                if app.state.lazy_encoder is not None
                else {"mode": "injected", "loaded": app.state.encoder is not None}
            ),
            "qwen3_dynamic_batcher": (
                app.state.qwen3_dynamic_batcher.stats()
                if app.state.qwen3_dynamic_batcher is not None
                else {"enabled": False}
            ),
            "qwen3_batch_admission": (
                app.state.qwen3_batch_admission.stats()
                if app.state.qwen3_batch_admission is not None
                else {"enabled": False}
            ),
            "qwen3_content_first_decode": _qwen3_content_first_decode_stats(),
            "qwen3_preprocess_pipeline": _qwen3_preprocess_pipeline_stats(),
            "qwen3_predicted_descriptor_overlap": (
                _qwen3_predicted_descriptor_stats()
            ),
            "pending_direct_bundles": _pending_direct_bundle_stats(),
            "direct_transfer_engine": (
                direct_engine_coordinator.stats()
                if direct_engine_coordinator is not None
                else {"enabled": False}
            ),
            "loaded": app.state.encoder is not None,
        }

    def _qwen3_content_first_decode_stats() -> Dict[str, Any]:
        decodes = int(app.state.qwen3_content_first_image_decodes)
        decode_ms = float(app.state.qwen3_content_first_image_decode_ms)
        return {
            "enabled": bool(config.qwen3_content_first_decode),
            "raw_sources": int(app.state.qwen3_content_first_raw_sources),
            "raw_bytes": int(app.state.qwen3_content_first_raw_bytes),
            "image_decodes": decodes,
            "image_decode_ms_total": decode_ms,
            "image_decode_ms_avg": decode_ms / decodes if decodes else 0.0,
            "cache_decode_skips": int(
                app.state.qwen3_content_first_cache_decode_skips
            ),
            "singleflight_decode_skips": int(
                app.state.qwen3_content_first_singleflight_decode_skips
            ),
        }

    def _qwen3_preprocess_pipeline_stats() -> Dict[str, Any]:
        media_loads = int(app.state.qwen3_media_source_loads)
        processor_calls = int(app.state.qwen3_processor_calls)
        media_executor = app.state.qwen3_media_executor
        processor_executor = app.state.qwen3_processor_executor
        return {
            "enabled": bool(config.enable_qwen3_preprocess_executors),
            "media_source": {
                "calls": media_loads,
                "failures": int(app.state.qwen3_media_source_failures),
                "ms_total": float(app.state.qwen3_media_source_ms),
                "ms_avg": (
                    float(app.state.qwen3_media_source_ms) / media_loads
                    if media_loads
                    else 0.0
                ),
            },
            "processor": {
                "calls": processor_calls,
                "failures": int(app.state.qwen3_processor_failures),
                "ms_total": float(app.state.qwen3_processor_ms),
                "ms_avg": (
                    float(app.state.qwen3_processor_ms) / processor_calls
                    if processor_calls
                    else 0.0
                ),
            },
            "vision_only_processor": {
                "enabled": bool(config.enable_qwen3_vision_only_processor),
                "calls": int(app.state.qwen3_vision_only_processor_calls),
                "fallbacks": int(
                    app.state.qwen3_vision_only_processor_fallbacks
                ),
            },
            "media_executor": (
                media_executor.stats()
                if media_executor is not None
                else {"enabled": False}
            ),
            "processor_executor": (
                processor_executor.stats()
                if processor_executor is not None
                else {"enabled": False}
            ),
            "processor_pool": (
                app.state.qwen3_processor_pool.stats()
                if app.state.qwen3_processor_pool is not None
                else {"enabled": False}
            ),
        }

    def _qwen3_predicted_descriptor_stats() -> Dict[str, Any]:
        pending = list(app.state.pending_direct_bundles.values())
        inflight = sum(
            1
            for entry in pending
            if isinstance(entry.get("compute_task"), asyncio.Task)
            and not entry["compute_task"].done()
        )
        predicted_pending = sum(
            1 for entry in pending if entry.get("descriptor_state") == "predicted"
        )
        return {
            "enabled": bool(config.enable_qwen3_predicted_descriptor_overlap),
            "checksum_compatible": not bool(config.checksum),
            "created": int(app.state.predicted_descriptor_created),
            "published": int(app.state.predicted_descriptor_published),
            "validation_failures": int(
                app.state.predicted_descriptor_validation_failures
            ),
            "compute_failures": int(app.state.predicted_descriptor_compute_failures),
            "fallbacks": int(app.state.predicted_descriptor_fallbacks),
            "pending": int(predicted_pending),
            "vit_inflight": int(inflight),
            "deferred_cleanups": int(
                app.state.predicted_descriptor_deferred_cleanups
            ),
            "cleanup_tasks": len(app.state.predicted_descriptor_cleanup_tasks),
            "prepare_inflight": len(
                app.state.qwen3_prediction_prepare_inflight
            ),
            "prepare_singleflight_waiters": int(
                app.state.qwen3_prediction_prepare_singleflight_waiters
            ),
        }

    def _pending_direct_bundle_stats() -> Dict[str, Any]:
        now = time.monotonic()
        entries = list(app.state.pending_direct_bundles.values())
        oldest_age_s = 0.0
        states: Dict[str, int] = {}
        for entry in entries:
            oldest_age_s = max(
                oldest_age_s,
                max(0.0, now - float(entry.get("created_at") or now)),
            )
            state = str(entry.get("state") or "ready")
            states[state] = states.get(state, 0) + 1
        return {
            "tickets": len(entries),
            "retained_bytes": int(app.state.pending_direct_retained_bytes),
            "oldest_age_s": float(oldest_age_s),
            "states": states,
            "created": int(app.state.pending_direct_created),
            "published": int(app.state.pending_direct_published),
            "publish_failures": int(app.state.pending_direct_publish_failures),
            "publish_failure_records": int(
                app.state.pending_direct_publish_failure_records
            ),
            "publish_failure_bytes": int(
                app.state.pending_direct_publish_failure_bytes
            ),
            "publish_failure_visibility_sync_ms": float(
                app.state.pending_direct_publish_failure_visibility_sync_ms
            ),
            "publish_cancellations": int(
                app.state.pending_direct_publish_cancellations
            ),
            "discarded": int(app.state.pending_direct_discarded),
            "expired": int(app.state.pending_direct_expired),
            "ttl_s": max(0.001, float(config.direct_ticket_ttl_s)),
            "max_entries": max(1, int(config.direct_ticket_max_entries)),
            "max_bytes": max(1, int(config.direct_ticket_max_bytes)),
            "high_watermark_tickets": int(
                app.state.pending_direct_high_watermark_tickets
            ),
            "high_watermark_bytes": int(
                app.state.pending_direct_high_watermark_bytes
            ),
            "capacity_rejections": int(
                app.state.pending_direct_capacity_rejections
            ),
            "entry_limit_rejections": int(
                app.state.pending_direct_entry_limit_rejections
            ),
            "byte_limit_rejections": int(
                app.state.pending_direct_byte_limit_rejections
            ),
            "rejected_records": int(app.state.pending_direct_rejected_records),
            "rejected_bytes": int(app.state.pending_direct_rejected_bytes),
            "rejection_visibility_sync_ms": float(
                app.state.pending_direct_rejection_visibility_sync_ms
            ),
        }

    def _direct_records_nbytes(records: List[Dict[str, Any]]) -> int:
        return sum(int(record["bundle"].nbytes()) for record in records)

    def _canonical_descriptor_json(payload: Dict[str, Any]) -> str:
        """Return the exact JSON wire form used by the allocation handshake."""

        return json.dumps(
            payload,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    def _register_pending_direct_bundle(
        ticket: str,
        *,
        records: Optional[List[Dict[str, Any]]],
        retained_bytes: int,
        request_metadata: Dict[str, Any],
        descriptors: List[FeatureBundleDescriptor],
        descriptor_state: str,
        compute_task: Optional[asyncio.Task] = None,
    ) -> Dict[str, Any]:
        for descriptor in descriptors:
            _canonical_descriptor_json(descriptor.to_dict())
        pending = {
            "records": records,
            "created_at": time.monotonic(),
            "last_access_at": time.monotonic(),
            "request_metadata": dict(request_metadata),
            "retained_bytes": int(retained_bytes),
            "descriptors": list(descriptors),
            "descriptor_state": str(descriptor_state),
            "state": (
                "vit_inflight"
                if compute_task is not None and not compute_task.done()
                else "descriptor_ready"
            ),
            "compute_task": compute_task,
            "lock": asyncio.Lock(),
            "cleanup_task": None,
        }
        app.state.pending_direct_bundles[ticket] = pending
        app.state.pending_direct_retained_bytes += int(retained_bytes)
        app.state.pending_direct_created += 1
        app.state.pending_direct_high_watermark_tickets = max(
            int(app.state.pending_direct_high_watermark_tickets),
            len(app.state.pending_direct_bundles),
        )
        app.state.pending_direct_high_watermark_bytes = max(
            int(app.state.pending_direct_high_watermark_bytes),
            int(app.state.pending_direct_retained_bytes),
        )
        return pending

    def _pending_direct_record_count(pending: Dict[str, Any]) -> int:
        descriptors = list(pending.get("descriptors") or [])
        if descriptors:
            return len(descriptors)
        return len(list(pending.get("records") or []))

    def _validate_predicted_direct_records(
        descriptors: List[FeatureBundleDescriptor],
        records: List[Dict[str, Any]],
    ) -> None:
        if len(descriptors) != len(records):
            raise ValueError(
                "predicted descriptor count mismatch: "
                f"predicted={len(descriptors)} actual={len(records)}"
            )
        for index, (descriptor, record) in enumerate(zip(descriptors, records)):
            bundle = record["bundle"]
            descriptor.validate_bundle(
                bundle,
                expected_model_fingerprint=descriptor.model_fingerprint,
                expected_processor_fingerprint=descriptor.processor_fingerprint,
                require_checksum=False,
            )
            predicted_grid = descriptor.metadata.get("grid_thw_values")
            if predicted_grid is not None:
                actual_grid = (
                    None
                    if bundle.grid_thw is None
                    else bundle.grid_thw.detach().to(device="cpu").tolist()
                )
                if actual_grid != predicted_grid:
                    raise ValueError(
                        f"predicted descriptor[{index}] grid_thw value mismatch"
                    )
            if int(bundle.nbytes()) != int(descriptor.nbytes):
                raise ValueError(
                    f"predicted descriptor[{index}] total nbytes mismatch: "
                    f"predicted={descriptor.nbytes} actual={bundle.nbytes()}"
                )

    def _validate_direct_publish_target(
        descriptor: FeatureBundleDescriptor,
        target: Dict[str, Any],
    ) -> None:
        if str(target.get("feature_id") or "") != descriptor.feature_id:
            raise ValueError(
                "direct target feature_id mismatch: "
                f"target={target.get('feature_id')} descriptor={descriptor.feature_id}"
            )
        raw_descriptor = target.get("descriptor")
        if not isinstance(raw_descriptor, dict):
            raise ValueError("direct target must carry its allocated descriptor")
        # Compare canonical JSON before schema coercion. The allocation service
        # necessarily turns metadata tuples into JSON arrays, but any extra,
        # missing, or type-changed wire field (including bool-vs-int) must fail
        # closed before a native transfer can begin.
        expected_wire = _canonical_descriptor_json(descriptor.to_dict())
        try:
            target_wire = _canonical_descriptor_json(raw_descriptor)
        except (TypeError, ValueError) as exc:
            raise ValueError("direct target descriptor is not canonical JSON") from exc
        if target_wire != expected_wire:
            raise ValueError("direct target descriptor differs from pending descriptor")
        remote_session = str(target.get("remote_session") or "")
        if not remote_session:
            raise ValueError("direct target requires remote_session")
        remote_pointers = target.get("remote_pointers")
        if not isinstance(remote_pointers, dict):
            raise ValueError("direct target requires remote_pointers")

        expected: Dict[str, int] = {
            "last_hidden": int(descriptor.last_hidden.nbytes),
        }
        if descriptor.grid_thw is not None:
            expected["grid_thw"] = int(descriptor.grid_thw.nbytes)
        for ordinal, (layer, spec) in enumerate(descriptor.intermediates):
            expected[f"intermediate:{int(layer)}:{ordinal}"] = int(spec.nbytes)
        expected_keys = set(expected)
        expected_keys.update(f"{name}:nbytes" for name in expected)
        actual_keys = {str(key) for key in remote_pointers}
        if actual_keys != expected_keys:
            raise ValueError(
                "direct target pointer ABI mismatch: "
                f"missing={sorted(expected_keys - actual_keys)} "
                f"unexpected={sorted(actual_keys - expected_keys)}"
            )
        for name, nbytes in expected.items():
            pointer = int(remote_pointers[name])
            capacity = int(remote_pointers[f"{name}:nbytes"])
            if pointer <= 0:
                raise ValueError(f"direct target pointer must be positive: {name}")
            if capacity != nbytes:
                raise ValueError(
                    f"direct target capacity mismatch for {name}: "
                    f"target={capacity} descriptor={nbytes}"
                )

    async def _resolve_pending_direct_records(
        pending: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        records = pending.get("records")
        if records is not None:
            return list(records)
        compute_task = pending.get("compute_task")
        if not isinstance(compute_task, asyncio.Task):
            raise RuntimeError("pending direct ticket has neither records nor compute task")
        try:
            computed_records, encode_ms_total, cache_stats = await asyncio.shield(
                compute_task
            )
        except asyncio.CancelledError:
            raise
        except _PredictedDescriptorValidationError:
            raise
        except Exception:
            app.state.predicted_descriptor_compute_failures += 1
            raise
        resolved = list(computed_records)
        # Retain the actual records before validation so fail-closed paths can
        # synchronize asynchronous CUDA producers before dropping references.
        pending["records"] = resolved
        descriptors = list(pending.get("descriptors") or [])
        try:
            _validate_predicted_direct_records(descriptors, resolved)
        except Exception as exc:
            app.state.predicted_descriptor_validation_failures += 1
            raise _PredictedDescriptorValidationError(str(exc)) from exc
        pending["encode_time_ms"] = float(encode_ms_total)
        pending["cache_stats"] = dict(cache_stats or {})
        pending["state"] = "verified"
        return resolved

    def _pop_pending_direct_bundle(ticket: str) -> Optional[Dict[str, Any]]:
        pending = app.state.pending_direct_bundles.pop(ticket, None)
        if pending is None:
            return None
        retained_bytes = int(pending.get("retained_bytes") or 0)
        app.state.pending_direct_retained_bytes = max(
            0,
            int(app.state.pending_direct_retained_bytes) - retained_bytes,
        )
        return pending

    def _direct_ticket_capacity_reasons(*, additional_bytes: int) -> Tuple[bool, bool]:
        entry_limited = (
            len(app.state.pending_direct_bundles)
            >= max(1, int(config.direct_ticket_max_entries))
        )
        byte_limited = (
            int(app.state.pending_direct_retained_bytes) + max(0, int(additional_bytes))
            > max(1, int(config.direct_ticket_max_bytes))
        )
        return entry_limited, byte_limited

    def _record_direct_ticket_rejection(
        *,
        entry_limited: bool,
        byte_limited: bool,
        records: int = 0,
        rejected_bytes: int = 0,
        visibility_sync_ms: float = 0.0,
    ) -> None:
        app.state.pending_direct_capacity_rejections += 1
        app.state.pending_direct_entry_limit_rejections += int(entry_limited)
        app.state.pending_direct_byte_limit_rejections += int(byte_limited)
        app.state.pending_direct_rejected_records += max(0, int(records))
        app.state.pending_direct_rejected_bytes += max(0, int(rejected_bytes))
        app.state.pending_direct_rejection_visibility_sync_ms += max(
            0.0,
            float(visibility_sync_ms),
        )

    def _raise_direct_ticket_capacity_rejection(
        *,
        entry_limited: bool,
        byte_limited: bool,
        rejected_bytes: int,
    ) -> None:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "pending direct-ticket capacity exhausted",
                "entry_limit_reached": bool(entry_limited),
                "byte_limit_reached": bool(byte_limited),
                "rejected_bytes": max(0, int(rejected_bytes)),
                "pending": _pending_direct_bundle_stats(),
            },
            headers={"Retry-After": "1"},
        )

    def _synchronize_feature_records(records: List[Dict[str, Any]]) -> float:
        """Wait for asynchronous CUDA producers before releasing bundle refs."""

        devices: set[torch.device] = set()
        for record in records:
            bundle = record["bundle"]
            tensors = [bundle.last_hidden]
            tensors.extend(tensor for _layer, tensor in bundle.intermediates)
            if bundle.grid_thw is not None:
                tensors.append(bundle.grid_thw)
            devices.update(
                tensor.device
                for tensor in tensors
                if isinstance(tensor, torch.Tensor) and tensor.device.type == "cuda"
            )
        started = time.perf_counter()
        for device in sorted(devices, key=str):
            torch.cuda.synchronize(device)
        return (time.perf_counter() - started) * 1000.0

    async def _finalize_abandoned_direct_bundle(
        ticket: str,
        pending: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Release a ticket only after any non-cancellable native work finishes."""

        lock: asyncio.Lock = pending["lock"]
        async with lock:
            if app.state.pending_direct_bundles.get(ticket) is not pending:
                return {
                    "released": False,
                    "records": 0,
                    "bytes": 0,
                    "visibility_sync_ms": 0.0,
                }
            records = list(pending.get("records") or [])
            compute_task = pending.get("compute_task")
            if not records and isinstance(compute_task, asyncio.Task):
                try:
                    computed_records, _encode_ms, _cache_stats = await asyncio.shield(
                        compute_task
                    )
                    records = list(computed_records)
                except asyncio.CancelledError:
                    raise
                except _PredictedDescriptorValidationError:
                    records = []
                except Exception:
                    app.state.predicted_descriptor_compute_failures += 1
                    records = []
            publish_task = pending.get("publish_task")
            if isinstance(publish_task, asyncio.Task):
                try:
                    await asyncio.shield(publish_task)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # The caller-side publish path may have been cancelled, so
                    # deferred cleanup owns consuming the native transfer error.
                    pass
            removed = _pop_pending_direct_bundle(ticket)
            if removed is None:
                return {
                    "released": False,
                    "records": 0,
                    "bytes": 0,
                    "visibility_sync_ms": 0.0,
                }
            visibility_sync_ms = 0.0
            if records:
                visibility_sync_ms = await asyncio.to_thread(
                    _synchronize_feature_records,
                    records,
                )
                records.clear()
            return {
                "released": True,
                "records": len(list(removed.get("records") or []))
                or _pending_direct_record_count(removed),
                "bytes": int(removed.get("retained_bytes") or 0),
                "visibility_sync_ms": float(visibility_sync_ms),
            }

    def _schedule_abandoned_direct_bundle_cleanup(
        ticket: str,
        pending: Dict[str, Any],
    ) -> asyncio.Task:
        existing = pending.get("cleanup_task")
        if isinstance(existing, asyncio.Task):
            return existing
        task = asyncio.create_task(
            _finalize_abandoned_direct_bundle(ticket, pending),
            name=f"epd-direct-ticket-cleanup-{ticket[:12]}",
        )
        pending["cleanup_task"] = task
        app.state.predicted_descriptor_cleanup_tasks.add(task)

        def _release_cleanup(done: asyncio.Task) -> None:
            app.state.predicted_descriptor_cleanup_tasks.discard(done)
            try:
                done.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("direct ticket deferred cleanup failed")

        task.add_done_callback(_release_cleanup)
        return task

    async def _request_direct_bundle_abandon(
        ticket: str,
        *,
        reason: str,
    ) -> Dict[str, Any]:
        pending = app.state.pending_direct_bundles.get(ticket)
        if pending is None:
            return {
                "found": False,
                "deferred": False,
                "records": 0,
                "bytes": 0,
                "visibility_sync_ms": 0.0,
            }
        current_state = str(pending.get("state") or "")
        if current_state in {"publish_wait", "transfer_inflight"} or current_state.endswith(
            "_requested"
        ):
            return {
                "found": False,
                "deferred": False,
                "records": 0,
                "bytes": 0,
                "visibility_sync_ms": 0.0,
            }
        pending["state"] = f"{reason}_requested"
        pending["last_access_at"] = time.monotonic()
        compute_task = pending.get("compute_task")
        if isinstance(compute_task, asyncio.Task) and not compute_task.done():
            app.state.predicted_descriptor_deferred_cleanups += 1
            _schedule_abandoned_direct_bundle_cleanup(ticket, pending)
            return {
                "found": True,
                "deferred": True,
                "records": 0,
                "bytes": 0,
                "visibility_sync_ms": 0.0,
            }
        result = await _finalize_abandoned_direct_bundle(ticket, pending)
        return {
            "found": bool(result["released"]),
            "deferred": False,
            "records": int(result["records"]),
            "bytes": int(result["bytes"]),
            "visibility_sync_ms": float(result["visibility_sync_ms"]),
        }

    async def _sweep_expired_direct_bundles() -> Dict[str, Any]:
        """Release abandoned direct-allocation handshakes after a bounded TTL."""

        if config.publish_backend != "direct_engine":
            return {
                "tickets": 0,
                "records": 0,
                "bytes": 0,
                "visibility_sync_ms": 0.0,
            }
        now = time.monotonic()
        ttl_s = max(0.001, float(config.direct_ticket_ttl_s))
        expired_tickets: List[str] = []
        expired_records = 0
        expired_bytes = 0
        deferred = 0
        visibility_sync_ms = 0.0
        for ticket, pending in list(app.state.pending_direct_bundles.items()):
            created_at = float(pending.get("created_at") or now)
            if now - created_at < ttl_s:
                continue
            result = await _request_direct_bundle_abandon(
                ticket,
                reason="expired",
            )
            if not result["found"]:
                continue
            expired_tickets.append(ticket)
            expired_records += int(result["records"])
            expired_bytes += int(result["bytes"])
            visibility_sync_ms += float(result["visibility_sync_ms"])
            deferred += int(bool(result["deferred"]))
        app.state.pending_direct_expired += len(expired_tickets)
        return {
            "tickets": len(expired_tickets),
            "records": int(expired_records),
            "bytes": int(expired_bytes),
            "visibility_sync_ms": float(visibility_sync_ms),
            "deferred": int(deferred),
        }

    async def _run_direct_ticket_sweeper() -> None:
        interval_s = max(
            0.01,
            min(1.0, max(0.001, float(config.direct_ticket_ttl_s)) / 4.0),
        )
        while True:
            await asyncio.sleep(interval_s)
            try:
                await _sweep_expired_direct_bundles()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("direct ticket TTL sweep failed")

    async def _drain_pending_direct_bundles() -> None:
        if config.publish_backend != "direct_engine":
            return
        cleanup_tasks: List[asyncio.Task] = []
        for ticket, pending in list(app.state.pending_direct_bundles.items()):
            pending["state"] = "shutdown_requested"
            cleanup_tasks.append(
                _schedule_abandoned_direct_bundle_cleanup(ticket, pending)
            )
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

    def _feature_bundle_outcomes(
        records: List[Dict[str, Any]],
    ) -> List[Dict[str, bool]]:
        return [
            {
                "cache_hit": bool(
                    record["metadata"].get("feature_bundle_cache_hit", False)
                ),
                "singleflight_wait": bool(
                    record["metadata"].get("feature_bundle_singleflight_wait", False)
                ),
                "cache_admitted": bool(
                    record["metadata"].get("feature_bundle_cache_admitted", False)
                ),
            }
            for record in records
        ]

    def _feature_bundle_cache_stats() -> Dict[str, Any]:
        cache = app.state.feature_bundle_cache
        stats = dict(cache.stats()) if cache is not None else {
            "entries": 0,
            "bytes": 0,
            "hits": 0,
            "misses": 0,
            "hit_rate": 0.0,
            "evictions": 0,
        }
        stats.update(
            {
                "enabled": cache is not None,
                "max_entries": max(1, int(config.feature_bundle_cache_max_entries)),
                "max_bytes": max(1, int(config.feature_bundle_cache_max_bytes)),
                "configured_admission_policy": str(
                    config.feature_bundle_cache_admission_policy
                ),
                "computes": int(app.state.feature_bundle_cache_computes),
                "singleflight_waiters": int(
                    app.state.feature_bundle_cache_singleflight_waiters
                ),
                "inflight": len(app.state.feature_bundle_inflight),
            }
        )
        return stats

    async def _run_qwen3_cpu_stage(
        executor_name: str,
        fn: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        executor = getattr(app.state, executor_name, None)
        if executor is not None:
            return await executor.run(fn, *args, **kwargs)
        return await asyncio.to_thread(fn, *args, **kwargs)

    def _qwen3_cache_key(source_digest: str) -> str:
        return hashlib.sha256(
            f"qwen3-vl-content-bundle-v2:{source_digest}".encode("utf-8")
        ).hexdigest()

    def _bind_qwen3_request_identity(
        canonical: FeatureBundle,
        source_mm_hash: str,
    ) -> FeatureBundle:
        if str(canonical.image_hash) == str(source_mm_hash):
            return canonical
        metadata = dict(canonical.metadata)
        metadata.update(
            {
                "content_cache_feature_id": str(canonical.image_hash),
                "request_identity_rebound": True,
            }
        )
        return FeatureBundle(
            image_hash=str(source_mm_hash),
            last_hidden=canonical.last_hidden,
            intermediates=list(canonical.intermediates),
            grid_thw=canonical.grid_thw,
            metadata=metadata,
        )

    async def _prepare_qwen3_inputs(
        *,
        processor: Any,
        image: Optional[Image.Image],
        image_payload: Optional[bytes],
        prompt: str,
    ) -> Dict[str, torch.Tensor]:
        resolved_image = image
        if resolved_image is None:
            if image_payload is None:
                raise RuntimeError("content-first Qwen3 encode has no image bytes")
            decode_started = time.perf_counter()
            resolved_image = await _run_qwen3_cpu_stage(
                "qwen3_media_executor",
                _decode_image_payload,
                image_payload,
            )
            app.state.qwen3_content_first_image_decodes += 1
            app.state.qwen3_content_first_image_decode_ms += (
                time.perf_counter() - decode_started
            ) * 1000.0
        processor_started = time.perf_counter()
        try:
            processor_pool = app.state.qwen3_processor_pool
            use_vision_only_processor = bool(
                config.enable_qwen3_vision_only_processor
                and callable(getattr(processor, "image_processor", None))
            )
            if (
                config.enable_qwen3_vision_only_processor
                and not use_vision_only_processor
            ):
                app.state.qwen3_vision_only_processor_fallbacks += 1
            if use_vision_only_processor:
                app.state.qwen3_vision_only_processor_calls += 1
            if processor_pool is not None:
                processor_fn = (
                    processor_pool.process_vision_only
                    if use_vision_only_processor
                    else processor_pool.process
                )
                processor_args = (
                    (resolved_image,)
                    if use_vision_only_processor
                    else (resolved_image, prompt)
                )
            else:
                processor_fn = (
                    _processor_vision_inputs
                    if use_vision_only_processor
                    else _processor_inputs
                )
                processor_args = (
                    (processor, resolved_image)
                    if use_vision_only_processor
                    else (processor, resolved_image, prompt)
                )
            inputs = await _run_qwen3_cpu_stage(
                "qwen3_processor_executor",
                processor_fn,
                *processor_args,
            )
        except Exception:
            app.state.qwen3_processor_failures += 1
            raise
        finally:
            app.state.qwen3_processor_calls += 1
            app.state.qwen3_processor_ms += (
                time.perf_counter() - processor_started
            ) * 1000.0
        if "pixel_values" not in inputs or "image_grid_thw" not in inputs:
            raise HTTPException(
                status_code=500,
                detail="processor did not produce pixel_values/image_grid_thw",
            )
        return inputs

    async def _run_qwen3_vit(
        *,
        worker: Any,
        inputs: Dict[str, torch.Tensor],
        image_id: str,
        source_digest: str,
    ) -> Any:
        batcher: Optional[_Qwen3DynamicBatcher] = app.state.qwen3_dynamic_batcher
        admission: Optional[_Qwen3BatchAdmission] = app.state.qwen3_batch_admission
        if batcher is not None and admission is not None and admission.should_batch():
            return await batcher.submit(
                worker=worker,
                pixel_values=inputs["pixel_values"],
                image_grid_thw=inputs["image_grid_thw"],
                image_id=image_id,
            )
        encode_kwargs: Dict[str, Any] = {
            "pixel_values": inputs["pixel_values"],
            "image_grid_thw": inputs["image_grid_thw"],
            "image_id": image_id,
        }
        if _accepts_keyword(worker.encode, "cache_key"):
            encode_kwargs["cache_key"] = source_digest
        return await asyncio.to_thread(worker.encode, **encode_kwargs)

    def _cache_qwen3_output(
        *,
        cache_key: str,
        source_digest: str,
        source_mm_hash: str,
        bundle: FeatureBundle,
    ) -> bool:
        cache = app.state.feature_bundle_cache
        if cache is None:
            return False
        recompute_cost = max(
            1.0,
            float(
                bundle.last_hidden.shape[0]
                if bundle.last_hidden.ndim
                else bundle.last_hidden.numel()
            ),
        )
        admitted = cache.put(
            cache_key,
            bundle,
            metadata={
                "source_digest": source_digest,
                "source_mm_hash": source_mm_hash,
                "recompute_cost_source": "visual_output_rows",
                "recompute_cost_units": recompute_cost,
            },
            recompute_cost=recompute_cost,
        )
        app.state.feature_bundle_cache_computes += 1
        return bool(admitted)

    def _install_qwen3_inflight(
        cache_key: str,
        task: asyncio.Task,
    ) -> None:
        inflight = app.state.feature_bundle_inflight
        inflight[cache_key] = task

        def _release(done: asyncio.Task, *, key: str = cache_key) -> None:
            if inflight.get(key) is done:
                inflight.pop(key, None)

        task.add_done_callback(_release)

    async def _encode_qwen3_bundle(
        *,
        worker: Any,
        processor: Any,
        image: Optional[Image.Image],
        image_payload: Optional[bytes],
        prompt: str,
        source_digest: str,
        source_mm_hash: str,
    ) -> Tuple[Any, float, bool, bool, bool]:
        """Return an exact cached Qwen3-VL feature bundle with singleflight."""

        cache = app.state.feature_bundle_cache
        cache_key = _qwen3_cache_key(source_digest)
        if cache is not None:
            cached = cache.get(cache_key)
            if cached is not None:
                if image is None:
                    app.state.qwen3_content_first_cache_decode_skips += 1
                return (
                    _bind_qwen3_request_identity(cached, source_mm_hash),
                    0.0,
                    True,
                    False,
                    False,
                )
            task = app.state.feature_bundle_inflight.get(cache_key)
            if task is not None:
                app.state.feature_bundle_cache_singleflight_waiters += 1
                if image is None:
                    app.state.qwen3_content_first_singleflight_decode_skips += 1
                canonical_bundle, _encode_ms, _admitted = await asyncio.shield(task)
                return (
                    _bind_qwen3_request_identity(canonical_bundle, source_mm_hash),
                    0.0,
                    False,
                    True,
                    False,
                )

        image_id = source_mm_hash if cache is None else source_digest

        async def _compute() -> Tuple[Any, float, bool]:
            inputs = await _prepare_qwen3_inputs(
                processor=processor,
                image=image,
                image_payload=image_payload,
                prompt=prompt,
            )
            enc_out = await _run_qwen3_vit(
                worker=worker,
                inputs=inputs,
                image_id=image_id,
                source_digest=source_digest,
            )
            admitted = _cache_qwen3_output(
                cache_key=cache_key,
                source_digest=source_digest,
                source_mm_hash=source_mm_hash,
                bundle=enc_out.bundle,
            )
            return enc_out.bundle, float(enc_out.encode_time_ms), admitted

        if cache is None:
            canonical_bundle, encode_ms, _admitted = await _compute()
            return canonical_bundle, encode_ms, False, False, False

        # Install ownership before decode/processor work so exact-content
        # singleflight waiters skip every expensive preprocessing stage.
        task = asyncio.create_task(_compute())
        _install_qwen3_inflight(cache_key, task)
        canonical_bundle, encode_ms, admitted = await asyncio.shield(task)
        return (
            _bind_qwen3_request_identity(canonical_bundle, source_mm_hash),
            encode_ms,
            False,
            False,
            bool(admitted),
        )

    def _qwen3_prediction_template(worker: Any) -> Optional[Dict[str, Any]]:
        explicit_capability = getattr(
            worker,
            "qwen3_predicted_descriptor_overlap",
            None,
        )
        if explicit_capability is False:
            return None
        predictor = getattr(worker, "predicted_descriptor", None) or getattr(
            worker,
            "predict_descriptor",
            None,
        )
        if explicit_capability is True and callable(predictor):
            return {"worker_predictor": predictor}
        model = getattr(worker, "model", None)
        model_config = getattr(model, "config", None)
        vision_config = getattr(model_config, "vision_config", None)
        visual = getattr(model, "visual", None)
        feature_model = getattr(model, "model", None)
        if visual is None and feature_model is not None:
            visual = getattr(feature_model, "visual", None)
        dtype = getattr(visual, "dtype", None)
        width = getattr(vision_config, "out_hidden_size", None)
        merge_size = getattr(vision_config, "spatial_merge_size", None)
        deepstack_indices = getattr(worker, "deepstack_indices", None)
        model_fingerprint = str(getattr(worker, "model_fingerprint", "") or "")
        processor_fingerprint = str(
            getattr(worker, "processor_fingerprint", "") or ""
        )
        if (
            vision_config is None
            or not isinstance(dtype, torch.dtype)
            or width is None
            or merge_size is None
            or deepstack_indices is None
            or not model_fingerprint
            or not processor_fingerprint
        ):
            return None
        try:
            output_width = int(width)
            spatial_merge_size = int(merge_size)
            device_type = torch.device(
                getattr(worker, "device", config.device)
            ).type
            layers = [int(layer) for layer in list(deepstack_indices)]
        except (TypeError, ValueError, RuntimeError):
            return None
        if output_width <= 0 or spatial_merge_size <= 0:
            return None
        return {
            "output_width": output_width,
            "spatial_merge_size": spatial_merge_size,
            "dtype": dtype,
            "device_type": device_type,
            "deepstack_layers": layers,
            "model_fingerprint": model_fingerprint,
            "processor_fingerprint": processor_fingerprint,
        }

    def _predict_qwen3_descriptor(
        *,
        inputs: Dict[str, torch.Tensor],
        feature_id: str,
        template: Dict[str, Any],
    ) -> FeatureBundleDescriptor:
        predictor = template.get("worker_predictor")
        if callable(predictor):
            predicted = predictor(
                pixel_values=inputs["pixel_values"],
                image_grid_thw=inputs["image_grid_thw"],
                image_id=feature_id,
            )
            descriptor = (
                predicted
                if isinstance(predicted, FeatureBundleDescriptor)
                else FeatureBundleDescriptor.from_dict(dict(predicted))
            )
            if descriptor.feature_id != str(feature_id):
                raise ValueError(
                    "worker predicted descriptor feature id mismatch: "
                    f"expected={feature_id} actual={descriptor.feature_id}"
                )
            payload = descriptor.to_dict()
            metadata = dict(payload.get("metadata") or {})
            metadata["descriptor_state"] = "predicted"
            payload["metadata"] = metadata
            return FeatureBundleDescriptor.from_dict(payload)
        image_grid_thw = inputs.get("image_grid_thw")
        if not isinstance(image_grid_thw, torch.Tensor):
            raise ValueError("Qwen3 descriptor prediction requires tensor image_grid_thw")
        grid = image_grid_thw.detach().to(device="cpu")
        if grid.ndim != 2 or int(grid.shape[1]) != 3 or int(grid.shape[0]) < 1:
            raise ValueError(
                "Qwen3 descriptor prediction requires image_grid_thw shaped [N, 3]"
            )
        merge_size = int(template["spatial_merge_size"])
        merge_area = merge_size * merge_size
        patch_counts = grid.to(dtype=torch.int64).prod(dim=-1)
        if bool(torch.any(patch_counts <= 0)) or bool(
            torch.any(torch.remainder(patch_counts, merge_area) != 0)
        ):
            raise ValueError(
                "image_grid_thw products must be positive and divisible by "
                f"spatial_merge_size^2={merge_area}"
            )
        split_sizes = [int(value) for value in (patch_counts // merge_area).tolist()]
        rows = sum(split_sizes)
        width = int(template["output_width"])
        output_dtype: torch.dtype = template["dtype"]
        dtype_name = str(output_dtype).replace("torch.", "")
        device_type = str(template["device_type"])
        hidden_nbytes = rows * width * torch.empty((), dtype=output_dtype).element_size()
        hidden_spec = TensorSpec(
            shape=(rows, width),
            dtype=dtype_name,
            device_type=device_type,
            nbytes=int(hidden_nbytes),
        )
        grid_nbytes = int(grid.nelement() * grid.element_size())
        grid_spec = TensorSpec(
            shape=tuple(int(dim) for dim in grid.shape),
            dtype=str(grid.dtype).replace("torch.", ""),
            device_type=device_type,
            nbytes=grid_nbytes,
        )
        layers = list(template["deepstack_layers"])
        total_nbytes = int(hidden_nbytes * (1 + len(layers)) + grid_nbytes)
        return FeatureBundleDescriptor(
            feature_id=str(feature_id),
            model_fingerprint=str(template["model_fingerprint"]),
            processor_fingerprint=str(template["processor_fingerprint"]),
            last_hidden=hidden_spec,
            intermediates=tuple((int(layer), hidden_spec) for layer in layers),
            grid_thw=grid_spec,
            nbytes=total_nbytes,
            metadata={
                "kind": "qwen_vl_hidden_state",
                "model_fingerprint": str(template["model_fingerprint"]),
                "processor_fingerprint": str(
                    template["processor_fingerprint"]
                ),
                "num_images": len(split_sizes),
                "split_sizes": split_sizes,
                "last_hidden_shape": [rows, width],
                "last_hidden_dtype": dtype_name,
                "deepstack_layers": layers,
                "deepstack_shapes": [[rows, width] for _ in layers],
                "grid_thw_shape": list(grid_spec.shape),
                "grid_thw_values": grid.tolist(),
                "spatial_merge_size": merge_size,
                "descriptor_state": "predicted",
            },
        )

    async def _worker_and_processor() -> Tuple[Any, Any]:
        worker = app.state.encoder
        if worker is None:
            worker = await app.state.lazy_encoder.worker()
            app.state.encoder = worker
        processor = getattr(worker, "processor", None) or getattr(app.state.lazy_encoder, "processor", None)
        if processor is None:
            raise HTTPException(status_code=500, detail="encoder worker has no processor")
        if (
            app.state.qwen3_processor_executor is not None
            and not hasattr(worker, "encode_images")
            and app.state.qwen3_processor_pool is None
        ):
            async with app.state.qwen3_processor_pool_lock:
                if app.state.qwen3_processor_pool is None:
                    try:
                        app.state.qwen3_processor_pool = await asyncio.to_thread(
                            _ExclusiveProcessorPool,
                            processor,
                            config.qwen3_processor_workers,
                        )
                    except Exception as exc:
                        raise HTTPException(
                            status_code=500,
                            detail=(
                                "failed to create exclusive Qwen3 processor replicas; "
                                "shared processor concurrency is disabled: "
                                f"{exc}"
                            ),
                        ) from exc
        return worker, processor

    async def _encode_records(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], float, Dict[str, Any]]:
        items = list(_iter_mm_image_items(payload))
        if not items:
            raise HTTPException(status_code=400, detail="no image/image_url items found")
        worker, processor = await _worker_and_processor()

        content_first = bool(config.qwen3_content_first_decode) and not hasattr(
            worker,
            "encode_images",
        )
        loaded: List[
            Tuple[Dict[str, Any], Optional[Image.Image], Optional[bytes], str, str, str]
        ] = []
        async def _load_one(
            item: Dict[str, Any],
        ) -> Tuple[
            Dict[str, Any],
            Optional[Image.Image],
            Optional[bytes],
            str,
            str,
            str,
        ]:
            if content_first:
                image_payload, content_type, source_digest = await _load_image_source(
                    app,
                    item,
                    max_bytes=int(config.max_image_bytes),
                )
                app.state.qwen3_content_first_raw_sources += 1
                app.state.qwen3_content_first_raw_bytes += len(image_payload)
                image = None
            else:
                image, content_type, source_digest = await _load_image_item(
                    app,
                    item,
                    max_bytes=int(config.max_image_bytes),
                )
                image_payload = None
            return (
                item,
                image,
                image_payload,
                content_type,
                source_digest,
                _stable_mm_hash(item),
            )

        if app.state.qwen3_media_executor is not None and len(items) > 1:
            loaded.extend(await asyncio.gather(*(_load_one(item) for item in items)))
        else:
            for item in items:
                loaded.append(await _load_one(item))

        records: List[Dict[str, Any]] = []
        encode_ms_total = 0.0
        cache_stats: Dict[str, Any] = {}
        if hasattr(worker, "encode_images"):
            prompt = _prompt_for_processor(payload)
            batch_kwargs: Dict[str, Any] = {
                "image_ids": [
                    source_mm_hash
                    for _, _, _, _, _, source_mm_hash in loaded
                ],
                "prompt": prompt,
            }
            if _accepts_keyword(worker.encode_images, "cache_keys"):
                batch_kwargs["cache_keys"] = [
                    source_digest
                    for _, _, _, _, source_digest, _ in loaded
                ]
            batch_out = await asyncio.to_thread(
                worker.encode_images,
                [image for _, image, _, _, _, _ in loaded],
                **batch_kwargs,
            )
            encode_ms_total += float(getattr(batch_out, "encode_time_ms", 0.0) or 0.0)
            cache_stats = dict(getattr(batch_out, "cache_stats", {}) or {})
            for index, (
                enc_out,
                (_, _, _, content_type, source_digest, source_mm_hash),
            ) in enumerate(zip(batch_out.outputs, loaded)):
                metadata = {
                    "source_mm_hash": source_mm_hash,
                    "source_digest": source_digest,
                    "source_content_type": content_type,
                    "source_index": index,
                    "encoder_service_model": config.model,
                    "encoder_service_device": config.device,
                    "encoder_family": config.encoder_family,
                    "encode_time_ms": enc_out.encode_time_ms,
                    "publish_backend": config.publish_backend,
                    "omni_hidden_prefix_cache": cache_stats,
                }
                records.append({"bundle": enc_out.bundle, "metadata": metadata})
        else:
            prompt = _prompt_for_processor(payload)
            for index, item in enumerate(items):
                (
                    _,
                    image,
                    image_payload,
                    content_type,
                    source_digest,
                    source_mm_hash,
                ) = loaded[index]
                (
                    bundle,
                    encode_ms,
                    cache_hit,
                    singleflight_wait,
                    cache_admitted,
                ) = await _encode_qwen3_bundle(
                    worker=worker,
                    processor=processor,
                    image=image,
                    image_payload=image_payload,
                    prompt=prompt,
                    source_digest=source_digest,
                    source_mm_hash=source_mm_hash,
                )
                encode_ms_total += encode_ms
                metadata = {
                    "source_mm_hash": source_mm_hash,
                    "source_digest": source_digest,
                    "source_content_type": content_type,
                    "source_index": index,
                    "encoder_service_model": config.model,
                    "encoder_service_device": config.device,
                    "encoder_family": config.encoder_family,
                    "encode_time_ms": encode_ms,
                    "feature_bundle_cache_hit": cache_hit,
                    "feature_bundle_singleflight_wait": singleflight_wait,
                    "feature_bundle_cache_admitted": cache_admitted,
                    "publish_backend": config.publish_backend,
                }
                records.append({"bundle": bundle, "metadata": metadata})
        return records, encode_ms_total, cache_stats

    async def _store_completed_direct_describe(
        *,
        payload: Dict[str, Any],
        records: List[Dict[str, Any]],
        encode_ms_total: float,
        cache_stats: Dict[str, Any],
        started: float,
    ) -> Dict[str, Any]:
        retained_bytes = _direct_records_nbytes(records)
        entry_limited, byte_limited = _direct_ticket_capacity_reasons(
            additional_bytes=retained_bytes
        )
        if entry_limited or byte_limited:
            visibility_sync_ms = await asyncio.to_thread(
                _synchronize_feature_records,
                records,
            )
            rejected_records = len(records)
            _record_direct_ticket_rejection(
                entry_limited=entry_limited,
                byte_limited=byte_limited,
                records=rejected_records,
                rejected_bytes=retained_bytes,
                visibility_sync_ms=visibility_sync_ms,
            )
            records.clear()
            _raise_direct_ticket_capacity_rejection(
                entry_limited=entry_limited,
                byte_limited=byte_limited,
                rejected_bytes=retained_bytes,
            )
        descriptors = [
            record["bundle"].descriptor(checksum=bool(config.checksum))
            for record in records
        ]
        ticket = uuid.uuid4().hex
        _register_pending_direct_bundle(
            ticket,
            records=records,
            retained_bytes=retained_bytes,
            request_metadata=dict(payload.get("metadata") or {}),
            descriptors=descriptors,
            descriptor_state="completed",
        )
        return {
            "ticket": ticket,
            "descriptors": [descriptor.to_dict() for descriptor in descriptors],
            "count": len(records),
            "reservation_count": len(records),
            "reservation_bytes": int(retained_bytes),
            "descriptor_state": "completed",
            "vit_inflight": False,
            "encode_time_ms": float(encode_ms_total),
            "total_time_ms": (time.perf_counter() - started) * 1000.0,
            "publish_backend": config.publish_backend,
            "encoder_family": config.encoder_family,
            "omni_hidden_prefix_cache": dict(cache_stats or {}),
            "feature_bundle_cache": _feature_bundle_cache_stats(),
            "feature_bundle_outcomes": _feature_bundle_outcomes(records),
        }

    def _qwen3_record(
        *,
        bundle: FeatureBundle,
        source_mm_hash: str,
        source_digest: str,
        content_type: str,
        encode_ms: float,
        cache_hit: bool,
        singleflight_wait: bool,
        cache_admitted: bool,
    ) -> Dict[str, Any]:
        return {
            "bundle": bundle,
            "metadata": {
                "source_mm_hash": source_mm_hash,
                "source_digest": source_digest,
                "source_content_type": content_type,
                "source_index": 0,
                "encoder_service_model": config.model,
                "encoder_service_device": config.device,
                "encoder_family": config.encoder_family,
                "encode_time_ms": float(encode_ms),
                "feature_bundle_cache_hit": bool(cache_hit),
                "feature_bundle_singleflight_wait": bool(singleflight_wait),
                "feature_bundle_cache_admitted": bool(cache_admitted),
                "publish_backend": config.publish_backend,
            },
        }

    async def _try_predicted_direct_describe(
        payload: Dict[str, Any],
        *,
        started: float,
    ) -> Optional[Dict[str, Any]]:
        if not config.enable_qwen3_predicted_descriptor_overlap:
            return None
        if config.checksum:
            app.state.predicted_descriptor_fallbacks += 1
            return None
        items = list(_iter_mm_image_items(payload))
        if len(items) != 1:
            app.state.predicted_descriptor_fallbacks += 1
            return None
        worker, processor = await _worker_and_processor()
        if hasattr(worker, "encode_images"):
            app.state.predicted_descriptor_fallbacks += 1
            return None
        template = _qwen3_prediction_template(worker)
        if template is None:
            app.state.predicted_descriptor_fallbacks += 1
            return None

        item = items[0]
        content_first = bool(config.qwen3_content_first_decode)
        if content_first:
            image_payload, content_type, source_digest = await _load_image_source(
                app,
                item,
                max_bytes=int(config.max_image_bytes),
            )
            app.state.qwen3_content_first_raw_sources += 1
            app.state.qwen3_content_first_raw_bytes += len(image_payload)
            image: Optional[Image.Image] = None
        else:
            image, content_type, source_digest = await _load_image_item(
                app,
                item,
                max_bytes=int(config.max_image_bytes),
            )
            image_payload = None
        source_mm_hash = _stable_mm_hash(item)
        cache = app.state.feature_bundle_cache
        cache_key = _qwen3_cache_key(source_digest)

        if cache is not None:
            cached = cache.get(cache_key)
            if cached is not None:
                if image is None:
                    app.state.qwen3_content_first_cache_decode_skips += 1
                record = _qwen3_record(
                    bundle=_bind_qwen3_request_identity(cached, source_mm_hash),
                    source_mm_hash=source_mm_hash,
                    source_digest=source_digest,
                    content_type=content_type,
                    encode_ms=0.0,
                    cache_hit=True,
                    singleflight_wait=False,
                    cache_admitted=False,
                )
                return await _store_completed_direct_describe(
                    payload=payload,
                    records=[record],
                    encode_ms_total=0.0,
                    cache_stats={},
                    started=started,
                )
            existing = app.state.feature_bundle_inflight.get(cache_key)
            if existing is not None:
                app.state.feature_bundle_cache_singleflight_waiters += 1
                if image is None:
                    app.state.qwen3_content_first_singleflight_decode_skips += 1
                canonical, _encode_ms, _admitted = await asyncio.shield(existing)
                record = _qwen3_record(
                    bundle=_bind_qwen3_request_identity(canonical, source_mm_hash),
                    source_mm_hash=source_mm_hash,
                    source_digest=source_digest,
                    content_type=content_type,
                    encode_ms=0.0,
                    cache_hit=False,
                    singleflight_wait=True,
                    cache_admitted=False,
                )
                return await _store_completed_direct_describe(
                    payload=payload,
                    records=[record],
                    encode_ms_total=0.0,
                    cache_stats={},
                    started=started,
                )

        async def _prepare_prediction_inputs() -> Tuple[Dict[str, torch.Tensor], float]:
            processor_started = time.perf_counter()
            prepared = await _prepare_qwen3_inputs(
                processor=processor,
                image=image,
                image_payload=image_payload,
                prompt=_prompt_for_processor(payload),
            )
            return prepared, (time.perf_counter() - processor_started) * 1000.0

        prepare_task: Optional[asyncio.Task] = None
        prepare_waited = False
        if cache is not None:
            prepare_task = app.state.qwen3_prediction_prepare_inflight.get(
                cache_key
            )
        if prepare_task is None:
            prepare_task = asyncio.create_task(
                _prepare_prediction_inputs(),
                name=f"epd-qwen3-predict-prepare-{source_digest[:12]}",
            )
            if cache is not None:
                prepare_inflight = app.state.qwen3_prediction_prepare_inflight
                prepare_inflight[cache_key] = prepare_task

                def _release_prepare(
                    done: asyncio.Task,
                    *,
                    key: str = cache_key,
                ) -> None:
                    if prepare_inflight.get(key) is done:
                        prepare_inflight.pop(key, None)

                prepare_task.add_done_callback(_release_prepare)
        else:
            prepare_waited = True
            app.state.qwen3_prediction_prepare_singleflight_waiters += 1
            if image is None:
                app.state.qwen3_content_first_singleflight_decode_skips += 1
        inputs, processor_time_ms = await asyncio.shield(prepare_task)
        descriptor = _predict_qwen3_descriptor(
            inputs=inputs,
            feature_id=source_mm_hash,
            template=template,
        )
        try:
            _canonical_descriptor_json(descriptor.to_dict())
        except (TypeError, ValueError):
            # A custom predictor may return metadata that cannot cross the JSON
            # control plane (for example NaN/Infinity). Fall back to the
            # completed path instead of issuing an unusable reservation ticket.
            app.state.predicted_descriptor_fallbacks += 1
            return None
        retained_bytes = int(descriptor.nbytes)
        entry_limited, byte_limited = _direct_ticket_capacity_reasons(
            additional_bytes=retained_bytes
        )
        if entry_limited or byte_limited:
            _record_direct_ticket_rejection(
                entry_limited=entry_limited,
                byte_limited=byte_limited,
                records=1,
                rejected_bytes=retained_bytes,
            )
            _raise_direct_ticket_capacity_rejection(
                entry_limited=entry_limited,
                byte_limited=byte_limited,
                rejected_bytes=retained_bytes,
            )

        waited = False
        compute_task = (
            app.state.feature_bundle_inflight.get(cache_key)
            if cache is not None
            else None
        )
        if compute_task is not None:
            waited = True
            app.state.feature_bundle_cache_singleflight_waiters += 1
        else:
            image_id = source_mm_hash if cache is None else source_digest

            async def _compute_bundle() -> Tuple[FeatureBundle, float, bool]:
                enc_out = await _run_qwen3_vit(
                    worker=worker,
                    inputs=inputs,
                    image_id=image_id,
                    source_digest=source_digest,
                )
                validation_record = _qwen3_record(
                    bundle=_bind_qwen3_request_identity(
                        enc_out.bundle,
                        source_mm_hash,
                    ),
                    source_mm_hash=source_mm_hash,
                    source_digest=source_digest,
                    content_type=content_type,
                    encode_ms=float(enc_out.encode_time_ms),
                    cache_hit=False,
                    singleflight_wait=False,
                    cache_admitted=False,
                )
                try:
                    _validate_predicted_direct_records(
                        [descriptor],
                        [validation_record],
                    )
                except Exception as exc:
                    app.state.predicted_descriptor_validation_failures += 1
                    await asyncio.to_thread(
                        _synchronize_feature_records,
                        [validation_record],
                    )
                    raise _PredictedDescriptorValidationError(str(exc)) from exc
                admitted = _cache_qwen3_output(
                    cache_key=cache_key,
                    source_digest=source_digest,
                    source_mm_hash=source_mm_hash,
                    bundle=enc_out.bundle,
                )
                return (
                    enc_out.bundle,
                    float(enc_out.encode_time_ms),
                    bool(admitted),
                )

            compute_task = asyncio.create_task(
                _compute_bundle(),
                name=f"epd-qwen3-vit-{source_digest[:12]}",
            )
            if cache is not None:
                _install_qwen3_inflight(cache_key, compute_task)

        async def _compute_record() -> Tuple[List[Dict[str, Any]], float, Dict[str, Any]]:
            canonical, encode_ms, admitted = await asyncio.shield(compute_task)
            record = _qwen3_record(
                bundle=_bind_qwen3_request_identity(canonical, source_mm_hash),
                source_mm_hash=source_mm_hash,
                source_digest=source_digest,
                content_type=content_type,
                encode_ms=(0.0 if waited else encode_ms),
                cache_hit=False,
                singleflight_wait=waited,
                cache_admitted=bool(admitted and not waited),
            )
            return [record], (0.0 if waited else float(encode_ms)), {}

        record_task = asyncio.create_task(
            _compute_record(),
            name=f"epd-qwen3-direct-record-{source_digest[:12]}",
        )
        ticket = uuid.uuid4().hex
        _register_pending_direct_bundle(
            ticket,
            records=None,
            retained_bytes=retained_bytes,
            request_metadata=dict(payload.get("metadata") or {}),
            descriptors=[descriptor],
            descriptor_state="predicted",
            compute_task=record_task,
        )
        app.state.predicted_descriptor_created += 1
        return {
            "ticket": ticket,
            "descriptors": [descriptor.to_dict()],
            "count": 1,
            "reservation_count": 1,
            "reservation_bytes": retained_bytes,
            "descriptor_state": "predicted",
            "checksum_policy": "deferred_disabled",
            "processor_done": True,
            "processor_time_ms": float(processor_time_ms),
            "vit_inflight": not record_task.done(),
            "encode_time_ms": 0.0,
            "encode_time_available": False,
            "total_time_ms": (time.perf_counter() - started) * 1000.0,
            "publish_backend": config.publish_backend,
            "encoder_family": config.encoder_family,
            "omni_hidden_prefix_cache": {},
            "feature_bundle_cache": _feature_bundle_cache_stats(),
            "feature_bundle_outcomes": [
                {
                    "cache_hit": False,
                    "singleflight_wait": bool(waited or prepare_waited),
                    "cache_admitted": False,
                }
            ],
        }

    @app.post("/describe")
    async def describe(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Encode once and return descriptors for Prefill direct-buffer allocation.

        For ``publish_backend=direct_engine`` the bundle is held in encoder
        memory under a short-lived ticket until ``/publish_direct`` supplies
        Prefill-owned peer-buffer targets. This avoids the bad double-encode
        control flow while still keeping Prefill as the owner of destination
        tensors.
        """

        if config.publish_backend != "direct_engine":
            raise HTTPException(
                status_code=400,
                detail="/describe requires publish_backend=direct_engine",
            )
        await _sweep_expired_direct_bundles()
        entry_limited, _byte_limited = _direct_ticket_capacity_reasons(
            additional_bytes=0
        )
        if entry_limited:
            _record_direct_ticket_rejection(
                entry_limited=True,
                byte_limited=False,
            )
            _raise_direct_ticket_capacity_rejection(
                entry_limited=True,
                byte_limited=False,
                rejected_bytes=0,
            )
        started = time.perf_counter()
        predicted_response = await _try_predicted_direct_describe(
            payload,
            started=started,
        )
        if predicted_response is not None:
            return predicted_response
        records, encode_ms_total, cache_stats = await _encode_records(payload)
        return await _store_completed_direct_describe(
            payload=payload,
            records=records,
            encode_ms_total=encode_ms_total,
            cache_stats=cache_stats,
            started=started,
        )

    @app.post("/publish_direct")
    async def publish_direct(payload: Dict[str, Any]) -> Dict[str, Any]:
        if config.publish_backend != "direct_engine":
            raise HTTPException(status_code=400, detail="/publish_direct requires publish_backend=direct_engine")
        await _sweep_expired_direct_bundles()
        ticket = str(payload.get("ticket") or "")
        pending = app.state.pending_direct_bundles.get(ticket)
        if pending is None:
            raise HTTPException(status_code=404, detail=f"unknown or already consumed direct publish ticket: {ticket}")
        direct_targets = _direct_feature_targets_for_payload(payload)
        descriptor_count = _pending_direct_record_count(pending)
        if len(direct_targets) != descriptor_count:
            app.state.pending_direct_publish_failures += 1
            cleanup = await _request_direct_bundle_abandon(
                ticket,
                reason="publish_failure",
            )
            app.state.pending_direct_publish_failure_records += descriptor_count
            app.state.pending_direct_publish_failure_bytes += int(
                pending.get("retained_bytes") or 0
            )
            app.state.pending_direct_publish_failure_visibility_sync_ms += float(
                cleanup["visibility_sync_ms"]
            )
            raise HTTPException(
                status_code=400,
                detail=(
                    "direct target count mismatch: "
                    f"targets={len(direct_targets)} descriptors={descriptor_count}"
                ),
            )
        lock: asyncio.Lock = pending["lock"]
        async with lock:
            if app.state.pending_direct_bundles.get(ticket) is not pending:
                raise HTTPException(
                    status_code=404,
                    detail=f"unknown or already consumed direct publish ticket: {ticket}",
                )
            current_state = str(pending.get("state") or "")
            if current_state.endswith("_requested"):
                raise HTTPException(
                    status_code=409,
                    detail=f"direct publish ticket is being cleaned up: {ticket}",
                )
            pending["state"] = "publish_wait"
            pending["last_access_at"] = time.monotonic()
            try:
                records = await _resolve_pending_direct_records(pending)
            except asyncio.CancelledError:
                pending["state"] = "publish_cancelled_requested"
                _schedule_abandoned_direct_bundle_cleanup(ticket, pending)
                raise
            except Exception as exc:
                failed_records = list(pending.get("records") or [])
                visibility_sync_ms = 0.0
                if failed_records:
                    visibility_sync_ms = await asyncio.to_thread(
                        _synchronize_feature_records,
                        failed_records,
                    )
                removed = _pop_pending_direct_bundle(ticket)
                app.state.pending_direct_publish_failures += 1
                app.state.pending_direct_publish_failure_records += descriptor_count
                app.state.pending_direct_publish_failure_bytes += int(
                    pending.get("retained_bytes") or 0
                )
                app.state.pending_direct_publish_failure_visibility_sync_ms += float(
                    visibility_sync_ms
                )
                failed_records.clear()
                if removed is not None:
                    removed["state"] = "failed_closed"
                raise HTTPException(
                    status_code=502,
                    detail=f"predicted descriptor compute/validation failed: {exc}",
                ) from exc
            try:
                descriptors = list(pending.get("descriptors") or [])
                for descriptor, target in zip(descriptors, direct_targets):
                    _validate_direct_publish_target(descriptor, target)
            except Exception as exc:
                visibility_sync_ms = await asyncio.to_thread(
                    _synchronize_feature_records,
                    records,
                )
                _pop_pending_direct_bundle(ticket)
                app.state.pending_direct_publish_failures += 1
                app.state.pending_direct_publish_failure_records += len(records)
                app.state.pending_direct_publish_failure_bytes += int(
                    pending.get("retained_bytes") or _direct_records_nbytes(records)
                )
                app.state.pending_direct_publish_failure_visibility_sync_ms += float(
                    visibility_sync_ms
                )
                records.clear()
                raise HTTPException(
                    status_code=409,
                    detail=f"direct publish target validation failed: {exc}",
                ) from exc

            def _publish_all() -> List[FeatureHandle]:
                published_handles: List[FeatureHandle] = []
                for index, record in enumerate(records):
                    handle = _publish_bundle_from_app(
                        record["bundle"],
                        dict(record["metadata"]),
                        direct_targets[index],
                    )
                    if pending.get("descriptor_state") == "predicted":
                        handle = replace(
                            handle,
                            descriptor=descriptors[index],
                        )
                    published_handles.append(handle)
                return published_handles

            pending["state"] = "transfer_inflight"
            publish_task = asyncio.create_task(
                asyncio.to_thread(_publish_all),
                name=f"epd-direct-publish-{ticket[:12]}",
            )
            pending["publish_task"] = publish_task
            try:
                handles = await asyncio.shield(publish_task)
            except asyncio.CancelledError:
                app.state.pending_direct_publish_cancellations += 1
                pending["state"] = "publish_cancelled_requested"
                _schedule_abandoned_direct_bundle_cleanup(ticket, pending)
                raise
            except Exception as exc:
                app.state.pending_direct_publish_failures += 1
                visibility_sync_ms = await asyncio.to_thread(
                    _synchronize_feature_records,
                    records,
                )
                app.state.pending_direct_publish_failure_records += len(records)
                app.state.pending_direct_publish_failure_bytes += int(
                    pending.get("retained_bytes") or _direct_records_nbytes(records)
                )
                app.state.pending_direct_publish_failure_visibility_sync_ms += float(
                    visibility_sync_ms
                )
                _pop_pending_direct_bundle(ticket)
                records.clear()
                raise HTTPException(status_code=502, detail=f"direct publish failed: {exc}") from exc
            removed = _pop_pending_direct_bundle(ticket)
            if removed is None:
                raise HTTPException(
                    status_code=409,
                    detail=f"direct publish ticket was concurrently consumed: {ticket}",
                )
            removed["state"] = "published"
        app.state.pending_direct_published += 1
        if pending.get("descriptor_state") == "predicted":
            app.state.predicted_descriptor_published += 1
        return {
            "handles": [handle.as_control_payload() for handle in handles],
            "count": len(handles),
            "descriptor_state": str(pending.get("descriptor_state") or "completed"),
            "encode_time_ms": float(pending.get("encode_time_ms") or 0.0),
            "publish_backend": config.publish_backend,
            "encoder_family": config.encoder_family,
            "feature_bundle_cache": _feature_bundle_cache_stats(),
        }

    @app.post("/discard_direct")
    async def discard_direct(payload: Dict[str, Any]) -> Dict[str, Any]:
        """Release described bundles that will not be published.

        This is the cancellation counterpart to ``/publish_direct``.  It lets
        clients terminate failed or benchmark-only handshakes without retaining
        large vision tensors on the Encoder GPU. Repeated cleanup is idempotent
        so timeout recovery does not turn into a second failure.
        """

        if config.publish_backend != "direct_engine":
            raise HTTPException(
                status_code=400,
                detail="/discard_direct requires publish_backend=direct_engine",
            )
        await _sweep_expired_direct_bundles()
        raw_tickets = payload.get("tickets")
        if raw_tickets is None:
            raw_tickets = [payload.get("ticket")]
        if not isinstance(raw_tickets, list):
            raise HTTPException(status_code=400, detail="tickets must be a list")
        tickets = list(dict.fromkeys(str(ticket or "") for ticket in raw_tickets))
        if not tickets or any(not ticket for ticket in tickets):
            raise HTTPException(status_code=400, detail="ticket or tickets is required")

        discarded: List[str] = []
        unknown: List[str] = []
        deferred: List[str] = []
        released_records = 0
        released_bytes = 0
        visibility_sync_ms = 0.0
        for ticket in tickets:
            result = await _request_direct_bundle_abandon(
                ticket,
                reason="discard",
            )
            if not result["found"]:
                unknown.append(ticket)
                continue
            discarded.append(ticket)
            if result["deferred"]:
                deferred.append(ticket)
            released_records += int(result["records"])
            released_bytes += int(result["bytes"])
            visibility_sync_ms += float(result["visibility_sync_ms"])
        app.state.pending_direct_discarded += len(discarded)
        return {
            "discarded": discarded,
            "unknown": unknown,
            "deferred": deferred,
            "released_records": int(released_records),
            "released_bytes": int(released_bytes),
            "visibility_sync_ms": float(visibility_sync_ms),
            "pending_direct_bundles": _pending_direct_bundle_stats(),
        }

    @app.post("/encode")
    async def encode(payload: Dict[str, Any]) -> Dict[str, Any]:
        started = time.perf_counter()
        records, encode_ms_total, cache_stats = await _encode_records(payload)
        direct_targets = _direct_feature_targets_for_payload(payload)
        handles: List[FeatureHandle] = []
        for index, record in enumerate(records):
            try:
                handle = await asyncio.to_thread(
                    _publish_bundle_from_app,
                    record["bundle"],
                    dict(record["metadata"]),
                    direct_targets[index] if index < len(direct_targets) else None,
                )
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"feature publish failed: {exc}") from exc
            handles.append(handle)

        elapsed_ms = (time.perf_counter() - started) * 1000.0
        return {
            "handles": [handle.as_control_payload() for handle in handles],
            "count": len(handles),
            "encode_time_ms": encode_ms_total,
            "total_time_ms": elapsed_ms,
            "publish_backend": config.publish_backend,
            "encoder_family": config.encoder_family,
            "omni_hidden_prefix_cache": cache_stats,
            "feature_bundle_cache": _feature_bundle_cache_stats(),
            "feature_bundle_outcomes": _feature_bundle_outcomes(records),
        }

    return app


def _torch_dtype(name: str) -> torch.dtype:
    normalized = str(name or "").lower().replace("torch.", "")
    if normalized in {"bf16", "bfloat16"}:
        return torch.bfloat16
    if normalized in {"fp16", "float16", "half"}:
        return torch.float16
    if normalized in {"fp32", "float32", "float"}:
        return torch.float32
    raise ValueError(f"unsupported dtype: {name}")


def _iter_mm_image_items(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    messages = payload.get("messages")
    if isinstance(messages, list):
        for message in messages:
            if not isinstance(message, dict):
                continue
            content = message.get("content")
            if isinstance(content, list):
                for item in content:
                    if isinstance(item, dict) and _image_url_from_item(item):
                        yield item
    prompt = payload.get("prompt")
    if isinstance(prompt, list):
        for item in prompt:
            if isinstance(item, dict) and _image_url_from_item(item):
                yield item
    images = payload.get("images")
    if isinstance(images, list):
        for image in images:
            if isinstance(image, dict):
                yield image
            elif isinstance(image, str):
                yield {"type": "image_url", "image_url": {"url": image}}


def _image_url_from_item(item: Dict[str, Any]) -> Optional[str]:
    item_type = str(item.get("type", "")).strip().lower()
    if item_type not in {"image", "image_url", "input_image"}:
        return None
    image_url = item.get("image_url")
    if isinstance(image_url, str):
        return image_url
    if isinstance(image_url, dict) and image_url.get("url"):
        return str(image_url.get("url"))
    if item.get("url"):
        return str(item.get("url"))
    return None


async def _load_image_item(app: FastAPI, item: Dict[str, Any], *, max_bytes: int) -> Tuple[Image.Image, str, str]:
    payload, content_type, source_digest = await _load_image_source(
        app,
        item,
        max_bytes=max_bytes,
    )
    executor = getattr(app.state, "qwen3_media_executor", None)
    image = (
        await executor.run(_decode_image_payload, payload)
        if executor is not None
        else _decode_image_payload(payload)
    )
    return image, content_type, source_digest


async def _load_image_source(
    app: FastAPI,
    item: Dict[str, Any],
    *,
    max_bytes: int,
) -> Tuple[bytes, str, str]:
    url = _image_url_from_item(item)
    if not url:
        raise HTTPException(status_code=400, detail="image item has no URL")
    started = time.perf_counter()
    try:
        executor = getattr(app.state, "qwen3_media_executor", None)
        if executor is not None:
            return await executor.run(
                _load_and_hash_image_source_sync,
                app.state.config,
                url,
                max_bytes,
            )
        return _load_and_hash_image_source_sync(
            app.state.config,
            url,
            max_bytes,
        )
    except Exception:
        if hasattr(app.state, "qwen3_media_source_failures"):
            app.state.qwen3_media_source_failures += 1
        raise
    finally:
        if hasattr(app.state, "qwen3_media_source_loads"):
            app.state.qwen3_media_source_loads += 1
            app.state.qwen3_media_source_ms += (
                time.perf_counter() - started
            ) * 1000.0


def _load_and_hash_image_source_sync(
    config: EncoderServiceConfig,
    url: str,
    max_bytes: int,
) -> Tuple[bytes, str, str]:
    payload, content_type = _load_url_bytes_sync(
        config,
        url,
        max_bytes=max_bytes,
    )
    return payload, content_type, hashlib.sha256(payload).hexdigest()


def _decode_image_payload(payload: bytes) -> Image.Image:
    try:
        image = Image.open(io.BytesIO(payload)).convert("RGB")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"invalid image bytes: {exc}") from exc
    return image


async def _load_url_bytes(app: FastAPI, url: str, *, max_bytes: int) -> Tuple[bytes, str]:
    executor = getattr(app.state, "qwen3_media_executor", None)
    if executor is not None:
        return await executor.run(
            _load_url_bytes_sync,
            app.state.config,
            url,
            max_bytes=max_bytes,
        )
    return _load_url_bytes_sync(
        app.state.config,
        url,
        max_bytes=max_bytes,
    )


def _load_url_bytes_sync(
    config: EncoderServiceConfig,
    url: str,
    *,
    max_bytes: int,
) -> Tuple[bytes, str]:
    if url.startswith("data:"):
        return _parse_data_url(url, max_bytes=max_bytes)
    is_http = url.startswith("http://") or url.startswith("https://")
    if not is_http:
        if url.startswith("file://"):
            parsed = urlsplit(url)
            if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
                raise HTTPException(
                    status_code=403,
                    detail="file image URL host must be empty or localhost",
                )
            try:
                raw_path = unquote(parsed.path, encoding="utf-8", errors="strict")
            except UnicodeDecodeError as exc:
                raise HTTPException(
                    status_code=400,
                    detail="file image URL path is not valid UTF-8",
                ) from exc
            if "\x00" in raw_path:
                raise HTTPException(status_code=400, detail="file image URL contains NUL")
        else:
            raw_path = url
        root_raw = str(config.local_image_root or "").strip()
        if not root_raw:
            raise HTTPException(
                status_code=403,
                detail="local image paths are disabled; configure --local-image-root",
            )
        root = Path(root_raw).expanduser().resolve(strict=True)
        path = Path(raw_path).expanduser()
        if not path.is_absolute():
            path = Path.cwd() / path
        try:
            relative_path = path.relative_to(root)
        except ValueError:
            raise HTTPException(status_code=403, detail="image path is outside allowed root")
        if not relative_path.parts or any(part in {"", ".", ".."} for part in relative_path.parts):
            raise HTTPException(status_code=403, detail="invalid local image path")
        payload = _read_regular_file_beneath(
            root,
            relative_path,
            max_bytes=max_bytes,
        )
        if len(payload) > max_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"image too large: {len(payload)} > {max_bytes}",
            )
        return payload, _guess_content_type(path)

    # A hostname allowlist plus a separate DNS preflight is still vulnerable to
    # DNS rebinding because the HTTP client resolves the hostname again while
    # connecting. Keep the legacy flags parseable for rollback compatibility,
    # but fail closed until requests use a transport that connects to a pinned,
    # prevalidated IP while preserving TLS SNI/Host semantics.
    raise HTTPException(
        status_code=403,
        detail="HTTP image URLs are disabled because pinned-IP transport is unavailable",
    )


def _read_regular_file_beneath(
    root: Path,
    relative_path: Path,
    *,
    max_bytes: int,
) -> bytes:
    """Read one regular file without following a path component after validation.

    Every component is opened relative to the already-opened parent directory
    with ``O_NOFOLLOW``. The bytes are then read from the validated final file
    descriptor, eliminating the resolve/stat/reopen race that could otherwise
    swap an allowed file for a symlink outside ``local_image_root``.
    """

    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    file_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
    descriptors: List[int] = []
    try:
        current_fd = os.open(root, directory_flags)
        descriptors.append(current_fd)
        for component in relative_path.parts[:-1]:
            current_fd = os.open(component, directory_flags, dir_fd=current_fd)
            descriptors.append(current_fd)
        file_fd = os.open(relative_path.parts[-1], file_flags, dir_fd=current_fd)
        descriptors.append(file_fd)
        file_stat = os.fstat(file_fd)
        if not stat.S_ISREG(file_stat.st_mode):
            raise HTTPException(status_code=403, detail="local image is not a regular file")
        if int(file_stat.st_size) > max_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"image too large: {file_stat.st_size} > {max_bytes}",
            )
        chunks: List[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            chunk = os.read(file_fd, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)
    except HTTPException:
        raise
    except OSError as exc:
        raise HTTPException(
            status_code=403,
            detail=f"local image path is not safely readable: {exc.strerror or exc}",
        ) from exc
    finally:
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError:
                pass


def _parse_data_url(url: str, *, max_bytes: int) -> Tuple[bytes, str]:
    header, sep, data = url.partition(",")
    if sep != "," or not header.startswith("data:"):
        raise HTTPException(status_code=400, detail="invalid data URL")
    meta = header[5:]
    parts = [part for part in meta.split(";") if part]
    content_type = parts[0] if parts and "/" in parts[0] else "application/octet-stream"
    if "base64" in parts:
        if len(data) > ((max_bytes + 2) // 3) * 4 + 8:
            raise HTTPException(status_code=413, detail=f"image too large: > {max_bytes}")
        payload = base64.b64decode(data, validate=True)
    else:
        from urllib.parse import unquote_to_bytes
        payload = unquote_to_bytes(data)
    if len(payload) > max_bytes:
        raise HTTPException(status_code=413, detail=f"image too large: {len(payload)} > {max_bytes}")
    return payload, content_type


def _guess_content_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if suffix == ".png":
        return "image/png"
    if suffix == ".webp":
        return "image/webp"
    return "application/octet-stream"


def _prompt_for_processor(payload: Dict[str, Any]) -> str:
    pieces: List[str] = []
    for message in payload.get("messages") or []:
        if not isinstance(message, dict):
            continue
        content = message.get("content")
        if isinstance(content, str):
            pieces.append(content)
        elif isinstance(content, list):
            for item in content:
                if isinstance(item, dict) and str(item.get("type", "text")) in {"text", "input_text"}:
                    pieces.append(str(item.get("text") or item.get("content") or ""))
    if not pieces and isinstance(payload.get("prompt"), str):
        pieces.append(str(payload["prompt"]))
    return " ".join(part for part in pieces if part).strip() or "Describe the image."


def _processor_inputs(processor: Any, image: Image.Image, prompt: str) -> Dict[str, torch.Tensor]:
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "image", "image": image},
                {"type": "text", "text": prompt},
            ],
        }
    ]
    return processor.apply_chat_template(
        messages,
        tokenize=True,
        add_generation_prompt=True,
        return_dict=True,
        return_tensors="pt",
    )


def _processor_vision_inputs(
    processor: Any,
    image: Image.Image,
) -> Dict[str, torch.Tensor]:
    """Run only the image transform required by the disaggregated E stage.

    Encoder service consumers use only ``pixel_values`` and
    ``image_grid_thw``. Tokenization and chat-template tensor construction
    belong to Prefill, so repeating them in E is redundant work.
    """

    image_processor = getattr(processor, "image_processor", None)
    if not callable(image_processor):
        raise TypeError("Qwen3 processor has no callable image_processor")
    return image_processor(images=[image], return_tensors="pt")


def _stable_mm_hash(item: Dict[str, Any]) -> str:
    return stable_multimodal_identity_hash(item)


def _direct_feature_targets_for_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    metadata = dict(payload.get("metadata") or {})
    raw = (
        metadata.get("mooncake_epd_direct_feature_targets")
        or metadata.get("direct_feature_targets")
        or payload.get("mooncake_epd_direct_feature_targets")
        or payload.get("direct_feature_targets")
        or []
    )
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise HTTPException(status_code=400, detail="direct feature targets must be a list")
    return [dict(item or {}) for item in raw]


def _publish_bundle(
    config: EncoderServiceConfig,
    bundle,
    metadata: Dict[str, Any],
    direct_target: Optional[Dict[str, Any]] = None,
    direct_transfer_engine: Optional[TransferEngine] = None,
    direct_transfer_context: Optional[Dict[str, Any]] = None,
) -> FeatureHandle:
    if config.publish_backend == "file":
        return publish_feature_bundle_to_dir(
            bundle,
            config.store_dir,
            checksum=bool(config.checksum),
            metadata=metadata,
        )
    if config.publish_backend == "mooncake":
        store = MooncakeFeatureBundleStore(
            MooncakeFeatureBundleStoreConfig(
                store_id=config.mooncake_store_id,
                store_url=config.mooncake_store_url,
                config_path=config.mooncake_config,
                timeout_s=float(config.mooncake_timeout_s),
            )
        )
        try:
            return store.publish_bundle(
                bundle,
                checksum=bool(config.checksum),
                metadata=metadata,
            )
        finally:
            store.close()
    if config.publish_backend == "direct_engine":
        if not isinstance(direct_target, dict) or not direct_target:
            raise ValueError(
                "publish_backend=direct_engine requires metadata.mooncake_epd_direct_feature_targets"
            )
        remote_session = str(direct_target.get("remote_session") or "")
        remote_pointers = direct_target.get("remote_pointers")
        if not isinstance(remote_pointers, dict):
            raise ValueError("direct feature target requires remote_pointers dict")
        engine = direct_transfer_engine or TransferEngine(
            protocol=config.mooncake_protocol,
            local_hostname=config.mooncake_local_hostname,
            metadata_server=config.mooncake_metadata_server,
            device_name=config.mooncake_device_name,
        )
        plan = engine.build_feature_bundle_peer_buffer_plan(
            bundle,
            remote_session=remote_session,
            remote_pointers={str(k): int(v) for k, v in remote_pointers.items()},
            checksum=bool(config.checksum),
        )
        result = engine.transfer_feature_bundle_peer_buffer_plan(
            bundle,
            plan,
            source_memory_mode=config.direct_source_mode,
        )
        descriptor = bundle.descriptor(checksum=bool(config.checksum))
        md = dict(metadata)
        md.update(
            {
                "backend": "direct_engine",
                "direct_backend": result.backend_label,
                "direct_remote_session": remote_session,
                "direct_tensor_count": result.tensor_count,
                "direct_descriptor_count": result.descriptor_count,
                "direct_bytes": result.nbytes,
                "direct_plan": {
                    "feature_id": plan.feature_id,
                    "targets": [
                        {
                            "name": target.name,
                            "remote_pointer": target.remote_pointer,
                            "nbytes": target.nbytes,
                        }
                        for target in plan.targets
                    ],
                },
            }
        )
        md.update(dict(direct_transfer_context or {}))
        return FeatureHandle(
            handle_id=f"direct-{bundle.image_hash}-{int(time.time() * 1_000_000)}",
            feature_id=str(bundle.image_hash),
            store_id=config.mooncake_store_id,
            uri=f"epd-direct://{config.mooncake_store_id}/{bundle.image_hash}",
            descriptor=descriptor,
            metadata=md,
        )
    raise ValueError(f"unsupported publish backend: {config.publish_backend}")


def parse_args() -> EncoderServiceConfig:
    ap = argparse.ArgumentParser(description="Mooncake EPD online encoder service")
    ap.add_argument("--model", default=os.getenv("MOONCAKE_EPD_ENCODER_MODEL", EncoderServiceConfig.model))
    ap.add_argument("--device", default=os.getenv("MOONCAKE_EPD_ENCODER_DEVICE", EncoderServiceConfig.device))
    ap.add_argument("--dtype", default=os.getenv("MOONCAKE_EPD_ENCODER_DTYPE", EncoderServiceConfig.dtype))
    ap.add_argument(
        "--encoder-family",
        choices=["auto", "qwen3_vl", "qwen2_5_omni"],
        default=os.getenv("MOONCAKE_EPD_ENCODER_FAMILY", "auto"),
    )
    ap.add_argument("--publish-backend", choices=["file", "mooncake", "direct_engine"], default=os.getenv("MOONCAKE_EPD_ENCODER_PUBLISH_BACKEND", "file"))
    ap.add_argument("--mooncake-local-hostname", default=os.getenv("MOONCAKE_LOCAL_HOSTNAME", "localhost"))
    ap.add_argument("--mooncake-metadata-server", default=os.getenv("MOONCAKE_TE_META_DATA_SERVER", "P2PHANDSHAKE"))
    ap.add_argument("--mooncake-device-name", default=os.getenv("MOONCAKE_DEVICE_NAME", ""))
    ap.add_argument("--direct-source-mode", choices=["registered_tensor", "managed_buffer"], default=os.getenv("MOONCAKE_EPD_DIRECT_SOURCE_MODE", "registered_tensor"))
    ap.add_argument("--store-dir", default=os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_STORE_DIR", EncoderServiceConfig.store_dir))
    ap.add_argument("--mooncake-store-url", default=os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_STORE_URL"))
    ap.add_argument("--mooncake-store-id", default=os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_STORE_ID", EncoderServiceConfig.mooncake_store_id))
    ap.add_argument("--mooncake-config", default=os.getenv("MOONCAKE_CONFIG_PATH"))
    ap.add_argument("--mooncake-timeout-s", type=float, default=float(os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_STORE_TIMEOUT_S", "30")))
    ap.add_argument("--mooncake-protocol", default=os.getenv("MOONCAKE_PROTOCOL", EncoderServiceConfig.mooncake_protocol))
    ap.add_argument("--checksum", action=argparse.BooleanOptionalAction, default=os.getenv("MOONCAKE_EPD_FEATURE_HANDLE_CHECKSUM", "0") in {"1", "true", "TRUE"})
    ap.add_argument("--max-image-bytes", type=int, default=int(os.getenv("MOONCAKE_EPD_ENCODER_MAX_IMAGE_BYTES", str(32 * 1024 * 1024))))
    ap.add_argument(
        "--local-image-root",
        default=os.getenv("MOONCAKE_EPD_ENCODER_LOCAL_IMAGE_ROOT"),
        help="Optional root for local/file image URLs; disabled when unset.",
    )
    ap.add_argument(
        "--allow-http-image-urls",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_ENCODER_ALLOW_HTTP_IMAGE_URLS", "0").lower()
        in {"1", "true", "yes", "on"},
        help=(
            "Legacy compatibility flag; HTTP fetching remains fail-closed until "
            "a pinned-IP transport is implemented."
        ),
    )
    ap.add_argument(
        "--http-image-host-allowlist",
        default=os.getenv("MOONCAKE_EPD_ENCODER_HTTP_IMAGE_HOST_ALLOWLIST", ""),
        help="Reserved for a future pinned-IP HTTP image transport.",
    )
    ap.add_argument("--request-timeout-s", type=float, default=float(os.getenv("MOONCAKE_EPD_ENCODER_REQUEST_TIMEOUT_S", "15")))
    ap.add_argument(
        "--direct-ticket-ttl-s",
        type=float,
        default=float(os.getenv("MOONCAKE_EPD_DIRECT_TICKET_TTL_S", "60")),
        help="TTL for unpublished /describe direct-transfer tickets.",
    )
    ap.add_argument(
        "--direct-ticket-max-entries",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_DIRECT_TICKET_MAX_ENTRIES", "32")),
        help="Hard cap on simultaneously retained /describe tickets.",
    )
    ap.add_argument(
        "--direct-ticket-max-bytes",
        type=int,
        default=int(
            os.getenv(
                "MOONCAKE_EPD_DIRECT_TICKET_MAX_BYTES",
                str(2 * 1024 * 1024 * 1024),
            )
        ),
        help="Hard cap on logical FeatureBundle bytes retained by /describe tickets.",
    )
    ap.add_argument(
        "--enable-omni-hidden-prefix-cache",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_OMNI_HIDDEN_PREFIX_CACHE", "1").lower()
        not in {"0", "false", "no", "off"},
    )
    ap.add_argument(
        "--omni-hidden-prefix-cache-metrics",
        default=os.getenv("MOONCAKE_EPD_OMNI_HIDDEN_PREFIX_CACHE_METRICS"),
    )
    ap.add_argument(
        "--omni-allow-partial-prefix",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_OMNI_HIDDEN_PREFIX_CACHE_ALLOW_PARTIAL", "0").lower()
        in {"1", "true", "yes", "on"},
    )
    ap.add_argument(
        "--enable-feature-bundle-cache",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_ENCODER_FEATURE_BUNDLE_CACHE", "1").lower()
        not in {"0", "false", "no", "off"},
    )
    ap.add_argument(
        "--feature-bundle-cache-max-entries",
        type=int,
        default=int(
            os.getenv("MOONCAKE_EPD_ENCODER_FEATURE_BUNDLE_CACHE_MAX_ENTRIES", "32")
        ),
    )
    ap.add_argument(
        "--feature-bundle-cache-max-bytes",
        type=int,
        default=int(
            os.getenv(
                "MOONCAKE_EPD_ENCODER_FEATURE_BUNDLE_CACHE_MAX_BYTES",
                str(2 * 1024 * 1024 * 1024),
            )
        ),
    )
    ap.add_argument(
        "--feature-bundle-cache-admission-policy",
        choices=["lru", "reuse_density"],
        default=os.getenv(
            "MOONCAKE_EPD_ENCODER_FEATURE_BUNDLE_CACHE_ADMISSION_POLICY",
            "lru",
        ),
    )
    ap.add_argument(
        "--qwen3-content-first-decode",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_QWEN3_CONTENT_FIRST_DECODE", "0").lower()
        in {"1", "true", "yes", "on"},
        help=(
            "Hash raw Qwen3 image bytes before PIL decode so exact cache hits "
            "and singleflight waiters skip redundant decode work."
        ),
    )
    ap.add_argument(
        "--enable-qwen3-preprocess-executors",
        action=argparse.BooleanOptionalAction,
        default=os.getenv(
            "MOONCAKE_EPD_QWEN3_PREPROCESS_EXECUTORS",
            "0",
        ).lower()
        in {"1", "true", "yes", "on"},
        help=(
            "Isolate media I/O/PIL and HF processor work in dedicated bounded "
            "thread pools instead of the shared asyncio default executor."
        ),
    )
    ap.add_argument(
        "--qwen3-media-workers",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_QWEN3_MEDIA_WORKERS", "4")),
    )
    ap.add_argument(
        "--qwen3-media-max-pending",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_QWEN3_MEDIA_MAX_PENDING", "16")),
    )
    ap.add_argument(
        "--qwen3-processor-workers",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_QWEN3_PROCESSOR_WORKERS", "4")),
    )
    ap.add_argument(
        "--qwen3-processor-max-pending",
        type=int,
        default=int(
            os.getenv("MOONCAKE_EPD_QWEN3_PROCESSOR_MAX_PENDING", "16")
        ),
    )
    ap.add_argument(
        "--enable-qwen3-vision-only-processor",
        action=argparse.BooleanOptionalAction,
        default=os.getenv(
            "MOONCAKE_EPD_QWEN3_VISION_ONLY_PROCESSOR",
            "0",
        ).lower()
        in {"1", "true", "yes", "on"},
        help=(
            "Use only the HF image processor in the disaggregated Qwen3 E "
            "stage, avoiding redundant chat-template tokenization whose "
            "outputs are never consumed by ViT."
        ),
    )
    ap.add_argument(
        "--enable-qwen3-predicted-descriptor-overlap",
        action=argparse.BooleanOptionalAction,
        default=os.getenv(
            "MOONCAKE_EPD_QWEN3_PREDICTED_DESCRIPTOR_OVERLAP",
            "0",
        ).lower()
        in {"1", "true", "yes", "on"},
        help=(
            "Return exact predicted Qwen3 FeatureBundle descriptors after HF "
            "processing while ViT compute continues, allowing Prefill direct "
            "buffer allocation to overlap with encoder GPU work."
        ),
    )
    ap.add_argument(
        "--enable-qwen3-dynamic-batching",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_QWEN3_DYNAMIC_BATCHING", "0").lower()
        in {"1", "true", "yes", "on"},
    )
    ap.add_argument(
        "--qwen3-vision-only",
        action=argparse.BooleanOptionalAction,
        default=os.getenv("MOONCAKE_EPD_QWEN3_VISION_ONLY", "0").lower()
        in {"1", "true", "yes", "on"},
    )
    ap.add_argument(
        "--qwen3-dynamic-batch-max-size",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_QWEN3_DYNAMIC_BATCH_MAX_SIZE", "4")),
    )
    ap.add_argument(
        "--qwen3-dynamic-batch-wait-ms",
        type=float,
        default=float(os.getenv("MOONCAKE_EPD_QWEN3_DYNAMIC_BATCH_WAIT_MS", "2.0")),
    )
    ap.add_argument(
        "--qwen3-dynamic-batch-min-inflight",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_QWEN3_DYNAMIC_BATCH_MIN_INFLIGHT", "2")),
    )
    ap.add_argument(
        "--qwen3-dynamic-batch-direct-credits",
        type=int,
        default=int(
            os.getenv("MOONCAKE_EPD_QWEN3_DYNAMIC_BATCH_DIRECT_CREDITS", "0")
        ),
        help=(
            "Allow this many direct ViT launches after each pressure burst "
            "activates before routing remaining work through the bounded "
            "dynamic-batch dispatcher."
        ),
    )
    ap.add_argument(
        "--qwen3-dynamic-batch-max-queue",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_QWEN3_DYNAMIC_BATCH_MAX_QUEUE", "64")),
    )
    ap.add_argument(
        "--qwen3-dynamic-batch-max-patches",
        type=int,
        default=int(os.getenv("MOONCAKE_EPD_QWEN3_DYNAMIC_BATCH_MAX_PATCHES", "65536")),
    )
    ap.add_argument(
        "--qwen3-dynamic-batch-policy",
        choices=["fifo", "patch_aging"],
        default=os.getenv("MOONCAKE_EPD_QWEN3_DYNAMIC_BATCH_POLICY", "fifo"),
        help=(
            "Select FIFO or bounded patch-count/aging scheduling. patch_aging "
            "is fail-closed unless max batch size is 1."
        ),
    )
    ap.add_argument(
        "--qwen3-dynamic-batch-reorder-window",
        type=int,
        default=int(
            os.getenv("MOONCAKE_EPD_QWEN3_DYNAMIC_BATCH_REORDER_WINDOW", "8")
        ),
    )
    ap.add_argument(
        "--qwen3-dynamic-batch-starvation-ms",
        type=float,
        default=float(
            os.getenv("MOONCAKE_EPD_QWEN3_DYNAMIC_BATCH_STARVATION_MS", "2000")
        ),
    )
    args = ap.parse_args()
    return EncoderServiceConfig(**vars(args))


def main() -> None:
    import uvicorn

    config = parse_args()
    host = os.getenv("MOONCAKE_EPD_ENCODER_HOST", "127.0.0.1")
    port = int(os.getenv("MOONCAKE_EPD_ENCODER_PORT", "8300"))
    uvicorn.run(create_app(config), host=host, port=port)


if __name__ == "__main__":
    main()
