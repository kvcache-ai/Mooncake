from __future__ import annotations

import asyncio
import base64
import io
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
import torch
from fastapi import Request
from fastapi.testclient import TestClient
from PIL import Image

from mooncake_epd.core.state import (
    FeatureBundle,
    FeatureHandleError,
    FeatureHandleProvider,
    FeatureHandleProviderConfig,
    MooncakeKVStateStore,
    MooncakeRemoteKVMaterializer,
    PagedKVManager,
)
from mooncake_epd.core.transfer import TransferEngine
from mooncake_epd.scripts.epd_encoder_service import (
    _BoundedThreadExecutor,
    EncoderServiceConfig,
    _DirectTransferEngineCoordinator,
    _Qwen3BatchAdmission,
    _Qwen3DynamicBatcher,
    create_app as create_encoder_app,
)
from mooncake_epd.scripts.vllm_disagg_proxy import ProxyConfig, create_app as create_proxy_app
from mooncake_epd.tests.test_vllm_disagg_proxy_semantics import (
    _build_decode_app,
    _build_prefill_app,
    _client_override,
)


def _png_data_url(color=(32, 64, 128)) -> str:
    image = Image.new("RGB", (4, 4), color=color)
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("ascii")


class _DummyProcessor:
    def apply_chat_template(self, *args, **kwargs):
        return {
            "pixel_values": torch.arange(3 * 4, dtype=torch.float32).reshape(3, 4),
            "image_grid_thw": torch.tensor([[1, 2, 2]], dtype=torch.long),
        }


class _DummyEncoder:
    processor = _DummyProcessor()

    def __init__(self):
        self.encode_calls = 0

    def encode(self, *, pixel_values, image_grid_thw, image_id=None):
        self.encode_calls += 1
        class Out:
            pass

        out = Out()
        out.encode_time_ms = 1.25
        out.bundle = FeatureBundle(
            image_hash=image_id or "img",
            last_hidden=torch.ones((4, 8), dtype=torch.float32),
            intermediates=[(1, torch.full((4, 8), 2.0, dtype=torch.float32))],
            grid_thw=image_grid_thw.detach().cpu(),
            metadata={"kind": "dummy_qwen_vl_hidden_state"},
        )
        return out


class _BatchDummyEncoder(_DummyEncoder):
    def __init__(self):
        super().__init__()
        self.batch_calls = []

    def encode_many(self, items):
        self.batch_calls.append(len(items))
        return [
            self.encode(
                pixel_values=pixel_values,
                image_grid_thw=image_grid_thw,
                image_id=image_id,
            )
            for pixel_values, image_grid_thw, image_id in items
        ]


def test_bounded_thread_executor_caps_submission_and_reports_queueing():
    running = 0
    max_running = 0
    lock = threading.Lock()

    def _work(value):
        nonlocal running, max_running
        with lock:
            running += 1
            max_running = max(max_running, running)
        try:
            time.sleep(0.04)
            return value * 2
        finally:
            with lock:
                running -= 1

    async def _run():
        executor = _BoundedThreadExecutor(
            name="test-stage",
            max_workers=2,
            max_pending=3,
            submit_timeout_s=1.0,
        )
        try:
            results = await asyncio.gather(
                *(executor.run(_work, value) for value in range(6))
            )
            stats = executor.stats()
        finally:
            await executor.close()
        return results, stats

    results, stats = asyncio.run(_run())
    assert results == [value * 2 for value in range(6)]
    assert max_running == 2
    assert stats["submitted"] == 6
    assert stats["completed"] == 6
    assert stats["failures"] == 0
    assert stats["peak_inflight"] == 3
    assert stats["queue_waits"] >= 1
    assert stats["inflight"] == 0


def test_bounded_thread_executor_keeps_cancelled_slot_until_native_work_finishes():
    started = threading.Event()
    release = threading.Event()

    def _blocking():
        started.set()
        release.wait(timeout=2.0)
        return "done"

    async def _run():
        executor = _BoundedThreadExecutor(
            name="cancel-stage",
            max_workers=1,
            max_pending=1,
            submit_timeout_s=1.0,
        )
        task = asyncio.create_task(executor.run(_blocking))
        await asyncio.to_thread(started.wait, 1.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert executor.stats()["inflight"] == 1
        release.set()
        for _ in range(100):
            if executor.stats()["inflight"] == 0:
                break
            await asyncio.sleep(0.01)
        stats = executor.stats()
        await executor.close()
        return stats

    stats = asyncio.run(_run())
    assert stats["completed"] == 1
    assert stats["caller_cancellations"] == 1
    assert stats["inflight"] == 0


def test_qwen3_dynamic_batcher_coalesces_requests_and_preserves_identity():
    class _BatchEncoder:
        def __init__(self):
            self.calls = []

        def encode_many(self, items):
            self.calls.append([item[2] for item in items])
            outputs = []
            for pixel_values, image_grid_thw, image_id in items:
                del pixel_values

                class Out:
                    pass

                out = Out()
                out.image_id = image_id
                out.encode_time_ms = 4.0
                out.bundle = FeatureBundle(
                    image_hash=image_id,
                    last_hidden=torch.ones((4, 8), dtype=torch.float32),
                    intermediates=[],
                    grid_thw=image_grid_thw,
                )
                outputs.append(out)
            return outputs

    async def _run():
        encoder = _BatchEncoder()
        batcher = _Qwen3DynamicBatcher(
            max_batch_size=4,
            max_wait_ms=20.0,
            max_queue=8,
            max_patches=64,
            submit_timeout_s=1.0,
        )
        try:
            outputs = await asyncio.gather(
                *[
                    batcher.submit(
                        worker=encoder,
                        pixel_values=torch.ones((4, 8), dtype=torch.float32),
                        image_grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
                        image_id=f"image-{index}",
                    )
                    for index in range(4)
                ]
            )
            stats = batcher.stats()
        finally:
            await batcher.close()
        return encoder, outputs, stats

    encoder, outputs, stats = asyncio.run(_run())

    assert encoder.calls == [["image-0", "image-1", "image-2", "image-3"]]
    assert [output.image_id for output in outputs] == [
        "image-0",
        "image-1",
        "image-2",
        "image-3",
    ]
    assert stats["batches"] == 1
    assert stats["multi_item_batches"] == 1
    assert stats["max_observed_batch_size"] == 4
    assert stats["completed"] == 4


def test_qwen3_batch_admission_spends_direct_credits_once_per_burst():
    admission = _Qwen3BatchAdmission(
        min_inflight=2,
        direct_credits_per_burst=3,
    )
    admission.enter()
    assert admission.should_batch() is False
    admission.enter()

    assert [admission.should_batch() for _ in range(4)] == [
        False,
        False,
        False,
        True,
    ]
    stats = admission.stats()
    assert stats["burst_activations"] == 1
    assert stats["direct_credits_per_burst"] == 3
    assert stats["direct_credits_remaining"] == 0
    assert stats["direct_credit_decisions"] == 3
    assert stats["direct_decisions"] == 4
    assert stats["batched_decisions"] == 1

    admission.leave()
    admission.leave()
    assert admission.stats()["direct_credits_remaining"] == 0

    admission.enter()
    admission.enter()
    assert admission.should_batch() is False
    assert admission.stats()["direct_credits_remaining"] == 2
    admission.leave()
    admission.leave()


def test_qwen3_batch_admission_rejects_negative_direct_credits():
    with pytest.raises(ValueError, match="direct credits must be non-negative"):
        create_encoder_app(
            EncoderServiceConfig(qwen3_dynamic_batch_direct_credits=-1),
            encoder=_BatchDummyEncoder(),
        )


def test_qwen3_dynamic_batcher_respects_patch_budget_without_dropping_work():
    class _BatchEncoder:
        def __init__(self):
            self.batch_sizes = []

        def encode_many(self, items):
            self.batch_sizes.append(len(items))
            outputs = []
            for _pixel_values, image_grid_thw, image_id in items:
                class Out:
                    pass

                out = Out()
                out.image_id = image_id
                out.encode_time_ms = 1.0
                out.bundle = FeatureBundle(
                    image_hash=image_id,
                    last_hidden=torch.ones((1, 2)),
                    grid_thw=image_grid_thw,
                )
                outputs.append(out)
            return outputs

    async def _run():
        encoder = _BatchEncoder()
        batcher = _Qwen3DynamicBatcher(
            max_batch_size=4,
            max_wait_ms=10.0,
            max_queue=8,
            max_patches=6,
            submit_timeout_s=1.0,
        )
        try:
            outputs = await asyncio.gather(
                *[
                    batcher.submit(
                        worker=encoder,
                        pixel_values=torch.ones((4, 8)),
                        image_grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
                        image_id=f"budget-{index}",
                    )
                    for index in range(3)
                ]
            )
            stats = batcher.stats()
        finally:
            await batcher.close()
        return encoder, outputs, stats

    encoder, outputs, stats = asyncio.run(_run())

    assert encoder.batch_sizes == [1, 1, 1]
    assert len(outputs) == 3
    assert stats["completed"] == 3
    assert stats["max_observed_batch_patches"] == 4


def test_qwen3_dynamic_batcher_skips_cancelled_work_before_native_encode():
    started = threading.Event()
    release = threading.Event()

    class _BlockingEncoder(_BatchDummyEncoder):
        def encode_many(self, items):
            started.set()
            release.wait(timeout=2.0)
            return super().encode_many(items)

    async def _run():
        encoder = _BlockingEncoder()
        batcher = _Qwen3DynamicBatcher(
            max_batch_size=1,
            max_wait_ms=0.0,
            max_queue=8,
            max_patches=64,
            submit_timeout_s=1.0,
        )
        first = asyncio.create_task(
            batcher.submit(
                worker=encoder,
                pixel_values=torch.ones((4, 8)),
                image_grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
                image_id="first",
            )
        )
        await asyncio.to_thread(started.wait, 1.0)
        second = asyncio.create_task(
            batcher.submit(
                worker=encoder,
                pixel_values=torch.ones((4, 8)),
                image_grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
                image_id="cancelled",
            )
        )
        for _ in range(100):
            if batcher.stats()["submitted"] == 2:
                break
            await asyncio.sleep(0.01)
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
        release.set()
        output = await first
        for _ in range(100):
            if batcher.stats()["skipped_cancelled"] == 1:
                break
            await asyncio.sleep(0.01)
        stats = batcher.stats()
        await batcher.close()
        return encoder, output, stats

    encoder, output, stats = asyncio.run(_run())

    assert output.bundle.image_hash == "first"
    assert encoder.batch_calls == [1]
    assert encoder.encode_calls == 1
    assert stats["submitted"] == 2
    assert stats["completed"] == 1
    assert stats["skipped_cancelled"] == 1
    assert stats["failures"] == 0
    assert stats["pending"] == 0


def test_qwen3_dynamic_batcher_close_accounts_for_cancelled_queued_work():
    started = threading.Event()
    release = threading.Event()

    class _BlockingEncoder(_BatchDummyEncoder):
        def encode_many(self, items):
            started.set()
            release.wait(timeout=2.0)
            return super().encode_many(items)

    async def _run():
        encoder = _BlockingEncoder()
        batcher = _Qwen3DynamicBatcher(
            max_batch_size=1,
            max_wait_ms=0.0,
            max_queue=8,
            max_patches=64,
            submit_timeout_s=1.0,
        )
        first = asyncio.create_task(
            batcher.submit(
                worker=encoder,
                pixel_values=torch.ones((4, 8)),
                image_grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
                image_id="inflight",
            )
        )
        await asyncio.to_thread(started.wait, 1.0)
        queued = asyncio.create_task(
            batcher.submit(
                worker=encoder,
                pixel_values=torch.ones((4, 8)),
                image_grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
                image_id="queued-cancelled",
            )
        )
        for _ in range(100):
            if batcher.stats()["submitted"] == 2:
                break
            await asyncio.sleep(0.01)
        queued.cancel()
        with pytest.raises(asyncio.CancelledError):
            await queued
        close_task = asyncio.create_task(batcher.close())
        await asyncio.sleep(0.01)
        release.set()
        await close_task
        with pytest.raises(RuntimeError, match="stopped during execution"):
            await first
        return encoder, batcher.stats()

    encoder, stats = asyncio.run(_run())

    assert encoder.batch_calls == [1]
    assert encoder.encode_calls == 1
    assert stats["submitted"] == 2
    assert stats["completed"] == 0
    assert stats["skipped_cancelled"] == 1
    assert stats["failures"] == 1
    assert stats["pending"] == 0


def test_qwen3_patch_aging_selects_smaller_ready_work_and_preserves_identity():
    entered = threading.Event()
    release = threading.Event()

    class _BlockingOrderEncoder(_BatchDummyEncoder):
        def __init__(self):
            super().__init__()
            self.execution_order = []

        def encode_many(self, items):
            self.execution_order.extend(item[2] for item in items)
            if len(self.execution_order) == 1:
                entered.set()
                release.wait(timeout=2.0)
            return super().encode_many(items)

    async def _run():
        encoder = _BlockingOrderEncoder()
        batcher = _Qwen3DynamicBatcher(
            max_batch_size=1,
            max_wait_ms=0.0,
            max_queue=8,
            max_patches=16384,
            submit_timeout_s=1.0,
            policy="patch_aging",
            reorder_window=8,
            starvation_ms=60_000.0,
        )

        def submit(image_id, patches):
            return batcher.submit(
                worker=encoder,
                pixel_values=torch.ones((patches, 8)),
                image_grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
                image_id=image_id,
            )

        blocker = asyncio.create_task(submit("blocker", 256))
        assert await asyncio.to_thread(entered.wait, 1.0)
        large = asyncio.create_task(submit("large", 8192))
        small = asyncio.create_task(submit("small", 256))
        for _ in range(100):
            if batcher.stats()["submitted"] == 3:
                break
            await asyncio.sleep(0.01)
        release.set()
        outputs = await asyncio.gather(blocker, large, small)
        await asyncio.wait_for(batcher._queue.join(), timeout=1.0)
        stats = batcher.stats()
        await batcher.close()
        return encoder, outputs, stats

    encoder, outputs, stats = asyncio.run(_run())

    assert encoder.execution_order == ["blocker", "small", "large"]
    assert [output.bundle.image_hash for output in outputs] == [
        "blocker",
        "large",
        "small",
    ]
    assert stats["scheduler_policy"] == "patch_aging"
    assert stats["reordered_selections"] == 1
    assert stats["starvation_overrides"] == 0
    assert stats["max_selection_width"] == 2
    assert stats["selected_patch_bucket_counts"] == {
        "large_4097_16384": 1,
        "small_le_512": 2,
    }
    assert stats["queue_wait_ms"]["count"] == 3
    assert stats["pending"] == 0


def test_qwen3_dynamic_batcher_fifo_policy_preserves_queue_order():
    entered = threading.Event()
    release = threading.Event()

    class _BlockingOrderEncoder(_BatchDummyEncoder):
        def __init__(self):
            super().__init__()
            self.execution_order = []

        def encode_many(self, items):
            self.execution_order.extend(item[2] for item in items)
            if len(self.execution_order) == 1:
                entered.set()
                release.wait(timeout=2.0)
            return super().encode_many(items)

    async def _run():
        encoder = _BlockingOrderEncoder()
        batcher = _Qwen3DynamicBatcher(
            max_batch_size=1,
            max_wait_ms=0.0,
            max_queue=8,
            max_patches=16384,
            submit_timeout_s=1.0,
            policy="fifo",
        )

        def submit(image_id, patches):
            return batcher.submit(
                worker=encoder,
                pixel_values=torch.ones((patches, 8)),
                image_grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
                image_id=image_id,
            )

        blocker = asyncio.create_task(submit("blocker", 256))
        assert await asyncio.to_thread(entered.wait, 1.0)
        large = asyncio.create_task(submit("large", 8192))
        small = asyncio.create_task(submit("small", 256))
        for _ in range(100):
            if batcher.stats()["submitted"] == 3:
                break
            await asyncio.sleep(0.01)
        release.set()
        await asyncio.gather(blocker, large, small)
        stats = batcher.stats()
        await batcher.close()
        return encoder, stats

    encoder, stats = asyncio.run(_run())

    assert encoder.execution_order == ["blocker", "large", "small"]
    assert stats["scheduler_policy"] == "fifo"
    assert stats["selection_scans"] == 0
    assert stats["reordered_selections"] == 0
    assert stats["starvation_overrides"] == 0
    assert stats["pending"] == 0


def test_qwen3_patch_aging_starvation_override_runs_oldest_large_work():
    entered = threading.Event()
    release = threading.Event()

    class _ManualClock:
        def __init__(self):
            self.now = 0.0

        def __call__(self):
            return self.now

    class _BlockingOrderEncoder(_BatchDummyEncoder):
        def __init__(self):
            super().__init__()
            self.execution_order = []

        def encode_many(self, items):
            self.execution_order.extend(item[2] for item in items)
            if len(self.execution_order) == 1:
                entered.set()
                release.wait(timeout=2.0)
            return super().encode_many(items)

    async def _run():
        clock = _ManualClock()
        encoder = _BlockingOrderEncoder()
        batcher = _Qwen3DynamicBatcher(
            max_batch_size=1,
            max_wait_ms=0.0,
            max_queue=8,
            max_patches=16384,
            submit_timeout_s=1.0,
            policy="patch_aging",
            reorder_window=8,
            starvation_ms=50_000.0,
            clock=clock,
        )

        def submit(image_id, patches):
            return batcher.submit(
                worker=encoder,
                pixel_values=torch.ones((patches, 8)),
                image_grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
                image_id=image_id,
            )

        blocker = asyncio.create_task(submit("blocker", 256))
        assert await asyncio.to_thread(entered.wait, 1.0)
        clock.now = 1.0
        large = asyncio.create_task(submit("oldest-large", 8192))
        await asyncio.sleep(0)
        clock.now = 2.0
        small = asyncio.create_task(submit("new-small", 256))
        for _ in range(100):
            if batcher.stats()["submitted"] == 3:
                break
            await asyncio.sleep(0.01)
        clock.now = 100.0
        release.set()
        await asyncio.gather(blocker, large, small)
        stats = batcher.stats()
        await batcher.close()
        return encoder, stats

    encoder, stats = asyncio.run(_run())

    assert encoder.execution_order == ["blocker", "oldest-large", "new-small"]
    assert stats["starvation_overrides"] == 1
    assert stats["reordered_selections"] == 0
    assert stats["max_observed_queue_wait_ms"] >= 99_000.0
    assert stats["pending"] == 0


def test_qwen3_patch_aging_reorders_only_inside_configured_window():
    entered = threading.Event()
    release = threading.Event()

    class _BlockingOrderEncoder(_BatchDummyEncoder):
        def __init__(self):
            super().__init__()
            self.execution_order = []

        def encode_many(self, items):
            self.execution_order.extend(item[2] for item in items)
            if len(self.execution_order) == 1:
                entered.set()
                release.wait(timeout=2.0)
            return super().encode_many(items)

    async def _run():
        encoder = _BlockingOrderEncoder()
        batcher = _Qwen3DynamicBatcher(
            max_batch_size=1,
            max_wait_ms=0.0,
            max_queue=8,
            max_patches=16384,
            submit_timeout_s=1.0,
            policy="patch_aging",
            reorder_window=2,
            starvation_ms=60_000.0,
        )

        def submit(image_id, patches):
            return batcher.submit(
                worker=encoder,
                pixel_values=torch.ones((patches, 8)),
                image_grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
                image_id=image_id,
            )

        blocker = asyncio.create_task(submit("blocker", 256))
        assert await asyncio.to_thread(entered.wait, 1.0)
        tasks = [
            asyncio.create_task(submit("large", 8192)),
            asyncio.create_task(submit("medium", 2048)),
            asyncio.create_task(submit("small-outside-window", 256)),
        ]
        for _ in range(100):
            if batcher.stats()["submitted"] == 4:
                break
            await asyncio.sleep(0.01)
        release.set()
        await asyncio.gather(blocker, *tasks)
        stats = batcher.stats()
        await batcher.close()
        return encoder, stats

    encoder, stats = asyncio.run(_run())

    assert encoder.execution_order == [
        "blocker",
        "medium",
        "small-outside-window",
        "large",
    ]
    assert stats["reorder_window"] == 2
    assert stats["max_selection_width"] == 2
    assert stats["reordered_selections"] == 1
    assert stats["pending"] == 0


def test_qwen3_patch_aging_skips_cancelled_ready_item_before_selection():
    entered = threading.Event()
    release = threading.Event()

    class _BlockingOrderEncoder(_BatchDummyEncoder):
        def __init__(self):
            super().__init__()
            self.execution_order = []

        def encode_many(self, items):
            self.execution_order.extend(item[2] for item in items)
            if len(self.execution_order) == 1:
                entered.set()
                release.wait(timeout=2.0)
            return super().encode_many(items)

    async def _run():
        encoder = _BlockingOrderEncoder()
        batcher = _Qwen3DynamicBatcher(
            max_batch_size=1,
            max_wait_ms=0.0,
            max_queue=8,
            max_patches=16384,
            submit_timeout_s=1.0,
            policy="patch_aging",
            reorder_window=8,
            starvation_ms=60_000.0,
        )

        def submit(image_id, patches):
            return batcher.submit(
                worker=encoder,
                pixel_values=torch.ones((patches, 8)),
                image_grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
                image_id=image_id,
            )

        blocker = asyncio.create_task(submit("blocker", 256))
        assert await asyncio.to_thread(entered.wait, 1.0)
        cancelled = asyncio.create_task(submit("cancelled-large", 8192))
        live = asyncio.create_task(submit("live-small", 256))
        for _ in range(100):
            if batcher.stats()["submitted"] == 3:
                break
            await asyncio.sleep(0.01)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        release.set()
        await asyncio.gather(blocker, live)
        await asyncio.wait_for(batcher._queue.join(), timeout=1.0)
        stats = batcher.stats()
        await batcher.close()
        return encoder, stats

    encoder, stats = asyncio.run(_run())

    assert encoder.execution_order == ["blocker", "live-small"]
    assert stats["skipped_cancelled"] == 1
    assert stats["completed"] == 2
    assert stats["failures"] == 0
    assert stats["queue_size"] == 0
    assert stats["pending"] == 0


def test_qwen3_patch_aging_configuration_fails_closed_and_reaches_health():
    with pytest.raises(ValueError, match="requires max batch size 1"):
        create_encoder_app(
            EncoderServiceConfig(
                enable_qwen3_dynamic_batching=True,
                qwen3_dynamic_batch_policy="patch_aging",
                qwen3_dynamic_batch_max_size=2,
            ),
            encoder=_BatchDummyEncoder(),
        )
    with pytest.raises(ValueError, match="reorder window must be positive"):
        create_encoder_app(
            EncoderServiceConfig(qwen3_dynamic_batch_reorder_window=0),
            encoder=_BatchDummyEncoder(),
        )
    with pytest.raises(ValueError, match="starvation must be non-negative"):
        create_encoder_app(
            EncoderServiceConfig(qwen3_dynamic_batch_starvation_ms=-1.0),
            encoder=_BatchDummyEncoder(),
        )

    app = create_encoder_app(
        EncoderServiceConfig(
            enable_qwen3_dynamic_batching=True,
            qwen3_dynamic_batch_policy="patch_aging",
            qwen3_dynamic_batch_max_size=1,
            qwen3_dynamic_batch_reorder_window=7,
            qwen3_dynamic_batch_starvation_ms=321.0,
        ),
        encoder=_BatchDummyEncoder(),
    )
    with TestClient(app) as client:
        stats = client.get("/health").json()["qwen3_dynamic_batcher"]
    assert stats["scheduler_policy"] == "patch_aging"
    assert stats["reorder_window"] == 7
    assert stats["starvation_ms"] == 321.0
    assert stats["reordered_selections"] == 0


def test_qwen3_dynamic_batcher_close_interrupts_inflight_batch_without_hanging():
    entered = threading.Event()
    finished = threading.Event()

    class _SlowBatchEncoder:
        def encode_many(self, items):
            entered.set()
            try:
                time.sleep(0.2)
                return []
            finally:
                finished.set()

    async def _run():
        batcher = _Qwen3DynamicBatcher(
            max_batch_size=1,
            max_wait_ms=0.0,
            max_queue=2,
            max_patches=64,
            submit_timeout_s=1.0,
        )
        submitted = asyncio.create_task(
            batcher.submit(
                worker=_SlowBatchEncoder(),
                pixel_values=torch.ones((4, 8)),
                image_grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
                image_id="shutdown-inflight",
            )
        )
        assert await asyncio.to_thread(entered.wait, 1.0)
        await asyncio.wait_for(batcher.close(), timeout=1.0)
        assert finished.is_set()
        assert batcher.stats()["native_encode_inflight"] == 0
        with pytest.raises(RuntimeError, match="stopped during execution"):
            await submitted

    asyncio.run(_run())


def test_qwen3_dynamic_batcher_close_unblocks_queue_putters_without_leaking_work():
    entered = threading.Event()
    finished = threading.Event()

    class _SlowBatchEncoder:
        def encode_many(self, items):
            entered.set()
            try:
                time.sleep(0.2)
                return []
            finally:
                finished.set()

    async def _wait_for(predicate, timeout_s=1.0):
        deadline = asyncio.get_running_loop().time() + timeout_s
        while not predicate():
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError("timed out waiting for batcher state")
            await asyncio.sleep(0.001)

    async def _run():
        batcher = _Qwen3DynamicBatcher(
            max_batch_size=1,
            max_wait_ms=0.0,
            max_queue=1,
            max_patches=64,
            submit_timeout_s=5.0,
        )
        encoder = _SlowBatchEncoder()

        def _submit(index):
            return batcher.submit(
                worker=encoder,
                pixel_values=torch.ones((4, 8)),
                image_grid_thw=torch.tensor([[1, 2, 2]], dtype=torch.long),
                image_id=f"shutdown-queued-{index}",
            )

        first = asyncio.create_task(_submit(0))
        assert await asyncio.to_thread(entered.wait, 1.0)
        second = asyncio.create_task(_submit(1))
        await _wait_for(lambda: batcher.stats()["queue_size"] == 1)
        blocked = asyncio.create_task(_submit(2))
        await _wait_for(lambda: batcher.stats()["queue_full_waits"] >= 1)

        await asyncio.wait_for(batcher.close(), timeout=1.0)
        assert finished.is_set()
        results = await asyncio.gather(first, second, blocked, return_exceptions=True)
        stats = batcher.stats()
        return results, stats

    results, stats = asyncio.run(_run())
    assert all(isinstance(result, RuntimeError) for result in results)
    assert stats["submitted"] == 2
    assert stats["failures"] == 2
    assert stats["pending"] == 0
    assert stats["queue_size"] == 0


def test_online_encoder_routes_concurrent_cache_misses_through_dynamic_batcher():
    encoder = _BatchDummyEncoder()
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=True,
            enable_qwen3_dynamic_batching=True,
            qwen3_dynamic_batch_max_size=4,
            qwen3_dynamic_batch_wait_ms=50.0,
            qwen3_dynamic_batch_max_patches=64,
        ),
        encoder=encoder,
    )
    bodies = [
        {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": _png_data_url((32 + index, 64, 128))
                            },
                        },
                        {"type": "text", "text": f"describe {index}"},
                    ],
                }
            ]
        }
        for index in range(4)
    ]

    with TestClient(app) as client:
        with ThreadPoolExecutor(max_workers=4) as pool:
            responses = list(pool.map(lambda body: client.post("/describe", json=body), bodies))
        health = client.get("/health").json()

    assert all(response.status_code == 200 for response in responses)
    assert encoder.batch_calls == [4]
    assert encoder.encode_calls == 4
    stats = health["qwen3_dynamic_batcher"]
    assert stats["completed"] == 4
    assert stats["multi_item_batches"] == 1
    assert stats["batch_size_counts"] == {"4": 1}


def test_online_encoder_spends_and_resets_direct_credits_per_burst():
    class _SlowCreditEncoder(_BatchDummyEncoder):
        def encode(self, *, pixel_values, image_grid_thw, image_id=None):
            time.sleep(0.05)
            return super().encode(
                pixel_values=pixel_values,
                image_grid_thw=image_grid_thw,
                image_id=image_id,
            )

    encoder = _SlowCreditEncoder()
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=False,
            enable_qwen3_dynamic_batching=True,
            qwen3_dynamic_batch_max_size=2,
            qwen3_dynamic_batch_wait_ms=100.0,
            qwen3_dynamic_batch_min_inflight=1,
            qwen3_dynamic_batch_direct_credits=2,
            qwen3_dynamic_batch_max_patches=64,
        ),
        encoder=encoder,
    )

    def _bodies(burst: int):
        return [
            {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": _png_data_url(
                                        (32 + burst * 8 + index, 64, 128)
                                    )
                                },
                            },
                            {
                                "type": "text",
                                "text": f"credit burst={burst} item={index}",
                            },
                        ],
                    }
                ]
            }
            for index in range(4)
        ]

    with TestClient(app) as client:
        for burst in range(2):
            with ThreadPoolExecutor(max_workers=4) as pool:
                responses = list(
                    pool.map(
                        lambda body: client.post("/describe", json=body),
                        _bodies(burst),
                    )
                )
            assert all(response.status_code == 200 for response in responses)
        health = client.get("/health").json()

    admission = health["qwen3_batch_admission"]
    assert admission["burst_activations"] == 2
    assert admission["direct_credits_per_burst"] == 2
    assert admission["direct_credit_decisions"] == 4
    assert admission["direct_decisions"] == 4
    assert admission["batched_decisions"] == 4
    assert admission["direct_credits_remaining"] == 0
    assert encoder.batch_calls == [2, 2]
    stats = health["qwen3_dynamic_batcher"]
    assert stats["submitted"] == 4
    assert stats["completed"] == 4
    assert stats["batch_size_counts"] == {"2": 2}


def test_online_encoder_admission_bypasses_batching_below_pressure_threshold():
    encoder = _BatchDummyEncoder()
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=False,
            enable_qwen3_dynamic_batching=True,
            qwen3_dynamic_batch_max_size=4,
            qwen3_dynamic_batch_wait_ms=50.0,
            qwen3_dynamic_batch_min_inflight=6,
            qwen3_dynamic_batch_max_patches=64,
        ),
        encoder=encoder,
    )
    bodies = [
        {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": _png_data_url((64 + index, 32, 128))
                            },
                        },
                        {"type": "text", "text": f"threshold {index}"},
                    ],
                }
            ]
        }
        for index in range(4)
    ]

    with TestClient(app) as client:
        with ThreadPoolExecutor(max_workers=4) as pool:
            responses = list(pool.map(lambda body: client.post("/describe", json=body), bodies))
        health = client.get("/health").json()

    assert all(response.status_code == 200 for response in responses)
    assert encoder.batch_calls == []
    assert encoder.encode_calls == 4
    assert health["qwen3_dynamic_batcher"]["submitted"] == 0
    admission = health["qwen3_batch_admission"]
    assert admission["min_inflight"] == 6
    assert admission["peak_active"] == 4
    assert admission["direct_decisions"] == 4
    assert admission["batched_decisions"] == 0


def test_online_encoder_pressure_only_singleton_dispatch_uses_three_ready_credits():
    barrier = threading.Barrier(8)

    class _BarrierProcessor(_DummyProcessor):
        def apply_chat_template(self, *args, **kwargs):
            barrier.wait(timeout=5.0)
            return super().apply_chat_template(*args, **kwargs)

    encoder = _BatchDummyEncoder()
    encoder.processor = _BarrierProcessor()
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=False,
            enable_qwen3_dynamic_batching=True,
            qwen3_dynamic_batch_max_size=1,
            qwen3_dynamic_batch_wait_ms=0.0,
            qwen3_dynamic_batch_min_inflight=6,
            qwen3_dynamic_batch_direct_credits=3,
            qwen3_dynamic_batch_max_patches=64,
        ),
        encoder=encoder,
    )
    bodies = [
        {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": _png_data_url((80 + index, 32, 128))
                            },
                        },
                        {"type": "text", "text": f"pressure {index}"},
                    ],
                }
            ]
        }
        for index in range(8)
    ]

    with TestClient(app) as client:
        with ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(lambda body: client.post("/describe", json=body), bodies))
        health = client.get("/health").json()

    assert all(response.status_code == 200 for response in responses)
    admission = health["qwen3_batch_admission"]
    assert admission["peak_active"] == 8
    assert admission["burst_activations"] == 1
    assert admission["direct_credit_decisions"] == 3
    assert admission["direct_decisions"] == 3
    assert admission["batched_decisions"] == 5
    batcher = health["qwen3_dynamic_batcher"]
    assert batcher["submitted"] == 5
    assert batcher["completed"] == 5
    assert batcher["skipped_cancelled"] == 0
    assert batcher["batch_size_counts"] == {"1": 5}
    assert batcher["multi_item_batches"] == 0
    assert batcher["pending"] == 0
    assert batcher["native_encode_inflight"] == 0
    assert encoder.batch_calls == [1] * 5
    assert encoder.encode_calls == 8


def test_online_encoder_qwen3_bundle_cache_reuses_exact_image():
    encoder = _DummyEncoder()
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=True,
            feature_bundle_cache_max_entries=4,
            feature_bundle_cache_max_bytes=1024 * 1024,
        ),
        encoder=encoder,
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": _png_data_url()}},
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
    }

    with TestClient(app) as client:
        first = client.post("/describe", json=body)
        second = client.post("/describe", json=body)
        health = client.get("/health")

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert encoder.encode_calls == 1
    assert first.json()["feature_bundle_outcomes"] == [
        {
            "cache_hit": False,
            "singleflight_wait": False,
            "cache_admitted": True,
        }
    ]
    assert second.json()["feature_bundle_outcomes"] == [
        {
            "cache_hit": True,
            "singleflight_wait": False,
            "cache_admitted": False,
        }
    ]
    assert first.json()["feature_bundle_cache"]["misses"] == 1
    assert second.json()["feature_bundle_cache"]["hits"] == 1
    assert health.json()["feature_bundle_cache"]["entries"] == 1


def test_online_encoder_content_first_cache_hit_skips_decode_with_exact_descriptor():
    encoder = _DummyEncoder()
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=True,
            feature_bundle_cache_max_entries=4,
            feature_bundle_cache_max_bytes=1024 * 1024,
            qwen3_content_first_decode=True,
        ),
        encoder=encoder,
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": _png_data_url()}},
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
    }

    with TestClient(app) as client:
        first = client.post("/describe", json=body)
        second = client.post("/describe", json=body)
        health = client.get("/health").json()
        cleanup = client.post(
            "/discard_direct",
            json={"tickets": [first.json()["ticket"], second.json()["ticket"]]},
        )

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert cleanup.status_code == 200
    assert encoder.encode_calls == 1
    assert first.json()["descriptors"] == second.json()["descriptors"]
    assert second.json()["feature_bundle_outcomes"] == [
        {
            "cache_hit": True,
            "singleflight_wait": False,
            "cache_admitted": False,
        }
    ]
    decode = health["qwen3_content_first_decode"]
    assert decode["enabled"] is True
    assert decode["raw_sources"] == 2
    assert decode["image_decodes"] == 1
    assert decode["cache_decode_skips"] == 1
    assert decode["singleflight_decode_skips"] == 0
    assert decode["image_decode_ms_total"] > 0.0


def test_online_encoder_bounded_preprocess_executors_preserve_descriptors():
    encoder = _DummyEncoder()
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=True,
            feature_bundle_cache_max_entries=8,
            feature_bundle_cache_max_bytes=1024 * 1024,
            qwen3_content_first_decode=True,
            enable_qwen3_preprocess_executors=True,
            qwen3_media_workers=2,
            qwen3_media_max_pending=4,
            qwen3_processor_workers=4,
            qwen3_processor_max_pending=4,
        ),
        encoder=encoder,
    )
    bodies = [
        {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": _png_data_url((32 + index, 64, 128))},
                        },
                        {"type": "text", "text": "describe"},
                    ],
                }
            ]
        }
        for index in range(4)
    ]

    with TestClient(app) as client, ThreadPoolExecutor(max_workers=4) as pool:
        futures = [
            pool.submit(client.post, "/describe", json=body)
            for body in bodies
        ]
        responses = [future.result(timeout=3.0) for future in futures]
        health = client.get("/health").json()
        cleanup = client.post(
            "/discard_direct",
            json={"tickets": [response.json()["ticket"] for response in responses]},
        )

    assert all(response.status_code == 200 for response in responses)
    assert cleanup.status_code == 200
    descriptors = [response.json()["descriptors"][0] for response in responses]
    for descriptor in descriptors[1:]:
        assert descriptor["last_hidden"] == descriptors[0]["last_hidden"]
        assert descriptor["intermediates"] == descriptors[0]["intermediates"]
        assert descriptor["grid_thw"] == descriptors[0]["grid_thw"]
    pipeline = health["qwen3_preprocess_pipeline"]
    assert pipeline["enabled"] is True
    assert pipeline["media_source"]["calls"] == 4
    assert pipeline["media_source"]["failures"] == 0
    assert pipeline["processor"]["calls"] == 4
    assert pipeline["processor"]["failures"] == 0
    assert pipeline["media_executor"]["max_workers"] == 2
    assert pipeline["media_executor"]["submitted"] == 8
    assert pipeline["media_executor"]["completed"] == 8
    assert pipeline["processor_executor"]["max_workers"] == 4
    assert pipeline["processor_executor"]["submitted"] == 4
    assert pipeline["processor_executor"]["completed"] == 4
    assert pipeline["processor_executor"]["inflight"] == 0
    assert pipeline["processor_pool"]["enabled"] is True
    assert pipeline["processor_pool"]["replicas"] == 4
    assert pipeline["processor_pool"]["available"] == 4


def test_online_encoder_processor_replicas_prevent_shared_instance_reentry():
    replicas = []

    class _NonReentrantProcessor:
        def __init__(self, registry):
            self.registry = registry
            self.active = False
            self.max_active = 0
            self.lock = threading.Lock()
            registry.append(self)

        def __deepcopy__(self, memo):
            del memo
            return _NonReentrantProcessor(self.registry)

        def apply_chat_template(self, *args, **kwargs):
            del args, kwargs
            with self.lock:
                if self.active:
                    raise RuntimeError("processor instance was re-entered")
                self.active = True
                self.max_active = max(self.max_active, 1)
            try:
                time.sleep(0.04)
                return {
                    "pixel_values": torch.arange(12, dtype=torch.float32).reshape(3, 4),
                    "image_grid_thw": torch.tensor([[1, 2, 2]], dtype=torch.long),
                }
            finally:
                with self.lock:
                    self.active = False

    encoder = _DummyEncoder()
    encoder.processor = _NonReentrantProcessor(replicas)
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=False,
            qwen3_content_first_decode=True,
            enable_qwen3_preprocess_executors=True,
            qwen3_media_workers=4,
            qwen3_media_max_pending=8,
            qwen3_processor_workers=4,
            qwen3_processor_max_pending=8,
        ),
        encoder=encoder,
    )
    bodies = [
        {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": _png_data_url((index, 40, 80))},
                        },
                        {"type": "text", "text": "replica stress"},
                    ],
                }
            ]
        }
        for index in range(4)
    ]

    with TestClient(app) as client, ThreadPoolExecutor(max_workers=4) as pool:
        responses = [
            future.result(timeout=3.0)
            for future in [pool.submit(client.post, "/describe", json=body) for body in bodies]
        ]
        health = client.get("/health").json()
        cleanup = client.post(
            "/discard_direct",
            json={"tickets": [response.json()["ticket"] for response in responses]},
        )

    assert all(response.status_code == 200 for response in responses)
    assert cleanup.status_code == 200
    assert len(replicas) == 4
    assert all(replica.max_active == 1 for replica in replicas)
    pool_stats = health["qwen3_preprocess_pipeline"]["processor_pool"]
    assert pool_stats["replicas"] == 4
    assert pool_stats["checkouts"] == 4
    assert pool_stats["peak_in_use"] >= 2
    assert pool_stats["failures"] == 0
    assert pool_stats["in_use"] == 0
    assert pool_stats["available"] == 4


def test_online_encoder_singleflight_waiter_does_not_claim_owner_admission():
    class _SlowDummyEncoder(_DummyEncoder):
        def encode(
            self,
            *,
            pixel_values,
            image_grid_thw,
            image_id=None,
            cache_key=None,
        ):
            del cache_key
            time.sleep(0.1)
            return super().encode(
                pixel_values=pixel_values,
                image_grid_thw=image_grid_thw,
                image_id=image_id,
            )

    encoder = _SlowDummyEncoder()
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=True,
            feature_bundle_cache_max_entries=4,
            feature_bundle_cache_max_bytes=1024 * 1024,
            qwen3_content_first_decode=True,
        ),
        encoder=encoder,
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": _png_data_url()}},
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
    }

    with TestClient(app) as client, ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(client.post, "/describe", json=body) for _ in range(2)]
        responses = [future.result(timeout=3.0) for future in futures]
        tickets = [response.json()["ticket"] for response in responses]
        health = client.get("/health").json()
        cleanup = client.post("/discard_direct", json={"tickets": tickets})

    assert all(response.status_code == 200 for response in responses)
    assert cleanup.status_code == 200
    assert encoder.encode_calls == 1
    outcomes = [response.json()["feature_bundle_outcomes"][0] for response in responses]
    assert sum(int(outcome["cache_admitted"]) for outcome in outcomes) == 1
    assert sum(int(outcome["singleflight_wait"]) for outcome in outcomes) == 1
    waiter = next(outcome for outcome in outcomes if outcome["singleflight_wait"])
    assert waiter == {
        "cache_hit": False,
        "singleflight_wait": True,
        "cache_admitted": False,
    }
    decode = health["qwen3_content_first_decode"]
    assert decode["raw_sources"] == 2
    assert decode["image_decodes"] == 1
    assert decode["cache_decode_skips"] == 0
    assert decode["singleflight_decode_skips"] == 1


def test_online_encoder_content_cache_rebinds_distinct_client_identities():
    encoder = _DummyEncoder()
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=True,
            feature_bundle_cache_max_entries=4,
            feature_bundle_cache_max_bytes=1024 * 1024,
        ),
        encoder=encoder,
    )

    def _body(identity: str):
        return {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "uuid": identity,
                            "image_url": {"url": _png_data_url()},
                        },
                        {"type": "text", "text": "describe"},
                    ],
                }
            ]
        }

    with TestClient(app) as client:
        first = client.post("/describe", json=_body("client-image-a"))
        second = client.post("/describe", json=_body("client-image-b"))
        health = client.get("/health").json()

    assert first.status_code == 200
    assert second.status_code == 200
    assert encoder.encode_calls == 1
    first_descriptor = first.json()["descriptors"][0]
    second_descriptor = second.json()["descriptors"][0]
    assert first_descriptor["feature_id"] != second_descriptor["feature_id"]
    assert first_descriptor["last_hidden"] == second_descriptor["last_hidden"]
    assert second_descriptor["metadata"]["request_identity_rebound"] is True
    assert second_descriptor["metadata"]["content_cache_feature_id"]
    assert health["feature_bundle_cache"]["entries"] == 1
    assert health["feature_bundle_cache"]["hits"] == 1


def test_online_encoder_reuse_density_cache_retains_hot_content_during_scan():
    encoder = _DummyEncoder()
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=True,
            feature_bundle_cache_max_entries=2,
            feature_bundle_cache_max_bytes=1024 * 1024,
            feature_bundle_cache_admission_policy="reuse_density",
        ),
        encoder=encoder,
    )

    def _body(color):
        return {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {"url": _png_data_url(color=color)},
                        },
                        {"type": "text", "text": "describe"},
                    ],
                }
            ]
        }

    hot = _body((255, 0, 0))
    cold_0 = _body((0, 255, 0))
    cold_1 = _body((0, 0, 255))
    with TestClient(app) as client:
        assert client.post("/describe", json=hot).status_code == 200
        assert client.post("/describe", json=cold_0).status_code == 200
        assert client.post("/describe", json=hot).status_code == 200
        assert client.post("/describe", json=cold_1).status_code == 200
        final_hot = client.post("/describe", json=hot)
        health = client.get("/health").json()

    assert final_hot.status_code == 200
    assert encoder.encode_calls == 3
    cache = health["feature_bundle_cache"]
    assert cache["admission_policy"] == "reuse_density"
    assert cache["hits"] == 2
    assert cache["value_evictions"] == 1


def test_online_encoder_rejects_local_and_http_image_urls_by_default(tmp_path):
    image_path = tmp_path / "image.png"
    Image.new("RGB", (4, 4), color=(1, 2, 3)).save(image_path)
    app = create_encoder_app(
        EncoderServiceConfig(publish_backend="direct_engine", device="cpu"),
        encoder=_DummyEncoder(),
    )

    def _body(url):
        return {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "image_url", "image_url": {"url": url}},
                        {"type": "text", "text": "describe"},
                    ],
                }
            ]
        }

    with TestClient(app) as client:
        local = client.post("/describe", json=_body(image_path.as_uri()))
        remote = client.post(
            "/describe",
            json=_body("http://127.0.0.1:9/private.png"),
        )

    assert local.status_code == 403
    assert "local image paths are disabled" in local.text
    assert remote.status_code == 403
    assert "HTTP image URLs are disabled" in remote.text


def test_online_encoder_keeps_http_fail_closed_when_legacy_flags_are_enabled():
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            allow_http_image_urls=True,
            http_image_host_allowlist="example.com",
        ),
        encoder=_DummyEncoder(),
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": "https://example.com/image.png"},
                    },
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
    }

    with TestClient(app) as client:
        response = client.post("/describe", json=body)
        health = client.get("/health").json()

    assert response.status_code == 403
    assert "pinned-IP transport" in response.text
    assert health["http_image_urls"] == {
        "enabled": False,
        "legacy_enable_requested": True,
        "reason": "pinned-IP transport is not implemented",
    }


def test_online_encoder_allows_local_image_inside_explicit_root(tmp_path):
    image_path = tmp_path / "image.png"
    Image.new("RGB", (4, 4), color=(4, 5, 6)).save(image_path)
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            local_image_root=str(tmp_path),
        ),
        encoder=_DummyEncoder(),
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": image_path.as_uri()},
                    },
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
    }

    with TestClient(app) as client:
        response = client.post("/describe", json=body)
        assert response.status_code == 200, response.text
        cleanup = client.post(
            "/discard_direct",
            json={"ticket": response.json()["ticket"]},
        )
        assert cleanup.status_code == 200


def test_online_encoder_allows_percent_encoded_file_uri_inside_root(tmp_path):
    image_path = tmp_path / "with space-图.png"
    Image.new("RGB", (4, 4), color=(9, 8, 7)).save(image_path)
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            local_image_root=str(tmp_path),
        ),
        encoder=_DummyEncoder(),
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": image_path.as_uri()},
                    },
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
    }

    with TestClient(app) as client:
        response = client.post("/describe", json=body)
        cleanup = client.post(
            "/discard_direct",
            json={"ticket": response.json()["ticket"]},
        )

    assert "%20" in image_path.as_uri()
    assert response.status_code == 200, response.text
    assert cleanup.status_code == 200


def test_online_encoder_rejects_remote_host_in_file_uri(tmp_path):
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            local_image_root=str(tmp_path),
        ),
        encoder=_DummyEncoder(),
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": "file://remote-host/tmp/image.png"},
                    },
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
    }

    with TestClient(app) as client:
        response = client.post("/describe", json=body)

    assert response.status_code == 403
    assert "host must be empty or localhost" in response.text


def test_online_encoder_rejects_symlinked_local_image_without_reading_target(tmp_path):
    allowed_root = tmp_path / "allowed"
    allowed_root.mkdir()
    outside = tmp_path / "outside.png"
    Image.new("RGB", (4, 4), color=(200, 1, 2)).save(outside)
    (allowed_root / "swapped.png").symlink_to(outside)
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            local_image_root=str(allowed_root),
        ),
        encoder=_DummyEncoder(),
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": (allowed_root / "swapped.png").as_uri()},
                    },
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
    }

    with TestClient(app) as client:
        response = client.post("/describe", json=body)

    assert response.status_code == 403
    assert "safely readable" in response.text


def test_online_encoder_discard_direct_releases_unpublished_gpu_bundles():
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=False,
        ),
        encoder=_DummyEncoder(),
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": _png_data_url()}},
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
    }

    with TestClient(app) as client:
        first = client.post("/describe", json=body)
        second = client.post("/describe", json=body)
        before = client.get("/health").json()["pending_direct_bundles"]
        cleanup = client.post(
            "/discard_direct",
            json={"tickets": [first.json()["ticket"], "already-gone"]},
        )
        repeated = client.post(
            "/discard_direct",
            json={"ticket": first.json()["ticket"]},
        )
        final = client.post(
            "/discard_direct",
            json={"ticket": second.json()["ticket"]},
        )

    assert first.status_code == 200
    assert second.status_code == 200
    assert before["tickets"] == 2
    assert before["retained_bytes"] > 0
    assert cleanup.status_code == 200
    assert cleanup.json()["discarded"] == [first.json()["ticket"]]
    assert cleanup.json()["unknown"] == ["already-gone"]
    assert cleanup.json()["released_records"] == 1
    assert cleanup.json()["released_bytes"] > 0
    assert cleanup.json()["visibility_sync_ms"] >= 0.0
    assert cleanup.json()["pending_direct_bundles"]["tickets"] == 1
    assert repeated.status_code == 200
    assert repeated.json()["discarded"] == []
    assert repeated.json()["unknown"] == [first.json()["ticket"]]
    assert final.json()["pending_direct_bundles"]["tickets"] == 0
    assert final.json()["pending_direct_bundles"]["created"] == 2
    assert final.json()["pending_direct_bundles"]["discarded"] == 2


def test_online_encoder_publish_target_mismatch_synchronizes_and_releases_ticket():
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=False,
        ),
        encoder=_DummyEncoder(),
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": _png_data_url()}},
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
    }

    with TestClient(app) as client:
        described = client.post("/describe", json=body)
        ticket = described.json()["ticket"]
        failed = client.post(
            "/publish_direct",
            json={
                "ticket": ticket,
                "mooncake_epd_direct_feature_targets": [],
            },
        )
        health = client.get("/health").json()["pending_direct_bundles"]

    assert described.status_code == 200
    assert failed.status_code == 400
    assert "direct target count mismatch: targets=0 descriptors=1" in failed.text
    assert health["tickets"] == 0
    assert health["retained_bytes"] == 0
    assert health["publish_failures"] == 1
    assert health["publish_failure_records"] == 1
    assert health["publish_failure_bytes"] > 0
    assert health["publish_failure_visibility_sync_ms"] >= 0.0


def test_online_encoder_rejects_direct_ticket_entry_overload_before_encoding():
    encoder = _DummyEncoder()
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=False,
            direct_ticket_max_entries=1,
        ),
        encoder=encoder,
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": _png_data_url()}},
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
    }

    with TestClient(app) as client:
        accepted = client.post("/describe", json=body)
        rejected = client.post("/describe", json=body)
        health = client.get("/health").json()["pending_direct_bundles"]
        cleanup = client.post(
            "/discard_direct",
            json={"ticket": accepted.json()["ticket"]},
        )

    assert accepted.status_code == 200
    assert rejected.status_code == 503
    assert rejected.headers["retry-after"] == "1"
    assert encoder.encode_calls == 1
    assert health["tickets"] == 1
    assert health["max_entries"] == 1
    assert health["capacity_rejections"] == 1
    assert health["entry_limit_rejections"] == 1
    assert health["byte_limit_rejections"] == 0
    assert health["high_watermark_tickets"] == 1
    assert cleanup.json()["pending_direct_bundles"]["retained_bytes"] == 0


def test_online_encoder_rejects_direct_ticket_byte_overload_and_releases_records():
    encoder = _DummyEncoder()
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=False,
            direct_ticket_max_bytes=1,
        ),
        encoder=encoder,
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": _png_data_url()}},
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
    }

    with TestClient(app) as client:
        rejected = client.post("/describe", json=body)
        health = client.get("/health").json()["pending_direct_bundles"]

    assert rejected.status_code == 503
    assert rejected.headers["retry-after"] == "1"
    assert encoder.encode_calls == 1
    assert health["tickets"] == 0
    assert health["retained_bytes"] == 0
    assert health["max_bytes"] == 1
    assert health["capacity_rejections"] == 1
    assert health["byte_limit_rejections"] == 1
    assert health["rejected_records"] == 1
    assert health["rejected_bytes"] > 1
    assert health["rejection_visibility_sync_ms"] >= 0.0


def test_online_encoder_expires_abandoned_direct_tickets():
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=False,
            direct_ticket_ttl_s=0.01,
        ),
        encoder=_DummyEncoder(),
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": _png_data_url()}},
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
    }

    with TestClient(app) as client:
        described = client.post("/describe", json=body)
        deadline = time.monotonic() + 1.0
        while app.state.pending_direct_bundles and time.monotonic() < deadline:
            time.sleep(0.005)
        assert len(app.state.pending_direct_bundles) == 0
        health = client.get("/health").json()
        discarded = client.post(
            "/discard_direct",
            json={"ticket": described.json()["ticket"]},
        )

    assert described.status_code == 200
    pending = health["pending_direct_bundles"]
    assert pending["tickets"] == 0
    assert pending["expired"] == 1
    assert pending["ttl_s"] == pytest.approx(0.01)
    assert pending["retained_bytes"] == 0
    assert pending["high_watermark_tickets"] == 1
    assert pending["high_watermark_bytes"] > 0
    assert discarded.status_code == 200
    assert discarded.json()["unknown"] == [described.json()["ticket"]]


def test_online_encoder_describe_rejects_non_direct_backends_without_encoding(tmp_path):
    encoder = _DummyEncoder()
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="file",
            store_dir=str(tmp_path),
            device="cpu",
        ),
        encoder=encoder,
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": _png_data_url()}},
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
    }

    with TestClient(app) as client:
        response = client.post("/describe", json=body)

    assert response.status_code == 400
    assert "/describe requires publish_backend=direct_engine" in response.text
    assert encoder.encode_calls == 0


class _FakeMooncakeEngine:
    def __init__(self, *, return_code: int = 0):
        self.calls = []
        self.return_code = int(return_code)

    def batch_transfer_sync_write(self, remote_session, local_ptrs, remote_ptrs, lengths):
        self.calls.append((remote_session, list(local_ptrs), list(remote_ptrs), list(lengths)))
        return self.return_code

    def transfer_sync_write(self, remote_session, local_ptr, remote_ptr, length):
        self.calls.append((remote_session, [int(local_ptr)], [int(remote_ptr)], [int(length)]))
        return self.return_code


class _TrackingTransferEngine(TransferEngine):
    def __init__(self, *, return_code: int = 0):
        super().__init__(protocol="tcp")
        self.fake_backend = _FakeMooncakeEngine(return_code=return_code)
        self.bind_mooncake_backend(
            self.fake_backend,
            initialized=True,
            owns_backend=False,
        )
        self.shutdown_calls = 0

    def shutdown(self) -> None:
        self.shutdown_calls += 1
        super().shutdown()


class _TrackingTransferEngineFactory:
    def __init__(self, return_codes):
        self.return_codes = list(return_codes)
        self.engines = []

    def __call__(self):
        return_code = self.return_codes[len(self.engines)]
        engine = _TrackingTransferEngine(return_code=return_code)
        self.engines.append(engine)
        return engine


def _direct_encode_body(*, incarnation: str | None = None):
    target = {
        "remote_session": "prefill-session",
        "remote_pointers": {
            "last_hidden": 10000,
            "last_hidden:nbytes": 4 * 8 * 4,
            "grid_thw": 20000,
            "grid_thw:nbytes": 1 * 3 * 8,
            "intermediate:1:0": 30000,
            "intermediate:1:0:nbytes": 4 * 8 * 4,
        },
    }
    if incarnation is not None:
        target["remote_incarnation"] = incarnation
    return {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": _png_data_url()}},
                    {"type": "text", "text": "describe"},
                ],
            }
        ],
        "metadata": {"mooncake_epd_direct_feature_targets": [target]},
    }


def test_online_encoder_service_publishes_file_feature_handle(tmp_path):
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="file",
            store_dir=str(tmp_path / "feature-store"),
            device="cpu",
        ),
        encoder=_DummyEncoder(),
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": _png_data_url()}},
                    {"type": "text", "text": "describe"},
                ],
            }
        ]
    }

    with TestClient(app) as client:
        response = client.post("/encode", json=body)
        assert response.status_code == 200, response.text
        payload = response.json()

    assert payload["count"] == 1
    handle = payload["handles"][0]
    assert handle["uri"].startswith("file:")
    assert handle["metadata"]["source_mm_hash"] == handle["feature_id"]
    provider = FeatureHandleProvider(FeatureHandleProviderConfig(store_dirs=(tmp_path / "feature-store",)))
    resolved = provider.resolve_from_sources({"mm_feature_handles": [handle]}, device="cpu", dtype=torch.float32)
    assert resolved is not None
    assert tuple(resolved.image_embeds.shape) == (4, 16)  # main + deepstack packed
    assert resolved.image_grid_thw.tolist() == [[1, 2, 2]]


def test_online_encoder_encode_reports_explicit_cache_outcomes(tmp_path):
    encoder = _DummyEncoder()
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="file",
            store_dir=str(tmp_path / "feature-store"),
            device="cpu",
            enable_feature_bundle_cache=True,
        ),
        encoder=encoder,
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": _png_data_url()}},
                    {"type": "text", "text": "encode"},
                ],
            }
        ]
    }

    with TestClient(app) as client:
        first = client.post("/encode", json=body)
        second = client.post("/encode", json=body)

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert encoder.encode_calls == 1
    assert first.json()["feature_bundle_outcomes"] == [
        {
            "cache_hit": False,
            "singleflight_wait": False,
            "cache_admitted": True,
        }
    ]
    assert second.json()["feature_bundle_outcomes"] == [
        {
            "cache_hit": True,
            "singleflight_wait": False,
            "cache_admitted": False,
        }
    ]


def test_online_encoder_service_publishes_direct_engine_feature_handle():
    direct_engine = TransferEngine(protocol="tcp")
    fake = _FakeMooncakeEngine()
    direct_engine.bind_mooncake_backend(fake, initialized=True, owns_backend=False)
    app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
        ),
        encoder=_DummyEncoder(),
        direct_transfer_engine=direct_engine,
    )
    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": _png_data_url()}},
                    {"type": "text", "text": "describe"},
                ],
            }
        ],
        "metadata": {
            "mooncake_epd_direct_feature_targets": [
                {
                    "remote_session": "prefill-session",
                    "remote_pointers": {
                        "last_hidden": 10000,
                        "last_hidden:nbytes": 4 * 8 * 4,
                        "grid_thw": 20000,
                        "grid_thw:nbytes": 1 * 3 * 8,
                        "intermediate:1:0": 30000,
                        "intermediate:1:0:nbytes": 4 * 8 * 4,
                    },
                }
            ]
        },
    }

    with TestClient(app) as client:
        response = client.post("/encode", json=body)
        assert response.status_code == 200, response.text
        payload = response.json()

    handle = payload["handles"][0]
    assert handle["uri"].startswith("epd-direct://")
    assert handle["metadata"]["backend"] == "direct_engine"
    assert handle["metadata"]["direct_backend"] == "feature_peer_buffer_direct"
    assert handle["metadata"]["direct_tensor_count"] == 3
    assert handle["metadata"]["direct_bytes"] == 280
    assert fake.calls and fake.calls[0][0] == "prefill-session"

    provider = FeatureHandleProvider(FeatureHandleProviderConfig(device="cpu", strict=True))
    with pytest.raises(FeatureHandleError, match="epd-direct FeatureHandle"):
        provider.resolve_from_sources({"mm_feature_handles": [handle]}, device="cpu", dtype=torch.float32)


def test_online_encoder_rotates_direct_engine_before_new_prefill_incarnation():
    encoder = _DummyEncoder()
    factory = _TrackingTransferEngineFactory([0, 0])
    app = create_encoder_app(
        EncoderServiceConfig(publish_backend="direct_engine", device="cpu"),
        encoder=encoder,
        direct_transfer_engine_factory=factory,
    )

    with TestClient(app) as client:
        first = client.post(
            "/encode",
            json=_direct_encode_body(incarnation="prefill-incarnation-a"),
        )
        second = client.post(
            "/encode",
            json=_direct_encode_body(incarnation="prefill-incarnation-b"),
        )
        health = client.get("/health").json()["direct_transfer_engine"]

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert encoder.encode_calls == 1
    assert len(factory.engines) == 2
    assert len(factory.engines[0].fake_backend.calls) == 1
    assert len(factory.engines[1].fake_backend.calls) == 1
    assert factory.engines[0].shutdown_calls == 1
    assert health["generation"] == 2
    assert health["engine_resets"] == 1
    assert health["proactive_resets"] == 1
    metadata = second.json()["handles"][0]["metadata"]
    assert metadata["direct_engine_generation"] == 2
    assert metadata["direct_engine_recovery"] == "proactive_incarnation"
    assert metadata["direct_remote_incarnation"] == "prefill-incarnation-b"


def test_online_encoder_rebuilds_and_retries_direct_transfer_once_without_reencoding():
    encoder = _DummyEncoder()
    factory = _TrackingTransferEngineFactory([-1, 0])
    app = create_encoder_app(
        EncoderServiceConfig(publish_backend="direct_engine", device="cpu"),
        encoder=encoder,
        direct_transfer_engine_factory=factory,
    )

    with TestClient(app) as client:
        response = client.post("/encode", json=_direct_encode_body())
        health = client.get("/health").json()["direct_transfer_engine"]

    assert response.status_code == 200, response.text
    assert encoder.encode_calls == 1
    assert len(factory.engines) == 2
    assert len(factory.engines[0].fake_backend.calls) == 1
    assert len(factory.engines[1].fake_backend.calls) == 1
    assert factory.engines[0].shutdown_calls == 1
    assert health["generation"] == 2
    assert health["engine_resets"] == 1
    assert health["reactive_resets"] == 1
    assert health["publish_retries"] == 1
    assert health["recovery_successes"] == 1
    assert health["recovery_failures"] == 0
    metadata = response.json()["handles"][0]["metadata"]
    assert metadata["direct_engine_generation"] == 2
    assert metadata["direct_engine_recovery"] == "reactive_transfer_failure"
    assert metadata["direct_engine_recovered"] is True


def test_online_encoder_direct_recovery_is_bounded_to_one_retry():
    encoder = _DummyEncoder()
    factory = _TrackingTransferEngineFactory([-1, -2])
    app = create_encoder_app(
        EncoderServiceConfig(publish_backend="direct_engine", device="cpu"),
        encoder=encoder,
        direct_transfer_engine_factory=factory,
    )

    with TestClient(app) as client:
        response = client.post("/encode", json=_direct_encode_body())
        health = client.get("/health").json()["direct_transfer_engine"]

    assert response.status_code == 502
    assert "peer-buffer transfer failed: rc=-2" in response.text
    assert encoder.encode_calls == 1
    assert len(factory.engines) == 2
    assert [len(engine.fake_backend.calls) for engine in factory.engines] == [1, 1]
    assert health["publish_retries"] == 1
    assert health["recovery_successes"] == 0
    assert health["recovery_failures"] == 1


def test_online_encoder_does_not_retry_invalid_direct_target_contract():
    encoder = _DummyEncoder()
    factory = _TrackingTransferEngineFactory([0, 0])
    app = create_encoder_app(
        EncoderServiceConfig(publish_backend="direct_engine", device="cpu"),
        encoder=encoder,
        direct_transfer_engine_factory=factory,
    )
    body = _direct_encode_body()
    target = body["metadata"]["mooncake_epd_direct_feature_targets"][0]
    del target["remote_pointers"]["grid_thw"]

    with TestClient(app) as client:
        response = client.post("/encode", json=body)
        health = client.get("/health").json()["direct_transfer_engine"]

    assert response.status_code == 502
    assert "missing FeatureBundle peer-buffer targets" in response.text
    assert encoder.encode_calls == 1
    assert len(factory.engines) == 1
    assert health["publish_retries"] == 0
    assert health["engine_resets"] == 0


def test_direct_engine_incarnation_rotation_waits_for_all_inflight_leases():
    factory = _TrackingTransferEngineFactory([0, 0])
    coordinator = _DirectTransferEngineCoordinator(factory)
    first = coordinator.acquire(
        remote_session="prefill-session",
        remote_incarnation="prefill-process-a",
    )
    second = coordinator.acquire(
        remote_session="prefill-session",
        remote_incarnation="prefill-process-a",
    )
    assert first.generation == second.generation == 1
    assert coordinator.stats()["inflight"] == 2

    rotated = {}

    def _rotate():
        rotated["lease"] = coordinator.acquire(
            remote_session="prefill-session",
            remote_incarnation="prefill-process-b",
        )

    thread = threading.Thread(target=_rotate, daemon=True)
    thread.start()
    deadline = time.monotonic() + 2.0
    while not coordinator.stats()["rebuilding"] and time.monotonic() < deadline:
        time.sleep(0.005)

    assert coordinator.stats()["rebuilding"] is True
    assert factory.engines[0].shutdown_calls == 0
    first.release()
    time.sleep(0.02)
    assert thread.is_alive()
    assert factory.engines[0].shutdown_calls == 0
    second.release()
    thread.join(timeout=2.0)

    assert not thread.is_alive()
    assert factory.engines[0].shutdown_calls == 1
    assert rotated["lease"].generation == 2
    rotated["lease"].release()
    coordinator.shutdown()


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_proxy_feature_handle_mode_calls_online_encoder_when_handles_absent(tmp_path):
    record: dict = {}
    prefill_app = _build_prefill_app(record)
    decode_app = _build_decode_app(record)
    encoder_app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="file",
            store_dir=str(tmp_path / "feature-store"),
            device="cpu",
        ),
        encoder=_DummyEncoder(),
    )
    proxy_app = create_proxy_app(
        ProxyConfig(
            mm_prefetch_mode="feature_handle",
            prefill_supports_feature_handles=True,
            encoder_service_url="http://encoder.local",
        ),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
    )

    with TestClient(proxy_app) as client:
        proxy_app.state.encoder_client = httpx.AsyncClient(
            base_url="http://encoder.local",
            transport=httpx.ASGITransport(app=encoder_app),
            timeout=None,
        )
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "image_url", "image_url": {"url": _png_data_url()}},
                            {"type": "text", "text": "describe"},
                        ],
                    }
                ],
                "metadata": {"workflow_id": "wf-online-encoder"},
            },
        )
        assert response.status_code == 200, response.text
        prefill_kv = record["prefill_generate_body"]["sampling_params"]["extra_args"]["kv_transfer_params"]
        assert prefill_kv["mm_prefetch_policy"] == "feature_handle"
        assert prefill_kv["mm_feature_handles"][0]["uri"].startswith("file:")
        assert prefill_kv["mm_feature_handle_target_worker"] == "prefill-0"


def _pm(node_id: str) -> PagedKVManager:
    return PagedKVManager(
        page_size=4,
        num_layers=2,
        num_kv_heads=1,
        head_dim=2,
        dtype=torch.float32,
        device=torch.device("cpu"),
        node_id=node_id,
    )


def test_descriptor_shared_state_materializes_for_write_on_target_node():
    src = _pm("prefill-a")
    dst = _pm("decode-b")
    refs = src.allocate_pages(1, filled=4)
    key = torch.arange(16, dtype=torch.float32).reshape(2, 1, 4, 2)
    val = key + 100
    src.write_page_slots(refs[0], key, val)

    store = MooncakeKVStateStore(
        src,
        node_id="prefill-a",
        page_managers_by_node={"decode-b": dst},
        remote_materializer=MooncakeRemoteKVMaterializer(
            src,
            dst,
            transfer_engine=TransferEngine(protocol="local"),
        ),
        allow_remote_descriptor_sharing=True,
    )
    store.register_state(refs, workflow_id="wf", state_id="parent")
    store.clone_state(
        "parent",
        child_state_id="child",
        target_node_id="decode-b",
        share_remote_descriptor=True,
    )
    assert src.refcount(refs[0].global_block_id) == 2
    assert store.resolve_remote_refs("child", target_node_id="decode-b")[0].physical_id == refs[0].physical_id
    with pytest.raises(RuntimeError, match="materialized before write"):
        store.resolve_remote_refs("child", target_node_id="decode-b", for_write=True)

    materialized = store.materialize_for_write("child", target_node_id="decode-b")
    assert materialized.owner_node_id == "decode-b"
    assert not materialized.metadata.get("remote_descriptor_shared", False)
    assert src.refcount(refs[0].global_block_id) == 1
    child_refs = store.resolve_remote_refs("child", target_node_id="decode-b", for_write=True)
    assert child_refs[0].physical_node_id == "decode-b"
    moved_key, moved_val = dst.get_page_slice(child_refs[0])
    assert torch.equal(moved_key, key)
    assert torch.equal(moved_val, val)
    assert store.release_state("child") == 1
    assert store.release_state("parent") == 1

class _CopyingMooncakeEngine:
    def __init__(self):
        self.calls = []

    def batch_transfer_sync_write(self, remote_session, local_ptrs, remote_ptrs, lengths):
        import ctypes

        self.calls.append((remote_session, list(local_ptrs), list(remote_ptrs), list(lengths)))
        for src, dst, n in zip(local_ptrs, remote_ptrs, lengths):
            ctypes.memmove(int(dst), int(src), int(n))
        return 0

    def transfer_sync_write(self, remote_session, local_ptr, remote_ptr, length):
        import ctypes

        self.calls.append((remote_session, [int(local_ptr)], [int(remote_ptr)], [int(length)]))
        ctypes.memmove(int(remote_ptr), int(local_ptr), int(length))
        return 0


def test_direct_feature_buffer_service_allocates_and_releases():
    from mooncake_epd.core.state import DirectFeatureBufferRegistry
    from mooncake_epd.scripts.direct_feature_buffer_service import create_app as create_direct_app

    bundle = FeatureBundle(
        image_hash="img-direct",
        last_hidden=torch.ones((2, 3), dtype=torch.float32),
        intermediates=[(7, torch.ones((2, 3), dtype=torch.float32))],
        grid_thw=torch.tensor([[1, 1, 2]], dtype=torch.long),
    )
    registry = DirectFeatureBufferRegistry(
        worker_id="prefill-0",
        device="cpu",
        remote_session="prefill-session",
        register_memory=False,
    )
    app = create_direct_app(registry=registry)
    with TestClient(app) as client:
        response = client.post("/allocate", json={"descriptors": [bundle.descriptor().to_dict()]})
        assert response.status_code == 200, response.text
        target = response.json()["targets"][0]
        assert target["remote_session"] == "prefill-session"
        assert set(target["remote_pointers"]) >= {
            "last_hidden",
            "last_hidden:nbytes",
            "grid_thw",
            "grid_thw:nbytes",
            "intermediate:7:0",
            "intermediate:7:0:nbytes",
        }
        assert client.get("/stats").json()["allocations"] == 1
        released = client.post("/release", json={"feature_ids": ["img-direct"]})
        assert released.status_code == 200
        assert released.json()["stats"]["allocations"] == 0


def test_direct_feature_buffer_service_rolls_back_partial_batch_allocation(monkeypatch):
    from mooncake_epd.core.state import DirectFeatureBufferRegistry
    from mooncake_epd.scripts.direct_feature_buffer_service import create_app as create_direct_app

    registry = DirectFeatureBufferRegistry(
        worker_id="prefill-0",
        device="cpu",
        remote_session="prefill-session",
        register_memory=False,
    )
    first = FeatureBundle("first", torch.ones((2, 3), dtype=torch.float32))
    second = FeatureBundle("second", torch.ones((4, 3), dtype=torch.float32))
    original_allocate = registry.allocate_for_descriptor
    calls = 0

    def _allocate(descriptor, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("synthetic second allocation failure")
        return original_allocate(descriptor, **kwargs)

    monkeypatch.setattr(registry, "allocate_for_descriptor", _allocate)
    app = create_direct_app(registry=registry)
    with TestClient(app) as client:
        response = client.post(
            "/allocate",
            json={
                "descriptors": [
                    first.descriptor().to_dict(),
                    second.descriptor().to_dict(),
                ]
            },
        )
        stats = client.get("/stats").json()

    assert response.status_code == 400
    assert "second allocation failure" in response.text
    assert stats["allocations"] == 0
    assert stats["bytes"] == 0


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_proxy_online_encoder_direct_engine_handshake_materializes_prefill_buffers():
    from mooncake_epd.core.state import DirectFeatureBufferRegistry
    from mooncake_epd.scripts.direct_feature_buffer_service import create_app as create_direct_app

    record: dict = {}
    prefill_app = _build_prefill_app(record)
    decode_app = _build_decode_app(record)

    direct_engine = TransferEngine(protocol="tcp")
    fake_engine = _CopyingMooncakeEngine()
    direct_engine.bind_mooncake_backend(fake_engine, initialized=True, owns_backend=False)
    encoder_app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
        ),
        encoder=_DummyEncoder(),
        direct_transfer_engine=direct_engine,
    )
    registry = DirectFeatureBufferRegistry(
        worker_id="prefill-0",
        device="cpu",
        remote_session="prefill-session",
        register_memory=False,
    )
    direct_app = create_direct_app(registry=registry)
    proxy_app = create_proxy_app(
        ProxyConfig(
            mm_prefetch_mode="feature_handle",
            prefill_supports_feature_handles=True,
            encoder_service_url="http://encoder.local",
            prefill_direct_buffer_service_url="http://prefill-direct.local",
            release_direct_feature_buffers_after_prefill=False,
            enable_direct_feature_handle_cache=True,
            direct_feature_handle_cache_max_entries=8,
            direct_feature_handle_cache_max_bytes=1024 * 1024,
        ),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
    )

    with TestClient(proxy_app) as client:
        proxy_app.state.encoder_client = httpx.AsyncClient(
            base_url="http://encoder.local",
            transport=httpx.ASGITransport(app=encoder_app),
            timeout=None,
        )
        proxy_app.state.prefill_direct_buffer_client = httpx.AsyncClient(
            base_url="http://prefill-direct.local",
            transport=httpx.ASGITransport(app=direct_app),
            timeout=None,
        )
        request_payload = {
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "image_url", "image_url": {"url": _png_data_url()}},
                            {"type": "text", "text": "describe"},
                        ],
                    }
                ],
                "metadata": {"workflow_id": "wf-online-direct"},
            }
        response = client.post("/v1/chat/completions", json=request_payload)
        assert response.status_code == 200, response.text
        repeated = client.post("/v1/chat/completions", json=request_payload)
        assert repeated.status_code == 200, repeated.text
        cache_metrics = client.get("/metrics").json()["direct_feature_handle_cache"]
        assert cache_metrics["entries"] == 1
        assert cache_metrics["hits"] == 1
        assert cache_metrics["misses"] == 1
        assert registry.stats()["allocations"] == 1

        prefill_kv = record["prefill_generate_body"]["sampling_params"]["extra_args"]["kv_transfer_params"]
        handle_payload = prefill_kv["mm_feature_handles"][0]
        provider = FeatureHandleProvider(
            FeatureHandleProviderConfig(worker_id="prefill-0", device="cpu", strict=True)
        )
        resolved = provider.resolve_from_sources(
            {"mm_feature_handles": [handle_payload]},
            device="cpu",
            dtype=torch.float32,
        )
        assert resolved is not None
        assert tuple(resolved.image_embeds.shape) == (4, 16)
        assert torch.allclose(resolved.image_embeds[:, :8], torch.ones((4, 8)))
        assert torch.allclose(resolved.image_embeds[:, 8:], torch.full((4, 8), 2.0))

    assert prefill_kv["mm_prefetch_policy"] == "feature_handle"
    assert handle_payload["uri"].startswith("epd-direct://")
    assert handle_payload["metadata"]["backend"] == "direct_engine"
    assert handle_payload["metadata"]["direct_backend"] == "feature_peer_buffer_direct"
    assert len(fake_engine.calls) == 1
    assert fake_engine.calls[0][0] == "prefill-session"


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_proxy_failed_direct_publish_releases_ticket_and_prefill_allocation():
    from mooncake_epd.core.state import DirectFeatureBufferRegistry
    from mooncake_epd.scripts.direct_feature_buffer_service import create_app as create_direct_app

    record: dict = {}
    prefill_app = _build_prefill_app(record)
    decode_app = _build_decode_app(record)
    encoder_app = create_encoder_app(
        EncoderServiceConfig(
            publish_backend="direct_engine",
            device="cpu",
            enable_feature_bundle_cache=False,
        ),
        encoder=_DummyEncoder(),
        direct_transfer_engine_factory=_TrackingTransferEngineFactory([-1, -2]),
    )
    registry = DirectFeatureBufferRegistry(
        worker_id="prefill-0",
        device="cpu",
        remote_session="prefill-session",
        register_memory=False,
    )
    direct_app = create_direct_app(registry=registry)
    proxy_app = create_proxy_app(
        ProxyConfig(
            mm_prefetch_mode="feature_handle",
            prefill_supports_feature_handles=True,
            encoder_service_url="http://encoder.local",
            prefill_direct_buffer_service_url="http://prefill-direct.local",
            enable_direct_feature_handle_cache=False,
        ),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
    )

    with TestClient(encoder_app) as encoder_client, TestClient(proxy_app) as client:
        proxy_app.state.encoder_client = httpx.AsyncClient(
            base_url="http://encoder.local",
            transport=httpx.ASGITransport(app=encoder_app),
            timeout=None,
        )
        proxy_app.state.prefill_direct_buffer_client = httpx.AsyncClient(
            base_url="http://prefill-direct.local",
            transport=httpx.ASGITransport(app=direct_app),
            timeout=None,
        )
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "image_url", "image_url": {"url": _png_data_url()}},
                            {"type": "text", "text": "force publish failure"},
                        ],
                    }
                ],
                "metadata": {"workflow_id": "wf-direct-publish-failure"},
            },
        )
        health = encoder_client.get("/health").json()["pending_direct_bundles"]

    assert response.status_code == 502
    assert "encoder direct publish returned error" in response.text
    assert registry.stats()["allocations"] == 0
    assert health["tickets"] == 0
    assert health["retained_bytes"] == 0
    assert health["publish_failures"] == 1
    assert health["publish_failure_records"] == 1

@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_proxy_releases_direct_feature_buffers_after_prefill_consumes_them():
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    from mooncake_epd.core.state import DirectFeatureBufferRegistry
    from mooncake_epd.scripts.direct_feature_buffer_service import create_app as create_direct_app

    record: dict = {}
    prefill_app = FastAPI()

    @prefill_app.post("/v1/chat/completions/render")
    async def render_chat(request: Request):
        body = await request.json()
        record["prefill_render_body"] = body
        return JSONResponse(
            {
                "request_id": "rendered-prefill-direct-release",
                "token_ids": [1, 2, 3, 4],
                "sampling_params": {"temperature": 0.0, "top_p": 1.0, "max_tokens": 16, "min_tokens": 0},
                "model": "fake-model",
                "stream": False,
                "priority": 0,
            }
        )

    @prefill_app.post("/inference/v1/generate")
    async def generate(request: Request):
        body = await request.json()
        record["prefill_generate_body"] = body
        kv = dict((body.get("sampling_params") or {}).get("extra_args", {}).get("kv_transfer_params") or {})
        provider = FeatureHandleProvider(FeatureHandleProviderConfig(worker_id="prefill-0", device="cpu", strict=True))
        resolved = provider.resolve_from_sources(kv, device="cpu", dtype=torch.float32)
        assert resolved is not None
        record["resolved_image_embeds_shape"] = list(resolved.image_embeds.shape)
        kv.update(
            {
                "transfer_id": kv.get("transfer_id") or request.headers.get("X-Request-Id"),
                "remote_engine_id": "prefill-engine-0",
                "remote_bootstrap_addr": "http://prefill-bootstrap:8998",
                "remote_block_ids": [[11, 12, 13]],
            }
        )
        return JSONResponse(
            {
                "request_id": "prefill-response",
                "choices": [{"index": 0, "finish_reason": "length", "token_ids": []}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 0, "total_tokens": 10},
                "kv_transfer_params": kv,
            }
        )

    decode_app = _build_decode_app(record)
    direct_engine = TransferEngine(protocol="tcp")
    fake_engine = _CopyingMooncakeEngine()
    direct_engine.bind_mooncake_backend(fake_engine, initialized=True, owns_backend=False)
    encoder_app = create_encoder_app(
        EncoderServiceConfig(publish_backend="direct_engine", device="cpu"),
        encoder=_DummyEncoder(),
        direct_transfer_engine=direct_engine,
    )
    registry = DirectFeatureBufferRegistry(
        worker_id="prefill-0",
        device="cpu",
        remote_session="prefill-session",
        register_memory=False,
    )
    direct_app = create_direct_app(registry=registry)
    proxy_app = create_proxy_app(
        ProxyConfig(
            mm_prefetch_mode="feature_handle",
            prefill_supports_feature_handles=True,
            encoder_service_url="http://encoder.local",
            prefill_direct_buffer_service_url="http://prefill-direct.local",
        ),
        prefill_clients=[_client_override(prefill_app, "prefill-0", "prefill.local", 8100)],
        decode_clients=[_client_override(decode_app, "decode-0", "decode.local", 8200)],
    )

    with TestClient(proxy_app) as client:
        proxy_app.state.encoder_client = httpx.AsyncClient(
            base_url="http://encoder.local",
            transport=httpx.ASGITransport(app=encoder_app),
            timeout=None,
        )
        proxy_app.state.prefill_direct_buffer_client = httpx.AsyncClient(
            base_url="http://prefill-direct.local",
            transport=httpx.ASGITransport(app=direct_app),
            timeout=None,
        )
        response = client.post(
            "/v1/chat/completions",
            json={
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "image_url", "image_url": {"url": _png_data_url()}},
                            {"type": "text", "text": "describe"},
                        ],
                    }
                ],
                "metadata": {"workflow_id": "wf-online-direct-release"},
            },
        )
        assert response.status_code == 200, response.text

    assert record["resolved_image_embeds_shape"] == [4, 16]
    assert registry.stats()["allocations"] == 0
