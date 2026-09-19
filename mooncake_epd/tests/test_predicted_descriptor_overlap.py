from __future__ import annotations

import asyncio
import base64
import io
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

import pytest
import torch
import httpx
from fastapi.testclient import TestClient
from PIL import Image

from mooncake_epd.core.state import FeatureBundle, FeatureBundleDescriptor
from mooncake_epd.core.transfer.engine import FeatureBundlePeerBufferResult
from mooncake_epd.scripts.epd_encoder_service import EncoderServiceConfig, create_app


def _png_data_url(color=(17, 29, 43)) -> str:
    image = Image.new("RGB", (4, 4), color=color)
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("ascii")


def _describe_body(color=(17, 29, 43)) -> dict:
    return {
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": _png_data_url(color)}},
                    {"type": "text", "text": "describe this image"},
                ],
            }
        ]
    }


class _EventProcessor:
    def __init__(self) -> None:
        self.completed = threading.Event()
        self.calls = 0

    def apply_chat_template(self, *args, **kwargs):
        self.calls += 1
        inputs = {
            "pixel_values": torch.arange(12, dtype=torch.float32).reshape(3, 4),
            "image_grid_thw": torch.tensor([[1, 2, 2]], dtype=torch.long),
        }
        self.completed.set()
        return inputs


class _BlockingPredictedQwen3Encoder:
    """Deterministic EncoderWorker-compatible test double with real events/tensors."""

    qwen3_predicted_descriptor_overlap = True
    encoder_family = "qwen3_vl"
    model_fingerprint = "blocking-qwen3-model-fp"
    processor_fingerprint = "blocking-qwen3-processor-fp"

    def __init__(self, *, actual_variant: str = "match") -> None:
        self.processor = _EventProcessor()
        self.actual_variant = actual_variant
        self.encode_entered = threading.Event()
        self.release_encode = threading.Event()
        self.encode_finished = threading.Event()
        self.encode_calls = 0

    def predicted_descriptor(self, *, pixel_values, image_grid_thw, image_id=None, **kwargs):
        del pixel_values, kwargs
        return self._bundle(image_id or "img", variant="match", grid_thw=image_grid_thw).descriptor(
            checksum=False
        )

    # Alias accepted by implementations that prefer a verb-oriented capability.
    def predict_descriptor(self, *, pixel_values, image_grid_thw, image_id=None, **kwargs):
        return self.predicted_descriptor(
            pixel_values=pixel_values,
            image_grid_thw=image_grid_thw,
            image_id=image_id,
            **kwargs,
        )

    def encode(self, *, pixel_values, image_grid_thw, image_id=None, **kwargs):
        del pixel_values, kwargs
        self.encode_calls += 1
        self.encode_entered.set()
        if not self.release_encode.wait(timeout=0.75):
            raise TimeoutError("test encoder was not released")

        class Out:
            pass

        out = Out()
        out.encode_time_ms = 7.5
        out.bundle = self._bundle(
            image_id or "img",
            variant=self.actual_variant,
            grid_thw=image_grid_thw,
        )
        self.encode_finished.set()
        return out

    def _bundle(self, image_id: str, *, variant: str, grid_thw: torch.Tensor) -> FeatureBundle:
        hidden = torch.ones((4, 8), dtype=torch.float32)
        intermediates = [(8, torch.full((4, 8), 2.0, dtype=torch.float32))]
        model_fp = self.model_fingerprint
        processor_fp = self.processor_fingerprint
        if variant == "shape":
            hidden = torch.ones((5, 8), dtype=torch.float32)
        elif variant == "dtype":
            hidden = torch.ones((4, 8), dtype=torch.float16)
        elif variant == "nbytes":
            hidden = torch.ones((4, 4), dtype=torch.float64)
        elif variant == "layer":
            intermediates = [(9, torch.full((4, 8), 2.0, dtype=torch.float32))]
        elif variant == "fingerprint":
            model_fp = "unexpected-qwen3-model-fp"
            processor_fp = "unexpected-qwen3-processor-fp"
        elif variant != "match":
            raise AssertionError(f"unknown actual variant: {variant}")
        return FeatureBundle(
            image_hash=str(image_id),
            last_hidden=hidden,
            intermediates=intermediates,
            grid_thw=grid_thw.detach().clone(),
            metadata={
                "kind": "qwen_vl_hidden_state",
                "model_fingerprint": model_fp,
                "processor_fingerprint": processor_fp,
            },
        )


class _UnpredictableBlockingEncoder(_BlockingPredictedQwen3Encoder):
    qwen3_predicted_descriptor_overlap = False

    def predicted_descriptor(self, **kwargs):  # pragma: no cover - must not be called
        raise AssertionError("unpredictable worker must not be asked for predicted descriptors")

    def predict_descriptor(self, **kwargs):  # pragma: no cover - must not be called
        raise AssertionError("unpredictable worker must not be asked for predicted descriptors")


class _CompletedTupleMetadataEncoder(_UnpredictableBlockingEncoder):
    """Models the real worker's tuple-shaped descriptor metadata."""

    def _bundle(self, image_id: str, *, variant: str, grid_thw: torch.Tensor) -> FeatureBundle:
        bundle = super()._bundle(image_id, variant=variant, grid_thw=grid_thw)
        bundle.metadata["last_hidden_shape"] = tuple(int(dim) for dim in bundle.last_hidden.shape)
        bundle.metadata["deepstack_shapes"] = [
            tuple(int(dim) for dim in tensor.shape)
            for _layer, tensor in bundle.intermediates
        ]
        return bundle


class _NonFinitePredictedMetadataEncoder(_BlockingPredictedQwen3Encoder):
    def predicted_descriptor(self, **kwargs):
        descriptor = super().predicted_descriptor(**kwargs)
        payload = descriptor.to_dict()
        payload["metadata"]["invalid_nonfinite"] = float("nan")
        return FeatureBundleDescriptor.from_dict(payload)


class _RecordingDirectEngine:
    def __init__(self) -> None:
        self.plans = []
        self.transfers = []

    def initialize(self) -> None:
        return None

    def shutdown(self) -> None:
        return None

    def build_feature_bundle_peer_buffer_plan(
        self,
        bundle,
        *,
        remote_session: str,
        remote_pointers: dict,
        checksum: bool = False,
    ):
        descriptor = bundle.descriptor(checksum=checksum).to_dict()
        targets = []
        tensors = [("last_hidden", bundle.last_hidden)]
        if bundle.grid_thw is not None:
            tensors.append(("grid_thw", bundle.grid_thw))
        tensors.extend(
            (f"intermediate:{int(layer)}:{ordinal}", tensor)
            for ordinal, (layer, tensor) in enumerate(bundle.intermediates)
        )
        missing = [name for name, _tensor in tensors if name not in remote_pointers]
        if missing:
            raise ValueError(f"missing FeatureBundle peer-buffer targets: {missing}")
        for name, tensor in tensors:
            nbytes = int(tensor.nelement() * tensor.element_size())
            capacity = remote_pointers.get(f"{name}:nbytes")
            if capacity is not None and int(capacity) < nbytes:
                raise ValueError(f"undersized FeatureBundle peer-buffer target: {name}")
            targets.append({"name": name, "remote_pointer": int(remote_pointers[name]), "nbytes": nbytes})
        plan = type(
            "Plan",
            (),
            {
                "feature_id": str(bundle.image_hash),
                "remote_session": str(remote_session),
                "descriptor": descriptor,
                "targets": tuple(
                    type("Target", (), target) for target in targets
                ),
            },
        )()
        self.plans.append(plan)
        return plan

    def transfer_feature_bundle_peer_buffer_plan(self, bundle, plan, *, source_memory_mode=None):
        del source_memory_mode
        self.transfers.append((bundle, plan))
        return FeatureBundlePeerBufferResult(
            feature_id=plan.feature_id,
            nbytes=sum(int(target.nbytes) for target in plan.targets),
            tensor_count=len(plan.targets),
            descriptor_count=len(plan.targets),
            backend_label="test_cpu_peer_buffer_direct",
        )


class _BlockingDirectEngine(_RecordingDirectEngine):
    def __init__(self) -> None:
        super().__init__()
        self.transfer_entered = threading.Event()
        self.release_transfer = threading.Event()

    def transfer_feature_bundle_peer_buffer_plan(
        self,
        bundle,
        plan,
        *,
        source_memory_mode=None,
    ):
        self.transfer_entered.set()
        if not self.release_transfer.wait(timeout=1.5):
            raise TimeoutError("test direct transfer was not released")
        return super().transfer_feature_bundle_peer_buffer_plan(
            bundle,
            plan,
            source_memory_mode=source_memory_mode,
        )


def _config(**overrides) -> EncoderServiceConfig:
    values = {
        "publish_backend": "direct_engine",
        "device": "cpu",
        "checksum": False,
        "enable_feature_bundle_cache": True,
        "direct_ticket_ttl_s": 30.0,
        "enable_qwen3_predicted_descriptor_overlap": True,
    }
    values.update(overrides)
    return EncoderServiceConfig(**values)


def _target_for_descriptor(descriptor: dict) -> dict:
    pointers = {"last_hidden": 101, "last_hidden:nbytes": descriptor["last_hidden"]["nbytes"]}
    if descriptor.get("grid_thw") is not None:
        pointers["grid_thw"] = 102
        pointers["grid_thw:nbytes"] = descriptor["grid_thw"]["nbytes"]
    for ordinal, item in enumerate(descriptor.get("intermediates") or []):
        name = f"intermediate:{int(item['layer'])}:{ordinal}"
        pointers[name] = 200 + ordinal
        pointers[f"{name}:nbytes"] = item["spec"]["nbytes"]
    return {
        "feature_id": descriptor["feature_id"],
        "worker_id": "prefill-0",
        "remote_session": "prefill-session-0",
        "remote_incarnation": "prefill-incarnation-0",
        "remote_pointers": pointers,
        "descriptor": descriptor,
        "target_memory_mode": "managed_buffer",
    }


def _assert_event(event: threading.Event, timeout: float, message: str) -> None:
    assert event.wait(timeout=timeout), message


def _wait_until(predicate, *, timeout_s: float = 1.0) -> dict:
    deadline = time.monotonic() + timeout_s
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(0.01)
    raise AssertionError(f"condition did not become true; last={last!r}")


def _post_describe_expecting_overlap(client: TestClient, encoder: _BlockingPredictedQwen3Encoder, body: dict):
    """Return /describe only if it overlaps encode; fail fast if old path blocks."""

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(client.post, "/describe", json=body)
        try:
            return future.result(timeout=0.3)
        except FutureTimeout:
            encoder.release_encode.set()
            try:
                future.result(timeout=1.0)
            except Exception:
                pass
            raise


def test_describe_returns_predicted_descriptor_while_vit_encode_is_inflight_on_cold_miss():
    encoder = _BlockingPredictedQwen3Encoder()
    app = create_app(_config(), encoder=encoder, direct_transfer_engine=_RecordingDirectEngine())

    with TestClient(app) as client:
        try:
            response = _post_describe_expecting_overlap(client, encoder, _describe_body())
            health = client.get("/health").json()

            assert response.status_code == 200, response.text
            body = response.json()
            _assert_event(encoder.processor.completed, 0.1, "processor must complete before predicted response")
            _assert_event(encoder.encode_entered, 0.1, "ViT encode should already be in flight")
            assert not encoder.encode_finished.is_set()
            assert body["descriptor_state"] == "predicted"
            assert body["vit_inflight"] is True
            assert body["reservation_bytes"] > 0
            assert FeatureBundleDescriptor.from_dict(body["descriptors"][0]).nbytes == body["reservation_bytes"]
            assert health["pending_direct_bundles"]["tickets"] == 1
            assert health["pending_direct_bundles"]["retained_bytes"] == body["reservation_bytes"]
        finally:
            encoder.release_encode.set()


def test_publish_direct_waits_for_inflight_encode_and_publishes_validated_actual_bundle():
    encoder = _BlockingPredictedQwen3Encoder()
    engine = _RecordingDirectEngine()
    app = create_app(_config(), encoder=encoder, direct_transfer_engine=engine)

    with TestClient(app) as client, ThreadPoolExecutor(max_workers=1) as pool:
        described = _post_describe_expecting_overlap(client, encoder, _describe_body())
        assert described.status_code == 200, described.text
        descriptor = described.json()["descriptors"][0]
        target = _target_for_descriptor(descriptor)
        future = pool.submit(
            client.post,
            "/publish_direct",
            json={
                "ticket": described.json()["ticket"],
                "mooncake_epd_direct_feature_targets": [target],
            },
        )
        _assert_event(encoder.encode_entered, 0.2, "publish should start the pending ViT encode")
        with pytest.raises(FutureTimeout):
            future.result(timeout=0.15)
        encoder.release_encode.set()
        published = future.result(timeout=1.0)
        health = client.get("/health").json()["pending_direct_bundles"]

    assert published.status_code == 200, published.text
    handle = published.json()["handles"][0]
    assert handle["descriptor"] == descriptor
    assert handle["metadata"]["direct_backend"] == "test_cpu_peer_buffer_direct"
    assert len(engine.transfers) == 1
    assert health["tickets"] == 0
    assert health["retained_bytes"] == 0


@pytest.mark.parametrize("variant", ["shape", "dtype", "nbytes", "layer", "fingerprint"])
def test_publish_direct_rejects_actual_bundle_that_does_not_match_predicted_descriptor(variant: str):
    encoder = _BlockingPredictedQwen3Encoder(actual_variant=variant)
    engine = _RecordingDirectEngine()
    app = create_app(_config(), encoder=encoder, direct_transfer_engine=engine)

    with TestClient(app) as client:
        described = _post_describe_expecting_overlap(client, encoder, _describe_body())
        assert described.status_code == 200, described.text
        descriptor = described.json()["descriptors"][0]
        target = _target_for_descriptor(descriptor)
        encoder.release_encode.set()
        published = client.post(
            "/publish_direct",
            json={
                "ticket": described.json()["ticket"],
                "mooncake_epd_direct_feature_targets": [target],
            },
        )
        full_health = client.get("/health").json()
        health = full_health["pending_direct_bundles"]

    assert published.status_code in {400, 409, 422, 502}, published.text
    assert "descriptor" in published.text.lower() or "mismatch" in published.text.lower()
    assert engine.transfers == []
    assert health["tickets"] == 0
    assert health["retained_bytes"] == 0
    overlap = full_health["qwen3_predicted_descriptor_overlap"]
    assert overlap["validation_failures"] == 1
    assert overlap["compute_failures"] == 0


@pytest.mark.parametrize(
    "target_variant",
    [
        "feature_id",
        "descriptor",
        "descriptor_metadata",
        "descriptor_extra_field",
        "descriptor_bool_shape",
        "descriptor_numeric_string",
        "descriptor_missing_null",
        "missing_pointer",
        "wrong_capacity",
        "extra_pointer",
    ],
)
def test_publish_direct_rejects_target_that_does_not_match_allocated_descriptor(
    target_variant: str,
):
    encoder = _BlockingPredictedQwen3Encoder()
    engine = _RecordingDirectEngine()
    app = create_app(_config(), encoder=encoder, direct_transfer_engine=engine)

    with TestClient(app) as client:
        described = _post_describe_expecting_overlap(client, encoder, _describe_body())
        descriptor = described.json()["descriptors"][0]
        target = _target_for_descriptor(descriptor)
        if target_variant == "feature_id":
            target["feature_id"] = "different-feature"
        elif target_variant == "descriptor":
            target["descriptor"] = dict(descriptor)
            target["descriptor"]["feature_id"] = "different-feature"
        elif target_variant == "descriptor_metadata":
            target["descriptor"] = json.loads(json.dumps(descriptor))
            target["descriptor"]["metadata"]["kind"] = "tampered-kind"
        elif target_variant == "descriptor_extra_field":
            target["descriptor"] = json.loads(json.dumps(descriptor))
            target["descriptor"]["unexpected"] = "must-fail-exact-wire"
        elif target_variant == "descriptor_bool_shape":
            target["descriptor"] = json.loads(json.dumps(descriptor))
            target["descriptor"]["last_hidden"]["shape"][0] = True
        elif target_variant == "descriptor_numeric_string":
            target["descriptor"] = json.loads(json.dumps(descriptor))
            target["descriptor"]["last_hidden"]["nbytes"] = str(
                target["descriptor"]["last_hidden"]["nbytes"]
            )
        elif target_variant == "descriptor_missing_null":
            target["descriptor"] = json.loads(json.dumps(descriptor))
            target["descriptor"]["last_hidden"].pop("checksum")
        elif target_variant == "missing_pointer":
            target["remote_pointers"].pop("last_hidden")
        elif target_variant == "wrong_capacity":
            target["remote_pointers"]["last_hidden:nbytes"] += 1
        elif target_variant == "extra_pointer":
            target["remote_pointers"]["unexpected"] = 999
        encoder.release_encode.set()
        published = client.post(
            "/publish_direct",
            json={
                "ticket": described.json()["ticket"],
                "mooncake_epd_direct_feature_targets": [target],
            },
        )
        health = client.get("/health").json()["pending_direct_bundles"]

    assert published.status_code == 409, published.text
    assert "target validation failed" in published.text
    assert engine.transfers == []
    assert health["tickets"] == 0
    assert health["retained_bytes"] == 0


def test_completed_descriptor_accepts_lossless_json_tuple_to_list_round_trip():
    encoder = _CompletedTupleMetadataEncoder()
    engine = _RecordingDirectEngine()
    app = create_app(
        _config(),
        encoder=encoder,
        direct_transfer_engine=engine,
    )

    with TestClient(app) as client, ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(client.post, "/describe", json=_describe_body())
        _assert_event(
            encoder.encode_entered,
            0.2,
            "completed descriptor path must finish ViT before responding",
        )
        encoder.release_encode.set()
        described = future.result(timeout=1.0)
        assert described.status_code == 200, described.text
        body = described.json()
        assert body["descriptor_state"] == "completed"
        descriptor = body["descriptors"][0]
        assert isinstance(descriptor["metadata"]["last_hidden_shape"], list)
        target = _target_for_descriptor(json.loads(json.dumps(descriptor)))
        published = client.post(
            "/publish_direct",
            json={
                "ticket": body["ticket"],
                "mooncake_epd_direct_feature_targets": [target],
            },
        )
        health = client.get("/health").json()["pending_direct_bundles"]

    assert published.status_code == 200, published.text
    assert len(engine.transfers) == 1
    assert health["tickets"] == 0
    assert health["retained_bytes"] == 0


def test_nonfinite_predicted_metadata_falls_back_without_issuing_predicted_ticket():
    encoder = _NonFinitePredictedMetadataEncoder()
    app = create_app(
        _config(),
        encoder=encoder,
        direct_transfer_engine=_RecordingDirectEngine(),
    )

    with TestClient(app) as client, ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(client.post, "/describe", json=_describe_body())
        _assert_event(
            encoder.encode_entered,
            0.5,
            "invalid prediction must fall back to completed ViT path",
        )
        encoder.release_encode.set()
        described = future.result(timeout=1.0)
        discarded = client.post(
            "/discard_direct",
            json={"ticket": described.json()["ticket"]},
        )
        health = client.get("/health").json()

    assert described.status_code == 200, described.text
    body = described.json()
    assert body["descriptor_state"] == "completed"
    assert "invalid_nonfinite" not in body["descriptors"][0]["metadata"]
    assert discarded.status_code == 200, discarded.text
    assert health["qwen3_predicted_descriptor_overlap"]["fallbacks"] == 1
    assert health["qwen3_predicted_descriptor_overlap"]["created"] == 0
    assert health["pending_direct_bundles"]["tickets"] == 0


def test_publish_cancellation_keeps_reservation_until_native_transfer_finishes():
    async def _run() -> tuple[dict, dict, _BlockingDirectEngine]:
        encoder = _BlockingPredictedQwen3Encoder()
        engine = _BlockingDirectEngine()
        app = create_app(_config(), encoder=encoder, direct_transfer_engine=engine)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                base_url="http://encoder.local",
                transport=httpx.ASGITransport(app=app),
                timeout=None,
            ) as client:
                described = await client.post("/describe", json=_describe_body())
                assert described.status_code == 200, described.text
                descriptor = described.json()["descriptors"][0]
                encoder.release_encode.set()
                publish_task = asyncio.create_task(
                    client.post(
                        "/publish_direct",
                        json={
                            "ticket": described.json()["ticket"],
                            "mooncake_epd_direct_feature_targets": [
                                _target_for_descriptor(descriptor)
                            ],
                        },
                    )
                )
                assert await asyncio.to_thread(engine.transfer_entered.wait, 1.0)
                publish_task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await publish_task
                during = (await client.get("/health")).json()[
                    "pending_direct_bundles"
                ]
                engine.release_transfer.set()
                deadline = time.monotonic() + 1.0
                final = during
                while time.monotonic() < deadline:
                    final = (await client.get("/health")).json()[
                        "pending_direct_bundles"
                    ]
                    if final["tickets"] == 0:
                        break
                    await asyncio.sleep(0.01)
                return during, final, engine

    during, final, engine = asyncio.run(_run())
    assert during["tickets"] == 1
    assert during["retained_bytes"] > 0
    assert during["publish_cancellations"] == 1
    assert final["tickets"] == 0
    assert final["retained_bytes"] == 0
    assert len(engine.transfers) == 1


def test_discard_direct_does_not_release_predicted_reservation_until_inflight_encode_finishes():
    encoder = _BlockingPredictedQwen3Encoder()
    app = create_app(_config(), encoder=encoder, direct_transfer_engine=_RecordingDirectEngine())

    with TestClient(app) as client:
        described = _post_describe_expecting_overlap(client, encoder, _describe_body())
        assert described.status_code == 200, described.text
        ticket = described.json()["ticket"]
        reservation_bytes = described.json()["reservation_bytes"]
        cleanup = client.post("/discard_direct", json={"ticket": ticket})
        during = client.get("/health").json()["pending_direct_bundles"]
        encoder.release_encode.set()
        final = _wait_until(
            lambda: (
                stats
                if (stats := client.get("/health").json()["pending_direct_bundles"])["retained_bytes"] == 0
                else None
            ),
            timeout_s=1.0,
        )

    assert cleanup.status_code == 200, cleanup.text
    assert cleanup.json()["discarded"] == [ticket]
    assert during["tickets"] == 1
    assert during["retained_bytes"] == reservation_bytes
    assert final["tickets"] == 0
    assert final["retained_bytes"] == 0


def test_abandoned_validation_mismatch_is_not_counted_as_compute_failure():
    encoder = _BlockingPredictedQwen3Encoder(actual_variant="shape")
    app = create_app(
        _config(),
        encoder=encoder,
        direct_transfer_engine=_RecordingDirectEngine(),
    )

    with TestClient(app) as client:
        described = _post_describe_expecting_overlap(client, encoder, _describe_body())
        cleanup = client.post(
            "/discard_direct",
            json={"ticket": described.json()["ticket"]},
        )
        encoder.release_encode.set()
        final = _wait_until(
            lambda: (
                health
                if (health := client.get("/health").json())[
                    "pending_direct_bundles"
                ]["tickets"]
                == 0
                else None
            ),
            timeout_s=1.0,
        )

    assert cleanup.status_code == 200
    overlap = final["qwen3_predicted_descriptor_overlap"]
    assert overlap["validation_failures"] == 1
    assert overlap["compute_failures"] == 0


def test_ttl_sweeper_does_not_release_predicted_reservation_until_inflight_encode_finishes():
    encoder = _BlockingPredictedQwen3Encoder()
    app = create_app(
        _config(direct_ticket_ttl_s=0.05),
        encoder=encoder,
        direct_transfer_engine=_RecordingDirectEngine(),
    )

    with TestClient(app) as client:
        described = _post_describe_expecting_overlap(client, encoder, _describe_body())
        assert described.status_code == 200, described.text
        reservation_bytes = described.json()["reservation_bytes"]
        time.sleep(0.12)
        during = client.get("/health").json()["pending_direct_bundles"]
        encoder.release_encode.set()
        final = _wait_until(
            lambda: (
                stats
                if (stats := client.get("/health").json()["pending_direct_bundles"])["retained_bytes"] == 0
                else None
            ),
            timeout_s=1.0,
        )

    assert during["tickets"] == 1
    assert during["retained_bytes"] == reservation_bytes
    assert final["tickets"] == 0
    assert final["retained_bytes"] == 0
    assert final["expired"] == 1


@pytest.mark.parametrize(
    "encoder, checksum",
    [(_BlockingPredictedQwen3Encoder(), True), (_UnpredictableBlockingEncoder(), False)],
)
def test_checksum_or_unpredictable_worker_falls_back_to_completed_descriptor_path(encoder, checksum: bool):
    app = create_app(
        _config(checksum=checksum),
        encoder=encoder,
        direct_transfer_engine=_RecordingDirectEngine(),
    )

    with TestClient(app) as client, ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(client.post, "/describe", json=_describe_body())
        _assert_event(encoder.encode_entered, 0.2, "fallback path must run full ViT encode before responding")
        with pytest.raises(FutureTimeout):
            future.result(timeout=0.15)
        encoder.release_encode.set()
        response = future.result(timeout=1.0)

    assert response.status_code == 200, response.text
    body = response.json()
    assert encoder.encode_finished.is_set()
    assert body["descriptor_state"] == "completed"
    assert body["vit_inflight"] is False
    assert body["reservation_bytes"] == body["descriptors"][0]["nbytes"]
    checksum_value = body["descriptors"][0]["last_hidden"]["checksum"]
    assert (checksum_value is not None) is bool(checksum)
