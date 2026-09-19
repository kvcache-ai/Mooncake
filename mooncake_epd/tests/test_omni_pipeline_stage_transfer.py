from __future__ import annotations

import torch

import ctypes
import pytest

from mooncake_epd.core.omni_pipeline import (
    OmniPipeline,
    OmniPipelineProcessRuntime,
    OmniPipelineRuntime,
    OmniRemoteTensorRef,
    OmniStageWorkerSpec,
)
from mooncake_epd.core.state import FeatureBundle
from mooncake_epd.core.transfer import Channel, Mode, TransferEngine, TransferPolicy


class _FakeMooncakeDirectEngine:
    def __init__(self):
        self._next_ptr = 10_000
        self.buffers = {}
        self.transfer_calls = []
        self.registered = set()

    def allocate_managed_buffer(self, size):
        ptr = self._next_ptr
        self._next_ptr += max(1, int(size)) + 4096
        self.buffers[ptr] = bytearray(max(1, int(size)))
        return ptr

    def free_managed_buffer(self, ptr, size):
        self.buffers.pop(int(ptr), None)
        return 0

    def write_bytes_to_buffer(self, ptr, payload, nbytes):
        if isinstance(payload, str):
            payload = payload.encode("latin1")
        self.buffers[int(ptr)][: int(nbytes)] = bytes(payload)[: int(nbytes)]
        return 0

    def read_bytes_from_buffer(self, ptr, nbytes):
        return bytes(self.buffers[int(ptr)][: int(nbytes)])

    def register_memory(self, ptr, nbytes):
        self.registered.add((int(ptr), int(nbytes)))
        return 0

    def unregister_memory(self, ptr):
        self.registered = {item for item in self.registered if item[0] != int(ptr)}
        return 0

    def transfer_sync_write(self, remote_session, src_ptr, dst_ptr, length):
        self.transfer_calls.append((remote_session, [int(src_ptr)], [int(dst_ptr)], [int(length)]))
        if int(src_ptr) in self.buffers and int(dst_ptr) in self.buffers:
            self.buffers[int(dst_ptr)][: int(length)] = self.buffers[int(src_ptr)][: int(length)]
        elif int(dst_ptr) in self.buffers:
            self.buffers[int(dst_ptr)][: int(length)] = ctypes.string_at(int(src_ptr), int(length))
        elif int(src_ptr) in self.buffers:
            ctypes.memmove(int(dst_ptr), bytes(self.buffers[int(src_ptr)][: int(length)]), int(length))
        else:
            ctypes.memmove(int(dst_ptr), int(src_ptr), int(length))
        return 0

    def batch_transfer_sync_write(self, remote_session, src_ptrs, dst_ptrs, lengths):
        self.transfer_calls.append((remote_session, list(src_ptrs), list(dst_ptrs), list(lengths)))
        for src, dst, length in zip(src_ptrs, dst_ptrs, lengths):
            if int(src) in self.buffers and int(dst) in self.buffers:
                self.buffers[int(dst)][: int(length)] = self.buffers[int(src)][: int(length)]
            elif int(dst) in self.buffers:
                self.buffers[int(dst)][: int(length)] = ctypes.string_at(int(src), int(length))
            elif int(src) in self.buffers:
                ctypes.memmove(int(dst), bytes(self.buffers[int(src)][: int(length)]), int(length))
            else:
                ctypes.memmove(int(dst), int(src), int(length))
        return 0


def test_omni_worker_edge_uses_cpu_shm_for_same_host_stage_transfer():
    class AR:
        name = "AR"

        def run(self, inputs):
            return [
                torch.tensor([1.0, 2.0], dtype=torch.float32),
                FeatureBundle(
                    image_hash="img-stage",
                    last_hidden=torch.tensor([[3.0, 4.0]], dtype=torch.float32),
                    intermediates=[],
                    grid_thw=torch.tensor([[1, 1, 1]], dtype=torch.int64),
                ),
            ]

    class Generation:
        name = "Generation"

        def run(self, inputs):
            tensor, bundle = inputs
            assert tensor.is_shared()
            assert bundle.last_hidden.is_shared()
            assert bundle.grid_thw is not None and bundle.grid_thw.is_shared()
            return [tensor + bundle.last_hidden.flatten()]

    pipe = OmniPipeline(
        [AR(), Generation()],
        transfer=TransferEngine(protocol="local"),
        device_per_stage=["cpu", "cpu"],
        worker_per_stage=[
            OmniStageWorkerSpec("AR", worker_id="ar-0", device="cpu", transport_backend="shm"),
            OmniStageWorkerSpec("Generation", worker_id="gen-0", device="cpu", transport_backend="shm"),
        ],
        policy_per_edge=[
            TransferPolicy(Mode.SHM, channel=Channel.AGENT_TO_AGENT),
        ],
    )

    out = pipe.process([])

    assert torch.equal(out[0], torch.tensor([4.0, 6.0]))
    edge = pipe.stats()["edges"]["AR->Generation"]
    assert edge["bytes"] > 0
    assert edge["backend_counts"]["shm"] >= 2
    assert edge["fallback_count"] == 0


def test_omni_worker_edge_uses_mooncake_direct_peer_buffer_when_requested():
    class Generation:
        name = "Generation"

        def run(self, inputs):
            return [inputs[0] + 1]

    class Diffusion:
        name = "Diffusion"

        def run(self, inputs):
            return [inputs[0] * 2]

    fake = _FakeMooncakeDirectEngine()
    engine = TransferEngine(protocol="tcp", local_hostname="127.0.0.1")
    engine.bind_mooncake_backend(fake, initialized=True, owns_backend=False)

    pipe = OmniPipeline(
        [
            type("AR", (), {"name": "AR", "run": lambda self, inputs: [torch.arange(6, dtype=torch.float32)]})(),
            Generation(),
            Diffusion(),
        ],
        transfer=engine,
        device_per_stage=["cpu", "cpu", "cpu"],
        worker_per_stage=[
            {"stage_name": "AR", "worker_id": "ar-0", "device": "cpu", "transport_backend": "mooncake_engine_direct"},
            {"stage_name": "Generation", "worker_id": "gen-0", "device": "cpu", "transport_backend": "mooncake_engine_direct"},
            {"stage_name": "Diffusion", "worker_id": "diff-0", "device": "cpu", "transport_backend": "mooncake_engine_direct"},
        ],
        policy_per_edge=[
            TransferPolicy(
                Mode.PUSH_BATCH,
                channel=Channel.AGENT_TO_AGENT,
                extra={
                    "transport_backend": "mooncake_engine_direct",
                    "source_memory_mode": "managed_buffer",
                    "strict_no_fallback": True,
                },
            ),
            TransferPolicy(
                Mode.PUSH_BATCH,
                channel=Channel.AGENT_TO_AGENT,
                extra={
                    "transport_backend": "mooncake_engine_direct",
                    "source_memory_mode": "managed_buffer",
                    "strict_no_fallback": True,
                },
            ),
        ],
    )

    out = pipe.process([])

    assert torch.equal(out[0], (torch.arange(6, dtype=torch.float32) + 1) * 2)
    assert len(fake.transfer_calls) == 2
    stats = pipe.stats()["edges"]
    assert stats["AR->Generation"]["backend_counts"] == {"mooncake_engine_direct:tcp:local_dst_tensor": 1}
    assert stats["Generation->Diffusion"]["backend_counts"] == {"mooncake_engine_direct:tcp:local_dst_tensor": 1}
    assert stats["AR->Generation"]["path_counts"] == {"local_destination_tensor": 1}
    assert stats["AR->Generation"]["protocol_counts"] == {"tcp": 1}
    assert stats["AR->Generation"]["remote_pointer_count"] == 1
    assert stats["AR->Generation"]["remote_sessions"]


def test_mooncake_direct_remote_peer_buffer_returns_descriptor_and_stats():
    payload = torch.arange(4, dtype=torch.float32)
    nbytes = payload.nelement() * payload.element_size()
    fake = _FakeMooncakeDirectEngine()
    remote_ptr = fake.allocate_managed_buffer(nbytes)
    engine = TransferEngine(protocol="tcp", local_hostname="127.0.0.1")
    engine.bind_mooncake_backend(fake, initialized=True, owns_backend=False)

    class AR:
        name = "AR"

        def run(self, inputs):
            return [payload]

    class Generation:
        name = "Generation"

        def run(self, inputs):
            ref = inputs[0]
            assert isinstance(ref, OmniRemoteTensorRef)
            assert ref.remote_session == "peer-host:12345"
            assert ref.remote_pointer == remote_ptr
            assert ref.nbytes == nbytes
            assert ref.owner_worker_id == "gen-0"
            return [ref]

    pipe = OmniPipeline(
        [AR(), Generation()],
        transfer=engine,
        device_per_stage=["cpu", "cpu"],
        worker_per_stage=[
            {"stage_name": "AR", "worker_id": "ar-0", "device": "cpu", "transport_backend": "mooncake_engine_direct"},
            {"stage_name": "Generation", "worker_id": "gen-0", "device": "cpu", "transport_backend": "mooncake_engine_direct"},
        ],
        policy_per_edge=[
            TransferPolicy(
                Mode.PUSH_BATCH,
                channel=Channel.AGENT_TO_AGENT,
                extra={
                    "transport_backend": "mooncake_engine_direct",
                    "remote_session": "peer-host:12345",
                    "peer_buffer_addr": remote_ptr,
                    "peer_buffer_nbytes": nbytes,
                    "source_memory_mode": "registered_tensor",
                    "strict_no_fallback": True,
                    "require_remote_peer_buffer": True,
                },
            ),
        ],
    )

    out = pipe.process([])

    assert isinstance(out[0], OmniRemoteTensorRef)
    assert bytes(fake.buffers[remote_ptr][:nbytes]) == payload.numpy().tobytes()
    edge = pipe.stats()["edges"]["AR->Generation"]
    assert edge["backend_counts"] == {"mooncake_engine_direct:tcp:remote_peer_buffer": 1}
    assert edge["path_counts"] == {"remote_peer_buffer": 1}
    assert edge["protocol_counts"] == {"tcp": 1}
    assert edge["source_memory_mode_counts"] == {"registered_tensor": 1}
    assert edge["remote_sessions"] == ["peer-host:12345"]
    assert edge["remote_pointer_count"] == 1
    assert edge["remote_pointers_sample"] == [remote_ptr]
    assert edge["fallback_count"] == 0


def test_strict_mooncake_direct_requires_remote_buffer_nbytes_and_valid_backend():
    class AR:
        name = "AR"

        def run(self, inputs):
            return [torch.arange(8, dtype=torch.float32)]

    class Generation:
        name = "Generation"

        def run(self, inputs):
            return inputs

    fake = _FakeMooncakeDirectEngine()
    remote_ptr = fake.allocate_managed_buffer(4)
    engine = TransferEngine(protocol="tcp", local_hostname="127.0.0.1")
    engine.bind_mooncake_backend(fake, initialized=True, owns_backend=False)
    pipe = OmniPipeline(
        [AR(), Generation()],
        transfer=engine,
        device_per_stage=["cpu", "cpu"],
        policy_per_edge=[
            TransferPolicy(
                Mode.PUSH_BATCH,
                channel=Channel.AGENT_TO_AGENT,
                extra={
                    "transport_backend": "mooncake_engine_direct",
                    "remote_session": "peer-host:12345",
                    "peer_buffer_addr": remote_ptr,
                    "peer_buffer_nbytes": 4,
                    "strict_no_fallback": True,
                },
            ),
        ],
    )
    with pytest.raises(RuntimeError, match="remote peer buffer is smaller"):
        pipe.process([])

    bad_backend = OmniPipeline(
        [AR(), Generation()],
        transfer=TransferEngine(protocol="local"),
        device_per_stage=["cpu", "cpu"],
        policy_per_edge=[
            TransferPolicy(
                Mode.PUSH_BATCH,
                channel=Channel.AGENT_TO_AGENT,
                extra={"transport_backend": "mooncake_engine_direct", "strict_no_fallback": True},
            ),
        ],
    )
    with pytest.raises(RuntimeError, match="protocol 'tcp' or 'rdma'"):
        bad_backend.process([])


def test_omni_pipeline_runtime_reports_worker_level_backend_counts():
    class AR:
        name = "AR"

        def run(self, inputs):
            return [torch.tensor([7], dtype=torch.int64)]

    class Generation:
        name = "Generation"

        def run(self, inputs):
            return [inputs[0] + 35]

    pipe = OmniPipeline(
        [AR(), Generation()],
        transfer=TransferEngine(protocol="local"),
        device_per_stage=["cpu", "cpu"],
        policy_per_edge=[TransferPolicy(Mode.SHM, channel=Channel.AGENT_TO_AGENT)],
    )
    runtime = OmniPipelineRuntime(pipe, queue_size=1, worker_name_prefix="omni-test")
    try:
        out = runtime.run([], timeout=5.0)
    finally:
        runtime.stop()

    assert torch.equal(out[0], torch.tensor([42], dtype=torch.int64))
    edge = runtime.stats()["edges"]["AR->Generation"]
    assert edge["backend_counts"]["shm"] == 1
    assert edge["fallback_count"] == 0


def test_strict_explicit_transfer_fails_fast_without_device_topology():
    class AR:
        name = "AR"

        def run(self, inputs):
            return [torch.tensor([1.0])]

    class Generation:
        name = "Generation"

        def run(self, inputs):
            return inputs

    with pytest.raises(ValueError, match="complete worker_per_stage devices"):
        OmniPipeline(
            [AR(), Generation()],
            transfer=TransferEngine(protocol="local"),
            worker_per_stage=[
                {"stage_name": "AR", "worker_id": "ar-0", "transport_backend": "shm"},
                {"stage_name": "Generation", "worker_id": "gen-0", "transport_backend": "shm"},
            ],
            policy_per_edge=[
                TransferPolicy(Mode.SHM, channel=Channel.AGENT_TO_AGENT, extra={"strict_no_fallback": True}),
            ],
        )


class _ProcAR:
    name = "AR"

    def run(self, inputs):
        return [torch.tensor([2.0, 3.0])]


class _ProcGeneration:
    name = "Generation"

    def run(self, inputs):
        tensor = inputs[0]
        assert tensor.is_shared()
        return [tensor + 5.0]


class _ProcDiffusion:
    name = "Diffusion"

    def run(self, inputs):
        return [inputs[0] * 2.0]


def test_process_runtime_runs_stage_workers_in_separate_processes_with_shm():
    pipe = OmniPipeline(
        [_ProcAR(), _ProcGeneration(), _ProcDiffusion()],
        transfer=TransferEngine(protocol="local"),
        device_per_stage=["cpu", "cpu", "cpu"],
        policy_per_edge=[
            TransferPolicy(Mode.SHM, channel=Channel.AGENT_TO_AGENT),
            TransferPolicy(Mode.SHM, channel=Channel.AGENT_TO_AGENT),
        ],
    )
    runtime = OmniPipelineProcessRuntime(pipe, queue_size=2, start_method="fork")
    try:
        out = runtime.run([], timeout=10.0)
        full_stats = runtime.stats()
        stats = full_stats["runtime"]
    finally:
        runtime.stop()

    assert torch.equal(out[0], torch.tensor([14.0, 16.0]))
    assert len(stats["processes"]) == 3
    assert all(proc["pid"] for proc in stats["processes"])
    traces = stats["last_job_meta"]["stage_traces"]
    assert [trace["stage"] for trace in traces] == ["AR", "Generation", "Diffusion"]
    assert len({trace["pid"] for trace in traces}) == 3
    assert traces[0]["edge_stats"]["backend_counts"] == {"shm": 1}
    assert traces[1]["edge_stats"]["backend_counts"] == {"shm": 1}
    assert full_stats["stages"]["AR"]["runs"] == 1
    assert full_stats["edges"]["AR->Generation"]["backend_counts"] == {"shm": 1}
    assert full_stats["edges"]["Generation->Diffusion"]["backend_counts"] == {"shm": 1}
