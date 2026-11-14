"""Opt-in GPU/RDMA coverage for shared allocations and participant lifetimes."""

import multiprocessing as mp
import os
import traceback

import pytest
from mooncake.reshard.kv_cache import KVCacheComponent, KVCacheTransferEngineExecutor
from test_kv_cache_reshard import _snapshot
from test_runtime_transfer_native import _receive
from test_shared_registration import (
    CountingEngine,
    resolve_all,
    shared_bindings,
    shared_plan,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("MC_SHARED_GPU_NATIVE") != "1",
    reason="set MC_SHARED_GPU_NATIVE=1 for real GPU/RDMA tests",
)


def worker(connection, role, ssm, strided, order):
    try:
        import torch
        from mooncake.engine import TransferEngine

        gpu = int(
            os.environ.get(
                "MC_SHARED_SOURCE_GPU" if role == "source" else "MC_SHARED_TARGET_GPU",
                "0" if role == "source" else "4",
            )
        )
        torch.cuda.set_device(gpu)
        pattern = (torch.arange(512, dtype=torch.int64) % 251).to(torch.uint8)
        expected = (
            pattern.clone() if role == "source" else torch.full_like(pattern, 165)
        )
        data = expected.cuda()
        torch.cuda.synchronize()
        native = TransferEngine()
        host = os.environ.get("MC_R2R_HOST", "127.0.0.1")
        assert (
            native.initialize(
                host, "P2PHANDSHAKE", "rdma", os.environ.get("MC_R2R_DEVICE", "erdma_0")
            )
            == 0
        )
        endpoint = f"{host}:{native.get_rpc_port()}"
        engine = CountingEngine(native)
        snapshot = _snapshot(token_start=13, token_count=2 if strided else 1)
        placement, raw = shared_bindings(
            role,
            snapshot,
            data.data_ptr(),
            component=KVCacheComponent.SSM if ssm else None,
            strided=strided,
            endpoint=endpoint,
            device=f"cuda:{gpu}",
        )
        bindings = resolve_all(
            placement,
            snapshot,
            raw,
            first_token_offset=int(strided and role == "source"),
        )
        assert (
            len(bindings[0].regions) == 1 and bindings[0].regions == bindings[1].regions
        )
        executor = KVCacheTransferEngineExecutor(
            engine, instance_id=bindings[0].instance_id, endpoint=endpoint
        )
        for binding in bindings:
            region = binding.regions[0]
            assert (region.address, region.nbytes) == (data.data_ptr(), 512)
            executor.register_region(region, owner_id=binding.digest)
            executor.register_region(region, owner_id=binding.digest)
            executor.validate_local_binding(binding)
        assert engine.registers == 1
        connection.send(("binding", (placement, bindings)))
        plan = connection.recv() if role == "source" else None
        for step, index in enumerate(order):
            binding = bindings[index]
            if role == "source":
                receipts = executor.execute(plan, binding.participant_id)
                assert receipts and all(r.success and r.quiesced for r in receipts)
                connection.send(("transferred", None))
            connection.recv()  # Target verifies only after synchronous RDMA finishes.
            if role == "target":
                for view in range(1 if ssm else 2):
                    offset = index * (64 if strided else 128) + view * (
                        32 if strided else 64
                    )
                    for row in range(snapshot.token_count):
                        dst = offset + row * (128 if strided else 32)
                        src = dst + (128 if strided else 0)
                        expected[dst : dst + 32] = pattern[src : src + 32]
            assert torch.equal(data.cpu(), expected), "payload or guard bytes changed"
            executor.unregister_region(
                binding.regions[0].region_id, owner_id=binding.digest
            )
            assert engine.unregisters == int(step == 1)
            if step == 0:
                with pytest.raises(ValueError, match="owner claim"):
                    executor.validate_local_binding(binding)
                if role == "source":
                    with pytest.raises(ValueError, match="owner claim"):
                        executor.execute(plan, binding.participant_id)
            connection.send(("released", None))
        region = bindings[0].regions[0]
        executor.register_region(region, owner_id="reuse")
        executor.unregister_region(region.region_id, owner_id="reuse")
        assert engine.registers == engine.unregisters == 2
        connection.send(("done", None))
    except BaseException:  # noqa: BLE001 - forward worker diagnostics to parent
        connection.send(("error", traceback.format_exc()))
    finally:
        connection.close()


@pytest.mark.parametrize("ssm", [False, True])
@pytest.mark.parametrize("strided", [False, True])
@pytest.mark.parametrize("order", [(0, 1), (1, 0)])
def test_shared_gpu_allocation(ssm, strided, order):
    context = mp.get_context("spawn")
    children = []
    try:
        for role in ("source", "target"):
            parent, child = context.Pipe()
            process = context.Process(
                target=worker, args=(child, role, ssm, strided, order)
            )
            process.start()
            child.close()
            children.append((process, parent))
        (source, sources), (target, targets) = [
            _receive(p, "binding") for _, p in children
        ]
        snapshot = _snapshot(token_start=13, token_count=2 if strided else 1)
        source_pipe, target_pipe = [pipe for _, pipe in children]
        source_pipe.send(shared_plan(source, target, snapshot, sources, targets))
        for _ in order:
            _receive(source_pipe, "transferred")
            target_pipe.send("verify")
            _receive(target_pipe, "released")
            source_pipe.send("release")
            _receive(source_pipe, "released")
        for _, pipe in children:
            _receive(pipe, "done")
    finally:
        for process, pipe in children:
            process.join(5)
            if process.is_alive():
                process.terminate()
                process.join(10)
            pipe.close()
