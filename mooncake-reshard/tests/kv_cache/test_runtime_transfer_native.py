"""Opt-in real TE conformance across independently registered worker processes.

MC_R2R_NATIVE=1 python -m pytest mooncake-reshard/tests/kv_cache/test_runtime_transfer_native.py -v
MC_R2R_PROTOCOL=tcp|rdma selects a transport; MC_R2R_CUDA=1 selects GPU memory.
The same worker protocol can be launched on separate hosts by a runtime's
control plane. This harness exercises process boundaries on one physical host.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import traceback
from dataclasses import replace

import pytest
from mooncake.reshard.kv_cache import (
    KVCacheCompletion,
    KVCacheRuntimeTransferPlan,
    KVCacheTargetReceipt,
    KVCacheTransferEngineExecutor,
    KVCacheTransferLimits,
    kv_cache_resolved_binding_from_json,
    kv_cache_resolved_binding_to_json,
    kv_cache_runtime_transfer_from_json,
    kv_cache_runtime_transfer_to_json,
    kv_cache_writer_receipt_from_json,
    kv_cache_writer_receipt_to_json,
    plan_kv_cache_transfer_to_local_target,
)
from test_kv_cache_reshard import _placement, _snapshot
from test_runtime_transfer import assert_content, memory_binding

pytestmark = pytest.mark.skipif(
    os.environ.get("MC_R2R_NATIVE") != "1",
    reason="set MC_R2R_NATIVE=1 for real TE tests",
)


def _worker(connection, role, tp, rank, protocol, use_cuda):
    try:
        from mooncake.engine import TransferEngine

        placement = _placement(role, ((0, 1),), tp)
        part = placement.parts[rank]
        snapshot = _snapshot(token_start=13, token_count=5)
        engine = TransferEngine()
        host = os.environ.get("MC_R2R_HOST", "127.0.0.1")
        result = engine.initialize(
            host,
            "P2PHANDSHAKE",
            protocol,
            os.environ.get("MC_R2R_DEVICE", "") if protocol == "rdma" else "",
        )
        if result != 0:
            raise RuntimeError(f"TE initialize failed: {result}")
        endpoint = f"{host}:{engine.get_rpc_port()}"
        binding, buffers = memory_binding(
            placement,
            part.participant_id,
            snapshot,
            source=role == "source",
            reverse=role == "source",
            padding=4,
            endpoint=endpoint,
        )
        gpu_buffers = []
        if use_cuda:
            import torch

            device = (rank + (0 if role == "source" else 2)) % torch.cuda.device_count()
            torch.cuda.set_device(device)
            for buffer in buffers:
                gpu_buffers.append(
                    torch.frombuffer(buffer, dtype=torch.uint8)
                    .clone()
                    .to(f"cuda:{device}")
                )
            torch.cuda.synchronize()
            binding = replace(
                binding,
                regions=tuple(
                    replace(r, address=t.data_ptr())
                    for r, t in zip(binding.regions, gpu_buffers)
                ),
            )
        executor = KVCacheTransferEngineExecutor(
            engine, instance_id=binding.instance_id, endpoint=endpoint
        )
        for region in binding.regions:
            executor.register_region(region)
        executor.validate_local_binding(binding)
        connection.send(("binding", kv_cache_resolved_binding_to_json(binding)))
        message = connection.recv()
        if role == "source":
            plan = kv_cache_runtime_transfer_from_json(message)
            receipts = executor.execute(plan, part.participant_id, warmup=False)
            connection.send(
                ("receipts", [kv_cache_writer_receipt_to_json(r) for r in receipts])
            )
        else:
            if message != "verify":
                raise ValueError("unexpected target command")
            if use_cuda:
                cpu_buffers = [t.cpu() for t in gpu_buffers]
                binding = replace(
                    binding,
                    regions=tuple(
                        replace(r, address=t.data_ptr())
                        for r, t in zip(binding.regions, cpu_buffers)
                    ),
                )
            assert_content(placement, binding)
            connection.send(("verified", part.participant_id))
        connection.recv()  # coordinator keeps registrations/pins until barrier
    except BaseException:  # noqa: BLE001 - forward worker diagnostics to parent
        connection.send(("error", traceback.format_exc()))
    finally:
        connection.close()


def _receive(connection, expected, timeout=90):
    assert connection.poll(timeout), f"worker timed out waiting for {expected}"
    kind, payload = connection.recv()
    assert kind == expected, payload
    return payload


@pytest.mark.parametrize(
    "source_tp,target_tp,kill_peer", [(2, 1, False), (1, 2, False), (1, 1, True)]
)
def test_native_content_and_peer_failure(source_tp, target_tp, kill_peer):
    protocol = os.environ.get("MC_R2R_PROTOCOL", "tcp")
    use_cuda = os.environ.get("MC_R2R_CUDA") == "1"
    context = mp.get_context("spawn")
    source = _placement("source", ((0, 1),), source_tp)
    target = _placement("target", ((0, 1),), target_tp)
    snapshot = _snapshot(token_start=13, token_count=5)
    processes, sources, targets = [], [], []
    try:
        for role, tp, output in (
            ("source", source_tp, sources),
            ("target", target_tp, targets),
        ):
            for rank in range(tp):
                parent, child = context.Pipe()
                process = context.Process(
                    target=_worker, args=(child, role, tp, rank, protocol, use_cuda)
                )
                process.start()
                child.close()
                processes.append((process, parent))
                output.append(
                    (
                        process,
                        parent,
                        kv_cache_resolved_binding_from_json(
                            _receive(parent, "binding")
                        ),
                    )
                )
        logical = tuple(
            plan_kv_cache_transfer_to_local_target(
                source, target, p.participant_id, snapshot=snapshot
            )
            for p in target.parts
        )
        plan = KVCacheRuntimeTransferPlan(
            "operation-1",
            logical,
            tuple(b for _, _, b in sources),
            tuple(b for _, _, b in targets),
            KVCacheTransferLimits(max_batch_operations=3, max_batch_bytes=96),
        )
        barrier = KVCacheCompletion(plan, timeout_seconds=180)
        if kill_peer:
            targets[0][0].terminate()
            targets[0][0].join(10)
        for _, connection, _ in sources:
            connection.send(kv_cache_runtime_transfer_to_json(plan))
        receipts = [
            kv_cache_writer_receipt_from_json(r)
            for _, c, _ in sources
            for r in _receive(c, "receipts")
        ]
        if kill_peer:
            assert any(not r.success for r in receipts)
            barrier.record_writer(next(r for r in receipts if not r.success))
            assert not barrier.can_activate
            assert barrier.state == "failed"
            return
        for receipt in receipts:
            assert receipt.success, receipt.error
            barrier.record_writer(receipt)
        for _, connection, _ in targets:
            connection.send("verify")
        for _, connection, _ in targets:
            participant = _receive(connection, "verified")
            barrier.record_target(
                KVCacheTargetReceipt(plan.operation_id, plan.digest, participant, True)
            )
        assert barrier.can_activate
    finally:
        for process, connection in processes:
            if process.is_alive():
                try:
                    connection.send("close")
                except (BrokenPipeError, EOFError):
                    pass
                process.join(5)
            if process.is_alive():
                process.terminate()
                process.join(10)
            connection.close()
