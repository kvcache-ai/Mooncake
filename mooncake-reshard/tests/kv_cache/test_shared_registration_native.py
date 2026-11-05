"""Opt-in GPU/RDMA coverage for shared allocations and participant lifetimes."""

import json
import multiprocessing as mp
import os
import traceback
from dataclasses import replace
from pathlib import Path

import pytest
from mooncake.reshard.contracts import RuntimeInstanceId
from mooncake.reshard.kv_cache import (
    KVCacheComponent,
    KVCacheLayout,
    KVCachePlacementManifest,
    KVCacheRuntimeTransferPlan,
    KVCacheTransferEngineExecutor,
    kv_cache_resolved_binding_from_json,
    kv_cache_resolved_binding_to_json,
    kv_cache_runtime_transfer_from_json,
    kv_cache_runtime_transfer_to_json,
    plan_kv_cache_transfer_to_local_target,
    resolve_contiguous_runtime_binding,
    validate_runtime_bindings,
)
from test_kv_cache_reshard import _binding, _placement, _snapshot

pytestmark = pytest.mark.skipif(
    os.environ.get("MC_SHARED_GPU_NATIVE") != "1",
    reason="set MC_SHARED_GPU_NATIVE=1 for real GPU/RDMA tests",
)


def placement_for(role, ssm, strided):
    placement = _placement(role, ((0,),), 2, page_size=1)
    descriptor = replace(
        placement.descriptor,
        components=(KVCacheComponent.SSM,)
        if ssm
        else (KVCacheComponent.KEY, KVCacheComponent.VALUE),
        dtype="float32" if ssm else "bfloat16",
        itemsize=4 if ssm else 2,
        key_head_dim=4 if ssm else 8,
        value_head_dim=4 if ssm else 8,
        layout=KVCacheLayout.NHD_STRIDED if strided else KVCacheLayout.NHD,
    )
    return KVCachePlacementManifest(
        resource_id=placement.resource_id,
        revision=placement.revision,
        placement_set_id=placement.placement_set_id,
        topology=placement.topology,
        descriptor=descriptor,
        parts=tuple(replace(p, descriptor=descriptor) for p in placement.parts),
    )


class CountingEngine:
    """Record real native return codes; no registration or transfer is mocked."""

    def __init__(self, engine):
        self.engine = engine
        self.registers, self.unregisters, self.transfers = [], [], []

    def register_memory(self, address, nbytes, location="*"):
        result = self.engine.register_memory(address, nbytes)
        self.registers.append([address, nbytes, result])
        return result

    def unregister_memory(self, address):
        result = self.engine.unregister_memory(address)
        self.unregisters.append([address, result])
        return result

    def send_probe(self, endpoint):
        return self.engine.send_probe(endpoint)

    def batch_transfer_sync_write(self, endpoint, sources, targets, lengths):
        result = self.engine.batch_transfer_sync_write(
            endpoint, sources, targets, lengths
        )
        self.transfers.append(
            {
                "endpoint": endpoint,
                "sources": sources,
                "targets": targets,
                "lengths": lengths,
                "result": result,
            }
        )
        return result

    def evidence(self):
        return {
            "registers": self.registers,
            "unregisters": self.unregisters,
            "transfers": self.transfers,
        }


def worker(connection, role, ssm, strided):
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
        data = (pattern if role == "source" else torch.full_like(pattern, 165)).cuda()
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
        placement = placement_for(role, ssm, strided)
        snapshot = _snapshot(token_start=13, token_count=2 if strided else 1)
        first_row = int(strided and role == "source")
        raw = []
        for rank, part in enumerate(placement.parts):
            binding = _binding(
                placement,
                part.participant_id,
                base_address=data.data_ptr(),
                snapshot=snapshot,
                capacity_tokens=3 if strided else 1,
            )
            buffers = []
            for view, item in enumerate(binding.buffers):
                offset = rank * (64 if strided else 128) + view * (
                    32 if strided else 64
                )
                f = item.fragment
                buffers.append(
                    replace(
                        item,
                        fragment=replace(
                            f,
                            address=data.data_ptr() + offset,
                            worker_id=role,
                            endpoint=endpoint,
                            device=f"cuda:{gpu}",
                            storage_address=data.data_ptr(),
                            storage_nbytes=512,
                            storage_offset_bytes=offset,
                            nbytes=288 if strided else 32,
                            strides_bytes=(
                                128 if strided else 32,
                                *f.strides_bytes[1:],
                            ),
                        ),
                    )
                )
            raw.append(
                replace(
                    binding,
                    instance_id=RuntimeInstanceId(f"{role}:{os.getpid()}"),
                    buffers=tuple(buffers),
                )
            )
        validate_runtime_bindings(placement, tuple(raw), snapshot=snapshot)
        bindings = tuple(
            resolve_contiguous_runtime_binding(
                placement,
                snapshot,
                b,
                operation_id="gpu-shared",
                first_token_offset=first_row,
            )
            for b in raw
        )
        assert bindings[0].regions == bindings[1].regions
        executor = KVCacheTransferEngineExecutor(
            engine, instance_id=bindings[0].instance_id, endpoint=endpoint
        )
        for binding in bindings:
            for region in binding.regions:
                executor.register_region(region, owner_id=binding.digest)
                executor.register_region(region, owner_id=binding.digest)
            executor.validate_local_binding(binding)
        assert len(engine.registers) == 1 and engine.registers[0][1:] == [512, 0]
        expected = (
            pattern.clone() if role == "source" else torch.full_like(pattern, 165)
        )
        connection.send(
            (
                "ready",
                {
                    "bindings": [
                        kv_cache_resolved_binding_to_json(b) for b in bindings
                    ],
                    "gpu": gpu,
                    "address": data.data_ptr(),
                    **engine.evidence(),
                },
            )
        )
        while True:
            command, payload = connection.recv()
            if command == "transfer":
                index, wire = payload
                plan = kv_cache_runtime_transfer_from_json(wire)
                receipts = executor.execute(plan, bindings[index].participant_id)
                assert receipts and all(r.success and r.quiesced for r in receipts)
                assert torch.equal(data.cpu(), pattern)
                connection.send(
                    (
                        "transferred",
                        {
                            "completed_bytes": sum(r.completed_bytes for r in receipts),
                            **engine.evidence(),
                        },
                    )
                )
            elif command == "verify":
                for index in payload:
                    for view in range(1 if ssm else 2):
                        offset = index * (64 if strided else 128) + view * (
                            32 if strided else 64
                        )
                        for row in range(snapshot.token_count):
                            target_offset = offset + row * (128 if strided else 32)
                            source_offset = target_offset + (128 if strided else 0)
                            expected[target_offset : target_offset + 32] = pattern[
                                source_offset : source_offset + 32
                            ]
                wrong = int(torch.count_nonzero(data.cpu() != expected))
                assert wrong == 0, f"{wrong} target payload/guard bytes differ"
                connection.send(("verified", {"wrong_bytes": wrong}))
            elif command == "release":
                binding = bindings[payload]
                for region in binding.regions:
                    executor.unregister_region(
                        region.region_id, owner_id=binding.digest
                    )
                connection.send(("released", engine.evidence()))
            elif command == "reuse":
                region = bindings[0].regions[0]
                executor.register_region(region, owner_id="reuse")
                executor.unregister_region(region.region_id, owner_id="reuse")
                connection.send(("reused", engine.evidence()))
            elif command == "stop":
                break
            else:
                raise ValueError(command)
    except BaseException:  # noqa: BLE001 - forward worker diagnostics to parent
        connection.send(("error", traceback.format_exc()))
    finally:
        connection.close()


def receive(connection, kind):
    assert connection.poll(90), f"timed out waiting for {kind}"
    actual, result = connection.recv()
    assert actual == kind, result
    return result


@pytest.mark.parametrize("ssm", [False, True])
@pytest.mark.parametrize("strided", [False, True])
@pytest.mark.parametrize("order", [(0, 1), (1, 0)])
def test_shared_gpu_allocation(ssm, strided, order):
    context = mp.get_context("spawn")
    children, result = (
        [],
        {"ssm": ssm, "strided": strided, "order": order, "stages": []},
    )
    try:
        for role in ("source", "target"):
            parent, child = context.Pipe()
            process = context.Process(target=worker, args=(child, role, ssm, strided))
            process.start()
            child.close()
            children.append((process, parent))
        ready = [receive(pipe, "ready") for _, pipe in children]
        result["ready"] = ready
        sources, targets = [
            tuple(kv_cache_resolved_binding_from_json(w) for w in item["bindings"])
            for item in ready
        ]
        source, target = [
            placement_for(role, ssm, strided) for role in ("source", "target")
        ]
        snapshot = _snapshot(token_start=13, token_count=2 if strided else 1)
        plan = KVCacheRuntimeTransferPlan(
            "gpu-shared",
            tuple(
                plan_kv_cache_transfer_to_local_target(
                    source, target, p.participant_id, snapshot=snapshot
                )
                for p in target.parts
            ),
            sources,
            targets,
        )
        wire = kv_cache_runtime_transfer_to_json(plan)
        result["plan"] = json.loads(wire)
        done = []
        for index in order:
            children[0][1].send(("transfer", (index, wire)))
            sent = receive(children[0][1], "transferred")
            done.append(index)
            children[1][1].send(("verify", done))
            verified = receive(children[1][1], "verified")
            releases = []
            for _, pipe in children:
                pipe.send(("release", index))
                released = receive(pipe, "released")
                assert len(released["unregisters"]) == int(len(done) == 2)
                assert all(call[-1] == 0 for call in released["unregisters"])
                releases.append(released)
            result["stages"].append(
                {
                    "participant": index,
                    "transfer": sent,
                    "verify": verified,
                    "release": releases,
                }
            )
        reuse = []
        for _, pipe in children:
            pipe.send(("reuse", None))
            reused = receive(pipe, "reused")
            assert len(reused["registers"]) == len(reused["unregisters"]) == 2
            assert all(c[-1] == 0 for c in reused["registers"] + reused["unregisters"])
            reuse.append(reused)
        result.update(reuse=reuse, passed=True)
    finally:
        for process, pipe in children:
            if process.is_alive():
                try:
                    pipe.send(("stop", None))
                except (BrokenPipeError, EOFError):
                    pass
                process.join(10)
            if process.is_alive():
                process.terminate()
                process.join(10)
            pipe.close()
        if directory := os.environ.get("MC_SHARED_RESULTS"):
            out = Path(directory)
            out.mkdir(parents=True, exist_ok=True)
            name = f"{'ssm' if ssm else 'kv'}-{'strided' if strided else 'dense'}-{order[0]}{order[1]}.json"
            (out / name).write_text(json.dumps(result, indent=2) + "\n")
