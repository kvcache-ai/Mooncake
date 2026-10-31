"""Node-sharded immutable CPU tables with GPU-initiated DeepEP reads."""

import base64
import contextlib
import operator
import os
import time
from pathlib import Path
import uuid

import numpy as np

from ._engram_shard import MasterRendezvous, ShardLayout


class DeepEPBackend:
    def __init__(
        self,
        *,
        tables,
        namespace,
        local_tables,
        owners,
        shard_id,
        num_shards,
        master_addr,
        session_id,
        caller_id,
        local_dir,
        max_lookup_rows,
        rdma_device=None,
        read_policy="auto",
        opt_max_rows=12288,
        native_qps=15,
        opt_qps=32,
        timeout=300,
        num_callers=1,
    ):
        import torch

        self.reader = self.comm = self.share = self.local = None
        self.rendezvous = None
        self.metadata = {}
        self.closed = False
        self.ready = False
        self.timeout = timeout
        self.num_callers = operator.index(num_callers)
        if (
            self.num_callers < 1
            or not isinstance(caller_id, int)
            or not 0 <= caller_id < self.num_callers
        ):
            raise ValueError(
                "caller_id must be in [0,num_callers), independently of GPU ID"
            )
        self.shard = operator.index(shard_id)
        self.layout = layout = ShardLayout(tables, owners, num_shards)
        self.capacity = operator.index(max_lookup_rows)
        self.opt_max_rows = operator.index(opt_max_rows)
        self.native_qps, self.opt_qps = map(operator.index, (native_qps, opt_qps))
        self.policy = read_policy
        self.device = torch.cuda.current_device()
        if not 0 <= self.shard < num_shards or self.capacity < 1:
            raise ValueError("Invalid storage shard or lookup capacity")
        if read_policy not in ("auto", "native", "flat") or self.opt_max_rows < 0:
            raise ValueError("Invalid DeepEP read policy")
        if (
            min(self.native_qps, self.opt_qps) < 1
            or max(self.native_qps, self.opt_qps) > 32
        ):
            raise ValueError("QP counts must be in [1,32]")
        if set(local_tables) != set(layout.local_names(self.shard)):
            raise ValueError(
                "local_tables must contain exactly this node's assigned heads"
            )
        if torch.cuda.is_current_stream_capturing():
            raise RuntimeError("initialize must run before CUDA graph capture")
        try:
            self._initialize(
                namespace,
                local_tables,
                master_addr,
                session_id,
                caller_id,
                local_dir,
                rdma_device,
                timeout,
            )
        except BaseException:
            with contextlib.suppress(Exception):
                self.close()
            raise

    def _initialize(
        self,
        namespace,
        local_tables,
        master_addr,
        session_id,
        caller_id,
        local_dir,
        rdma_device,
        timeout,
    ):
        import torch
        from ._engram_deepep_build import load_bridge

        layout = self.layout
        from ._engram_local_share import LocalShare

        self.bridge = load_bridge()
        arrays = {}
        for name in layout.local_names(self.shard):
            value = local_tables[name]
            if isinstance(value, torch.Tensor):
                if value.device.type != "cpu":
                    raise ValueError("Initialize requires CPU byte rows")
                value = value.detach().numpy()
            spec = layout.tables[name]
            if value.dtype != np.uint8 or value.shape != (
                spec["num_rows"],
                layout.width,
            ):
                raise ValueError(f"Invalid local table {name}")
            arrays[name] = value

        def populate(cpu_view):
            packed = (
                cpu_view[: layout.slot_rows * layout.stride]
                .numpy()
                .reshape(layout.slot_rows, layout.stride)
            )
            for name, value in arrays.items():
                start = layout.starts[name]
                packed[start : start + len(value), : layout.width] = value

        self.share = LocalShare(
            self.bridge,
            [
                namespace,
                session_id,
                layout.identity,
                self.shard,
                str(Path(local_dir).resolve()),
            ],
            layout.slot_rows * layout.stride,
            populate,
        )
        rendezvous = self.rendezvous = MasterRendezvous(
            master_addr, namespace, session_id, caller_id, timeout
        )
        descriptor = {
            "layout": layout.identity,
            "capacity": self.capacity,
            "native_qps": self.native_qps,
            "opt_qps": self.opt_qps,
            "num_callers": self.num_callers,
            "nonce": uuid.uuid4().hex,
        }
        rendezvous.publish(f"member/{self.shard}", descriptor)
        for peer in range(layout.num_shards):
            other = rendezvous.wait(f"member/{peer}")
            if any(other[k] != descriptor[k] for k in descriptor if k != "nonce"):
                raise ValueError(f"Engram configuration mismatch at shard {peer}")
        if self.shard == 0:
            rendezvous.publish(
                "nccl", base64.b64encode(bytes(self.bridge.unique_id())).decode()
            )
        unique_id = bytearray(base64.b64decode(rendezvous.wait("nccl")))
        qps = max(self.native_qps, self.opt_qps)
        depth = 1024
        slots = 2 * min(self.native_qps, self.opt_qps)
        while depth < (self.capacity + slots - 1) // slots:
            depth *= 2
        if depth > 32768 or layout.num_shards * qps > 2048:
            raise ValueError("Lookup capacity/shard count exceeds DeepEP queue limits")
        env = {"NCCL_GIN_TYPE": "3"}
        if rdma_device is not None:
            env["NCCL_IB_HCA"] = "=" + rdma_device
        previous = {key: os.environ.get(key) for key in env}
        try:
            os.environ.update(env)
            self.comm = self.bridge.connect(unique_id, layout.num_shards, self.shard)
            self.reader = self.bridge.Reader(
                self.comm,
                self.shard,
                layout.num_shards,
                self.share.storage,
                layout.slot_rows,
                layout.stride,
                self.capacity,
                qps,
                depth,
                int(timeout),
            )
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
        self.local = self.reader.local_address()
        rendezvous.publish(f"ready/{self.shard}", layout.identity)
        for peer in range(layout.num_shards):
            if rendezvous.wait(f"ready/{peer}") != layout.identity:
                raise ValueError("Engram ready layout mismatch")
        # All local callers import the one CPU allocation before its exporting
        # caller may return from initialize (and eventually close its FD server).
        local_group = MasterRendezvous(
            master_addr, namespace, session_id, "node-callers", timeout
        )
        try:
            local_group.publish(f"{self.shard}/{caller_id}", self.share.allocation_id)
            for caller in range(self.num_callers):
                if (
                    local_group.wait(f"{self.shard}/{caller}")
                    != self.share.allocation_id
                ):
                    raise RuntimeError(
                        "Local callers did not import the same Engram allocation"
                    )
        finally:
            local_group.close()
        self.ready = True

    def lookup(self, names, ids, output, stream, offsets):
        import torch
        import triton
        from . import _engram_deepep_kernels as kernels

        if self.closed:
            raise RuntimeError("Engram store is closed")
        if stream is None:
            raise NotImplementedError(
                "DeepEP remote lookup requires CUDA IDs and output"
            )
        if (
            ids.dtype not in (torch.int32, torch.int64)
            or not ids.is_cuda
            or ids.device.index != self.device
            or output.device != ids.device
            or output.dtype != torch.uint8
            or ids.ndim != 2
            or ids.shape[1] != len(names)
            or output.shape != (*ids.shape, self.layout.width)
        ):
            raise ValueError("Expected CUDA int IDs [T,K] and uint8 output [T,K,R]")
        if ids.numel() > self.capacity:
            raise ValueError("Lookup exceeds initialized max_lookup_rows")
        if any(s < 0 for s in ids.stride()) or output.stride(-1) != 1:
            raise ValueError("Invalid Engram tensor strides")
        span = self.layout.width
        for stride, count in sorted(zip(output.stride()[:2], output.shape[:2])):
            if count > 1:
                if stride < span:
                    raise ValueError("Output rows overlap")
                span += stride * (count - 1)
        if ids.untyped_storage().data_ptr() == output.untyped_storage().data_ptr():
            raise ValueError("IDs and output must not alias")
        if not ids.numel():
            return
        # One workspace per caller: callers order overlapping lookup streams,
        # including graph replays. Warmup/capture may use different streams.
        with torch.cuda.device(self.device), torch.cuda.stream(
            torch.cuda.ExternalStream(stream, device=self.device)
        ):
            key = (names, offsets)
            if key not in self.metadata:
                if torch.cuda.is_current_stream_capturing():
                    raise RuntimeError(
                        "Warm up table combinations before graph capture"
                    )
                layout = self.layout
                self.metadata[key] = torch.tensor(
                    [
                        [layout.owners[n] for n in names],
                        [layout.starts[n] for n in names],
                        [layout.tables[n]["num_rows"] for n in names],
                        list(offsets),
                    ],
                    dtype=torch.int64,
                    device=ids.device,
                )
            mapped = torch.empty(ids.shape, dtype=torch.int32, device=ids.device)
            remote = torch.empty_like(mapped)
            kernels.route[(triton.cdiv(ids.numel(), 256),)](
                ids,
                self.metadata[key],
                mapped,
                remote,
                ids.numel(),
                len(names),
                *ids.stride(),
                self.layout.slot_rows,
                self.shard,
                256,
                debug=True,
            )
            flat = self.policy == "flat" or (
                self.policy == "auto" and ids.numel() <= self.opt_max_rows
            )
            fetched = self.reader.fetch(
                remote.reshape(-1, 1) if flat else remote,
                self.opt_qps if flat else self.native_qps,
            )
            kernels.finish[(ids.numel(),)](
                mapped,
                self.local,
                fetched,
                output,
                ids.numel(),
                len(names),
                self.layout.width,
                self.layout.stride,
                self.layout.slot_rows,
                self.shard,
                *output.stride()[:2],
                triton.next_power_of_2(self.layout.width),
            )

    def close(self):
        if self.closed:
            return
        if self.ready:
            import torch

            torch.cuda.synchronize(self.device)
            self.rendezvous.deadline = time.monotonic() + self.timeout
            self.rendezvous.publish(f"closed/{self.shard}", True)
            for peer in range(self.layout.num_shards):
                self.rendezvous.wait(f"closed/{peer}")
        self.closed = True
        self.metadata.clear()
        self.local = None
        if self.reader is not None:
            self.reader.close()
            self.reader = None
        if self.comm is not None:
            self.bridge.disconnect(self.comm)
            self.comm = None
        if self.share is not None:
            self.share.close()
            self.share = None
        if self.rendezvous is not None:
            self.rendezvous.close()

    def __del__(self):
        with contextlib.suppress(Exception):
            self.close()
