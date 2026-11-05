#!/usr/bin/env python3
# Copyright 2026 KVCache.AI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Real-HBM benchmark of Mooncake TENT's TPU path on a TPU VM.

JAX arrays are exported through a TPU buffer provider (tpu-sync by default),
registered with ``mooncake.tpu`` and moved over loopback TCP between two
processes with one TENT engine each: this process (the initiator) owns chips
0-3 and starts the target process on chips 4-7. Three sections:

* ``copy``:   the adapter's HBM<->host data path (``mc_tpu_pjrt_copy_d2h/h2d``,
  i.e. provider DMA plus fake-address lookup) into page-aligned host memory,
  as TENT's staging buffers are, and into DMA-mapped host memory (the ceiling
  if the staging pool were mapped), next to JAX's own
  ``device_get``/``device_put`` for the same bytes.
* ``engine``: end-to-end TENT transfers between two TPU chips (staged through
  host DRAM), TPU<->host, and a host->host baseline through the same engines.
* ``shards``: one batch moving every shard of an array sharded over several
  chips into an array sharded over other chips (the KV-cache layout).

Every transfer section verifies the destination bytes.

``--protocol`` picks the transport for the host-to-host hop: ``tcp`` (the
default) or ``hp_tcp``, whose connections per peer ``--connections`` sets.

Requirements: Mooncake built with -DUSE_TENT=ON -DUSE_TPU=ON, JAX on TPU, and
tpu-sync's JAX provider (tpu_sync.solutions.mooncake.jax.mooncake_provider)
importable.

  python tpu_hbm_bench.py --sizes 1M,16M,256M --iters 10
  python tpu_hbm_bench.py --sections engine,shards --protocol hp_tcp --connections 8
"""

import argparse
import ctypes
import json
import mmap
import os
import statistics
import subprocess
import sys
import time

# The initiator and the target it starts each own half of the VM's chips: a
# chip can be open in only one process, and libtpu refuses to start in a second
# process unless ALLOW_MULTIPLE_LIBTPU_LOAD is set.
CHIPS = {"initiator": (0, 1, 2, 3), "target": (4, 5, 6, 7)}
ROLE = os.environ.get("TPU_HBM_BENCH_ROLE", "initiator")
os.environ.setdefault("TPU_VISIBLE_CHIPS", ",".join(map(str, CHIPS[ROLE])))
os.environ.setdefault("TPU_CHIPS_PER_PROCESS_BOUNDS", "2,2,1")
os.environ.setdefault("TPU_PROCESS_BOUNDS", "1,1,1")
os.environ.setdefault("ALLOW_MULTIPLE_LIBTPU_LOAD", "true")

# TENT must be selected before mooncake.engine creates its engine.
os.environ.setdefault("MC_USE_TENT", "1")

import jax  # noqa: E402
import jax.numpy as jnp  # noqa: E402
import numpy as np  # noqa: E402
from jax.sharding import Mesh, NamedSharding, PartitionSpec  # noqa: E402

from mooncake import tpu  # noqa: E402

ROW_BYTES = 4096  # One float32 row of 1024 elements.


def parse_size(text):
    units = {"K": 1 << 10, "M": 1 << 20, "G": 1 << 30}
    text = text.strip().upper().rstrip("B").rstrip("I")
    if text and text[-1] in units:
        return int(float(text[:-1]) * units[text[-1]])
    return int(text)


def human(nbytes):
    for unit in ("B", "KiB", "MiB", "GiB"):
        if nbytes < 1024 or unit == "GiB":
            return f"{nbytes:g} {unit}" if unit == "B" else f"{nbytes:.4g} {unit}"
        nbytes /= 1024


def host_values(nbytes, seed):
    """The values make_array puts on the device; either process can rebuild
    them from the seed to verify a transfer."""
    rows = max(8, (nbytes // ROW_BYTES + 7) // 8 * 8)
    return np.random.default_rng(seed).standard_normal((rows, 1024), np.float32)


def make_array(nbytes, device, seed):
    """A float32 (rows, 1024) array on `device`; rows are a multiple of 8 so
    the TPU's tiled layout has no padding."""
    return jax.device_put(host_values(nbytes, seed), device).block_until_ready()


class Adapter:
    """Raw data-path ABI of the adapter mooncake.tpu loaded."""

    def __init__(self, path):
        lib = ctypes.CDLL(path)
        for name in ("mc_tpu_pjrt_copy_d2h", "mc_tpu_pjrt_copy_h2d"):
            fn = getattr(lib, name)
            fn.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t]
            fn.restype = ctypes.c_int
        self.lib = lib

    def d2h(self, host, fake_addr, nbytes):
        if self.lib.mc_tpu_pjrt_copy_d2h(host.ctypes.data, fake_addr, nbytes):
            raise RuntimeError("adapter D2H failed")

    def h2d(self, fake_addr, host, nbytes):
        if self.lib.mc_tpu_pjrt_copy_h2d(fake_addr, host.ctypes.data, nbytes):
            raise RuntimeError("adapter H2D failed")

    def read(self, fake_addr, nbytes):
        host = np.empty(nbytes, np.uint8)
        self.d2h(host, fake_addr, nbytes)
        return host


def timed(fn, iters, warmup=2):
    for _ in range(warmup):
        fn()
    samples = []
    for _ in range(iters):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    return samples


def cell(nbytes, samples):
    gbps = nbytes * len(samples) / sum(samples) / 1e9
    return f"{gbps:8.2f} ({statistics.median(samples) * 1e6:9.0f})"


def print_header(title, columns):
    print(f"\n== {title}")
    print(f"{'size':<12}" + "".join(f"{c:>26}" for c in columns))


# --- copy ---------------------------------------------------------------------


HUGE_PAGE = 2 << 20


class HostRegion:
    """Prefaulted host memory for the copy section, 2 MiB aligned.

    The host side's page size dominates provider copy speed when the memory is
    not DMA-mapped (measured with tpu-sync on v6e: ~0.3 GB/s on 4 KiB pages vs
    ~14-20 GB/s on 2 MiB pages). TENT's staging pool comes from
    numa_alloc_onnode, i.e. anonymous mmap, so it gets huge pages only through
    transparent hugepages; `pages="huge"` models that and `pages="small"` a
    host with THP off. `dma_map` registers the region with the TPU's DMA
    engine (PjRtClient::DmaMap), the ceiling.
    """

    def __init__(self, nbytes, pages, dma_map_device=None):
        size = (nbytes + HUGE_PAGE - 1) // HUGE_PAGE * HUGE_PAGE
        self._mmap = mmap.mmap(
            -1, size + HUGE_PAGE, flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS
        )
        base = np.frombuffer(self._mmap, np.uint8)
        start = (-base.ctypes.data) % HUGE_PAGE
        offset = start  # mmap.madvise needs a page-aligned start
        self._mmap.madvise(
            mmap.MADV_HUGEPAGE if pages == "huge" else mmap.MADV_NOHUGEPAGE,
            offset,
            size,
        )
        self.array = base[start : start + nbytes]
        base[start : start + size] = 0  # prefault
        self._device = dma_map_device
        if dma_map_device is not None:
            dma_map_device.client.dma_map(base.ctypes.data + start, size)
            self._mapped = base.ctypes.data + start

    def close(self):
        if self._device is not None:
            self._device.client.dma_unmap(self._mapped)
        del self.array
        self._mmap.close()


def run_copy(args, adapter, devices):
    print_header(
        "copy: HBM<->host, GB/s (p50 us); host memory on 2 MiB pages (TENT's "
        "staging pool under THP), 4 KiB pages, or DMA-mapped",
        [
            "D2H 2MiB pages",
            "H2D 2MiB pages",
            "D2H 4KiB pages",
            "D2H dma-mapped",
            "H2D dma-mapped",
            "jax.device_get",
            "jax.device_put",
        ],
    )
    device = devices[args.src_device]
    for nbytes in args.sizes:
        array = make_array(nbytes, device, seed=1)
        shards = tpu.prepare([array], framework="jax")
        shard = shards[0][0]
        size = shard.size
        regions = [
            HostRegion(size, "huge"),
            HostRegion(size, "small"),
            HostRegion(size, "huge", dma_map_device=device),
        ]
        huge, small, mapped = (r.array for r in regions)
        addr = shard.fake_addr
        try:
            cols = [
                timed(lambda: adapter.d2h(huge, addr, size), args.iters),
                timed(lambda: adapter.h2d(addr, huge, size), args.iters),
                timed(lambda: adapter.d2h(small, addr, size), args.iters),
                timed(lambda: adapter.d2h(mapped, addr, size), args.iters),
                timed(lambda: adapter.h2d(addr, mapped, size), args.iters),
            ]
            # JAX caches an array's host value, so time fresh copies.
            fresh = [(array + 0).block_until_ready() for _ in range(args.iters + 2)]
            it = iter(fresh)
            cols.append(timed(lambda: jax.device_get(next(it)), args.iters))
            del fresh
            host_value = np.asarray(array)
            cols.append(
                timed(
                    lambda: jax.device_put(host_value, device).block_until_ready(),
                    args.iters,
                )
            )
        finally:
            huge = small = mapped = None  # Drop views so the mmaps can close.
            for region in regions:
                region.close()
            tpu.unregister(shards[0])
        print(f"{human(size):<12}" + "".join(f"{cell(size, c):>26}" for c in cols))


# --- engine -------------------------------------------------------------------


class Channel:
    """One JSON message per line over a pair of pipe file descriptors."""

    def __init__(self, read_fd, write_fd):
        self._reader = os.fdopen(read_fd, "r")
        self._writer = os.fdopen(write_fd, "w")

    def send(self, **message):
        self._writer.write(json.dumps(message) + "\n")
        self._writer.flush()

    def recv(self):
        line = self._reader.readline()
        if not line:
            raise RuntimeError("the other benchmark process exited")
        return json.loads(line)


def tent_conf(protocol, connections):
    """TENT configuration for both engines: `protocol` carries the host-to-host
    hop, with `connections` hp_tcp connections per peer if set."""
    hp_tcp = {"enable": protocol == "hp_tcp"}
    if connections is not None:
        hp_tcp["connections_per_peer"] = connections
    return json.dumps(
        {
            "log_level": "warning",
            "transports": {
                "tcp": {"enable": protocol == "tcp"},
                "hp_tcp": hp_tcp,
                "rdma": {"enable": False},
                "shm": {"enable": False},
                "io_uring": {"enable": False},
                "tpu": {"enable": True},
            },
        }
    )


class Engine:
    """This process's TENT engine; `peer` names the engine it transfers to."""

    def __init__(self, protocol):
        from mooncake.engine import TransferEngine

        self.engine = TransferEngine()
        if self.engine.initialize("127.0.0.1", "P2PHANDSHAKE", protocol, "") != 0:
            raise RuntimeError("TransferEngine.initialize failed")
        self.peer = None
        self.registered = []

    def register_host(self, nbytes):
        host = np.zeros(nbytes, np.uint8)
        if self.engine.register_memory(host.ctypes.data, nbytes) != 0:
            raise RuntimeError("register_memory failed")
        self.registered.append((host.ctypes.data, host))
        return host

    def register_tpu(self, shards):
        tpu.register_with_engine(self.engine, [shards])
        for shard in shards:
            self.registered.append((shard.fake_addr, None))

    def write(self, local, remote, nbytes):
        rc = self.engine.transfer_sync_write(self.peer, local, remote, nbytes)
        if rc != 0:
            raise RuntimeError(f"transfer_sync_write failed: {rc}")

    def read(self, local, remote, nbytes):
        rc = self.engine.transfer_sync_read(self.peer, local, remote, nbytes)
        if rc != 0:
            raise RuntimeError(f"transfer_sync_read failed: {rc}")

    def batch_write(self, locals_, remotes, lengths):
        rc = self.engine.batch_transfer_sync_write(self.peer, locals_, remotes, lengths)
        if rc != 0:
            raise RuntimeError(f"batch_transfer_sync_write failed: {rc}")

    def unregister_all(self):
        for addr, _ in reversed(self.registered):
            self.engine.unregister_memory(addr)
        self.registered.clear()


class Target:
    """The target engine, run by a child process that owns the target chips."""

    def __init__(self, args):
        to_child_r, to_child_w = os.pipe()
        to_parent_r, to_parent_w = os.pipe()
        config = {
            "sizes": args.sizes,
            "dst_device": args.dst_device,
            "adapter": args.adapter,
            "protocol": args.protocol,
        }
        env = dict(
            os.environ,
            TPU_HBM_BENCH_ROLE="target",
            TPU_VISIBLE_CHIPS=",".join(map(str, CHIPS["target"])),
            TPU_HBM_BENCH_FDS=f"{to_child_r},{to_parent_w}",
            TPU_HBM_BENCH_CONFIG=json.dumps(config),
        )
        # This process's libtpu is already up, so the two never start at the
        # same moment. The child's stdout goes to our stderr, leaving stdout to
        # the result tables.
        self.proc = subprocess.Popen(
            [sys.executable, os.path.abspath(__file__)],
            env=env,
            pass_fds=(to_child_r, to_parent_w),
            stdout=sys.stderr,
        )
        os.close(to_child_r)
        os.close(to_parent_w)
        self.channel = Channel(to_parent_r, to_child_w)
        ready = self.channel.recv()
        self.name = f"127.0.0.1:{ready['port']}"
        self.host = ready["host"]
        self.dst = {n: (addr, size) for n, addr, size in ready["dst"]}

    def call(self, op, **kwargs):
        self.channel.send(op=op, **kwargs)
        return self.channel.recv()

    def close(self):
        try:
            self.call("exit")
        except (OSError, RuntimeError):
            pass  # The child already exited; its traceback is on stderr.
        try:
            self.proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()


def target_main():
    """The target process: owns the destination buffers and checks what lands
    in them, on the initiator's request."""
    config = json.loads(os.environ["TPU_HBM_BENCH_CONFIG"])
    channel = Channel(*map(int, os.environ["TPU_HBM_BENCH_FDS"].split(",")))
    devices = jax.local_devices(backend="tpu")
    adapter = Adapter(tpu.get_adapter(config["adapter"]).path)
    device = devices[config["dst_device"]]
    dst_arrays = {n: make_array(n, device, 3) for n in config["sizes"]}
    dst = {n: tpu.prepare([a], framework="jax")[0][0] for n, a in dst_arrays.items()}
    # As in the initiator, register buffers on every device before the engine
    # is created, since TENT reads the TPU topology then.
    anchors = [jax.device_put(jnp.zeros((8, 1024), jnp.float32), d) for d in devices]
    all_shards = list(dst.values())
    all_shards.extend(
        s for per_array in tpu.prepare(anchors, framework="jax") for s in per_array
    )
    engine = Engine(config["protocol"])
    sharded = {}
    try:
        for shard in dst.values():
            engine.register_tpu([shard])
        max_size = max(s.size for s in dst.values())
        host = engine.register_host(max_size)
        # The initiator fills its host buffer from the same seed.
        expected_host = np.random.default_rng(4).integers(0, 255, max_size, np.uint8)
        channel.send(
            port=engine.engine.get_rpc_port(),
            host=host.ctypes.data,
            dst=[[n, s.fake_addr, s.size] for n, s in dst.items()],
        )
        while True:
            message = channel.recv()
            op, n = message["op"], message.get("nbytes")
            if op == "exit":
                channel.send(ok=True)
                return
            if op == "shards":
                k = message["k"]
                mesh = Mesh(np.array(devices[:k]), ("x",))
                zeros = np.zeros((shard_rows(n, k), 1024), np.float32)
                array = jax.device_put(
                    zeros, NamedSharding(mesh, PartitionSpec("x", None))
                )
                jax.block_until_ready(array)
                (shards,) = tpu.prepare([array], framework="jax")
                all_shards.extend(shards)
                engine.register_tpu(shards)
                sharded[n] = (array, k)
                channel.send(remotes=[s.fake_addr for s in shards])
                continue
            if op == "check_host":
                d = dst[n]
                ok = np.array_equal(
                    adapter.read(d.fake_addr, d.size), expected_host[: d.size]
                )
            elif op == "check_tpu":
                ok = np.array_equal(np.asarray(dst_arrays[n]), host_values(n, 2))
            else:  # check_shards
                array, k = sharded[n]
                ok = np.array_equal(np.asarray(array), shard_values(n, k))
            channel.send(ok=bool(ok))
    finally:
        engine.unregister_all()
        tpu.unregister(all_shards)


def prepare_engine_arrays(args, devices, all_shards):
    src_arrays = {n: make_array(n, devices[args.src_device], 2) for n in args.sizes}
    src = {n: tpu.prepare([a], framework="jax")[0][0] for n, a in src_arrays.items()}
    all_shards.extend(src.values())
    return src_arrays, src


def bench_engine(args, initiator, target, src):
    for n in args.sizes:
        initiator.register_tpu([src[n]])
    max_size = max(s.size for s in src.values())
    local_host = initiator.register_host(max_size)
    local_host[:] = np.random.default_rng(4).integers(0, 255, max_size, np.uint8)

    print_header(
        f"engine: TENT over loopback TCP between two processes, chip "
        f"{CHIPS['initiator'][args.src_device]} -> chip "
        f"{CHIPS['target'][args.dst_device]}, GB/s (p50 us)",
        [
            "host->host WRITE",
            "tpu->tpu WRITE",
            "tpu->tpu READ",
            "tpu->host WRITE",
            "host->tpu WRITE",
        ],
    )
    lh, rh = local_host.ctypes.data, target.host
    for n in args.sizes:
        s, (d, _) = src[n], target.dst[n]
        size = s.size
        cols = [
            timed(lambda: initiator.write(lh, rh, size), args.iters),
            timed(lambda: initiator.write(s.fake_addr, d, size), args.iters),
            timed(lambda: initiator.read(s.fake_addr, d, size), args.iters),
            timed(lambda: initiator.write(s.fake_addr, rh, size), args.iters),
            timed(lambda: initiator.write(lh, d, size), args.iters),
        ]
        print(f"{human(size):<12}" + "".join(f"{cell(size, c):>26}" for c in cols))

    # Verify: host->tpu landed, then tpu->tpu landed and JAX sees it. The
    # target checks its own HBM.
    for n in args.sizes:
        s, (d, d_size) = src[n], target.dst[n]
        initiator.write(lh, d, d_size)
        if not target.call("check_host", nbytes=n)["ok"]:
            raise RuntimeError(f"host->tpu payload mismatch at {human(d_size)}")
        initiator.write(s.fake_addr, d, s.size)
        if not target.call("check_tpu", nbytes=n)["ok"]:
            raise RuntimeError(f"tpu->tpu payload mismatch at {human(s.size)}")
    print("verified: destination HBM matches the source (raw bytes and via JAX)")


def shard_rows(nbytes, k):
    return max(8 * k, (nbytes // ROW_BYTES + 8 * k - 1) // (8 * k) * (8 * k))


def shard_values(nbytes, k):
    """The shards section's payload; the target rebuilds it to verify."""
    rows = shard_rows(nbytes, k)
    return np.random.default_rng(5).standard_normal((rows, 1024), np.float32)


def bench_shards(args, initiator, target, all_shards, devices):
    k = args.shards
    if k > len(devices):
        print(f"\n== shards: skipped, needs {k} TPU devices per process")
        return
    src_mesh = Mesh(np.array(devices[:k]), ("x",))
    src_chips, dst_chips = CHIPS["initiator"][:k], CHIPS["target"][:k]
    print_header(
        f"shards: one batch, {k} shards on chips {src_chips[0]}-{src_chips[-1]} "
        f"-> chips {dst_chips[0]}-{dst_chips[-1]}, aggregate GB/s (p50 us)",
        ["batch WRITE"],
    )
    for n in args.sizes:
        host = shard_values(n, k)
        src_array = jax.device_put(
            host, NamedSharding(src_mesh, PartitionSpec("x", None))
        )
        jax.block_until_ready(src_array)
        (src_shards,) = tpu.prepare([src_array], framework="jax")
        all_shards.extend(src_shards)
        initiator.register_tpu(src_shards)
        remotes = target.call("shards", nbytes=n, k=k)["remotes"]
        locals_ = [s.fake_addr for s in src_shards]
        lengths = [s.size for s in src_shards]
        samples = timed(
            lambda: initiator.batch_write(locals_, remotes, lengths), args.iters
        )
        total = sum(lengths)
        print(f"{human(total):<12}{cell(total, samples):>26}")
        if not target.call("check_shards", nbytes=n)["ok"]:
            raise RuntimeError(f"sharded payload mismatch at {human(total)}")
    print("verified: every destination shard matches its source shard (via JAX)")


def main():
    if ROLE == "target":
        return target_main()
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--sizes", default="1M,16M,64M,256M")
    parser.add_argument("--iters", type=int, default=10)
    parser.add_argument(
        "--src-device", type=int, default=0, help="index into the initiator's chips"
    )
    parser.add_argument(
        "--dst-device", type=int, default=0, help="index into the target's chips"
    )
    parser.add_argument("--shards", type=int, default=4)
    parser.add_argument(
        "--sections", default="copy,engine,shards", help="comma-separated subset"
    )
    parser.add_argument("--adapter", default=None, help="libmooncake_tpu_pjrt.so")
    parser.add_argument(
        "--protocol",
        choices=("tcp", "hp_tcp"),
        default="tcp",
        help="transport for the host-to-host hop",
    )
    parser.add_argument(
        "--connections",
        type=int,
        default=None,
        help="hp_tcp connections per peer (default: TENT's)",
    )
    args = parser.parse_args()
    if args.connections is not None and args.protocol != "hp_tcp":
        parser.error("--connections needs --protocol hp_tcp")
    args.sizes = [parse_size(s) for s in args.sizes.split(",")]
    sections = set(args.sections.split(","))
    # Before either engine exists; the target process inherits it. An
    # MC_TENT_CONF already in the environment takes precedence.
    os.environ.setdefault("MC_TENT_CONF", tent_conf(args.protocol, args.connections))

    devices = jax.local_devices(backend="tpu")
    if not devices:
        sys.exit("no TPU devices visible to JAX")
    adapter_lib = tpu.get_adapter(args.adapter)
    adapter = Adapter(adapter_lib.path)
    link = args.protocol
    if args.connections is not None:
        link += f" with {args.connections} connections per peer"
    print(
        f"TPU HBM benchmark: {len(devices)} x {devices[0].device_kind} per process, "
        f"jax {jax.__version__}, adapter {adapter_lib.path}, host-to-host over "
        f"{link}"
    )

    if "copy" in sections:
        run_copy(args, adapter, devices)
    if not sections & {"engine", "shards"}:
        return

    # TENT reads the TPU topology when the engine is created, so register
    # buffers on every device the transfers will touch first. `src_arrays`
    # keeps the source arrays alive while their shards are registered.
    all_shards = []
    src_arrays, src = prepare_engine_arrays(args, devices, all_shards)
    anchors = [jax.device_put(jnp.zeros((8, 1024), jnp.float32), d) for d in devices]
    all_shards.extend(
        s for per_array in tpu.prepare(anchors, framework="jax") for s in per_array
    )
    initiator = Engine(args.protocol)
    target = None
    try:
        target = Target(args)
        initiator.peer = target.name
        if "engine" in sections:
            bench_engine(args, initiator, target, src)
        if "shards" in sections:
            bench_shards(args, initiator, target, all_shards, devices)
    finally:
        initiator.unregister_all()
        tpu.unregister(all_shards)
        if target is not None:
            target.close()


if __name__ == "__main__":
    main()
