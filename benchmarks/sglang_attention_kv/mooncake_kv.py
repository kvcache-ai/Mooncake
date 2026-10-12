# The Mooncake path for a step's cached history: the history is staged in registered
# host buffers and moved into the KV pool by Mooncake's Transfer Engine, so a row can
# price the transfer against the read that follows it.
#
# The transfer moves whole pages: the pool's pages are what the history occupies, so
# one batch entry covers one page, which is also the unit a serving deployment moves.
# The number of entries a case needs is recorded with the row.

import importlib.metadata
import json
import socket
import time

import torch

from mooncake.engine import TransferEngine, TransferOpcode

from .cases import KV_SOURCE_PROTOCOLS


def transfer_engine_version():
    """The version of the Mooncake distribution that provides the engine module.

    The wheel's distribution name varies with the build (cuda13, non-cuda, npu, and
    so on), so it is looked up from the installed distributions rather than spelled
    out here.
    """
    providers = importlib.metadata.packages_distributions().get("mooncake", [])
    if not providers:
        return None
    return importlib.metadata.version(providers[0])


def local_ip():
    """The address this process is reachable at, which the engine registers under."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]


class MooncakeHistory:
    """One case's cached history, held in host memory and moved into the pool by
    Mooncake's Transfer Engine.

    The input is the case's shape and the content the benchmark generates; the
    output is the pool filled with that content, so the content check that follows
    a step is also the check on this transfer.
    """

    def __init__(self, case, device, dtype, protocol="rdma", device_name=""):
        if protocol not in KV_SOURCE_PROTOCOLS:
            raise ValueError(
                f"unknown protocol {protocol!r}, expected one of {KV_SOURCE_PROTOCOLS}"
            )
        self.case = case
        self.device = device
        self.dtype = dtype
        self.protocol = protocol
        self.engine = TransferEngine()
        code = self.engine.initialize(local_ip(), "P2PHANDSHAKE", protocol, device_name)
        if code != 0:
            raise RuntimeError(
                f"Mooncake TransferEngine.initialize({protocol!r}) returned {code}"
            )
        self.target = f"{local_ip()}:{self.engine.get_rpc_port()}"
        self.buffers = {}
        self.registered = []
        self.runs = []
        self.rows = 0
        self.pages = 0
        self.entries = 0
        self.bytes = 0
        self.descriptors_ms = 0.0
        self.submit_ms = 0.0
        self.version = transfer_engine_version()
        self.nics = self._nics()

    def _register(self, what, tensor):
        code = self.engine.register_memory(tensor.data_ptr(), tensor.nbytes)
        if code != 0:
            raise RuntimeError(f"Mooncake register_memory for {what} returned {code}")
        self.registered.append(tensor)

    def _nics(self):
        """The devices the engine's topology puts next to the GPU this case runs on.
        It is what the engine would pick from, not a statement that it used one."""
        topology = json.loads(self.engine.get_local_topology())
        entry = topology.get(str(self.device)) or topology.get("cpu:0") or []
        names = {name for group in entry for name in group}
        return sorted(names)

    # ------------------------------------------------------------- staging

    def stage(self, step):
        """Put the history the step will read into registered host buffers.

        Each buffer is laid out the way the pool lays its rows out — the same row
        pitch and one row per token of the aligned prefix — so a run of consecutive
        slots on both sides becomes one entry. The pool's own buffers are registered
        here as well: an RDMA transfer can only write into memory the engine knows
        about.

        A step with no cached history has nothing to hold and nothing to register:
        the engine rejects a zero-length registration, and the move would move
        nothing. The holder then states no bytes, no entries and no moves.
        """
        case = step.case
        self.rows = self._rows(step)
        if self.rows == 0:
            self.entries = 0
            self.pages = 0
            return
        for layer_id in range(case.num_layers):
            for name, buffer, source in self._layer_tensors(step, layer_id):
                self._register(f"layer {layer_id} {name} in the pool", buffer)
                pitch = buffer.stride(0)
                host = torch.zeros(self.rows, pitch, dtype=self.dtype)
                offset = 0
                for index, prefix_len in enumerate(case.prefix_lens):
                    aligned = int(step.prefix_slots[index].numel())
                    if prefix_len:
                        content = source(layer_id, index, prefix_len)
                        width = content.shape[-1] * content.shape[-2]
                        host[offset : offset + prefix_len, :width] = content.view(
                            prefix_len, -1
                        )
                    offset += aligned
                self._register(f"layer {layer_id} {name} in host memory", host)
                self.buffers[(layer_id, name)] = host
        self._build_runs(step)

    def _rows(self, step):
        return sum(int(slots.numel()) for slots in step.prefix_slots)

    def _layer_tensors(self, step, layer_id):
        key_buffer, value_buffer = step.kv_pool.get_kv_buffer(layer_id)
        return (
            ("k", key_buffer, step.prefix_source),
            ("v", value_buffer, step.prefix_value_source),
        )

    def _build_runs(self, step):
        """One entry per run of consecutive pool slots: where the run starts in the
        host buffer, the pool row it starts at, and how many rows it holds.

        A run grows while the next token's slot is the one after it, so a pool whose
        history sits in consecutive slots needs one entry for the whole of it while a
        pool that churned needs one per page. The source side is always consecutive,
        because the staging buffers are laid out token by token. The entries a case
        needs are therefore its shape's own: they are what a serving deployment would
        submit for the same layout.
        """
        offset = 0
        for index in range(len(step.case.prefix_lens)):
            slots = step.prefix_slots[index].tolist()
            start = 0
            while start < len(slots):
                count = 1
                while start + count < len(slots) and slots[start + count] == (
                    slots[start + count - 1] + 1
                ):
                    count += 1
                self.runs.append((offset + start, slots[start], count))
                start += count
            offset += len(slots)
        self.pages = sum(
            -(-int(slots.numel()) // step.case.page_size) for slots in step.prefix_slots
        )
        self.entries = len(self.runs) * step.case.num_layers * 2

    # ------------------------------------------------------------- transfer

    def fetch(self, step):
        """Move the staged history into the pool, one batch over every layer.

        The wall clock is taken in two parts as well as the device span the caller
        times: building one entry per run on the Python side, and the engine's
        submit and wait. A reader can then see how much of the move is the engine
        and how much is the descriptor work in front of it.
        """
        if not self.runs:
            self.bytes = 0
            return 0
        descriptors_start = time.perf_counter()
        sources, destinations, lengths = [], [], []
        for layer_id in range(step.case.num_layers):
            for name, buffer, _ in self._layer_tensors(step, layer_id):
                host = self.buffers[(layer_id, name)]
                pitch = buffer.stride(0) * buffer.element_size()
                host_base = host.data_ptr()
                pool_base = buffer.data_ptr()
                for row_offset, slot, count in self.runs:
                    sources.append(host_base + row_offset * pitch)
                    destinations.append(pool_base + slot * pitch)
                    lengths.append(count * pitch)
        self.descriptors_ms = (time.perf_counter() - descriptors_start) * 1000.0

        submit_start = time.perf_counter()
        code = self.engine.batch_transfer_sync(
            self.target, sources, destinations, lengths, TransferOpcode.Write
        )
        if code != 0:
            raise RuntimeError(f"Mooncake batch_transfer_sync returned {code}")
        self.submit_ms = (time.perf_counter() - submit_start) * 1000.0
        torch.cuda.synchronize()
        self.bytes = sum(lengths)
        return self.bytes

    def close(self):
        for tensor in self.registered:
            self.engine.unregister_memory(tensor.data_ptr())
        self.registered = []
        self.buffers = {}

    def describe(self):
        """What the record states about this holder: what it registered, how many
        entries a move takes, which engine and version ran it, and where it points.
        The endpoint is this process's own, which is what the engine was told to
        write to; a move between machines would name a peer here."""
        return {
            "source": "host",
            "protocol": self.protocol,
            "engine_version": self.version,
            "engine": "mooncake TransferEngine",
            "endpoint": self.target,
            "nics": self.nics,
            "rows": self.rows,
            "registered_bytes": sum(host.nbytes for host in self.buffers.values()),
            "entries": self.entries,
            "pages": self.pages,
        }
