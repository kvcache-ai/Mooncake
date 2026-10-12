# The Mooncake path for a step's cached history: the history is staged in registered
# host buffers and moved into the KV pool by Mooncake's Transfer Engine, so a row can
# price the transfer against the read that follows it.
#
# The transfer moves whole pages: the pool's pages are what the history occupies, so
# one batch entry covers one page, which is also the unit a serving deployment moves.
# The number of entries a case needs is recorded with the row.

import socket

import torch

from mooncake.engine import TransferEngine, TransferOpcode

from .cases import KV_SOURCE_PROTOCOLS


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
        self.entries = 0
        self.bytes = 0

    def _register(self, what, tensor):
        code = self.engine.register_memory(tensor.data_ptr(), tensor.nbytes)
        if code != 0:
            raise RuntimeError(f"Mooncake register_memory for {what} returned {code}")
        self.registered.append(tensor)

    # ------------------------------------------------------------- staging

    def stage(self, step):
        """Put the history the step will read into registered host buffers.

        Each buffer is laid out the way the pool lays its rows out — the same row
        pitch and one row per token of the aligned prefix — so a page becomes one
        contiguous run on both sides and the transfer can move it in one entry. The
        pool's own buffers are registered here as well: an RDMA transfer can only
        write into memory the engine knows about.
        """
        case = step.case
        for layer_id in range(case.num_layers):
            for name, buffer, source in self._layer_tensors(step, layer_id):
                self._register(f"layer {layer_id} {name} in the pool", buffer)
                pitch = buffer.stride(0)
                host = torch.zeros(self._rows(step), pitch, dtype=self.dtype)
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
        """One entry per pool page: the page's row range in the host buffer, the
        pool row it starts at, and how many rows it holds."""
        page_size = step.case.page_size
        offset = 0
        for index in range(len(step.case.prefix_lens)):
            slots = step.prefix_slots[index]
            for start in range(0, int(slots.numel()), page_size):
                count = min(page_size, int(slots.numel()) - start)
                self.runs.append((offset + start, int(slots[start]), count))
            offset += int(slots.numel())
        self.entries = len(self.runs) * step.case.num_layers * 2

    # ------------------------------------------------------------- transfer

    def fetch(self, step):
        """Move the staged history into the pool, one batch over every layer."""
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
        code = self.engine.batch_transfer_sync(
            self.target, sources, destinations, lengths, TransferOpcode.Write
        )
        if code != 0:
            raise RuntimeError(f"Mooncake batch_transfer_sync returned {code}")
        torch.cuda.synchronize()
        self.bytes = sum(lengths)
        return self.bytes

    def close(self):
        for tensor in self.registered:
            self.engine.unregister_memory(tensor.data_ptr())
        self.registered = []
        self.buffers = {}

    def describe(self):
        return {
            "source": "host",
            "protocol": self.protocol,
            "registered_bytes": sum(host.nbytes for host in self.buffers.values()),
            "entries": self.entries,
            "pages": len(self.runs),
        }
