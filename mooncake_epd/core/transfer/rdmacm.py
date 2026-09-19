"""Intel iWARP-compatible RDMA data plane built on rdma-core rsockets.

The transport intentionally uses host staging.  Intel X722/``irdma`` can move
the bytes over hardware iWARP, but it is not a validated GPUDirect device for
this host.  CUDA pointers are copied synchronously to/from a host buffer around
the RDMA operation.  Correctness therefore does not depend on GPUDirect.

The receiver only accepts writes into explicitly registered pointer ranges.
This is essential because vLLM exchanges raw KV-cache virtual addresses.
"""

from __future__ import annotations

import ctypes
import errno
import os
import socket
import struct
import sys
import threading
from dataclasses import dataclass
from typing import Iterable, Sequence

from .rdma import RdmaCapabilities, default_rdma_bind_address, detect_rdma_capabilities


_REQUEST_MAGIC = b"EPDRDMA1"
_ACK_MAGIC = b"EPDACK01"
_REQUEST_HEADER = struct.Struct("!8sII")
_DESCRIPTOR = struct.Struct("!QQI")
_ACK = struct.Struct("!8siQ")
_MEMORY_HOST = 0
_MEMORY_CUDA = 1


class RdmaTransportError(RuntimeError):
    pass


class _RSocketLibrary:
    def __init__(self, path: str):
        if not path:
            raise RdmaTransportError("librdmacm.so.1 is unavailable")
        self.lib = ctypes.CDLL(path, use_errno=True)
        self.lib.rsocket.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_int]
        self.lib.rsocket.restype = ctypes.c_int
        self.lib.rbind.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_uint]
        self.lib.rbind.restype = ctypes.c_int
        self.lib.rlisten.argtypes = [ctypes.c_int, ctypes.c_int]
        self.lib.rlisten.restype = ctypes.c_int
        self.lib.raccept.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p]
        self.lib.raccept.restype = ctypes.c_int
        self.lib.rconnect.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_uint]
        self.lib.rconnect.restype = ctypes.c_int
        self.lib.rsend.argtypes = [
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_int,
        ]
        self.lib.rsend.restype = ctypes.c_ssize_t
        self.lib.rrecv.argtypes = [
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_int,
        ]
        self.lib.rrecv.restype = ctypes.c_ssize_t
        self.lib.rclose.argtypes = [ctypes.c_int]
        self.lib.rclose.restype = ctypes.c_int

    @staticmethod
    def _sockaddr(address: str, port: int) -> ctypes.Array:
        if ":" in address:
            raise RdmaTransportError(
                "IPv6 rdmacm endpoints are not enabled yet; configure an IPv4 RDMA address"
            )
        raw = (
            socket.AF_INET.to_bytes(2, byteorder=sys.byteorder)
            + socket.htons(int(port)).to_bytes(2, byteorder=sys.byteorder)
            + socket.inet_aton(address)
            + b"\0" * 8
        )
        return ctypes.create_string_buffer(raw)

    @staticmethod
    def _raise(operation: str) -> None:
        code = ctypes.get_errno()
        raise RdmaTransportError(f"{operation} failed: [{code}] {os.strerror(code)}")

    def socket(self) -> int:
        fd = int(self.lib.rsocket(socket.AF_INET, socket.SOCK_STREAM, 0))
        if fd < 0:
            self._raise("rsocket")
        return fd

    def bind_listen(self, address: str, port: int, backlog: int = 16) -> int:
        fd = self.socket()
        sockaddr = self._sockaddr(address, port)
        if self.lib.rbind(fd, sockaddr, 16) != 0:
            self.close(fd)
            self._raise("rbind")
        if self.lib.rlisten(fd, int(backlog)) != 0:
            self.close(fd)
            self._raise("rlisten")
        return fd

    def connect(self, address: str, port: int) -> int:
        fd = self.socket()
        sockaddr = self._sockaddr(address, port)
        if self.lib.rconnect(fd, sockaddr, 16) != 0:
            self.close(fd)
            self._raise("rconnect")
        return fd

    def accept(self, fd: int) -> int:
        client = int(self.lib.raccept(int(fd), None, None))
        if client < 0:
            self._raise("raccept")
        return client

    def close(self, fd: int) -> None:
        if int(fd) >= 0:
            self.lib.rclose(int(fd))

    def send_all(self, fd: int, payload: bytes | bytearray | memoryview) -> None:
        view = memoryview(payload).cast("B")
        offset = 0
        while offset < len(view):
            chunk = (ctypes.c_ubyte * (len(view) - offset)).from_buffer_copy(view[offset:])
            sent = int(self.lib.rsend(fd, chunk, len(chunk), 0))
            if sent < 0:
                self._raise("rsend")
            if sent == 0:
                raise RdmaTransportError("rsend returned zero before payload completion")
            offset += sent

    def recv_exact(self, fd: int, size: int) -> bytes:
        if size < 0:
            raise ValueError("negative receive size")
        output = bytearray(size)
        offset = 0
        while offset < size:
            chunk = (ctypes.c_ubyte * (size - offset)).from_buffer(output, offset)
            received = int(self.lib.rrecv(fd, chunk, len(chunk), 0))
            if received < 0:
                code = ctypes.get_errno()
                if code == errno.EINTR:
                    continue
                self._raise("rrecv")
            if received == 0:
                raise RdmaTransportError("RDMA peer closed before payload completion")
            offset += received
        return bytes(output)


class _CudaRuntime:
    def __init__(self):
        last_error: Exception | None = None
        for name in ("libcudart.so.13", "libcudart.so.12", "libcudart.so"):
            try:
                self.lib = ctypes.CDLL(name)
                break
            except OSError as exc:
                last_error = exc
        else:
            raise RdmaTransportError(f"CUDA runtime is unavailable: {last_error}")
        self.lib.cudaMemcpy.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_int,
        ]
        self.lib.cudaMemcpy.restype = ctypes.c_int

    def copy_device_to_host(self, pointer: int, size: int) -> bytearray:
        output = bytearray(size)
        target = (ctypes.c_ubyte * size).from_buffer(output)
        rc = int(self.lib.cudaMemcpy(ctypes.addressof(target), int(pointer), size, 2))
        if rc != 0:
            raise RdmaTransportError(f"cudaMemcpy D2H failed: rc={rc}, bytes={size}")
        return output

    def copy_host_to_device(self, pointer: int, payload: bytes) -> None:
        source = (ctypes.c_ubyte * len(payload)).from_buffer_copy(payload)
        rc = int(
            self.lib.cudaMemcpy(
                int(pointer),
                ctypes.addressof(source),
                len(payload),
                1,
            )
        )
        if rc != 0:
            raise RdmaTransportError(
                f"cudaMemcpy H2D failed: rc={rc}, bytes={len(payload)}"
            )


@dataclass(frozen=True)
class RegisteredRegion:
    base_address: int
    size_bytes: int
    memory_kind: str = "cuda"

    @property
    def end_address(self) -> int:
        return self.base_address + self.size_bytes

    def contains(self, pointer: int, size: int) -> bool:
        return (
            size >= 0
            and pointer >= self.base_address
            and pointer + size <= self.end_address
        )


class RdmaStagedServer:
    """Receive staged iWARP writes and commit them into registered regions."""

    def __init__(
        self,
        *,
        bind_address: str = "",
        port: int,
        capabilities: RdmaCapabilities | None = None,
        backlog: int = 16,
    ):
        self.capabilities = capabilities or detect_rdma_capabilities()
        self.bind_address = bind_address or default_rdma_bind_address(self.capabilities)
        self.port = int(port)
        self.backlog = int(backlog)
        if not self.bind_address:
            raise RdmaTransportError("no IPv4 RDMA bind address is available")
        if not self.capabilities.rdmacm_compatible:
            raise RdmaTransportError("rdmacm server requires an active IP-routed RDMA device")
        self._lib = _RSocketLibrary(self.capabilities.rdmacm_library)
        self._cuda: _CudaRuntime | None = None
        self._listen_fd = -1
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._regions: list[RegisteredRegion] = []
        self._region_lock = threading.RLock()
        self._error: Exception | None = None
        self.completed_batches = 0
        self.completed_bytes = 0

    def register_region(
        self,
        base_address: int,
        size_bytes: int,
        *,
        memory_kind: str = "cuda",
    ) -> None:
        kind = str(memory_kind).lower()
        if kind not in {"host", "cuda"}:
            raise ValueError(f"unsupported memory kind: {memory_kind}")
        region = RegisteredRegion(int(base_address), int(size_bytes), kind)
        if region.base_address <= 0 or region.size_bytes <= 0:
            raise ValueError("registered region requires a positive address and size")
        with self._region_lock:
            self._regions.append(region)

    def register_regions(self, regions: Iterable[RegisteredRegion]) -> None:
        for region in regions:
            self.register_region(
                region.base_address,
                region.size_bytes,
                memory_kind=region.memory_kind,
            )

    def _find_region(self, pointer: int, size: int) -> RegisteredRegion | None:
        with self._region_lock:
            for region in self._regions:
                if region.contains(pointer, size):
                    return region
        return None

    def listener_smoke(self) -> dict:
        """Open a real RDMA-CM listening endpoint and close it immediately."""

        fd = self._lib.bind_listen(self.bind_address, self.port, self.backlog)
        self._lib.close(fd)
        return {
            "ok": True,
            "bind_address": self.bind_address,
            "port": self.port,
            "backend": "rdmacm_rsocket",
        }

    def start(self) -> None:
        if self._thread is not None:
            return
        self._listen_fd = self._lib.bind_listen(
            self.bind_address, self.port, self.backlog
        )
        self._thread = threading.Thread(
            target=self._serve,
            name=f"epd-rdmacm-{self.port}",
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        fd, self._listen_fd = self._listen_fd, -1
        self._lib.close(fd)
        thread, self._thread = self._thread, None
        if thread is not None:
            thread.join(timeout=2.0)

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                client = self._lib.accept(self._listen_fd)
            except Exception as exc:
                if not self._stop.is_set():
                    self._error = exc
                return
            try:
                self._handle_client(client)
            except Exception as exc:
                self._error = exc
                try:
                    message = str(exc).encode("utf-8")[:4096]
                    self._lib.send_all(
                        client,
                        _ACK.pack(_ACK_MAGIC, -1, 0) + message,
                    )
                except Exception:
                    pass
            finally:
                self._lib.close(client)

    def _handle_client(self, fd: int) -> None:
        magic, version, count = _REQUEST_HEADER.unpack(
            self._lib.recv_exact(fd, _REQUEST_HEADER.size)
        )
        if magic != _REQUEST_MAGIC or version != 1:
            raise RdmaTransportError("invalid EPD RDMA request header")
        if count > 4096:
            raise RdmaTransportError(f"RDMA descriptor count exceeds limit: {count}")
        descriptors = [
            _DESCRIPTOR.unpack(self._lib.recv_exact(fd, _DESCRIPTOR.size))
            for _ in range(count)
        ]
        resolved: list[tuple[int, int, RegisteredRegion]] = []
        for pointer, size, declared_kind in descriptors:
            region = self._find_region(int(pointer), int(size))
            if region is None:
                raise RdmaTransportError(
                    f"destination range is not registered: ptr={pointer} bytes={size}"
                )
            expected_kind = _MEMORY_CUDA if region.memory_kind == "cuda" else _MEMORY_HOST
            if int(declared_kind) != expected_kind:
                raise RdmaTransportError(
                    f"destination memory kind mismatch for ptr={pointer}"
                )
            resolved.append((int(pointer), int(size), region))

        total = 0
        for pointer, size, region in resolved:
            payload = self._lib.recv_exact(fd, size)
            if region.memory_kind == "cuda":
                if self._cuda is None:
                    self._cuda = _CudaRuntime()
                self._cuda.copy_host_to_device(pointer, payload)
            else:
                ctypes.memmove(pointer, payload, size)
            total += size
        self.completed_batches += 1
        self.completed_bytes += total
        self._lib.send_all(fd, _ACK.pack(_ACK_MAGIC, 0, total))


class RdmaStagedClient:
    """Send raw host/CUDA pointer descriptors through hardware rdma_cm."""

    def __init__(
        self,
        *,
        capabilities: RdmaCapabilities | None = None,
    ):
        self.capabilities = capabilities or detect_rdma_capabilities()
        if not self.capabilities.rdmacm_compatible:
            raise RdmaTransportError("rdmacm client requires an active IP-routed RDMA device")
        self._lib = _RSocketLibrary(self.capabilities.rdmacm_library)
        self._cuda: _CudaRuntime | None = None

    def write_descriptors(
        self,
        *,
        remote_address: str,
        remote_port: int,
        source_pointers: Sequence[int],
        destination_pointers: Sequence[int],
        lengths: Sequence[int],
        source_memory: str = "cuda",
        destination_memory: str = "cuda",
    ) -> int:
        if not (
            len(source_pointers) == len(destination_pointers) == len(lengths)
        ):
            raise ValueError("source, destination and length lists must match")
        if destination_memory not in {"host", "cuda"}:
            raise ValueError(f"unsupported destination memory: {destination_memory}")
        if source_memory not in {"host", "cuda"}:
            raise ValueError(f"unsupported source memory: {source_memory}")
        kind = _MEMORY_CUDA if destination_memory == "cuda" else _MEMORY_HOST
        fd = self._lib.connect(remote_address, int(remote_port))
        try:
            self._lib.send_all(
                fd,
                _REQUEST_HEADER.pack(_REQUEST_MAGIC, 1, len(lengths)),
            )
            for pointer, size in zip(destination_pointers, lengths):
                self._lib.send_all(
                    fd,
                    _DESCRIPTOR.pack(int(pointer), int(size), kind),
                )
            total = 0
            for pointer, size in zip(source_pointers, lengths):
                size = int(size)
                payload: bytes | bytearray
                if source_memory == "cuda":
                    if self._cuda is None:
                        self._cuda = _CudaRuntime()
                    payload = self._cuda.copy_device_to_host(int(pointer), size)
                else:
                    payload = ctypes.string_at(int(pointer), size)
                self._lib.send_all(fd, payload)
                total += size
            magic, status, committed = _ACK.unpack(
                self._lib.recv_exact(fd, _ACK.size)
            )
            if magic != _ACK_MAGIC or status != 0:
                raise RdmaTransportError(
                    f"RDMA receiver rejected transfer: status={status}"
                )
            if int(committed) != total:
                raise RdmaTransportError(
                    f"RDMA receiver committed {committed} of {total} bytes"
                )
            return total
        finally:
            self._lib.close(fd)


def rdmacm_listener_smoke(
    *,
    bind_address: str = "",
    port: int = 0,
    capabilities: RdmaCapabilities | None = None,
) -> dict:
    caps = capabilities or detect_rdma_capabilities()
    if port <= 0:
        # Pick a regular high port. rsockets cannot bind port zero reliably on
        # all rdma-core versions because querying the assigned port differs from
        # POSIX getsockname.
        port = int(os.getenv("MOONCAKE_EPD_RDMACM_SMOKE_PORT", "47991"))
    server = RdmaStagedServer(
        bind_address=bind_address,
        port=port,
        capabilities=caps,
    )
    return server.listener_smoke()


def rdmacm_cuda_staging_smoke(
    *,
    cuda_device: str = "cuda:0",
    size_bytes: int = 4096,
) -> dict:
    """Exercise the exact D2H/H2D staging implementation on a real GPU."""

    import torch

    if not torch.cuda.is_available():
        raise RdmaTransportError("CUDA is unavailable")
    if size_bytes <= 0:
        raise ValueError("size_bytes must be positive")
    device = torch.device(cuda_device)
    source = torch.arange(size_bytes, dtype=torch.int64, device=device).remainder(251)
    source = source.to(torch.uint8)
    target = torch.zeros_like(source)
    runtime = _CudaRuntime()
    payload = runtime.copy_device_to_host(source.data_ptr(), source.numel())
    runtime.copy_host_to_device(target.data_ptr(), payload)
    torch.cuda.synchronize(device)
    return {
        "ok": bool(torch.equal(source, target)),
        "device": str(device),
        "nbytes": int(source.numel()),
        "backend": "cuda_host_staging",
    }
