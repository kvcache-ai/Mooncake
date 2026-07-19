"""Truthful worker-level Omni transport implementations.

The CPU same-host path uses POSIX shared memory and passes only descriptors
between processes.  CUDA IPC/NVLink/RDMA are intentionally not aliased to this
path: callers must install a concrete implementation before selecting them.
"""

from __future__ import annotations

import hashlib
import json
import socket
import socketserver
import struct
import threading
import time
import uuid
from multiprocessing import shared_memory
from typing import Any, Dict, Mapping, Optional, Tuple

import numpy as np
import torch

from .protocol import TensorRef


class StageTransportError(RuntimeError):
    """Raised when a stage tensor cannot use the declared transport."""


_MAX_TCP_HEADER_BYTES = 64 * 1024


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = int(size)
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise StageTransportError("TCP relay connection closed before a complete message arrived")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _send_relay_message(
    sock: socket.socket,
    header: Mapping[str, Any],
    payload: bytes = b"",
) -> None:
    encoded = json.dumps(dict(header), separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(encoded) > _MAX_TCP_HEADER_BYTES:
        raise StageTransportError("TCP relay header exceeds the configured limit")
    sock.sendall(struct.pack("!I", len(encoded)))
    sock.sendall(encoded)
    if payload:
        sock.sendall(payload)


def _recv_relay_message(sock: socket.socket) -> tuple[dict[str, Any], bytes]:
    header_size = struct.unpack("!I", _recv_exact(sock, 4))[0]
    if header_size <= 0 or header_size > _MAX_TCP_HEADER_BYTES:
        raise StageTransportError("TCP relay returned an invalid header size")
    try:
        header = json.loads(_recv_exact(sock, header_size).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StageTransportError("TCP relay returned invalid JSON metadata") from exc
    if not isinstance(header, dict):
        raise StageTransportError("TCP relay metadata must be an object")
    payload_size = int(header.get("payload_bytes", 0) or 0)
    if payload_size < 0:
        raise StageTransportError("TCP relay returned a negative payload size")
    return header, _recv_exact(sock, payload_size) if payload_size else b""


def _numpy_dtype(dtype: str) -> np.dtype:
    try:
        return np.dtype(str(dtype).replace("torch.", ""))
    except TypeError as exc:
        raise StageTransportError(f"unsupported shared-memory dtype: {dtype}") from exc


def _torch_dtype(dtype: str) -> torch.dtype:
    name = str(dtype).replace("torch.", "")
    value = getattr(torch, name, None)
    if not isinstance(value, torch.dtype):
        raise StageTransportError(f"unsupported torch dtype: {dtype}")
    return value


def _checksum(array: np.ndarray) -> str:
    return hashlib.sha256(memoryview(array).cast("B")).hexdigest()


def _handoff_shared_memory_tracker(segment: shared_memory.SharedMemory) -> None:
    """Transfer normal-cleanup ownership to the pipeline coordinator.

    Python registers *every* ``SharedMemory`` attachment with that process's
    resource tracker.  Our worker processes only open/publish descriptors;
    the coordinator owns the terminal ``unlink`` once all stage consumers have
    finished.  Without this handoff each exiting worker warns about objects
    that the coordinator correctly unlinked in another process.  This uses the
    standard-library tracker protocol while keeping normal cleanup explicit in
    :meth:`PosixSharedMemoryTransport.release`.
    """

    try:
        from multiprocessing import resource_tracker

        name = getattr(segment, "_name", None)
        if name:
            resource_tracker.unregister(name, "shared_memory")
    except Exception:
        # Older Python versions or alternate multiprocessing implementations
        # may not expose the tracker internals. Normal explicit unlink remains
        # correct; at worst those runtimes retain their warning behavior.
        return


class PosixSharedMemoryTransport:
    """Same-host CPU transport that sends metadata instead of tensor pickles."""

    label = "posix_shm"

    def __init__(self, *, namespace: str = "mooncake_epd") -> None:
        self.namespace = "".join(ch if ch.isalnum() else "_" for ch in namespace)[:32] or "mooncake_epd"

    def publish(self, tensor: torch.Tensor) -> TensorRef:
        if tensor.device.type != "cpu":
            raise StageTransportError(
                "posix_shm only supports CPU tensors; do not label a .to(device) copy as SHM"
            )
        if not tensor.is_contiguous():
            raise StageTransportError("posix_shm requires a contiguous source tensor")
        array = tensor.detach().numpy()
        size = int(array.nbytes)
        if size <= 0:
            raise StageTransportError("cannot publish an empty tensor through POSIX SHM")
        name = f"{self.namespace}_{uuid.uuid4().hex}"
        segment = shared_memory.SharedMemory(name=name, create=True, size=size)
        try:
            destination = np.ndarray(array.shape, dtype=array.dtype, buffer=segment.buf)
            destination[...] = array
            checksum = _checksum(destination)
        finally:
            segment.close()
            _handoff_shared_memory_tracker(segment)
        return TensorRef(
            transport=self.label,
            handle=name,
            shape=tuple(int(value) for value in array.shape),
            dtype=str(tensor.dtype),
            nbytes=size,
            checksum=checksum,
        )

    def open(self, ref: TensorRef, *, verify_checksum: bool = True) -> Tuple[torch.Tensor, shared_memory.SharedMemory]:
        ref.validate()
        if ref.transport != self.label:
            raise StageTransportError(f"cannot read transport={ref.transport} with POSIX SHM")
        dtype = _numpy_dtype(ref.dtype)
        expected = int(np.prod(ref.shape)) * int(dtype.itemsize)
        if expected != int(ref.nbytes):
            raise StageTransportError(f"SHM descriptor size mismatch: expected={expected} ref={ref.nbytes}")
        segment = shared_memory.SharedMemory(name=ref.handle, create=False)
        _handoff_shared_memory_tracker(segment)
        array = np.ndarray(ref.shape, dtype=dtype, buffer=segment.buf)
        if verify_checksum and _checksum(array) != ref.checksum:
            segment.close()
            raise StageTransportError(f"SHM checksum mismatch for {ref.handle}")
        return torch.from_numpy(array), segment

    @staticmethod
    def release(handle: str) -> None:
        try:
            segment = shared_memory.SharedMemory(name=str(handle), create=False)
        except FileNotFoundError:
            return
        try:
            segment.unlink()
        except FileNotFoundError:
            pass
        finally:
            segment.close()


class _RelayTCPServer(socketserver.ThreadingTCPServer):
    """Loopback TCP relay backing descriptor-only worker edges."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        handler,
        *,
        auth_token: str,
        max_payload_bytes: int,
    ) -> None:
        super().__init__(address, handler)
        self.auth_token = str(auth_token)
        self.max_payload_bytes = int(max_payload_bytes)
        self.payloads: Dict[str, bytes] = {}
        self.payload_lock = threading.RLock()
        self.puts = 0
        self.gets = 0
        self.deletes = 0


class _RelayRequestHandler(socketserver.BaseRequestHandler):
    """One authenticated binary relay operation per TCP connection."""

    server: _RelayTCPServer

    def _reply(self, *, ok: bool, error: str = "", payload: bytes = b"") -> None:
        _send_relay_message(
            self.request,
            {
                "ok": bool(ok),
                "error": str(error),
                "payload_bytes": len(payload),
            },
            payload,
        )

    def handle(self) -> None:
        try:
            header, payload = _recv_relay_message(self.request)
            if str(header.get("token") or "") != self.server.auth_token:
                self._reply(ok=False, error="unauthorized relay request")
                return
            operation = str(header.get("op") or "")
            handle = str(header.get("handle") or "")
            if not handle:
                self._reply(ok=False, error="relay handle is required")
                return
            if operation == "put":
                declared = int(header.get("payload_bytes", -1))
                if declared != len(payload) or declared <= 0:
                    self._reply(ok=False, error="relay put payload size mismatch")
                    return
                if declared > self.server.max_payload_bytes:
                    self._reply(ok=False, error="relay payload exceeds configured limit")
                    return
                with self.server.payload_lock:
                    self.server.payloads[handle] = payload
                    self.server.puts += 1
                self._reply(ok=True)
                return
            if operation == "get":
                with self.server.payload_lock:
                    stored = self.server.payloads.get(handle)
                    if stored is not None:
                        self.server.gets += 1
                if stored is None:
                    self._reply(ok=False, error="relay object does not exist")
                    return
                self._reply(ok=True, payload=stored)
                return
            if operation == "delete":
                with self.server.payload_lock:
                    self.server.payloads.pop(handle, None)
                    self.server.deletes += 1
                self._reply(ok=True)
                return
            self._reply(ok=False, error=f"unsupported relay operation: {operation}")
        except Exception as exc:  # pragma: no cover - defensive server boundary
            try:
                self._reply(ok=False, error=f"relay error: {type(exc).__name__}: {exc}")
            except Exception:
                return


class TCPRelayServer:
    """Local authenticated TCP payload relay for actual TCP stage edges.

    Multiprocessing queues carry only :class:`TensorRef` descriptors.  The
    tensor bytes themselves travel over loopback TCP to this relay and from the
    relay to the consumer.  It is intentionally a CPU compatibility adapter,
    not an RDMA or zero-copy implementation.
    """

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        max_payload_bytes: int = 512 * 1024 * 1024,
    ) -> None:
        if int(max_payload_bytes) <= 0:
            raise ValueError("max_payload_bytes must be positive")
        self.host = str(host)
        self.port = int(port)
        self.max_payload_bytes = int(max_payload_bytes)
        self._token = uuid.uuid4().hex
        self._server: Optional[_RelayTCPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> Mapping[str, Any]:
        if self._server is not None:
            return self.endpoint
        server = _RelayTCPServer(
            (self.host, self.port),
            _RelayRequestHandler,
            auth_token=self._token,
            max_payload_bytes=self.max_payload_bytes,
        )
        thread = threading.Thread(
            target=server.serve_forever,
            name="mooncake-omni-tcp-relay",
            daemon=True,
        )
        thread.start()
        self._server = server
        self._thread = thread
        return self.endpoint

    @property
    def endpoint(self) -> Mapping[str, Any]:
        if self._server is None:
            raise RuntimeError("TCP relay server has not been started")
        host, port = self._server.server_address[:2]
        return {"host": str(host), "port": int(port), "token": self._token}

    def stats(self) -> Mapping[str, int]:
        server = self._server
        if server is None:
            return {"puts": 0, "gets": 0, "deletes": 0, "objects": 0}
        with server.payload_lock:
            return {
                "puts": int(server.puts),
                "gets": int(server.gets),
                "deletes": int(server.deletes),
                "objects": len(server.payloads),
            }

    def stop(self) -> None:
        server = self._server
        thread = self._thread
        self._server = None
        self._thread = None
        if server is None:
            return
        server.shutdown()
        server.server_close()
        if thread is not None:
            thread.join(timeout=2.0)


class TCPRelayTransport:
    """CPU tensor transport backed by authenticated TCP relay operations."""

    label = "tcp"

    def __init__(self, endpoint: Mapping[str, Any], *, timeout: float = 15.0) -> None:
        self.host = str(endpoint.get("host") or "").strip()
        self.port = int(endpoint.get("port") or 0)
        self.token = str(endpoint.get("token") or "")
        self.timeout = max(0.1, float(timeout))
        if not self.host or self.port <= 0 or not self.token:
            raise StageTransportError("incomplete TCP relay endpoint")

    def _request(self, operation: str, handle: str, payload: bytes = b"") -> bytes:
        try:
            with socket.create_connection((self.host, self.port), timeout=self.timeout) as sock:
                sock.settimeout(self.timeout)
                _send_relay_message(
                    sock,
                    {
                        "op": str(operation),
                        "handle": str(handle),
                        "token": self.token,
                        "payload_bytes": len(payload),
                    },
                    payload,
                )
                response, body = _recv_relay_message(sock)
        except OSError as exc:
            raise StageTransportError(
                f"TCP relay request failed for {operation}:{handle}: {exc}"
            ) from exc
        if not bool(response.get("ok")):
            raise StageTransportError(
                f"TCP relay {operation} failed for {handle}: {response.get('error') or 'unknown error'}"
            )
        return body

    def publish(self, tensor: torch.Tensor) -> TensorRef:
        if tensor.device.type != "cpu":
            raise StageTransportError(
                "TCP relay currently supports CPU tensors only; do not label a CUDA copy as TCP direct"
            )
        if not tensor.is_contiguous():
            raise StageTransportError("TCP relay requires a contiguous source tensor")
        array = tensor.detach().numpy()
        payload = array.tobytes(order="C")
        if not payload:
            raise StageTransportError("cannot publish an empty tensor through TCP relay")
        handle = uuid.uuid4().hex
        self._request("put", handle, payload)
        return TensorRef(
            transport=self.label,
            handle=handle,
            shape=tuple(int(value) for value in array.shape),
            dtype=str(tensor.dtype),
            nbytes=len(payload),
            checksum=_checksum(array),
        )

    def open(self, ref: TensorRef, *, verify_checksum: bool = True) -> Tuple[torch.Tensor, None]:
        ref.validate()
        if ref.transport != self.label:
            raise StageTransportError(f"cannot read transport={ref.transport} with TCP relay")
        dtype = _numpy_dtype(ref.dtype)
        expected = int(np.prod(ref.shape)) * int(dtype.itemsize)
        if expected != int(ref.nbytes):
            raise StageTransportError(f"TCP descriptor size mismatch: expected={expected} ref={ref.nbytes}")
        payload = self._request("get", ref.handle)
        if len(payload) != expected:
            raise StageTransportError(
                f"TCP relay payload size mismatch for {ref.handle}: expected={expected} got={len(payload)}"
            )
        array = np.frombuffer(payload, dtype=dtype).reshape(ref.shape).copy()
        if verify_checksum and _checksum(array) != ref.checksum:
            raise StageTransportError(f"TCP relay checksum mismatch for {ref.handle}")
        return torch.from_numpy(array), None

    def release(self, handle: str) -> None:
        try:
            self._request("delete", str(handle))
        except StageTransportError:
            # Cleanup is idempotent. A terminal consumer may already have
            # removed a descriptor after a cancellation race.
            return


def build_stage_transport(
    transport: str,
    *,
    namespace: str,
    tcp_endpoint: Optional[Mapping[str, Any]] = None,
) -> PosixSharedMemoryTransport | TCPRelayTransport:
    """Construct only concrete transport implementations.

    CUDA IPC, NVLink and RDMA deliberately have no compatibility alias here.
    Selecting one without its real adapter must fail at pipeline construction.
    """

    selected = str(transport).strip().lower()
    if selected == PosixSharedMemoryTransport.label:
        return PosixSharedMemoryTransport(namespace=namespace)
    if selected == TCPRelayTransport.label:
        if tcp_endpoint is None:
            raise StageTransportError("TCP transport requires a running relay endpoint")
        return TCPRelayTransport(tcp_endpoint)
    raise StageTransportError(
        f"transport={selected} has no installed concrete Omni adapter; "
        "do not relabel POSIX SHM or TCP as CUDA IPC, NVLink, or RDMA"
    )
