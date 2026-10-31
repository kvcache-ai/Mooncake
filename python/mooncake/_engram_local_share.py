"""Share one CUDA-VMM CPU allocation between same-UID local caller processes."""

import array
import contextlib
import fcntl
import json
import os
from pathlib import Path
import socket
import struct
import tempfile
import threading
import uuid

from ._engram_shard import fingerprint


class LocalShare:
    def __init__(self, bridge, identity, size, populate):
        self.storage = None
        self.server = None
        self.thread = None
        self.stop = threading.Event()
        root = Path(tempfile.gettempdir()) / f"mooncake-engram-{os.getuid()}"
        root.mkdir(mode=0o700, exist_ok=True)
        if root.stat().st_uid != os.getuid() or root.stat().st_mode & 0o077:
            raise RuntimeError("Local Engram FD directory must be private to its owner")
        self.path = root / (fingerprint(identity) + ".sock")
        with open(str(self.path) + ".lock", "a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as client:
                try:
                    client.connect(str(self.path))
                except (FileNotFoundError, ConnectionRefusedError):
                    self.path.unlink(missing_ok=True)
                else:
                    payload, ancillary, flags, _ = client.recvmsg(
                        4096, socket.CMSG_SPACE(4)
                    )
                    metadata = json.loads(payload)
                    if flags & socket.MSG_CTRUNC or metadata["size"] != size:
                        raise RuntimeError("Local Engram allocation metadata mismatch")
                    self.allocation_id = metadata["allocation_id"]
                    fds = array.array("i")
                    for level, kind, data in ancillary:
                        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                            fds.frombytes(data[: len(data) - len(data) % fds.itemsize])
                    try:
                        if len(fds) != 1:
                            raise RuntimeError("Missing local Engram allocation FD")
                        self.storage = bridge.SharedStorage(size, fds[0])
                    finally:
                        for fd in fds:
                            os.close(fd)
                    return
            self.storage = bridge.SharedStorage(size, -1)
            self.allocation_id = uuid.uuid4().hex
            populate(self.storage.cpu_view())
            self.server = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
            self.server.bind(str(self.path))
            self.server.listen(64)
            self.server.settimeout(0.2)
            self.thread = threading.Thread(
                target=self._serve, args=(size,), daemon=True
            )
            self.thread.start()

    def _serve(self, size):
        while not self.stop.is_set():
            try:
                client, _ = self.server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with client:
                _, uid, _ = struct.unpack(
                    "3i", client.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
                )
                if uid != os.getuid():
                    continue
                with contextlib.suppress(BrokenPipeError):
                    payload = json.dumps(
                        {"size": size, "allocation_id": self.allocation_id}
                    ).encode()
                    client.sendmsg(
                        [payload],
                        [
                            (
                                socket.SOL_SOCKET,
                                socket.SCM_RIGHTS,
                                array.array("i", [self.storage.fd()]),
                            )
                        ],
                    )

    def close(self):
        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout=2)
            self.server.close()
            self.path.unlink(missing_ok=True)
            self.thread = None
        self.storage = None
