"""Opt-in three-GPU regression for the public peer-reset binding.

Build reshard_ipc_trace.cpp as a shared library with CUDA headers and -ldl,
then set LD_PRELOAD to it and MC_RUN_RESHARD_IPC_TESTS=1 before running this file.
"""

import ctypes as C
import json
import multiprocessing as mp
import os
from pathlib import Path
import signal
import subprocess
import sys
import unittest

SIZE = 1024 * 1024
CASES = (
    "subset",
    "empty",
    "unknown",
    "duplicate",
    "all",
    "alias",
    "refresh",
    "local",
    "failure",
    "partial",
)


def checked(rc):
    if rc:
        # A failed transfer does not authorize releasing exported allocations.
        print(
            f"operation failed: {rc}; isolated process group requires termination",
            flush=True,
        )
        while True:
            signal.pause()


class GPU:
    def __init__(self, device):
        self.lib = C.CDLL("libcudart.so", mode=C.RTLD_GLOBAL)
        self.lib.cudaMalloc.argtypes = [C.POINTER(C.c_void_p), C.c_size_t]
        self.lib.cudaMemset.argtypes = [C.c_void_p, C.c_int, C.c_size_t]
        self.lib.cudaMemcpy.argtypes = [C.c_void_p, C.c_void_p, C.c_size_t, C.c_int]
        checked(self.lib.cudaSetDevice(device))

    def alloc(self, marker):
        ptr = C.c_void_p()
        checked(self.lib.cudaMalloc(C.byref(ptr), SIZE))
        checked(self.lib.cudaMemset(ptr, marker, SIZE))
        self.sync()
        return ptr.value

    def sync(self):
        checked(self.lib.cudaDeviceSynchronize())

    def verify(self, ptr, marker):
        host = C.create_string_buffer(SIZE)
        checked(self.lib.cudaMemcpy(host, ptr, SIZE, 2))
        assert host.raw == bytes([marker]) * SIZE


def engine():
    from mooncake.engine import TransferEngine

    result = TransferEngine()
    checked(result.initialize("127.0.0.1", "P2PHANDSHAKE", "nvlink_intra", ""))
    return result


def receive(conn):
    assert conn.poll(30), "peer command timed out"
    return conn.recv()


def target(conn, device):
    gpu, te = GPU(device), engine()
    ptr = gpu.alloc(0)
    checked(te.register_memory(ptr, SIZE))
    conn.send((f"127.0.0.1:{te.get_rpc_port()}", ptr))
    while True:
        command = receive(conn)
        if command == "stop":
            checked(te.unregister_memory(ptr))
            del te
            return
        if command == "reregister":
            checked(te.unregister_memory(ptr))
            old = ptr
            # Keep the old exported allocation live until the initiator exits.
            ptr = gpu.alloc(0)
            assert ptr != old
            checked(te.register_memory(ptr, SIZE))
            conn.send(ptr)
        else:
            gpu.sync()
            gpu.verify(ptr, command)
            conn.send("ok")


def run_case(case):
    trace = C.CDLL(None)
    for name in ("probe_opened", "probe_closed"):
        getattr(trace, name).argtypes = [C.c_int]
        getattr(trace, name).restype = C.c_void_p
    trace.probe_fail_once.argtypes = [C.c_void_p]
    context = mp.get_context("spawn")
    pairs = [context.Pipe() for _ in range(2)]
    processes = [
        context.Process(target=target, args=(pairs[i][1], i + 1)) for i in range(2)
    ]
    for process in processes:
        process.start()
    peers = [list(receive(pair[0])) for pair in pairs]
    if case == "alias":
        peers[0][0] = peers[0][0].replace("127.0.0.1", "localhost")
    gpu, te = GPU(0), engine()
    source = gpu.alloc(0x51)
    checked(te.register_memory(source, SIZE))
    markers = iter(range(1, 16))

    def transfer(owner, i):
        marker = next(markers)
        checked(gpu.lib.cudaMemset(source, marker, SIZE))
        gpu.sync()
        checked(
            owner.transfer_sync_write(
                peers[i][0], source, peers[i][1], SIZE, "nvlink_intra"
            )
        )
        gpu.sync()
        pairs[i][0].send(marker)
        assert receive(pairs[i][0]) == "ok"

    for i in range(2):
        transfer(te, i)
    assert trace.probe_opens() == 2
    a, b = trace.probe_opened(0), trace.probe_opened(1)
    selection = {
        "subset": [peers[0][0]],
        "empty": [],
        "unknown": ["not-an-endpoint"],
        "duplicate": [peers[0][0], peers[0][0]],
        "all": [peer[0] for peer in peers],
        "alias": [peers[0][0]],
        "refresh": [peers[0][0]],
        "local": [f"127.0.0.1:{te.get_rpc_port()}"],
        "failure": [peers[0][0]],
        "partial": [peer[0] for peer in peers],
    }[case]
    if case == "refresh":
        pairs[0][0].send("reregister")
        peers[0][1] = receive(pairs[0][0])
    if case == "failure":
        trace.probe_fail_once(a)
    elif case == "partial":
        trace.probe_fail_once(b)
    before = trace.probe_closes()
    rc = te.reset_reshard_peers_quiescent(selection)
    attempts = [trace.probe_closed(i) for i in range(before, trace.probe_closes())]
    expected = (
        []
        if case in ("empty", "unknown", "local")
        else ([a, b] if case in ("all", "partial") else [a])
    )
    matches = sorted(attempts) == sorted(expected)
    print(
        json.dumps(
            dict(
                case=case,
                rc=rc,
                opened=[a, b],
                close_attempts=attempts,
                expected=expected,
                scope_pass=matches,
            )
        ),
        flush=True,
    )
    assert (rc != 0) == (case in ("local", "failure", "partial")), rc
    if case not in ("local", "failure", "partial"):
        for i in range(2):
            transfer(te, i)
        print(json.dumps(dict(markers="ok")), flush=True)
        checked(te.unregister_memory(source))
    # A failed reset requires engine restart; do not resume transfers or retry.
    before_destroy = trace.probe_closes()
    del te
    if case in ("failure", "partial"):
        destroyed = [
            trace.probe_closed(i) for i in range(before_destroy, trace.probe_closes())
        ]
        expected_owned = [b] if case == "partial" else [a, b]
        assert sorted(destroyed) == sorted(
            expected_owned
        ), "failed entry must remain owned"
    for pair in pairs:
        pair[0].send("stop")
    for process in processes:
        process.join(30)
        assert process.exitcode == 0, process.exitcode
    assert matches, "public reset touched an unlisted CUDA IPC mapping"


@unittest.skipUnless(
    os.environ.get("MC_RUN_RESHARD_IPC_TESTS") == "1",
    "requires three GPUs and the IPC trace library",
)
class PeerResetTest(unittest.TestCase):
    def test_public_peer_reset(self):
        for case in CASES:
            with self.subTest(case=case):
                process = subprocess.Popen(
                    [sys.executable, str(Path(__file__).resolve()), case],
                    start_new_session=True,
                )
                try:
                    self.assertEqual(process.wait(timeout=120), 0)
                finally:
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] in CASES:
        run_case(sys.argv[1])
    else:
        unittest.main()
