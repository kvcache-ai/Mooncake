"""Cross-process HIP IPC transfer of sub-allocated device buffers.

hipIpcGetMemHandle exports the whole allocation, so a buffer registered as a
sub-range of one must still read back its own bytes and not the bytes at the
allocation base. Two processes are required: a loopback transfer inside a single
process never opens an IPC handle.
"""

import multiprocessing as mp
import queue
import unittest

import torch

BUFFERS = 4
NBYTES = 1024 * 1024
EXPECTED = [i + 1 for i in range(BUFFERS)]
TIMEOUT = 120


def _engine():
    from mooncake.engine import TransferEngine

    engine = TransferEngine()
    assert engine.initialize("127.0.0.1", "P2PHANDSHAKE", "hip", "") == 0
    return engine


def _sub_ranges_of_one_allocation():
    torch.cuda.set_device(0)
    pool = torch.zeros(BUFFERS * NBYTES, dtype=torch.uint8, device="cuda:0")
    return [pool[i * NBYTES : (i + 1) * NBYTES] for i in range(BUFFERS)]


def _source(meta, done):
    views = _sub_ranges_of_one_allocation()
    for view, value in zip(views, EXPECTED):
        view.fill_(value)
    torch.cuda.synchronize()

    engine = _engine()
    for view in views:
        assert engine.register_memory(view.data_ptr(), NBYTES) == 0

    meta.put((f"127.0.0.1:{engine.get_rpc_port()}", [v.data_ptr() for v in views]))
    done.get(timeout=TIMEOUT)


def _sink(meta, result):
    segment, addresses = meta.get(timeout=TIMEOUT)
    views = _sub_ranges_of_one_allocation()

    engine = _engine()
    for view in views:
        assert engine.register_memory(view.data_ptr(), NBYTES) == 0
    for view, address in zip(views, addresses):
        read = engine.transfer_sync_read(segment, view.data_ptr(), address, NBYTES)
        assert read == 0, f"transfer_sync_read returned {read}"
    torch.cuda.synchronize()

    observed = []
    for view in views:
        value = int(view[0])
        observed.append(value if bool(torch.all(view == value)) else "mixed")
    result.put(observed)


class TestTransferSubAllocationOnHip(unittest.TestCase):
    def setUp(self):
        if not torch.cuda.is_available():
            raise unittest.SkipTest("ROCm device not available")
        if not getattr(torch.version, "hip", None):
            raise unittest.SkipTest("PyTorch is not built with HIP support")

    def test_each_sub_range_reads_its_own_bytes(self):
        # spawn: the children initialize HIP, which a forked process cannot do.
        ctx = mp.get_context("spawn")
        meta, done, result = ctx.Queue(), ctx.Queue(), ctx.Queue()
        source = ctx.Process(target=_source, args=(meta, done))
        sink = ctx.Process(target=_sink, args=(meta, result))
        source.start()
        sink.start()
        try:
            observed = result.get(timeout=TIMEOUT)
        except queue.Empty:
            self.fail(f"no result: source={source.exitcode} sink={sink.exitcode}")
        finally:
            done.put(True)
            for process in (sink, source):
                process.join(TIMEOUT)
                if process.is_alive():
                    process.terminate()
                    process.join()

        self.assertEqual(observed, EXPECTED)


if __name__ == "__main__":
    unittest.main()
