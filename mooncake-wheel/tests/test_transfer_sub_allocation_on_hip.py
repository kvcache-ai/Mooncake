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


def _source(meta, done, result):
    try:
        views = _sub_ranges_of_one_allocation()
        for view, value in zip(views, EXPECTED):
            view.fill_(value)
        torch.cuda.synchronize()

        engine = _engine()
        addresses = [view.data_ptr() for view in views]
        rc = engine.batch_register_memory(addresses, [NBYTES] * BUFFERS)
        assert rc == 0, f"batch_register_memory returned {rc}"

        meta.put((f"127.0.0.1:{engine.get_rpc_port()}", addresses))
        done.get(timeout=TIMEOUT)

        # `views` is still alive, so this is last-alias teardown of the shared
        # registration, not an unregister-after-free.
        rc = engine.batch_unregister_memory(addresses)
        assert rc == 0, f"batch_unregister_memory returned {rc}"
        result.put(("source", None))
    except Exception as e:  # noqa: BLE001
        result.put(("source", str(e)))


def _sink(meta, result):
    try:
        segment, addresses = meta.get(timeout=TIMEOUT)
        views = _sub_ranges_of_one_allocation()

        engine = _engine()
        rc = engine.batch_register_memory(
            [view.data_ptr() for view in views], [NBYTES] * BUFFERS
        )
        assert rc == 0, f"batch_register_memory returned {rc}"
        for view, address in zip(views, addresses):
            read = engine.transfer_sync_read(
                segment, view.data_ptr(), address, NBYTES
            )
            assert read == 0, f"transfer_sync_read returned {read}"
        torch.cuda.synchronize()

        observed = []
        for view in views:
            value = int(view[0])
            observed.append(value if bool(torch.all(view == value)) else "mixed")

        # Report the payload before unregistering so a failure below surfaces
        # as itself, not as a missing result.
        result.put(("sink", observed))
        rc = engine.batch_unregister_memory(
            [view.data_ptr() for view in views]
        )
        assert rc == 0, f"batch_unregister_memory returned {rc}"
        result.put(("sink", None))
    except Exception as e:  # noqa: BLE001
        result.put(("sink", str(e)))


def _alias_lifecycle(result):
    try:
        torch.cuda.set_device(0)
        # Offset into the allocation so no sub-range starts at its base.
        pool = torch.zeros(
            (BUFFERS + 1) * NBYTES, dtype=torch.uint8, device="cuda:0"
        )
        views = [
            pool[(i + 1) * NBYTES : (i + 2) * NBYTES] for i in range(BUFFERS)
        ]
        engine = _engine()
        addresses = [view.data_ptr() for view in views]

        for address in addresses:
            assert engine.register_memory(address, NBYTES) == 0

        # A double unregister of one alias fails but must not consume
        # another alias's reference on the shared registration.
        assert engine.unregister_memory(addresses[0]) == 0
        assert engine.unregister_memory(addresses[0]) != 0
        for address in addresses[1:]:
            assert engine.unregister_memory(address) == 0

        # The last unregister removed the registration, so no alias of the
        # allocation can be released again.
        assert engine.unregister_memory(addresses[-1]) != 0
        result.put(("alias", None))
    except Exception as e:  # noqa: BLE001
        result.put(("alias", str(e)))


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
        source = ctx.Process(target=_source, args=(meta, done, result))
        sink = ctx.Process(target=_sink, args=(meta, result))
        source.start()
        sink.start()
        observed = None
        try:
            finished = set()
            while len(finished) < 2:
                who, payload = result.get(timeout=TIMEOUT)
                if isinstance(payload, str):
                    self.fail(f"{who} failed: {payload}")
                if payload is not None:
                    observed = payload
                    # Transfers are done; let the source tear down its
                    # registration while its tensors are still alive.
                    done.put(True)
                else:
                    finished.add(who)
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

    def test_alias_registration_lifecycle(self):
        ctx = mp.get_context("spawn")
        result = ctx.Queue()
        child = ctx.Process(target=_alias_lifecycle, args=(result,))
        child.start()
        try:
            _, payload = result.get(timeout=TIMEOUT)
        except queue.Empty:
            self.fail(f"no result: exitcode={child.exitcode}")
        finally:
            child.join(TIMEOUT)
            if child.is_alive():
                child.terminate()
                child.join()

        if payload is not None:
            self.fail(f"alias failed: {payload}")


if __name__ == "__main__":
    unittest.main()
