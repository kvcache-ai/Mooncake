"""Exercise real TENT cancellation through the classic Python sync APIs."""

import ctypes
import json
import os
import socket
import sys
import time
import unittest

# The preloaded test shim resolves native methods from this module even when
# transfer_engine is built statically into it.
dlopen_flags = sys.getdlopenflags()
sys.setdlopenflags(dlopen_flags | os.RTLD_GLOBAL)
import engine  # noqa: E402

sys.setdlopenflags(dlopen_flags)


class TentSyncCanceledTest(unittest.TestCase):
    def test_canceled_sync_returns_without_retry_or_timeout(self):
        injector = ctypes.CDLL(os.environ["TENT_CANCEL_INJECTOR"])
        for batch in (False, True):
            for opcode in ("read", "write"):
                with self.subTest(batch=batch, opcode=opcode):
                    self.check_cancellation(injector, batch, opcode)

    def check_cancellation(self, injector, batch, opcode):
        config = {
            "transports": {
                "rdma": {"enable": False},
                "tcp": {"enable": False},
                "shm": {"enable": False},
                "hp_tcp": {
                    "enable": True,
                    "bind_address": "127.0.0.2",
                    "advertise_address": "127.0.0.1",
                    "worker_count": 1,
                    "connect_timeout_ms": 10000,
                    "progress_timeout_ms": 10000,
                },
            },
            "rpc_server_threads": 1,
        }
        # Publish the address of a listening socket that never replies. The
        # actual target binds the same port on a different loopback address;
        # its metadata and registered buffer are real, but data cannot finish.
        with socket.socket() as stalled_peer:
            stalled_peer.bind(("127.0.0.1", 0))
            stalled_peer.listen()
            config["transports"]["hp_tcp"]["port"] = stalled_peer.getsockname()[1]
            os.environ["MC_TENT_CONF"] = json.dumps(config)
            target = engine.TransferEngine()
            self.assertEqual(target.initialize("", "P2PHANDSHAKE", "", ""), 0)
            target_name = f"127.0.0.1:{target.get_rpc_port()}"
            config["transports"]["hp_tcp"].update(bind_address="127.0.0.1", port=0)
            os.environ["MC_TENT_CONF"] = json.dumps(config)
            initiator = engine.TransferEngine()
            self.assertEqual(initiator.initialize("", "P2PHANDSHAKE", "", ""), 0)
            length = 4096
            count = 2 if batch else 1
            source = initiator.allocate_managed_buffer(length * count)
            destination = target.allocate_managed_buffer(length * count)
            self.assertNotEqual(source, 0)
            self.assertNotEqual(destination, 0)
            try:
                injector.tent_test_reset()
                start = time.monotonic()
                if batch:
                    result = getattr(initiator, f"batch_transfer_sync_{opcode}")(
                        target_name,
                        [source + i * length for i in range(count)],
                        [destination + i * length for i in range(count)],
                        [length] * count,
                    )
                else:
                    result = getattr(initiator, f"transfer_sync_{opcode}")(
                        target_name, source, destination, length
                    )
                elapsed = time.monotonic() - start
                self.assertEqual(result, -1)
                self.assertEqual(injector.tent_test_cancellations(), count)
                self.assertEqual(injector.tent_test_submissions(), 1)
                self.assertEqual(injector.tent_test_frees(), 1)
                # MC_TRANSFER_TIMEOUT=5; allow ample scheduling slack while
                # distinguishing a terminal return from the old timeout path.
                self.assertLess(elapsed, 3, f"cancellation took {elapsed:.3f}s")
            finally:
                initiator.free_managed_buffer(source, length * count)
                target.free_managed_buffer(destination, length * count)


if __name__ == "__main__":
    unittest.main()
