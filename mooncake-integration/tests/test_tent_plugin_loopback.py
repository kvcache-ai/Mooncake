import ctypes
import os
import unittest

import engine


class TentPluginLoopbackTest(unittest.TestCase):
    """The python extension hosts TENT transport plugins: MC_TENT_CONF points
    plugins/dir at the build's plugin directory and transport_hint selects the
    loopback plugin by name."""

    def make_engines(self, protocol):
        # protocol="tcp" makes the classic shim force TCP for memory segments
        # (ConfigHelper::forceTcp); any other protocol keeps the TENT config.
        # RDMA itself is disabled by MC_TENT_CONF.
        target = engine.TransferEngine()
        initiator = engine.TransferEngine()
        for te in (target, initiator):
            self.assertEqual(
                te.initialize("127.0.0.1", "P2PHANDSHAKE", protocol, ""), 0
            )
        return target, initiator, f"127.0.0.1:{target.get_rpc_port()}"

    def test_loopback_hint(self):
        target, initiator, name = self.make_engines("rdma")

        # Already loaded by the engine: RTLD_NOLOAD fails otherwise.
        plugin = ctypes.CDLL(
            os.path.join(
                os.environ["TENT_TEST_PLUGIN_DIR"], "libtent_xport_loopback.so"
            ),
            mode=os.RTLD_NOLOAD | os.RTLD_LOCAL,
        )
        copied = plugin.tent_xport_loopback_bytes_copied
        copied.restype = ctypes.c_uint64

        size = 1 << 20
        target_buffer = ctypes.create_string_buffer(size)
        source_buffer = ctypes.create_string_buffer(os.urandom(size), size)
        target_addr = ctypes.addressof(target_buffer)
        source_addr = ctypes.addressof(source_buffer)
        self.assertEqual(target.register_memory(target_addr, size), 0)
        self.addCleanup(lambda: target.unregister_memory(target_addr))
        self.assertEqual(initiator.register_memory(source_addr, size), 0)
        self.addCleanup(lambda: initiator.unregister_memory(source_addr))

        before = copied()
        self.assertEqual(
            initiator.transfer_sync_write(
                name, source_addr, target_addr, size, transport_hint="loopback"
            ),
            0,
        )
        self.assertEqual(target_buffer.raw, source_buffer.raw)
        self.assertEqual(copied() - before, size)

        expected = target_buffer.raw
        ctypes.memset(source_addr, 0, size)
        self.assertEqual(
            initiator.transfer_sync_read(
                name, source_addr, target_addr, size, transport_hint="loopback"
            ),
            0,
        )
        self.assertEqual(source_buffer.raw, expected)
        self.assertEqual(copied() - before, 2 * size)

        with self.assertRaises(ValueError):
            initiator.transfer_sync_write(
                name, source_addr, target_addr, size, transport_hint="no_such"
            )

    def test_force_tcp_wins_over_plugin_hint(self):
        target, initiator, name = self.make_engines("tcp")
        size = 4096
        target_buffer = ctypes.create_string_buffer(size)
        source_buffer = ctypes.create_string_buffer(b"x" * size, size)
        target_addr = ctypes.addressof(target_buffer)
        source_addr = ctypes.addressof(source_buffer)
        self.assertEqual(target.register_memory(target_addr, size), 0)
        self.addCleanup(lambda: target.unregister_memory(target_addr))
        self.assertEqual(initiator.register_memory(source_addr, size), 0)
        self.addCleanup(lambda: initiator.unregister_memory(source_addr))
        self.assertNotEqual(
            initiator.transfer_sync_write(
                name, source_addr, target_addr, size, transport_hint="loopback"
            ),
            0,
        )
        self.assertEqual(
            initiator.transfer_sync_write(name, source_addr, target_addr, size), 0
        )
        self.assertEqual(target_buffer.raw, source_buffer.raw)


if __name__ == "__main__":
    unittest.main()
