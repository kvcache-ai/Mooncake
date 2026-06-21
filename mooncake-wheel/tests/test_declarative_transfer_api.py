import unittest
import types

from mooncake.transfer import (
    TransferEndpoint,
    TransferRequest,
    local_buffer,
    remote_buffer,
    transfer,
)


class FakeEngine:
    def __init__(self):
        self.calls = []

    def transfer_sync_write(
        self, target_hostname, buffer, peer_buffer_address, length, transport_hint=""
    ):
        self.calls.append(
            (
                "write",
                target_hostname,
                buffer,
                peer_buffer_address,
                length,
                transport_hint,
            )
        )
        return 0

    def transfer_sync_read(
        self, target_hostname, buffer, peer_buffer_address, length, transport_hint=""
    ):
        self.calls.append(
            (
                "read",
                target_hostname,
                buffer,
                peer_buffer_address,
                length,
                transport_hint,
            )
        )
        return 0


class TestDeclarativeTransferApi(unittest.TestCase):
    def test_transfer_submodule_imports_as_module(self):
        import mooncake.transfer as transfer_module

        self.assertIsInstance(transfer_module, types.ModuleType)
        self.assertIs(transfer_module.transfer, transfer)

    def test_transfer_from_local_to_remote_uses_sync_write(self):
        engine = FakeEngine()
        src = local_buffer(0x1000, length=4096)
        dst = remote_buffer("decoder-0:12345", 0x2000)

        ret = transfer(src, dst, engine=engine, transport_hint="tcp")

        self.assertEqual(ret, 0)
        self.assertEqual(
            engine.calls,
            [("write", "decoder-0:12345", 0x1000, 0x2000, 4096, "tcp")],
        )

    def test_transfer_from_remote_to_local_uses_sync_read(self):
        engine = FakeEngine()
        src = remote_buffer("prefill-0:12345", 0x3000, length=8192)
        dst = local_buffer(0x4000)

        ret = transfer(src, dst, engine=engine)

        self.assertEqual(ret, 0)
        self.assertEqual(
            engine.calls,
            [("read", "prefill-0:12345", 0x4000, 0x3000, 8192, "")],
        )

    def test_transfer_request_executes_with_explicit_length(self):
        engine = FakeEngine()
        request = TransferRequest(
            src=local_buffer(0x5000),
            dst=remote_buffer("decoder-1:12345", 0x6000),
            length=1024,
            transport_hint="rdma",
        )

        ret = request.execute(engine)

        self.assertEqual(ret, 0)
        self.assertEqual(
            engine.calls,
            [("write", "decoder-1:12345", 0x5000, 0x6000, 1024, "rdma")],
        )

    def test_transfer_requires_one_local_and_one_remote_endpoint(self):
        engine = FakeEngine()

        with self.assertRaisesRegex(ValueError, "exactly one remote endpoint"):
            transfer(local_buffer(0x1000, 1), local_buffer(0x2000, 1), engine=engine)

        with self.assertRaisesRegex(ValueError, "exactly one remote endpoint"):
            transfer(
                remote_buffer("prefill-0:12345", 0x1000, 1),
                remote_buffer("decoder-0:12345", 0x2000, 1),
                engine=engine,
            )

    def test_transfer_requires_positive_length(self):
        engine = FakeEngine()

        with self.assertRaisesRegex(ValueError, "length"):
            transfer(
                local_buffer(0x1000),
                remote_buffer("decoder-0:12345", 0x2000),
                engine=engine,
            )

        with self.assertRaisesRegex(ValueError, "positive"):
            transfer(
                local_buffer(0x1000),
                remote_buffer("decoder-0:12345", 0x2000),
                engine=engine,
                length=0,
            )

    def test_transfer_rejects_conflicting_endpoint_lengths(self):
        engine = FakeEngine()

        with self.assertRaisesRegex(ValueError, "conflicting endpoint lengths"):
            transfer(
                local_buffer(0x1000, length=128),
                remote_buffer("decoder-0:12345", 0x2000, length=256),
                engine=engine,
            )

    def test_endpoint_validation_catches_bad_values(self):
        with self.assertRaisesRegex(ValueError, "address"):
            TransferEndpoint.local(-1)

        with self.assertRaisesRegex(ValueError, "address"):
            TransferEndpoint.local(True)

        with self.assertRaisesRegex(ValueError, "hostname"):
            TransferEndpoint.remote("", 0x1000)

        with self.assertRaisesRegex(ValueError, "length"):
            TransferEndpoint.local(0x1000, length=-1)

        with self.assertRaisesRegex(ValueError, "length"):
            TransferEndpoint.local(0x1000, length=True)

    def test_transfer_rejects_non_endpoint_values(self):
        engine = FakeEngine()

        with self.assertRaisesRegex(TypeError, "TransferEndpoint"):
            transfer(
                0x1000, remote_buffer("decoder-0:12345", 0x2000, 1), engine=engine
            )

        with self.assertRaisesRegex(TypeError, "TransferEndpoint"):
            transfer(local_buffer(0x1000, 1), 0x2000, engine=engine)


if __name__ == "__main__":
    unittest.main()
