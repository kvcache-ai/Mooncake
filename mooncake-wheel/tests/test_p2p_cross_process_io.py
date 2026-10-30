"""Cross-process P2P configuration and remote I/O in reverse and forward modes.

The test process owns A; one spawned process owns B. Run through
scripts/run_p2p_tests.sh with an otherwise empty master and an outer timeout.
"""

from contextlib import contextmanager, ExitStack
import json
import logging
import multiprocessing
import os
import time
import unittest
import uuid

# Set this before loading the native module, including in the spawned process.
os.environ["GLOG_logtostderr"] = "1"
from mooncake.store import (
    MooncakeDistributedStore,
    P2PReadRouteConfig,
    P2PWriteRouteConfig,
)


READ_VALUES = [b"read\x00first", b"read\x00second\xffvalue"]


@contextmanager
def client(direction, prefix, role):
    store = MooncakeDistributedStore()
    try:
        result = store.setup_p2p_real_client(
            local_hostname=os.getenv("LOCAL_HOSTNAME", "127.0.0.1"),
            metadata_server=os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE"),
            tiered_backend_config_json=json.dumps({
                "tiers": [{"type": "DRAM", "capacity": 64 << 20, "priority": 100}],
            }),
            local_buffer_size=16 << 20,
            protocol=os.getenv("PROTOCOL", "tcp"),
            rdma_devices=os.getenv("DEVICE_NAME", ""),
            master_server_addr=os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
            client_rpc_port=0,
            client_rpc_thread_num=2,
            te_async_poll_worker_num=2,
            async_sender_thread_count=0,
            enable_http_server=False,
            enable_metric_collection=False,
            p2p_transfer_direction_mode=direction,
            runtime_config=json.dumps({
                "write": {"remote_weight": 1.0, "local_write_waterline": 0.0},
                "read": {"p2p_config": {"priority_limit": 101}},
            }),
        )
        if result != 0:
            raise RuntimeError(f"Real {role} setup failed: {result}")
        local = P2PWriteRouteConfig()
        local.remote_weight = 0.0
        key = f"{prefix}_{role}_identity"
        check = unittest.TestCase()
        check.assertEqual(store.put(key, b"ready", config=local), 0)
        replicas = store.get_replica_desc(key)
        check.assertEqual(len(replicas), 1, key)
        owner = tuple(replicas[0].client_id)
        logging.info("Real %s ready: mode=%s, endpoint=%s", role, direction,
                     store.get_hostname())
        yield store, owner
    except Exception:
        logging.exception("Real %s failed in %s mode", role, direction)
        raise
    finally:
        result = store.close()
        if result != 0:
            logging.error("Real %s close failed: %s", role, result)
            raise RuntimeError(f"Real {role} close failed: {result}")
        logging.info("Real %s closed", role)


def assert_owner(store, key, value, owner):
    check = unittest.TestCase()
    replicas = store.get_replica_desc(key)
    check.assertEqual(len(replicas), 1, key)
    check.assertEqual(tuple(replicas[0].client_id), owner, key)
    check.assertEqual(replicas[0].object_size, len(value), key)
    check.assertGreater(replicas[0].rpc_port, 0, key)


def wait_for_peer(store, peer_owner, prefix):
    """Retry only disposable startup writes while waiting for peer heartbeats."""
    remote = P2PWriteRouteConfig()
    remote.remote_weight = 1.0
    remote.local_write_waterline = 0.0
    deadline = time.monotonic() + 30
    while True:
        key = f"{prefix}_ready_{uuid.uuid4().hex}"
        result = store.put(key, b"ready", config=remote)
        if result == 0:
            assert_owner(store, key, b"ready", peer_owner)
            return
        if time.monotonic() >= deadline:
            logging.error("Peer routing timed out: last write returned %s", result)
            raise TimeoutError(f"Peer routing timed out: {result}")
        time.sleep(0.05)


def check_remote_roundtrip(store, peer_owner, prefix):
    """The caller writes onto its peer, then reads its own data back remotely."""
    check = unittest.TestCase()
    allowed = P2PReadRouteConfig()
    key, value = f"{prefix}_single", b"remote\x00single\xff"
    check.assertEqual(store.put(key, value), 0)
    assert_owner(store, key, value, peer_owner)
    check.assertEqual(store.get(key, config=allowed), value)

    keys = [f"{prefix}_batch_{i}" for i in range(3)]
    values = [b"a\x00", b"second\x00value", b"third\x00longer\xffvalue"]
    check.assertEqual(store.put_batch(keys, values), 0)
    for key, value in zip(keys, values):
        assert_owner(store, key, value, peer_owner)
    check.assertEqual(
        store.get_batch([keys[2], keys[0], keys[1]], config=allowed),
        [values[2], values[0], values[1]],
    )
    missing = [f"{prefix}_missing_{i}" for i in range(3)]
    check.assertEqual(store.get(missing[0], config=allowed), b"")
    check.assertEqual(
        store.get_batch(
            [missing[0], keys[2], missing[1], keys[0], keys[1], missing[2]],
            config=allowed,
        ),
        [b"", values[2], b"", values[0], values[1], b""],
    )


def peer_main(connection, direction, prefix, read_cases, b_roundtrip):
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [Real B pid=%(process)d] %(message)s",
        force=True,
    )
    with connection, client(direction, prefix, "B") as (b, b_owner):
        check = unittest.TestCase()
        check.assertTrue(connection.poll(60), "Timed out waiting for A's identity")
        a_owner = connection.recv()
        wait_for_peer(b, a_owner, f"{prefix}_b")

        # Only B touches these keys before A tests the restrictive read policy.
        local = P2PWriteRouteConfig()
        local.remote_weight = 0.0
        for name in read_cases:
            key = f"{prefix}_read_{name}"
            keys = [f"{prefix}_read_{name}_{i}" for i in range(2)]
            check.assertEqual(b.put(key, READ_VALUES[0], config=local), 0)
            check.assertEqual(b.put_batch(keys, READ_VALUES, config=local), 0)
            assert_owner(b, key, READ_VALUES[0], b_owner)
            for batch_key, value in zip(keys, READ_VALUES):
                assert_owner(b, batch_key, value, b_owner)
        connection.send((os.getpid(), b.get_hostname(), b_owner))

        check.assertTrue(connection.poll(60), "Timed out waiting for A's checks")
        check.assertEqual(connection.recv(), "a_done")
        if b_roundtrip:
            logging.info("Checking B -> A remote roundtrip")
            check_remote_roundtrip(b, a_owner, f"{prefix}_b_roundtrip")


def stop_peer(peer):
    forced = peer.is_alive()
    if forced:
        logging.error("Real B did not finish; terminating pid=%s", peer.pid)
        peer.terminate()
        peer.join(timeout=10)
        if peer.is_alive():
            logging.error("Real B did not terminate; killing pid=%s", peer.pid)
            peer.kill()
            peer.join(timeout=5)
        if peer.is_alive():
            raise RuntimeError("Real B is still alive after SIGKILL")
    peer.close()
    if forced:
        raise RuntimeError("Real B required forced termination")


class TestP2PCrossProcessIO(unittest.TestCase):
    @contextmanager
    def clients(self, direction, prefix, *, read_cases=(), b_roundtrip=False):
        with ExitStack() as cleanup:
            a, a_owner = cleanup.enter_context(client(direction, prefix, "A"))
            context = multiprocessing.get_context("spawn")
            connection, child_connection = context.Pipe()
            cleanup.callback(connection.close)
            cleanup.callback(child_connection.close)
            b_process = context.Process(
                name="Real B", target=peer_main,
                args=(child_connection, direction, prefix, read_cases, b_roundtrip),
            )
            cleanup.callback(stop_peer, b_process)
            b_process.start()
            child_connection.close()
            connection.send(a_owner)
            self.assertTrue(connection.poll(60), "Timed out waiting for B to be ready")
            b_pid, b_endpoint, b_owner = connection.recv()
            self.assertNotEqual(os.getpid(), b_pid)
            self.assertNotEqual(a.get_hostname(), b_endpoint)
            self.assertNotEqual(a_owner, b_owner)
            wait_for_peer(a, b_owner, f"{prefix}_a")
            yield a, a_owner, b_owner
            # Keep A alive while B completes any requested roundtrip and closes.
            connection.send("a_done")
            b_process.join(timeout=60)
            self.assertEqual(b_process.exitcode, 0, "Real B failed or timed out")

    def test_write_config(self):
        for direction in ("reverse", "forward"):
            prefix = f"p2p_write_{uuid.uuid4().hex}"
            with self.subTest(mode=direction), self.clients(
                direction, prefix,
            ) as (a, a_owner, b_owner):
                allowed = P2PReadRouteConfig()
                allowed.priority_limit = 0
                allowed.max_candidates = 1
                local = P2PWriteRouteConfig()
                local.remote_weight = 0.0
                remote = P2PWriteRouteConfig()
                remote.remote_weight = 1.0
                remote.local_write_waterline = 0.0
                names = ("omitted", "none", "empty", "local", "remote")
                singles = {name: f"{prefix}_write_{name}" for name in names}
                batches = {
                    name: [f"{prefix}_write_{name}_{i}" for i in range(2)]
                    for name in names
                }
                values = [b"write\x00first", b"write\x00second\xffvalue"]

                # Omitted/None use runtime remote routing. An empty config
                # uses its own defaults: our tiny data stays below the waterline.
                self.assertEqual(a.put(singles["omitted"], values[0]), 0)
                self.assertEqual(a.put(singles["none"], values[0], config=None), 0)
                self.assertEqual(
                    a.put(singles["empty"], values[0], config=P2PWriteRouteConfig()), 0,
                )
                self.assertEqual(a.put(singles["local"], values[0], config=local), 0)
                self.assertEqual(a.put(singles["remote"], values[0], config=remote), 0)
                self.assertEqual(a.put_batch(batches["omitted"], values), 0)
                self.assertEqual(a.put_batch(batches["none"], values, config=None), 0)
                self.assertEqual(
                    a.put_batch(batches["empty"], values, config=P2PWriteRouteConfig()), 0,
                )
                self.assertEqual(a.put_batch(batches["local"], values, config=local), 0)
                self.assertEqual(a.put_batch(batches["remote"], values, config=remote), 0)

                for name, expected_owner in (
                    ("omitted", b_owner), ("none", b_owner),
                    ("empty", a_owner), ("local", a_owner), ("remote", b_owner),
                ):
                    with self.subTest(config=name):
                        assert_owner(a, singles[name], values[0], expected_owner)
                        for key, value in zip(batches[name], values):
                            assert_owner(a, key, value, expected_owner)
                        self.assertEqual(a.get(singles[name], config=allowed), values[0])
                        self.assertEqual(a.get_batch(batches[name], config=allowed), values)

    def test_read_config(self):
        for direction in ("reverse", "forward"):
            prefix = f"p2p_read_{uuid.uuid4().hex}"
            read_names = ("omitted", "none", "empty", "allowed")
            read_singles = {name: f"{prefix}_read_{name}" for name in read_names}
            read_batches = {
                name: [f"{prefix}_read_{name}_{i}" for i in range(2)]
                for name in read_names
            }
            with self.subTest(mode=direction), self.clients(
                direction, prefix, read_cases=read_names,
            ) as (a, _a_owner, _b_owner):
                allowed = P2PReadRouteConfig()
                allowed.priority_limit = 0
                allowed.max_candidates = 1
                # Runtime priority 101 excludes B's priority-100 tier.
                # These keys have never been written, queried or read by A.
                self.assertEqual(a.get(read_singles["omitted"]), b"")
                self.assertEqual(a.get(read_singles["none"], config=None), b"")
                self.assertEqual(a.get_batch(read_batches["omitted"]), [b"", b""])
                self.assertEqual(
                    a.get_batch(read_batches["none"], config=None), [b"", b""],
                )
                self.assertEqual(
                    a.get(read_singles["empty"], config=P2PReadRouteConfig()),
                    READ_VALUES[0],
                )
                self.assertEqual(
                    a.get(read_singles["allowed"], config=allowed), READ_VALUES[0],
                )
                self.assertEqual(
                    a.get_batch(read_batches["empty"], config=P2PReadRouteConfig()),
                    READ_VALUES,
                )
                self.assertEqual(
                    a.get_batch(read_batches["allowed"], config=allowed), READ_VALUES,
                )
                # Prove that the rejected keys exist and contain the full values.
                for name in ("omitted", "none"):
                    self.assertEqual(
                        a.get(read_singles[name], config=allowed), READ_VALUES[0],
                    )
                    self.assertEqual(
                        a.get_batch(read_batches[name], config=allowed), READ_VALUES,
                    )

    def test_remote_roundtrip(self):
        for direction in ("reverse", "forward"):
            prefix = f"p2p_roundtrip_{uuid.uuid4().hex}"
            with self.subTest(mode=direction), self.clients(
                direction, prefix, b_roundtrip=True,
            ) as (a, _a_owner, b_owner):
                logging.info("Checking A -> B remote roundtrip in %s mode", direction)
                check_remote_roundtrip(a, b_owner, f"{prefix}_a_roundtrip")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [Real A pid=%(process)d] %(message)s",
    )
    unittest.main(verbosity=2, failfast=True)
