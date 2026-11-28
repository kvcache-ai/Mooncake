"""TCP integration tests using the actual mooncake_master/mooncake_client binaries.

Run after building/installing the C++ Store and Python bindings:
  MC_RUN_CLIENT_PREFETCH_E2E=1 python python/tests/ssd/test_mooncake_client_prefetch.py

MOONCAKE_MASTER_BIN and MOONCAKE_CLIENT_BIN can override the binaries on PATH.
Each test owns its master, SSD holder, temporary storage and client connections.
"""

import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import unittest

MB = 1024 * 1024


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@unittest.skipUnless(
    os.getenv("MC_RUN_CLIENT_PREFETCH_E2E") == "1",
    "set MC_RUN_CLIENT_PREFETCH_E2E=1 to run the standalone binary integration tests",
)
class TestMooncakeClientPrefetch(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from mooncake.store import ExistOptions, MooncakeDistributedStore, ReplicaStatus

        cls.Store = MooncakeDistributedStore
        cls.Options = ExistOptions
        cls.complete = ReplicaStatus.COMPLETE
        cls.master_bin = shutil.which(
            os.getenv("MOONCAKE_MASTER_BIN", "mooncake_master")
        )
        cls.client_bin = shutil.which(
            os.getenv("MOONCAKE_CLIENT_BIN", "mooncake_client")
        )
        if not cls.master_bin or not cls.client_bin:
            raise RuntimeError(
                "mooncake_master and mooncake_client must be built and on PATH"
            )
        cls.master_bin = str(Path(cls.master_bin).resolve())
        cls.client_bin = str(Path(cls.client_bin).resolve())

    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="mooncake-client-prefetch-")
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        (self.directory / "ssd").mkdir()
        (self.directory / "master").mkdir()
        self.processes = []
        self.addCleanup(self.stop_processes)

    def stop_processes(self):
        for process, _ in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)

    def diagnostics(self):
        return "\n".join(
            f"{path.name} (exit={process.poll()}):\n{path.read_text(errors='replace')[-4000:]}"
            for process, path in self.processes
        )

    def wait_until(self, condition, description, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.assertTrue(
                all(process.poll() is None for process, _ in self.processes),
                self.diagnostics(),
            )
            value = condition()
            if value:
                return value
            # The master grants 500 ms read leases, including on metadata queries.
            time.sleep(1)
        self.fail(f"Timed out waiting for {description}\n{self.diagnostics()}")

    def start_process(self, name, command, port):
        env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("MOONCAKE_OFFLOAD_")
        }
        env.update(
            MC_USE_TENT="0",
            MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=str(self.directory / "ssd"),
            MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS="1",
            MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES=str(16 * MB),
            MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT="1",
            MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=str(MB),
        )
        path = self.directory / f"{name}.log"
        with path.open("w") as log:
            process = subprocess.Popen(
                command, cwd=self.directory, env=env, stdout=log, stderr=log
            )
        self.processes.append((process, path))

        def listening():
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    return True
            except OSError:
                return False

        self.wait_until(listening, f"{name} to listen on {port}")

    def new_store(self, segment_size=0):
        store = self.Store()
        self.addCleanup(store.close)
        self.assertEqual(
            store.setup(
                {
                    "local_hostname": "127.0.0.1",
                    "metadata_server": "P2PHANDSHAKE",
                    "global_segment_size": segment_size,
                    "local_buffer_size": 16 * MB,
                    "protocol": "tcp",
                    "master_server_addr": self.master_address,
                    "enable_ssd_offload": False,
                    "enable_ssd_prefetch": True,
                    "ssd_prefetch_cooldown_sec": 0,
                    "ssd_prefetch_dedup_ttl_sec": 0,
                }
            ),
            0,
        )
        return store

    def start_cluster(self, dedicated_rpc=True, prefetch=True, client_port=None):
        master_port = free_port()
        self.master_address = f"127.0.0.1:{master_port}"
        self.client_port = client_port or free_port()
        self.start_process(
            "master",
            [
                self.master_bin,
                f"--port={master_port}",
                "--metrics_port=0",
                "--default_kv_lease_ttl=500",
                "--enable_offload=true",
                "--offload_on_evict=true",
                "--promotion_on_hit=false",
                f"--root_fs_dir={self.directory / 'master'}",
            ],
            master_port,
        )
        command = [
            self.client_bin,
            "--host=127.0.0.1",
            f"--port={self.client_port}",
            f"--master_server_address={self.master_address}",
            "--metadata_server=P2PHANDSHAKE",
            "--protocol=tcp",
            "--global_segment_size=32MB",
            "--local_buffer_size=16MB",
            "--enable_offload=true",
            f"--start_offload_rpc_server={str(dedicated_rpc).lower()}",
        ]
        if prefetch:
            command += [
                "--enable_ssd_prefetch=true",
                "--ssd_prefetch_cooldown_sec=0",
                "--ssd_prefetch_dedup_ttl_sec=0",
            ]
        self.start_process("holder", command, self.client_port)
        # All object data initially lives in the independent holder process.
        self.reader = self.new_store()

    def replicas(self, key):
        return self.reader.batch_get_replica_desc([key]).get(key, [])

    def has_memory(self, key):
        return any(
            replica.is_memory_replica() and replica.status == self.complete
            for replica in self.replicas(key)
        )

    def cold_key(self):
        keys = []
        for index in range(96):
            key = f"standalone_prefetch_{index}"
            if self.reader.put(key, bytes([index]) * MB) == 0:
                keys.append((key, index))
        self.assertTrue(keys, "No puts succeeded")

        def find():
            descriptions = self.reader.batch_get_replica_desc([key for key, _ in keys])
            for key, index in keys:
                replicas = descriptions.get(key, [])
                if any(
                    replica.is_local_disk_replica() and replica.status == self.complete
                    for replica in replicas
                ) and not any(replica.is_memory_replica() for replica in replicas):
                    return key, bytes([index]) * MB
            return None

        key, value = self.wait_until(find, "a COMPLETE SSD-only object")
        # Supply headroom after eviction so promotion cannot fail for lack of DRAM.
        # The initiating reader still contributes no segment and owns no SSD.
        self.new_store(64 * MB)
        return key, value

    def assert_stays_cold(self, key):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            self.assertFalse(self.has_memory(key))
            time.sleep(0.25)

    def check_prefetch(
        self, dedicated_rpc=True, batch=False, dummy=False, client_port=None
    ):
        self.start_cluster(dedicated_rpc=dedicated_rpc, client_port=client_port)
        key, value = self.cold_key()
        probe = self.reader
        if dummy:
            probe = self.Store()
            self.addCleanup(probe.close)
            self.assertEqual(
                probe.setup_dummy(16 * MB, 16 * MB, f"127.0.0.1:{self.client_port}"), 0
            )
        self.assertEqual(probe.is_exist(key), 1)
        self.assertEqual(probe.is_exist(key, self.Options()), 1)
        self.assert_stays_cold(key)
        options = self.Options()
        options.prefetch_to_memory = True
        if batch:
            self.assertEqual(
                probe.batch_is_exist([key, "missing", key], options), [1, 0, 1]
            )
        else:
            self.assertEqual(probe.is_exist(key, options), 1)
        # Do not get until the MEMORY replica is complete: get-side kicks must
        # never make a broken exist-prefetch path appear to work.
        self.wait_until(
            lambda: self.has_memory(key), "prefetch to publish COMPLETE MEMORY"
        )
        self.assertEqual(bytes(self.reader.get(key)), value)

    def test_remote_single_exist(self):
        self.check_prefetch()

    def test_remote_batch_via_main_rpc_with_endpoint_alias(self):
        # Python's no-SSD reader advertises local_rpc_addr=127.0.0.1:50052.
        # It must still delegate to this distinct process's matching endpoint.
        self.check_prefetch(dedicated_rpc=False, batch=True, client_port=50052)

    def test_dummy_single_exist(self):
        self.check_prefetch(dummy=True)

    def test_dummy_batch_exist_via_main_rpc(self):
        self.check_prefetch(dedicated_rpc=False, batch=True, dummy=True)

    def test_holder_prefetch_is_disabled_by_default(self):
        self.start_cluster(prefetch=False)
        key, value = self.cold_key()
        options = self.Options()
        options.prefetch_to_memory = True
        self.assertEqual(self.reader.is_exist(key, options), 1)
        self.assertEqual(self.reader.batch_is_exist([key], options), [1])
        self.assert_stays_cold(key)
        self.assertEqual(bytes(self.reader.get(key)), value)

    def test_cli_rejects_invalid_durations(self):
        for name in ("ssd_prefetch_cooldown_sec", "ssd_prefetch_dedup_ttl_sec"):
            for value in (-1, 2**63 - 1):
                with self.subTest(name=name, value=value):
                    result = subprocess.run(
                        [self.client_bin, f"--{name}={value}"],
                        cwd=self.directory,
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("SSD prefetch durations", result.stderr)


if __name__ == "__main__":
    unittest.main()
