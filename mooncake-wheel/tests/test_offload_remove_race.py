import os
import re
import shutil
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.request

from mooncake.store import MooncakeDistributedStore, ReplicateConfig

MB = 1 << 20
ROUNDS = max(1, int(os.getenv("OFFLOAD_RACE_ROUNDS", "24")))
OBJECT_MB = max(1, int(os.getenv("OFFLOAD_RACE_OBJECT_MB", "16")))
OBJECTS_PER_ROUND = max(1, int(os.getenv("OFFLOAD_RACE_OBJECTS", "96")))
SEGMENT = os.getenv("OFFLOAD_RACE_SEGMENT", "1GB")
POLL_SECONDS = max(1.0, float(os.getenv("OFFLOAD_RACE_POLL_SECONDS", "30")))
PUT_TIMEOUT_SECONDS = 120
REMOVE_DELAYS_MS = [0, 50, 200, 500, 1000, 2000]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def binary(name):
    bin_dir = os.getenv("MOONCAKE_BIN_DIR")
    if bin_dir:
        return os.path.join(bin_dir, name)
    import mooncake

    return os.path.join(os.path.dirname(mooncake.__file__), name)


def offload_counters(metrics_port):
    try:
        text = (
            urllib.request.urlopen(
                f"http://127.0.0.1:{metrics_port}/metrics", timeout=2
            )
            .read()
            .decode()
        )
    except OSError:
        return None
    counters = {"enqueued": 0.0, "completed": 0.0}
    for line in text.splitlines():
        name = line.split("{")[0].split(" ")[0]
        if name == "master_offload_enqueued_total":
            counters["enqueued"] += float(line.split()[-1])
        elif name == "master_offload_completed_total":
            counters["completed"] += float(line.split()[-1])
    return counters


class TestOffloadRemoveRace(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workdir = tempfile.mkdtemp(prefix="offload-remove-race-")
        cls.procs = []
        cls.logs = []
        cls.store = None
        try:
            cls._start()
        except BaseException:
            cls._stop()
            raise

    @classmethod
    def _spawn(cls, args, log_name, env=None):
        log = open(os.path.join(cls.workdir, log_name), "w")
        cls.logs.append(log)
        cls.procs.append(
            subprocess.Popen(args, env=env, stdout=log, stderr=subprocess.STDOUT)
        )

    @classmethod
    def _wait_until(cls, ready, what, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            for proc in cls.procs:
                if proc.poll() is not None:
                    raise RuntimeError(f"{proc.args[0]} exited with {proc.returncode}")
            if ready():
                return
            time.sleep(0.2)
        raise RuntimeError(f"timed out waiting for {what}")

    @classmethod
    def _start(cls):
        port, cls.metrics_port = free_port(), free_port()
        address = f"127.0.0.1:{port}"
        cls._spawn(
            [
                binary("mooncake_master"),
                "-port",
                str(port),
                "-metrics_port",
                str(cls.metrics_port),
                "-enable_offload=true",
                "-offload_on_evict=true",
                "-offload_force_evict=false",
                "-enable_disk_eviction=false",
                "-client_ttl=600",
            ],
            "master.log",
        )
        cls._wait_until(
            lambda: offload_counters(cls.metrics_port) is not None, "master"
        )
        ssd = os.path.join(cls.workdir, "ssd")
        os.makedirs(ssd)
        holder_env = dict(
            os.environ,
            MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=ssd,
            MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY="none",
            MOONCAKE_OFFLOAD_ENABLE_EVICTION="false",
            MOONCAKE_OFFLOAD_ENABLE_DISK_WATERMARK_EVICTION="false",
        )
        cls._spawn(
            [
                binary("mooncake_client"),
                "-port",
                str(free_port()),
                "-master_server_address",
                address,
                "-metadata_server",
                "P2PHANDSHAKE",
                "-protocol",
                "tcp",
                "-global_segment_size",
                SEGMENT,
                "-enable_offload=true",
            ],
            "holder.log",
            holder_env,
        )
        time.sleep(5)
        cls._wait_until(lambda: True, "holder start")
        cls.store = MooncakeDistributedStore()
        rc = cls.store.setup(
            "127.0.0.1", "P2PHANDSHAKE", 0, 1024 * MB, "tcp", "", address
        )
        if rc != 0:
            raise RuntimeError(f"store setup failed: {rc}")

    @classmethod
    def _stop(cls):
        try:
            if cls.store is not None:
                cls.store.close()
        finally:
            try:
                cls._stop_processes()
            finally:
                if os.getenv("OFFLOAD_RACE_KEEP_WORKDIR"):
                    print(f"workdir kept: {cls.workdir}")
                else:
                    shutil.rmtree(cls.workdir, ignore_errors=True)

    @classmethod
    def _stop_processes(cls):
        for proc in reversed(cls.procs):
            if proc.poll() is None:
                proc.terminate()
        for proc in cls.procs:
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        for log in cls.logs:
            log.close()

    @classmethod
    def tearDownClass(cls):
        cls._stop()

    def counters(self):
        counters = offload_counters(self.metrics_port)
        self.assertIsNotNone(counters, "master metrics unavailable")
        return counters

    def put(self, key, value, config):
        deadline = time.time() + PUT_TIMEOUT_SECONDS
        while self.store.put_batch([key], [value], config) != 0:
            self.assertLess(time.time(), deadline, f"put of {key} timed out")
            time.sleep(0.02)

    def test_removed_keys_stay_removed_while_offload_in_flight(self):
        value = os.urandom(OBJECT_MB * MB)
        config = ReplicateConfig()
        reappeared = {}
        rounds_removed_in_flight = 0
        for round_id in range(ROUNDS):
            keys = [f"race{round_id}/k{i}" for i in range(OBJECTS_PER_ROUND)]
            before = self.counters()
            for key in keys:
                self.put(key, value, config)
            deadline = time.time() + 30
            while time.time() < deadline:
                now = offload_counters(self.metrics_port)
                if now is not None and now["completed"] > before["completed"]:
                    break
                time.sleep(0.01)
            time.sleep(REMOVE_DELAYS_MS[round_id % len(REMOVE_DELAYS_MS)] / 1000)
            at_remove = offload_counters(self.metrics_port)
            codes = self.store.batch_remove(keys, True)
            removed = [key for key, code in zip(keys, codes) if code == 0]
            if at_remove is not None:
                enqueued = at_remove["enqueued"] - before["enqueued"]
                completed = at_remove["completed"] - before["completed"]
                if enqueued > completed > 0:
                    rounds_removed_in_flight += 1
            deadline = time.time() + POLL_SECONDS
            while time.time() < deadline:
                for key, exists in zip(removed, self.store.batch_is_exist(removed)):
                    if exists == 1:
                        reappeared.setdefault(key, round_id)
                time.sleep(0.5)
            self.store.batch_remove([k for k in keys if k in reappeared], True)
        self.assertGreater(rounds_removed_in_flight, 0)
        self.assertEqual(reappeared, {})
        with open(os.path.join(self.workdir, "master.log")) as log:
            skipped = re.findall(
                r"notify_offload_success_skip_cancelled, completed=(\d+)",
                log.read(),
            )
        self.assertGreater(
            sum(map(int, skipped)), 0, "no completion of a removed key was skipped"
        )


if __name__ == "__main__":
    unittest.main()
