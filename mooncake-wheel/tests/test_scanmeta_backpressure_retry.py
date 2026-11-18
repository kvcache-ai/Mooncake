"""Regression test: startup ScanMeta re-registration must retry OpLog
backpressure instead of failing (issue #5563).

With SSD offload enabled, a client that restarts over an existing SSD
cache re-registers the cached objects via ``FileStorage::Init() ->
ScanMeta() -> NotifyOffloadSuccess()``. When the burst exceeds the HA
OpLog in-flight window (``--oplog_batch_max_entries``), the master returns
``TASK_PENDING_LIMIT_EXCEEDED`` (-1401) purely as flow control. Before the
fix, the client treated this transient backpressure as fatal and the whole
initialization failed — deterministically for TP workers restarting
together (ranks x scan limit >= window).

Test scenario (CPU/TCP only, no accelerator), against a master whose OpLog
window (256) is below one registration burst: the burst competes with the
window for in-flight slots, and with 550 cached entries the retry path is
exercised in practice — although the master reserves per key, so slots
can free between reservations and a backpressure-free run is legal.
Deterministic backpressure/retry behavior (attempts, backoff schedule,
split-range completeness, error propagation) is covered by the C++ unit
test ``mooncake-store/tests/file_storage_notify_retry_test.cpp``.
  1. Writer client puts 550 keys and bounded-waits until every key has a
     COMPLETE LOCAL_DISK replica on the client's own ``ssd_offload_path``,
     then reads a sample back byte-exact.
  2. The writer closes; the SSD cache must still hold the expected bucket
     files, so the re-registration below has real work to do. A fresh
     client over the same ``ssd_offload_path`` runs the startup
     re-registration. Baseline (unfixed) fails setup with -1401 when the
     window is exhausted; with the fix it must succeed.

What this e2e deliberately does NOT assert: reads through the fresh
client after the restart. The dead writer stays the registered replica
owner until the master's client offboarding removes the stale entries,
and the fresh client's re-registration only lands afterwards — but the
current client has no trigger to re-register once setup already
succeeded, so read recovery has no bounded timeline. That gap is a
master/client lifecycle issue independent of this fix (tracked as a
follow-up).

Prerequisites (provided by ``scripts/ci/run_ssd_offload_smoke.sh``):
  - ``etcd`` reachable at ``ETCD_ENDPOINT`` (default 127.0.0.1:2379)
  - ``mooncake_master`` running with:
      ``--enable_ha=true --ha_backend_type=etcd``
      ``--ha_backend_connstring=<ETCD_ENDPOINT>``
      ``--enable_oplog=true --oplog_batch_max_entries=256``
      ``--enable_offload=true`` and WITHOUT ``--root_fs_dir``: with a
      root_fs_dir the master registers shared DISK replicas for every key,
      and the scenario would no longer exercise the client-side LOCAL_DISK
      path the ScanMeta re-registration manages.
  - ``MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=<dir>`` on the client side.
"""

import glob
import math
import os
import re
import tempfile
import time
import unittest

# Bucket sizing keeps the master's own recovery traffic below the OpLog
# window: each bucket is renotified as one batch, so a bucket must fit the
# window on its own.
os.environ.setdefault("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "64")
os.environ.setdefault("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "2")
# Surface MasterClient's VLOG(1) request lines (they carry tasks_count=)
# so the reopen-phase registration below is observable. Must be set
# before the native logging is initialized; forced (not setdefault) so
# inherited GLOG_v / MC_LOG_LEVEL / MC_LOG_DIR cannot disable the
# observation this test relies on (WARNING filters the INFO request
# lines; a log dir redirects them away from stderr).
os.environ["GLOG_v"] = "1"
os.environ["GLOG_alsologtostderr"] = "1"
os.environ["MC_LOG_LEVEL"] = "INFO"

from mooncake.store import MooncakeDistributedStore  # noqa: E402

N_KEYS = int(os.getenv("SCANMETA_TEST_KEYS", "550"))
VALUE_SIZE = 4096
# Read back after the setdefault above: the effective bucket limit
# (respects an operator-provided override).
BUCKET_KEYS_LIMIT = int(os.getenv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "64"))
# Offload drains bucket by bucket on a background schedule; bounded-wait
# for the whole key set to reach the client's local disk.
OFFLOAD_WAIT_SECONDS = int(os.getenv("OFFLOAD_WAIT_SECONDS", "120"))
OFFLOAD_POLL_SECONDS = 3.0
# Sample derived from the actual key set: both ends, the first binary
# split midpoint, and the bucket boundary.
SAMPLE_INDICES = tuple(
    sorted(
        i
        for i in {
            0,
            BUCKET_KEYS_LIMIT - 1,
            BUCKET_KEYS_LIMIT,
            N_KEYS // 2 - 1,
            N_KEYS // 2,
            N_KEYS - 1,
        }
        if 0 <= i < N_KEYS
    )
)


def key_at(index):
    return f"scanmeta-backpressure/key_{index:05d}"


def value_at(index):
    return bytes(65 + (index + j) % 26 for j in range(VALUE_SIZE))


def make_store(master_addr):
    store = MooncakeDistributedStore()
    config = {
        "local_hostname": os.getenv("LOCAL_HOSTNAME", "127.0.0.1"),
        "metadata_server": os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE"),
        "global_segment_size": str(256 * 1024 * 1024),
        "local_buffer_size": str(512 * 1024 * 1024),
        "protocol": os.getenv("PROTOCOL", "tcp"),
        "rdma_devices": os.getenv("DEVICE_NAME", ""),
        "master_server_addr": master_addr,
        "enable_ssd_offload": "true",
        "ssd_offload_path": os.getenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", ""),
    }
    retcode = store.setup(config)
    if retcode:
        raise RuntimeError(f"Failed to setup store client: {retcode}")
    return store


def has_complete_local_disk(store, key):
    try:
        descs = store.get_replica_desc(key)
    except Exception:
        return False
    return any(d.is_local_disk_replica() and "COMPLETE" in str(d.status) for d in descs)


def count_bucket_meta_files():
    ssd_dir = os.getenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", "")
    return len(glob.glob(os.path.join(ssd_dir, "*.meta")))


class TestScanMetaBackpressureRetry(unittest.TestCase):
    def test_startup_rescan_survives_oplog_backpressure(self):
        master_addr = os.getenv("MASTER_SERVER", "127.0.0.1:50051")
        keys = [key_at(i) for i in range(N_KEYS)]
        writer = None
        try:
            writer = make_store(master_addr)
            for i, key in enumerate(keys):
                self.assertEqual(writer.put(key, value_at(i)), 0)

            # Bounded-wait for the whole set on the client's local disk:
            # this drains the background offload completely, so the reopen
            # burst below covers every key under test.
            offloaded = 0
            deadline = time.monotonic() + OFFLOAD_WAIT_SECONDS
            while time.monotonic() < deadline:
                offloaded = sum(
                    1 for key in keys if has_complete_local_disk(writer, key)
                )
                if offloaded == N_KEYS:
                    break
                time.sleep(OFFLOAD_POLL_SECONDS)
            if offloaded != N_KEYS:
                self.fail(
                    f"only {offloaded}/{N_KEYS} keys have COMPLETE LOCAL_DISK "
                    f"replicas within {OFFLOAD_WAIT_SECONDS}s; is the client "
                    "offload engine running?"
                )

            # The offloaded data must read back byte-exact while the
            # owner is alive (reads may be served from the memory
            # replica; the point here is data correctness, while the
            # registration check above proves the local-disk copies).
            for index in SAMPLE_INDICES:
                self.assertEqual(
                    writer.get(key_at(index)),
                    value_at(index),
                    f"mismatched bytes for key index {index}",
                )

            # Close and re-open over the same SSD path. The cache must
            # still hold the offloaded buckets, so the startup
            # re-registration below has real work to do; baseline
            # (unfixed) fails setup with -1401 when the window is
            # exhausted, with the fix it must succeed. (Reads through
            # this client are not asserted — see module docstring.)
            self.assertEqual(writer.close(), 0)
            writer = None

            min_buckets = math.ceil(N_KEYS / BUCKET_KEYS_LIMIT)
            self.assertGreaterEqual(
                count_bucket_meta_files(),
                min_buckets,
                "SSD cache lost its bucket files after close; the reopen "
                "would re-register nothing",
            )

            # Capture the native client's stderr around the reopen:
            # MasterClient logs each NotifyOffloadSuccess request with
            # tasks_count=, so the registration work of the second setup
            # is directly observable (a setup that skipped registration
            # would leave no tasks_count>0 lines).
            time.sleep(3)
            rescan = None
            try:
                saved_stderr = os.dup(2)
                try:
                    # errors="replace": a stray invalid byte in the
                    # native log must never mask the original failure.
                    with tempfile.TemporaryFile(
                        mode="w+", encoding="utf-8", errors="replace"
                    ) as capture:
                        os.dup2(capture.fileno(), 2)
                        try:
                            rescan = make_store(master_addr)
                        except Exception as exc:
                            # Read the tail while the capture file is
                            # still open, so a failing setup keeps its
                            # diagnostics.
                            capture.seek(0)
                            tail = capture.read()[-3000:]
                            raise RuntimeError(
                                f"reopen setup failed: {exc}\n"
                                f"stderr during reopen setup:\n{tail}"
                            ) from None
                        finally:
                            # Restore stderr on every path (setup failure,
                            # interruption); without this the process
                            # would keep logging into the temp file.
                            os.dup2(saved_stderr, 2)
                        capture.seek(0)
                        captured = capture.read()
                finally:
                    os.close(saved_stderr)

                registered = sum(
                    int(count) for count in re.findall(r"tasks_count=(\d+)", captured)
                )
                # Count only the retry helper's own WARNING lines; the RPC
                # response lines echo the same error text.
                backpressure_hits = captured.count(
                    "hit transient backpressure"
                ) + captured.count("hit sustained backpressure")
                # Report the reopen-phase observation (the capture above
                # swallows that phase's stderr, so outer log greps cannot
                # see it). A zero here is legal: the master reserves per
                # key and slots can free between reservations.
                print(
                    f"reopen-phase: registration entries={registered}, "
                    f"backpressure warnings={backpressure_hits}",
                    flush=True,
                )
                self.assertGreaterEqual(
                    registered,
                    N_KEYS,
                    "reopen setup issued no registration RPCs with "
                    "tasks_count > 0 (startup registration skipped?)\n"
                    f"stderr tail:\n{captured[-3000:]}",
                )
            finally:
                if rescan is not None:
                    self.assertEqual(rescan.close(), 0)
        finally:
            if writer is not None:
                writer.close()


if __name__ == "__main__":
    unittest.main()
