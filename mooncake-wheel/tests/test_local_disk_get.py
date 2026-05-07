"""End-to-end test for ``store.get(key)`` on LOCAL_DISK-bearing keys.

Reproduces the user-facing bug fixed by this PR: when offload-on-eviction
has placed bytes on a peer client's local SSD, single-key ``store.get``
must return them correctly. Pre-fix, the read could either crash with
``RuntimeError("Expected DiskDescriptor")`` (if ``FindFirstCompleteReplica``
selected the LOCAL_DISK descriptor) or silently return wrong/empty bytes
(if the outer ``get_buffer_internal`` and inner ``Client::Get`` disagreed
on which replica to use, e.g., during the async ``PutEnd(DISK)`` race
window). Post-fix, both layers see the same COMPLETE replica and the
LOCAL_DISK path routes through the offload RPC.

The fix landed in three places:
  - ``Client::TransferRead`` returns ``ErrorCode::UNSUPPORTED_REPLICA_TYPE``
    on LOCAL_DISK instead of throwing.
  - ``RealClient::get_buffer_internal`` adds a status filter so it picks a
    COMPLETE replica even when ``GetPreferredReplica`` returned a
    PROCESSING one, and feeds a single-replica ``QueryResult`` to the
    inner ``Client::Get`` so the two layers can't disagree.
  - LOCAL_DISK reads route to ``batch_get_into_offload_object_internal``
    (the existing offload-RPC helper) instead of falling through to the
    local TransferSubmitter (which can't reach the holder client's SSD).

Reproduction conditions
-----------------------
The race window we care about opens up under offload pressure: many
small async ``PutEnd(DISK)`` tasks contending while the bucket-flush
heartbeat races ahead, so for some subset of keys the master sees DISK
in PROCESSING and LOCAL_DISK in COMPLETE simultaneously. We force this
by lowering ``MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT`` and
``MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES`` so the bucket flushes
quickly with few keys, while putting enough keys to overflow the
segment.

Prerequisites:
  - ``mooncake_master`` running with:
      ``--enable_offload=true``
      ``--root_fs_dir=<dir>``  (so the client's FileStorage initializes
          via ``GetStorageConfig``; without this no LOCAL_DISK replicas
          are ever created)
  - ``MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=<dir>`` set on the client side.
  - ``MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT=10`` and
    ``MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=10485760``.
"""

import os
import time
import unittest
from mooncake.store import MooncakeDistributedStore

DEFAULT_KV_LEASE_TTL_MS = int(os.getenv("DEFAULT_KV_LEASE_TTL", "5000"))

# 32 MB is small enough that the 64 x 1 MB workload reliably overflows
# past the eviction high watermark.
SEGMENT_SIZE = int(os.getenv("SEGMENT_SIZE_BYTES", str(32 * 1024 * 1024)))
LOCAL_BUFFER_SIZE = int(
    os.getenv("LOCAL_BUFFER_SIZE_BYTES", str(64 * 1024 * 1024))
)

# Wait for two heartbeat ticks (default 10s each) so offload completes
# and the master has had a chance to add LOCAL_DISK replicas + free
# MEMORY of the offloaded keys.
EVICTION_WAIT_SECONDS = int(os.getenv("EVICTION_WAIT_SECONDS", "25"))


def _setup_client(store):
    retcode = store.setup(
        os.getenv("LOCAL_HOSTNAME", "localhost"),
        os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE"),
        SEGMENT_SIZE,
        LOCAL_BUFFER_SIZE,
        os.getenv("PROTOCOL", "tcp"),
        os.getenv("DEVICE_NAME", ""),
        os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
        None,  # engine
        True,  # enable_ssd_offload — required for FileStorage / LOCAL_DISK
    )
    if retcode:
        raise RuntimeError(f"Failed to setup store client. Return code: {retcode}")


class TestLocalDiskGet(unittest.TestCase):
    """``store.get(key)`` returns the original bytes for keys that have
    been offloaded to LOCAL_DISK after eviction. End-to-end coverage for
    the read-path fix in this PR."""

    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        _setup_client(cls.store)

    def test_get_returns_bytes_after_eviction(self):
        VALUE_SIZE = 1024 * 1024  # 1 MB
        NUM_KEYS = 64  # 64 MB workload into 32 MB segment forces eviction

        timestamp = int(time.time())
        keys = [f"ldg_{i}_{timestamp}" for i in range(NUM_KEYS)]
        reference = {}

        try:
            # Phase 1: PUT until DRAM overflows. The master accepts puts
            # until the high watermark; the rest fail with NO_AVAILABLE_HANDLE
            # (which is fine — we just need *some* keys to land on disk).
            for key in keys:
                value = os.urandom(VALUE_SIZE)
                if self.store.put(key, value) == 0:
                    reference[key] = value
            self.assertGreater(
                len(reference),
                0,
                "No PUTs succeeded — cannot exercise the read path.",
            )

            # Phase 2: wait for offload heartbeats to flush bucket files
            # and for master eviction to free DRAM in favor of LOCAL_DISK.
            time.sleep(EVICTION_WAIT_SECONDS)

            # Phase 3: read every successfully-PUT key back. Pre-fix, any
            # key whose read picks a LOCAL_DISK descriptor (either as the
            # only complete replica or due to the async-PutEnd race
            # leaving DISK in PROCESSING) would crash with
            # RuntimeError("Expected DiskDescriptor"). Post-fix, the
            # status filter in get_buffer_internal picks a COMPLETE
            # replica and the LOCAL_DISK branch routes through the
            # offload-RPC helper, returning the original bytes.
            mismatches = []
            errors = []
            for key, expected in reference.items():
                try:
                    got = self.store.get(key)
                except RuntimeError as e:
                    errors.append((key, str(e)))
                    continue
                if got != expected:
                    mismatches.append(
                        (key, len(got) if got else 0, len(expected))
                    )

            self.assertEqual(
                errors,
                [],
                f"store.get raised RuntimeError on {len(errors)} key(s) — "
                f"the LOCAL_DISK descriptor bug regressed. Sample: "
                f"{errors[:3]}",
            )
            self.assertEqual(
                mismatches,
                [],
                f"store.get returned wrong/empty bytes on {len(mismatches)} "
                f"key(s) — the get_buffer_internal replica-selection fix "
                f"regressed. Sample (key, got_len, expected_len): "
                f"{mismatches[:3]}",
            )
        finally:
            for key in keys:
                try:
                    self.store.remove(key)
                except Exception:
                    pass
            time.sleep(DEFAULT_KV_LEASE_TTL_MS / 1000 + 0.5)


if __name__ == "__main__":
    unittest.main()
