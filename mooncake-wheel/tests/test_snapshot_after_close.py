"""Regression test: a retained ranged-read snapshot must not outlive close().

A ``prepare_get_into_ranges_snapshot`` snapshot used to hold a strong
reference to the native store client. ``close()`` tore the client down but
the snapshot kept it (and its transfer-engine segment) alive, so the master
kept serving a COMPLETE MEMORY replica for the closed client's endpoint. A
later reader could then be routed to that stale replica and receive a
failed transfer or an all-zero buffer instead of the surviving DISK replica
(see issue #5464).

Test scenario:
  1. Writer store A (SSD offload enabled) puts one 64 KiB value.
  2. Poll until the key has a COMPLETE disk replica, so a copy survives
     without A's memory segment.
  3. Prepare a ranged-read snapshot on A and close A while the snapshot is
     still referenced (the buggy lifetime extension).
  4. Reader store B must observe no memory replica for the key once close
     finishes, and ``get`` must return bit-exact bytes from the disk
     replica without another put.
  5. The retained snapshot must not be usable through B: the store
     mismatch check must reject it.

Prerequisites:
  - ``mooncake_master`` running with master config containing:
      ``--enable_offload=true``
      ``--root_fs_dir=<dir>``
      ``--default_kv_lease_ttl=500`` (the refresh-deadline test waits 2s,
          past the read-lease midpoint; scale ``REFRESH_WAIT_SECONDS``
          with a different lease TTL)
  - ``MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=<dir>`` set on the client side.
"""

import ctypes
import os
import time
import unittest

from mooncake.store import MooncakeDistributedStore

VALUE_SIZE = 64 * 1024
# Offload copies memory replicas to disk on a background schedule; poll
# instead of relying on a single fixed sleep.
OFFLOAD_WAIT_SECONDS = int(os.getenv("OFFLOAD_WAIT_SECONDS", "45"))
CLOSE_SETTLE_SECONDS = int(os.getenv("CLOSE_SETTLE_SECONDS", "30"))
POLL_INTERVAL_SECONDS = 0.5
# refresh_at is the read-lease midpoint: with the CI master's
# --default_kv_lease_ttl=500 that is ~250 ms, so two seconds is safely
# past the deadline.
REFRESH_WAIT_SECONDS = float(os.getenv("REFRESH_WAIT_SECONDS", "2"))


def make_store(reader: bool) -> MooncakeDistributedStore:
    store = MooncakeDistributedStore()
    config = {
        "local_hostname": os.getenv("LOCAL_HOSTNAME", "127.0.0.1"),
        "metadata_server": os.getenv("MC_METADATA_SERVER", "P2PHANDSHAKE"),
        "global_segment_size": str(16 * 1024 * 1024),
        "local_buffer_size": str(64 * 1024 * 1024),
        "protocol": os.getenv("PROTOCOL", "tcp"),
        "rdma_devices": os.getenv("DEVICE_NAME", ""),
        "master_server_addr": os.getenv("MASTER_SERVER", "127.0.0.1:50051"),
        "enable_ssd_offload": "false" if reader else "true",
    }
    if not reader:
        offload_path = os.getenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", "")
        if offload_path:
            config["ssd_offload_path"] = offload_path
    retcode = store.setup(config)
    if retcode:
        raise RuntimeError(f"Failed to setup store client: {retcode}")
    return store


def replica_rows(store: MooncakeDistributedStore, key: str):
    rows = []
    for desc in store.get_replica_desc(key):
        rows.append(
            {
                "status": str(desc.status),
                "memory": desc.is_memory_replica(),
                "disk": desc.is_disk_replica(),
            }
        )
    return rows


def has_complete_disk_replica(store: MooncakeDistributedStore, key: str) -> bool:
    return any(
        row["disk"] and "COMPLETE" in row["status"] for row in replica_rows(store, key)
    )


def has_memory_replica(store: MooncakeDistributedStore, key: str) -> bool:
    return any(row["memory"] for row in replica_rows(store, key))


def read_through_snapshot(store, snapshot, key):
    """One ranged read through the snapshot into a fresh registered buffer."""
    buf = (ctypes.c_ubyte * VALUE_SIZE).from_buffer(bytearray(VALUE_SIZE))
    ptr = ctypes.addressof(buf)
    if store.register_buffer(ptr, VALUE_SIZE) != 0:
        raise RuntimeError("register_buffer failed")
    try:
        rc = store.get_into_ranges_from_snapshot(
            snapshot, [ptr], [[key]], [[[0]]], [[[0]]], [[[VALUE_SIZE]]]
        )
        return rc, bytes(buf)
    finally:
        if store.unregister_buffer(ptr) != 0:
            raise RuntimeError("unregister_buffer failed")


class TestSnapshotAfterClose(unittest.TestCase):
    def test_retained_snapshot_does_not_survive_close(self):
        key = "snapshot-after-close/value"
        value = bytes(65 + i % 26 for i in range(VALUE_SIZE))
        writer = make_store(reader=False)
        reader = None
        snapshot = None
        try:
            self.assertEqual(writer.put(key, value), 0)

            deadline = time.monotonic() + OFFLOAD_WAIT_SECONDS
            while time.monotonic() < deadline:
                if has_complete_disk_replica(writer, key):
                    break
                time.sleep(POLL_INTERVAL_SECONDS)
            else:
                self.fail(
                    f"no COMPLETE disk replica within {OFFLOAD_WAIT_SECONDS}s; "
                    f"is the master running with --enable_offload=true?"
                )

            # Retain the snapshot across close: the regression is that the
            # snapshot's strong client reference used to keep the closed
            # client's segment published.
            snapshot = writer.prepare_get_into_ranges_snapshot([key])
            self.assertEqual(writer.close(), 0)

            reader = make_store(reader=True)
            deadline = time.monotonic() + CLOSE_SETTLE_SECONDS
            while time.monotonic() < deadline:
                if not has_memory_replica(reader, key):
                    break
                time.sleep(POLL_INTERVAL_SECONDS)
            rows = replica_rows(reader, key)
            self.assertFalse(
                any(row["memory"] for row in rows),
                f"closed client's memory replica still visible: {rows}",
            )

            # The surviving disk replica must serve bit-exact bytes with no
            # further put.
            self.assertEqual(reader.get(key), value)

            # A retained snapshot whose store is gone must be rejected
            # instead of reading through the closed client. belongs_to()
            # fails, which pybind surfaces as ValueError.
            with self.assertRaisesRegex(ValueError, "belongs to another Store"):
                reader.get_into_ranges_from_snapshot(
                    snapshot, [], [[key]], [[[]]], [[[]]], [[[]]]
                )
        finally:
            if snapshot is not None:
                del snapshot
            if reader is not None:
                reader.close()
            writer.close()

    def test_snapshot_reuse_across_refresh_deadline(self):
        """Read through the snapshot twice, straddling the refresh deadline.

        The second read happens after refresh_at (the read-lease midpoint),
        so it exercises the weak-reference promotion plus the metadata
        refresh path through the real Python binding, on a Store that is
        still open.
        """
        key = "snapshot-refresh-deadline/value"
        value = bytes(97 + i % 26 for i in range(VALUE_SIZE))
        writer = make_store(reader=False)
        snapshot = None
        try:
            self.assertEqual(writer.put(key, value), 0)
            snapshot = writer.prepare_get_into_ranges_snapshot([key])

            rc, data = read_through_snapshot(writer, snapshot, key)
            self.assertEqual(rc, [[[VALUE_SIZE]]])
            self.assertEqual(data, value)

            time.sleep(REFRESH_WAIT_SECONDS)

            rc, data = read_through_snapshot(writer, snapshot, key)
            self.assertEqual(rc, [[[VALUE_SIZE]]])
            self.assertEqual(data, value)
        finally:
            del snapshot
            writer.close()


if __name__ == "__main__":
    unittest.main()
