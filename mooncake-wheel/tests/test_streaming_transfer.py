"""
Tests for streaming transfer: progressive put + progressive get.

These tests verify the streaming write (ProgressivePutHandle) and
streaming read (ProgressiveGetHandle, ScatterReadHandle) functionality.

Requirements:
- A running mooncake master server
- A running metadata server (etcd/redis/p2p)
- Environment variables: LOCAL_HOSTNAME, MC_METADATA_SERVER, MASTER_SERVER
"""

import ctypes
import os
import struct
import threading
import time
import unittest

from mooncake.store import MooncakeDistributedStore


def get_client(store, local_buffer_size_param=None):
    """Initialize and setup the distributed store client."""
    protocol = os.getenv("PROTOCOL", "tcp")
    device_name = os.getenv("DEVICE_NAME", "ibp6s0")
    local_hostname = os.getenv("LOCAL_HOSTNAME", "localhost")
    metadata_server = os.getenv(
        "MC_METADATA_SERVER", "http://127.0.0.1:8080/metadata"
    )
    global_segment_size = 3200 * 1024 * 1024  # 3200 MB
    local_buffer_size = (
        local_buffer_size_param
        if local_buffer_size_param is not None
        else 512 * 1024 * 1024  # 512 MB
    )
    master_server_address = os.getenv("MASTER_SERVER", "127.0.0.1:50051")

    retcode = store.setup(
        local_hostname,
        metadata_server,
        global_segment_size,
        local_buffer_size,
        protocol,
        device_name,
        master_server_address,
    )

    if retcode:
        raise RuntimeError(
            f"Failed to setup store client. Return code: {retcode}"
        )


def allocate_registered_buffer(store, size):
    """Allocate a buffer from the store's memory pool and return ptr."""
    ptr = store.alloc(size)
    if ptr == 0:
        raise RuntimeError(f"Failed to allocate {size} bytes from memory pool")
    return ptr


def read_range_via_get_into_ranges(store, key, buffer_ptr, dst_offset,
                                   src_offset, size):
    """Read a single range using get_into_ranges API.

    Returns bytes_read (positive) or error code (negative).
    """
    # get_into_ranges signature:
    #   buffer_ptrs: [ptr]
    #   all_keys: [[key]]
    #   all_dst_offsets: [[[dst_offset]]]
    #   all_src_offsets: [[[src_offset]]]
    #   all_sizes: [[[size]]]
    results = store.get_into_ranges(
        [buffer_ptr],
        [[key]],
        [[[dst_offset]]],
        [[[src_offset]]],
        [[[size]]],
    )
    # results shape: [1][1][1] -> single value
    return results[0][0][0]


class TestProgressivePut(unittest.TestCase):
    """Test progressive (streaming) put operations."""

    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def test_progressive_put_basic(self):
        """Test basic progressive put: write chunks then verify readback."""
        key = "test_progressive_put_basic"
        chunk_size = 4096
        num_chunks = 4
        total_size = chunk_size * num_chunks

        # Start progressive put
        handle = self.store.progressive_put(key, total_size, num_chunks)
        self.assertIsNotNone(handle, "progressive_put should return a handle")
        self.assertEqual(handle.num_chunks, num_chunks)
        self.assertEqual(handle.completed_count, 0)
        self.assertFalse(handle.is_sealed)

        # Write chunks with known data pattern
        src_ptr = allocate_registered_buffer(self.store, total_size)
        for i in range(num_chunks):
            # Fill chunk with pattern: each byte = chunk_index + 1
            offset = i * chunk_size
            ctypes.memset(src_ptr + offset, i + 1, chunk_size)
            ret = handle.write_chunk(src_ptr + offset, offset, chunk_size)
            self.assertEqual(ret, 0, f"write_chunk {i} should succeed")
            self.assertEqual(handle.completed_count, i + 1)

        # Seal
        ret = handle.seal()
        self.assertEqual(ret, 0, "seal should succeed")
        self.assertTrue(handle.is_sealed)

        # Read back full object and verify
        dst_ptr = allocate_registered_buffer(self.store, total_size)
        ctypes.memset(dst_ptr, 0, total_size)
        bytes_read = self.store.get_into(key, dst_ptr, total_size)
        self.assertEqual(bytes_read, total_size)

        # Verify data pattern
        dst_buf = (ctypes.c_char * total_size).from_address(dst_ptr)
        for i in range(num_chunks):
            offset = i * chunk_size
            expected = bytes([i + 1]) * chunk_size
            self.assertEqual(
                dst_buf[offset : offset + chunk_size], expected,
                f"Chunk {i} data mismatch"
            )

        # Cleanup
        self.store.remove(key)
        self.store.remove(key + "/__progress__")

    def test_progressive_put_progress_tracking(self):
        """Test that the sideband progress key is updated after each chunk."""
        key = "test_progressive_put_progress"
        chunk_size = 1024
        num_chunks = 3
        total_size = chunk_size * num_chunks

        handle = self.store.progressive_put(key, total_size, num_chunks)
        self.assertIsNotNone(handle)

        progress_key = key + "/__progress__"
        src_ptr = allocate_registered_buffer(self.store, total_size)

        for i in range(num_chunks):
            offset = i * chunk_size
            ctypes.memset(src_ptr + offset, 0xAB, chunk_size)
            ret = handle.write_chunk(src_ptr + offset, offset, chunk_size)
            self.assertEqual(ret, 0)

            # Read progress key - should contain (i+1) as uint64_t
            progress_buf = allocate_registered_buffer(self.store, 8)
            bytes_read = self.store.get_into(progress_key, progress_buf, 8)
            self.assertEqual(bytes_read, 8, "Progress key should be 8 bytes")
            progress_val = struct.unpack(
                "Q", (ctypes.c_char * 8).from_address(progress_buf)[:]
            )[0]
            self.assertEqual(
                progress_val, i + 1,
                f"Progress should be {i+1} after writing chunk {i}"
            )

        handle.seal()
        self.store.remove(key)
        self.store.remove(progress_key)

    def test_progressive_put_seal_prevents_further_writes(self):
        """Test that write_chunk fails after seal."""
        key = "test_progressive_put_seal_block"
        chunk_size = 1024
        num_chunks = 2
        total_size = chunk_size * num_chunks

        handle = self.store.progressive_put(key, total_size, num_chunks)
        self.assertIsNotNone(handle)

        src_ptr = allocate_registered_buffer(self.store, chunk_size)
        ctypes.memset(src_ptr, 0xFF, chunk_size)

        # Write first chunk
        ret = handle.write_chunk(src_ptr, 0, chunk_size)
        self.assertEqual(ret, 0)

        # Seal
        handle.seal()

        # Attempt to write after seal should fail
        ret = handle.write_chunk(src_ptr, chunk_size, chunk_size)
        self.assertNotEqual(ret, 0, "write_chunk after seal should fail")

        self.store.remove(key)
        self.store.remove(key + "/__progress__")


class TestProgressiveGet(unittest.TestCase):
    """Test progressive (streaming) get operations."""

    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def test_progressive_get_basic(self):
        """Test progressive get with per-chunk completion tracking."""
        key = "test_progressive_get_basic"
        chunk_size = 4096
        data = bytes([i % 256 for i in range(chunk_size * 4)])

        # Put full object
        ret = self.store.put(key, data)
        self.assertEqual(ret, 0)

        # Progressive get
        total_size = len(data)
        dst_ptr = allocate_registered_buffer(self.store, total_size)
        handle = self.store.progressive_get(key, dst_ptr, total_size, chunk_size)
        self.assertIsNotNone(handle, "progressive_get should return a handle")
        self.assertEqual(handle.num_chunks, 4)

        # Wait for all chunks
        ret = handle.wait_all()
        self.assertEqual(ret, 0, "wait_all should succeed")
        self.assertEqual(handle.completed_count, 4)

        # Verify data
        dst_buf = (ctypes.c_char * total_size).from_address(dst_ptr)
        self.assertEqual(dst_buf[:], data)

        self.store.remove(key)

    def test_progressive_get_per_chunk_wait(self):
        """Test waiting for individual chunks."""
        key = "test_progressive_get_per_chunk"
        chunk_size = 2048
        num_chunks = 3
        total_size = chunk_size * num_chunks
        data = os.urandom(total_size)

        ret = self.store.put(key, data)
        self.assertEqual(ret, 0)

        dst_ptr = allocate_registered_buffer(self.store, total_size)
        handle = self.store.progressive_get(key, dst_ptr, total_size, chunk_size)
        self.assertIsNotNone(handle)

        # Wait for each chunk individually
        for i in range(num_chunks):
            ret = handle.wait_chunk(i)
            self.assertEqual(ret, 0, f"wait_chunk({i}) should succeed")
            self.assertTrue(handle.is_chunk_ready(i))

        self.store.remove(key)


class TestStreamingScatterRead(unittest.TestCase):
    """Test streaming batch scatter read operations."""

    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def test_streaming_scatter_read_basic(self):
        """Test scatter read of multiple ranges from multiple keys."""
        keys = ["scatter_key_0", "scatter_key_1"]
        chunk_size = 4096
        data = [os.urandom(chunk_size * 2) for _ in keys]

        # Put objects
        for k, d in zip(keys, data):
            ret = self.store.put(k, d)
            self.assertEqual(ret, 0)

        # Scatter read: read first half of each key
        total_read_size = chunk_size * len(keys)
        dst_ptr = allocate_registered_buffer(self.store, total_read_size)
        dest_offsets = [0, chunk_size]
        src_offsets = [0, 0]
        sizes = [chunk_size, chunk_size]

        handle = self.store.streaming_batch_get_buffer_ranges(
            keys, dst_ptr, dest_offsets, src_offsets, sizes
        )
        self.assertIsNotNone(handle)
        self.assertEqual(handle.num_chunks, 2)

        ret = handle.wait_all()
        self.assertEqual(ret, 0)

        # Verify data
        dst_buf = (ctypes.c_char * total_read_size).from_address(dst_ptr)
        self.assertEqual(dst_buf[0:chunk_size], data[0][:chunk_size])
        self.assertEqual(
            dst_buf[chunk_size : chunk_size * 2], data[1][:chunk_size]
        )

        for k in keys:
            self.store.remove(k)


class TestStreamingPutGetIntegration(unittest.TestCase):
    """Integration test: writer and reader interleaved via progress key."""

    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        get_client(cls.store)

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def test_writer_reader_interleaved(self):
        """Writer progressively puts chunks; reader polls progress and reads
        completed chunks using get_into_ranges."""
        key = "test_streaming_interleaved"
        progress_key = key + "/__progress__"
        chunk_size = 4096
        num_chunks = 4
        total_size = chunk_size * num_chunks

        # Writer: progressive put in a thread
        src_ptr = allocate_registered_buffer(self.store, total_size)
        for i in range(num_chunks):
            ctypes.memset(src_ptr + i * chunk_size, i + 10, chunk_size)

        write_done = threading.Event()
        write_errors = []

        def writer():
            try:
                handle = self.store.progressive_put(
                    key, total_size, num_chunks
                )
                if handle is None:
                    write_errors.append("progressive_put returned None")
                    return
                for i in range(num_chunks):
                    offset = i * chunk_size
                    ret = handle.write_chunk(
                        src_ptr + offset, offset, chunk_size
                    )
                    if ret != 0:
                        write_errors.append(f"write_chunk {i} failed: {ret}")
                        return
                    time.sleep(0.01)  # Simulate async write delay
                handle.seal()
            finally:
                write_done.set()

        writer_thread = threading.Thread(target=writer)
        writer_thread.start()

        # Reader: poll progress key, then read completed chunks
        dst_ptr = allocate_registered_buffer(self.store, total_size)
        ctypes.memset(dst_ptr, 0, total_size)
        progress_buf = allocate_registered_buffer(self.store, 8)
        chunks_verified = 0

        deadline = time.time() + 10.0  # 10s timeout
        while chunks_verified < num_chunks and time.time() < deadline:
            bytes_read = self.store.get_into(progress_key, progress_buf, 8)
            if bytes_read != 8:
                time.sleep(0.005)
                continue
            progress_val = struct.unpack(
                "Q", (ctypes.c_char * 8).from_address(progress_buf)[:]
            )[0]

            # Read newly available chunks using get_into_ranges
            while chunks_verified < progress_val:
                offset = chunks_verified * chunk_size
                result = read_range_via_get_into_ranges(
                    self.store, key, dst_ptr, offset, offset, chunk_size
                )
                self.assertEqual(
                    result, chunk_size,
                    f"get_into_ranges for chunk {chunks_verified} failed: "
                    f"{result}"
                )
                chunks_verified += 1

            time.sleep(0.005)

        writer_thread.join(timeout=5.0)
        self.assertFalse(writer_thread.is_alive())
        self.assertEqual(write_errors, [])
        self.assertEqual(chunks_verified, num_chunks)

        # Verify all data
        dst_buf = (ctypes.c_char * total_size).from_address(dst_ptr)
        for i in range(num_chunks):
            offset = i * chunk_size
            expected = bytes([i + 10]) * chunk_size
            self.assertEqual(
                dst_buf[offset : offset + chunk_size], expected,
                f"Chunk {i} data mismatch in reader"
            )

        self.store.remove(key)
        self.store.remove(progress_key)


if __name__ == "__main__":
    unittest.main()
