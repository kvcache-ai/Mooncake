import unittest

from mooncake.engine import allocate_host_buffer, free_host_buffer


class TestAllocateHostBuffer(unittest.TestCase):
    def test_alloc_free_roundtrip(self):
        size = 4 * 1024 * 1024
        ptr = allocate_host_buffer(size)
        self.assertNotEqual(ptr, 0)
        free_host_buffer(ptr)

    def test_zero_size_returns_zero(self):
        self.assertEqual(allocate_host_buffer(0), 0)

    def test_free_null_is_noop(self):
        free_host_buffer(0)


if __name__ == "__main__":
    unittest.main()
