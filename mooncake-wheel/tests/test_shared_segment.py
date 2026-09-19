#!/usr/bin/env python3
import unittest


def _hugetlb_pool_ready():
    page_kib = 0
    free_pages = 0
    try:
        with open("/proc/meminfo", encoding="utf-8") as meminfo:
            for line in meminfo:
                if line.startswith("Hugepagesize:"):
                    page_kib = int(line.split()[1])
                elif line.startswith("HugePages_Free:"):
                    free_pages = int(line.split()[1])
    except OSError:
        return False
    return page_kib > 0 and free_pages > 0


class TestSharedSegment(unittest.TestCase):
    def test_engine_thp_round_trip(self):
        from mooncake.engine import SharedSegment

        self.assertTrue(SharedSegment.supported(mmap=True, hugetlb=False))
        segment, blob = SharedSegment.create(
            "py-thp", 2 * 1024 * 1024, 1, 0, mmap=True, hugetlb=False
        )
        segment.complete([blob])
        self.assertTrue(segment.ready())
        self.assertNotEqual(segment.base_addr(), 0)

    def test_engine_hugetlb_round_trip(self):
        from mooncake.engine import SharedSegment

        self.assertTrue(
            SharedSegment.supported(mmap=True, host_register=False, hugetlb=True)
        )
        if not _hugetlb_pool_ready():
            self.skipTest("HugeTLB pool is empty; set vm.nr_hugepages")
        segment, blob = SharedSegment.create(
            "py-hugetlb", 2 * 1024 * 1024, 1, 0, mmap=True, hugetlb=True
        )
        segment.complete([blob])
        self.assertTrue(segment.ready())
        self.assertNotEqual(segment.base_addr(), 0)

    def test_hugetlb_requires_mmap(self):
        from mooncake.engine import SharedSegment

        self.assertFalse(
            SharedSegment.supported(mmap=False, host_register=False, hugetlb=True)
        )


if __name__ == "__main__":
    unittest.main()
