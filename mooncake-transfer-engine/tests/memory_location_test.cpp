#include "memory_location.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <numa.h>
#include <numaif.h>
#include <sys/mman.h>

TEST(MemoryLocationTest, MallocSimpleNode0) {
    int size = 4096 * 10;
    void *addr = numa_alloc_onnode(size, 0);
    ASSERT_NE(addr, nullptr);

    auto entries = mooncake::getMemoryLocation(addr, size);
    ASSERT_EQ(entries.size(), 1);

    // check the memory location, no node before page fault
    EXPECT_EQ(entries[0].start, (uint64_t)addr);
    EXPECT_EQ(entries[0].location, "*");
    EXPECT_EQ(entries[0].len, size);

    // trigger page fault
    memset(addr, 0, size);

    entries = mooncake::getMemoryLocation(addr, size);
    ASSERT_EQ(entries.size(), 1);

    // check the memory location, node 0 after page fault
    EXPECT_EQ(entries[0].start, (uint64_t)addr);
    EXPECT_EQ(entries[0].location, "cpu:0");
    EXPECT_EQ(entries[0].len, size);

    numa_free(addr, size);
}

TEST(MemoryLocationTest, MallocSimpleNodeLargest) {
    int node = numa_max_node();
    LOG(INFO) << "node: " << node;

    std::string location = "cpu:" + std::to_string(node);

    int size = 4096 * 10;
    void *addr = numa_alloc_onnode(size, node);
    ASSERT_NE(addr, nullptr);

    // trigger page fault
    memset(addr, 0, size);

    auto entries = mooncake::getMemoryLocation(addr, size);
    ASSERT_EQ(entries.size(), 1);

    // check the memory location
    EXPECT_EQ(entries[0].start, (uint64_t)addr);
    EXPECT_EQ(entries[0].location, location);
    EXPECT_EQ(entries[0].len, size);

    numa_free(addr, size);
}

TEST(MemoryLocationTest, MallocMultipleNodes) {
    int nodea = 0;
    int nodeb = numa_max_node();
    LOG(INFO) << "node a: " << nodea << " node b: " << nodeb;

    std::string locationa = "cpu:" + std::to_string(nodea);
    std::string locationb = "cpu:" + std::to_string(nodeb);

    int size = 4096 * 10;
    void *addr = numa_alloc_onnode(size, nodea);
    ASSERT_NE(addr, nullptr);
    ASSERT_EQ((uint64_t)addr % 4096, 0);  // page aligned

    // trigger page fault
    memset(addr, 0, size);

    int rc;

    // move first two pages & last one page to nodeb
    void *pages[3] = {addr, (void *)((uint64_t)addr + 4096),
                      (void *)((uint64_t)addr + 4096 * 9)};
    int nodes[3] = {nodeb, nodeb, nodeb};
    int status[3];
    rc = numa_move_pages(0, 3, pages, nodes, status, MPOL_MF_MOVE);
    if (rc != 0) {
        PLOG(ERROR) << "numa_move_pages failed, rc: " << rc;
    }
    ASSERT_EQ(rc, 0);

    // not page aligned
    void *start = (void *)((uint64_t)addr + 1024 * 2);

    auto entries = mooncake::getMemoryLocation(start, size - 1024 * 4);

    if (nodea == nodeb) {
        // only one numa node
        ASSERT_EQ(entries.size(), 1);

        // check the first memory location
        EXPECT_EQ(entries[0].start, (uint64_t)start);
        EXPECT_EQ(entries[0].location, locationa);
        EXPECT_EQ(entries[0].len, size - 1024 * 4);

    } else {
        ASSERT_EQ(entries.size(), 3);

        // check the first memory location
        EXPECT_EQ(entries[0].start, (uint64_t)start);
        EXPECT_EQ(entries[0].location, locationb);
        EXPECT_EQ(entries[0].len, 4096 * 2 - 1024 * 2);

        // check the second memory location
        EXPECT_EQ(entries[1].start, (uint64_t)addr + 4096 * 2);
        EXPECT_EQ(entries[1].location, locationa);
        EXPECT_EQ(entries[1].len, 4096 * 7);

        // check the third memory location
        EXPECT_EQ(entries[2].start, (uint64_t)addr + 4096 * 9);
        EXPECT_EQ(entries[2].location, locationb);
        EXPECT_EQ(entries[2].len, 4096 - 1024 * 2);
    }

    numa_free(addr, size);
}
