// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <numa.h>
#include <numaif.h>
#include <sys/mman.h>

#include <cstdint>
#include <cstring>
#include <memory>

#include "tent/common/config.h"
#include "tent/common/utils/prefault.h"
#include "tent/platform/cpu.h"

namespace mooncake {
namespace tent {
namespace {

constexpr size_t kHugePageSize = 2UL << 20;

void* mapHugeTLB(size_t len) {
    void* addr = mmap(nullptr, len, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    return addr == MAP_FAILED ? nullptr : addr;
}

TEST(CpuLocationTest, ProbePageSizeBasePage) {
    const size_t len = 16 * 4096;
    void* addr = mmap(nullptr, len, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(addr, MAP_FAILED);
    EXPECT_EQ(probePageSize(addr, len), 4096u);
    munmap(addr, len);
}

TEST(CpuLocationTest, ProbePageSizeHugeTLB) {
    const size_t len = 2 * kHugePageSize;
    void* addr = mapHugeTLB(len);
    if (!addr) GTEST_SKIP() << "no 2MB HugeTLB pages available";
    EXPECT_EQ(probePageSize(addr, len), kHugePageSize);
    // A range that runs past the VMA falls back to 4 KiB steps.
    EXPECT_EQ(probePageSize(addr, len + 4096), 4096u);
    munmap(addr, len);
}

TEST(CpuLocationTest, GetLocationHugeTLB) {
    if (numa_available() < 0) GTEST_SKIP() << "NUMA not available";
    const size_t len = 4 * kHugePageSize;
    char* addr = static_cast<char*>(mapHugeTLB(len));
    if (!addr) GTEST_SKIP() << "no 2MB HugeTLB pages available";
    memset(addr, 0, len);

    CpuPlatform platform(std::make_shared<Config>());
    // Start and end inside a huge page, as a registered sub-range would.
    void* start = addr + 4096;
    const size_t sub_len = len - 2 * 4096;
    auto entries = platform.getLocation(start, sub_len, true);
    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(entries[0].start, reinterpret_cast<uint64_t>(start));
    EXPECT_EQ(entries[0].len, sub_len);
    void* page = addr;
    int node = -1;
    ASSERT_EQ(numa_move_pages(0, 1, &page, nullptr, &node, 0), 0);
    EXPECT_EQ(entries[0].location, "cpu:" + std::to_string(node));
    munmap(addr, len);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
