#include "shared_nvme_kv_id_allocator.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <thread>
#include <vector>

namespace mooncake {
namespace {
TEST(SharedKvIdsTest, ConcurrentCallsDoNotReuseIds) {
    SharedNvmeKvIdAllocator ids;
    std::vector<std::vector<uint64_t>> results(8);
    std::vector<std::thread> workers;
    for (auto& result : results) {
        workers.emplace_back([&ids, &result] {
            for (size_t i = 0; i < 10000; ++i) {
                auto id = ids.Allocate();
                ASSERT_TRUE(id);
                result.push_back(*id);
            }
        });
    }
    for (auto& worker : workers) worker.join();

    std::vector<uint64_t> all;
    for (const auto& result : results) {
        all.insert(all.end(), result.begin(), result.end());
    }
    std::sort(all.begin(), all.end());
    ASSERT_EQ(all.size(), 80000);
    EXPECT_EQ(all.front(), 1);
    EXPECT_EQ(std::adjacent_find(all.begin(), all.end()), all.end());
}
}  // namespace
}  // namespace mooncake
