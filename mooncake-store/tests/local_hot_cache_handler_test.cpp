#include "local_hot_cache.h"

#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <string>

namespace mooncake {
namespace {

constexpr size_t kBlockSize = 64;

TEST(LocalHotCacheHandlerTest, OversizedAdmissionPreservesResident) {
    auto cache = std::make_shared<LocalHotCache>(kBlockSize, kBlockSize);
    std::string resident = "resident data";
    {
        LocalHotCacheHandler handler(cache, 1);
        ASSERT_TRUE(handler.SubmitPutTask(
            "resident", Slice{resident.data(), resident.size()}));
    }  // Drain the fill before testing eviction.
    ASSERT_TRUE(cache->HasHotKey("resident"));

    std::array<char, kBlockSize + 1> oversized{};
    {
        LocalHotCacheHandler handler(cache, 1);
        EXPECT_FALSE(handler.SubmitPutTask(
            "oversized", Slice{oversized.data(), oversized.size()}));
    }

    EXPECT_FALSE(cache->HasHotKey("oversized"));
    auto* block = cache->GetHotKey("resident");
    ASSERT_NE(block, nullptr);
    EXPECT_EQ(std::string(static_cast<char*>(block->addr), block->size),
              resident);
    cache->ReleaseHotKey("resident");
    EXPECT_EQ(cache->GetCacheSize(), 1);
}

TEST(LocalHotCacheHandlerTest, ExactCapacityCanReplaceSmallerResident) {
    auto cache = std::make_shared<LocalHotCache>(kBlockSize, kBlockSize);
    char small = 's';
    {
        LocalHotCacheHandler handler(cache, 1);
        ASSERT_TRUE(handler.SubmitPutTask("small", Slice{&small, 1}));
    }
    std::array<char, kBlockSize> exact;
    exact.fill('e');
    {
        LocalHotCacheHandler handler(cache, 1);
        ASSERT_TRUE(
            handler.SubmitPutTask("exact", Slice{exact.data(), exact.size()}));
    }
    EXPECT_FALSE(cache->HasHotKey("small"));
    auto* block = cache->GetHotKey("exact");
    ASSERT_NE(block, nullptr);
    EXPECT_EQ(block->size, exact.size());
    EXPECT_EQ(std::string(static_cast<char*>(block->addr), block->size),
              std::string(kBlockSize, 'e'));
    cache->ReleaseHotKey("exact");
}

TEST(LocalHotCacheHandlerTest, InvalidSlicesLeaveFreeBlockAvailable) {
    auto cache = std::make_shared<LocalHotCache>(kBlockSize, kBlockSize);
    std::array<char, kBlockSize + 1> data{};
    LocalHotCacheHandler handler(cache, 1);
    EXPECT_FALSE(handler.SubmitPutTask("null", Slice{nullptr, 1}));
    EXPECT_FALSE(handler.SubmitPutTask("empty", Slice{data.data(), 0}));
    EXPECT_FALSE(
        handler.SubmitPutTask("oversized", Slice{data.data(), data.size()}));
    auto* block = cache->GetFreeBlock();
    ASSERT_NE(block, nullptr);
    EXPECT_TRUE(block->key_.empty());
    EXPECT_FALSE(cache->PutHotKey(block));
}

}  // namespace
}  // namespace mooncake
