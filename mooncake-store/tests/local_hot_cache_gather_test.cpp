#include "local_hot_cache.h"
#include "device/accelerator_registry.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace mooncake {
namespace {

constexpr size_t kBlockSize = 64;

void ExpectCached(LocalHotCache& cache, const std::string& key,
                  const std::string& expected) {
    auto* block = cache.GetHotKey(key);
    ASSERT_NE(block, nullptr);
    EXPECT_EQ(block->size, expected.size());
    EXPECT_EQ(std::string(static_cast<char*>(block->addr), block->size),
              expected);
    cache.ReleaseHotKey(key);
    EXPECT_EQ(block->ref_count.load(), 0);
}

class TestAccelerator final : public device::AcceleratorDevice {
   public:
    device::AcceleratorVendor Vendor() const override {
        // A test-only identity cannot replace a linked hardware backend.
        return static_cast<device::AcceleratorVendor>(-1);
    }
    bool Available(bool = false) const override { return true; }
    device::PointerInfo QueryPointer(const void* ptr) const override {
        return {ptr == data.data() ? device::MemoryKind::kDevice
                                   : device::MemoryKind::kHost,
                0};
    }
    int32_t CurrentDeviceId() const override { return 0; }
    void SetContext(int32_t) const override {}
    bool Copy(void* dst, const void* src, size_t size,
              device::CopyDirection direction) const override {
        last_direction.store(direction);
        if (copy_started != nullptr) {
            copy_started->set_value();
            resume_copy.wait();
        }
        // Even a failed copy may have written bytes. They must never become
        // visible as a published cache entry.
        std::memcpy(dst, src, size);
        return copy_succeeds;
    }
    PinnedHostBuffer AllocatePinnedHost(size_t) const override { return {}; }

    std::array<char, 4> data{'d', 'e', 'v', '!'};
    bool copy_succeeds = true;
    std::promise<void>* copy_started = nullptr;
    std::shared_future<void> resume_copy;
    mutable std::atomic<device::CopyDirection> last_direction{
        device::CopyDirection::kAuto};
};

TestAccelerator& GetTestAccelerator() {
    static TestAccelerator accelerator;
    static device::AcceleratorDeviceRegistrar registration(accelerator);
    return accelerator;
}

TEST(LocalHotCacheGatherTest, NoncontiguousSlicesPublishOneCompleteObject) {
    auto cache = std::make_shared<LocalHotCache>(kBlockSize, kBlockSize);
    std::array<char, 12> storage{'a', 'b', 'c', 'd', '_', '_',
                                 '_', '_', 'e', 'f', 'g', 'h'};
    const std::vector<Slice> slices{{storage.data(), 4},
                                    {storage.data() + 8, 4}};
    {
        LocalHotCacheHandler handler(cache, 1);
        ASSERT_TRUE(handler.SubmitPutTask("object", slices));
    }
    ExpectCached(*cache, "object", "abcdefgh");
    EXPECT_EQ(cache->GetCacheSize(), 1);
}

TEST(LocalHotCacheGatherTest, CopiesBeforeSourceBuffersAreReleased) {
    auto cache = std::make_shared<LocalHotCache>(kBlockSize, kBlockSize);
    {
        LocalHotCacheHandler handler(cache, 1);
        {
            std::vector<char> first{'a', 'b'}, second{'c', 'd'};
            const std::vector<Slice> slices{{first.data(), first.size()},
                                            {second.data(), second.size()}};
            ASSERT_TRUE(handler.SubmitPutTask("object", slices));
            first.assign(first.size(), 'x');
            second.assign(second.size(), 'y');
        }
    }
    ExpectCached(*cache, "object", "abcd");
}

TEST(LocalHotCacheGatherTest, RejectsOversizedTotalBeforeEviction) {
    auto cache = std::make_shared<LocalHotCache>(kBlockSize, kBlockSize);
    char resident = 'r';
    {
        LocalHotCacheHandler handler(cache, 1);
        ASSERT_TRUE(handler.SubmitPutTask("resident", Slice{&resident, 1}));
    }
    std::array<char, 40> first{}, second{};
    const std::vector<Slice> slices{{first.data(), first.size()},
                                    {second.data(), second.size()}};
    {
        LocalHotCacheHandler handler(cache, 1);
        EXPECT_FALSE(handler.SubmitPutTask("oversized", slices));
    }
    ExpectCached(*cache, "resident", "r");
    EXPECT_FALSE(cache->HasHotKey("oversized"));
}

TEST(LocalHotCacheGatherTest, ValidatesEverySliceAndTotalBeforeCopy) {
    auto cache = std::make_shared<LocalHotCache>(kBlockSize, kBlockSize);
    char value = 'v';
    LocalHotCacheHandler handler(cache, 1);
    EXPECT_FALSE(handler.SubmitPutTask("empty", std::span<const Slice>{}));
    const std::vector<Slice> invalid{{&value, 1}, {nullptr, 1}};
    EXPECT_FALSE(handler.SubmitPutTask("invalid", invalid));
    const std::vector<Slice> overflow{
        {&value, std::numeric_limits<size_t>::max()}, {&value, 1}};
    EXPECT_FALSE(handler.SubmitPutTask("overflow", overflow));
    EXPECT_EQ(cache->GetCacheSize(), 1);
    EXPECT_FALSE(cache->HasHotKey("invalid"));
    EXPECT_FALSE(cache->HasHotKey("overflow"));
}

TEST(LocalHotCacheGatherTest, IgnoresEmptySlicesAndAcceptsExactCapacity) {
    auto cache = std::make_shared<LocalHotCache>(kBlockSize, kBlockSize);
    std::array<char, kBlockSize / 2> first, second;
    first.fill('a');
    second.fill('b');
    const std::vector<Slice> slices{{nullptr, 0},
                                    {first.data(), first.size()},
                                    {nullptr, 0},
                                    {second.data(), second.size()}};
    {
        LocalHotCacheHandler handler(cache, 1);
        ASSERT_TRUE(handler.SubmitPutTask("object", slices));
    }
    ExpectCached(
        *cache, "object",
        std::string(first.size(), 'a') + std::string(second.size(), 'b'));
}

TEST(LocalHotCacheGatherTest, CopiesDeviceSliceThroughAccelerator) {
    auto& accelerator = GetTestAccelerator();
    auto cache = std::make_shared<LocalHotCache>(kBlockSize, kBlockSize);
    char host = 'h';
    const std::vector<Slice> slices{
        {&host, 1}, {accelerator.data.data(), accelerator.data.size()}};
    {
        LocalHotCacheHandler handler(cache, 1);
        ASSERT_TRUE(handler.SubmitPutTask("object", slices));
    }
    ExpectCached(*cache, "object", "hdev!");
    EXPECT_EQ(accelerator.last_direction.load(),
              device::CopyDirection::kDeviceToHost);
}

TEST(LocalHotCacheGatherTest, FailedCopyReturnsBlockWithoutPublishingPrefix) {
    auto& accelerator = GetTestAccelerator();
    auto cache = std::make_shared<LocalHotCache>(kBlockSize, kBlockSize);
    char host = 'h';
    const std::vector<Slice> slices{
        {&host, 1}, {accelerator.data.data(), accelerator.data.size()}};
    {
        LocalHotCacheHandler handler(cache, 1);
        accelerator.copy_succeeds = false;
        EXPECT_FALSE(handler.SubmitPutTask("failed", slices));
        accelerator.copy_succeeds = true;
        EXPECT_FALSE(cache->HasHotKey("failed"));
        EXPECT_EQ(cache->GetCacheSize(), 1);
        ASSERT_TRUE(handler.SubmitPutTask("replacement", slices));
    }
    ExpectCached(*cache, "replacement", "hdev!");
    EXPECT_FALSE(cache->HasHotKey("failed"));
}

TEST(LocalHotCacheGatherTest, InvalidationDuringGatherCancelsWholeObject) {
    auto& accelerator = GetTestAccelerator();
    auto cache = std::make_shared<LocalHotCache>(kBlockSize, kBlockSize);
    std::promise<void> copy_started, resume_copy;
    auto started = copy_started.get_future();
    accelerator.copy_started = &copy_started;
    accelerator.resume_copy = resume_copy.get_future().share();
    char host = 'h';
    const std::vector<Slice> slices{
        {&host, 1}, {accelerator.data.data(), accelerator.data.size()}};
    bool submitted = false;
    {
        LocalHotCacheHandler handler(cache, 1);
        std::thread producer(
            [&] { submitted = handler.SubmitPutTask("object", slices); });
        EXPECT_EQ(started.wait_for(std::chrono::seconds(5)),
                  std::future_status::ready);
        EXPECT_FALSE(cache->HasHotKey("object"));
        cache->RemoveHotKey("object");
        resume_copy.set_value();
        producer.join();
        accelerator.copy_started = nullptr;
        accelerator.resume_copy = {};
        EXPECT_TRUE(submitted);
    }
    EXPECT_FALSE(cache->HasHotKey("object"));
    EXPECT_EQ(cache->GetCacheSize(), 1);
}

}  // namespace
}  // namespace mooncake
