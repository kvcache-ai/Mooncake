#include <gtest/gtest.h>

#include "tent/common/mapped_memory.h"

namespace mooncake::tent {

TEST(MappedMemoryTest, ExternalReferenceSurvivesCacheRemoval) {
    int released = 0;
    auto mapping = std::make_shared<MappedMemory>(4096, 8192, [&] {
        ++released;
        return Status::OK();
    });
    std::weak_ptr<MappedMemory> cache = mapping;
    auto table = cache.lock();
    cache.reset();
    mapping.reset();
    EXPECT_EQ(released, 0);
    EXPECT_EQ(table->address(), 4096);
    table.reset();
    EXPECT_EQ(released, 1);
}

TEST(MappedMemoryTest, ExplicitReleaseRejectsOutstandingTables) {
    int released = 0;
    auto mapping = std::make_shared<MappedMemory>(4096, 8192, [&] {
        ++released;
        return Status::OK();
    });
    auto table = mapping;
    EXPECT_FALSE(mapping->release().ok());
    EXPECT_EQ(released, 0);
    table.reset();
    EXPECT_TRUE(mapping->release().ok());
    EXPECT_EQ(mapping->address(), 0);
    EXPECT_TRUE(mapping->release().ok());
    mapping.reset();
    EXPECT_EQ(released, 1);
}

TEST(MappedMemoryTest, PartialCleanupIsNotReusedAndCanRetry) {
    int attempts = 0;
    auto mapping = std::make_shared<MappedMemory>(4096, 8192, [&] {
        return ++attempts == 1 ? Status::InternalError("injected release error")
                               : Status::OK();
    });
    EXPECT_FALSE(mapping->release().ok());
    EXPECT_EQ(mapping->address(), 0);
    EXPECT_TRUE(mapping->release().ok());
    mapping.reset();
    EXPECT_EQ(attempts, 2);
}

TEST(MappedMemoryTest, LastReferencePerformsCleanup) {
    int released = 0;
    {
        auto mapping = std::make_shared<MappedMemory>(4096, 8192, [&] {
            ++released;
            return Status::OK();
        });
        EXPECT_EQ(mapping->length(), 8192);
    }
    EXPECT_EQ(released, 1);
}

}  // namespace mooncake::tent
