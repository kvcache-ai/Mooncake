#include <glog/logging.h>
#include <gtest/gtest.h>

#include "serializer.h"
#include "offset_allocator/offset_allocator.hpp"

namespace mooncake::test {

class SerializerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("SerializerTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(SerializerTest, SerializeSizeCounter) {
    SerializeSizeCounter counter;
    counter.write(nullptr, 10);
    EXPECT_EQ(counter.get_size(), 0);
}

TEST_F(SerializerTest, SerializeOffsetAllocator) {
    std::vector<SerializedByte> buffer;
    std::shared_ptr<offset_allocator::OffsetAllocator> allocator = offset_allocator::OffsetAllocator::create(0, 1024 * 1024);
    ASSERT_EQ(serialize_to(allocator, buffer), ErrorCode::OK);
    std::shared_ptr<offset_allocator::OffsetAllocator> allocator2 = deserialize_from<offset_allocator::OffsetAllocator>(buffer);
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}