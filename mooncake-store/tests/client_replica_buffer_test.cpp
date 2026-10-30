#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "client_buffer.hpp"
#include "client_service.h"

namespace mooncake {
namespace testing {

// Centralized client helpers consume Replica descriptors; buffer allocation
// itself remains covered by the architecture-neutral client_buffer_test.
class ClientReplicaBufferTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("ClientReplicaBufferTest");
        FLAGS_logtostderr = 1;
    }
    void TearDown() override { google::ShutdownGoogleLogging(); }
};

// Test calculate_total_size function with memory replica
TEST_F(ClientReplicaBufferTest, CalculateTotalSizeMemoryReplica) {
    // Create a memory replica descriptor
    Replica::Descriptor replica;
    MemoryDescriptor mem_desc;

    // Set buffer descriptor with proper initialization
    mem_desc.buffer_descriptor.size_ = 4096;
    mem_desc.buffer_descriptor.buffer_address_ = 0x1000;

    replica.descriptor_variant = mem_desc;
    replica.status = ReplicaStatus::COMPLETE;

    uint64_t total_size = calculate_total_size(replica);
    EXPECT_EQ(total_size, 4096);
}

// Test calculate_total_size function with disk replica
TEST_F(ClientReplicaBufferTest, CalculateTotalSizeDiskReplica) {
    // Create a disk replica descriptor
    Replica::Descriptor replica;
    DiskDescriptor disk_desc;
    disk_desc.object_size = 4096;

    replica.descriptor_variant = disk_desc;
    replica.status = ReplicaStatus::COMPLETE;

    uint64_t total_size = calculate_total_size(replica);
    EXPECT_EQ(total_size, 4096);
}

// Test calculate_total_size function with zero-size memory replica
TEST_F(ClientReplicaBufferTest, CalculateTotalSizeZeroSizeMemoryReplica) {
    // Create a memory replica descriptor with zero size
    Replica::Descriptor replica;
    MemoryDescriptor mem_desc;
    mem_desc.buffer_descriptor.size_ = 0;
    mem_desc.buffer_descriptor.buffer_address_ = 0x1000;

    replica.descriptor_variant = mem_desc;
    replica.status = ReplicaStatus::COMPLETE;

    uint64_t total_size = calculate_total_size(replica);
    EXPECT_EQ(total_size, 0);
}

// Test allocateSlices function with memory replica
TEST_F(ClientReplicaBufferTest, AllocateSlicesMemoryReplica) {
    const size_t buffer_size = 1024 * 1024;  // 1MB
    const size_t alloc_size = 4096;          // 4KB

    auto allocator = ClientBufferAllocator::create(buffer_size);
    ASSERT_NE(allocator, nullptr);

    auto handle_opt = allocator->allocate(alloc_size);
    ASSERT_TRUE(handle_opt.has_value());

    BufferHandle handle = std::move(handle_opt.value());

    // Create a memory replica descriptor
    Replica::Descriptor replica;
    MemoryDescriptor mem_desc;
    mem_desc.buffer_descriptor.size_ = 4096;
    mem_desc.buffer_descriptor.buffer_address_ = 0x1000;

    replica.descriptor_variant = mem_desc;
    replica.status = ReplicaStatus::COMPLETE;

    std::vector<Slice> slices;
    int result = allocateSlices(slices, replica, handle.ptr());

    EXPECT_EQ(result, 0);
    EXPECT_EQ(slices.size(), 1);

    // Verify slice size matches buffer descriptor
    EXPECT_EQ(slices[0].size, 4096);

    // Verify slice pointer matches buffer pointer
    EXPECT_EQ(slices[0].ptr, handle.ptr());
}

// Test allocateSlices function with disk replica
TEST_F(ClientReplicaBufferTest, AllocateSlicesDiskReplica) {
    const size_t buffer_size = 1024 * 1024;  // 1MB
    const size_t alloc_size = 8192;          // 8KB

    auto allocator = ClientBufferAllocator::create(buffer_size);
    ASSERT_NE(allocator, nullptr);

    auto handle_opt = allocator->allocate(alloc_size);
    ASSERT_TRUE(handle_opt.has_value());

    BufferHandle handle = std::move(handle_opt.value());

    // Create a disk replica descriptor
    Replica::Descriptor replica;
    DiskDescriptor disk_desc;
    disk_desc.object_size = 8192;

    replica.descriptor_variant = disk_desc;
    replica.status = ReplicaStatus::COMPLETE;

    std::vector<Slice> slices;
    int result = allocateSlices(slices, replica, handle.ptr());

    EXPECT_EQ(result, 0);
    EXPECT_GE(slices.size(), 1);

    // Verify total size matches file size
    size_t total_slice_size = 0;
    for (const auto& slice : slices) {
        EXPECT_NE(slice.ptr, nullptr);
        EXPECT_GT(slice.size, 0);
        EXPECT_LE(slice.size, kMaxSliceSize);
        total_slice_size += slice.size;
    }

    EXPECT_EQ(total_slice_size, 8192);
}

// Test allocateSlices function with zero-size memory replica
TEST_F(ClientReplicaBufferTest, AllocateSlicesZeroSizeMemoryReplica) {
    const size_t buffer_size = 1024 * 1024;  // 1MB
    const size_t alloc_size = 1024;          // 1KB

    auto allocator = ClientBufferAllocator::create(buffer_size);
    ASSERT_NE(allocator, nullptr);

    auto handle_opt = allocator->allocate(alloc_size);
    ASSERT_TRUE(handle_opt.has_value());

    BufferHandle handle = std::move(handle_opt.value());

    // Create a memory replica descriptor with zero size
    Replica::Descriptor replica;
    MemoryDescriptor mem_desc;
    mem_desc.buffer_descriptor.size_ = 0;
    mem_desc.buffer_descriptor.buffer_address_ = 0x1000;

    replica.descriptor_variant = mem_desc;
    replica.status = ReplicaStatus::COMPLETE;

    std::vector<Slice> slices;
    int result = allocateSlices(slices, replica, handle.ptr());

    EXPECT_EQ(result, 0);
    EXPECT_EQ(slices.size(), 1);
    EXPECT_EQ(slices[0].size, 0);
    EXPECT_EQ(slices[0].ptr, handle.ptr());
}

}  // namespace testing
}  // namespace mooncake
