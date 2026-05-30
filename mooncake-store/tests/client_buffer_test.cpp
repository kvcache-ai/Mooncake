// client_buffer_test.cpp
#include "client_buffer.hpp"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstring>
#include <thread>
#include <vector>

namespace mooncake {

// Test fixture for ClientBufferAllocator tests
class ClientBufferTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Initialize glog for logging
        google::InitGoogleLogging("ClientBufferTest");
        FLAGS_logtostderr = 1;  // Output logs to stderr
    }

    void TearDown() override {
        // Cleanup glog
        google::ShutdownGoogleLogging();
    }

    // Helper function to verify buffer handle properties
    void VerifyBufferHandle(const BufferHandle& handle, size_t expected_size) {
        EXPECT_NE(handle.ptr(), nullptr);
        EXPECT_EQ(handle.size(), expected_size);

        // Verify memory is usable by writing and reading
        void* ptr = handle.ptr();
        std::memset(ptr, 0xAB, expected_size);

        // Verify the written data
        const uint8_t* data = static_cast<const uint8_t*>(ptr);
        for (size_t i = 0; i < expected_size; ++i) {
            EXPECT_EQ(data[i], 0xAB) << "Memory corruption at offset " << i;
        }
    }

    // Helper function to check memory alignment
    void VerifyAlignment(void* ptr, size_t alignment = 64) {
        uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
        EXPECT_EQ(addr % alignment, 0)
            << "Memory not aligned to " << alignment << " bytes";
    }
};

// Test allocator zero size
TEST_F(ClientBufferTest, ZeroSizeAllocator) {
    auto allocator = ClientBufferAllocator::create(0);
    EXPECT_NE(allocator, nullptr);

    auto handle_opt = allocator->allocate(1024);
    EXPECT_FALSE(handle_opt.has_value());
}

// Test multiple allocations
TEST_F(ClientBufferTest, MultipleAllocations) {
    const size_t buffer_size = 1024 * 1024;  // 1MB
    const size_t alloc_size = 64 * 1024;     // 64KB per allocation
    const int num_allocations = 8;           // Total: 512KB

    auto allocator = ClientBufferAllocator::create(buffer_size);
    ASSERT_NE(allocator, nullptr);

    std::vector<BufferHandle> handles;
    handles.reserve(num_allocations);

    // Allocate multiple blocks
    for (int i = 0; i < num_allocations; ++i) {
        auto handle_opt = allocator->allocate(alloc_size);
        ASSERT_TRUE(handle_opt.has_value()) << "Failed to allocate block " << i;

        BufferHandle handle = std::move(handle_opt.value());
        VerifyBufferHandle(handle, alloc_size);
        handles.push_back(std::move(handle));
    }

    // Verify all handles are still valid
    for (const auto& handle : handles) {
        EXPECT_NE(handle.ptr(), nullptr);
        EXPECT_EQ(handle.size(), alloc_size);
    }

    // All handles will be automatically deallocated when vector is destroyed
}

// Test allocation failure when requesting too much memory
TEST_F(ClientBufferTest, AllocationTooLarge) {
    const size_t buffer_size = 1024 * 1024;  // 1MB

    auto allocator = ClientBufferAllocator::create(buffer_size);
    ASSERT_NE(allocator, nullptr);

    // Try to allocate more than the total buffer size
    auto handle_opt = allocator->allocate(buffer_size + 1);
    EXPECT_FALSE(handle_opt.has_value());
}

// Test zero-size allocation
TEST_F(ClientBufferTest, ZeroSizeAllocation) {
    const size_t buffer_size = 1024 * 1024;  // 1MB

    auto allocator = ClientBufferAllocator::create(buffer_size);
    ASSERT_NE(allocator, nullptr);

    // Try to allocate zero bytes
    auto handle_opt = allocator->allocate(0);
    EXPECT_FALSE(handle_opt.has_value());
}

// Test very small allocation (1 byte)
TEST_F(ClientBufferTest, SmallAllocation) {
    const size_t buffer_size = 1024 * 1024;  // 1MB

    auto allocator = ClientBufferAllocator::create(buffer_size);
    ASSERT_NE(allocator, nullptr);

    // Allocate just 1 byte
    auto handle_opt = allocator->allocate(1);
    ASSERT_TRUE(handle_opt.has_value());

    BufferHandle handle = std::move(handle_opt.value());
    EXPECT_NE(handle.ptr(), nullptr);
    EXPECT_EQ(handle.size(), 1);

    // Verify we can write and read the single byte
    uint8_t* ptr = static_cast<uint8_t*>(handle.ptr());
    *ptr = 0xFF;
    EXPECT_EQ(*ptr, 0xFF);
}

// Test BufferHandle move constructor
TEST_F(ClientBufferTest, BufferHandleMoveConstructor) {
    const size_t buffer_size = 1024 * 1024;  // 1MB
    const size_t alloc_size = 1024;

    auto allocator = ClientBufferAllocator::create(buffer_size);
    ASSERT_NE(allocator, nullptr);

    auto handle_opt = allocator->allocate(alloc_size);
    ASSERT_TRUE(handle_opt.has_value());

    BufferHandle handle1 = std::move(handle_opt.value());
    void* original_ptr = handle1.ptr();
    size_t original_size = handle1.size();

    // Move construct handle2 from handle1
    BufferHandle handle2 = std::move(handle1);

    // handle2 should have the original properties
    EXPECT_EQ(handle2.ptr(), original_ptr);
    EXPECT_EQ(handle2.size(), original_size);

    // handle1 should be invalid after move
    EXPECT_EQ(handle1.ptr(), nullptr);
    EXPECT_EQ(handle1.size(), 0);

    // Verify memory is still usable through handle2
    VerifyBufferHandle(handle2, alloc_size);
}

// Test split_into_slices function
TEST_F(ClientBufferTest, SplitIntoSlices) {
    const size_t buffer_size = 1024 * 1024;  // 1MB
    const size_t alloc_size = 100 * 1024;    // 100KB

    auto allocator = ClientBufferAllocator::create(buffer_size);
    ASSERT_NE(allocator, nullptr);

    auto handle_opt = allocator->allocate(alloc_size);
    ASSERT_TRUE(handle_opt.has_value());

    BufferHandle handle = std::move(handle_opt.value());

    // Test split_into_slices
    auto slices = split_into_slices(handle);

    // Verify slices cover the entire buffer
    size_t total_slice_size = 0;
    for (const auto& slice : slices) {
        EXPECT_NE(slice.ptr, nullptr);
        EXPECT_GT(slice.size, 0);
        EXPECT_LE(slice.size, kMaxSliceSize);
        total_slice_size += slice.size;
    }

    EXPECT_EQ(total_slice_size, alloc_size);

    // Verify slices are contiguous
    if (slices.size() > 1) {
        for (size_t i = 1; i < slices.size(); ++i) {
            char* prev_end =
                static_cast<char*>(slices[i - 1].ptr) + slices[i - 1].size;
            char* curr_start = static_cast<char*>(slices[i].ptr);
            EXPECT_EQ(prev_end, curr_start)
                << "Slices are not contiguous at index " << i;
        }
    }
}

// Test split_into_slices with small buffer
TEST_F(ClientBufferTest, SplitIntoSlicesSmallBuffer) {
    const size_t buffer_size = 1024 * 1024;  // 1MB
    const size_t alloc_size = 64;  // 64 bytes - smaller than kMaxSliceSize

    auto allocator = ClientBufferAllocator::create(buffer_size);
    ASSERT_NE(allocator, nullptr);

    auto handle_opt = allocator->allocate(alloc_size);
    ASSERT_TRUE(handle_opt.has_value());

    BufferHandle handle = std::move(handle_opt.value());

    auto slices = split_into_slices(handle);

    // Should have exactly one slice for small buffer
    EXPECT_EQ(slices.size(), 1);
    EXPECT_EQ(slices[0].ptr, handle.ptr());
    EXPECT_EQ(slices[0].size, alloc_size);
}

// Test split_into_slices with ptr and length
TEST_F(ClientBufferTest, SplitIntoSlicesPtrLength) {
    const size_t buffer_size = 1024 * 1024;  // 1MB
    const size_t alloc_size = 300 * 1024;    // 300KB
    auto allocator = ClientBufferAllocator::create(buffer_size);
    ASSERT_NE(allocator, nullptr);

    auto handle_opt = allocator->allocate(alloc_size);
    ASSERT_TRUE(handle_opt.has_value());
    BufferHandle handle = std::move(handle_opt.value());
    void* buffer_ptr = handle.ptr();
    size_t length = handle.size();
    auto slices = split_into_slices(buffer_ptr, length);
    // Verify slices cover the entire buffer
    size_t total_slice_size = 0;
    for (const auto& slice : slices) {
        EXPECT_NE(slice.ptr, nullptr);
        EXPECT_GT(slice.size, 0);
        EXPECT_LE(slice.size, kMaxSliceSize);
        total_slice_size += slice.size;
    }
    EXPECT_EQ(total_slice_size, alloc_size);
}

// Test memory exhaustion scenario
TEST_F(ClientBufferTest, MemoryExhaustion) {
    const size_t buffer_size =
        64 * 1024;  // 64KB - small buffer for quick exhaustion
    const size_t alloc_size = 8 * 1024;  // 8KB per allocation

    auto allocator = ClientBufferAllocator::create(buffer_size);
    ASSERT_NE(allocator, nullptr);

    std::vector<BufferHandle> handles;

    // Allocate until we run out of space
    int successful_allocations = 0;
    for (int i = 0; i < 10; ++i) {  // Try more than should fit
        auto handle_opt = allocator->allocate(alloc_size);
        if (handle_opt.has_value()) {
            handles.push_back(std::move(handle_opt.value()));
            successful_allocations++;
        } else {
            break;  // Out of memory
        }
    }

    // Since 8 * 8KB = 64KB, we should be able to allocate exactly 8 times
    EXPECT_EQ(successful_allocations, 8);

    // Try one more allocation - should fail
    auto final_handle_opt = allocator->allocate(alloc_size);
    EXPECT_FALSE(final_handle_opt.has_value());

    // Free one allocation
    handles.pop_back();

    // Now allocation should succeed again
    auto new_handle_opt = allocator->allocate(alloc_size);
    EXPECT_TRUE(new_handle_opt.has_value());
}

// Test calculate_total_size function with memory replica
TEST_F(ClientBufferTest, CalculateTotalSizeMemoryReplica) {
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
TEST_F(ClientBufferTest, CalculateTotalSizeDiskReplica) {
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
TEST_F(ClientBufferTest, CalculateTotalSizeZeroSizeMemoryReplica) {
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
TEST_F(ClientBufferTest, AllocateSlicesMemoryReplica) {
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
TEST_F(ClientBufferTest, AllocateSlicesDiskReplica) {
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
TEST_F(ClientBufferTest, AllocateSlicesZeroSizeMemoryReplica) {
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

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
