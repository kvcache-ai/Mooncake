// client_buffer_test.cpp
#include "client_buffer.h"
#include "nof/nof_runtime.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <unistd.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <new>
#include <string>
#include <thread>
#include <vector>

#include "common/client_buffer_allocation.h"

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

#if defined(USE_NOF)
TEST_F(ClientBufferTest, SpdkDmaAllocatorDestroysWithSpdkFree) {
    constexpr size_t buffer_size = 4096;
    constexpr size_t alloc_size = 64;

    auto runtime = CreateNofRuntime();
    if (!runtime.initiator) {
        GTEST_SKIP() << "NoF unavailable";
    }

    std::shared_ptr<ClientBufferAllocator> allocator;
    try {
        allocator = ClientBufferAllocator::create(buffer_size, "tcp",
                                                  /*use_hugepage=*/false,
                                                  runtime.dma_allocator);
    } catch (const std::bad_alloc&) {
        GTEST_SKIP()
            << "SPDK DMA allocation is unavailable in this environment";
    }

    ASSERT_NE(allocator, nullptr);
    VerifyAlignment(allocator->getBase());

    {
        auto handle_opt = allocator->allocate(alloc_size);
        ASSERT_TRUE(handle_opt.has_value());
        BufferHandle handle = std::move(handle_opt.value());
        VerifyBufferHandle(handle, alloc_size);
    }

    allocator.reset();
}
#endif

#ifndef USE_VRAM_SEGMENT
// VmFlags of the VMA containing addr in /proc/self/smaps, or "" if unmapped.
std::string VmaFlags(const void* addr) {
    std::ifstream smaps("/proc/self/smaps");
    const auto target = reinterpret_cast<uintptr_t>(addr);
    bool in_vma = false;
    for (std::string line; std::getline(smaps, line);) {
        unsigned long start = 0, end = 0;
        if (std::sscanf(line.c_str(), "%lx-%lx ", &start, &end) == 2) {
            in_vma = start <= target && target < end;
        } else if (in_vma && line.rfind("VmFlags:", 0) == 0) {
            return line + " ";
        }
    }
    return "";
}

// Sets MC_STORE_MADV_HUGEPAGE for one test and restores the previous value.
class ScopedMadvHugepageEnv {
   public:
    explicit ScopedMadvHugepageEnv(const char* value) {
        const char* old = std::getenv(kName);
        if (old) old_ = old;
        had_old_ = old != nullptr;
        if (value) {
            setenv(kName, value, 1);
        } else {
            unsetenv(kName);
        }
    }
    ~ScopedMadvHugepageEnv() {
        if (had_old_) {
            setenv(kName, old_.c_str(), 1);
        } else {
            unsetenv(kName);
        }
    }

   private:
    static constexpr const char* kName = "MC_STORE_MADV_HUGEPAGE";
    std::string old_;
    bool had_old_ = false;
};

// With MC_STORE_MADV_HUGEPAGE=1, an owned RDMA client buffer is a slab-aligned
// THP-advised mapping like the base-page RDMA global segment, and the whole
// mapping is unmapped on destruction.
TEST_F(ClientBufferTest, RdmaOwnedBufferRequestsThp) {
    if (access("/sys/kernel/mm/transparent_hugepage/enabled", F_OK) != 0) {
        GTEST_SKIP() << "THP is not available";
    }
    if (get_hugepage_size_from_env() != 0) {
        GTEST_SKIP() << "MC_STORE_USE_HUGEPAGE selects HugeTLB instead";
    }
    ScopedMadvHugepageEnv env("1");
    const size_t size = 3 * SZ_2MB + 64;
    auto allocator = ClientBufferAllocator::create(size, "rdma");
    ASSERT_NE(allocator, nullptr);
    auto* base = static_cast<unsigned char*>(allocator->getBase());
    ASSERT_NE(base, nullptr);
    EXPECT_EQ(allocator->size(), size);
    EXPECT_EQ(
        reinterpret_cast<uintptr_t>(base) % facebook::cachelib::Slab::kSize,
        0u);
    EXPECT_NE(VmaFlags(base).find(" hg "), std::string::npos);
    // The 2MB-rounded tail belongs to the same advised mapping.
    unsigned char* map_end = base + align_up(size, SZ_2MB);
    EXPECT_NE(VmaFlags(map_end - 1).find(" hg "), std::string::npos);

    std::memset(base, 0x5a, size);
    {
        auto handle = allocator->allocate(SZ_2MB);
        ASSERT_TRUE(handle.has_value());
        EXPECT_GE(handle->ptr(), static_cast<void*>(base));
        EXPECT_LE(static_cast<unsigned char*>(handle->ptr()) + SZ_2MB,
                  base + size);
    }

    allocator.reset();
    EXPECT_EQ(VmaFlags(base), "");
    EXPECT_EQ(VmaFlags(map_end - 1), "");
}

// Without the opt-in (unset, off, or an invalid value), an owned RDMA client
// buffer keeps the generic allocation path.
TEST_F(ClientBufferTest, RdmaOwnedBufferIsNotThpAdvisedByDefault) {
    const char* tunables = std::getenv("GLIBC_TUNABLES");
    if (tunables && std::strstr(tunables, "hugetlb")) {
        GTEST_SKIP() << "glibc may advise malloc'd mappings itself";
    }
    if (get_hugepage_size_from_env() != 0) {
        GTEST_SKIP() << "MC_STORE_USE_HUGEPAGE selects HugeTLB instead";
    }
    for (const char* value :
         {static_cast<const char*>(nullptr), "0", "off", "not-a-bool"}) {
        ScopedMadvHugepageEnv env(value);
        const size_t size = 3 * SZ_2MB + 64;
        auto allocator = ClientBufferAllocator::create(size, "rdma");
        ASSERT_NE(allocator, nullptr);
        ASSERT_NE(allocator->getBase(), nullptr);
        EXPECT_EQ(VmaFlags(allocator->getBase()).find(" hg "),
                  std::string::npos)
            << "MC_STORE_MADV_HUGEPAGE=" << (value ? value : "<unset>");
    }
}

// Other protocols keep the generic allocation path even with the opt-in.
TEST_F(ClientBufferTest, TcpOwnedBufferIsNotThpAdvised) {
    const char* tunables = std::getenv("GLIBC_TUNABLES");
    if (tunables && std::strstr(tunables, "hugetlb")) {
        GTEST_SKIP() << "glibc may advise malloc'd mappings itself";
    }
    ScopedMadvHugepageEnv env("1");
    const size_t size = 3 * SZ_2MB + 64;
    auto allocator = ClientBufferAllocator::create(size, "tcp");
    ASSERT_NE(allocator, nullptr);
    ASSERT_NE(allocator->getBase(), nullptr);
    EXPECT_EQ(VmaFlags(allocator->getBase()).find(" hg "), std::string::npos);
}
#endif

// A caller-owned RDMA buffer is neither advised nor unmapped.
TEST_F(ClientBufferTest, RdmaExternalBufferRetainsCallerOwnership) {
    std::vector<unsigned char> storage(3 * SZ_2MB + 64, 0x5a);
    {
        auto allocator = ClientBufferAllocator::create(storage.data(),
                                                       storage.size(), "rdma");
        ASSERT_NE(allocator, nullptr);
        EXPECT_EQ(allocator->getBase(), storage.data());
    }
    std::memset(storage.data(), 0xa5, storage.size());
    EXPECT_EQ(storage.front(), 0xa5);
    EXPECT_EQ(storage.back(), 0xa5);
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
