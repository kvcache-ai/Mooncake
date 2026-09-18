// buffer_allocator_test.cpp
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstring>
#include <chrono>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <thread>
#include <vector>

#include "allocator.h"
#include "master_metric_manager.h"
#include "types.h"

namespace mooncake {

namespace {

LiveAllocation ToLiveAllocation(uintptr_t base,
                                const AllocatedBuffer::Descriptor& descriptor) {
    return {descriptor.buffer_address_ - base, descriptor.size_};
}

}  // namespace

// Test fixture for BufferAllocator tests
class BufferAllocatorTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Initialize glog for logging
        google::InitGoogleLogging("BufferAllocatorTest");
        FLAGS_logtostderr = 1;  // Output logs to stderr
    }

    void TearDown() override {
        // Cleanup glog
        google::ShutdownGoogleLogging();
    }

    // Helper function to create a BufferAllocator for testing
    std::shared_ptr<BufferAllocatorBase> CreateTestAllocator(
        const std::string& segment_name, size_t base_offset, size_t size,
        BufferAllocatorType allocator_type) {
        const size_t base = 0x100000000ULL + base_offset;  // 4GB + offset
        auto allocator = CreateBufferAllocator(allocator_type, segment_name,
                                               base, size, segment_name);
        if (!allocator) {
            throw std::invalid_argument("Invalid allocator test parameters");
        }
        return std::move(*allocator);
    }

    void VerifyAllocatedBuffer(const AllocatedBuffer& bufHandle,
                               size_t alloc_size,
                               const std::string& segment_name,
                               const std::string& transport_endpoint) {
        auto descriptor = bufHandle.get_descriptor();
        EXPECT_EQ(bufHandle.getSegmentName(), segment_name);
        EXPECT_EQ(descriptor.transport_endpoint_, transport_endpoint);
        EXPECT_EQ(descriptor.size_, alloc_size);
        EXPECT_NE(bufHandle.data(), nullptr);
    }

    std::vector<BufferAllocatorType> allocator_types_ = {
        BufferAllocatorType::CACHELIB, BufferAllocatorType::OFFSET};
};

// Test basic allocation and deallocation functionality
TEST_F(BufferAllocatorTest, AllocateAndDeallocate) {
    for (const auto& allocator_type : allocator_types_) {
        std::string segment_name = "1";
        size_t size = 1024 * 1024 * 16;  // 16MB (multiple of 4MB)
        auto allocator =
            CreateTestAllocator(segment_name, 0, size, allocator_type);

        // Allocate memory block
        size_t alloc_size = 1024;
        auto bufHandle = allocator->allocate(alloc_size);
        auto descriptor = bufHandle->get_descriptor();
        // Verify allocation success and properties
        ASSERT_NE(bufHandle, nullptr);
        VerifyAllocatedBuffer(*bufHandle, alloc_size, segment_name,
                              segment_name);

        // Release memory
        bufHandle.reset();
    }
}

// Test multiple allocations within the buffer
TEST_F(BufferAllocatorTest, AllocateMultiple) {
    for (const auto& allocator_type : allocator_types_) {
        std::string segment_name = "1";
        size_t size = 1024 * 1024 * 16;  // 16MB (must be multiple of 4MB)
        auto allocator =
            CreateTestAllocator(segment_name, 0, size, allocator_type);

        // Allocate multiple memory blocks
        size_t alloc_size = 1024 * 1024;  // 1MB per block
        std::vector<std::unique_ptr<AllocatedBuffer>> handles;

        // Attempt to allocate 8 blocks (should succeed as total size is less
        // than buffer size)
        for (int i = 0; i < 8; ++i) {
            auto bufHandle = allocator->allocate(alloc_size);
            ASSERT_NE(bufHandle, nullptr);
            VerifyAllocatedBuffer(*bufHandle, alloc_size, segment_name,
                                  segment_name);
            handles.push_back(std::move(bufHandle));
        }

        // Clean up allocated memory
        handles.clear();
        LOG(INFO) << "Cleaned up handles in AllocateMultiple test";
    }
}

TEST_F(BufferAllocatorTest, OffsetLargestFreeRegionRemainsExact) {
    constexpr size_t CAPACITY = 16 * 1024 * 1024;
    auto allocator = std::make_shared<OffsetBufferAllocator>(
        "exact-largest-free-region", 0x140000000ULL, CAPACITY,
        "exact-largest-free-region");

    EXPECT_EQ(allocator->getLargestFreeRegion(), CAPACITY);

    auto buffer = allocator->allocate(CAPACITY / 2);
    ASSERT_NE(buffer, nullptr);
    const auto internal_allocator = allocator->getOffsetAllocator();

    // Successful allocations intentionally leave the fast-fail hint high, but
    // the segment-selection query must still return the authoritative value.
    EXPECT_GT(internal_allocator->getLargestFreeRegion(),
              allocator->getLargestFreeRegion());
    EXPECT_EQ(allocator->getLargestFreeRegion(),
              internal_allocator->storageReport().largestFreeRegion);

    buffer.reset();
    EXPECT_EQ(allocator->getLargestFreeRegion(), CAPACITY);
}

TEST_F(BufferAllocatorTest, ImportOffsetAllocationsAtOriginalAddresses) {
    constexpr uintptr_t kBase = 0x180000000ULL;
    constexpr size_t kCapacity = 16 * 1024 * 1024;
    const std::string segment = "restore-segment";
    const std::string endpoint = "restore-endpoint";

    auto original = std::make_shared<OffsetBufferAllocator>(
        segment, kBase, kCapacity, endpoint);
    auto first = original->allocate(123);
    auto removed = original->allocate(5003);
    auto last = original->allocate(777);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(removed, nullptr);
    ASSERT_NE(last, nullptr);

    const std::vector<AllocatedBuffer::Descriptor> descriptors = {
        first->get_descriptor(), last->get_descriptor()};
    std::vector<LiveAllocation> allocations = {
        ToLiveAllocation(kBase, descriptors[0]),
        ToLiveAllocation(kBase, descriptors[1])};
    const auto removed_descriptor = removed->get_descriptor();
    removed.reset();

    auto restored = ImportOffsetBufferAllocator(segment, kBase, kCapacity,
                                                endpoint, allocations);
    ASSERT_TRUE(restored.has_value());
    ASSERT_EQ(restored->buffers.size(), descriptors.size());
    EXPECT_EQ(restored->allocator->size(),
              descriptors[0].size_ + descriptors[1].size_);
    EXPECT_EQ(restored->buffers[0]->get_descriptor().buffer_address_,
              descriptors[0].buffer_address_);
    EXPECT_EQ(restored->buffers[1]->get_descriptor().buffer_address_,
              descriptors[1].buffer_address_);

    auto new_buffer = restored->allocator->allocate(removed_descriptor.size_);
    ASSERT_NE(new_buffer, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(new_buffer->data()),
              removed_descriptor.buffer_address_);
}

TEST_F(BufferAllocatorTest, ImportOffsetAllocationsValidatesRangesAndOrder) {
    constexpr uintptr_t kBase = 0x190000000ULL;
    constexpr size_t kCapacity = 4096;
    const std::string segment = "restore-validation";
    const std::string endpoint = "restore-validation-endpoint";
    auto allocation = [&](uintptr_t address, uint64_t size) {
        return LiveAllocation{address - kBase, size};
    };

    std::vector<LiveAllocation> unsorted = {allocation(kBase + 512, 64),
                                            allocation(kBase + 128, 64)};
    auto restored = ImportOffsetBufferAllocator(segment, kBase, kCapacity,
                                                endpoint, unsorted);
    ASSERT_TRUE(restored.has_value());
    ASSERT_EQ(restored->buffers.size(), unsorted.size());
    EXPECT_EQ(reinterpret_cast<uintptr_t>(restored->buffers[0]->data()),
              kBase + unsorted[0].offset_from_base);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(restored->buffers[1]->data()),
              kBase + unsorted[1].offset_from_base);

    std::vector<LiveAllocation> overlapping = {allocation(kBase + 128, 100),
                                               allocation(kBase + 200, 32)};
    EXPECT_FALSE(ImportOffsetBufferAllocator(segment, kBase, kCapacity,
                                             endpoint, overlapping)
                     .has_value());

    std::vector<LiveAllocation> normalized_past_end = {
        allocation(kBase + kCapacity - 100, 100)};
    EXPECT_FALSE(ImportOffsetBufferAllocator(segment, kBase, kCapacity,
                                             endpoint, normalized_past_end)
                     .has_value());

    EXPECT_FALSE(ImportOffsetBufferAllocator(
                     segment, std::numeric_limits<size_t>::max() - 100, 200,
                     endpoint, {})
                     .has_value());
    std::vector<LiveAllocation> allocation_overflow = {
        {std::numeric_limits<uintptr_t>::max() - kBase - 10, 20}};
    EXPECT_FALSE(ImportOffsetBufferAllocator(segment, kBase, kCapacity,
                                             endpoint, allocation_overflow)
                     .has_value());
}

TEST_F(BufferAllocatorTest, ImportedOffsetHandleReleasesItsExactAddress) {
    constexpr uintptr_t kBase = 0x1A0000000ULL;
    constexpr size_t kCapacity = 4096;
    const std::string endpoint = "restore-release";
    std::vector<LiveAllocation> allocations = {{0, 64}, {512, 64}};
    auto restored = ImportOffsetBufferAllocator(
        "restore-release", kBase, kCapacity, endpoint, allocations);
    ASSERT_TRUE(restored.has_value());

    restored->buffers[0].reset();
    auto replacement = restored->allocator->allocate(64);
    ASSERT_NE(replacement, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(replacement->data()),
              kBase + allocations[0].offset_from_base);
}

TEST_F(BufferAllocatorTest, ImportOffsetAllocationsHasNoArbitraryGapLimit) {
    constexpr uintptr_t kBase = 0x1B0000000ULL;
    constexpr size_t kGapCount = 65537;
    const std::string endpoint = "restore-many-gaps";
    std::vector<LiveAllocation> allocations;
    allocations.reserve(kGapCount);
    for (size_t i = 0; i < kGapCount; ++i) {
        allocations.push_back({1 + i * 2, 1});
    }

    auto restored = ImportOffsetBufferAllocator(
        "restore-many-gaps", kBase, kGapCount * 2 + 1, endpoint, allocations);
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->buffers.size(), allocations.size());
    EXPECT_EQ(reinterpret_cast<uintptr_t>(restored->buffers.back()->data()),
              kBase + allocations.back().offset_from_base);
}

TEST_F(BufferAllocatorTest, CachelibCreateRejectsInvalidMemoryLayout) {
    constexpr size_t kSlabSize = facebook::cachelib::Slab::kSize;
    constexpr uintptr_t kBase = 0x1C0000000ULL;

    auto expect_invalid = [](size_t base, size_t size) {
        auto result = CachelibBufferAllocator::Create("cachelib-invalid", base,
                                                      size, "endpoint");
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    };

    expect_invalid(kBase + 1, kSlabSize);
    expect_invalid(kBase, kSlabSize + 1);
    expect_invalid(std::numeric_limits<size_t>::max() - kSlabSize,
                   2 * kSlabSize);
    if constexpr (std::numeric_limits<size_t>::max() / kSlabSize >
                  std::numeric_limits<unsigned int>::max()) {
        const size_t too_many_slabs =
            (static_cast<size_t>(std::numeric_limits<unsigned int>::max()) +
             1) *
            kSlabSize;
        expect_invalid(kBase, too_many_slabs);
    }
}

TEST_F(BufferAllocatorTest, OffsetRestoreFactoryRejectsInvalidState) {
    auto& metrics = MasterMetricManager::instance();
    const auto baseline = metrics.get_allocated_mem_size();
    const std::string segment_name = "restore-factory";
    constexpr size_t kBase = 0x100000000ULL;
    constexpr size_t kCapacity = 16U * 1024 * 1024;
    auto state = offset_allocator::OffsetAllocator::create(kBase, kCapacity);
    ASSERT_NE(state, nullptr);

    auto snapshot = [&] {
        return OffsetBufferAllocatorSnapshot{segment_name,
                                             kBase,
                                             kCapacity,
                                             0,
                                             segment_name + "-endpoint",
                                             state->CaptureSnapshot()};
    };
    const auto expect_rejected = [](OffsetBufferAllocatorSnapshot candidate) {
        auto restored = OffsetBufferAllocator::Restore(std::move(candidate));
        ASSERT_FALSE(restored.has_value());
        EXPECT_EQ(restored.error(), ErrorCode::INVALID_PARAMS);
    };
    auto missing_layout = snapshot();
    missing_layout.allocation_state.layout.reset();
    expect_rejected(std::move(missing_layout));
    auto invalid_usage = snapshot();
    invalid_usage.used_bytes = kCapacity + 1;
    expect_rejected(std::move(invalid_usage));
    auto mismatched_base = snapshot();
    ++mismatched_base.allocation_state.base;
    expect_rejected(std::move(mismatched_base));
    auto overflow = snapshot();
    overflow.used_bytes = std::numeric_limits<size_t>::max();
    expect_rejected(std::move(overflow));
    // The persisted counters must describe the persisted layout.
    auto wrong_alloc_num = snapshot();
    ++wrong_alloc_num.allocation_state.allocated_num;
    expect_rejected(std::move(wrong_alloc_num));
    auto wrong_alloc_size = snapshot();
    wrong_alloc_size.allocation_state.allocated_size = kCapacity;
    expect_rejected(std::move(wrong_alloc_size));
    EXPECT_EQ(metrics.get_allocated_mem_size(), baseline);

    {
        auto restored = OffsetBufferAllocator::Restore(snapshot());
        ASSERT_TRUE(restored.has_value());
        EXPECT_EQ((*restored)->size(), 0U);
        EXPECT_EQ((*restored)->capacity(), kCapacity);

        auto buffer = (*restored)->allocate(4096);
        ASSERT_NE(buffer, nullptr);
        EXPECT_EQ((*restored)->size(), 4096U);
        EXPECT_EQ(metrics.get_allocated_mem_size(), baseline + 4096);
    }
    EXPECT_EQ(metrics.get_allocated_mem_size(), baseline);
}

TEST_F(BufferAllocatorTest,
       SnapshotValidationReportsErrorsWithoutPublishingUsage) {
    auto& metrics = MasterMetricManager::instance();
    const auto baseline = metrics.get_allocated_mem_size();
    constexpr size_t kBase = 0x100000000ULL;
    constexpr size_t kCapacity = 16U << 20;
    auto state =
        offset_allocator::OffsetAllocator::create(kBase, kCapacity, 16, 32);
    auto allocation = state->allocate(4097);
    ASSERT_TRUE(allocation.has_value());
    auto snapshot = [&] {
        return OffsetBufferAllocatorSnapshot{
            .segment_name = "validate-snapshot",
            .base = kBase,
            .capacity = kCapacity,
            .used_bytes = 4097,
            .transport_endpoint = "endpoint",
            .allocation_state = state->CaptureSnapshot(),
        };
    };
    struct Fault {
        const char* diagnostic;
        std::function<void(OffsetBufferAllocatorSnapshot&)> apply;
    };
    const std::vector<Fault> faults{
        {"bounds", [](auto& s) { ++s.base; }},
        {"bounds", [](auto& s) { ++s.capacity; }},
        {"usage", [](auto& s) { s.used_bytes = s.capacity + 1; }},
        {"used_bytes", [](auto& s) { s.used_bytes = 0; }},
        {"layout", [](auto& s) { s.allocation_state.layout.reset(); }},
        {"multiplier_bits",
         [](auto& s) { s.allocation_state.multiplier_bits = 64; }},
        {"allocated_num", [](auto& s) { ++s.allocation_state.allocated_num; }},
        {"allocated_size",
         [](auto& s) { s.used_bytes = s.allocation_state.allocated_size = 0; }},
    };
    for (const auto& fault : faults) {
        SCOPED_TRACE(fault.diagnostic);
        auto candidate = snapshot();
        fault.apply(candidate);
        const auto valid = candidate.Validate();
        ASSERT_FALSE(valid.has_value());
        EXPECT_NE(valid.error().find(fault.diagnostic), std::string::npos);
        EXPECT_EQ(metrics.get_allocated_mem_size(), baseline);
        auto restored = OffsetBufferAllocator::Restore(std::move(candidate));
        ASSERT_FALSE(restored.has_value());
        EXPECT_EQ(restored.error(), ErrorCode::INVALID_PARAMS);
        EXPECT_EQ(metrics.get_allocated_mem_size(), baseline);
    }
    auto clean = snapshot();
    ASSERT_TRUE(clean.Validate().has_value());
    EXPECT_EQ(metrics.get_allocated_mem_size(), baseline);
    {
        auto restored = OffsetBufferAllocator::Restore(std::move(clean));
        ASSERT_TRUE(restored.has_value());
        EXPECT_EQ((*restored)->size(), 4097U);
        EXPECT_EQ(metrics.get_allocated_mem_size(), baseline + 4097);
    }
    EXPECT_EQ(metrics.get_allocated_mem_size(), baseline);
}

TEST_F(BufferAllocatorTest, ImportCachelibAllocationsAtOriginalAddresses) {
    constexpr uintptr_t kBase = 0x1C0000000ULL;
    constexpr size_t kCapacity = 4 * facebook::cachelib::Slab::kSize;
    const std::string segment = "cachelib-restore";
    const std::string endpoint = "cachelib-restore-endpoint";
    auto created =
        CachelibBufferAllocator::Create(segment, kBase, kCapacity, endpoint);
    ASSERT_TRUE(created.has_value());
    auto original = std::move(*created);

    auto small_first = original->allocate(64);
    auto small_hole = original->allocate(64);
    auto small_last = original->allocate(64);
    auto large_first = original->allocate(4096);
    auto large_hole = original->allocate(4096);
    auto large_last = original->allocate(4096);
    ASSERT_NE(small_first, nullptr);
    ASSERT_NE(small_hole, nullptr);
    ASSERT_NE(small_last, nullptr);
    ASSERT_NE(large_first, nullptr);
    ASSERT_NE(large_hole, nullptr);
    ASSERT_NE(large_last, nullptr);

    std::vector<AllocatedBuffer::Descriptor> descriptors = {
        large_last->get_descriptor(), small_first->get_descriptor(),
        large_first->get_descriptor(), small_last->get_descriptor()};
    std::vector<LiveAllocation> allocations;
    allocations.reserve(descriptors.size());
    for (const auto& descriptor : descriptors) {
        allocations.push_back(ToLiveAllocation(kBase, descriptor));
    }
    small_hole.reset();
    large_hole.reset();

    auto restored = ImportCachelibBufferAllocator(segment, kBase, kCapacity,
                                                  endpoint, allocations);
    ASSERT_TRUE(restored.has_value());
    ASSERT_EQ(restored->buffers.size(), descriptors.size());
    for (size_t i = 0; i < descriptors.size(); ++i) {
        EXPECT_EQ(reinterpret_cast<uintptr_t>(restored->buffers[i]->data()),
                  descriptors[i].buffer_address_);
    }

    auto new_buffer = restored->allocator->allocate(64);
    ASSERT_NE(new_buffer, nullptr);
    const auto new_address = reinterpret_cast<uintptr_t>(new_buffer->data());
    for (const auto& descriptor : descriptors) {
        EXPECT_NE(new_address, descriptor.buffer_address_);
    }

    const uintptr_t released = descriptors[1].buffer_address_;
    restored->buffers[1].reset();
    auto replacement = restored->allocator->allocate(descriptors[1].size_);
    ASSERT_NE(replacement, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(replacement->data()), released);
}

TEST_F(BufferAllocatorTest, ImportCachelibAllocationsRejectsInvalidLayouts) {
    constexpr uintptr_t kBase = 0x1D0000000ULL;
    constexpr size_t kCapacity = 4 * facebook::cachelib::Slab::kSize;
    const std::string endpoint = "cachelib-invalid-endpoint";
    auto allocation = [&](uintptr_t address, uint64_t size) {
        return LiveAllocation{address - kBase, size};
    };
    auto import = [&](const std::vector<LiveAllocation>& allocations) {
        return ImportCachelibBufferAllocator("cachelib-invalid", kBase,
                                             kCapacity, endpoint, allocations);
    };

    EXPECT_FALSE(
        import({allocation(kBase, 64), allocation(kBase, 4096)}).has_value());
    EXPECT_FALSE(import({allocation(kBase + 1, 64)}).has_value());
    EXPECT_FALSE(
        import({allocation(kBase, 64), allocation(kBase, 64)}).has_value());
    EXPECT_FALSE(import({allocation(kBase + kCapacity, 64)}).has_value());
}

TEST_F(BufferAllocatorTest, CachelibImportRejectsChunkInSlabTail) {
    constexpr uintptr_t kBase = 0x1E0000000ULL;
    constexpr size_t kCapacity = 2 * facebook::cachelib::Slab::kSize;
    constexpr uint32_t kAllocSize = facebook::cachelib::Slab::kSize - 16;
    const size_t header_size = sizeof(facebook::cachelib::SlabHeader) * 2 + 1;
    auto headers = std::make_unique<char[]>(header_size);
    facebook::cachelib::MemoryAllocator allocator(
        facebook::cachelib::MemoryAllocator::Config({kAllocSize}),
        headers.get(), header_size, reinterpret_cast<void*>(kBase), kCapacity);
    const auto pool = allocator.addPool("main", kCapacity);

    EXPECT_FALSE(allocator.importAllocations(
        pool, {{reinterpret_cast<void*>(kBase + kAllocSize), kAllocSize}}));
}

TEST_F(BufferAllocatorTest, CachelibImportRejectsNonMemoryReplicaType) {
    constexpr uintptr_t kBase = 0x1F0000000ULL;
    constexpr size_t kCapacity = 2 * facebook::cachelib::Slab::kSize;
    const std::string endpoint = "cachelib-memory-only";
    std::vector<LiveAllocation> allocations = {{0, 64}};

    EXPECT_FALSE(ImportCachelibBufferAllocator("cachelib-memory-only", kBase,
                                               kCapacity, endpoint, allocations,
                                               ReplicaType::NOF_SSD)
                     .has_value());
}

// ============================================================================
// E01: requested size vs allocator-reserved size
// ============================================================================

namespace {

// 512 MiB of synthetic address space. Only SlabHeader metadata is really
// allocated; the region itself is never dereferenced, exactly like the
// existing import tests.
constexpr size_t kE01CachelibCapacity = 32 * facebook::cachelib::Slab::kSize;

// Independent oracle for a CacheLib chunk extent: the allocation-class set the
// allocator was configured with. It is read from the configuration, never from
// the chunk being checked.
size_t ExpectedCachelibReserved(size_t requested) {
    static const std::set<uint32_t> kAllocSizes =
        facebook::cachelib::MemoryAllocator::generateAllocSizes();
    const size_t padded =
        std::max<size_t>(requested, static_cast<size_t>(kMinSliceSize));
    const auto it = kAllocSizes.lower_bound(static_cast<uint32_t>(padded));
    return it == kAllocSizes.end() ? 0 : static_cast<size_t>(*it);
}

}  // namespace

// T01/T13: CacheLib exposes the real allocation-class chunk while the transfer
// length stays the requested byte count.
TEST_F(BufferAllocatorTest, E01CachelibReservedMatchesAllocationClass) {
    const std::string segment = "e01-cachelib-reserved";
    auto created = CachelibBufferAllocator::Create(
        segment, 0x200000000ULL, kE01CachelibCapacity, segment);
    ASSERT_TRUE(created.has_value());

    const std::vector<size_t> requests = {
        1, 64, 72, 477, 4096, 4097, 24576, 100000, 1024 * 1024 + 1};
    for (const size_t request : requests) {
        auto buffer = (*created)->allocate(request);
        ASSERT_NE(buffer, nullptr) << "request=" << request;
        EXPECT_EQ(buffer->requested_size(), request);
        EXPECT_EQ(buffer->size(), request);
        ASSERT_TRUE(buffer->reserved_size().has_value())
            << "request=" << request;
        const size_t reserved = *buffer->reserved_size();
        EXPECT_GE(reserved, request) << "request=" << request;
        EXPECT_EQ(reserved, ExpectedCachelibReserved(request))
            << "request=" << request;
        // A single chunk is never reported as a whole slab.
        EXPECT_LT(reserved, facebook::cachelib::Slab::kSize)
            << "request=" << request;
        // The transfer descriptor keeps the requested length, not padded bytes.
        EXPECT_EQ(buffer->get_descriptor().size_, request);
    }
}

// T02/T13: Offset reports the real node extent; the free-space drop is an
// independent oracle for it.
TEST_F(BufferAllocatorTest, E01OffsetReservedMatchesNodeExtent) {
    constexpr size_t kCapacity = 16 * 1024 * 1024;
    const std::string segment = "e01-offset-reserved";
    auto allocator = std::make_shared<OffsetBufferAllocator>(
        segment, 0x240000000ULL, kCapacity, segment);
    auto internal = allocator->getOffsetAllocator();

    const std::vector<size_t> requests = {1, 64, 477, 4096, 4097, 100000};
    for (const size_t request : requests) {
        const uint64_t free_before = internal->storageReport().totalFreeSpace;
        auto buffer = allocator->allocate(request);
        ASSERT_NE(buffer, nullptr) << "request=" << request;
        const uint64_t free_after = internal->storageReport().totalFreeSpace;
        EXPECT_EQ(buffer->requested_size(), request);
        EXPECT_EQ(buffer->size(), request);
        ASSERT_TRUE(buffer->reserved_size().has_value())
            << "request=" << request;
        const size_t reserved = *buffer->reserved_size();
        EXPECT_GE(reserved, request) << "request=" << request;
        EXPECT_EQ(static_cast<uint64_t>(reserved), free_before - free_after)
            << "request=" << request;
        EXPECT_EQ(buffer->get_descriptor().size_, request);
    }
}

// T03: the multiplier path of a segment larger than the largest bin. No large
// payload is ever allocated; only synthetic offsets are used.
TEST_F(BufferAllocatorTest, E01OffsetMultiplierReservedExtent) {
    constexpr size_t kCapacity = 8ULL * 1024 * 1024 * 1024;  // 8 GiB synthetic
    const std::string segment = "e01-offset-multiplier";
    auto allocator = std::make_shared<OffsetBufferAllocator>(
        segment, 0x400000000ULL, kCapacity, segment);
    auto internal = allocator->getOffsetAllocator();
    const size_t request = 3ULL * 1024 * 1024 * 1024 + 4096;  // > 2 GiB

    const uint64_t free_before = internal->storageReport().totalFreeSpace;
    auto buffer = allocator->allocate(request);
    ASSERT_NE(buffer, nullptr);
    const uint64_t free_after = internal->storageReport().totalFreeSpace;
    ASSERT_TRUE(buffer->reserved_size().has_value());
    EXPECT_GE(*buffer->reserved_size(), request);
    EXPECT_EQ(static_cast<uint64_t>(*buffer->reserved_size()),
              free_before - free_after);
    // The multiplier must actually be engaged for this segment size: the
    // smallest request already occupies a quantum larger than one byte, and
    // that quantum divides the reported extent.
    const uint64_t quantum = internal->normalizedAllocationSize(1);
    EXPECT_GT(quantum, 1U) << "size multiplier must be engaged for 8 GiB";
    EXPECT_EQ(quantum & (quantum - 1), 0U) << "quantum must be a power of two";
    EXPECT_EQ(internal->normalizedAllocationSize(request) % quantum, 0U);
    EXPECT_EQ(*buffer->reserved_size() % quantum, 0U);
}

// T06 (continued): self-move assignment must be a no-op, not a release.
TEST_F(BufferAllocatorTest, E01SelfMoveAssignmentKeepsTheAllocation) {
    for (const auto& allocator_type : allocator_types_) {
        const size_t capacity = allocator_type == BufferAllocatorType::CACHELIB
                                    ? kE01CachelibCapacity
                                    : 16 * 1024 * 1024;
        auto allocator =
            CreateTestAllocator("e01-self-move", 0, capacity, allocator_type);

        auto buffer = allocator->allocate(4096);
        ASSERT_NE(buffer, nullptr);
        ASSERT_TRUE(buffer->reserved_size().has_value());
        const size_t reserved = *buffer->reserved_size();

        // Obscured behind a pointer so the compiler cannot fold this into a
        // trivially diagnosable self-move.
        auto self_assign = [](AllocatedBuffer& target) {
            AllocatedBuffer* alias = &target;
            target = std::move(*alias);
        };
        self_assign(*buffer);

        EXPECT_EQ(buffer->requested_size(), 4096U);
        ASSERT_TRUE(buffer->reserved_size().has_value());
        EXPECT_EQ(*buffer->reserved_size(), reserved);
        EXPECT_EQ(allocator->size(), 4096U)
            << "a self-move must not release the allocation";

        buffer.reset();
        EXPECT_EQ(allocator->size(), 0U);
    }
}

// T14 (metadata half): a CXL offset is not a normal virtual address, so it must
// not be attributed to CacheLib allocation metadata. The accounting fields and
// the transfer length must survive the rewrite unchanged.
TEST_F(BufferAllocatorTest, E01CxlOffsetIsNotAttributedToAllocatorMetadata) {
    constexpr uintptr_t kBase = 0x200000000ULL;
    const std::string endpoint = "e01-cxl-endpoint";
    auto created = CachelibBufferAllocator::Create(
        "e01-cxl", kBase, 4 * facebook::cachelib::Slab::kSize, endpoint);
    ASSERT_TRUE(created.has_value());

    auto buffer = (*created)->allocate(477);
    ASSERT_NE(buffer, nullptr);
    ASSERT_TRUE(buffer->reserved_size().has_value());
    const size_t reserved = *buffer->reserved_size();
    void* const real_address = buffer->data();
    // Before the rewrite the address is attributable.
    ASSERT_TRUE((*created)->lookupReservedSize(real_address).has_value());

    buffer->change_to_cxl("e01-cxl-segment");

    EXPECT_EQ(buffer->requested_size(), 477U);
    EXPECT_EQ(buffer->size(), 477U);
    ASSERT_TRUE(buffer->reserved_size().has_value());
    EXPECT_EQ(*buffer->reserved_size(), reserved);
    const auto descriptor = buffer->get_descriptor();
    EXPECT_EQ(descriptor.size_, 477U);
    EXPECT_EQ(descriptor.protocol_, "cxl");
    EXPECT_EQ(descriptor.transport_endpoint_, "e01-cxl-segment");
    EXPECT_NE(buffer->data(), real_address);
    // The rewritten address is an offset, so it must stay unattributable
    // rather than be interpreted as a slab address.
    EXPECT_FALSE((*created)->lookupReservedSize(buffer->data()).has_value());
    // And the real address is still recoverable for the free path.
    EXPECT_EQ(buffer->get_vaddr_from_cxl(), real_address);
}

// T04: zero-length behaviour stays per-backend and matches baseline.
TEST_F(BufferAllocatorTest, E01ZeroLengthSemanticsAreUnchanged) {
    // CacheLib pads a zero request to kMinSliceSize and hands out a chunk.
    {
        const std::string segment = "e01-zero-cachelib";
        auto created = CachelibBufferAllocator::Create(
            segment, 0x280000000ULL, kE01CachelibCapacity, segment);
        ASSERT_TRUE(created.has_value());
        auto buffer = (*created)->allocate(0);
        ASSERT_NE(buffer, nullptr);
        EXPECT_EQ(buffer->requested_size(), 0U);
        EXPECT_EQ(buffer->size(), 0U);
        ASSERT_TRUE(buffer->reserved_size().has_value());
        EXPECT_EQ(*buffer->reserved_size(), ExpectedCachelibReserved(0));
        EXPECT_GE(*buffer->reserved_size(), static_cast<size_t>(kMinSliceSize));
    }
    // The offset allocator rejects a zero request outright.
    {
        auto allocator = std::make_shared<OffsetBufferAllocator>(
            "e01-zero-offset", 0x2C0000000ULL, 1024 * 1024, "e01-zero-offset");
        EXPECT_EQ(allocator->allocate(0), nullptr);
        EXPECT_EQ(allocator->size(), 0U);
    }
}

// T05: a failed allocation leaves no chunk, buffer or accounting behind.
TEST_F(BufferAllocatorTest, E01FailedAllocationLeavesNoResidue) {
    for (const auto& allocator_type : allocator_types_) {
        const size_t capacity = allocator_type == BufferAllocatorType::CACHELIB
                                    ? kE01CachelibCapacity
                                    : 16 * 1024 * 1024;
        auto allocator =
            CreateTestAllocator("e01-failure", 0, capacity, allocator_type);
        ASSERT_EQ(allocator->size(), 0U);

        auto too_large = allocator->allocate(capacity * 2);
        EXPECT_EQ(too_large, nullptr);
        EXPECT_EQ(allocator->size(), 0U);

        auto live = allocator->allocate(4096);
        ASSERT_NE(live, nullptr);
        ASSERT_TRUE(live->reserved_size().has_value());
        EXPECT_GE(*live->reserved_size(), 4096U);
        EXPECT_EQ(allocator->size(), 4096U);
        live.reset();
        EXPECT_EQ(allocator->size(), 0U);
    }
}

// T06: object-level move carries both extents and releases each chunk once.
TEST_F(BufferAllocatorTest, E01MoveTransfersBothExtentsWithoutDoubleRelease) {
    for (const auto& allocator_type : allocator_types_) {
        const size_t capacity = allocator_type == BufferAllocatorType::CACHELIB
                                    ? kE01CachelibCapacity
                                    : 16 * 1024 * 1024;
        auto allocator =
            CreateTestAllocator("e01-move", 0, capacity, allocator_type);

        {
            auto source = allocator->allocate(4096);
            ASSERT_NE(source, nullptr);
            ASSERT_TRUE(source->reserved_size().has_value());
            const size_t source_reserved = *source->reserved_size();
            EXPECT_EQ(allocator->size(), 4096U);

            // Move construction.
            AllocatedBuffer moved(std::move(*source));
            source.reset();  // moved-from: owns nothing, must not free
            EXPECT_EQ(moved.requested_size(), 4096U);
            EXPECT_EQ(moved.size(), 4096U);
            ASSERT_TRUE(moved.reserved_size().has_value());
            EXPECT_EQ(*moved.reserved_size(), source_reserved);
            EXPECT_EQ(allocator->size(), 4096U);

            // Move assignment over an occupied destination.
            auto victim = allocator->allocate(8192);
            ASSERT_NE(victim, nullptr);
            EXPECT_EQ(allocator->size(), 4096U + 8192U);
            AllocatedBuffer destination(std::move(*victim));
            victim.reset();

            destination = std::move(moved);
            EXPECT_EQ(allocator->size(), 4096U)
                << "the destination's previous chunk must be released once";
            EXPECT_EQ(destination.requested_size(), 4096U);
            ASSERT_TRUE(destination.reserved_size().has_value());
            EXPECT_EQ(*destination.reserved_size(), source_reserved);

            // The moved-from source owns nothing.
            EXPECT_EQ(moved.requested_size(), 0U);
            EXPECT_FALSE(moved.reserved_size().has_value());
        }
        EXPECT_EQ(allocator->size(), 0U)
            << "every chunk must be released exactly once";
    }
}

// T07: mixed allocate/free in several orders returns the requested aggregate
// to the baseline and keeps the allocator usable.
TEST_F(BufferAllocatorTest, E01MixedAllocateFreeReturnsUsageToBaseline) {
    for (const auto& allocator_type : allocator_types_) {
        const size_t capacity = allocator_type == BufferAllocatorType::CACHELIB
                                    ? kE01CachelibCapacity
                                    : 16 * 1024 * 1024;
        auto allocator =
            CreateTestAllocator("e01-churn", 0, capacity, allocator_type);

        const std::vector<size_t> sizes = {1, 477, 4096, 4097, 100000, 1};
        for (size_t round = 0; round < 3; ++round) {
            std::vector<std::unique_ptr<AllocatedBuffer>> live;
            for (const size_t size : sizes) {
                auto buffer = allocator->allocate(size);
                ASSERT_NE(buffer, nullptr)
                    << "round=" << round << " size=" << size;
                ASSERT_TRUE(buffer->reserved_size().has_value());
                EXPECT_GE(*buffer->reserved_size(), size);
                live.push_back(std::move(buffer));
            }
            if (round % 2 == 0) {
                live.clear();  // reverse destruction order
            } else {
                while (!live.empty()) {
                    live.pop_back();
                }
            }
            EXPECT_EQ(allocator->size(), 0U) << "round=" << round;
        }

        auto after = allocator->allocate(4096);
        ASSERT_NE(after, nullptr);
        after.reset();
        EXPECT_EQ(allocator->size(), 0U);
    }
}

// T08: CacheLib import rebuilds the reserved extent from the imported class
// metadata, not from the requested size.
TEST_F(BufferAllocatorTest,
       E01CachelibImportRebuildsReservedFromClassMetadata) {
    constexpr uintptr_t kBase = 0x300000000ULL;
    const std::string segment = "e01-cachelib-import";
    auto created = CachelibBufferAllocator::Create(
        segment, kBase, kE01CachelibCapacity, segment);
    ASSERT_TRUE(created.has_value());

    const std::vector<size_t> requests = {477, 4096, 100000};
    std::vector<LiveAllocation> allocations;
    std::vector<size_t> expected_reserved;
    for (const size_t request : requests) {
        auto buffer = (*created)->allocate(request);
        ASSERT_NE(buffer, nullptr);
        ASSERT_TRUE(buffer->reserved_size().has_value());
        allocations.push_back(
            ToLiveAllocation(kBase, buffer->get_descriptor()));
        expected_reserved.push_back(*buffer->reserved_size());
    }

    auto restored = ImportCachelibBufferAllocator(
        segment, kBase, kE01CachelibCapacity, segment, allocations);
    ASSERT_TRUE(restored.has_value());
    ASSERT_EQ(restored->buffers.size(), requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        EXPECT_EQ(restored->buffers[i]->requested_size(), requests[i]);
        ASSERT_TRUE(restored->buffers[i]->reserved_size().has_value()) << i;
        EXPECT_EQ(*restored->buffers[i]->reserved_size(), expected_reserved[i])
            << i;
        EXPECT_EQ(*restored->buffers[i]->reserved_size(),
                  ExpectedCachelibReserved(requests[i]))
            << i;
    }
}

// T09: Offset import rebuilds the extent from the reconstructed node.
TEST_F(BufferAllocatorTest, E01OffsetImportRebuildsReservedFromNodeExtent) {
    constexpr uintptr_t kBase = 0x340000000ULL;
    constexpr size_t kCapacity = 16 * 1024 * 1024;
    const std::string segment = "e01-offset-import";
    auto created = CreateBufferAllocator(BufferAllocatorType::OFFSET, segment,
                                         kBase, kCapacity, segment);
    ASSERT_TRUE(created.has_value());
    auto original = std::move(*created);

    auto first = original->allocate(477);
    auto hole = original->allocate(4096);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(hole, nullptr);
    ASSERT_TRUE(first->reserved_size().has_value());
    ASSERT_TRUE(hole->reserved_size().has_value());
    const size_t first_reserved = *first->reserved_size();
    EXPECT_GT(first_reserved, 477U);
    std::vector<LiveAllocation> live = {
        ToLiveAllocation(kBase, first->get_descriptor())};
    hole.reset();

    auto restored =
        ImportOffsetBufferAllocator(segment, kBase, kCapacity, segment, live);
    ASSERT_TRUE(restored.has_value());
    ASSERT_EQ(restored->buffers.size(), 1U);
    EXPECT_EQ(restored->buffers[0]->requested_size(), 477U);
    ASSERT_TRUE(restored->buffers[0]->reserved_size().has_value());
    EXPECT_EQ(*restored->buffers[0]->reserved_size(), first_reserved);
}

// T11: a descriptor-only buffer keeps the reserved extent unknown instead of
// pretending it equals the requested length.
TEST_F(BufferAllocatorTest, E01DescriptorOnlyBufferKeepsReservedUnknown) {
    constexpr uintptr_t kBase = 0x380000000ULL;
    const std::string endpoint = "e01-descriptor-endpoint";
    auto created = CachelibBufferAllocator::Create(
        "e01-descriptor", kBase, 4 * facebook::cachelib::Slab::kSize, endpoint);
    ASSERT_TRUE(created.has_value());

    auto live = (*created)->allocate(477);
    ASSERT_NE(live, nullptr);
    ASSERT_TRUE(live->reserved_size().has_value());
    const auto descriptor = live->get_descriptor();

    // Standby NoF restore builds metadata-only buffers on a Dummy allocator.
    auto dummy =
        std::make_shared<DummyBufferAllocator>("e01-descriptor", endpoint);
    AllocatedBuffer metadata_only(dummy, descriptor);
    EXPECT_EQ(metadata_only.requested_size(), 477U);
    EXPECT_EQ(metadata_only.size(), 477U);
    EXPECT_FALSE(metadata_only.reserved_size().has_value());
    EXPECT_EQ(metadata_only.get_descriptor().size_, 477U);
    EXPECT_EQ(metadata_only.get_descriptor().transport_endpoint_, endpoint);
}

// T14: the base allocator's default lookup reports "unknown" instead of
// inventing an extent for allocators without allocation metadata.
TEST_F(BufferAllocatorTest, E01DefaultReservedLookupIsUnknown) {
    DummyBufferAllocator dummy("e01-dummy", "e01-dummy-endpoint");
    EXPECT_FALSE(dummy.lookupReservedSize(nullptr).has_value());
    EXPECT_FALSE(dummy.lookupReservedSize(reinterpret_cast<const void*>(0x1234))
                     .has_value());
}

// Test allocation request larger than available space
TEST_F(BufferAllocatorTest, AllocateTooLarge) {
    for (const auto& allocator_type : allocator_types_) {
        std::string segment_name = "3";
        size_t size = 1024 * 1024 * 16;  // 16MB (must be multiple of 4MB)

        auto allocator = CreateTestAllocator(segment_name, 0x20000000ULL, size,
                                             allocator_type);

        // Attempt to allocate more than total buffer size
        size_t alloc_size = size + 1;
        auto bufHandle = allocator->allocate(alloc_size);
        EXPECT_EQ(bufHandle, nullptr);
    }
}

// Test repeated allocation and deallocation until the total allocated size
// larger than the buffer size
TEST_F(BufferAllocatorTest, RepeatAllocateAndDeallocate) {
    for (const auto& allocator_type : allocator_types_) {
        std::string segment_name = "test";
        size_t size = 1024 * 1024 * 16;  // 16MB (must be multiple of 4MB)
        auto allocator = CreateTestAllocator(segment_name, 0x20000000ULL, size,
                                             allocator_type);

        // Allocate and deallocate multiple times
        size_t alloc_size = 1024;
        for (size_t i = 0; i < size / alloc_size * 2; ++i) {
            auto bufHandle = allocator->allocate(alloc_size);
            ASSERT_NE(bufHandle, nullptr);
            VerifyAllocatedBuffer(*bufHandle, alloc_size, segment_name,
                                  segment_name);
        }
    }
}

// Test parallel allocation and deallocation
TEST_F(BufferAllocatorTest, ParallelAllocation) {
    for (const auto& allocator_type : allocator_types_) {
        std::string segment_name = "test";
        size_t size = 1024 * 1024 * 32;  // 32MB (must be multiple of 4MB)
        auto allocator = CreateTestAllocator(segment_name, 0x20000000ULL, size,
                                             allocator_type);

        const int num_threads = 4;
        const auto test_duration = std::chrono::seconds(1);
        std::vector<std::thread> threads;
        std::atomic<int> success_count{0};
        std::atomic<bool> saw_invalid_buffer{false};

        // Create 4 threads, each performing repeated allocation and
        // deallocation for 1 second
        for (int thread_id = 0; thread_id < num_threads; ++thread_id) {
            threads.emplace_back([&allocator, test_duration, segment_name,
                                  &success_count, &saw_invalid_buffer]() {
                auto start_time = std::chrono::steady_clock::now();

                while (std::chrono::steady_clock::now() - start_time <
                       test_duration) {
                    size_t alloc_size = 477;
                    auto bufHandle = allocator->allocate(alloc_size);
                    if (!bufHandle) {
                        std::this_thread::yield();
                        continue;
                    }

                    auto descriptor = bufHandle->get_descriptor();
                    if (bufHandle->getSegmentName() != segment_name ||
                        descriptor.transport_endpoint_ != segment_name ||
                        descriptor.size_ != alloc_size ||
                        bufHandle->data() == nullptr) {
                        saw_invalid_buffer.store(true,
                                                 std::memory_order_relaxed);
                        bufHandle.reset();
                        break;
                    }
                    success_count.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }

        // Wait for all threads to complete
        for (auto& thread : threads) {
            thread.join();
        }

        LOG(INFO) << "Completed parallel allocation/deallocation test for "
                  << (allocator_type == BufferAllocatorType::CACHELIB
                          ? "CACHELIB"
                          : "OFFSET");
        EXPECT_FALSE(saw_invalid_buffer.load(std::memory_order_relaxed));
        EXPECT_GT(success_count.load(std::memory_order_relaxed), 0);
    }
}

// Test fixture for SimpleAllocator tests
class SimpleAllocatorTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("SimpleAllocatorTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

// Test basic memory allocation and deallocation
TEST_F(SimpleAllocatorTest, BasicAllocationAndDeallocation) {
    const size_t total_size = 1024 * 1024 * 16;  // 16MB (multiple of 4MB)
    SimpleAllocator allocator(total_size);

    // Test basic allocation
    size_t alloc_size = 1024;  // 1KB
    void* ptr = allocator.allocate(alloc_size);
    ASSERT_NE(ptr, nullptr);

    // Verify memory alignment
    EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % 8, 0)
        << "Memory not 8-byte aligned";

    // Verify memory is usable
    std::memset(ptr, 0xFF, alloc_size);

    // Clean up
    allocator.deallocate(ptr, alloc_size);
}

// Test multiple allocations and deallocations
TEST_F(SimpleAllocatorTest, MultipleAllocations) {
    const size_t total_size = 1024 * 1024 * 16;  // 16MB
    SimpleAllocator allocator(total_size);

    std::vector<std::pair<void*, size_t>> allocations;
    const size_t alloc_size = 1024 * 1024;  // 1MB per block

    // Allocate multiple blocks
    for (int i = 0; i < 8; ++i) {
        void* ptr = allocator.allocate(alloc_size);
        ASSERT_NE(ptr, nullptr) << "Failed to allocate block " << i;
        allocations.emplace_back(ptr, alloc_size);
    }

    // Verify and deallocate all blocks
    for (const auto& [ptr, size] : allocations) {
        EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % 8, 0)
            << "Memory not 8-byte aligned";
        allocator.deallocate(ptr, size);
    }
}

// Test allocation request larger than available space
TEST_F(SimpleAllocatorTest, AllocationTooLarge) {
    const size_t total_size = 1024 * 1024 * 16;  // 16MB
    SimpleAllocator allocator(total_size);

    void* ptr = allocator.allocate(total_size + 1);
    EXPECT_EQ(ptr, nullptr);
}

// Stress test with many small allocations
TEST_F(SimpleAllocatorTest, StressTest) {
    const size_t total_size = 1024 * 1024 * 256;  // 256MB for stress testing
    SimpleAllocator allocator(total_size);

    std::vector<std::pair<void*, size_t>> allocations;
    const size_t num_allocations = 100;

    // Perform multiple allocations of varying sizes
    for (size_t i = 0; i < num_allocations; ++i) {
        size_t size = 1024 * (1 + (i % 10));  // Vary between 1KB and 10KB
        void* ptr = allocator.allocate(size);
        if (ptr) {
            EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % 8, 0)
                << "Memory not 8-byte aligned";
            allocations.emplace_back(ptr, size);
        }
    }

    // Clean up all allocations in reverse order
    while (!allocations.empty()) {
        auto [ptr, size] = allocations.back();
        allocator.deallocate(ptr, size);
        allocations.pop_back();
    }
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
