#include "offset_allocator/offset_allocator.hpp"
#include "mutex.h"
#include "serializer.h"
#include "types.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <random>

namespace mooncake::offset_allocator {

// 240 bins, according to https://github.com/sebbbi/OffsetAllocator
constexpr uint32 NUM_BINS = 240;
const uint32 bin_sizes[] = {
    0,          1,          2,          3,          4,          5,
    6,          7,          8,          9,          10,         11,
    12,         13,         14,         15,         16,         18,
    20,         22,         24,         26,         28,         30,
    32,         36,         40,         44,         48,         52,
    56,         60,         64,         72,         80,         88,
    96,         104,        112,        120,        128,        144,
    160,        176,        192,        208,        224,        240,
    256,        288,        320,        352,        384,        416,
    448,        480,        512,        576,        640,        704,
    768,        832,        896,        960,        1024,       1152,
    1280,       1408,       1536,       1664,       1792,       1920,
    2048,       2304,       2560,       2816,       3072,       3328,
    3584,       3840,       4096,       4608,       5120,       5632,
    6144,       6656,       7168,       7680,       8192,       9216,
    10240,      11264,      12288,      13312,      14336,      15360,
    16384,      18432,      20480,      22528,      24576,      26624,
    28672,      30720,      32768,      36864,      40960,      45056,
    49152,      53248,      57344,      61440,      65536,      73728,
    81920,      90112,      98304,      106496,     114688,     122880,
    131072,     147456,     163840,     180224,     196608,     212992,
    229376,     245760,     262144,     294912,     327680,     360448,
    393216,     425984,     458752,     491520,     524288,     589824,
    655360,     720896,     786432,     851968,     917504,     983040,
    1048576,    1179648,    1310720,    1441792,    1572864,    1703936,
    1835008,    1966080,    2097152,    2359296,    2621440,    2883584,
    3145728,    3407872,    3670016,    3932160,    4194304,    4718592,
    5242880,    5767168,    6291456,    6815744,    7340032,    7864320,
    8388608,    9437184,    10485760,   11534336,   12582912,   13631488,
    14680064,   15728640,   16777216,   18874368,   20971520,   23068672,
    25165824,   27262976,   29360128,   31457280,   33554432,   37748736,
    41943040,   46137344,   50331648,   54525952,   58720256,   62914560,
    67108864,   75497472,   83886080,   92274688,   100663296,  109051904,
    117440512,  125829120,  134217728,  150994944,  167772160,  184549376,
    201326592,  218103808,  234881024,  251658240,  268435456,  301989888,
    335544320,  369098752,  402653184,  436207616,  469762048,  503316480,
    536870912,  603979776,  671088640,  738197504,  805306368,  872415232,
    939524096,  1006632960, 1073741824, 1207959552, 1342177280, 1476395008,
    1610612736, 1744830464, 1879048192, 2013265920, 2147483648, 2415919104,
    2684354560, 2952790016, 3221225472, 3489660928, 3758096384, 4026531840,
};

// Forward declaration
class AllocatorWrapper;

// The wrapper will inform the AllocatorWrapper when the handle is destroyed.
class AllocationHandleWrapper {
   public:
    // Constructor for valid allocation
    AllocationHandleWrapper(std::shared_ptr<AllocatorWrapper> allocator_wrapper,
                            OffsetAllocationHandle handle)
        : m_allocator_wrapper(std::move(allocator_wrapper)),
          m_handle(std::move(handle)) {}

    // Move constructor
    AllocationHandleWrapper(AllocationHandleWrapper&& other) noexcept
        : m_allocator_wrapper(std::move(other.m_allocator_wrapper)),
          m_handle(std::move(other.m_handle)) {}

    // Move assignment operator
    AllocationHandleWrapper& operator=(
        AllocationHandleWrapper&& other) noexcept;

    // Disable copy constructor and copy assignment
    AllocationHandleWrapper(const AllocationHandleWrapper&) = delete;
    AllocationHandleWrapper& operator=(const AllocationHandleWrapper&) = delete;

    // Destructor - automatically notifies allocator wrapper
    ~AllocationHandleWrapper();

    // Check if the allocation handle is valid
    bool isValid() const { return m_handle.isValid(); }

    // Get address
    uint64_t address() const { return m_handle.address(); }

    // Get size
    uint64_t size() const { return m_handle.size(); }

    // Get the underlying handle
    OffsetAllocationHandle& getHandle() { return m_handle; }

   private:
    std::shared_ptr<AllocatorWrapper> m_allocator_wrapper;
    OffsetAllocationHandle m_handle;
};

// The wrapper will track the allocated memory spaces and check if the
// allocation is legal.
class AllocatorWrapper : public std::enable_shared_from_this<AllocatorWrapper> {
   public:
    static std::shared_ptr<AllocatorWrapper> create(uint64_t base, size_t size,
                                                    uint32 max_capacity) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<uint32> init_capacity_dist(1,
                                                                 max_capacity);
        uint32 init_capacity = init_capacity_dist(gen);
        return std::shared_ptr<AllocatorWrapper>(
            new AllocatorWrapper(base, size, init_capacity, max_capacity));
    }

    static std::shared_ptr<AllocatorWrapper> create(uint64_t base, size_t size,
                                                    uint32 init_capacity,
                                                    uint32 max_capacity) {
        return std::shared_ptr<AllocatorWrapper>(
            new AllocatorWrapper(base, size, init_capacity, max_capacity));
    }
    AllocatorWrapper(const AllocatorWrapper&) = delete;
    AllocatorWrapper& operator=(const AllocatorWrapper&) = delete;
    AllocatorWrapper(AllocatorWrapper&& other) = default;
    AllocatorWrapper& operator=(AllocatorWrapper&& other) = default;

    ~AllocatorWrapper() = default;

    // Allocate memory and return a wrapped handle
    std::optional<AllocationHandleWrapper> allocate(size_t size) {
        if (!m_allocator) {
            return std::nullopt;
        }

        auto handle = m_allocator->allocate(size);
        if (!handle.has_value()) {
            return std::nullopt;
        }

        // Validate the allocation
        EXPECT_EQ(handle->size(), size)
            << "Allocation size mismatch: " << handle->size() << " != " << size;
        verifyAllocation(handle->address(), handle->address() + handle->size());

        // Record the allocation
        m_allocated_regions[handle->address()] = {
            handle->address(), handle->address() + handle->size()};

        return AllocationHandleWrapper(shared_from_this(), std::move(*handle));
    }

    // Substitute the allocator with a new one.
    void substituteAllocator(std::shared_ptr<OffsetAllocator> allocator) {
        m_allocator = std::move(allocator);
    }

    std::shared_ptr<OffsetAllocator> getAllocator() const {
        return m_allocator;
    }

    // Get storage report
    OffsetAllocStorageReport storageReport() const {
        return m_allocator->storageReport();
    }

    // Get metrics
    OffsetAllocatorMetrics getMetrics() const {
        return m_allocator->get_metrics();
    }

   private:
    // Constructor
    AllocatorWrapper(uint64_t base, size_t size, uint32 maxAllocs = 128 * 1024)
        : m_allocator(
              OffsetAllocator::create(base, size, maxAllocs / 2, maxAllocs)),
          m_base(base),
          m_buffer_size(size) {
        // The allocator is created with the specified base and size
        // We can now properly track the allocation bounds
    }

    // Constructor with specified init_capacity and max_capacity.
    AllocatorWrapper(uint64_t base, size_t size, uint32 init_capacity,
                     uint32 max_capacity)
        : m_allocator(
              OffsetAllocator::create(base, size, init_capacity, max_capacity)),
          m_base(base),
          m_buffer_size(size) {
        // The allocator is created with the specified base and size
        // We can now properly track the allocation bounds
    }

    // Called by AllocationHandleWrapper when it's destroyed
    void onHandleDeallocated(uint64_t address, uint64_t size) {
        ASSERT_TRUE(m_allocated_regions.find(address) !=
                    m_allocated_regions.end())
            << "Allocation not found in tracking: " << address;
        ASSERT_EQ(m_allocated_regions[address].end, address + size)
            << "Allocation size mismatch: " << m_allocated_regions[address].end
            << " != " << address + size;
        m_allocated_regions.erase(address);
    }

    // Check if an allocation is legal (within bounds and doesn't overlap)
    void verifyAllocation(uint64_t begin, uint64_t end) const {
        // Check bounds
        ASSERT_TRUE(begin >= m_base && end <= m_base + m_buffer_size)
            << "Allocation is out of bounds: "
            << "Begin: " << begin << ", End: " << end << ", Base: " << m_base
            << ", Buffer Size: " << m_buffer_size;

        // Check for overlap with existing allocations using O(log(N)) algorithm
        // Find the first region that starts >= begin
        auto it = m_allocated_regions.lower_bound(begin);

        // Check if the previous region (if exists) overlaps with our allocation
        if (it != m_allocated_regions.begin()) {
            auto prev_it = std::prev(it);
            if (prev_it->second.end > begin) {
                ASSERT_TRUE(false)
                    << "Allocation overlaps with previous region: "
                    << "New allocation [" << begin << ", " << end << ") "
                    << "overlaps with existing region ["
                    << prev_it->second.begin << ", " << prev_it->second.end
                    << ")";
            }
        }

        // Check if the current region (if exists) overlaps with our allocation
        if (it != m_allocated_regions.end() && it->second.begin < end) {
            ASSERT_TRUE(false)
                << "Allocation overlaps with current region: "
                << "New allocation [" << begin << ", " << end << ") "
                << "overlaps with existing region [" << it->second.begin << ", "
                << it->second.end << ")";
        }
    }

    struct AllocatedRegion {
        uint64_t begin;
        uint64_t end;
    };

    std::shared_ptr<OffsetAllocator> m_allocator;
    uint64_t m_base;
    uint64_t m_buffer_size;
    std::map<uint64_t, AllocatedRegion> m_allocated_regions;

    friend class AllocationHandleWrapper;
};

// Implementation of AllocationHandleWrapper methods that need AllocatorWrapper
// to be fully defined
AllocationHandleWrapper::~AllocationHandleWrapper() {
    if (m_allocator_wrapper && m_handle.isValid()) {
        m_allocator_wrapper->onHandleDeallocated(m_handle.address(),
                                                 m_handle.size());
    }
}

AllocationHandleWrapper& AllocationHandleWrapper::operator=(
    AllocationHandleWrapper&& other) noexcept {
    if (this != &other) {
        // Notify allocator wrapper about deallocation
        if (m_allocator_wrapper && m_handle.isValid()) {
            m_allocator_wrapper->onHandleDeallocated(m_handle.address(),
                                                     m_handle.size());
        }

        // Move from other
        m_allocator_wrapper = std::move(other.m_allocator_wrapper);
        m_handle = std::move(other.m_handle);
    }
    return *this;
}

class OffsetAllocatorTest : public ::testing::Test {
   protected:
    void SetUp() override {}

    void TearDown() override {}

    OffsetAllocationHandle copyHandleWithNewAllocator(
        const OffsetAllocationHandle& handle,
        const std::shared_ptr<OffsetAllocator>& new_allocator) {
        return OffsetAllocationHandle(new_allocator, handle.m_allocation,
                                      handle.real_base, handle.requested_size);
    }

    void substituteWithNewAllocator(
        AllocationHandleWrapper& handle,
        std::shared_ptr<OffsetAllocator> new_allocator) {
        handle.getHandle().m_allocator = new_allocator;
    }

    void assertAllocatorEQ(const std::shared_ptr<OffsetAllocator>& a,
                           const std::shared_ptr<OffsetAllocator>& b) {
        MutexLocker lock_a(&a->m_mutex);
        MutexLocker lock_b(&b->m_mutex);
        // Compare basic member variables
        ASSERT_EQ(a->m_base, b->m_base);
        ASSERT_EQ(a->m_multiplier_bits, b->m_multiplier_bits);
        ASSERT_EQ(a->m_capacity, b->m_capacity);
        ASSERT_EQ(a->m_allocated_size, b->m_allocated_size);
        ASSERT_EQ(a->m_allocated_num, b->m_allocated_num);

        // Compare __Allocator member variables
        ASSERT_EQ(a->m_allocator->m_size, b->m_allocator->m_size);
        ASSERT_EQ(a->m_allocator->m_current_capacity,
                  b->m_allocator->m_current_capacity);
        ASSERT_EQ(a->m_allocator->m_max_capacity,
                  b->m_allocator->m_max_capacity);
        ASSERT_EQ(a->m_allocator->m_freeStorage, b->m_allocator->m_freeStorage);
        ASSERT_EQ(a->m_allocator->m_usedBinsTop, b->m_allocator->m_usedBinsTop);
        ASSERT_EQ(a->m_allocator->m_freeOffset, b->m_allocator->m_freeOffset);

        // Compare arrays
        for (uint32 i = 0; i < NUM_TOP_BINS; ++i) {
            ASSERT_EQ(a->m_allocator->m_usedBins[i],
                      b->m_allocator->m_usedBins[i]);
        }

        for (uint32 i = 0; i < NUM_LEAF_BINS; ++i) {
            ASSERT_EQ(a->m_allocator->m_binIndices[i],
                      b->m_allocator->m_binIndices[i]);
        }

        // Compare Node arrays
        for (uint32 i = 0; i < a->m_allocator->m_current_capacity; ++i) {
            ASSERT_EQ(a->m_allocator->m_nodes[i].dataOffset,
                      b->m_allocator->m_nodes[i].dataOffset);
            ASSERT_EQ(a->m_allocator->m_nodes[i].dataSize,
                      b->m_allocator->m_nodes[i].dataSize);
            ASSERT_EQ(a->m_allocator->m_nodes[i].binListPrev,
                      b->m_allocator->m_nodes[i].binListPrev);
            ASSERT_EQ(a->m_allocator->m_nodes[i].binListNext,
                      b->m_allocator->m_nodes[i].binListNext);
            ASSERT_EQ(a->m_allocator->m_nodes[i].neighborPrev,
                      b->m_allocator->m_nodes[i].neighborPrev);
            ASSERT_EQ(a->m_allocator->m_nodes[i].neighborNext,
                      b->m_allocator->m_nodes[i].neighborNext);
            ASSERT_EQ(a->m_allocator->m_nodes[i].used,
                      b->m_allocator->m_nodes[i].used);
        }

        // Compare freeNodes array
        for (uint32 i = 0; i < a->m_allocator->m_current_capacity; ++i) {
            ASSERT_EQ(a->m_allocator->m_freeNodes[i],
                      b->m_allocator->m_freeNodes[i]);
        }
    }

    // Compare two allocators bytes by bytes to detect one bit difference.
    bool isAllocatorEqual(const std::shared_ptr<OffsetAllocator>& a,
                          const std::shared_ptr<OffsetAllocator>& b) {
        MutexLocker lock_a(&a->m_mutex);
        MutexLocker lock_b(&b->m_mutex);
        // Compare basic member variables
        if (memcmp(&a->m_base, &b->m_base, sizeof(a->m_base)) != 0)
            return false;
        if (memcmp(&a->m_multiplier_bits, &b->m_multiplier_bits,
                   sizeof(a->m_multiplier_bits)) != 0)
            return false;
        if (memcmp(&a->m_capacity, &b->m_capacity, sizeof(a->m_capacity)) != 0)
            return false;
        if (memcmp(&a->m_allocated_size, &b->m_allocated_size,
                   sizeof(a->m_allocated_size)) != 0)
            return false;
        if (memcmp(&a->m_allocated_num, &b->m_allocated_num,
                   sizeof(a->m_allocated_num)) != 0)
            return false;

        // Compare __Allocator member variables
        if (memcmp(&a->m_allocator->m_size, &b->m_allocator->m_size,
                   sizeof(a->m_allocator->m_size)) != 0)
            return false;
        if (memcmp(&a->m_allocator->m_current_capacity,
                   &b->m_allocator->m_current_capacity,
                   sizeof(a->m_allocator->m_current_capacity)) != 0)
            return false;
        if (memcmp(&a->m_allocator->m_max_capacity,
                   &b->m_allocator->m_max_capacity,
                   sizeof(a->m_allocator->m_max_capacity)) != 0)
            return false;
        if (memcmp(&a->m_allocator->m_freeStorage,
                   &b->m_allocator->m_freeStorage,
                   sizeof(a->m_allocator->m_freeStorage)) != 0)
            return false;
        if (memcmp(&a->m_allocator->m_usedBinsTop,
                   &b->m_allocator->m_usedBinsTop,
                   sizeof(a->m_allocator->m_usedBinsTop)) != 0)
            return false;
        if (memcmp(&a->m_allocator->m_freeOffset, &b->m_allocator->m_freeOffset,
                   sizeof(a->m_allocator->m_freeOffset)) != 0)
            return false;

        // Compare arrays
        if (memcmp(a->m_allocator->m_usedBins, b->m_allocator->m_usedBins,
                   NUM_TOP_BINS * sizeof(a->m_allocator->m_usedBins[0])) != 0)
            return false;

        if (memcmp(a->m_allocator->m_binIndices, b->m_allocator->m_binIndices,
                   NUM_LEAF_BINS * sizeof(a->m_allocator->m_binIndices[0])) !=
            0)
            return false;

        // Compare Node arrays
        if (memcmp(a->m_allocator->m_nodes.data(),
                   b->m_allocator->m_nodes.data(),
                   a->m_allocator->m_current_capacity *
                       sizeof(a->m_allocator->m_nodes[0])) != 0)
            return false;

        // Compare freeNodes array
        if (memcmp(a->m_allocator->m_freeNodes.data(),
                   b->m_allocator->m_freeNodes.data(),
                   a->m_allocator->m_current_capacity *
                       sizeof(a->m_allocator->m_freeNodes[0])) != 0)
            return false;

        // All comparisons passed, allocators are equal
        return true;
    }

    void testSerializeAllocator(
        const std::shared_ptr<OffsetAllocator>& alloc_a,
        const std::vector<OffsetAllocationHandle>& handles) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<uint32> size_dist(
            1,
            1024 * 64);  // 1B to 64KB
        // Serialize the allocator
        std::vector<SerializedByte> buffer;
        ASSERT_EQ(serialize_to(alloc_a, buffer), ErrorCode::OK);

        // Deserialize the allocator and compare
        std::shared_ptr<OffsetAllocator> alloc_b =
            deserialize_from<OffsetAllocator>(buffer);
        ASSERT_NE(alloc_b, nullptr);
        assertAllocatorEQ(alloc_a, alloc_b);

        //============== Begin test deserialization with corrupted buffer
        //==============
        // Set the log level to fatal to avoid the log output.
        auto log_level = FLAGS_minloglevel;
        FLAGS_minloglevel = google::GLOG_FATAL;

        // Remove the last byte from the buffer and try to deserialize
        auto corrupted_buffer = buffer;
        corrupted_buffer.pop_back();
        alloc_b = deserialize_from<OffsetAllocator>(corrupted_buffer);
        ASSERT_TRUE(alloc_b == nullptr || !isAllocatorEqual(alloc_a, alloc_b));

        // Add a byte to the buffer and try to deserialize
        corrupted_buffer = buffer;
        corrupted_buffer.push_back(0);
        alloc_b = deserialize_from<OffsetAllocator>(corrupted_buffer);
        ASSERT_TRUE(alloc_b == nullptr || !isAllocatorEqual(alloc_a, alloc_b));

        //============== End test deserialization with corrupted buffer
        //==============
        // Restore the log level.
        FLAGS_minloglevel = log_level;

        // Test if the deserialized allocator can properly free allocated
        // objects.
        alloc_b = deserialize_from<OffsetAllocator>(buffer);
        ASSERT_TRUE(alloc_b != nullptr);
        for (const auto& handle : handles) {
            OffsetAllocationHandle handle_copy =
                copyHandleWithNewAllocator(handle, alloc_b);
        }
    }
};

// Test basic allocation and deallocation
TEST_F(OffsetAllocatorTest, BasicAllocation) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024 * 1024;  // 1GB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    // Allocate handle
    auto handle = allocator->allocate(ALLOCATOR_SIZE);
    ASSERT_TRUE(handle.has_value());
    EXPECT_TRUE(handle->isValid());
    EXPECT_NE(handle->address(), OffsetAllocation::NO_SPACE);
    EXPECT_EQ(handle->size(), ALLOCATOR_SIZE);

    // Try allocate new handle
    auto handle2 = allocator->allocate(ALLOCATOR_SIZE);
    ASSERT_FALSE(handle2.has_value());

    // Release handle
    handle.reset();

    // Try allocate again
    handle2 = allocator->allocate(ALLOCATOR_SIZE);
    ASSERT_TRUE(handle2.has_value());
    EXPECT_TRUE(handle2->isValid());
    EXPECT_NE(handle2->address(), OffsetAllocation::NO_SPACE);
}

// Test allocation failure when out of space
TEST_F(OffsetAllocatorTest, AllocationFailure) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024 * 1024;  // 1GB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    // Try to allocate more than available space
    auto handle =
        allocator->allocate(2 * ALLOCATOR_SIZE);  // 2GB > 1GB available
    EXPECT_FALSE(handle.has_value());
}

// Test multiple allocations
TEST_F(OffsetAllocatorTest, MultipleAllocations) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024 * 1024;  // 1GB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    std::vector<AllocationHandleWrapper> handles;

    for (int i = 0; i < 10; ++i) {
        auto handle = allocator->allocate(1000);
        ASSERT_TRUE(handle.has_value());
        handles.push_back(std::move(*handle));
    }

    // All handles should be valid and have different offsets
    for (size_t i = 0; i < handles.size(); ++i) {
        EXPECT_TRUE(handles[i].isValid());
        EXPECT_EQ(handles[i].size(), 1000);
        for (size_t j = i + 1; j < handles.size(); ++j) {
            EXPECT_NE(handles[i].address(), handles[j].address());
        }
    }
}

// Test allocations with different sizes don't overlap
TEST_F(OffsetAllocatorTest, DifferentSizesNoOverlap) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024 * 1024;  // 1GB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    std::vector<AllocationHandleWrapper> handles;
    std::vector<uint32> sizes = {100, 500, 1000, 2000, 50, 1500, 800, 300};

    for (uint32 size : sizes) {
        auto handle = allocator->allocate(size);
        ASSERT_TRUE(handle.has_value()) << "Failed to allocate size: " << size;
        EXPECT_EQ(handle->size(), size);
        handles.push_back(std::move(*handle));
    }

    // Verify all handles are valid
    for (const auto& handle : handles) {
        EXPECT_TRUE(handle.isValid());
    }
}

// Test storage reports
TEST_F(OffsetAllocatorTest, StorageReports) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024 * 1024;  // 1GB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    OffsetAllocStorageReport report = allocator->storageReport();
    EXPECT_GT(report.totalFreeSpace, 0);
    EXPECT_GT(report.largestFreeRegion, 0);

    // Allocate some space
    auto handle = allocator->allocate(1000);
    ASSERT_TRUE(handle.has_value());

    OffsetAllocStorageReport newReport = allocator->storageReport();
    EXPECT_LT(newReport.totalFreeSpace, report.totalFreeSpace);
}

// Test continuous allocation and deallocation with random sizes
TEST_F(OffsetAllocatorTest, ContinuousRandomAllocationDeallocation) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024 * 1024;  // 1GB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32> size_dist(1,
                                                    1024 * 64);  // 1B to 64KB

    const int max_iterations = 20000;

    // Allocate and deallocate random sizes
    for (int i = 0; i < max_iterations; ++i) {
        uint32_t size = size_dist(gen);
        auto handle = allocator->allocate(size);
        EXPECT_TRUE(handle.has_value()) << "Failed to allocate size: " << size;
        // It will free automatically when handle goes out of scope
    }

    auto full_space_handle = allocator->allocate(ALLOCATOR_SIZE);
    ASSERT_TRUE(full_space_handle.has_value());
    EXPECT_EQ(full_space_handle->size(), ALLOCATOR_SIZE);
}

// Full size allocation is only possible when the buffer size is exactly the
// same as one of the bin sizes.
TEST_F(OffsetAllocatorTest, FullSizeAllocation) {
    for (uint32 size : bin_sizes) {
        if (size == 0) continue;  // Skip 0 size
        constexpr uint32 MAX_ALLOCS = 1000;
        auto allocator = AllocatorWrapper::create(0, size, MAX_ALLOCS);

        auto handle = allocator->allocate(size);
        ASSERT_TRUE(handle.has_value());
    }
}

TEST_F(OffsetAllocatorTest, RepeatedLargeSizeAllocation) {
    for (size_t i = 0; i < NUM_BINS; ++i) {
        uint32_t bin_size = bin_sizes[i];
        if (bin_size < 1024) continue;  // Skip small sizes
        constexpr uint32_t MAX_ALLOCS = 1000;
        auto allocator = AllocatorWrapper::create(0, bin_size + 10, MAX_ALLOCS);
        EXPECT_EQ(allocator->storageReport().totalFreeSpace, bin_size + 10);

        for (uint32_t j = 0; j < 10; j++) {
            auto handle = allocator->allocate(bin_size - (10 - j));
            ASSERT_TRUE(handle.has_value());
        }
    }
}

// Can only allocate MAX_ALLOCS - 1 times.
TEST_F(OffsetAllocatorTest, MaxNumAllocations) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024 * 1024;
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    std::vector<AllocationHandleWrapper> handles;
    for (uint32 i = 0; i < MAX_ALLOCS - 1; ++i) {
        auto handle = allocator->allocate(1024);
        ASSERT_TRUE(handle.has_value())
            << "Failed to allocate size: " << 1024 << " at iteration: " << i;
        handles.push_back(std::move(*handle));
    }

    auto handle = allocator->allocate(1024);
    ASSERT_FALSE(handle.has_value());
}

TEST_F(OffsetAllocatorTest, FullAllocationAfterRandomAllocationAndFree) {
    const uint32 ALLOCATOR_SIZE = bin_sizes[NUM_BINS - 1];
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32> size_dist(1, ALLOCATOR_SIZE / 1000);

    std::vector<AllocationHandleWrapper> handles;
    for (uint32 i = 0; i < MAX_ALLOCS; ++i) {
        uint32 size = size_dist(gen);
        auto handle = allocator->allocate(size);
        if (handle.has_value()) {
            handles.push_back(std::move(*handle));
        }
    }

    handles.clear();
    auto handle = allocator->allocate(ALLOCATOR_SIZE);
    ASSERT_TRUE(handle.has_value());
}

// The original implementation will fail this test.
TEST_F(OffsetAllocatorTest, AllocationSameSizeAfterFree) {
    constexpr uint32 ALLOCATOR_SIZE = 2048;
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    auto handle = allocator->allocate(1023);
    ASSERT_TRUE(handle.has_value());

    auto handle2 = allocator->allocate(16);
    ASSERT_TRUE(handle2.has_value());

    handle.reset();
    handle = allocator->allocate(1023);
    ASSERT_TRUE(handle.has_value());
}

// The original implementation will fail this test.
TEST_F(OffsetAllocatorTest, RandomRepeatAllocationSameSizeAfterFree) {
    const uint32 ALLOCATOR_SIZE = bin_sizes[NUM_BINS - 1];
    constexpr uint32 MAX_ALLOCS = 10000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32> size_dist(1, ALLOCATOR_SIZE / 100);

    std::vector<AllocationHandleWrapper> handles;
    std::vector<uint32> alloc_sizes;
    for (int i = 0; i < 2000; ++i) {
        uint32 size = size_dist(gen);
        auto handle = allocator->allocate(size);
        if (handle.has_value()) {
            handles.push_back(std::move(*handle));
            alloc_sizes.push_back(size);
        }

        std::uniform_int_distribution<uint32> index_dist(0, handles.size() - 1);
        uint32 index = index_dist(gen);
        std::swap(handles[index], handles.back());
        std::swap(alloc_sizes[index], alloc_sizes.back());
        uint32 test_size = alloc_sizes.back();
        handles.pop_back();
        alloc_sizes.pop_back();

        auto handle2 = allocator->allocate(test_size);
        ASSERT_TRUE(handle2.has_value());
        handles.push_back(std::move(*handle2));
        alloc_sizes.push_back(test_size);
    }
}

// Test when the size multiplier is more than one.
TEST_F(OffsetAllocatorTest, BasicLargeAllocatorSize) {
    // The size multiplier is larger than 1 when the allocator size is larger
    // than MAX_BIN_SIZE.
    constexpr size_t MIN_BUFFER_SIZE = (1ull << 30);
    constexpr size_t MAX_BUFFER_SIZE = (1ull << 40);
    constexpr uint32 MAX_ALLOCS = 10000;

    for (size_t buffer_size = MIN_BUFFER_SIZE; buffer_size <= MAX_BUFFER_SIZE;
         buffer_size *= 2) {
        auto allocator = AllocatorWrapper::create(0, buffer_size, MAX_ALLOCS);
        size_t max_alloc_size = allocator->storageReport().largestFreeRegion;
        // The largest free region equals buffer size only in this specific
        // buffer size.
        ASSERT_EQ(max_alloc_size, buffer_size);

        auto handle = allocator->allocate(1);
        ASSERT_TRUE(handle.has_value());
        handle.reset();

        handle = allocator->allocate(max_alloc_size - 1);
        ASSERT_TRUE(handle.has_value());
        handle.reset();

        handle = allocator->allocate(max_alloc_size);
        ASSERT_TRUE(handle.has_value());
        EXPECT_EQ(handle->size(), max_alloc_size);
    }
}

// Test when the size multiplier is more than one.
TEST_F(OffsetAllocatorTest, PowerOfTwoLargeAllocatorSize) {
    // The size multiplier is larger than 1 when the allocator size is larger
    // than MAX_BIN_SIZE.
    constexpr size_t MIN_BUFFER_SIZE = (1ull << 30);
    constexpr size_t MAX_BUFFER_SIZE = (1ull << 40);
    constexpr uint32 MAX_ALLOCS = 10000;
    std::random_device rd;
    std::mt19937 gen(rd());
    for (size_t buffer_size = MIN_BUFFER_SIZE; buffer_size <= MAX_BUFFER_SIZE;
         buffer_size *= 2) {
        auto allocator = AllocatorWrapper::create(0, buffer_size, MAX_ALLOCS);
        size_t max_alloc_size = buffer_size / 100;

        std::vector<AllocationHandleWrapper> handles;
        std::vector<size_t> alloc_sizes;
        std::uniform_int_distribution<size_t> size_dist(1, max_alloc_size);
        for (int i = 0; i < 200; ++i) {
            size_t size = size_dist(gen);
            auto handle = allocator->allocate(size);
            if (handle.has_value()) {
                handles.push_back(std::move(*handle));
                alloc_sizes.push_back(size);
            }

            std::uniform_int_distribution<uint32> index_dist(
                0, handles.size() - 1);
            uint32 index = index_dist(gen);
            std::swap(handles[index], handles.back());
            std::swap(alloc_sizes[index], alloc_sizes.back());
            uint32 test_size = alloc_sizes.back();
            handles.pop_back();
            alloc_sizes.pop_back();

            auto handle2 = allocator->allocate(test_size);
            ASSERT_TRUE(handle2.has_value());
            handles.push_back(std::move(*handle2));
            alloc_sizes.push_back(test_size);
        }
    }
}

// Test when the size multiplier is more than one.
TEST_F(OffsetAllocatorTest, MaxAllocSizeWithLargeAllocatorSize) {
    // The size multiplier is larger than 1 when the allocator size is larger
    // than MAX_BIN_SIZE.
    constexpr size_t MIN_BUFFER_SIZE = (1ull << 31) + 1;
    constexpr size_t MAX_BUFFER_SIZE = (1ull << 40);
    constexpr uint32 MAX_ALLOCS = 10000;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> buffer_size_dist(MIN_BUFFER_SIZE,
                                                           MAX_BUFFER_SIZE);
    for (int i = 0; i < 100; i++) {
        size_t buffer_size = buffer_size_dist(gen);
        auto allocator = AllocatorWrapper::create(0, buffer_size, MAX_ALLOCS);
        size_t max_alloc_size = allocator->storageReport().largestFreeRegion;
        ASSERT_GT(max_alloc_size, buffer_size / 2);

        auto handle = allocator->allocate(max_alloc_size);
        ASSERT_TRUE(handle.has_value());
    }
}

// Test when the size multiplier is more than one.
TEST_F(OffsetAllocatorTest, RandomSmallAllocWithLargeAllocatorSize) {
    // The size multiplier is larger than 1 when the allocator size is larger
    // than MAX_BIN_SIZE.
    constexpr size_t MIN_BUFFER_SIZE = (1ull << 31) + 1;
    constexpr size_t MAX_BUFFER_SIZE = (1ull << 40);
    constexpr uint32 MAX_ALLOCS = 10000;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> buffer_size_dist(MIN_BUFFER_SIZE,
                                                           MAX_BUFFER_SIZE);
    for (int i = 0; i < 100; i++) {
        size_t buffer_size = buffer_size_dist(gen);
        auto allocator = AllocatorWrapper::create(0, buffer_size, MAX_ALLOCS);
        size_t max_alloc_size = buffer_size / 100;

        std::vector<AllocationHandleWrapper> handles;
        std::vector<size_t> alloc_sizes;
        std::uniform_int_distribution<size_t> size_dist(1, max_alloc_size);
        for (int j = 0; j < 200; ++j) {
            size_t size = size_dist(gen);
            auto handle = allocator->allocate(size);
            if (handle.has_value()) {
                handles.push_back(std::move(*handle));
                alloc_sizes.push_back(size);
            }

            std::uniform_int_distribution<uint32> index_dist(
                0, handles.size() - 1);
            uint32 index = index_dist(gen);
            std::swap(handles[index], handles.back());
            std::swap(alloc_sizes[index], alloc_sizes.back());
            uint32 test_size = alloc_sizes.back();
            handles.pop_back();
            alloc_sizes.pop_back();

            auto handle2 = allocator->allocate(test_size);
            ASSERT_TRUE(handle2.has_value());
            handles.push_back(std::move(*handle2));
            alloc_sizes.push_back(test_size);
        }
    }
}

// ========== EDGE CASE TESTS, Generated by AI ==========

// Test zero size allocation - should fail
TEST_F(OffsetAllocatorTest, ZeroSizeAllocation) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024;  // 1MB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    auto handle = allocator->allocate(0);
    EXPECT_FALSE(handle.has_value()) << "Zero size allocation should fail";
}

// Test allocation size of 1 byte (minimum valid size)
TEST_F(OffsetAllocatorTest, OneByteAllocation) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024;  // 1MB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    auto handle = allocator->allocate(1);
    ASSERT_TRUE(handle.has_value());
    EXPECT_TRUE(handle->isValid());
    EXPECT_EQ(handle->size(), 1);
    EXPECT_NE(handle->address(), OffsetAllocation::NO_SPACE);
}

// Test allocation at exact allocator capacity
TEST_F(OffsetAllocatorTest, ExactCapacityAllocation) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024;  // 1MB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    auto handle = allocator->allocate(ALLOCATOR_SIZE);
    ASSERT_TRUE(handle.has_value());
    EXPECT_EQ(handle->size(), ALLOCATOR_SIZE);
    EXPECT_EQ(handle->address(), 0);  // Should start at base address

    // Verify no more space available
    auto handle2 = allocator->allocate(1);
    EXPECT_FALSE(handle2.has_value());
}

// Test allocation slightly larger than capacity
TEST_F(OffsetAllocatorTest, OversizeAllocation) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024;  // 1MB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    auto handle = allocator->allocate(ALLOCATOR_SIZE + 1);
    EXPECT_FALSE(handle.has_value())
        << "Allocation larger than capacity should fail";
}

// Test allocation with size just below bin size
TEST_F(OffsetAllocatorTest, JustBelowBinSizeAllocation) {
    constexpr uint32 ALLOCATOR_SIZE = 2048;
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    // Allocate size just below a bin size
    auto handle = allocator->allocate(1023);
    ASSERT_TRUE(handle.has_value());
    EXPECT_EQ(handle->size(), 1023);

    // Should still be able to allocate the remainder
    auto handle2 = allocator->allocate(1024);
    ASSERT_TRUE(handle2.has_value());
    EXPECT_EQ(handle2->size(), 1024);
}

// Test maximum allocation count edge case
TEST_F(OffsetAllocatorTest, MaxAllocationCountEdgeCase) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024;  // 1MB
    constexpr uint32 MAX_ALLOCS = 10;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    std::vector<AllocationHandleWrapper> handles;

    // Allocate up to the limit
    for (uint32 i = 0; i < MAX_ALLOCS - 1; ++i) {
        auto handle = allocator->allocate(1024);
        ASSERT_TRUE(handle.has_value()) << "Failed at iteration " << i;
        handles.push_back(std::move(*handle));
    }

    // Try one more allocation - should fail
    auto handle = allocator->allocate(1024);
    EXPECT_FALSE(handle.has_value()) << "Should fail at max allocation count";

    // Free one allocation
    handles.pop_back();

    // Should be able to allocate again
    handle = allocator->allocate(1024);
    EXPECT_TRUE(handle.has_value());
}

// Test very small allocator size
TEST_F(OffsetAllocatorTest, VerySmallAllocatorSize) {
    constexpr uint32 ALLOCATOR_SIZE = 16;  // Very small
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    auto handle = allocator->allocate(16);
    ASSERT_TRUE(handle.has_value());
    EXPECT_EQ(handle->size(), 16);

    // Should not be able to allocate more
    auto handle2 = allocator->allocate(1);
    EXPECT_FALSE(handle2.has_value());
}

// Test allocation with size equal to allocator size minus 1
TEST_F(OffsetAllocatorTest, AllocatorSizeMinusOne) {
    constexpr uint32 ALLOCATOR_SIZE = 1024;
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    auto handle = allocator->allocate(ALLOCATOR_SIZE - 1);
    ASSERT_TRUE(handle.has_value());
    EXPECT_EQ(handle->size(), ALLOCATOR_SIZE - 1);
}

// Test allocation with size that is a power of 2
TEST_F(OffsetAllocatorTest, PowerOfTwoAllocation) {
    constexpr uint32 ALLOCATOR_SIZE = 2048;
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    // Test various power of 2 sizes
    std::vector<uint32> power_of_two_sizes = {1,  2,   4,   8,   16,  32,
                                              64, 128, 256, 512, 1024};

    for (uint32 size : power_of_two_sizes) {
        if (size <= ALLOCATOR_SIZE) {
            auto handle = allocator->allocate(size);
            ASSERT_TRUE(handle.has_value())
                << "Failed to allocate size: " << size;
            EXPECT_EQ(handle->size(), size);
            // Handle will be automatically freed when it goes out of scope
        }
    }
}

// ========== BIN SYSTEM TESTS, Generated by AI ==========

// Test bin size calculations and selection
TEST_F(OffsetAllocatorTest, BinSizeCalculation) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024;  // 1MB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    // Test that allocations are placed in appropriate bins
    // SmallFloat::uintToFloatRoundUp should determine the bin
    auto handle1 = allocator->allocate(100);
    ASSERT_TRUE(handle1.has_value());

    auto handle2 = allocator->allocate(200);
    ASSERT_TRUE(handle2.has_value());

    auto handle3 = allocator->allocate(500);
    ASSERT_TRUE(handle3.has_value());

    // All allocations should be valid and non-overlapping
    EXPECT_TRUE(handle1->isValid());
    EXPECT_TRUE(handle2->isValid());
    EXPECT_TRUE(handle3->isValid());
}

// Test bin overflow scenarios
TEST_F(OffsetAllocatorTest, BinOverflowScenarios) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024;  // 1MB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    // Fill up a specific bin size with many small allocations
    std::vector<AllocationHandleWrapper> handles;
    uint32 small_size = 64;  // Choose a small bin size

    // Allocate many small blocks to potentially overflow the bin
    for (int i = 0; i < 100; ++i) {
        auto handle = allocator->allocate(small_size);
        if (handle.has_value()) {
            handles.push_back(std::move(*handle));
        } else {
            break;  // Bin is full or out of memory
        }
    }

    // Verify all allocations are valid
    for (const auto& handle : handles) {
        EXPECT_TRUE(handle.isValid());
        EXPECT_EQ(handle.size(), small_size);
    }
}

// Test bin merging behavior when adjacent blocks are freed
TEST_F(OffsetAllocatorTest, BinMergingBehavior) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024;  // 1MB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    // Allocate three adjacent blocks
    auto handle1 = allocator->allocate(1024);
    auto handle2 = allocator->allocate(1024);
    auto handle3 = allocator->allocate(1024);

    ASSERT_TRUE(handle1.has_value());
    ASSERT_TRUE(handle2.has_value());
    ASSERT_TRUE(handle3.has_value());

    // Free the middle block first
    handle2 = std::nullopt;

    // Free the first block - should merge with the freed middle block
    handle1 = std::nullopt;

    // Free the third block - should merge with the large freed block
    handle3 = std::nullopt;

    // Now we should be able to allocate the entire space again
    auto large_handle = allocator->allocate(ALLOCATOR_SIZE);
    ASSERT_TRUE(large_handle.has_value());
    EXPECT_EQ(large_handle->size(), ALLOCATOR_SIZE);
}

// Test bin selection for edge case sizes
TEST_F(OffsetAllocatorTest, BinSelectionEdgeCases) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024;  // 1MB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    // Test sizes that are just below and above bin boundaries
    std::vector<uint32> edge_sizes = {
        1,     // Minimum size
        2,     // Power of 2
        3,     // Just above power of 2
        7,     // Just below power of 2
        8,     // Power of 2
        15,    // Just below power of 2
        16,    // Power of 2
        31,    // Just below power of 2
        32,    // Power of 2
        63,    // Just below power of 2
        64,    // Power of 2
        127,   // Just below power of 2
        128,   // Power of 2
        255,   // Just below power of 2
        256,   // Power of 2
        511,   // Just below power of 2
        512,   // Power of 2
        1023,  // Just below power of 2
        1024,  // Power of 2
    };

    for (uint32 size : edge_sizes) {
        auto handle = allocator->allocate(size);
        ASSERT_TRUE(handle.has_value()) << "Failed to allocate size: " << size;
        EXPECT_EQ(handle->size(), size);
        // Handle will be automatically freed when it goes out of scope
    }
}

// Test bin system with very large allocations
TEST_F(OffsetAllocatorTest, BinSystemLargeAllocations) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024 * 1024;  // 1GB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    // Test large allocations that should go into high-numbered bins
    std::vector<uint32> large_sizes = {
        1024 * 1024,        // 1MB
        2 * 1024 * 1024,    // 2MB
        4 * 1024 * 1024,    // 4MB
        8 * 1024 * 1024,    // 8MB
        16 * 1024 * 1024,   // 16MB
        32 * 1024 * 1024,   // 32MB
        64 * 1024 * 1024,   // 64MB
        128 * 1024 * 1024,  // 128MB
        256 * 1024 * 1024,  // 256MB
        512 * 1024 * 1024,  // 512MB
    };

    for (uint32 size : large_sizes) {
        if (size <= ALLOCATOR_SIZE) {
            auto handle = allocator->allocate(size);
            ASSERT_TRUE(handle.has_value())
                << "Failed to allocate size: " << size;
            EXPECT_EQ(handle->size(), size);
            // Handle will be automatically freed when it goes out of scope
        }
    }
}

// Test bin system with mixed allocation patterns
TEST_F(OffsetAllocatorTest, BinSystemMixedPatterns) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024;  // 1MB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    std::vector<AllocationHandleWrapper> handles;

    // Mix of small, medium, and large allocations
    std::vector<uint32> mixed_sizes = {16, 64, 256, 1024, 4096, 16384, 65536};

    for (uint32 size : mixed_sizes) {
        auto handle = allocator->allocate(size);
        ASSERT_TRUE(handle.has_value()) << "Failed to allocate size: " << size;
        EXPECT_EQ(handle->size(), size);
        handles.push_back(std::move(*handle));
    }

    // Verify all allocations are valid
    for (const auto& handle : handles) {
        EXPECT_TRUE(handle.isValid());
    }
}

// Test bin system with repeated allocation/deallocation cycles
TEST_F(OffsetAllocatorTest, BinSystemRepeatedCycles) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024;  // 1MB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    std::vector<uint32> test_sizes = {64, 128, 256, 512, 1024, 2048, 4096};

    // Perform multiple allocation/deallocation cycles
    for (int cycle = 0; cycle < 10; ++cycle) {
        std::vector<AllocationHandleWrapper> cycle_handles;

        // Allocate blocks
        for (uint32 size : test_sizes) {
            auto handle = allocator->allocate(size);
            ASSERT_TRUE(handle.has_value())
                << "Failed to allocate size: " << size << " in cycle " << cycle;
            EXPECT_EQ(handle->size(), size);
            cycle_handles.push_back(std::move(*handle));
        }

        // Verify all allocations are valid
        for (const auto& handle : cycle_handles) {
            EXPECT_TRUE(handle.isValid());
        }

        // All handles will be automatically freed when cycle_handles goes out
        // of scope
    }

    // After all cycles, should be able to allocate the full size again
    auto full_handle = allocator->allocate(ALLOCATOR_SIZE);
    ASSERT_TRUE(full_handle.has_value());
    EXPECT_EQ(full_handle->size(), ALLOCATOR_SIZE);
}

// Test bin system with allocation sizes that don't match bin boundaries
TEST_F(OffsetAllocatorTest, BinSystemNonAlignedSizes) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024;  // 1MB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    // Test sizes that don't align with typical bin boundaries
    std::vector<uint32> non_aligned_sizes = {
        17,    // Not a power of 2
        33,    // Not a power of 2
        65,    // Not a power of 2
        129,   // Not a power of 2
        257,   // Not a power of 2
        513,   // Not a power of 2
        1025,  // Just above power of 2
        2049,  // Just above power of 2
        4097,  // Just above power of 2
    };

    for (uint32 size : non_aligned_sizes) {
        auto handle = allocator->allocate(size);
        ASSERT_TRUE(handle.has_value()) << "Failed to allocate size: " << size;
        EXPECT_EQ(handle->size(), size);
        // Handle will be automatically freed when it goes out of scope
    }
}

// Test bin system with allocation sizes that are prime numbers
TEST_F(OffsetAllocatorTest, BinSystemPrimeSizes) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024;  // 1MB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    // Test sizes that are prime numbers (should be challenging for bin system)
    std::vector<uint32> prime_sizes = {
        2,    3,   5,   7,   11,  13,  17,  19,  23,  29,  31,  37,  41,  43,
        47,   53,  59,  61,  67,  71,  73,  79,  83,  89,  97,  101, 103, 107,
        109,  113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181,
        191,  193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263,
        269,  271, 277, 281, 283, 293, 307, 311, 313, 317, 331, 337, 347, 349,
        353,  359, 367, 373, 379, 383, 389, 397, 401, 409, 419, 421, 431, 433,
        439,  443, 449, 457, 461, 463, 467, 479, 487, 491, 499, 503, 509, 521,
        523,  541, 547, 557, 563, 569, 571, 577, 587, 593, 599, 601, 607, 613,
        617,  619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691, 701,
        709,  719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797, 809,
        811,  821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887,
        907,  911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997,
        1009, 1013};

    for (uint32 size : prime_sizes) {
        auto handle = allocator->allocate(size);
        ASSERT_TRUE(handle.has_value())
            << "Failed to allocate prime size: " << size;
        EXPECT_EQ(handle->size(), size);
        // Handle will be automatically freed when it goes out of scope
    }
}

// Test bin system with Fibonacci sequence sizes
TEST_F(OffsetAllocatorTest, BinSystemFibonacciSizes) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024;  // 1MB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    // Test sizes that follow the Fibonacci sequence
    std::vector<uint32> fibonacci_sizes = {
        1,     1,     2,     3,     5,     8,      13,     21,    34,   55,
        89,    144,   233,   377,   610,   987,    1597,   2584,  4181, 6765,
        10946, 17711, 28657, 46368, 75025, 121393, 196418, 317811};

    for (uint32 size : fibonacci_sizes) {
        auto handle = allocator->allocate(size);
        ASSERT_TRUE(handle.has_value())
            << "Failed to allocate Fibonacci size: " << size;
        EXPECT_EQ(handle->size(), size);
        // Handle will be automatically freed when it goes out of scope
    }
}

// Test bin system with allocation sizes that are multiples of common page sizes
TEST_F(OffsetAllocatorTest, BinSystemPageSizeMultiples) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024;  // 1MB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    // Test sizes that are multiples of common page sizes (4KB, 8KB, 16KB, 64KB)
    std::vector<uint32> page_size_multiples = {
        4096,    // 4KB
        8192,    // 8KB
        16384,   // 16KB
        32768,   // 32KB
        65536,   // 64KB
        131072,  // 128KB
        262144,  // 256KB
        524288,  // 512KB
        1048576  // 1MB
    };

    for (uint32 size : page_size_multiples) {
        auto handle = allocator->allocate(size);
        ASSERT_TRUE(handle.has_value())
            << "Failed to allocate page size multiple: " << size;
        EXPECT_EQ(handle->size(), size);
        // Handle will be automatically freed when it goes out of scope
    }
}

// Test metrics interface functionality
TEST_F(OffsetAllocatorTest, MetricsInterface) {
    constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024;  // 1MB
    constexpr uint32 MAX_ALLOCS = 1000;
    auto allocator = AllocatorWrapper::create(0, ALLOCATOR_SIZE, MAX_ALLOCS);

    // Test initial metrics - should show empty allocator
    OffsetAllocatorMetrics initial_metrics = allocator->getMetrics();
    EXPECT_EQ(initial_metrics.allocated_size_, 0);
    EXPECT_EQ(initial_metrics.allocated_num_, 0);
    EXPECT_EQ(initial_metrics.capacity, ALLOCATOR_SIZE);
    EXPECT_GT(initial_metrics.total_free_space_, 0);
    EXPECT_GT(initial_metrics.largest_free_region_, 0);
    EXPECT_EQ(initial_metrics.total_free_space_, ALLOCATOR_SIZE);

    // Allocate some memory and verify metrics update
    constexpr size_t ALLOC_SIZE_1 = 1024;
    auto handle1 = allocator->allocate(ALLOC_SIZE_1);
    ASSERT_TRUE(handle1.has_value());

    OffsetAllocatorMetrics after_first_alloc = allocator->getMetrics();
    EXPECT_EQ(after_first_alloc.allocated_size_, ALLOC_SIZE_1);
    EXPECT_EQ(after_first_alloc.allocated_num_, 1);
    EXPECT_EQ(after_first_alloc.capacity, ALLOCATOR_SIZE);
    EXPECT_LT(after_first_alloc.total_free_space_,
              initial_metrics.total_free_space_);
    EXPECT_EQ(after_first_alloc.total_free_space_,
              ALLOCATOR_SIZE - ALLOC_SIZE_1);

    // Allocate more memory and verify metrics continue to update
    constexpr size_t ALLOC_SIZE_2 = 2048;
    auto handle2 = allocator->allocate(ALLOC_SIZE_2);
    ASSERT_TRUE(handle2.has_value());

    OffsetAllocatorMetrics after_second_alloc = allocator->getMetrics();
    EXPECT_EQ(after_second_alloc.allocated_size_, ALLOC_SIZE_1 + ALLOC_SIZE_2);
    EXPECT_EQ(after_second_alloc.allocated_num_, 2);
    EXPECT_EQ(after_second_alloc.capacity, ALLOCATOR_SIZE);
    EXPECT_LT(after_second_alloc.total_free_space_,
              after_first_alloc.total_free_space_);
    EXPECT_EQ(after_second_alloc.total_free_space_,
              ALLOCATOR_SIZE - ALLOC_SIZE_1 - ALLOC_SIZE_2);

    // Free first allocation and verify metrics reflect the change
    handle1.reset();

    OffsetAllocatorMetrics after_first_free = allocator->getMetrics();
    EXPECT_EQ(after_first_free.allocated_size_, ALLOC_SIZE_2);
    EXPECT_EQ(after_first_free.allocated_num_, 1);
    EXPECT_EQ(after_first_free.capacity, ALLOCATOR_SIZE);
    EXPECT_GT(after_first_free.total_free_space_,
              after_second_alloc.total_free_space_);
    EXPECT_EQ(after_first_free.total_free_space_,
              ALLOCATOR_SIZE - ALLOC_SIZE_2);

    // Free remaining allocation and verify metrics return to initial state
    handle2.reset();

    OffsetAllocatorMetrics after_all_free = allocator->getMetrics();
    EXPECT_EQ(after_all_free.allocated_size_, 0);
    EXPECT_EQ(after_all_free.allocated_num_, 0);
    EXPECT_EQ(after_all_free.capacity, ALLOCATOR_SIZE);
    EXPECT_EQ(after_all_free.total_free_space_, ALLOCATOR_SIZE);
    EXPECT_EQ(after_all_free.largest_free_region_, ALLOCATOR_SIZE);
}

// ========== Serialization TESTS ==========

TEST_F(OffsetAllocatorTest, SerializationEmptyAllocator) {
    // Create an empty allocator
    const uint64_t base = 1024 * 16;
    const size_t size = 1024 * 1024;
    const uint32_t init_capacity = 1000;
    const uint32_t max_capacity = 10000;
    std::shared_ptr<OffsetAllocator> alloc_a =
        OffsetAllocator::create(base, size, init_capacity, max_capacity);
    // test
    testSerializeAllocator(alloc_a, {});
}

TEST_F(OffsetAllocatorTest, SerializationOneElementAllocator) {
    // Create an empty allocator
    const uint64_t base = 1024 * 16;
    const size_t size = 1024 * 1024;
    const uint32_t init_capacity = 1;
    const uint32_t max_capacity = 10000;
    std::shared_ptr<OffsetAllocator> alloc_a =
        OffsetAllocator::create(base, size, init_capacity, max_capacity);
    // Allocate one element
    auto handle = alloc_a->allocate(1024);
    ASSERT_TRUE(handle.has_value());
    // test
    std::vector<OffsetAllocationHandle> handles;
    handles.push_back(std::move(*handle));
    testSerializeAllocator(alloc_a, handles);
}

TEST_F(OffsetAllocatorTest, SerializationRandomAllocatedAllocator) {
    // The size multiplier is larger than 1 when the allocator size is larger
    // than MAX_BIN_SIZE.
    constexpr size_t MIN_BUFFER_SIZE = (1ull << 31) + 1;
    constexpr size_t MAX_BUFFER_SIZE = (1ull << 40);
    constexpr uint32 MAX_ALLOCS = 10000;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> buffer_size_dist(MIN_BUFFER_SIZE,
                                                           MAX_BUFFER_SIZE);
    for (int i = 0; i < 100; i++) {
        size_t buffer_size = buffer_size_dist(gen);
        auto alloc_a = OffsetAllocator::create(0, buffer_size, 1, MAX_ALLOCS);
        size_t max_alloc_size = buffer_size / 100;

        std::vector<OffsetAllocationHandle> handles;
        std::vector<size_t> alloc_sizes;
        std::uniform_int_distribution<size_t> size_dist(1, max_alloc_size);
        for (int j = 0; j < 200; ++j) {
            size_t size = size_dist(gen);
            auto handle = alloc_a->allocate(size);
            if (handle.has_value()) {
                handles.push_back(std::move(*handle));
                alloc_sizes.push_back(size);
            }

            std::uniform_int_distribution<uint32> index_dist(
                0, handles.size() - 1);
            uint32 index = index_dist(gen);
            std::swap(handles[index], handles.back());
            std::swap(alloc_sizes[index], alloc_sizes.back());
            uint32 test_size = alloc_sizes.back();
            handles.pop_back();
            alloc_sizes.pop_back();

            auto handle2 = alloc_a->allocate(test_size);
            ASSERT_TRUE(handle2.has_value());
            handles.push_back(std::move(*handle2));
            alloc_sizes.push_back(test_size);
        }
        // test
        testSerializeAllocator(alloc_a, handles);
    }
}

TEST_F(OffsetAllocatorTest, AllocateAfterDeserialization) {
    // Create an empty allocator
    const uint64_t base = 1024 * 16;
    const size_t size = 1024 * 1024;
    const uint32_t init_capacity = 10;
    const uint32_t max_capacity = 10000;
    std::shared_ptr<AllocatorWrapper> allocator =
        AllocatorWrapper::create(base, size, init_capacity, max_capacity);

    std::random_device rd;
    std::mt19937 gen(rd());

    // Do a serias of allocations and deallocations.
    std::vector<AllocationHandleWrapper> handles;
    for (int i = 0; i < 100; i++) {
        std::uniform_int_distribution<size_t> size_dist(1, 1024);
        size_t size = size_dist(gen);
        auto handle = allocator->allocate(size);
        ASSERT_TRUE(handle.has_value());
        handles.push_back(std::move(*handle));

        // Free a random handle for 50% probability
        std::uniform_int_distribution<size_t> free_dist(0, 1);
        if (free_dist(gen) == 1 && !handles.empty()) {
            std::uniform_int_distribution<size_t> index_dist(
                0, handles.size() - 1);
            size_t random_index = index_dist(gen);
            std::swap(handles[random_index], handles.back());
            handles.pop_back();
        }
    }

    // Serialize the allocator
    std::vector<SerializedByte> buffer;
    ASSERT_EQ(serialize_to(allocator->getAllocator(), buffer), ErrorCode::OK);

    // Deserialize the allocator
    std::shared_ptr<OffsetAllocator> alloc_b =
        deserialize_from<OffsetAllocator>(buffer);
    ASSERT_NE(alloc_b, nullptr);

    // Substitute the allocator with the deserialized one.
    allocator->substituteAllocator(alloc_b);
    for (auto& handle : handles) {
        substituteWithNewAllocator(handle, alloc_b);
    }

    // Continue to do a serias of allocations and deallocations.
    for (int i = 0; i < 100; i++) {
        std::uniform_int_distribution<size_t> size_dist(1, 1024);
        size_t size = size_dist(gen);
        auto handle = allocator->allocate(size);
        ASSERT_TRUE(handle.has_value());
        handles.push_back(std::move(*handle));

        // Free a random handle for 50% probability
        std::uniform_int_distribution<size_t> free_dist(0, 1);
        if (free_dist(gen) == 1 && !handles.empty()) {
            std::uniform_int_distribution<size_t> index_dist(
                0, handles.size() - 1);
            size_t random_index = index_dist(gen);
            std::swap(handles[random_index], handles.back());
            handles.pop_back();
        }
    }
}

TEST_F(OffsetAllocatorTest, ChainedAllocationAndDeserialization) {
    // The size multiplier is larger than 1 when the allocator size is larger
    // than MAX_BIN_SIZE.
    constexpr size_t MIN_BUFFER_SIZE = (1ull << 31) + 1;
    constexpr size_t MAX_BUFFER_SIZE = (1ull << 40);
    constexpr uint32 INIT_CAPACITY = 1;
    constexpr uint32 MAX_ALLOCS = 10000;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> buffer_size_dist(MIN_BUFFER_SIZE,
                                                           MAX_BUFFER_SIZE);
    // Test 10 times.
    for (int i = 0; i < 10; i++) {
        size_t buffer_size = buffer_size_dist(gen);
        auto allocator =
            AllocatorWrapper::create(0, buffer_size, INIT_CAPACITY, MAX_ALLOCS);
        size_t max_alloc_size = buffer_size / 100;

        std::vector<AllocationHandleWrapper> handles;
        std::vector<size_t> alloc_sizes;
        std::uniform_int_distribution<size_t> size_dist(1, max_alloc_size);
        // 10 times serilization and deserialization.
        for (int j = 0; j < 10; j++) {
            // 100 times allocation
            for (int k = 0; k < 100; k++) {
                size_t size = size_dist(gen);
                auto handle = allocator->allocate(size);
                if (handle.has_value()) {
                    handles.push_back(std::move(*handle));
                    alloc_sizes.push_back(size);
                }

                std::uniform_int_distribution<uint32> index_dist(
                    0, handles.size() - 1);
                uint32 index = index_dist(gen);
                std::swap(handles[index], handles.back());
                std::swap(alloc_sizes[index], alloc_sizes.back());
                uint32 test_size = alloc_sizes.back();
                handles.pop_back();
                alloc_sizes.pop_back();

                auto handle2 = allocator->allocate(test_size);
                ASSERT_TRUE(handle2.has_value());
                handles.push_back(std::move(*handle2));
                alloc_sizes.push_back(test_size);
            }
        }
        // Serialize the allocator
        std::vector<SerializedByte> buffer;
        ASSERT_EQ(serialize_to(allocator->getAllocator(), buffer),
                  ErrorCode::OK);

        // Deserialize the allocator
        std::shared_ptr<OffsetAllocator> new_alloc =
            deserialize_from<OffsetAllocator>(buffer);
        ASSERT_NE(new_alloc, nullptr);

        // Verify the allocator is equal to the original one.
        assertAllocatorEQ(allocator->getAllocator(), new_alloc);

        // Substitute the allocator with the deserialized one.
        allocator->substituteAllocator(new_alloc);
        // Substitute the handles with the deserialized allocator.
        for (auto& handle : handles) {
            substituteWithNewAllocator(handle, new_alloc);
        }
    }
}

}  // namespace mooncake::offset_allocator

int main(int argc, char** argv) {
    // Initialize Google Test
    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
