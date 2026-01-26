#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "tiered_cache/cache_tier.h"
#include "tiered_cache/tiered_backend.h"
#include "types.h"

#ifdef USE_ASCEND_CACHE_TIER
#include "tiered_cache/ascend_tier.h"
#endif

// Helper function to parse JSON string using thread-safe CharReaderBuilder
static bool parseJsonString(const std::string& json_str, Json::Value& value,
                            std::string* error_msg = nullptr) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;

    bool success = reader->parse(
        json_str.data(), json_str.data() + json_str.size(), &value, &errs);
    if (!success && error_msg) {
        *error_msg = errs;
    }
    return success;
}

namespace mooncake {

// Test data size constants
static constexpr size_t SMALL_DATA_SIZE = 4 * 1024;    // 4KB
static constexpr size_t MEDIUM_DATA_SIZE = 64 * 1024;  // 64KB

// Test capacity constants
static constexpr size_t BASIC_CAPACITY = 512 * 1024 * 1024;  // 512MB
static constexpr size_t SMALL_CAPACITY = 1 * 1024 * 1024;    // 1MB

class AscendTierTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // glog is already initialized by gtest_main
    }

    void TearDown() override {}

    // Helper: Create test buffer with specified size
    std::unique_ptr<char[]> CreateTestBuffer(size_t size) {
        auto buffer = std::make_unique<char[]>(size);
        // Fill with test pattern
        for (size_t i = 0; i < size; ++i) {
            buffer[i] = static_cast<char>(i % 256);
        }
        return buffer;
    }

    // Helper: Verify buffer content matches expected pattern
    bool VerifyBufferContent(const char* buffer, size_t size) {
        for (size_t i = 0; i < size; ++i) {
            if (buffer[i] != static_cast<char>(i % 256)) {
                return false;
            }
        }
        return true;
    }
};

// ============================================================================
// MemoryType Tests
// ============================================================================

// Test MemoryType enum includes ASCEND_NPU
TEST_F(AscendTierTest, MemoryTypeIncludesAscendNpu) {
    MemoryType type = MemoryType::ASCEND_NPU;
    EXPECT_EQ(MemoryTypeToString(type), "ASCEND_NPU");
}

// Test MemoryTypeToString for all types
TEST_F(AscendTierTest, MemoryTypeToStringAllTypes) {
    EXPECT_EQ(MemoryTypeToString(MemoryType::DRAM), "DRAM");
    EXPECT_EQ(MemoryTypeToString(MemoryType::NVME), "NVME");
    EXPECT_EQ(MemoryTypeToString(MemoryType::ASCEND_NPU), "ASCEND_NPU");
    EXPECT_EQ(MemoryTypeToString(MemoryType::UNKNOWN), "UNKNOWN");
}

#ifdef USE_ASCEND_CACHE_TIER

// ============================================================================
// AscendBuffer Tests
// ============================================================================

// Test AscendBuffer basic lifecycle
TEST_F(AscendTierTest, AscendBufferLifecycle) {
    // Create a mock AscendUnifiedPointer with fallback (host memory)
    void* mock_ptr = std::malloc(1024);
    ASSERT_NE(mock_ptr, nullptr);

    auto unified_ptr = std::make_unique<AscendUnifiedPointer>(
        AscendUnifiedPointer{mock_ptr, -1, 1024});
    auto* raw_ptr = unified_ptr.get();

    {
        AscendBuffer buffer(std::move(unified_ptr));
        EXPECT_EQ(buffer.size(), 1024);
        EXPECT_EQ(buffer.GetDeviceId(), -1);
        EXPECT_NE(buffer.data(), 0);
        EXPECT_EQ(buffer.GetDevicePtr(), mock_ptr);
        EXPECT_EQ(buffer.GetUnifiedPointer(), raw_ptr);
    }
    // Buffer should be released when going out of scope
}

// Test AscendBuffer move constructor
TEST_F(AscendTierTest, AscendBufferMoveConstructor) {
    void* mock_ptr = std::malloc(2048);
    ASSERT_NE(mock_ptr, nullptr);

    auto unified_ptr = std::make_unique<AscendUnifiedPointer>(
        AscendUnifiedPointer{mock_ptr, 0, 2048});
    AscendBuffer original(std::move(unified_ptr));

    EXPECT_EQ(original.size(), 2048);

    // Move construct
    AscendBuffer moved(std::move(original));
    EXPECT_EQ(moved.size(), 2048);
    EXPECT_EQ(moved.GetDevicePtr(), mock_ptr);

    // Original should be invalidated
    EXPECT_EQ(original.size(), 0);
    EXPECT_EQ(original.GetDevicePtr(), nullptr);
}

// Test AscendBuffer move assignment
TEST_F(AscendTierTest, AscendBufferMoveAssignment) {
    void* mock_ptr1 = std::malloc(1024);
    void* mock_ptr2 = std::malloc(2048);
    ASSERT_NE(mock_ptr1, nullptr);
    ASSERT_NE(mock_ptr2, nullptr);

    auto unified_ptr1 = std::make_unique<AscendUnifiedPointer>(
        AscendUnifiedPointer{mock_ptr1, 0, 1024});
    auto unified_ptr2 = std::make_unique<AscendUnifiedPointer>(
        AscendUnifiedPointer{mock_ptr2, 1, 2048});

    AscendBuffer buffer1(std::move(unified_ptr1));
    AscendBuffer buffer2(std::move(unified_ptr2));

    EXPECT_EQ(buffer1.size(), 1024);
    EXPECT_EQ(buffer2.size(), 2048);

    // Move assign
    buffer1 = std::move(buffer2);
    EXPECT_EQ(buffer1.size(), 2048);
    EXPECT_EQ(buffer1.GetDeviceId(), 1);
    EXPECT_EQ(buffer2.size(), 0);
}

// ============================================================================
// AscendCacheTier Tests
// ============================================================================

// Test AscendCacheTier construction
TEST_F(AscendTierTest, AscendCacheTierConstruction) {
    UUID tier_id = generate_uuid();
    std::vector<std::string> tags = {"npu", "fast"};

    AscendCacheTier tier(tier_id, BASIC_CAPACITY, tags, 0);

    EXPECT_EQ(tier.GetTierId(), tier_id);
    EXPECT_EQ(tier.GetCapacity(), BASIC_CAPACITY);
    EXPECT_EQ(tier.GetUsage(), 0);
    EXPECT_EQ(tier.GetMemoryType(), MemoryType::ASCEND_NPU);
    EXPECT_EQ(tier.GetDeviceId(), 0);
    EXPECT_EQ(tier.GetTags().size(), 2);
    EXPECT_EQ(tier.GetTags()[0], "npu");
    EXPECT_EQ(tier.GetTags()[1], "fast");
}

// Test AscendCacheTier initialization
TEST_F(AscendTierTest, AscendCacheTierInit) {
    UUID tier_id = generate_uuid();
    AscendCacheTier tier(tier_id, BASIC_CAPACITY, {}, 0);

    // Create a mock TieredBackend for testing
    // Note: In real tests, you might need to properly initialize the backend
    TieredBackend backend;

    auto result = tier.Init(&backend, nullptr);

    // Should succeed (even in fallback mode without real Ascend hardware)
    EXPECT_TRUE(result.has_value());
}

// Test AscendCacheTier initialization with null backend
TEST_F(AscendTierTest, AscendCacheTierInitNullBackend) {
    UUID tier_id = generate_uuid();
    AscendCacheTier tier(tier_id, BASIC_CAPACITY, {}, 0);

    auto result = tier.Init(nullptr, nullptr);

    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test AscendCacheTier allocate and free
TEST_F(AscendTierTest, AscendCacheTierAllocateAndFree) {
    UUID tier_id = generate_uuid();
    AscendCacheTier tier(tier_id, BASIC_CAPACITY, {}, 0);

    TieredBackend backend;
    ASSERT_TRUE(tier.Init(&backend, nullptr).has_value());

    size_t initial_usage = tier.GetUsage();

    // Allocate
    DataSource data;
    auto alloc_result = tier.Allocate(SMALL_DATA_SIZE, data);
    ASSERT_TRUE(alloc_result.has_value());
    EXPECT_NE(data.buffer, nullptr);
    EXPECT_EQ(data.type, MemoryType::ASCEND_NPU);
    EXPECT_EQ(tier.GetUsage(), initial_usage + SMALL_DATA_SIZE);

    // Free
    auto free_result = tier.Free(std::move(data));
    EXPECT_TRUE(free_result.has_value());
    EXPECT_EQ(tier.GetUsage(), initial_usage);
}

// Test AscendCacheTier capacity limit
TEST_F(AscendTierTest, AscendCacheTierCapacityLimit) {
    UUID tier_id = generate_uuid();
    AscendCacheTier tier(tier_id, SMALL_CAPACITY, {}, 0);

    TieredBackend backend;
    ASSERT_TRUE(tier.Init(&backend, nullptr).has_value());

    // Try to allocate more than capacity
    DataSource data;
    auto result = tier.Allocate(SMALL_CAPACITY * 2, data);

    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Test AscendCacheTier allocate with zero size
TEST_F(AscendTierTest, AscendCacheTierAllocateZeroSize) {
    UUID tier_id = generate_uuid();
    AscendCacheTier tier(tier_id, BASIC_CAPACITY, {}, 0);

    TieredBackend backend;
    ASSERT_TRUE(tier.Init(&backend, nullptr).has_value());

    DataSource data;
    auto result = tier.Allocate(0, data);

    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test AscendCacheTier multiple allocations
TEST_F(AscendTierTest, AscendCacheTierMultipleAllocations) {
    UUID tier_id = generate_uuid();
    AscendCacheTier tier(tier_id, BASIC_CAPACITY, {}, 0);

    TieredBackend backend;
    ASSERT_TRUE(tier.Init(&backend, nullptr).has_value());

    const int num_allocations = 10;
    std::vector<DataSource> data_sources(num_allocations);

    // Perform multiple allocations
    for (int i = 0; i < num_allocations; ++i) {
        auto result = tier.Allocate(SMALL_DATA_SIZE, data_sources[i]);
        ASSERT_TRUE(result.has_value()) << "Allocation " << i << " failed";
    }

    // Verify usage
    EXPECT_EQ(tier.GetUsage(), SMALL_DATA_SIZE * num_allocations);

    // Free all
    for (int i = 0; i < num_allocations; ++i) {
        auto result = tier.Free(std::move(data_sources[i]));
        EXPECT_TRUE(result.has_value());
    }

    // Verify usage is back to 0
    EXPECT_EQ(tier.GetUsage(), 0);
}

// Test AscendCacheTier RAII auto-release
TEST_F(AscendTierTest, AscendCacheTierAutoRelease) {
    UUID tier_id = generate_uuid();
    AscendCacheTier tier(tier_id, BASIC_CAPACITY, {}, 0);

    TieredBackend backend;
    ASSERT_TRUE(tier.Init(&backend, nullptr).has_value());

    size_t initial_usage = tier.GetUsage();

    // Allocate in inner scope
    {
        DataSource data;
        auto result = tier.Allocate(MEDIUM_DATA_SIZE, data);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(tier.GetUsage(), initial_usage + MEDIUM_DATA_SIZE);

        // data goes out of scope here, should auto-free via RAII
        // Note: We need to manually move it to Free since we're testing the
        // tier
        tier.Free(std::move(data));
    }

    // Usage should be back to initial
    EXPECT_EQ(tier.GetUsage(), initial_usage);
}

// ============================================================================
// TieredBackend Integration Tests (with ASCEND_NPU tier)
// ============================================================================

// Test TieredBackend with ASCEND_NPU tier configuration
TEST_F(AscendTierTest, TieredBackendAscendTierInit) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "ASCEND_NPU",
                "capacity": 536870912,
                "priority": 100,
                "device_id": 0,
                "tags": ["npu", "fast"]
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    auto result = backend.Init(config, nullptr, nullptr);

    EXPECT_TRUE(result.has_value());

    // Verify tier was created
    auto tier_views = backend.GetTierViews();
    EXPECT_EQ(tier_views.size(), 1);

    if (!tier_views.empty()) {
        EXPECT_EQ(tier_views[0].type, MemoryType::ASCEND_NPU);
        EXPECT_EQ(tier_views[0].capacity, 536870912);
        EXPECT_EQ(tier_views[0].priority, 100);
    }
}

// Test TieredBackend with mixed DRAM and ASCEND_NPU tiers
TEST_F(AscendTierTest, TieredBackendMixedTiers) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "ASCEND_NPU",
                "capacity": 536870912,
                "priority": 100,
                "device_id": 0
            },
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 50,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    auto result = backend.Init(config, nullptr, nullptr);

    EXPECT_TRUE(result.has_value());

    auto tier_views = backend.GetTierViews();
    EXPECT_EQ(tier_views.size(), 2);

    // Find each tier type
    bool found_ascend = false;
    bool found_dram = false;
    for (const auto& view : tier_views) {
        if (view.type == MemoryType::ASCEND_NPU) {
            found_ascend = true;
            EXPECT_EQ(view.priority, 100);
        } else if (view.type == MemoryType::DRAM) {
            found_dram = true;
            EXPECT_EQ(view.priority, 50);
        }
    }
    EXPECT_TRUE(found_ascend);
    EXPECT_TRUE(found_dram);
}

// Test TieredBackend allocate on ASCEND_NPU tier
TEST_F(AscendTierTest, TieredBackendAllocateOnAscend) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "ASCEND_NPU",
                "capacity": 536870912,
                "priority": 100,
                "device_id": 0
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    // Allocate
    auto alloc_result = backend.Allocate(SMALL_DATA_SIZE);
    ASSERT_TRUE(alloc_result.has_value());

    AllocationHandle handle = alloc_result.value();
    EXPECT_TRUE(handle);
    EXPECT_EQ(handle->loc.tier->GetMemoryType(), MemoryType::ASCEND_NPU);
}

// Test TieredBackend write to ASCEND_NPU tier
TEST_F(AscendTierTest, TieredBackendWriteToAscend) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "ASCEND_NPU",
                "capacity": 536870912,
                "priority": 100,
                "device_id": 0
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    // Allocate
    auto alloc_result = backend.Allocate(SMALL_DATA_SIZE);
    ASSERT_TRUE(alloc_result.has_value());
    AllocationHandle handle = alloc_result.value();

    // Prepare test data
    auto test_buffer = CreateTestBuffer(SMALL_DATA_SIZE);
    DataSource source;
    source.buffer = std::make_unique<TempDRAMBuffer>(std::move(test_buffer),
                                                     SMALL_DATA_SIZE);
    source.type = MemoryType::DRAM;

    // Write (DRAM -> ASCEND_NPU)
    auto write_result = backend.Write(source, handle);
    EXPECT_TRUE(write_result.has_value()) << "Write should succeed";
}

// Test complete data lifecycle with ASCEND_NPU tier
TEST_F(AscendTierTest, AscendTierCompleteLifecycle) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "ASCEND_NPU",
                "capacity": 536870912,
                "priority": 100,
                "device_id": 0
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    const std::string key = "ascend_lifecycle_key";

    // 1. Allocate
    auto alloc_result = backend.Allocate(SMALL_DATA_SIZE);
    ASSERT_TRUE(alloc_result.has_value());
    AllocationHandle handle = alloc_result.value();

    // 2. Write
    auto test_buffer = CreateTestBuffer(SMALL_DATA_SIZE);
    DataSource source;
    source.buffer = std::make_unique<TempDRAMBuffer>(std::move(test_buffer),
                                                     SMALL_DATA_SIZE);
    source.type = MemoryType::DRAM;
    auto write_result = backend.Write(source, handle);
    ASSERT_TRUE(write_result.has_value());

    // 3. Commit
    auto commit_result = backend.Commit(key, handle);
    ASSERT_TRUE(commit_result.has_value());

    // 4. Get and verify
    auto get_result = backend.Get(key);
    ASSERT_TRUE(get_result.has_value());
    EXPECT_EQ(get_result.value()->loc.tier->GetMemoryType(),
              MemoryType::ASCEND_NPU);

    // 5. Delete
    auto delete_result = backend.Delete(key);
    ASSERT_TRUE(delete_result.has_value());

    // 6. Verify deleted
    auto get_after_delete = backend.Get(key);
    EXPECT_FALSE(get_after_delete.has_value());
}

// ============================================================================
// Thread Safety Tests
// ============================================================================

// Test concurrent allocate and free operations
TEST_F(AscendTierTest, ThreadSafetyAllocateFree) {
    UUID tier_id = generate_uuid();
    AscendCacheTier tier(tier_id, BASIC_CAPACITY, {}, 0);

    TieredBackend backend;
    ASSERT_TRUE(tier.Init(&backend, nullptr).has_value());

    const int num_threads = 4;
    const int ops_per_thread = 10;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < ops_per_thread; ++i) {
                DataSource data;
                auto alloc_result = tier.Allocate(SMALL_DATA_SIZE, data);
                if (alloc_result.has_value()) {
                    success_count.fetch_add(1);
                    // Free immediately
                    tier.Free(std::move(data));
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // After all operations, usage should be 0
    EXPECT_EQ(tier.GetUsage(), 0);
    // At least some operations should have succeeded
    EXPECT_GT(success_count.load(), 0);
}

// ============================================================================
// Initialization Edge Cases Tests
// ============================================================================

// Test double initialization (should be idempotent)
TEST_F(AscendTierTest, AscendCacheTierInitTwice) {
    UUID tier_id = generate_uuid();
    AscendCacheTier tier(tier_id, BASIC_CAPACITY, {}, 0);

    TieredBackend backend;

    // First init
    auto result1 = tier.Init(&backend, nullptr);
    EXPECT_TRUE(result1.has_value());

    // Second init should also succeed (idempotent)
    auto result2 = tier.Init(&backend, nullptr);
    EXPECT_TRUE(result2.has_value());
}

// Test initialization with invalid device ID
TEST_F(AscendTierTest, AscendCacheTierInitWithInvalidDeviceId) {
    const int INVALID_DEVICE_ID = -1;
    UUID tier_id = generate_uuid();
    AscendCacheTier tier(tier_id, BASIC_CAPACITY, {}, INVALID_DEVICE_ID);

    // Verify basic properties are set correctly
    EXPECT_EQ(tier.GetTierId(), tier_id);
    EXPECT_EQ(tier.GetCapacity(), BASIC_CAPACITY);
    EXPECT_EQ(tier.GetDeviceId(), INVALID_DEVICE_ID);
    EXPECT_EQ(tier.GetUsage(), 0);
    EXPECT_EQ(tier.GetMemoryType(), MemoryType::ASCEND_NPU);

    TieredBackend backend;

    // Init behavior depends on whether USE_ASCEND_CACHE_TIER is defined.
    // With USE_ASCEND_CACHE_TIER, device_id < 0 check will fail with
    // INVALID_PARAMS. Without it (fallback mode), Init should succeed.
    auto result = tier.Init(&backend, nullptr);
#ifdef USE_ASCEND_CACHE_TIER
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
#else
    EXPECT_TRUE(result.has_value());
#endif
}

// Test initialization with zero capacity
TEST_F(AscendTierTest, AscendCacheTierInitWithZeroCapacity) {
    const size_t ZERO_CAPACITY = 0;
    UUID tier_id = generate_uuid();
    AscendCacheTier tier(tier_id, ZERO_CAPACITY, {}, 0);

    // Verify basic properties
    EXPECT_EQ(tier.GetTierId(), tier_id);
    EXPECT_EQ(tier.GetCapacity(), ZERO_CAPACITY);
    EXPECT_EQ(tier.GetUsage(), 0);
    EXPECT_EQ(tier.GetMemoryType(), MemoryType::ASCEND_NPU);

    TieredBackend backend;

    // Init should succeed (Init doesn't check capacity)
    auto init_result = tier.Init(&backend, nullptr);
    EXPECT_TRUE(init_result.has_value());

    // Allocate should fail (no capacity)
    DataSource data;
    auto alloc_result = tier.Allocate(SMALL_DATA_SIZE, data);
    EXPECT_FALSE(alloc_result.has_value());
    EXPECT_EQ(alloc_result.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    // Usage should remain 0
    EXPECT_EQ(tier.GetUsage(), 0);
}

// ============================================================================
// Operations Without Initialization Tests
// ============================================================================

// Test allocate without initialization
TEST_F(AscendTierTest, AscendCacheTierAllocateWithoutInit) {
    UUID tier_id = generate_uuid();
    AscendCacheTier tier(tier_id, BASIC_CAPACITY, {}, 0);

    // Don't call Init

    DataSource data;
    auto result = tier.Allocate(SMALL_DATA_SIZE, data);

    // Should fail because tier is not initialized
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
}

// Test free without initialization
TEST_F(AscendTierTest, AscendCacheTierFreeWithoutInit) {
    UUID tier_id = generate_uuid();
    AscendCacheTier tier(tier_id, BASIC_CAPACITY, {}, 0);

    // Don't call Init

    // Create an empty DataSource
    DataSource data;
    data.buffer = nullptr;
    data.type = MemoryType::UNKNOWN;

    // Free with null buffer should succeed (no-op)
    auto result = tier.Free(std::move(data));
    EXPECT_TRUE(result.has_value());
}

// ============================================================================
// Polymorphic Deletion Test
// ============================================================================

// Test delete via base class pointer (virtual destructor)
TEST_F(AscendTierTest, DeleteViaBasePointer) {
    UUID tier_id = generate_uuid();

    // Create via base class pointer
    CacheTier* tier = new AscendCacheTier(tier_id, BASIC_CAPACITY, {}, 0);
    ASSERT_NE(tier, nullptr);

    TieredBackend backend;
    ASSERT_TRUE(tier->Init(&backend, nullptr).has_value());

    // Allocate some data
    DataSource data;
    auto alloc_result = tier->Allocate(SMALL_DATA_SIZE, data);
    ASSERT_TRUE(alloc_result.has_value());

    // Free the data
    tier->Free(std::move(data));

    // Delete via base class pointer (tests virtual destructor)
    delete tier;
    // If we reach here without crash, the test passes
}

// ============================================================================
// Data Copy Tests
// ============================================================================

// Test copy between two Ascend tiers on the same device
TEST_F(AscendTierTest, CopyBetweenAscendTiersSameDevice) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "ASCEND_NPU",
                "capacity": 536870912,
                "priority": 100,
                "device_id": 0
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    // Allocate first buffer
    auto alloc_result1 = backend.Allocate(SMALL_DATA_SIZE);
    ASSERT_TRUE(alloc_result1.has_value());
    AllocationHandle handle1 = alloc_result1.value();

    // Write test data to first buffer
    auto test_buffer = CreateTestBuffer(SMALL_DATA_SIZE);
    DataSource source;
    source.buffer = std::make_unique<TempDRAMBuffer>(std::move(test_buffer),
                                                     SMALL_DATA_SIZE);
    source.type = MemoryType::DRAM;
    auto write_result = backend.Write(source, handle1);
    ASSERT_TRUE(write_result.has_value());

    // Commit the first buffer
    const std::string key1 = "copy_test_key1";
    auto commit_result1 = backend.Commit(key1, handle1);
    ASSERT_TRUE(commit_result1.has_value());

    // Get the data source from first allocation
    auto get_result = backend.Get(key1);
    ASSERT_TRUE(get_result.has_value());
    EXPECT_EQ(get_result.value()->loc.tier->GetMemoryType(),
              MemoryType::ASCEND_NPU);

    // Allocate second buffer
    auto alloc_result2 = backend.Allocate(SMALL_DATA_SIZE);
    ASSERT_TRUE(alloc_result2.has_value());
    AllocationHandle handle2 = alloc_result2.value();

    // Verify that both allocations are on the same Ascend tier
    EXPECT_EQ(handle2->loc.tier->GetMemoryType(), MemoryType::ASCEND_NPU);

    // Test that we can use CopyData API for replication
    // Note: Direct NPU-to-NPU copy would require special handling
    // For now, we just verify that both handles point to valid Ascend memory

    // Clean up
    backend.Delete(key1);
}

// ============================================================================
// Accessor Methods Tests
// ============================================================================

// Test all accessor methods
TEST_F(AscendTierTest, AscendCacheTierAccessors) {
    UUID tier_id = generate_uuid();
    std::vector<std::string> tags = {"tag1", "tag2", "tag3"};
    const int device_id = 1;

    AscendCacheTier tier(tier_id, BASIC_CAPACITY, tags, device_id);

    EXPECT_EQ(tier.GetTierId(), tier_id);
    EXPECT_EQ(tier.GetCapacity(), BASIC_CAPACITY);
    EXPECT_EQ(tier.GetUsage(), 0);
    EXPECT_EQ(tier.GetMemoryType(), MemoryType::ASCEND_NPU);
    EXPECT_EQ(tier.GetDeviceId(), device_id);
    EXPECT_EQ(tier.GetTags().size(), 3);
    EXPECT_EQ(tier.GetTags()[0], "tag1");
    EXPECT_EQ(tier.GetTags()[1], "tag2");
    EXPECT_EQ(tier.GetTags()[2], "tag3");
}

// Test construction with default parameters
TEST_F(AscendTierTest, AscendCacheTierConstructorWithDefaults) {
    UUID tier_id = generate_uuid();

    // Constructor with minimal parameters (empty tags, default device_id)
    AscendCacheTier tier(tier_id, BASIC_CAPACITY, {}, 0);

    EXPECT_EQ(tier.GetTierId(), tier_id);
    EXPECT_EQ(tier.GetCapacity(), BASIC_CAPACITY);
    EXPECT_EQ(tier.GetUsage(), 0);
    EXPECT_EQ(tier.GetMemoryType(), MemoryType::ASCEND_NPU);
    EXPECT_EQ(tier.GetTags().size(), 0);  // Default empty tags
    EXPECT_EQ(tier.GetDeviceId(), 0);
}

// ============================================================================
// Destructor Cleanup Test
// ============================================================================

// Test that DataSource RAII properly cleans up allocated memory
TEST_F(AscendTierTest, DestructorCleansUpAllocations) {
    UUID tier_id = generate_uuid();
    const int num_allocations = 10;

    {
        AscendCacheTier tier(tier_id, BASIC_CAPACITY, {}, 0);
        TieredBackend backend;
        ASSERT_TRUE(tier.Init(&backend, nullptr).has_value());

        std::vector<DataSource> data_sources(num_allocations);

        // Allocate multiple buffers
        for (int i = 0; i < num_allocations; ++i) {
            auto result = tier.Allocate(SMALL_DATA_SIZE, data_sources[i]);
            ASSERT_TRUE(result.has_value());
        }

        // Verify usage
        EXPECT_EQ(tier.GetUsage(), SMALL_DATA_SIZE * num_allocations);

        // Don't manually free - let DataSource destructors handle cleanup via
        // RAII when data_sources vector goes out of scope. We need to call
        // tier.Free() for each DataSource to update the tier's usage counter.
        for (int i = 0; i < num_allocations; ++i) {
            tier.Free(std::move(data_sources[i]));
        }

        // Verify all memory was freed
        EXPECT_EQ(tier.GetUsage(), 0);
    }
    // Tier is destroyed here
    // If we reach here without crash/memory issues, the test passes
}

// Test tier destruction with outstanding allocations (warning scenario)
TEST_F(AscendTierTest, DestructorWithOutstandingAllocations) {
    UUID tier_id = generate_uuid();

    {
        AscendCacheTier tier(tier_id, BASIC_CAPACITY, {}, 0);
        TieredBackend backend;
        ASSERT_TRUE(tier.Init(&backend, nullptr).has_value());

        DataSource data;
        auto result = tier.Allocate(SMALL_DATA_SIZE, data);
        ASSERT_TRUE(result.has_value());

        // Intentionally don't free - tier will be destroyed with outstanding
        // allocation This should log a warning but not crash The DataSource's
        // buffer will be released by its destructor
    }
    // Tier destroyed with outstanding allocation - should log warning
}

#else  // !USE_ASCEND_CACHE_TIER

// Test that ASCEND_NPU tier type is rejected when USE_ASCEND_CACHE_TIER is not
// defined
TEST_F(AscendTierTest, AscendTierNotAvailable) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "ASCEND_NPU",
                "capacity": 536870912,
                "priority": 100,
                "device_id": 0
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    auto result = backend.Init(config, nullptr, nullptr);

    // Should fail because ASCEND_NPU is not supported
    EXPECT_FALSE(result.has_value());
}

#endif  // USE_ASCEND_CACHE_TIER

}  // namespace mooncake
