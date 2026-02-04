#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>
#include <fstream>
#include <thread>
#include <future>
#include <chrono>
#include <iomanip>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <functional>
#include <cstring>

#include "tiered_cache/tiered_backend.h"
#include "utils.h"

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
static constexpr size_t SMALL_DATA_SIZE = 4 * 1024;          // 4KB
static constexpr size_t MEDIUM_DATA_SIZE = 256 * 1024;       // 256KB
static constexpr size_t LARGE_DATA_SIZE = 10 * 1024 * 1024;  // 10MB

// Test capacity constants
static constexpr size_t BASIC_CAPACITY = 1024 * 1024 * 1024;         // 1GB
static constexpr size_t HIGH_PRIORITY_CAPACITY = 512 * 1024 * 1024;  // 512MB
static constexpr size_t LOW_PRIORITY_CAPACITY = 1024 * 1024 * 1024;  // 1GB
static constexpr size_t SMALL_CAPACITY = 1 * 1024 * 1024;            // 1MB

class TieredBackendTest : public ::testing::Test {
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

    // Helper: Get tier ID by priority from tier views
    std::optional<UUID> GetTierIdByPriority(const TieredBackend& backend,
                                            int priority) {
        auto tier_views = backend.GetTierViews();
        for (const auto& view : tier_views) {
            if (view.priority == priority) {
                return view.id;
            }
        }
        return std::nullopt;
    }

    // Helper: Verify tier usage
    void VerifyTierUsage(const TieredBackend& backend, UUID tier_id,
                         size_t expected_usage) {
        auto tier_views = backend.GetTierViews();
        for (const auto& view : tier_views) {
            if (view.id == tier_id) {
                EXPECT_EQ(view.usage, expected_usage);
                return;
            }
        }
        FAIL() << "Tier not found with id: " << tier_id.first << "-"
               << tier_id.second;
    }

    // Helper: Allocate and write (combined operation)
    tl::expected<AllocationHandle, ErrorCode> AllocateAndWrite(
        TieredBackend& backend, size_t size, const char* data,
        std::optional<UUID> preferred_tier = std::nullopt) {
        auto alloc_result = backend.Allocate(size, preferred_tier);
        if (!alloc_result.has_value()) {
            return alloc_result;
        }

        AllocationHandle handle = alloc_result.value();

        // Prepare DataSource for write
        DataSource source;
        auto buffer = std::make_unique<char[]>(size);
        std::memcpy(buffer.get(), data, size);
        source.buffer =
            std::make_unique<TempDRAMBuffer>(std::move(buffer), size);
        source.type = MemoryType::DRAM;

        auto write_result = backend.Write(source, handle);
        if (!write_result.has_value()) {
            return tl::make_unexpected(write_result.error());
        }

        return handle;
    }
};

// Test basic DRAM tier initialization
TEST_F(TieredBackendTest, BasicDRAMTierInit) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "tags": ["fast", "local"],
                "allocator_type": "OFFSET"
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
        EXPECT_EQ(tier_views[0].type, MemoryType::DRAM);
        EXPECT_EQ(tier_views[0].capacity, 1073741824);
        EXPECT_EQ(tier_views[0].priority, 10);
        EXPECT_EQ(tier_views[0].tags.size(), 2);
    }
}

// Test DRAM tier with NUMA node
TEST_F(TieredBackendTest, DRAMTierWithNUMA) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 536870912,
                "priority": 20,
                "numa_node": 0,
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
    EXPECT_EQ(tier_views.size(), 1);
}

// Test multiple DRAM tiers
TEST_F(TieredBackendTest, MultipleDRAMTiers) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 536870912,
                "priority": 100,
                "allocator_type": "OFFSET"
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
}

// Test missing required fields
TEST_F(TieredBackendTest, MissingTypeField) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "capacity": 1073741824,
                "priority": 10
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    auto result = backend.Init(config, nullptr, nullptr);

    EXPECT_FALSE(result.has_value());
}

// Test missing capacity field
TEST_F(TieredBackendTest, MissingCapacityField) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "priority": 10
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    auto result = backend.Init(config, nullptr, nullptr);

    EXPECT_FALSE(result.has_value());
}

// Test invalid capacity (zero)
TEST_F(TieredBackendTest, InvalidCapacity) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 0,
                "priority": 10
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    auto result = backend.Init(config, nullptr, nullptr);

    EXPECT_FALSE(result.has_value());
}

// Test unsupported tier type
TEST_F(TieredBackendTest, UnsupportedTierType) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "NVME",
                "capacity": 1073741824,
                "priority": 10
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    auto result = backend.Init(config, nullptr, nullptr);

    EXPECT_FALSE(result.has_value());
}

// Test default allocator type
TEST_F(TieredBackendTest, DefaultAllocatorType) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    auto result = backend.Init(config, nullptr, nullptr);

    EXPECT_TRUE(result.has_value());

    auto tier_views = backend.GetTierViews();
    EXPECT_EQ(tier_views.size(), 1);
}

// Test unknown allocator type
TEST_F(TieredBackendTest, UnknownAllocatorType) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "UNKNOWN_TYPE"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    auto result = backend.Init(config, nullptr, nullptr);

    // Should succeed and use default OFFSET allocator
    EXPECT_TRUE(result.has_value());

    auto tier_views = backend.GetTierViews();
    EXPECT_EQ(tier_views.size(), 1);
}

// namespace mooncake

// ============================================================================
// Allocate API Tests
// ============================================================================

// Test basic allocation
TEST_F(TieredBackendTest, AllocateBasic) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    // Allocate space
    auto result = backend.Allocate(SMALL_DATA_SIZE);
    ASSERT_TRUE(result.has_value()) << "Allocation should succeed";

    AllocationHandle handle = result.value();
    EXPECT_TRUE(handle) << "Handle should be valid";

    // Verify tier usage increased
    auto tier_views = backend.GetTierViews();
    ASSERT_EQ(tier_views.size(), 1);
    EXPECT_GT(tier_views[0].usage, 0)
        << "Tier usage should increase after allocation";
}

// Test allocation failure due to insufficient space
TEST_F(TieredBackendTest, AllocateInsufficientSpace) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1048576,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    // Try to allocate more than capacity
    auto result = backend.Allocate(LARGE_DATA_SIZE);
    EXPECT_FALSE(result.has_value())
        << "Allocation should fail when space insufficient";
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Test allocation on specified tier
TEST_F(TieredBackendTest, AllocateOnSpecifiedTier) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 536870912,
                "priority": 100,
                "allocator_type": "OFFSET"
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
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    // Get low priority tier ID
    auto low_priority_tier_id = GetTierIdByPriority(backend, 50);
    ASSERT_TRUE(low_priority_tier_id.has_value());

    // Allocate on low priority tier
    auto result =
        backend.Allocate(SMALL_DATA_SIZE, low_priority_tier_id.value());
    ASSERT_TRUE(result.has_value());

    AllocationHandle handle = result.value();
    EXPECT_EQ(handle->loc.tier->GetTierId(), low_priority_tier_id.value());
}

// Test automatic resource release when handle goes out of scope
TEST_F(TieredBackendTest, AllocateAutoRelease) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    auto tier_views = backend.GetTierViews();
    ASSERT_EQ(tier_views.size(), 1);
    size_t initial_usage = tier_views[0].usage;

    // Allocate in inner scope
    {
        auto result = backend.Allocate(MEDIUM_DATA_SIZE);
        ASSERT_TRUE(result.has_value());

        // Verify usage increased
        tier_views = backend.GetTierViews();
        size_t usage_after_alloc = tier_views[0].usage;
        EXPECT_GT(usage_after_alloc, initial_usage);
    }
    // Handle goes out of scope, should auto-release

    // Verify usage returned to initial value
    tier_views = backend.GetTierViews();
    EXPECT_EQ(tier_views[0].usage, initial_usage)
        << "Usage should return to initial value after auto-release";
}

// ============================================================================
// Write API Tests
// ============================================================================

// Test basic write operation
TEST_F(TieredBackendTest, WriteBasic) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    // Allocate space
    auto alloc_result = backend.Allocate(SMALL_DATA_SIZE);
    ASSERT_TRUE(alloc_result.has_value());
    AllocationHandle handle = alloc_result.value();

    // Prepare test data
    auto test_buffer = CreateTestBuffer(SMALL_DATA_SIZE);
    DataSource source;
    source.buffer = std::make_unique<TempDRAMBuffer>(std::move(test_buffer),
                                                     SMALL_DATA_SIZE);
    source.type = MemoryType::DRAM;

    // Write data
    auto write_result = backend.Write(source, handle);
    EXPECT_TRUE(write_result.has_value()) << "Write should succeed";
}

// Test write with invalid handle
TEST_F(TieredBackendTest, WriteInvalidHandle) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    // Create invalid (null) handle
    AllocationHandle invalid_handle;

    // Prepare test data
    auto test_buffer = CreateTestBuffer(SMALL_DATA_SIZE);
    DataSource source;
    source.buffer = std::make_unique<TempDRAMBuffer>(std::move(test_buffer),
                                                     SMALL_DATA_SIZE);
    source.type = MemoryType::DRAM;

    // Try to write with invalid handle
    auto write_result = backend.Write(source, invalid_handle);
    EXPECT_FALSE(write_result.has_value())
        << "Write with invalid handle should fail";
    EXPECT_EQ(write_result.error(), ErrorCode::INVALID_PARAMS);
}

// ============================================================================
// Commit API Tests
// ============================================================================

// Test basic commit operation
TEST_F(TieredBackendTest, CommitBasic) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    // Allocate and write
    auto test_buffer = CreateTestBuffer(SMALL_DATA_SIZE);
    auto handle_result =
        AllocateAndWrite(backend, SMALL_DATA_SIZE, test_buffer.get());
    ASSERT_TRUE(handle_result.has_value());
    AllocationHandle handle = handle_result.value();

    // Commit
    auto commit_result = backend.Commit("test_key", handle);
    EXPECT_TRUE(commit_result.has_value()) << "Commit should succeed";

    // Verify we can get it back
    auto get_result = backend.Get("test_key");
    EXPECT_TRUE(get_result.has_value())
        << "Should be able to get committed data";
}

// Test commit replaces old data on same tier
TEST_F(TieredBackendTest, CommitReplace) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    // First commit
    auto test_buffer1 = CreateTestBuffer(SMALL_DATA_SIZE);
    auto handle1_result =
        AllocateAndWrite(backend, SMALL_DATA_SIZE, test_buffer1.get());
    ASSERT_TRUE(handle1_result.has_value());
    auto commit1_result = backend.Commit("key1", handle1_result.value());
    ASSERT_TRUE(commit1_result.has_value());

    UUID first_tier_id = handle1_result.value()->loc.tier->GetTierId();

    // Second commit with same key
    auto test_buffer2 = CreateTestBuffer(MEDIUM_DATA_SIZE);
    auto handle2_result =
        AllocateAndWrite(backend, MEDIUM_DATA_SIZE, test_buffer2.get());
    ASSERT_TRUE(handle2_result.has_value());
    auto commit2_result = backend.Commit("key1", handle2_result.value());
    ASSERT_TRUE(commit2_result.has_value());

    // Get should return the new handle
    auto get_result = backend.Get("key1");
    ASSERT_TRUE(get_result.has_value());
    EXPECT_EQ(get_result.value()->loc.tier->GetTierId(), first_tier_id);
}

// Test commit with invalid handle
TEST_F(TieredBackendTest, CommitInvalidHandle) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    // Create invalid handle
    AllocationHandle invalid_handle;

    // Try to commit with invalid handle
    auto commit_result = backend.Commit("test_key", invalid_handle);
    EXPECT_FALSE(commit_result.has_value())
        << "Commit with invalid handle should fail";
    EXPECT_EQ(commit_result.error(), ErrorCode::INVALID_PARAMS);
}

// ============================================================================
// Get API Tests
// ============================================================================

// Test basic get operation
TEST_F(TieredBackendTest, GetBasic) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    // Allocate, write and commit
    auto test_buffer = CreateTestBuffer(SMALL_DATA_SIZE);
    auto handle_result =
        AllocateAndWrite(backend, SMALL_DATA_SIZE, test_buffer.get());
    ASSERT_TRUE(handle_result.has_value());
    auto commit_result = backend.Commit("test_key", handle_result.value());
    ASSERT_TRUE(commit_result.has_value());

    UUID tier_id = handle_result.value()->loc.tier->GetTierId();

    // Get the data
    auto get_result = backend.Get("test_key");
    ASSERT_TRUE(get_result.has_value()) << "Get should succeed";
    EXPECT_EQ(get_result.value()->loc.tier->GetTierId(), tier_id);
}

// Test get with non-existent key
TEST_F(TieredBackendTest, GetNonExistentKey) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    // Try to get non-existent key
    auto get_result = backend.Get("non_existent_key");
    EXPECT_FALSE(get_result.has_value())
        << "Get should fail for non-existent key";
    EXPECT_EQ(get_result.error(), ErrorCode::INVALID_KEY);
}

// Test get from specified tier
TEST_F(TieredBackendTest, GetFromSpecifiedTier) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 536870912,
                "priority": 100,
                "allocator_type": "OFFSET"
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
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    auto high_tier_id = GetTierIdByPriority(backend, 100);
    auto low_tier_id = GetTierIdByPriority(backend, 50);
    ASSERT_TRUE(high_tier_id.has_value());
    ASSERT_TRUE(low_tier_id.has_value());

    // Commit to high priority tier
    auto test_buffer1 = CreateTestBuffer(SMALL_DATA_SIZE);
    auto handle1 = AllocateAndWrite(backend, SMALL_DATA_SIZE,
                                    test_buffer1.get(), high_tier_id.value());
    ASSERT_TRUE(handle1.has_value());
    backend.Commit("multi_tier_key", handle1.value());

    // Commit to low priority tier with same key
    auto test_buffer2 = CreateTestBuffer(SMALL_DATA_SIZE);
    auto handle2 = AllocateAndWrite(backend, SMALL_DATA_SIZE,
                                    test_buffer2.get(), low_tier_id.value());
    ASSERT_TRUE(handle2.has_value());
    backend.Commit("multi_tier_key", handle2.value());

    // Get from specific tier
    auto get_result = backend.Get("multi_tier_key", low_tier_id.value());
    ASSERT_TRUE(get_result.has_value());
    EXPECT_EQ(get_result.value()->loc.tier->GetTierId(), low_tier_id.value());
}

// Test get returns highest priority tier when tier not specified
TEST_F(TieredBackendTest, GetHighestPriority) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 536870912,
                "priority": 100,
                "allocator_type": "OFFSET"
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
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    auto high_tier_id = GetTierIdByPriority(backend, 100);
    auto low_tier_id = GetTierIdByPriority(backend, 50);
    ASSERT_TRUE(high_tier_id.has_value());
    ASSERT_TRUE(low_tier_id.has_value());

    // Commit to both tiers
    auto test_buffer1 = CreateTestBuffer(SMALL_DATA_SIZE);
    auto handle1 = AllocateAndWrite(backend, SMALL_DATA_SIZE,
                                    test_buffer1.get(), high_tier_id.value());
    ASSERT_TRUE(handle1.has_value());
    backend.Commit("priority_key", handle1.value());

    auto test_buffer2 = CreateTestBuffer(SMALL_DATA_SIZE);
    auto handle2 = AllocateAndWrite(backend, SMALL_DATA_SIZE,
                                    test_buffer2.get(), low_tier_id.value());
    ASSERT_TRUE(handle2.has_value());
    backend.Commit("priority_key", handle2.value());

    // Get without specifying tier should return highest priority
    auto get_result = backend.Get("priority_key");
    ASSERT_TRUE(get_result.has_value());
    EXPECT_EQ(get_result.value()->loc.tier->GetTierId(), high_tier_id.value())
        << "Should return highest priority tier";
}

// Test get from tier that doesn't have the key
TEST_F(TieredBackendTest, GetTierNotFound) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 536870912,
                "priority": 100,
                "allocator_type": "OFFSET"
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
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    auto high_tier_id = GetTierIdByPriority(backend, 100);
    auto low_tier_id = GetTierIdByPriority(backend, 50);
    ASSERT_TRUE(high_tier_id.has_value());
    ASSERT_TRUE(low_tier_id.has_value());

    // Commit only to high priority tier
    auto test_buffer = CreateTestBuffer(SMALL_DATA_SIZE);
    auto handle = AllocateAndWrite(backend, SMALL_DATA_SIZE, test_buffer.get(),
                                   high_tier_id.value());
    ASSERT_TRUE(handle.has_value());
    backend.Commit("tier_specific_key", handle.value());

    // Try to get from low priority tier where it doesn't exist
    auto get_result = backend.Get("tier_specific_key", low_tier_id.value());
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(get_result.error(), ErrorCode::TIER_NOT_FOUND);
}

// ============================================================================
// Delete API Tests
// ============================================================================

// Test delete single replica
TEST_F(TieredBackendTest, DeleteSingleReplica) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 536870912,
                "priority": 100,
                "allocator_type": "OFFSET"
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
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    auto high_tier_id = GetTierIdByPriority(backend, 100);
    auto low_tier_id = GetTierIdByPriority(backend, 50);
    ASSERT_TRUE(high_tier_id.has_value());
    ASSERT_TRUE(low_tier_id.has_value());

    // Commit to both tiers
    auto test_buffer1 = CreateTestBuffer(SMALL_DATA_SIZE);
    auto handle1 = AllocateAndWrite(backend, SMALL_DATA_SIZE,
                                    test_buffer1.get(), high_tier_id.value());
    ASSERT_TRUE(handle1.has_value());
    backend.Commit("delete_test_key", handle1.value());

    auto test_buffer2 = CreateTestBuffer(SMALL_DATA_SIZE);
    auto handle2 = AllocateAndWrite(backend, SMALL_DATA_SIZE,
                                    test_buffer2.get(), low_tier_id.value());
    ASSERT_TRUE(handle2.has_value());
    backend.Commit("delete_test_key", handle2.value());

    // Delete from high priority tier only
    auto delete_result =
        backend.Delete("delete_test_key", high_tier_id.value());
    EXPECT_TRUE(delete_result.has_value()) << "Delete should succeed";

    // Should not be able to get from high tier
    auto get_high = backend.Get("delete_test_key", high_tier_id.value());
    EXPECT_FALSE(get_high.has_value());

    // Should still be able to get from low tier
    auto get_low = backend.Get("delete_test_key", low_tier_id.value());
    EXPECT_TRUE(get_low.has_value());
}

// Test delete all replicas
TEST_F(TieredBackendTest, DeleteAllReplicas) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 536870912,
                "priority": 100,
                "allocator_type": "OFFSET"
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
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    auto high_tier_id = GetTierIdByPriority(backend, 100);
    auto low_tier_id = GetTierIdByPriority(backend, 50);
    ASSERT_TRUE(high_tier_id.has_value());
    ASSERT_TRUE(low_tier_id.has_value());

    // Commit to both tiers
    auto test_buffer1 = CreateTestBuffer(SMALL_DATA_SIZE);
    auto handle1 = AllocateAndWrite(backend, SMALL_DATA_SIZE,
                                    test_buffer1.get(), high_tier_id.value());
    ASSERT_TRUE(handle1.has_value());
    backend.Commit("delete_all_key", handle1.value());

    auto test_buffer2 = CreateTestBuffer(SMALL_DATA_SIZE);
    auto handle2 = AllocateAndWrite(backend, SMALL_DATA_SIZE,
                                    test_buffer2.get(), low_tier_id.value());
    ASSERT_TRUE(handle2.has_value());
    backend.Commit("delete_all_key", handle2.value());

    // Delete all replicas (no tier_id specified)
    auto delete_result = backend.Delete("delete_all_key");
    EXPECT_TRUE(delete_result.has_value()) << "Delete all should succeed";

    // Should not be able to get from any tier
    auto get_result = backend.Get("delete_all_key");
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(get_result.error(), ErrorCode::INVALID_KEY);
}

// Test delete non-existent key
TEST_F(TieredBackendTest, DeleteNonExistentKey) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    // Try to delete non-existent key
    auto delete_result = backend.Delete("non_existent_key");
    EXPECT_FALSE(delete_result.has_value())
        << "Delete should fail for non-existent key";
    EXPECT_EQ(delete_result.error(), ErrorCode::INVALID_KEY);
}

// ============================================================================
// Integration Tests
// ============================================================================

// Test complete data lifecycle
TEST_F(TieredBackendTest, CompleteDataLifecycle) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    const std::string key = "lifecycle_key";

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
    EXPECT_EQ(get_result.value()->loc.tier->GetTierId(),
              handle->loc.tier->GetTierId());

    // 5. Delete
    auto delete_result = backend.Delete(key);
    ASSERT_TRUE(delete_result.has_value());

    // 6. Verify deleted
    auto get_after_delete = backend.Get(key);
    EXPECT_FALSE(get_after_delete.has_value());
    EXPECT_EQ(get_after_delete.error(), ErrorCode::INVALID_KEY);
}

// Test multi-tier data management
TEST_F(TieredBackendTest, MultiTierDataManagement) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 536870912,
                "priority": 100,
                "allocator_type": "OFFSET"
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
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    auto high_tier_id = GetTierIdByPriority(backend, 100);
    auto low_tier_id = GetTierIdByPriority(backend, 50);
    ASSERT_TRUE(high_tier_id.has_value());
    ASSERT_TRUE(low_tier_id.has_value());

    const std::string key = "multi_tier_key";

    // 1. Commit to high priority tier
    auto test_buffer1 = CreateTestBuffer(SMALL_DATA_SIZE);
    auto handle1 = AllocateAndWrite(backend, SMALL_DATA_SIZE,
                                    test_buffer1.get(), high_tier_id.value());
    ASSERT_TRUE(handle1.has_value());
    backend.Commit(key, handle1.value());

    // 2. Commit to low priority tier
    auto test_buffer2 = CreateTestBuffer(SMALL_DATA_SIZE);
    auto handle2 = AllocateAndWrite(backend, SMALL_DATA_SIZE,
                                    test_buffer2.get(), low_tier_id.value());
    ASSERT_TRUE(handle2.has_value());
    backend.Commit(key, handle2.value());

    // 3. Get without specifying tier should return high priority
    auto get_high = backend.Get(key);
    ASSERT_TRUE(get_high.has_value());
    EXPECT_EQ(get_high.value()->loc.tier->GetTierId(), high_tier_id.value());

    // 4. Get from low tier explicitly
    auto get_low = backend.Get(key, low_tier_id.value());
    ASSERT_TRUE(get_low.has_value());
    EXPECT_EQ(get_low.value()->loc.tier->GetTierId(), low_tier_id.value());

    // 5. Delete high priority replica
    auto delete_high = backend.Delete(key, high_tier_id.value());
    ASSERT_TRUE(delete_high.has_value());

    // 6. Get without tier should now return low priority
    auto get_after_delete = backend.Get(key);
    ASSERT_TRUE(get_after_delete.has_value());
    EXPECT_EQ(get_after_delete.value()->loc.tier->GetTierId(),
              low_tier_id.value());
}

// Test concurrent allocations
TEST_F(TieredBackendTest, ConcurrentAllocations) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    ASSERT_TRUE(backend.Init(config, nullptr, nullptr).has_value());

    const int num_allocations = 10;
    std::vector<AllocationHandle> handles;

    // Perform multiple allocations
    for (int i = 0; i < num_allocations; ++i) {
        auto result = backend.Allocate(SMALL_DATA_SIZE);
        ASSERT_TRUE(result.has_value()) << "Allocation " << i << " failed";
        handles.push_back(result.value());
    }

    // Verify all handles are valid
    EXPECT_EQ(handles.size(), num_allocations);
    for (const auto& handle : handles) {
        EXPECT_TRUE(handle);
    }

    // Verify total usage
    auto tier_views = backend.GetTierViews();
    ASSERT_EQ(tier_views.size(), 1);
    EXPECT_GT(tier_views[0].usage, 0);
}

// Simple thread pool for testing TieredBackend concurrent performance
class SimpleThreadPool {
   public:
    explicit SimpleThreadPool(size_t num_threads) : shutdown_(false) {
        for (size_t i = 0; i < num_threads; ++i) {
            threads_.emplace_back(&SimpleThreadPool::WorkerThread, this);
        }
    }

    ~SimpleThreadPool() {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            shutdown_ = true;
        }
        cv_.notify_all();
        for (auto& thread : threads_) {
            thread.join();
        }
    }

    template <typename F>
    auto Submit(F&& f) -> std::future<decltype(f())> {
        auto task = std::make_shared<std::packaged_task<decltype(f())()>>(
            std::forward<F>(f));
        auto future = task->get_future();
        {
            std::unique_lock<std::mutex> lock(mutex_);
            tasks_.push([task]() { (*task)(); });
        }
        cv_.notify_one();
        return future;
    }

   private:
    void WorkerThread() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] { return !tasks_.empty() || shutdown_; });
                if (shutdown_ && tasks_.empty()) {
                    break;
                }
                if (!tasks_.empty()) {
                    task = std::move(tasks_.front());
                    tasks_.pop();
                }
            }
            if (task) {
                task();
            }
        }
    }

    std::vector<std::thread> threads_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool shutdown_;
};

// Test concurrent Allocate and Commit performance with different thread pool sizes
TEST_F(TieredBackendTest, ConcurrentAllocateCommitPerformance) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    const int num_operations = 50;
    const size_t data_size = 8 * 1024 * 1024;  // 8MB
    std::vector<size_t> thread_pool_sizes = {1, 2, 4, 8, 16, 32, 64, 128};

    LOG(INFO) << "=== TieredBackend Concurrent Allocate/Commit Performance ===";
    LOG(INFO) << "Testing with " << num_operations << " operations";
    LOG(INFO) << "Data size per operation: " << (data_size / (1024 * 1024)) << " MB";
    LOG(INFO) << "Thread pool sizes to test: ";
    for (size_t size : thread_pool_sizes) {
        LOG(INFO) << "  - " << size << " threads";
    }

    // Prepare keys and test data
    std::vector<std::string> keys;
    std::vector<std::unique_ptr<char[]>> data_buffers;
    for (int i = 0; i < num_operations; ++i) {
        std::string key = "tiered_perf_key_" + std::to_string(i);
        keys.push_back(key);
        
        // Create 8MB data buffer
        auto buffer = std::make_unique<char[]>(data_size);
        for (size_t j = 0; j < data_size; ++j) {
            buffer[j] = static_cast<char>((i + j) % 256);
        }
        data_buffers.push_back(std::move(buffer));
    }

    // ========== Sequential Execution (Baseline) ==========
    LOG(INFO) << "\n--- Sequential Execution (Baseline) ---";
    
    TieredBackend sequential_backend;
    ASSERT_TRUE(sequential_backend.Init(config, nullptr, nullptr).has_value());
    
    auto sequential_start = std::chrono::steady_clock::now();
    
    int64_t sequential_allocate_total = 0;
    int64_t sequential_write_total = 0;
    int64_t sequential_commit_total = 0;
    
    for (int i = 0; i < num_operations; ++i) {
        // Allocate
        auto allocate_start = std::chrono::steady_clock::now();
        auto alloc_result = sequential_backend.Allocate(data_size);
        auto allocate_end = std::chrono::steady_clock::now();
        auto allocate_duration = std::chrono::duration_cast<std::chrono::microseconds>(
            allocate_end - allocate_start).count();
        sequential_allocate_total += allocate_duration;
        
        ASSERT_TRUE(alloc_result.has_value())
            << "Sequential Allocate failed for key: " << keys[i];
        AllocationHandle handle = alloc_result.value();
        
        // Write data
        auto write_start = std::chrono::steady_clock::now();
        DataSource source;
        auto buffer = std::make_unique<char[]>(data_size);
        std::memcpy(buffer.get(), data_buffers[i].get(), data_size);
        source.buffer = std::make_unique<TempDRAMBuffer>(std::move(buffer), data_size);
        source.type = MemoryType::DRAM;
        auto write_result = sequential_backend.Write(source, handle);
        auto write_end = std::chrono::steady_clock::now();
        auto write_duration = std::chrono::duration_cast<std::chrono::microseconds>(
            write_end - write_start).count();
        sequential_write_total += write_duration;
        
        ASSERT_TRUE(write_result.has_value())
            << "Sequential Write failed for key: " << keys[i];
        
        // Commit
        auto commit_start = std::chrono::steady_clock::now();
        auto commit_result = sequential_backend.Commit(keys[i], handle);
        auto commit_end = std::chrono::steady_clock::now();
        auto commit_duration = std::chrono::duration_cast<std::chrono::microseconds>(
            commit_end - commit_start).count();
        sequential_commit_total += commit_duration;
        
        ASSERT_TRUE(commit_result.has_value())
            << "Sequential Commit failed for key: " << keys[i];
    }
    
    auto sequential_end = std::chrono::steady_clock::now();
    auto sequential_duration = std::chrono::duration_cast<std::chrono::microseconds>(
        sequential_end - sequential_start).count();
    
    LOG(INFO) << "Sequential execution time: " << sequential_duration << " us";
    LOG(INFO) << "Sequential breakdown:";
    LOG(INFO) << "  - Allocate total: " << sequential_allocate_total << " us"
              << " (avg: " << (sequential_allocate_total / num_operations) << " us/op)";
    LOG(INFO) << "  - Write total: " << sequential_write_total << " us"
              << " (avg: " << (sequential_write_total / num_operations) << " us/op)";
    LOG(INFO) << "  - Commit total: " << sequential_commit_total << " us"
              << " (avg: " << (sequential_commit_total / num_operations) << " us/op)";
    LOG(INFO) << "  - Total: " << sequential_duration << " us"
              << " (avg: " << (sequential_duration / num_operations) << " us/op)";
    LOG(INFO) << "  - Overhead: " << (sequential_duration - sequential_allocate_total - 
                                      sequential_write_total - sequential_commit_total) << " us";

    // Clean up
    for (const auto& key : keys) {
        sequential_backend.Delete(key);
    }

    // ========== Concurrent Execution with Different Thread Pool Sizes ==========
    LOG(INFO) << "\n--- Concurrent Execution with Different Thread Pool Sizes ---";
    
    struct PerformanceResult {
        size_t thread_pool_size;
        int64_t duration_us;
        double speedup;
    };
    std::vector<PerformanceResult> results;

    for (size_t pool_size : thread_pool_sizes) {
        LOG(INFO) << "\nTesting with " << pool_size << " threads...";
        
        TieredBackend concurrent_backend;
        ASSERT_TRUE(concurrent_backend.Init(config, nullptr, nullptr).has_value());
        
        SimpleThreadPool thread_pool(pool_size);
        
        auto concurrent_start = std::chrono::steady_clock::now();
        
        // Submit all Allocate + Write + Commit operations
        std::vector<std::future<tl::expected<std::tuple<int64_t, int64_t, int64_t>, ErrorCode>>> futures;
        for (int i = 0; i < num_operations; ++i) {
            auto future = thread_pool.Submit([&concurrent_backend, &keys, &data_buffers, 
                                               data_size, i]() 
                -> tl::expected<std::tuple<int64_t, int64_t, int64_t>, ErrorCode> {
                // Allocate
                auto allocate_start = std::chrono::steady_clock::now();
                auto alloc_result = concurrent_backend.Allocate(data_size);
                auto allocate_end = std::chrono::steady_clock::now();
                auto allocate_duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    allocate_end - allocate_start).count();
                
                if (!alloc_result.has_value()) {
                    return tl::make_unexpected(alloc_result.error());
                }
                AllocationHandle handle = alloc_result.value();
                
                // Write data
                auto write_start = std::chrono::steady_clock::now();
                DataSource source;
                auto buffer = std::make_unique<char[]>(data_size);
                std::memcpy(buffer.get(), data_buffers[i].get(), data_size);
                source.buffer = std::make_unique<TempDRAMBuffer>(std::move(buffer), data_size);
                source.type = MemoryType::DRAM;
                auto write_result = concurrent_backend.Write(source, handle);
                auto write_end = std::chrono::steady_clock::now();
                auto write_duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    write_end - write_start).count();
                
                if (!write_result.has_value()) {
                    return tl::make_unexpected(write_result.error());
                }
                
                // Commit
                auto commit_start = std::chrono::steady_clock::now();
                auto commit_result = concurrent_backend.Commit(keys[i], handle);
                auto commit_end = std::chrono::steady_clock::now();
                auto commit_duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    commit_end - commit_start).count();
                
                if (!commit_result.has_value()) {
                    return tl::make_unexpected(commit_result.error());
                }
                
                return std::make_tuple(allocate_duration, write_duration, commit_duration);
            });
            futures.push_back(std::move(future));
        }
        
        // Wait for all operations to complete and collect timing data
        bool all_success = true;
        int64_t concurrent_allocate_total = 0;
        int64_t concurrent_write_total = 0;
        int64_t concurrent_commit_total = 0;
        
        for (size_t i = 0; i < futures.size(); ++i) {
            auto result = futures[i].get();
            if (!result.has_value()) {
                LOG(ERROR) << "Concurrent operation failed for key: " << keys[i]
                           << ", error: " << toString(result.error())
                           << " with pool size " << pool_size;
                all_success = false;
            } else {
                auto [alloc_dur, write_dur, commit_dur] = result.value();
                concurrent_allocate_total += alloc_dur;
                concurrent_write_total += write_dur;
                concurrent_commit_total += commit_dur;
            }
        }
        
        auto concurrent_end = std::chrono::steady_clock::now();
        auto concurrent_duration = std::chrono::duration_cast<std::chrono::microseconds>(
            concurrent_end - concurrent_start).count();
        
        if (!all_success) {
            LOG(ERROR) << "Some operations failed with pool size " << pool_size;
            continue;
        }
        
        double speedup = static_cast<double>(sequential_duration) /
                        static_cast<double>(concurrent_duration);
        
        results.push_back({pool_size, concurrent_duration, speedup});
        
        LOG(INFO) << "  Total duration: " << concurrent_duration << " us";
        LOG(INFO) << "  Concurrent breakdown:";
        LOG(INFO) << "    - Allocate total: " << concurrent_allocate_total << " us"
                  << " (avg: " << (concurrent_allocate_total / num_operations) << " us/op)";
        LOG(INFO) << "    - Write total: " << concurrent_write_total << " us"
                  << " (avg: " << (concurrent_write_total / num_operations) << " us/op)";
        LOG(INFO) << "    - Commit total: " << concurrent_commit_total << " us"
                  << " (avg: " << (concurrent_commit_total / num_operations) << " us/op)";
        LOG(INFO) << "    - Overhead: " << (concurrent_duration - concurrent_allocate_total - 
                                            concurrent_write_total - concurrent_commit_total) << " us";
        
        // Compare with sequential
        double allocate_speedup = (sequential_allocate_total > 0) ?
            static_cast<double>(sequential_allocate_total) / concurrent_allocate_total : 0.0;
        double write_speedup = (sequential_write_total > 0) ?
            static_cast<double>(sequential_write_total) / concurrent_write_total : 0.0;
        double commit_speedup = (sequential_commit_total > 0) ?
            static_cast<double>(sequential_commit_total) / concurrent_commit_total : 0.0;
        
        LOG(INFO) << "  Speedup comparison:";
        LOG(INFO) << "    - Overall speedup: " << std::fixed << std::setprecision(2) << speedup << "x";
        LOG(INFO) << "    - Allocate speedup: " << std::fixed << std::setprecision(2) 
                  << allocate_speedup << "x";
        LOG(INFO) << "    - Write speedup: " << std::fixed << std::setprecision(2) 
                  << write_speedup << "x";
        LOG(INFO) << "    - Commit speedup: " << std::fixed << std::setprecision(2) 
                  << commit_speedup << "x";

        // Clean up for next iteration
        for (const auto& key : keys) {
            concurrent_backend.Delete(key);
        }
    }

    // Summary
    LOG(INFO) << "\n=== Performance Summary ===";
    LOG(INFO) << std::left << std::setw(10) << "Threads"
              << std::setw(15) << "Duration (us)"
              << std::setw(15) << "Speedup"
              << std::setw(20) << "Avg per op (us)";
    LOG(INFO) << std::string(60, '-');
    
    for (const auto& result : results) {
        LOG(INFO) << std::left << std::setw(10) << result.thread_pool_size
                  << std::setw(15) << result.duration_us
                  << std::setw(15) << std::fixed << std::setprecision(2)
                  << result.speedup << "x"
                  << std::setw(20)
                  << (result.duration_us / num_operations);
    }
    
    LOG(INFO) << "\nBaseline (Sequential): " << sequential_duration << " us";
    
    // Find best performance
    if (!results.empty()) {
        auto best_result = std::min_element(results.begin(), results.end(),
            [](const PerformanceResult& a, const PerformanceResult& b) {
                return a.duration_us < b.duration_us;
            });
        
        if (best_result != results.end()) {
            LOG(INFO) << "\nBest performance: " << best_result->thread_pool_size
                      << " threads (" << best_result->duration_us << " us, "
                      << std::fixed << std::setprecision(2) << best_result->speedup
                      << "x speedup)";
        }
    }
}

// Test concurrent Allocate performance only (without Commit)
TEST_F(TieredBackendTest, ConcurrentAllocatePerformance) {
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    const int num_operations = 50;
    const size_t data_size = 8 * 1024 * 1024;  // 8MB
    std::vector<size_t> thread_pool_sizes = {1, 2, 4, 8, 16, 32, 64, 128};

    LOG(INFO) << "=== TieredBackend Concurrent Allocate Performance (Allocate Only) ===";
    LOG(INFO) << "Testing with " << num_operations << " operations";
    LOG(INFO) << "Data size per operation: " << (data_size / (1024 * 1024)) << " MB";
    LOG(INFO) << "Thread pool sizes to test: ";
    for (size_t size : thread_pool_sizes) {
        LOG(INFO) << "  - " << size << " threads";
    }

    // ========== Sequential Execution (Baseline) ==========
    LOG(INFO) << "\n--- Sequential Allocate Execution (Baseline) ---";
    
    TieredBackend sequential_backend;
    ASSERT_TRUE(sequential_backend.Init(config, nullptr, nullptr).has_value());
    
    auto sequential_start = std::chrono::steady_clock::now();
    
    std::vector<AllocationHandle> sequential_handles;
    for (int i = 0; i < num_operations; ++i) {
        auto alloc_result = sequential_backend.Allocate(data_size);
        ASSERT_TRUE(alloc_result.has_value())
            << "Sequential Allocate failed for operation " << i;
        sequential_handles.push_back(alloc_result.value());
    }
    
    auto sequential_end = std::chrono::steady_clock::now();
    auto sequential_duration = std::chrono::duration_cast<std::chrono::microseconds>(
        sequential_end - sequential_start).count();
    
    LOG(INFO) << "Sequential Allocate execution time: " << sequential_duration << " us";
    LOG(INFO) << "Average per operation: " 
              << (sequential_duration / num_operations) << " us";

    // Clean up (handles will auto-free when destroyed)
    sequential_handles.clear();

    // ========== Concurrent Execution with Different Thread Pool Sizes ==========
    LOG(INFO) << "\n--- Concurrent Allocate Execution with Different Thread Pool Sizes ---";
    
    struct PerformanceResult {
        size_t thread_pool_size;
        int64_t duration_us;
        double speedup;
    };
    std::vector<PerformanceResult> results;

    for (size_t pool_size : thread_pool_sizes) {
        LOG(INFO) << "\nTesting with " << pool_size << " threads...";
        
        TieredBackend concurrent_backend;
        ASSERT_TRUE(concurrent_backend.Init(config, nullptr, nullptr).has_value());
        
        SimpleThreadPool thread_pool(pool_size);
        
        auto concurrent_start = std::chrono::steady_clock::now();
        
        // Submit all Allocate operations
        std::vector<std::future<tl::expected<AllocationHandle, ErrorCode>>> futures;
        for (int i = 0; i < num_operations; ++i) {
            auto future = thread_pool.Submit([&concurrent_backend, data_size]() 
                -> tl::expected<AllocationHandle, ErrorCode> {
                return concurrent_backend.Allocate(data_size);
            });
            futures.push_back(std::move(future));
        }
        
        // Wait for all operations to complete
        bool all_success = true;
        std::vector<AllocationHandle> concurrent_handles;
        for (size_t i = 0; i < futures.size(); ++i) {
            auto result = futures[i].get();
            if (!result.has_value()) {
                LOG(ERROR) << "Concurrent Allocate failed for operation " << i
                           << ", error: " << toString(result.error())
                           << " with pool size " << pool_size;
                all_success = false;
            } else {
                concurrent_handles.push_back(result.value());
            }
        }
        
        auto concurrent_end = std::chrono::steady_clock::now();
        auto concurrent_duration = std::chrono::duration_cast<std::chrono::microseconds>(
            concurrent_end - concurrent_start).count();
        
        if (!all_success) {
            LOG(ERROR) << "Some Allocate operations failed with pool size " << pool_size;
            continue;
        }
        
        double speedup = static_cast<double>(sequential_duration) /
                        static_cast<double>(concurrent_duration);
        
        results.push_back({pool_size, concurrent_duration, speedup});
        
        LOG(INFO) << "  Duration: " << concurrent_duration << " us";
        LOG(INFO) << "  Speedup: " << std::fixed << std::setprecision(2)
                  << speedup << "x";
        LOG(INFO) << "  Average per operation: "
                  << (concurrent_duration / num_operations) << " us";

        // Clean up (handles will auto-free when destroyed)
        concurrent_handles.clear();
    }

    // Summary
    LOG(INFO) << "\n=== Allocate Performance Summary ===";
    LOG(INFO) << std::left << std::setw(10) << "Threads"
              << std::setw(15) << "Duration (us)"
              << std::setw(15) << "Speedup"
              << std::setw(20) << "Avg per op (us)";
    LOG(INFO) << std::string(60, '-');
    
    for (const auto& result : results) {
        LOG(INFO) << std::left << std::setw(10) << result.thread_pool_size
                  << std::setw(15) << result.duration_us
                  << std::setw(15) << std::fixed << std::setprecision(2)
                  << result.speedup << "x"
                  << std::setw(20)
                  << (result.duration_us / num_operations);
    }
    
    LOG(INFO) << "\nBaseline (Sequential Allocate): " << sequential_duration << " us";
    
    // Find best performance
    if (!results.empty()) {
        auto best_result = std::min_element(results.begin(), results.end(),
            [](const PerformanceResult& a, const PerformanceResult& b) {
                return a.duration_us < b.duration_us;
            });
        
        if (best_result != results.end()) {
            LOG(INFO) << "\nBest performance: " << best_result->thread_pool_size
                      << " threads (" << best_result->duration_us << " us, "
                      << std::fixed << std::setprecision(2) << best_result->speedup
                      << "x speedup)";
        }
    }
}
}  // namespace mooncake
