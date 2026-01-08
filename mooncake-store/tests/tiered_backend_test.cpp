#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>
#include <fstream>

#include "tiered_cache/tiered_backend.h"

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

}  // namespace mooncake
