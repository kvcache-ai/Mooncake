#include "types.h"
#include "replica.h"
#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <sstream>
#include <vector>

namespace mooncake::test {

class ObjectDataTypeTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("ObjectDataTypeTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    static constexpr size_t kDefaultSegmentBase = 0x300000000;
    static constexpr size_t kDefaultSegmentSize = 1024 * 1024 * 16;

    Segment MakeSegment(std::string name = "test_segment",
                        size_t base = kDefaultSegmentBase,
                        size_t size = kDefaultSegmentSize) const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = size;
        segment.te_endpoint = segment.name;
        return segment;
    }
};

// Verify enum values match the RFC spec
TEST_F(ObjectDataTypeTest, EnumValues) {
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::UNKNOWN), 0);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::KVCACHE), 1);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::TENSOR), 2);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::WEIGHT), 3);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::SAMPLE), 4);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::ACTIVATION), 5);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::GRADIENT), 6);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::OPTIMIZER_STATE), 7);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::METADATA), 8);
    EXPECT_EQ(static_cast<uint8_t>(ObjectDataType::GENERAL), 9);
}

// Verify stream operator produces readable output
TEST_F(ObjectDataTypeTest, StreamOperator) {
    std::ostringstream oss;
    oss << ObjectDataType::KVCACHE;
    EXPECT_EQ(oss.str(), "KVCACHE");

    oss.str("");
    oss << ObjectDataType::UNKNOWN;
    EXPECT_EQ(oss.str(), "UNKNOWN");

    oss.str("");
    oss << ObjectDataType::OPTIMIZER_STATE;
    EXPECT_EQ(oss.str(), "OPTIMIZER_STATE");

    oss.str("");
    oss << ObjectDataType::GENERAL;
    EXPECT_EQ(oss.str(), "GENERAL");

    // Out-of-range value should print "UNKNOWN"
    oss.str("");
    oss << static_cast<ObjectDataType>(200);
    EXPECT_EQ(oss.str(), "UNKNOWN");
}

// ReplicateConfig defaults to UNKNOWN
TEST_F(ObjectDataTypeTest, ReplicateConfigDefaultDataType) {
    ReplicateConfig config;
    EXPECT_EQ(config.data_type, ObjectDataType::UNKNOWN);
}

// ReplicateConfig can be set to other types
TEST_F(ObjectDataTypeTest, ReplicateConfigSetDataType) {
    ReplicateConfig config;
    config.data_type = ObjectDataType::WEIGHT;
    EXPECT_EQ(config.data_type, ObjectDataType::WEIGHT);
}

// ReplicateConfig stream output includes data_type
TEST_F(ObjectDataTypeTest, ReplicateConfigStreamIncludesDataType) {
    ReplicateConfig config;
    config.data_type = ObjectDataType::TENSOR;
    std::ostringstream oss;
    oss << config;
    EXPECT_NE(oss.str().find("data_type: TENSOR"), std::string::npos);
}

// PutStart with data_type propagates to ObjectMetadata
TEST_F(ObjectDataTypeTest, PutStartWithDataType) {
    std::unique_ptr<MasterService> service(new MasterService());
    Segment segment = MakeSegment();
    UUID client_id = generate_uuid();
    auto mount_result = service->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    UUID put_client = generate_uuid();

    // Put with WEIGHT type
    ReplicateConfig config;
    config.replica_num = 1;
    config.data_type = ObjectDataType::WEIGHT;

    auto result = service->PutStart(put_client, "key_weight", 1024, config);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result.value().empty());

    auto end_result =
        service->PutEnd(put_client, "key_weight", ReplicaType::MEMORY);
    EXPECT_TRUE(end_result.has_value());
}

// PutStart with default UNKNOWN data_type still works (backward compat)
TEST_F(ObjectDataTypeTest, PutStartDefaultDataType) {
    std::unique_ptr<MasterService> service(new MasterService());
    Segment segment = MakeSegment();
    UUID client_id = generate_uuid();
    auto mount_result = service->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    UUID put_client = generate_uuid();
    ReplicateConfig config;
    config.replica_num = 1;
    // data_type left as default (UNKNOWN)

    auto result = service->PutStart(put_client, "key_default", 1024, config);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result.value().empty());
}

// Verify all enum values can roundtrip through uint8_t cast
TEST_F(ObjectDataTypeTest, EnumRoundtrip) {
    std::vector<ObjectDataType> all_types = {
        ObjectDataType::UNKNOWN,  ObjectDataType::KVCACHE,
        ObjectDataType::TENSOR,   ObjectDataType::WEIGHT,
        ObjectDataType::SAMPLE,   ObjectDataType::ACTIVATION,
        ObjectDataType::GRADIENT, ObjectDataType::OPTIMIZER_STATE,
        ObjectDataType::METADATA, ObjectDataType::GENERAL,
    };

    for (auto type : all_types) {
        uint8_t raw = static_cast<uint8_t>(type);
        auto recovered = static_cast<ObjectDataType>(raw);
        EXPECT_EQ(type, recovered);
    }
}

}  // namespace mooncake::test
