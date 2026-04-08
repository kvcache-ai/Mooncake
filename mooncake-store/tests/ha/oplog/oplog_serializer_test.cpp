#include "ha/oplog/oplog_serializer.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <xxhash.h>

#include <string>

namespace mooncake::test {

class OpLogSerializerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OpLogSerializerTest");
        FLAGS_logtostderr = 1;
    }
    void TearDown() override { google::ShutdownGoogleLogging(); }

    static OpLogEntry MakeEntry(uint64_t seq, OpType type,
                                const std::string& key,
                                const std::string& payload) {
        OpLogEntry e;
        e.sequence_id = seq;
        e.timestamp_ms = 1234567890;
        e.op_type = type;
        e.object_key = key;
        e.payload = payload;
        e.checksum =
            static_cast<uint32_t>(XXH32(payload.data(), payload.size(), 0));
        e.prefix_hash =
            key.empty()
                ? 0
                : static_cast<uint32_t>(XXH32(key.data(), key.size(), 0));
        return e;
    }
};

TEST_F(OpLogSerializerTest, RoundTrip_PutEnd) {
    OpLogEntry in = MakeEntry(1, OpType::PUT_END, "key1", "value1");
    std::string json = SerializeOpLogEntry(in);
    OpLogEntry out;
    ASSERT_TRUE(DeserializeOpLogEntry(json, out));
    EXPECT_EQ(in.sequence_id, out.sequence_id);
    EXPECT_EQ(in.timestamp_ms, out.timestamp_ms);
    EXPECT_EQ(in.op_type, out.op_type);
    EXPECT_EQ(in.object_key, out.object_key);
    EXPECT_EQ(in.payload, out.payload);
    EXPECT_EQ(in.checksum, out.checksum);
    EXPECT_EQ(in.prefix_hash, out.prefix_hash);
}

TEST_F(OpLogSerializerTest, RoundTrip_Remove) {
    OpLogEntry in = MakeEntry(42, OpType::REMOVE, "obj/to/remove", "");
    std::string json = SerializeOpLogEntry(in);
    OpLogEntry out;
    ASSERT_TRUE(DeserializeOpLogEntry(json, out));
    EXPECT_EQ(in.sequence_id, out.sequence_id);
    EXPECT_EQ(in.op_type, out.op_type);
    EXPECT_EQ(in.object_key, out.object_key);
    EXPECT_EQ(in.payload, out.payload);
}

TEST_F(OpLogSerializerTest, RoundTrip_PutRevoke) {
    OpLogEntry in = MakeEntry(99, OpType::PUT_REVOKE, "revoked_key", "meta");
    std::string json = SerializeOpLogEntry(in);
    OpLogEntry out;
    ASSERT_TRUE(DeserializeOpLogEntry(json, out));
    EXPECT_EQ(in.op_type, out.op_type);
    EXPECT_EQ(in.payload, out.payload);
}

TEST_F(OpLogSerializerTest, RoundTrip_BinaryPayload) {
    // Payload with null bytes, high bytes — must survive base64 round-trip
    std::string binary_payload;
    for (int i = 0; i < 256; ++i) {
        binary_payload.push_back(static_cast<char>(i));
    }
    OpLogEntry in = MakeEntry(7, OpType::PUT_END, "bin_key", binary_payload);
    std::string json = SerializeOpLogEntry(in);
    OpLogEntry out;
    ASSERT_TRUE(DeserializeOpLogEntry(json, out));
    EXPECT_EQ(in.payload, out.payload);
}

TEST_F(OpLogSerializerTest, RoundTrip_EmptyPayload) {
    OpLogEntry in = MakeEntry(10, OpType::REMOVE, "key", "");
    std::string json = SerializeOpLogEntry(in);
    OpLogEntry out;
    ASSERT_TRUE(DeserializeOpLogEntry(json, out));
    EXPECT_EQ("", out.payload);
}

TEST_F(OpLogSerializerTest, RoundTrip_EmptyKey) {
    OpLogEntry in = MakeEntry(11, OpType::PUT_END, "", "payload");
    std::string json = SerializeOpLogEntry(in);
    OpLogEntry out;
    ASSERT_TRUE(DeserializeOpLogEntry(json, out));
    EXPECT_EQ("", out.object_key);
    EXPECT_EQ(0u, out.prefix_hash);
}

TEST_F(OpLogSerializerTest, Deserialize_InvalidJson) {
    OpLogEntry out;
    EXPECT_FALSE(DeserializeOpLogEntry("{ not valid json }", out));
}

TEST_F(OpLogSerializerTest, Deserialize_EmptyString) {
    OpLogEntry out;
    EXPECT_FALSE(DeserializeOpLogEntry("", out));
}

TEST_F(OpLogSerializerTest, Deserialize_MissingFields) {
    // JSON with only partial fields — should fail or produce defaults
    std::string partial = R"({"sequence_id": 1})";
    OpLogEntry out;
    // Behavior depends on JsonCpp defaults for missing fields.
    // At minimum, should not crash.
    (void)DeserializeOpLogEntry(partial, out);
}

TEST_F(OpLogSerializerTest, Deserialize_KeyTooLarge) {
    OpLogEntry in = MakeEntry(1, OpType::PUT_END, "k", "v");
    in.object_key.assign(OpLogManager::kMaxObjectKeySize + 1, 'k');
    std::string json = SerializeOpLogEntry(in);
    OpLogEntry out;
    EXPECT_FALSE(DeserializeOpLogEntry(json, out));
}

TEST_F(OpLogSerializerTest, Deserialize_PayloadTooLarge) {
    OpLogEntry in = MakeEntry(1, OpType::PUT_END, "k", "");
    in.payload.assign(OpLogManager::kMaxPayloadSize + 1, 'p');
    std::string json = SerializeOpLogEntry(in);
    OpLogEntry out;
    EXPECT_FALSE(DeserializeOpLogEntry(json, out));
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
