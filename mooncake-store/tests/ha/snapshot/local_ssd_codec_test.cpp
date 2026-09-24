#include <gtest/gtest.h>

#include "ha/snapshot/local_ssd_codec.h"

namespace mooncake::ha {
namespace {

OffloadTaskItem Offload(std::string tenant, std::string key, int64_t size) {
    return OffloadTaskItem{
        .tenant_id = std::move(tenant), .key = std::move(key), .size = size};
}

std::string PackState(const LocalSsdPersistedState& state) {
    msgpack::sbuffer buffer;
    MsgpackPacker packer(&buffer);
    EXPECT_TRUE(LocalSsdCodec::Encode(state, packer).has_value());
    return std::string(buffer.data(), buffer.size());
}

TEST(LocalSsdCodecTest, RoundTripIsDeterministic) {
    LocalSsdPersistedState state;
    state[{9, 2}] = LocalSsdPersistedClient{
        .enable_offloading = false,
        .total_capacity_bytes = 10,
        .pending_offloads = {{"z", Offload("tenant", "z", 1)}}};
    state[{1, 8}] = LocalSsdPersistedClient{
        .enable_offloading = true,
        .total_capacity_bytes = 20,
        .pending_offloads = {{"b", Offload("tenant", "b", 2)},
                             {"a", Offload("tenant", "a", 3)}}};

    auto first = PackState(state);
    auto second = PackState(state);
    EXPECT_EQ(first, second);
    auto object = msgpack::unpack(first.data(), first.size());
    auto decoded = LocalSsdCodec::Decode(&object.get());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(PackState(*decoded), first);
}

TEST(LocalSsdCodecTest, DecodesLegacySizeAndPreservesEncodedKey) {
    const std::string encoded_key("legacy\0key", 10);
    msgpack::sbuffer buffer;
    MsgpackPacker packer(&buffer);
    packer.pack_map(1);
    packer.pack(UuidToString(UUID{11, 12}));
    packer.pack_array(4);
    packer.pack(true);
    packer.pack(uint64_t{1});
    packer.pack(encoded_key);
    packer.pack(int64_t{99});

    auto object = msgpack::unpack(buffer.data(), buffer.size());
    auto decoded = LocalSsdCodec::Decode(&object.get());
    ASSERT_TRUE(decoded.has_value());
    const auto& client = decoded->at(UUID{11, 12});
    EXPECT_EQ(client.total_capacity_bytes, 0);
    ASSERT_EQ(client.pending_offloads.size(), 1);
    auto task = client.pending_offloads.find(encoded_key);
    ASSERT_NE(task, client.pending_offloads.end());
    EXPECT_EQ(task->second.tenant_id, "legacy");
    EXPECT_EQ(task->second.key, "key");
    EXPECT_EQ(task->second.size, 99);
}

TEST(LocalSsdCodecTest, MissingFieldIsEmptyAndInvalidInputFails) {
    auto missing = LocalSsdCodec::Decode(nullptr);
    ASSERT_TRUE(missing.has_value());
    EXPECT_TRUE(missing->empty());

    msgpack::sbuffer buffer;
    MsgpackPacker packer(&buffer);
    packer.pack_array(0);
    auto object = msgpack::unpack(buffer.data(), buffer.size());
    EXPECT_FALSE(LocalSsdCodec::Decode(&object.get()).has_value());
}

}  // namespace
}  // namespace mooncake::ha
