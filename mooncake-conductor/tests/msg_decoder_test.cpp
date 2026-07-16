// Tests for the current vLLM msgspec and Mooncake publisher map protocols.

#include <gtest/gtest.h>
#include <msgpack.hpp>

#include <array>
#include <cstdint>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "conductor/zmq/msg_decoder.h"

namespace {

using conductor::zmq::DecodeMooncakeEventBatch;
using conductor::zmq::DecodeVllmEventBatch;
using conductor::zmq::MooncakeClearedEvent;
using conductor::zmq::MooncakeEventFields;
using conductor::zmq::MooncakeRemovedEvent;
using conductor::zmq::MooncakeStoredEvent;
using conductor::zmq::VllmClearedEvent;
using conductor::zmq::VllmRemovedEvent;
using conductor::zmq::VllmStoredEvent;

using Packer = msgpack::packer<std::stringstream>;

constexpr int64_t kMooncakeTimestamp = 1700000000123LL;
constexpr int64_t kMooncakeDpRank = 2;
constexpr uint64_t kProjectedHash = 0x18191a1b1c1d1e1fULL;

std::string BytesFromHex(std::string_view hex) {
    if (hex.size() % 2 != 0) {
        throw std::invalid_argument("hex input must have an even length");
    }
    const auto nibble = [](char value) -> uint8_t {
        if (value >= '0' && value <= '9') {
            return static_cast<uint8_t>(value - '0');
        }
        if (value >= 'a' && value <= 'f') {
            return static_cast<uint8_t>(value - 'a' + 10);
        }
        if (value >= 'A' && value <= 'F') {
            return static_cast<uint8_t>(value - 'A' + 10);
        }
        throw std::invalid_argument("invalid hex digit");
    };

    std::string bytes;
    bytes.reserve(hex.size() / 2);
    for (size_t index = 0; index < hex.size(); index += 2) {
        bytes.push_back(static_cast<char>((nibble(hex[index]) << 4) |
                                          nibble(hex[index + 1])));
    }
    return bytes;
}

template <typename PackEvents>
std::string PackVllmBatch(uint32_t event_count, PackEvents pack_events,
                          std::optional<int64_t> dp_rank = 3) {
    std::stringstream buffer;
    Packer packer(buffer);
    packer.pack_array(3);
    packer.pack_double(1.25);
    packer.pack_array(event_count);
    pack_events(packer);
    if (dp_rank.has_value()) {
        packer.pack(*dp_rank);
    } else {
        packer.pack_nil();
    }
    return buffer.str();
}

template <typename PackEvents>
std::string PackMooncakeBatch(
    uint32_t event_count, PackEvents pack_events,
    std::optional<int64_t> dp_rank = kMooncakeDpRank) {
    std::stringstream buffer;
    Packer packer(buffer);
    packer.pack_array(3);
    packer.pack(kMooncakeTimestamp);
    packer.pack_array(event_count);
    pack_events(packer);
    if (dp_rank.has_value()) {
        packer.pack(*dp_rank);
    } else {
        packer.pack_nil();
    }
    return buffer.str();
}

void PackBinary(Packer& packer, const std::vector<uint8_t>& bytes) {
    packer.pack_bin(bytes.size());
    packer.pack_bin_body(reinterpret_cast<const char*>(bytes.data()),
                         bytes.size());
}

void PackVllmStored(Packer& packer, bool include_unknown = false) {
    packer.pack_map(include_unknown ? 9 : 8);
    packer.pack("type");
    packer.pack("BlockStored");
    packer.pack("block_hashes");
    packer.pack_array(1);
    packer.pack_uint64(42);
    packer.pack("parent_block_hash");
    packer.pack_nil();
    packer.pack("token_ids");
    packer.pack_array(2);
    packer.pack_int32(1);
    packer.pack_int32(2);
    packer.pack("block_size");
    packer.pack_int64(2);
    packer.pack("lora_id");
    packer.pack_nil();
    packer.pack("medium");
    packer.pack("GPU");
    packer.pack("lora_name");
    packer.pack_nil();
    if (include_unknown) {
        packer.pack("future_store_metadata");
        packer.pack_map(1);
        packer.pack("version");
        packer.pack_int64(2);
    }
}

void PackVllmRemoved(Packer& packer) {
    packer.pack_map(4);
    packer.pack("type");
    packer.pack("BlockRemoved");
    packer.pack("block_hashes");
    packer.pack_array(1);
    packer.pack_uint64(42);
    packer.pack("medium");
    packer.pack("GPU");
    packer.pack("group_idx");
    packer.pack_int64(0);
}

void PackVllmCleared(Packer& packer) {
    packer.pack_map(1);
    packer.pack("type");
    packer.pack("AllBlocksCleared");
}

void PackMooncakeCommon(Packer& packer, uint64_t event_id,
                        std::string_view event_type,
                        std::string_view legacy_type, std::string_view medium) {
    packer.pack("event_id");
    packer.pack_uint64(event_id);
    packer.pack("timestamp");
    packer.pack(kMooncakeTimestamp);
    packer.pack("event_type");
    packer.pack(std::string(event_type));
    packer.pack("type");
    packer.pack(std::string(legacy_type));
    packer.pack("model_name");
    packer.pack("model-a");
    packer.pack("block_size");
    packer.pack_int64(16);
    packer.pack("additional_salt");
    packer.pack_nil();
    packer.pack("lora_name");
    packer.pack_nil();
    packer.pack("tenant_id");
    packer.pack("tenant-a");
    packer.pack("backend_id");
    packer.pack("backend-a");
    packer.pack("medium");
    if (medium.empty()) {
        packer.pack_nil();
    } else {
        packer.pack(std::string(medium));
    }
    packer.pack("dp_rank");
    packer.pack(kMooncakeDpRank);
}

void PackMooncakeObject(Packer& packer, std::string_view object_key) {
    packer.pack("group_id");
    packer.pack("0");
    packer.pack("object_key");
    packer.pack(std::string(object_key));
    packer.pack("connector_block_hash");
    packer.pack(
        "000102030405060708090a0b0c0d0e0f"
        "101112131415161718191a1b1c1d1e1f");
    packer.pack("cache_prefix");
    packer.pack("prefix-a");
    packer.pack("tp_rank");
    packer.pack_int64(1);
    packer.pack("head_or_tp_rank");
    packer.pack_int64(1);
    packer.pack("pcp_rank");
    packer.pack_int64(0);
    packer.pack("dcp_rank");
    packer.pack_int64(0);
    packer.pack("pp_rank");
    packer.pack_int64(3);
    packer.pack("layer_id");
    packer.pack_int64(31);
    packer.pack("seq_hashes");
    packer.pack_array(1);
    packer.pack_uint64(kProjectedHash);
    packer.pack("block_hashes");
    packer.pack_array(1);
    packer.pack_uint64(kProjectedHash);
}

void PackMooncakeStored(Packer& packer, bool include_unknown = false) {
    // Default publisher configuration emits legacy compatibility fields and
    // all connector metadata represented by this deterministic context.
    packer.pack_map(include_unknown ? 29 : 28);
    PackMooncakeCommon(packer, 7, "stored", "BlockStored", "cpu");
    PackMooncakeObject(packer, "object-a");
    packer.pack("base_block_idx");
    packer.pack_nil();
    packer.pack("parent_hash");
    packer.pack_nil();
    packer.pack("token_ids");
    packer.pack_nil();
    packer.pack("parent_block_hash");
    packer.pack_nil();
    if (include_unknown) {
        packer.pack("future_connector_metadata");
        packer.pack_true();
    }
}

void PackMooncakeRemoved(Packer& packer) {
    packer.pack_map(25);
    PackMooncakeCommon(packer, 8, "removed", "BlockRemoved", "disk");
    PackMooncakeObject(packer, "object-a");
    packer.pack("base_block_idx");
    packer.pack_nil();
}

void PackMooncakeCleared(Packer& packer, bool include_unknown = false) {
    packer.pack_map(include_unknown ? 13 : 12);
    PackMooncakeCommon(packer, 9, "cleared", "AllBlocksCleared", "");
    if (include_unknown) {
        packer.pack("future_clear_metadata");
        packer.pack("ignored");
    }
}

template <typename Event>
const Event* GetEvent(
    const conductor::zmq::DecodedEvent<conductor::zmq::VllmEvent>& decoded) {
    if (!decoded.event.has_value()) {
        return nullptr;
    }
    return std::get_if<Event>(&*decoded.event);
}

template <typename Event>
const Event* GetEvent(
    const conductor::zmq::DecodedEvent<conductor::zmq::MooncakeEvent>&
        decoded) {
    if (!decoded.event.has_value()) {
        return nullptr;
    }
    return std::get_if<Event>(&*decoded.event);
}

void ExpectMooncakeCommon(const MooncakeEventFields& fields, uint64_t event_id,
                          std::string_view medium) {
    EXPECT_EQ(fields.event_id, event_id);
    EXPECT_EQ(fields.timestamp_milliseconds, kMooncakeTimestamp);
    ASSERT_TRUE(fields.model_name.has_value());
    EXPECT_EQ(*fields.model_name, "model-a");
    ASSERT_TRUE(fields.block_size.has_value());
    EXPECT_EQ(*fields.block_size, 16);
    EXPECT_FALSE(fields.additional_salt.has_value());
    EXPECT_FALSE(fields.lora_name.has_value());
    EXPECT_EQ(fields.tenant_id, "tenant-a");
    EXPECT_EQ(fields.backend_id, "backend-a");
    if (medium.empty()) {
        EXPECT_FALSE(fields.medium.has_value());
    } else {
        ASSERT_TRUE(fields.medium.has_value());
        EXPECT_EQ(*fields.medium, medium);
    }
    EXPECT_EQ(fields.data_parallel_rank, kMooncakeDpRank);
}

void ExpectErrorContains(const std::string& error, std::string_view expected) {
    EXPECT_NE(error.find(expected), std::string::npos) << error;
}

TEST(DecodeVllmEventBatch, DecodesCanonicalMsgspecProducerFixture) {
    // Captured from vLLM's msgspec encoder in distributed/kv_events.py.
    const std::string payload = BytesFromHex(
        "93cb3ff40000000000009389a474797065ab426c6f636b53746f726564ac626c"
        "6f636b5f686173686573912ab1706172656e745f626c6f636b5f68617368c0a9"
        "746f6b656e5f696473920102aa626c6f636b5f73697a6502a76c6f72615f6964"
        "c0a66d656469756da3475055a96c6f72615f6e616d65c0a967726f75705f6964"
        "780084a474797065ac426c6f636b52656d6f766564ac626c6f636b5f68617368"
        "6573912aa66d656469756da3475055a967726f75705f6964780081a474797065"
        "b0416c6c426c6f636b73436c656172656403");

    const auto result = DecodeVllmEventBatch(payload.data(), payload.size());
    ASSERT_TRUE(result.ok) << result.error;
    EXPECT_DOUBLE_EQ(result.batch.timestamp_seconds, 1.25);
    ASSERT_TRUE(result.batch.data_parallel_rank.has_value());
    EXPECT_EQ(*result.batch.data_parallel_rank, 3);
    ASSERT_EQ(result.batch.events.size(), 3u);

    const auto* stored = GetEvent<VllmStoredEvent>(result.batch.events[0]);
    ASSERT_NE(stored, nullptr) << result.batch.events[0].error;
    ASSERT_EQ(stored->block_hashes.size(), 1u);
    ASSERT_TRUE(std::holds_alternative<uint64_t>(stored->block_hashes[0]));
    EXPECT_EQ(std::get<uint64_t>(stored->block_hashes[0]), 42u);
    EXPECT_FALSE(stored->parent_block_hash.has_value());
    ASSERT_TRUE(stored->token_ids.has_value());
    EXPECT_EQ(*stored->token_ids, (std::vector<int32_t>{1, 2}));
    EXPECT_EQ(stored->block_size, 2);
    EXPECT_FALSE(stored->lora_id.has_value());
    ASSERT_TRUE(stored->medium.has_value());
    EXPECT_EQ(*stored->medium, "GPU");
    EXPECT_FALSE(stored->lora_name.has_value());
    ASSERT_TRUE(stored->group_idx.has_value());
    EXPECT_EQ(*stored->group_idx, 0);

    const auto* removed = GetEvent<VllmRemovedEvent>(result.batch.events[1]);
    ASSERT_NE(removed, nullptr) << result.batch.events[1].error;
    ASSERT_EQ(removed->block_hashes.size(), 1u);
    EXPECT_EQ(std::get<uint64_t>(removed->block_hashes[0]), 42u);
    ASSERT_TRUE(removed->medium.has_value());
    EXPECT_EQ(*removed->medium, "GPU");
    ASSERT_TRUE(removed->group_idx.has_value());
    EXPECT_EQ(*removed->group_idx, 0);

    EXPECT_NE(GetEvent<VllmClearedEvent>(result.batch.events[2]), nullptr)
        << result.batch.events[2].error;
}

TEST(DecodeVllmEventBatch, PreservesIntegerAndBinaryHashesAndNullableFields) {
    std::vector<uint8_t> full_hash(32);
    for (size_t index = 0; index < full_hash.size(); ++index) {
        full_hash[index] = static_cast<uint8_t>(index);
    }
    const std::string payload = PackVllmBatch(
        1,
        [&](Packer& packer) {
            packer.pack_map(13);
            packer.pack("type");
            packer.pack("BlockStored");
            packer.pack("block_hashes");
            packer.pack_array(2);
            packer.pack_uint64(std::numeric_limits<uint64_t>::max());
            PackBinary(packer, full_hash);
            packer.pack("parent_block_hash");
            PackBinary(packer, full_hash);
            packer.pack("token_ids");
            packer.pack_nil();
            packer.pack("block_size");
            packer.pack_int64(16);
            packer.pack("lora_id");
            packer.pack_nil();
            packer.pack("medium");
            packer.pack_nil();
            packer.pack("lora_name");
            packer.pack_nil();
            packer.pack("extra_keys");
            packer.pack_array(2);
            packer.pack_nil();
            packer.pack_array(1);
            packer.pack("future-key");
            packer.pack("group_idx");
            packer.pack_nil();
            packer.pack("kv_cache_spec_kind");
            packer.pack("full");
            packer.pack("kv_cache_spec_sliding_window");
            packer.pack_nil();
            packer.pack("future_store_metadata");
            packer.pack_true();
        },
        std::nullopt);

    const auto result = DecodeVllmEventBatch(payload.data(), payload.size());
    ASSERT_TRUE(result.ok) << result.error;
    EXPECT_FALSE(result.batch.data_parallel_rank.has_value());
    ASSERT_EQ(result.batch.events.size(), 1u);
    const auto* stored = GetEvent<VllmStoredEvent>(result.batch.events[0]);
    ASSERT_NE(stored, nullptr) << result.batch.events[0].error;
    ASSERT_EQ(stored->block_hashes.size(), 2u);
    EXPECT_EQ(std::get<uint64_t>(stored->block_hashes[0]),
              std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(std::get<std::vector<uint8_t>>(stored->block_hashes[1]),
              full_hash);
    ASSERT_TRUE(stored->parent_block_hash.has_value());
    EXPECT_EQ(std::get<std::vector<uint8_t>>(*stored->parent_block_hash),
              full_hash);
    EXPECT_FALSE(stored->token_ids.has_value());
    EXPECT_FALSE(stored->medium.has_value());
    EXPECT_TRUE(stored->extra_keys_present);
    EXPECT_FALSE(stored->group_idx.has_value());
    ASSERT_TRUE(stored->kv_cache_spec_kind.has_value());
    EXPECT_EQ(*stored->kv_cache_spec_kind, "full");
    EXPECT_FALSE(stored->kv_cache_spec_sliding_window.has_value());
}

TEST(DecodeMooncakeEventBatch, DecodesDeterministicPublisherMapFixture) {
    const std::string payload = PackMooncakeBatch(3, [](Packer& packer) {
        PackMooncakeStored(packer);
        PackMooncakeRemoved(packer);
        PackMooncakeCleared(packer);
    });

    const auto result =
        DecodeMooncakeEventBatch(payload.data(), payload.size());
    ASSERT_TRUE(result.ok) << result.error;
    EXPECT_EQ(result.batch.timestamp_milliseconds, kMooncakeTimestamp);
    ASSERT_TRUE(result.batch.data_parallel_rank.has_value());
    EXPECT_EQ(*result.batch.data_parallel_rank, kMooncakeDpRank);
    ASSERT_EQ(result.batch.events.size(), 3u);

    const auto* stored = GetEvent<MooncakeStoredEvent>(result.batch.events[0]);
    ASSERT_NE(stored, nullptr) << result.batch.events[0].error;
    ExpectMooncakeCommon(stored->fields, 7, "cpu");
    ASSERT_TRUE(stored->object.group_id.has_value());
    EXPECT_EQ(*stored->object.group_id, "0");
    ASSERT_TRUE(stored->object.object_key.has_value());
    EXPECT_EQ(*stored->object.object_key, "object-a");
    ASSERT_TRUE(stored->object.connector_block_hash.has_value());
    EXPECT_EQ(*stored->object.connector_block_hash,
              "000102030405060708090a0b0c0d0e0f"
              "101112131415161718191a1b1c1d1e1f");
    ASSERT_TRUE(stored->object.cache_prefix.has_value());
    EXPECT_EQ(*stored->object.cache_prefix, "prefix-a");
    EXPECT_EQ(stored->object.seq_hashes,
              (std::vector<uint64_t>{kProjectedHash}));
    ASSERT_TRUE(stored->object.legacy_block_hashes.has_value());
    EXPECT_EQ(*stored->object.legacy_block_hashes, stored->object.seq_hashes);
    ASSERT_TRUE(stored->object.tp_rank.has_value());
    EXPECT_EQ(*stored->object.tp_rank, 1);
    ASSERT_TRUE(stored->object.head_or_tp_rank.has_value());
    EXPECT_EQ(*stored->object.head_or_tp_rank, 1);
    ASSERT_TRUE(stored->object.pcp_rank.has_value());
    EXPECT_EQ(*stored->object.pcp_rank, 0);
    ASSERT_TRUE(stored->object.dcp_rank.has_value());
    EXPECT_EQ(*stored->object.dcp_rank, 0);
    ASSERT_TRUE(stored->object.pp_rank.has_value());
    EXPECT_EQ(*stored->object.pp_rank, 3);
    ASSERT_TRUE(stored->object.layer_id.has_value());
    EXPECT_EQ(*stored->object.layer_id, 31);
    EXPECT_FALSE(stored->object.base_block_idx.has_value());
    EXPECT_FALSE(stored->parent_hash.has_value());
    EXPECT_FALSE(stored->token_ids.has_value());

    const auto* removed =
        GetEvent<MooncakeRemovedEvent>(result.batch.events[1]);
    ASSERT_NE(removed, nullptr) << result.batch.events[1].error;
    ExpectMooncakeCommon(removed->fields, 8, "disk");
    ASSERT_TRUE(removed->object.object_key.has_value());
    EXPECT_EQ(*removed->object.object_key, "object-a");
    EXPECT_EQ(removed->object.seq_hashes,
              (std::vector<uint64_t>{kProjectedHash}));
    EXPECT_FALSE(removed->object.base_block_idx.has_value());

    const auto* cleared =
        GetEvent<MooncakeClearedEvent>(result.batch.events[2]);
    ASSERT_NE(cleared, nullptr) << result.batch.events[2].error;
    ExpectMooncakeCommon(cleared->fields, 9, "");
}

TEST(DecodeMooncakeEventBatch, AcceptsUnknownKeysAndNullableBatchDpRank) {
    const std::string payload = PackMooncakeBatch(
        2,
        [](Packer& packer) {
            PackMooncakeStored(packer, true);
            PackMooncakeCleared(packer, true);
        },
        std::nullopt);

    const auto result =
        DecodeMooncakeEventBatch(payload.data(), payload.size());
    ASSERT_TRUE(result.ok) << result.error;
    EXPECT_FALSE(result.batch.data_parallel_rank.has_value());
    ASSERT_EQ(result.batch.events.size(), 2u);
    EXPECT_NE(GetEvent<MooncakeStoredEvent>(result.batch.events[0]), nullptr)
        << result.batch.events[0].error;
    EXPECT_NE(GetEvent<MooncakeClearedEvent>(result.batch.events[1]), nullptr)
        << result.batch.events[1].error;
}

TEST(DecodeVllmEventBatch, RejectsMalformedEnvelopeMetadata) {
    struct Case {
        std::string name;
        std::string payload;
        std::string error;
    };
    std::vector<Case> cases;

    {
        std::stringstream buffer;
        Packer packer(buffer);
        packer.pack_map(0);
        cases.push_back(
            {"map root", buffer.str(), "three-element array envelope"});
    }
    {
        std::stringstream buffer;
        Packer packer(buffer);
        packer.pack_array(2);
        packer.pack_double(1.25);
        packer.pack_array(0);
        cases.push_back({"two elements", buffer.str(), "three-element"});
    }
    {
        std::stringstream buffer;
        Packer packer(buffer);
        packer.pack_array(4);
        packer.pack_double(1.25);
        packer.pack_array(0);
        packer.pack_nil();
        packer.pack_nil();
        cases.push_back({"four elements", buffer.str(), "three-element"});
    }
    {
        std::stringstream buffer;
        Packer packer(buffer);
        packer.pack_array(3);
        packer.pack_int64(1);
        packer.pack_array(0);
        packer.pack_nil();
        cases.push_back(
            {"integer timestamp", buffer.str(), "timestamp must be a float"});
    }
    {
        std::stringstream buffer;
        Packer packer(buffer);
        packer.pack_array(3);
        packer.pack_double(std::numeric_limits<double>::infinity());
        packer.pack_array(0);
        packer.pack_nil();
        cases.push_back(
            {"non-finite timestamp", buffer.str(), "must be finite"});
    }
    {
        std::stringstream buffer;
        Packer packer(buffer);
        packer.pack_array(3);
        packer.pack_double(1.25);
        packer.pack("not-events");
        packer.pack_nil();
        cases.push_back(
            {"events not array", buffer.str(), "events must be an array"});
    }
    {
        std::stringstream buffer;
        Packer packer(buffer);
        packer.pack_array(3);
        packer.pack_double(1.25);
        packer.pack_array(0);
        packer.pack_int64(-1);
        cases.push_back({"negative DP", buffer.str(), "non-negative or nil"});
    }
    {
        std::stringstream buffer;
        Packer packer(buffer);
        packer.pack_array(3);
        packer.pack_double(1.25);
        packer.pack_array(0);
        packer.pack("rank");
        cases.push_back({"string DP", buffer.str(), "data_parallel_rank"});
    }

    for (const auto& test : cases) {
        SCOPED_TRACE(test.name);
        const auto result =
            DecodeVllmEventBatch(test.payload.data(), test.payload.size());
        EXPECT_FALSE(result.ok);
        ExpectErrorContains(result.error, test.error);
        EXPECT_TRUE(result.batch.events.empty());
    }
}

TEST(DecodeMooncakeEventBatch, RejectsMalformedEnvelopeMetadata) {
    struct Case {
        std::string name;
        std::string payload;
        std::string error;
    };
    std::vector<Case> cases;

    {
        std::stringstream buffer;
        Packer packer(buffer);
        packer.pack("not-an-envelope");
        cases.push_back(
            {"string root", buffer.str(), "three-element array envelope"});
    }
    {
        std::stringstream buffer;
        Packer packer(buffer);
        packer.pack_array(2);
        packer.pack(kMooncakeTimestamp);
        packer.pack_array(0);
        cases.push_back({"two elements", buffer.str(), "three-element"});
    }
    {
        std::stringstream buffer;
        Packer packer(buffer);
        packer.pack_array(4);
        packer.pack(kMooncakeTimestamp);
        packer.pack_array(0);
        packer.pack_nil();
        packer.pack_nil();
        cases.push_back({"four elements", buffer.str(), "three-element"});
    }
    {
        std::stringstream buffer;
        Packer packer(buffer);
        packer.pack_array(3);
        packer.pack_double(1.25);
        packer.pack_array(0);
        packer.pack_nil();
        cases.push_back({"float timestamp", buffer.str(),
                         "timestamp must be a non-negative integer"});
    }
    {
        std::stringstream buffer;
        Packer packer(buffer);
        packer.pack_array(3);
        packer.pack_int64(-1);
        packer.pack_array(0);
        packer.pack_nil();
        cases.push_back({"negative timestamp", buffer.str(),
                         "timestamp must be a non-negative integer"});
    }
    {
        std::stringstream buffer;
        Packer packer(buffer);
        packer.pack_array(3);
        packer.pack(kMooncakeTimestamp);
        packer.pack_map(0);
        packer.pack_nil();
        cases.push_back(
            {"events not array", buffer.str(), "events must be an array"});
    }
    {
        std::stringstream buffer;
        Packer packer(buffer);
        packer.pack_array(3);
        packer.pack(kMooncakeTimestamp);
        packer.pack_array(0);
        packer.pack_int64(-1);
        cases.push_back({"negative DP", buffer.str(), "non-negative or nil"});
    }
    {
        std::stringstream buffer;
        Packer packer(buffer);
        packer.pack_array(3);
        packer.pack(kMooncakeTimestamp);
        packer.pack_array(0);
        packer.pack_false();
        cases.push_back({"boolean DP", buffer.str(), "data_parallel_rank"});
    }

    for (const auto& test : cases) {
        SCOPED_TRACE(test.name);
        const auto result =
            DecodeMooncakeEventBatch(test.payload.data(), test.payload.size());
        EXPECT_FALSE(result.ok);
        ExpectErrorContains(result.error, test.error);
        EXPECT_TRUE(result.batch.events.empty());
    }
}

TEST(DecodeVllmEventBatch,
     RejectsPositionalDuplicateMissingWrongTypeAndUnknownTagLocally) {
    const std::string payload = PackVllmBatch(6, [](Packer& packer) {
        packer.pack_array(1);
        packer.pack("BlockStored");

        packer.pack_map(2);
        packer.pack("type");
        packer.pack("BlockStored");
        packer.pack("type");
        packer.pack("BlockStored");

        packer.pack_map(1);
        packer.pack("unrecognized_type");
        packer.pack("BlockStored");

        packer.pack_map(1);
        packer.pack("type");
        packer.pack_int64(7);

        packer.pack_map(1);
        packer.pack("type");
        packer.pack("BlockUpdated");

        PackVllmCleared(packer);
    });

    const auto result = DecodeVllmEventBatch(payload.data(), payload.size());
    ASSERT_TRUE(result.ok) << result.error;
    ASSERT_EQ(result.batch.events.size(), 6u);
    const std::array<std::string_view, 5> errors = {
        "expected event map", "duplicate recognized key: type",
        "missing required key: type", "invalid type: expected string",
        "unknown vLLM event tag"};
    for (size_t index = 0; index < errors.size(); ++index) {
        SCOPED_TRACE(index);
        EXPECT_FALSE(result.batch.events[index].ok());
        ExpectErrorContains(result.batch.events[index].error, errors[index]);
    }
    EXPECT_NE(GetEvent<VllmClearedEvent>(result.batch.events[5]), nullptr)
        << result.batch.events[5].error;
}

TEST(DecodeVllmEventBatch,
     RejectsStoreOnlyRecognizedFieldsOnRemovedAndAllowsUnknownKeys) {
    constexpr std::array<std::string_view, 8> kStoreOnlyFields = {
        "parent_block_hash",  "token_ids",
        "block_size",         "lora_id",
        "lora_name",          "extra_keys",
        "kv_cache_spec_kind", "kv_cache_spec_sliding_window",
    };
    const std::string payload =
        PackVllmBatch(kStoreOnlyFields.size() + 1, [&](Packer& packer) {
            for (std::string_view field : kStoreOnlyFields) {
                packer.pack_map(5);
                packer.pack("type");
                packer.pack("BlockRemoved");
                packer.pack("block_hashes");
                packer.pack_array(1);
                packer.pack_uint64(42);
                packer.pack("medium");
                packer.pack("GPU");
                packer.pack("group_idx");
                packer.pack_int64(0);
                packer.pack(std::string(field));
                packer.pack_nil();
            }

            packer.pack_map(5);
            packer.pack("type");
            packer.pack("BlockRemoved");
            packer.pack("block_hashes");
            packer.pack_array(1);
            packer.pack_uint64(42);
            packer.pack("medium");
            packer.pack("GPU");
            packer.pack("group_idx");
            packer.pack_int64(0);
            packer.pack("future_remove_metadata");
            packer.pack_map(1);
            packer.pack("version");
            packer.pack_int64(2);
        });

    const auto result = DecodeVllmEventBatch(payload.data(), payload.size());
    ASSERT_TRUE(result.ok) << result.error;
    ASSERT_EQ(result.batch.events.size(), kStoreOnlyFields.size() + 1);
    for (size_t index = 0; index < kStoreOnlyFields.size(); ++index) {
        SCOPED_TRACE(index);
        EXPECT_FALSE(result.batch.events[index].ok());
        ExpectErrorContains(result.batch.events[index].error,
                            "BlockRemoved contains recognized key: " +
                                std::string(kStoreOnlyFields[index]));
    }
    EXPECT_NE(GetEvent<VllmRemovedEvent>(result.batch.events.back()), nullptr)
        << result.batch.events.back().error;
}

TEST(DecodeVllmEventBatch,
     RejectsInvalidExtraKeysEntriesAndCardinalityLocally) {
    const std::string payload = PackVllmBatch(3, [](Packer& packer) {
        const auto pack_stored_prefix = [&](uint32_t block_count) {
            packer.pack_map(9);
            packer.pack("type");
            packer.pack("BlockStored");
            packer.pack("block_hashes");
            packer.pack_array(block_count);
            for (uint32_t index = 0; index < block_count; ++index) {
                packer.pack_uint64(42 + index);
            }
            packer.pack("parent_block_hash");
            packer.pack_nil();
            packer.pack("token_ids");
            packer.pack_nil();
            packer.pack("block_size");
            packer.pack_int64(16);
            packer.pack("lora_id");
            packer.pack_nil();
            packer.pack("medium");
            packer.pack("GPU");
            packer.pack("lora_name");
            packer.pack_nil();
            packer.pack("extra_keys");
        };

        pack_stored_prefix(1);
        packer.pack_array(1);
        packer.pack("not-an-extra-key-tuple");

        pack_stored_prefix(2);
        packer.pack_array(1);
        packer.pack_nil();

        PackVllmCleared(packer);
    });

    const auto result = DecodeVllmEventBatch(payload.data(), payload.size());
    ASSERT_TRUE(result.ok) << result.error;
    ASSERT_EQ(result.batch.events.size(), 3u);
    EXPECT_FALSE(result.batch.events[0].ok());
    ExpectErrorContains(result.batch.events[0].error,
                        "invalid extra_keys: element 0: expected array or nil");
    EXPECT_FALSE(result.batch.events[1].ok());
    ExpectErrorContains(
        result.batch.events[1].error,
        "invalid extra_keys: expected one entry per block hash");
    EXPECT_NE(GetEvent<VllmClearedEvent>(result.batch.events[2]), nullptr)
        << result.batch.events[2].error;
}

TEST(DecodeMooncakeEventBatch,
     RejectsPositionalDuplicateMissingWrongTypeAndUnknownTagLocally) {
    const std::string payload = PackMooncakeBatch(6, [](Packer& packer) {
        packer.pack_array(1);
        packer.pack("BlockStoreEvent");

        packer.pack_map(2);
        packer.pack("event_type");
        packer.pack("stored");
        packer.pack("event_type");
        packer.pack("stored");

        packer.pack_map(1);
        packer.pack("unrecognized_event_type");
        packer.pack("stored");

        packer.pack_map(1);
        packer.pack("event_type");
        packer.pack_int64(7);

        packer.pack_map(1);
        packer.pack("event_type");
        packer.pack("updated");

        PackMooncakeCleared(packer);
    });

    const auto result =
        DecodeMooncakeEventBatch(payload.data(), payload.size());
    ASSERT_TRUE(result.ok) << result.error;
    ASSERT_EQ(result.batch.events.size(), 6u);
    const std::array<std::string_view, 5> errors = {
        "expected event map", "duplicate recognized key: event_type",
        "missing required key: event_type",
        "invalid event_type: expected string", "unknown Mooncake event tag"};
    for (size_t index = 0; index < errors.size(); ++index) {
        SCOPED_TRACE(index);
        EXPECT_FALSE(result.batch.events[index].ok());
        ExpectErrorContains(result.batch.events[index].error, errors[index]);
    }
    EXPECT_NE(GetEvent<MooncakeClearedEvent>(result.batch.events[5]), nullptr)
        << result.batch.events[5].error;
}

TEST(DecodeVllmEventBatch, MalformedMiddleAndFinalEventsRemainIsolated) {
    const std::string middle = PackVllmBatch(3, [](Packer& packer) {
        PackVllmStored(packer, true);
        packer.pack_map(4);
        packer.pack("type");
        packer.pack("BlockRemoved");
        packer.pack("block_hashes");
        packer.pack_array(1);
        packer.pack_uint64(42);
        packer.pack("medium");
        packer.pack("GPU");
        packer.pack("medium");
        packer.pack("CPU");
        PackVllmRemoved(packer);
    });
    const auto middle_result =
        DecodeVllmEventBatch(middle.data(), middle.size());
    ASSERT_TRUE(middle_result.ok) << middle_result.error;
    ASSERT_EQ(middle_result.batch.events.size(), 3u);
    EXPECT_NE(GetEvent<VllmStoredEvent>(middle_result.batch.events[0]), nullptr)
        << middle_result.batch.events[0].error;
    EXPECT_FALSE(middle_result.batch.events[1].ok());
    ExpectErrorContains(middle_result.batch.events[1].error,
                        "duplicate recognized key: medium");
    EXPECT_NE(GetEvent<VllmRemovedEvent>(middle_result.batch.events[2]),
              nullptr)
        << middle_result.batch.events[2].error;

    const std::string final = PackVllmBatch(2, [](Packer& packer) {
        PackVllmStored(packer);
        packer.pack_map(2);
        packer.pack("type");
        packer.pack("BlockRemoved");
        packer.pack("block_hashes");
        packer.pack_array(1);
        packer.pack_uint64(42);
    });
    const auto final_result = DecodeVllmEventBatch(final.data(), final.size());
    ASSERT_TRUE(final_result.ok) << final_result.error;
    ASSERT_EQ(final_result.batch.events.size(), 2u);
    EXPECT_NE(GetEvent<VllmStoredEvent>(final_result.batch.events[0]), nullptr)
        << final_result.batch.events[0].error;
    EXPECT_FALSE(final_result.batch.events[1].ok());
    ExpectErrorContains(final_result.batch.events[1].error,
                        "missing required key: medium");
}

TEST(DecodeMooncakeEventBatch, MalformedMiddleAndFinalEventsRemainIsolated) {
    const std::string middle = PackMooncakeBatch(3, [](Packer& packer) {
        PackMooncakeStored(packer, true);
        packer.pack_map(2);
        packer.pack("event_type");
        packer.pack("removed");
        packer.pack("event_type");
        packer.pack("removed");
        PackMooncakeRemoved(packer);
    });
    const auto middle_result =
        DecodeMooncakeEventBatch(middle.data(), middle.size());
    ASSERT_TRUE(middle_result.ok) << middle_result.error;
    ASSERT_EQ(middle_result.batch.events.size(), 3u);
    EXPECT_NE(GetEvent<MooncakeStoredEvent>(middle_result.batch.events[0]),
              nullptr)
        << middle_result.batch.events[0].error;
    EXPECT_FALSE(middle_result.batch.events[1].ok());
    ExpectErrorContains(middle_result.batch.events[1].error,
                        "duplicate recognized key: event_type");
    EXPECT_NE(GetEvent<MooncakeRemovedEvent>(middle_result.batch.events[2]),
              nullptr)
        << middle_result.batch.events[2].error;

    const std::string final = PackMooncakeBatch(2, [](Packer& packer) {
        PackMooncakeStored(packer);
        packer.pack_map(1);
        packer.pack("event_type");
        packer.pack("stored");
    });
    const auto final_result =
        DecodeMooncakeEventBatch(final.data(), final.size());
    ASSERT_TRUE(final_result.ok) << final_result.error;
    ASSERT_EQ(final_result.batch.events.size(), 2u);
    EXPECT_NE(GetEvent<MooncakeStoredEvent>(final_result.batch.events[0]),
              nullptr)
        << final_result.batch.events[0].error;
    EXPECT_FALSE(final_result.batch.events[1].ok());
    ExpectErrorContains(final_result.batch.events[1].error,
                        "missing required key: event_id");
}

TEST(DecodeVllmEventBatch, RejectsWrongRecognizedHashTypeLocally) {
    const std::string payload = PackVllmBatch(2, [](Packer& packer) {
        packer.pack_map(8);
        packer.pack("type");
        packer.pack("BlockStored");
        packer.pack("block_hashes");
        packer.pack_array(1);
        packer.pack_int64(-1);
        packer.pack("parent_block_hash");
        packer.pack_nil();
        packer.pack("token_ids");
        packer.pack_nil();
        packer.pack("block_size");
        packer.pack_int64(16);
        packer.pack("lora_id");
        packer.pack_nil();
        packer.pack("medium");
        packer.pack("GPU");
        packer.pack("lora_name");
        packer.pack_nil();
        PackVllmCleared(packer);
    });

    const auto result = DecodeVllmEventBatch(payload.data(), payload.size());
    ASSERT_TRUE(result.ok) << result.error;
    ASSERT_EQ(result.batch.events.size(), 2u);
    EXPECT_FALSE(result.batch.events[0].ok());
    ExpectErrorContains(result.batch.events[0].error,
                        "expected unsigned integer or binary hash");
    EXPECT_NE(GetEvent<VllmClearedEvent>(result.batch.events[1]), nullptr)
        << result.batch.events[1].error;
}

TEST(DecodeMooncakeEventBatch, RejectsWrongRecognizedFieldTypeLocally) {
    const std::string payload = PackMooncakeBatch(2, [](Packer& packer) {
        packer.pack_map(12);
        packer.pack("event_id");
        packer.pack_uint64(9);
        packer.pack("timestamp");
        packer.pack(kMooncakeTimestamp);
        packer.pack("event_type");
        packer.pack("cleared");
        packer.pack("type");
        packer.pack("AllBlocksCleared");
        packer.pack("model_name");
        packer.pack("model-a");
        packer.pack("block_size");
        packer.pack_int64(16);
        packer.pack("additional_salt");
        packer.pack_nil();
        packer.pack("lora_name");
        packer.pack_nil();
        packer.pack("tenant_id");
        packer.pack("tenant-a");
        packer.pack("backend_id");
        packer.pack("backend-a");
        packer.pack("medium");
        packer.pack_int64(7);
        packer.pack("dp_rank");
        packer.pack(kMooncakeDpRank);
        PackMooncakeCleared(packer);
    });

    const auto result =
        DecodeMooncakeEventBatch(payload.data(), payload.size());
    ASSERT_TRUE(result.ok) << result.error;
    ASSERT_EQ(result.batch.events.size(), 2u);
    EXPECT_FALSE(result.batch.events[0].ok());
    ExpectErrorContains(result.batch.events[0].error,
                        "invalid medium: expected string");
    EXPECT_NE(GetEvent<MooncakeClearedEvent>(result.batch.events[1]), nullptr)
        << result.batch.events[1].error;
}

TEST(MessagePackEnvelope, RejectsEmptyGarbageAndTrailingBytes) {
    const auto empty_vllm = DecodeVllmEventBatch(nullptr, 0);
    EXPECT_FALSE(empty_vllm.ok);
    ExpectErrorContains(empty_vllm.error, "empty payload");

    const char garbage[] = "\xc1not-msgpack";
    const auto garbage_mooncake =
        DecodeMooncakeEventBatch(garbage, sizeof(garbage) - 1);
    EXPECT_FALSE(garbage_mooncake.ok);
    ExpectErrorContains(garbage_mooncake.error,
                        "failed to decode Mooncake envelope");

    std::string trailing = PackVllmBatch(0, [](Packer&) {});
    trailing.push_back('\0');
    const auto trailing_vllm =
        DecodeVllmEventBatch(trailing.data(), trailing.size());
    EXPECT_FALSE(trailing_vllm.ok);
    ExpectErrorContains(trailing_vllm.error, "trailing bytes");
}

}  // namespace
