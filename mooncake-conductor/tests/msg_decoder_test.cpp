// Tests for the vLLM / Mooncake msgpack event-batch decoders.

#include <gtest/gtest.h>
#include <msgpack.hpp>

#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include "conductor/zmq/msg_decoder.h"

namespace {

using conductor::zmq::BlockRemovedEvent;
using conductor::zmq::BlockStoredEvent;
using conductor::zmq::DecodeMooncakeEventBatch;
using conductor::zmq::DecodeVllmEventBatch;
using conductor::zmq::EventBatchResult;

// --- hand-built payloads (msgpack-cxx packer) ----------------------------

// Mooncake BlockStored decode (field layout per ParseMooncakeBlockStored).
std::string PackMooncakeStoreBatch() {
    std::stringstream buf;
    msgpack::packer<std::stringstream> pk(buf);
    pk.pack_array(2);  // [timestamp, events]
    pk.pack_int64(1700000000);
    pk.pack_array(1);  // events
    pk.pack_array(8);  // BlockStoreEvent
    pk.pack(std::string("BlockStoreEvent"));
    pk.pack(std::string("mooncake-key-123"));
    pk.pack_array(2);  // replica_list
    pk.pack_array(2);
    pk.pack(std::string("replica1"));
    pk.pack(std::string("replica2"));
    pk.pack_array(1);
    pk.pack(std::string("replica3"));
    pk.pack_nil();        // index 3 unused
    pk.pack_int64(1024);  // BlockSize
    pk.pack_array(2);     // BlockHashes
    pk.pack_uint64(100);
    pk.pack_uint64(200);
    pk.pack_uint64(50);  // ParentBlockHash
    pk.pack_array(3);    // TokenIDs
    pk.pack_int32(1);
    pk.pack_int32(2);
    pk.pack_int32(3);
    return buf.str();
}

enum class ParentEncoding { kUnsigned, kNil, kNegative, kFloat, kArray };

// Packs a vLLM BlockStored batch: the schema needs 7-element events
// (medium at index 6) and an integer dp_rank.
std::string PackVllmStoredBatch(
    uint64_t parent, int64_t dp_rank,
    ParentEncoding parent_encoding = ParentEncoding::kUnsigned) {
    std::stringstream buf;
    msgpack::packer<std::stringstream> pk(buf);
    pk.pack_array(3);  // [timestamp, events, dp_rank]
    pk.pack_int64(1700000000);
    pk.pack_array(1);
    pk.pack_array(7);
    pk.pack(std::string("BlockStored"));
    pk.pack_array(2);  // block_hashes
    pk.pack_uint64(100);
    pk.pack_uint64(200);
    switch (parent_encoding) {
        case ParentEncoding::kUnsigned:
            pk.pack_uint64(parent);
            break;
        case ParentEncoding::kNil:
            pk.pack_nil();
            break;
        case ParentEncoding::kNegative:
            pk.pack_int64(-1);
            break;
        case ParentEncoding::kFloat:
            pk.pack_double(1.5);
            break;
        case ParentEncoding::kArray:
            pk.pack_array(0);
            break;
    }
    pk.pack_array(3);  // token_ids
    pk.pack_int32(10000000);
    pk.pack_int32(2);
    pk.pack_int32(3);
    pk.pack_int64(1024);  // block_size
    pk.pack_nil();        // lora_id
    pk.pack(std::string("GPU"));
    pk.pack_int64(dp_rank);
    return buf.str();
}

TEST(DecodeMooncakeEventBatch, ParsesBlockStoreEvent) {
    const std::string data = PackMooncakeStoreBatch();
    const auto result = DecodeMooncakeEventBatch(data.data(), data.size());
    ASSERT_TRUE(result.ok) << result.error;
    EXPECT_EQ(result.batch.source, "mooncake");
    EXPECT_EQ(result.batch.data_parallel_rank, -1);  // 2-element batch
    ASSERT_EQ(result.batch.events.size(), 1u);

    const auto* event = std::get_if<BlockStoredEvent>(&result.batch.events[0]);
    ASSERT_NE(event, nullptr);
    EXPECT_EQ(event->type, "BlockStored");
    EXPECT_EQ(event->mooncake_key, "mooncake-key-123");
    EXPECT_EQ(event->block_size, 1024);
    ASSERT_EQ(event->block_hashes.size(), 2u);
    EXPECT_EQ(event->block_hashes[0], 100u);
    EXPECT_EQ(event->block_hashes[1], 200u);
    EXPECT_EQ(event->parent_block_hash, 50u);
    EXPECT_EQ(event->token_ids, (std::vector<int32_t>{1, 2, 3}));
    ASSERT_EQ(event->replica_list.size(), 2u);
    // BUG (preserved): Medium is never assigned for Mooncake events.
    EXPECT_EQ(event->medium, "");
    // BUG (preserved): the batch timestamp is ignored here.
    EXPECT_EQ(event->timestamp_unix_micro, 0);
}

TEST(DecodeVllmEventBatch, ParsesBlockStored) {
    const std::string data = PackVllmStoredBatch(5000000000ULL, 0);
    const auto result = DecodeVllmEventBatch(data.data(), data.size());
    ASSERT_TRUE(result.ok) << result.error;
    EXPECT_EQ(result.batch.source, "vllm");
    EXPECT_EQ(result.batch.data_parallel_rank, 0);
    ASSERT_EQ(result.batch.events.size(), 1u);

    const auto* event = std::get_if<BlockStoredEvent>(&result.batch.events[0]);
    ASSERT_NE(event, nullptr);
    EXPECT_EQ(event->type, "BlockStored");
    EXPECT_EQ(event->timestamp_unix_micro, 1700000000LL * 1000000);
    ASSERT_EQ(event->block_hashes.size(), 2u);
    EXPECT_EQ(event->block_hashes[0], 100u);
    EXPECT_EQ(event->block_hashes[1], 200u);
    EXPECT_EQ(event->parent_block_hash, 5000000000ULL);
    EXPECT_EQ(event->block_size, 1024);
    ASSERT_EQ(event->token_ids.size(), 3u);
    EXPECT_EQ(event->medium, "GPU");
}

TEST(DecodeVllmEventBatch, AcceptsAllNonNegativeParentHashValues) {
    const uint64_t values[] = {
        0,
        50,
        std::numeric_limits<uint32_t>::max(),
        static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1,
        std::numeric_limits<uint64_t>::max(),
    };
    for (const uint64_t parent : values) {
        const std::string data = PackVllmStoredBatch(parent, 0);
        const auto result = DecodeVllmEventBatch(data.data(), data.size());
        ASSERT_TRUE(result.ok) << "parent=" << parent << " " << result.error;
        ASSERT_EQ(result.batch.events.size(), 1u);
        const auto* event =
            std::get_if<BlockStoredEvent>(&result.batch.events[0]);
        ASSERT_NE(event, nullptr);
        EXPECT_EQ(event->parent_block_hash, parent);
    }
}

TEST(DecodeVllmEventBatch, NilParentHashDecodesAsZero) {
    const std::string data = PackVllmStoredBatch(0, 0, ParentEncoding::kNil);
    const auto result = DecodeVllmEventBatch(data.data(), data.size());
    ASSERT_TRUE(result.ok) << result.error;
    ASSERT_EQ(result.batch.events.size(), 1u);
    const auto* event = std::get_if<BlockStoredEvent>(&result.batch.events[0]);
    ASSERT_NE(event, nullptr);
    EXPECT_EQ(event->parent_block_hash, 0u);
}

TEST(DecodeVllmEventBatch, RejectsInvalidParentHashTypes) {
    const ParentEncoding invalid_encodings[] = {
        ParentEncoding::kNegative,
        ParentEncoding::kFloat,
        ParentEncoding::kArray,
    };
    for (const auto encoding : invalid_encodings) {
        const std::string data = PackVllmStoredBatch(0, 0, encoding);
        const auto result = DecodeVllmEventBatch(data.data(), data.size());
        ASSERT_FALSE(result.ok);
        EXPECT_NE(result.error.find("expected integer"), std::string::npos)
            << result.error;
    }
}

TEST(DecodeMooncakeEventBatch, InvalidArrayLength) {
    // A single-element batch is too short: the mooncake schema needs 2.
    std::stringstream buf;
    msgpack::packer<std::stringstream> pk(buf);
    pk.pack_array(1);
    pk.pack_int64(1700000000);
    const std::string data = buf.str();

    const auto result = DecodeMooncakeEventBatch(data.data(), data.size());
    ASSERT_FALSE(result.ok);
    EXPECT_NE(result.error.find("expected 2-element array"), std::string::npos)
        << result.error;
}

TEST(DecodeVllmEventBatch, InvalidArrayLength) {
    // A single-element batch is too short: the vLLM schema needs 3.
    std::stringstream buf;
    msgpack::packer<std::stringstream> pk(buf);
    pk.pack_array(1);
    pk.pack_int64(1700000000);
    const std::string data = buf.str();

    const auto result = DecodeVllmEventBatch(data.data(), data.size());
    ASSERT_FALSE(result.ok);
    EXPECT_NE(result.error.find("expected 3-element array"), std::string::npos)
        << result.error;
}

TEST(DecodeVllmEventBatch, GarbageBytes) {
    const char garbage[] = "\xc1not-msgpack";
    const auto result = DecodeVllmEventBatch(garbage, sizeof(garbage) - 1);
    ASSERT_FALSE(result.ok);
    EXPECT_NE(result.error.find("failed to decode event batch"),
              std::string::npos)
        << result.error;
}

}  // namespace
