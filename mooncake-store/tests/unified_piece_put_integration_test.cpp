#include "client_service.h"
#include "common/client_buffer_allocation.h"
#include "test_server_helpers.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <latch>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace mooncake::test {
namespace {

using mooncake::testing::InProcMaster;

constexpr size_t kMiB = 1024 * 1024;
constexpr uint64_t kSeed = 20261124;

class UnifiedPiecePutIntegrationTest : public ::testing::Test {
   protected:
    struct Source {
        std::vector<char> bytes;
        std::shared_ptr<Client> client;
    };
    struct Target {
        std::unique_ptr<void, decltype(&std::free)> bytes{nullptr, &std::free};
        std::shared_ptr<Client> client;
    };

    static std::shared_ptr<Client> NewClient() {
        auto client = Client::Create(
            "127.0.0.1:" + std::to_string(getFreeTcpPort()), "P2PHANDSHAKE",
            "tcp", std::nullopt, master_.master_address());
        return client ? *client : nullptr;
    }

    static void SetUpTestSuite() {
        setenv("MC_STORE_MEMCPY", "0", 1);
        setenv("MC_RPC_PROTOCOL", "tcp", 1);
        setenv("MC_TCP_PROTO", "2", 1);
        setenv("MC_TCP_ENABLE_CONNECTION_POOL", "1", 1);
        unsetenv("MC_USE_TENT");
        unsetenv("MC_USE_TEV1");
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder()
                                      .set_http_metadata_port(0)
                                      .set_default_kv_lease_ttl(60000)
                                      .build()));
        for (size_t i = 0; i < 2; ++i) {
            auto& target = targets_[i];
            target.bytes.reset(allocate_buffer_allocator_memory(64 * kMiB));
            ASSERT_NE(target.bytes.get(), nullptr);
            std::memset(target.bytes.get(), 0, 64 * kMiB);
            target.client = NewClient();
            ASSERT_NE(target.client, nullptr);
            ASSERT_TRUE(target.client->MountSegment(target.bytes.get(),
                                                    64 * kMiB, "tcp"));
        }
        for (auto& source : sources_) {
            source.bytes.resize(8 * kMiB);
            source.client = NewClient();
            ASSERT_NE(source.client, nullptr);
            ASSERT_TRUE(source.client->RegisterLocalMemory(
                source.bytes.data(), source.bytes.size(), "cpu:0", false,
                false));
        }
        read_buffer_.resize(kMiB);
        reader_ = NewClient();
        ASSERT_NE(reader_, nullptr);
        ASSERT_TRUE(reader_->RegisterLocalMemory(
            read_buffer_.data(), read_buffer_.size(), "cpu:0", false, false));
    }

    static void TearDownTestSuite() {
        for (auto& source : sources_) {
            source.client.reset();
            source.bytes.clear();
        }
        reader_.reset();
        read_buffer_.clear();
        for (auto& target : targets_) {
            target.client.reset();
            target.bytes.reset();
        }
        master_.Stop();
    }

    void TearDown() override {
        if (reader_) {
            auto removed = reader_->RemoveAll(true);
            EXPECT_TRUE(removed.has_value());
        }
    }

    static char PatternByte(size_t rank, size_t key_index, size_t byte) {
        return static_cast<char>(
            1 + (kSeed + rank * 17 + key_index * 7 + byte * 13) % 251);
    }

    static std::vector<Client::PutItem> MakeProducerItems(
        size_t rank, const std::vector<std::string>& keys, size_t total_size,
        size_t producer_count, const std::string& write_id) {
        size_t offset = 0;
        for (size_t i = 0; i < rank; ++i) {
            offset +=
                total_size / producer_count + (i < total_size % producer_count);
        }
        const size_t length =
            total_size / producer_count + (rank < total_size % producer_count);
        std::vector<Client::PutItem> items;
        items.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            auto* data = sources_[rank].bytes.data() + i * length;
            for (size_t b = 0; b < length; ++b) {
                data[b] = PatternByte(rank, i, b);
            }
            const size_t split = length / 3;
            items.push_back(Client::PutItem{
                .key = keys[i],
                .slices = {{data, split}, {data + split, length - split}},
                .target = RangeTarget{.offset = offset,
                                      .total_size = total_size,
                                      .write_id = write_id},
            });
        }
        return items;
    }

    static std::vector<char> ExpectedBytes(size_t key_index, size_t total_size,
                                           size_t producer_count) {
        std::vector<char> expected(total_size);
        size_t offset = 0;
        for (size_t rank = 0; rank < producer_count; ++rank) {
            const size_t length = total_size / producer_count +
                                  (rank < total_size % producer_count);
            for (size_t b = 0; b < length; ++b) {
                expected[offset + b] = PatternByte(rank, key_index, b);
            }
            offset += length;
        }
        return expected;
    }

    static InProcMaster master_;
    static std::array<Target, 2> targets_;
    static std::array<Source, 4> sources_;
    static std::shared_ptr<Client> reader_;
    static std::vector<char> read_buffer_;
};

InProcMaster UnifiedPiecePutIntegrationTest::master_;
std::array<UnifiedPiecePutIntegrationTest::Target, 2>
    UnifiedPiecePutIntegrationTest::targets_;
std::array<UnifiedPiecePutIntegrationTest::Source, 4>
    UnifiedPiecePutIntegrationTest::sources_;
std::shared_ptr<Client> UnifiedPiecePutIntegrationTest::reader_;
std::vector<char> UnifiedPiecePutIntegrationTest::read_buffer_;

TEST_F(UnifiedPiecePutIntegrationTest,
       ConcurrentProducersAssembleAndReadWholeAndArbitrarySubRanges) {
    constexpr size_t kProducers = 4;
    constexpr size_t kKeys = 8;
    constexpr size_t kTotalSize = 65536 + 13;  // non-power-of-two size
    std::vector<std::string> keys;
    for (size_t i = 0; i < kKeys; ++i) {
        keys.push_back("unified/piece/" + std::to_string(i));
    }

    ReplicateConfig cfg;
    cfg.replica_num = 2;

    std::latch ready(kProducers);
    std::vector<std::thread> threads;
    std::array<std::vector<tl::expected<void, ErrorCode>>, kProducers> outcomes;
    for (size_t rank = 0; rank < kProducers; ++rank) {
        threads.emplace_back([&, rank] {
            auto items = MakeProducerItems(rank, keys, kTotalSize, kProducers,
                                           "write-round-1");
            ready.arrive_and_wait();
            outcomes[rank] = sources_[rank].client->BatchPut(items, cfg);
        });
    }
    for (auto& t : threads) {
        t.join();
    }

    for (size_t rank = 0; rank < kProducers; ++rank) {
        ASSERT_EQ(outcomes[rank].size(), kKeys);
        for (size_t i = 0; i < kKeys; ++i) {
            ASSERT_TRUE(outcomes[rank][i])
                << "rank=" << rank << " key=" << i
                << " err=" << toString(outcomes[rank][i].error());
        }
    }

    // Unified BatchExist verifies all keys became readable.
    auto exists = reader_->BatchExist(keys);
    ASSERT_EQ(exists.size(), kKeys);
    for (const auto& e : exists) {
        ASSERT_TRUE(e);
        EXPECT_TRUE(*e);
    }

    // Verify full-object reads AND cross-producer-boundary range reads via
    // unified Client::BatchGet.
    for (size_t i = 0; i < kKeys; ++i) {
        const auto expected = ExpectedBytes(i, kTotalSize, kProducers);

        std::memset(read_buffer_.data(), 0, kTotalSize);
        Client::GetItem whole_get{
            .key = keys[i],
            .slices = {{read_buffer_.data(), kTotalSize}},
            .target = WholeObject{},
            .src_offset = 0,
        };
        ASSERT_TRUE(reader_->Get(whole_get));
        EXPECT_EQ(std::memcmp(read_buffer_.data(), expected.data(), kTotalSize),
                  0);

        // Arbitrary byte range crossing producer boundaries (e.g. different
        // read parallelism from write parallelism).
        constexpr size_t kSubOffset = 10000;
        constexpr size_t kSubLen = 25000;
        std::memset(read_buffer_.data(), 0, kSubLen);
        Client::GetItem range_get{
            .key = keys[i],
            .slices = {{read_buffer_.data(), kSubLen}},
            .target = WholeObject{},
            .src_offset = kSubOffset,
        };
        ASSERT_TRUE(reader_->Get(range_get));
        EXPECT_EQ(std::memcmp(read_buffer_.data(), expected.data() + kSubOffset,
                              kSubLen),
                  0);
    }

    // One unified BatchGet call can read the same key at multiple distinct
    // byte ranges in parallel (impossible with the legacy unordered_map API).
    constexpr size_t kOffA = 1234;
    constexpr size_t kLenA = 8000;
    constexpr size_t kOffB = 40000;
    constexpr size_t kLenB = 12000;
    std::memset(read_buffer_.data(), 0, kLenA + kLenB);
    std::vector<Client::GetItem> multi_range_same_key{
        {.key = keys[0],
         .slices = {{read_buffer_.data(), kLenA}},
         .target = WholeObject{},
         .src_offset = kOffA},
        {.key = keys[0],
         .slices = {{read_buffer_.data() + kLenA, kLenB}},
         .target = WholeObject{},
         .src_offset = kOffB},
    };
    auto multi_res = reader_->BatchGet(multi_range_same_key);
    ASSERT_EQ(multi_res.size(), 2u);
    ASSERT_TRUE(multi_res[0]);
    ASSERT_TRUE(multi_res[1]);
    const auto expected0 = ExpectedBytes(0, kTotalSize, kProducers);
    EXPECT_EQ(std::memcmp(read_buffer_.data(), expected0.data() + kOffA, kLenA),
              0);
    EXPECT_EQ(std::memcmp(read_buffer_.data() + kLenA, expected0.data() + kOffB,
                          kLenB),
              0);
}

TEST_F(UnifiedPiecePutIntegrationTest,
       BatchPutMixesWholeObjectAndRangeTargetAndSpansWindows) {
    // 70 keys > 64 internal window size, mixing WholeObject and RangeTarget.
    constexpr size_t kCount = 70;
    constexpr size_t kSize = 4096;
    std::vector<Client::PutItem> p0_items;
    std::vector<Client::PutItem> p1_items;
    std::vector<std::string> all_keys;
    p0_items.reserve(kCount);
    all_keys.reserve(kCount);

    for (size_t i = 0; i < kCount; ++i) {
        std::string key = "unified/mixed/" + std::to_string(i);
        all_keys.push_back(key);
        auto* buf0 = sources_[0].bytes.data() + i * kSize;
        std::memset(buf0, static_cast<int>('A' + (i % 26)), kSize);
        if (i % 2 == 0) {
            p0_items.push_back(Client::PutItem{
                .key = key,
                .slices = {{buf0, kSize}},
                .target = WholeObject{},
            });
        } else {
            auto* buf1 = sources_[1].bytes.data() + i * kSize;
            std::memset(buf1, static_cast<int>('a' + (i % 26)), kSize);
            p0_items.push_back(Client::PutItem{
                .key = key,
                .slices = {{buf0, kSize / 2}},
                .target = RangeTarget{.offset = 0,
                                      .total_size = kSize,
                                      .write_id = "mixed-round"},
            });
            p1_items.push_back(Client::PutItem{
                .key = key,
                .slices = {{buf1, kSize / 2}},
                .target = RangeTarget{.offset = kSize / 2,
                                      .total_size = kSize,
                                      .write_id = "mixed-round"},
            });
        }
    }

    auto res0 = sources_[0].client->BatchPut(p0_items);
    ASSERT_EQ(res0.size(), kCount);
    for (const auto& r : res0) {
        ASSERT_TRUE(r);
    }

    // Even keys (WholeObject) are readable; odd keys (incomplete RangeTarget)
    // stay hidden until producer 1 finishes.
    auto mid_exist = reader_->BatchExist(all_keys, {ProbeLeaseMode::None, 1});
    ASSERT_EQ(mid_exist.size(), kCount);
    for (size_t i = 0; i < kCount; ++i) {
        ASSERT_TRUE(mid_exist[i]);
        EXPECT_EQ(*mid_exist[i], (i % 2 == 0));
    }

    auto res1 = sources_[1].client->BatchPut(p1_items);
    for (const auto& r : res1) {
        ASSERT_TRUE(r);
    }

    auto final_exist = reader_->BatchExist(all_keys);
    ASSERT_EQ(final_exist.size(), kCount);
    for (const auto& e : final_exist) {
        ASSERT_TRUE(e);
        EXPECT_TRUE(*e);
    }

    // Spot-check data integrity on an odd (RangeTarget) and even (WholeObject).
    std::memset(read_buffer_.data(), 0, kSize);
    ASSERT_TRUE(reader_->Get(
        {.key = all_keys[69], .slices = {{read_buffer_.data(), kSize}}}));
    for (size_t b = 0; b < kSize / 2; ++b) {
        EXPECT_EQ(read_buffer_[b], static_cast<char>('A' + (69 % 26)));
    }
    for (size_t b = kSize / 2; b < kSize; ++b) {
        EXPECT_EQ(read_buffer_[b], static_cast<char>('a' + (69 % 26)));
    }
}

TEST_F(UnifiedPiecePutIntegrationTest, InvalidBatchItemDoesNotBreakValidPeers) {
    constexpr size_t kSize = 4096;
    auto* buf = sources_[0].bytes.data();
    std::memset(buf, 0x5A, kSize);

    std::vector<Client::PutItem> items{
        // Out-of-bounds range: INVALID_PARAMS
        Client::PutItem{
            .key = "bad/oob",
            .slices = {{buf, kSize}},
            .target =
                RangeTarget{
                    .offset = 2048, .total_size = 4096, .write_id = "w1"},
        },
        // Valid single-piece RangeTarget
        Client::PutItem{
            .key = "good/one",
            .slices = {{buf, kSize}},
            .target =
                RangeTarget{.offset = 0, .total_size = kSize, .write_id = "w1"},
        },
    };
    auto res = sources_[0].client->BatchPut(items);
    ASSERT_EQ(res.size(), 2u);
    ASSERT_FALSE(res[0]);
    EXPECT_EQ(res[0].error(), ErrorCode::INVALID_PARAMS);
    ASSERT_TRUE(res[1]);

    std::memset(read_buffer_.data(), 0, kSize);
    std::vector<Client::GetItem> get_items{
        Client::GetItem{.key = "", .slices = {{read_buffer_.data(), kSize}}},
        Client::GetItem{.key = "good/one",
                        .slices = {{read_buffer_.data(), kSize}}},
        Client::GetItem{.key = "good/one",
                        .slices = {{read_buffer_.data(), kSize}},
                        .target = PartIndex{2}},
    };
    auto get_res = reader_->BatchGet(get_items);
    ASSERT_EQ(get_res.size(), 3u);
    ASSERT_FALSE(get_res[0]);
    EXPECT_EQ(get_res[0].error(), ErrorCode::INVALID_PARAMS);
    ASSERT_TRUE(get_res[1]);
    EXPECT_EQ(std::memcmp(read_buffer_.data(), buf, kSize), 0);
    ASSERT_FALSE(get_res[2]);
    EXPECT_EQ(get_res[2].error(), ErrorCode::INVALID_PARAMS);
}

}  // namespace
}  // namespace mooncake::test
