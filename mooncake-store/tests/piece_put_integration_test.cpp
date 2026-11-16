#include "client_service.h"
#include "../src/config/client_object_checksum_config.h"
#include "common/client_buffer_allocation.h"
#include "master_client.h"
#include "test_server_helpers.h"

#include <algorithm>
#include <array>
#include <atomic>
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

#include <gtest/gtest.h>

namespace mooncake::test {
namespace {

constexpr size_t kMiB = 1024 * 1024;
constexpr uint64_t kSeed = 20261116;

class PiecePutIntegrationTest : public ::testing::Test {
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

    static char Byte(size_t rank, size_t key_index, size_t byte) {
        return static_cast<char>(
            1 + (kSeed + rank * 17 + key_index * 7 + byte * 13) % 251);
    }

    static std::vector<std::string> Keys(std::string_view prefix,
                                         size_t count) {
        std::vector<std::string> keys;
        for (size_t i = 0; i < count; ++i)
            keys.push_back(std::string(prefix) + "/" + std::to_string(i));
        return keys;
    }

    static std::vector<PiecePutItem> Items(size_t rank,
                                           const std::vector<std::string>& keys,
                                           size_t total_size,
                                           size_t producer_count,
                                           const std::string& write_id) {
        size_t offset = 0;
        for (size_t i = 0; i < rank; ++i)
            offset +=
                total_size / producer_count + (i < total_size % producer_count);
        const size_t length =
            total_size / producer_count + (rank < total_size % producer_count);
        std::vector<PiecePutItem> items;
        for (size_t i = 0; i < keys.size(); ++i) {
            auto* data = sources_[rank].bytes.data() + i * length;
            for (size_t byte = 0; byte < length; ++byte)
                data[byte] = Byte(rank, i, byte);
            const size_t split = length / 3;
            items.push_back({keys[i],
                             total_size,
                             offset,
                             write_id,
                             {{data, split}, {data + split, length - split}}});
        }
        return items;
    }

    static ReplicateConfig Replication(size_t count = 2) {
        ReplicateConfig config;
        config.replica_num = count;
        return config;
    }

    static void CheckCompleted(
        const std::vector<tl::expected<PiecePutItemResult, ErrorCode>>& results,
        size_t expected_count) {
        ASSERT_EQ(results.size(), expected_count);
        for (size_t i = 0; i < results.size(); ++i) {
            ASSERT_TRUE(results[i])
                << "item " << i << ": " << results[i].error();
            EXPECT_TRUE(results[i]->piece_completed);
            EXPECT_FALSE(results[i]->pending_commit.has_value());
        }
    }

    static void ExpectHidden(const std::vector<std::string>& keys) {
        for (const auto& key : keys) {
            const auto exists = reader_->IsExist(key);
            ASSERT_TRUE(exists) << exists.error();
            EXPECT_FALSE(*exists) << key;
            EXPECT_FALSE(reader_->Query(key)) << key;
        }
    }

    static void Verify(const std::vector<std::string>& keys, size_t total_size,
                       size_t producer_count, size_t replica_count = 2,
                       size_t first_key_index = 0) {
        std::vector<char> expected(total_size);
        for (size_t key_index = 0; key_index < keys.size(); ++key_index) {
            size_t offset = 0;
            for (size_t rank = 0; rank < producer_count; ++rank) {
                const size_t length = total_size / producer_count +
                                      (rank < total_size % producer_count);
                for (size_t byte = 0; byte < length; ++byte)
                    expected[offset + byte] =
                        Byte(rank, first_key_index + key_index, byte);
                offset += length;
            }
            auto query = reader_->Query(keys[key_index]);
            ASSERT_TRUE(query) << query.error();
            ASSERT_EQ(query->replicas.size(), replica_count);
            // Check each fixed replica, rather than accepting one good copy.
            for (const auto& replica : query->replicas) {
                ASSERT_TRUE(replica.is_memory_replica());
                EXPECT_EQ(replica.status, ReplicaStatus::COMPLETE);
                std::memset(read_buffer_.data(), 0, total_size);
                const auto read = reader_->BatchTransferReadRanges(
                    {replica}, {{{read_buffer_.data(), total_size}}}, {{0}});
                ASSERT_EQ(read.size(), 1);
                ASSERT_TRUE(read[0]) << read[0].error();
                EXPECT_EQ(*read[0], static_cast<int64_t>(total_size));
                EXPECT_EQ(std::memcmp(expected.data(), read_buffer_.data(),
                                      total_size),
                          0)
                    << "key " << keys[key_index] << " replica " << replica.id;
            }
            std::vector<Slice> slices{{read_buffer_.data(), total_size}};
            std::memset(read_buffer_.data(), 0, total_size);
            ASSERT_TRUE(reader_->Get(keys[key_index], slices));
            EXPECT_EQ(
                std::memcmp(expected.data(), read_buffer_.data(), total_size),
                0);
        }
    }

    static testing::InProcMaster master_;
    static std::array<Target, 2> targets_;
    static std::array<Source, 4> sources_;
    static std::vector<char> read_buffer_;
    static std::shared_ptr<Client> reader_;
};

testing::InProcMaster PiecePutIntegrationTest::master_;
std::array<PiecePutIntegrationTest::Target, 2>
    PiecePutIntegrationTest::targets_;
std::array<PiecePutIntegrationTest::Source, 4>
    PiecePutIntegrationTest::sources_;
std::vector<char> PiecePutIntegrationTest::read_buffer_;
std::shared_ptr<Client> PiecePutIntegrationTest::reader_;

TEST_F(PiecePutIntegrationTest,
       OutOfOrderMultiKeyScatterPublishesBothReplicas) {
    const auto keys = Keys("piece/out-of-order", 11);
    constexpr size_t bytes = 65539;
    for (size_t rank : {2, 0}) {
        const auto items = Items(rank, keys, bytes, 3, "request:out-of-order");
        const auto results =
            sources_[rank].client->BatchPiecePut(items, Replication());
        CheckCompleted(results, keys.size());
        for (const auto& result : results)
            if (result) EXPECT_FALSE(result->object_published);
        ExpectHidden(keys);
    }
    const auto final_items = Items(1, keys, bytes, 3, "request:out-of-order");
    const auto results =
        sources_[1].client->BatchPiecePut(final_items, Replication());
    CheckCompleted(results, keys.size());
    for (const auto& result : results)
        if (result) EXPECT_TRUE(result->object_published);
    Verify(keys, bytes, 3);
}

TEST_F(PiecePutIntegrationTest, BatchSpansInternalWindowsAndKeepsPerKeyHints) {
    const auto keys = Keys("piece/windows", 130);
    auto config = Replication();
    config.group_ids = std::vector<std::string>{};
    config.session_ids = std::vector<std::string>{};
    for (size_t i = 0; i < keys.size(); ++i) {
        config.group_ids->push_back("group/" + std::to_string(i));
        config.session_ids->push_back("session/" + std::to_string(i));
    }
    for (size_t rank = 0; rank < 3; ++rank) {
        auto items = Items(rank, keys, 4096, 3, "request:windows");
        auto rank_config = config;
        if (rank == 2) {
            // Reorder item descriptors and their hints together, preserving
            // each item's source bytes. A window-local hint index would now
            // conflict with the configuration admitted by earlier producers.
            std::reverse(items.begin(), items.end());
            std::reverse(rank_config.group_ids->begin(),
                         rank_config.group_ids->end());
            std::reverse(rank_config.session_ids->begin(),
                         rank_config.session_ids->end());
        }
        CheckCompleted(sources_[rank].client->BatchPiecePut(items, rank_config),
                       keys.size());
    }
    Verify(keys, 4096, 3);
}

TEST_F(PiecePutIntegrationTest, InvalidItemDoesNotDiscardValidBatchItems) {
    const auto keys = Keys("piece/invalid-item", 3);
    for (size_t rank = 0; rank < 3; ++rank) {
        auto items = Items(rank, keys, 8192, 3, "request:invalid-item");
        items[1].slices[0].ptr = nullptr;
        const auto results =
            sources_[rank].client->BatchPiecePut(items, Replication());
        ASSERT_EQ(results.size(), 3);
        EXPECT_TRUE(results[0]);
        EXPECT_FALSE(results[1]);
        EXPECT_TRUE(results[2]);
        if (!results[1]) {
            EXPECT_EQ(results[1].error(), ErrorCode::INVALID_PARAMS);
        }
    }
    ExpectHidden({keys[1]});
    // Preserve the original key index when constructing the expected bytes.
    auto queries = reader_->BatchQuery({keys[0], keys[2]});
    ASSERT_EQ(queries.size(), 2);
    EXPECT_TRUE(queries[0]);
    EXPECT_TRUE(queries[1]);
    Verify({keys[0]}, 8192, 3);
    Verify({keys[2]}, 8192, 3, 2, 2);
}

TEST_F(PiecePutIntegrationTest, ConcurrentProducersAndOrdinaryPutGetProgress) {
    const auto keys = Keys("piece/concurrent", 32);
    std::array<std::vector<PiecePutItem>, 4> items;
    std::array<std::vector<tl::expected<PiecePutItemResult, ErrorCode>>, 4>
        results;
    for (size_t rank = 0; rank < 4; ++rank)
        items[rank] = Items(rank, keys, 128 * 1024, 4, "request:concurrent");
    auto ordinary = NewClient();
    ASSERT_NE(ordinary, nullptr);
    std::vector<char> ordinary_buffer(8192, 'x');
    ASSERT_TRUE(ordinary->RegisterLocalMemory(
        ordinary_buffer.data(), ordinary_buffer.size(), "cpu:0", false, false));
    std::latch start(1);
    std::atomic<size_t> ordinary_completed{0};
    std::atomic<bool> ordinary_failed{false};
    std::vector<std::thread> workers;
    for (size_t rank = 0; rank < 4; ++rank)
        workers.emplace_back([&, rank] {
            start.wait();
            results[rank] = sources_[rank].client->BatchPiecePut(items[rank],
                                                                 Replication());
        });
    workers.emplace_back([&] {
        start.wait();
        for (size_t i = 0; i < 20; ++i) {
            const auto key = "ordinary/concurrent/" + std::to_string(i);
            std::vector<Slice> source{{ordinary_buffer.data(), 4096}};
            std::vector<Slice> destination{
                {ordinary_buffer.data() + 4096, 4096}};
            std::fill(ordinary_buffer.begin() + 4096, ordinary_buffer.end(), 0);
            if (!ordinary->Put(key, source, Replication(1)) ||
                !ordinary->Get(key, destination) ||
                std::memcmp(ordinary_buffer.data(),
                            ordinary_buffer.data() + 4096, 4096) != 0) {
                ordinary_failed.store(true);
                break;
            }
            ordinary_completed.fetch_add(1);
        }
    });
    start.count_down();
    for (auto& worker : workers) worker.join();
    for (const auto& rank_results : results)
        CheckCompleted(rank_results, keys.size());
    EXPECT_FALSE(ordinary_failed.load());
    EXPECT_EQ(ordinary_completed.load(), 20);
    Verify(keys, 128 * 1024, 4);
    ordinary.reset();  // Unregister before ordinary_buffer is destroyed.
}

TEST_F(PiecePutIntegrationTest,
       OriginalMetadataRetryAndLostFinishConfirmation) {
    auto& source = sources_[0];
    MasterClient control(source.client->getClientId());
    ASSERT_EQ(control.Connect(master_.master_address()), ErrorCode::OK);
    const std::string key = "piece/metadata-retry";
    auto items = Items(0, {key}, 4096, 1, "request:metadata-retry");
    PiecePutStartRequest request;
    request.key = key;
    request.total_size = 4096;
    request.length = 4096;
    request.write_id = items[0].write_id;
    request.op_id = 100000;
    request.config = Replication();
    auto initial = control.BatchPiecePutStart({request});
    ASSERT_EQ(initial.size(), 1);
    ASSERT_TRUE(initial[0]) << initial[0].error();
    // Simulate the caller losing a successful reply. This is application-level
    // reply discard, not packet loss or transport fault injection.
    const auto original_handle = initial[0]->handle;
    auto retry = control.BatchPiecePutStart({request});
    ASSERT_EQ(retry.size(), 1);
    ASSERT_TRUE(retry[0]) << retry[0].error();
    ASSERT_EQ(retry[0]->replicas.size(), initial[0]->replicas.size());
    for (size_t i = 0; i < retry[0]->replicas.size(); ++i)
        EXPECT_EQ(retry[0]->replicas[i].id, initial[0]->replicas[i].id);
    EXPECT_EQ(retry[0]->handle.op_id, original_handle.op_id);
    auto write = source.client->BatchTransferWriteRanges(
        {retry[0]->replicas}, {items[0].slices},
        {{0, items[0].slices[0].size}});
    ASSERT_EQ(write.size(), 1);
    ASSERT_TRUE(write[0]) << write[0].error();
    auto finished = control.BatchPiecePutFinish({{original_handle, true}});
    ASSERT_EQ(finished.size(), 1);
    ASSERT_TRUE(finished[0]) << finished[0].error();
    EXPECT_TRUE(finished[0]->object_published);
    // The server has published; the caller still only retains the handle.
    auto committed = source.client->CommitPiecePut(
        std::span<const PiecePutCommitHandle>(&original_handle, 1));
    ASSERT_EQ(committed.size(), 1);
    ASSERT_TRUE(committed[0]) << committed[0].error();
    EXPECT_TRUE(committed[0]->piece_completed);
    EXPECT_TRUE(committed[0]->object_published);
    EXPECT_FALSE(committed[0]->pending_commit.has_value());
    Verify({key}, 4096, 1);
}

TEST_F(PiecePutIntegrationTest, MissingProducerStaysHiddenUntilExplicitUpsert) {
    const auto keys = Keys("piece/missing-producer", 4);
    for (size_t rank = 0; rank < 3; ++rank) {
        auto items = Items(rank, keys, 32768, 4, "request:missing-producer");
        CheckCompleted(
            sources_[rank].client->BatchPiecePut(items, Replication()),
            keys.size());
    }
    ExpectHidden(keys);
    // Force removal correctly refuses PROCESSING objects. The ordinary Upsert
    // API explicitly preempts them; no new PiecePut implicitly steals a round.
    // Timeout recovery uses a short timeout in the Master tests instead.
    std::vector<Slice> replacement{{sources_[3].bytes.data(), 32768}};
    for (const auto& key : keys) {
        ASSERT_TRUE(
            sources_[3].client->Upsert(key, replacement, Replication()));
        ASSERT_TRUE(reader_->Remove(key, true));
    }
    for (size_t rank = 0; rank < 4; ++rank) {
        auto items = Items(rank, keys, 32768, 4, "request:recompute");
        CheckCompleted(
            sources_[rank].client->BatchPiecePut(items, Replication()),
            keys.size());
    }
    Verify(keys, 32768, 4);
}

TEST_F(PiecePutIntegrationTest, TargetUnmountAbortsWholeIncompleteRound) {
    const auto keys = Keys("piece/target-unmount", 1);
    auto first = Items(0, keys, 8192, 2, "request:target-unmount");
    CheckCompleted(sources_[0].client->BatchPiecePut(first, Replication()), 1);
    ExpectHidden(keys);
    ASSERT_TRUE(
        targets_[0].client->UnmountSegment(targets_[0].bytes.get(), 64 * kMiB));
    auto second = Items(1, keys, 8192, 2, "request:target-unmount");
    const auto result =
        sources_[1].client->BatchPiecePut(second, Replication());
    ASSERT_EQ(result.size(), 1);
    EXPECT_FALSE(result[0]);
    ExpectHidden(keys);
    ASSERT_TRUE(targets_[0].client->MountSegment(targets_[0].bytes.get(),
                                                 64 * kMiB, "tcp"));
    ASSERT_TRUE(reader_->RemoveAll(true));
    for (size_t rank = 0; rank < 2; ++rank) {
        auto item = Items(rank, keys, 8192, 2, "request:target-recompute");
        CheckCompleted(
            sources_[rank].client->BatchPiecePut(item, Replication()), 1);
    }
    Verify(keys, 8192, 2);
}

TEST_F(PiecePutIntegrationTest, ReusedKeyInvalidatesProducerLocalHotCache) {
    struct ScopedEnv {
        std::string key;
        std::optional<std::string> previous;
        ScopedEnv(const char* name, const char* value) : key(name) {
            if (const auto* old = std::getenv(name)) previous = old;
            setenv(name, value, 1);
        }
        ~ScopedEnv() {
            if (previous)
                setenv(key.c_str(), previous->c_str(), 1);
            else
                unsetenv(key.c_str());
        }
    };
    ScopedEnv cache_size("MC_STORE_LOCAL_HOT_CACHE_SIZE", "32768");
    ScopedEnv block_size("MC_STORE_LOCAL_HOT_BLOCK_SIZE", "4096");
    ScopedEnv admission("MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD", "1");
    ScopedEnv shared_memory("MC_STORE_LOCAL_HOT_CACHE_USE_SHM", "0");
    constexpr size_t bytes = 4096;
    std::vector<char> buffers(3 * bytes, 0);
    std::fill_n(buffers.data(), bytes, 'a');
    std::fill_n(buffers.data() + bytes, bytes, 'b');
    auto producer = NewClient();
    ASSERT_NE(producer, nullptr);
    ASSERT_TRUE(producer->IsHotCacheEnabled());
    ASSERT_TRUE(producer->RegisterLocalMemory(buffers.data(), buffers.size(),
                                              "cpu:0", false, false));
    const auto cache = producer->GetHotCache();
    const std::string key = "piece/reused-local-cache";
    const auto config = Replication(1);
    std::vector<Slice> old_source{{buffers.data(), bytes}};
    std::vector<Slice> destination{{buffers.data() + 2 * bytes, bytes}};
    ASSERT_TRUE(producer->Put(key, old_source, config));
    auto read_expected = [&](char expected) {
        std::fill_n(buffers.data() + 2 * bytes, bytes, 0);
        const auto read = producer->Get(key, destination);
        EXPECT_TRUE(read);
        return read &&
               std::all_of(buffers.begin() + 2 * bytes, buffers.end(),
                           [expected](char byte) { return byte == expected; });
    };
    auto await_cached = [&] {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (!cache->HasHotKey(key) &&
               std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        return cache->HasHotKey(key);
    };
    ASSERT_TRUE(read_expected('a'));
    ASSERT_TRUE(await_cached());
    // Another Client deletes the old object; this producer still has its local
    // cached copy. Reusing the key must invalidate it before the next Get.
    ASSERT_TRUE(reader_->Remove(key, true));
    ASSERT_TRUE(cache->HasHotKey(key));
    PiecePutItem item{
        key, bytes, 0, "cache/attempt1", {{buffers.data() + bytes, bytes}}};
    const auto written = producer->PiecePut(item, config);
    ASSERT_TRUE(written) << written.error();
    EXPECT_TRUE(written->object_published);
    EXPECT_FALSE(cache->HasHotKey(key));
    ASSERT_TRUE(read_expected('b'));
    ASSERT_TRUE(await_cached());

    // Also cover metadata-only confirmation: a lost Finish reply can leave an
    // old local cache entry until this producer calls CommitPiecePut.
    ASSERT_TRUE(reader_->Remove(key, true));
    std::fill_n(buffers.data() + bytes, bytes, 'c');
    MasterClient control(producer->getClientId());
    ASSERT_EQ(control.Connect(master_.master_address()), ErrorCode::OK);
    PiecePutStartRequest request;
    request.key = key;
    request.total_size = bytes;
    request.length = bytes;
    request.write_id = "cache/attempt2";
    request.op_id = 1000000;
    request.config = config;
    const auto start = control.BatchPiecePutStart({request});
    ASSERT_EQ(start.size(), 1);
    ASSERT_TRUE(start[0]) << start[0].error();
    const auto write = producer->BatchTransferWriteRanges(
        {start[0]->replicas}, {{{buffers.data() + bytes, bytes}}}, {{0}});
    ASSERT_EQ(write.size(), 1);
    ASSERT_TRUE(write[0]);
    const auto finish = control.BatchPiecePutFinish({{start[0]->handle, true}});
    ASSERT_EQ(finish.size(), 1);
    ASSERT_TRUE(finish[0]);
    EXPECT_TRUE(cache->HasHotKey(key));
    const auto confirmed = producer->CommitPiecePut(
        std::span<const PiecePutCommitHandle>(&start[0]->handle, 1));
    ASSERT_EQ(confirmed.size(), 1);
    ASSERT_TRUE(confirmed[0]);
    EXPECT_TRUE(confirmed[0]->object_published);
    EXPECT_FALSE(cache->HasHotKey(key));
    EXPECT_TRUE(read_expected('c'));
}

TEST_F(PiecePutIntegrationTest, ChecksumEnabledRejectsBeforeStartingAnObject) {
    if (!ClientObjectChecksumConfig::IsEnabledAtFirstUse())
        GTEST_SKIP() << "Run this case in a fresh process with "
                        "MOONCAKE_STORE_CHECKSUM=1";
    const auto keys = Keys("piece/checksum-rejected", 1);
    const auto items = Items(0, keys, 4096, 1, "checksum/attempt1");
    const auto results =
        sources_[0].client->BatchPiecePut(items, Replication());
    ASSERT_EQ(results.size(), 1);
    ASSERT_FALSE(results[0]);
    EXPECT_EQ(results[0].error(), ErrorCode::NOT_SUPPORTED);
    ExpectHidden(keys);
}

TEST_F(PiecePutIntegrationTest, DisjointSourceBuffersAssembleIntoOneObject) {
    const auto keys = Keys("piece/disjoint-sources", 1);
    constexpr size_t total_size = 65539;
    for (size_t rank = 0; rank < 2; ++rank) {
        auto items =
            Items(rank, keys, total_size, 2, "request:disjoint-sources");
        auto& second = items[0].slices[1];
        auto* separate_buffer = sources_[rank].bytes.data() + 4 * kMiB;
        std::memcpy(separate_buffer, second.ptr, second.size);
        // A writer that incorrectly treats both slices as contiguous would
        // transfer these sentinels instead of the second source buffer.
        std::memset(second.ptr, 0x5a, second.size);
        second.ptr = separate_buffer;
        CheckCompleted(
            sources_[rank].client->BatchPiecePut(items, Replication()), 1);
        if (rank == 0) ExpectHidden(keys);
    }
    Verify(keys, total_size, 2);
}

}  // namespace
}  // namespace mooncake::test
