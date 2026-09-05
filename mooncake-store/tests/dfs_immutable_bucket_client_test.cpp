#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "client_service.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/posix_fs_adapter.h"
#include "test_server_helpers.h"
#include "utils.h"

namespace mooncake::test {

namespace {

class FailingBucketFsAdapter : public PosixFsAdapter {
   public:
    int WriteCalls() const { return write_calls_.load(); }
    void FailWriteCall(int call) { fail_write_call_.store(call); }
    void FailAllWrites(bool fail) { fail_all_writes_.store(fail); }

    tl::expected<size_t, ErrorCode> WriteAt(int fd, const iovec* iov,
                                            int iovcnt,
                                            int64_t offset) override {
        const int call = ++write_calls_;
        if (fail_all_writes_.load() || call == fail_write_call_.load()) {
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        return PosixFsAdapter::WriteAt(fd, iov, iovcnt, offset);
    }

   private:
    std::atomic<int> write_calls_{0};
    std::atomic<int> fail_write_call_{-1};
    std::atomic<bool> fail_all_writes_{false};
};

}  // namespace

class DfsImmutableBucketClientTest : public ::testing::Test {
   protected:
    void SetUp() override {
        root_ = (std::filesystem::temp_directory_path() /
                 ("dfs_immutable_bucket_client_" +
                  std::to_string(::getpid()) + "_" +
                  std::to_string(++next_root_)))
                    .string();
        std::filesystem::create_directories(root_);

        SetEnv("MOONCAKE_ENABLE_DFS", "1");
        SetEnv("MOONCAKE_DFS_FS_ADAPTER", "posix");
        SetEnv("MOONCAKE_DFS_ROOT_DIR", root_);
        SetEnv("MOONCAKE_DFS_ALLOCATOR_TYPE", "bucket");
        SetEnv("MOONCAKE_DFS_BUCKET_CAPACITY", "1048576");
        SetEnv("MOONCAKE_DFS_MAX_BUCKET_COUNT", "16");
        SetEnv("MOONCAKE_DFS_ALIGNMENT", "4096");
        SetEnv("MOONCAKE_DFS_EVICTION_ENABLED", "0");
        SetEnv("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
        SetEnv("MOONCAKE_DFS_SINGLE_TENANT", "true");
        SetEnv("MOONCAKE_DFS_SHARD_COUNT", "1");
        SetEnv("MOONCAKE_DFS_SHARD_CAPACITY", "1048576");

        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
        writer_ = CreateClient("127.0.0.1:18201");
        ASSERT_NE(writer_, nullptr);

        segment_size_ = 16 * 1024 * 1024;
        segment_ = allocate_buffer_allocator_memory(segment_size_);
        ASSERT_NE(segment_, nullptr);
        ASSERT_TRUE(writer_->MountSegment(segment_, segment_size_, "tcp")
                        .has_value());

        FileStorageConfig file_config;
        file_config.storage_backend_type = StorageBackendType::kDistributed;
        file_config.storage_filepath = root_;

        distributed_config_.fsdir = root_;
        distributed_config_.fs_adapter_type = "posix";
        distributed_config_.allocator_type = DfsAllocatorType::BUCKET;
        distributed_config_.bucket_capacity = 1024 * 1024;
        distributed_config_.max_bucket_count = 16;
        distributed_config_.alignment = 4096;
        distributed_config_.shard_count = 1;
        distributed_config_.shard_capacity = 1024 * 1024;

        auto adapter = std::make_unique<FailingBucketFsAdapter>();
        adapter_ = adapter.get();
        backend_ = std::make_shared<DistributedStorageBackend>(
            file_config, distributed_config_, std::move(adapter));
        ASSERT_TRUE(backend_->Init().has_value());
        writer_->SetDfsStorageBackend(backend_);
    }

    void TearDown() override {
        if (writer_ != nullptr && segment_ != nullptr) {
            (void)writer_->UnmountSegment(segment_, segment_size_);
        }
        writer_.reset();
        backend_.reset();
        master_.Stop();
        if (segment_ != nullptr) {
            std::free(segment_);
            segment_ = nullptr;
        }
        RestoreEnv();
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }

    std::shared_ptr<Client> CreateClient(const std::string& hostname) {
        auto client = Client::Create(hostname, "P2PHANDSHAKE", "tcp",
                                     std::nullopt, master_.master_address());
        return client ? *client : nullptr;
    }

    ReplicateConfig DfsConfig() const {
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        return config;
    }

    tl::expected<QueryResult, ErrorCode> QueryDfsOnly(const std::string& key) {
        auto query = writer_->Query(key);
        if (!query) return tl::make_unexpected(query.error());
        std::vector<Replica::Descriptor> replicas;
        for (const auto& replica : query->replicas) {
            if (replica.is_dfs_replica()) replicas.push_back(replica);
        }
        if (replicas.empty()) {
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        }
        return QueryResult(std::move(replicas), query->lease_timeout,
                           query->object_checksum);
    }

    bool WaitForDfsReplica(const std::string& key) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            auto query = QueryDfsOnly(key);
            if (query.has_value() &&
                std::any_of(query->replicas.begin(), query->replicas.end(),
                            [](const Replica::Descriptor& replica) {
                                return replica.status ==
                                       ReplicaStatus::COMPLETE;
                            })) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    bool WaitForDfsReplicaGone(const std::string& key) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            auto query = writer_->Query(key);
            if (!query.has_value()) {
                if (query.error() == ErrorCode::OBJECT_NOT_FOUND) return true;
            } else {
                const bool has_dfs = std::any_of(
                    query->replicas.begin(), query->replicas.end(),
                    [](const Replica::Descriptor& replica) {
                        return replica.is_dfs_replica();
                    });
                if (!has_dfs) return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    void ExpectDfsValue(const std::string& key, const std::string& expected) {
        auto query = QueryDfsOnly(key);
        ASSERT_TRUE(query.has_value());
        std::vector<char> output(expected.size());
        auto results = backend_->BatchRead(
            {{key, query->replicas[0].get_dfs_descriptor(),
              {{output.data(), output.size()}}}});
        ASSERT_EQ(results.size(), 1u);
        ASSERT_TRUE(results[0].has_value());
        EXPECT_EQ(std::memcmp(output.data(), expected.data(), expected.size()),
                  0);
    }

    static std::vector<std::vector<Slice>> MakeSlices(
        std::vector<std::string>& values) {
        std::vector<std::vector<Slice>> slices;
        slices.reserve(values.size());
        for (auto& value : values) {
            slices.push_back({Slice{value.data(), value.size()}});
        }
        return slices;
    }

    void SetEnv(const std::string& key, const std::string& value) {
        const char* old_value = ::getenv(key.c_str());
        saved_env_.push_back({key, old_value
                                      ? std::optional<std::string>(old_value)
                                      : std::nullopt});
        ::setenv(key.c_str(), value.c_str(), 1);
    }

    void RestoreEnv() {
        for (auto it = saved_env_.rbegin(); it != saved_env_.rend(); ++it) {
            if (it->second.has_value()) {
                ::setenv(it->first.c_str(), it->second->c_str(), 1);
            } else {
                ::unsetenv(it->first.c_str());
            }
        }
        saved_env_.clear();
    }

    inline static std::atomic<int> next_root_{0};
    testing::InProcMaster master_;
    std::shared_ptr<Client> writer_;
    std::shared_ptr<DistributedStorageBackend> backend_;
    DistributedStorageConfig distributed_config_;
    FailingBucketFsAdapter* adapter_ = nullptr;
    void* segment_ = nullptr;
    size_t segment_size_ = 0;
    std::string root_;
    std::vector<std::pair<std::string, std::optional<std::string>>> saved_env_;
};

TEST_F(DfsImmutableBucketClientTest, BatchPutIsContiguousAndReadable) {
    std::vector<std::string> keys{"bucket_batch_0", "bucket_batch_1",
                                  "bucket_batch_2", "bucket_batch_3"};
    std::vector<std::string> values;
    for (size_t i = 0; i < keys.size(); ++i) {
        values.push_back(std::string(2048, static_cast<char>('a' + i)));
    }
    auto slices = MakeSlices(values);

    auto results = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(results.size(), keys.size());
    for (const auto& result : results) ASSERT_TRUE(result.has_value());

    std::vector<DistributedFSDescriptor> descriptors;
    for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_TRUE(WaitForDfsReplica(keys[i])) << keys[i];
        ExpectDfsValue(keys[i], values[i]);
        auto query = QueryDfsOnly(keys[i]);
        descriptors.push_back(query->replicas[0].get_dfs_descriptor());
    }

    for (size_t i = 1; i < descriptors.size(); ++i) {
        EXPECT_EQ(descriptors[i].shard_idx, descriptors[0].shard_idx);
        const uint64_t previous_start =
            descriptors[i - 1].offset - 8 - keys[i - 1].size();
        EXPECT_EQ(descriptors[i].offset - 8 - keys[i].size(),
                  previous_start + descriptors[i - 1].aligned_size);
    }
}

TEST_F(DfsImmutableBucketClientTest,
       AsyncWriteOwnsCallerBufferUntilCompletion) {
    const std::string key = "bucket_buffer_lifetime";
    const std::string expected(8192, 'L');

    {
        auto buffer = std::make_unique<char[]>(expected.size());
        std::memcpy(buffer.get(), expected.data(), expected.size());
        std::vector<std::vector<Slice>> slices{
            {Slice{buffer.get(), expected.size()}}};
        auto results = writer_->BatchPut({key}, slices, DfsConfig());
        ASSERT_EQ(results.size(), 1u);
        ASSERT_TRUE(results[0].has_value());
        std::memset(buffer.get(), 'X', expected.size());
    }

    ASSERT_TRUE(WaitForDfsReplica(key));
    ExpectDfsValue(key, expected);
}

TEST_F(DfsImmutableBucketClientTest, FailedAsyncWriteRevokesReplica) {
    adapter_->FailAllWrites(true);
    std::vector<std::string> values{std::string(4096, 'F')};
    auto slices = MakeSlices(values);
    auto results =
        writer_->BatchPut({"bucket_write_fail"}, slices, DfsConfig());
    ASSERT_EQ(results.size(), 1u);
    ASSERT_TRUE(results[0].has_value());
    ASSERT_TRUE(WaitForDfsReplicaGone("bucket_write_fail"));
}

TEST_F(DfsImmutableBucketClientTest, MergedWriteFailureRevokesWholeRun) {
    std::vector<std::string> keys{"bucket_run_0", "bucket_run_1"};
    std::vector<std::string> values{std::string(4096, 'G'),
                                    std::string(4096, 'H')};
    auto slices = MakeSlices(values);
    adapter_->FailWriteCall(adapter_->WriteCalls() + 1);

    auto results = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(results.size(), keys.size());
    ASSERT_TRUE(results[0].has_value());
    ASSERT_TRUE(results[1].has_value());
    EXPECT_TRUE(WaitForDfsReplicaGone(keys[0]));
    EXPECT_TRUE(WaitForDfsReplicaGone(keys[1]));
}

TEST_F(DfsImmutableBucketClientTest, BatchGetUsesBucketDescriptors) {
    std::vector<std::string> keys{"bucket_get_0", "bucket_get_1"};
    std::vector<std::string> values{std::string(4096, 'N'),
                                    std::string(4096, 'O')};
    auto write_slices = MakeSlices(values);
    auto put_results = writer_->BatchPut(keys, write_slices, DfsConfig());
    ASSERT_EQ(put_results.size(), keys.size());
    for (const auto& result : put_results) ASSERT_TRUE(result.has_value());

    std::vector<QueryResult> queries;
    for (const auto& key : keys) {
        ASSERT_TRUE(WaitForDfsReplica(key));
        auto query = QueryDfsOnly(key);
        ASSERT_TRUE(query.has_value());
        queries.push_back(*query);
    }

    std::vector<std::string> output{std::string(4096, '\0'),
                                    std::string(4096, '\0')};
    std::unordered_map<std::string, std::vector<Slice>> read_slices;
    for (size_t i = 0; i < keys.size(); ++i) {
        read_slices[keys[i]] = {{output[i].data(), output[i].size()}};
    }
    auto get_results = writer_->BatchGet(keys, queries, read_slices);
    ASSERT_EQ(get_results.size(), keys.size());
    for (size_t i = 0; i < get_results.size(); ++i) {
        ASSERT_TRUE(get_results[i].has_value()) << keys[i];
        EXPECT_EQ(output[i], values[i]) << keys[i];
    }
}

TEST_F(DfsImmutableBucketClientTest, ClientDestructionDrainsAsyncWrites) {
    const std::string key = "bucket_drain";
    std::string value(65536, 'W');
    auto client = CreateClient("127.0.0.1:18202");
    ASSERT_NE(client, nullptr);
    client->SetDfsStorageBackend(backend_);

    std::vector<std::vector<Slice>> slices{
        {Slice{value.data(), value.size()}}};
    auto results = client->BatchPut({key}, slices, DfsConfig());
    ASSERT_EQ(results.size(), 1u);
    ASSERT_TRUE(results[0].has_value());
    client.reset();

    ASSERT_TRUE(WaitForDfsReplica(key));
    ExpectDfsValue(key, value);
}

TEST_F(DfsImmutableBucketClientTest, ConcurrentBatchPutsRemainReadable) {
    constexpr int kThreadCount = 4;
    constexpr int kKeysPerThread = 3;
    std::vector<std::thread> threads;
    std::vector<std::vector<std::string>> all_keys(kThreadCount);
    std::vector<std::vector<std::string>> all_values(kThreadCount);
    std::atomic<int> failures{0};

    for (int thread_index = 0; thread_index < kThreadCount; ++thread_index) {
        threads.emplace_back([&, thread_index]() {
            auto& keys = all_keys[thread_index];
            auto& values = all_values[thread_index];
            for (int i = 0; i < kKeysPerThread; ++i) {
                keys.push_back("concurrent_" + std::to_string(thread_index) +
                               "_" + std::to_string(i));
                values.push_back(std::string(
                    1024, static_cast<char>('a' + thread_index *
                                                     kKeysPerThread + i)));
            }
            auto slices = MakeSlices(values);
            auto results = writer_->BatchPut(keys, slices, DfsConfig());
            if (results.size() != keys.size()) {
                ++failures;
                return;
            }
            for (const auto& result : results) {
                if (!result.has_value()) ++failures;
            }
        });
    }
    for (auto& thread : threads) thread.join();
    ASSERT_EQ(failures.load(), 0);

    for (int t = 0; t < kThreadCount; ++t) {
        for (size_t i = 0; i < all_keys[t].size(); ++i) {
            ASSERT_TRUE(WaitForDfsReplica(all_keys[t][i]));
            ExpectDfsValue(all_keys[t][i], all_values[t][i]);
        }
    }
}

}  // namespace mooncake::test
