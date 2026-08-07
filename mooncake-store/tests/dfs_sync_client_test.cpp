#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <unistd.h>

#include "client_service.h"
#include "storage/distributed/dfs_global_allocator.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/posix_fs_adapter.h"
#include "test_server_helpers.h"
#include "utils.h"

namespace mooncake::test {

class FailingPosixFsAdapter : public PosixFsAdapter {
   public:
    int WriteCalls() const { return write_calls_.load(); }
    void FailWriteCall(int call) { fail_write_call_.store(call); }

    tl::expected<size_t, ErrorCode> WriteAt(int fd, const iovec* iov,
                                            int iovcnt,
                                            int64_t offset) override {
        const int call = ++write_calls_;
        if (call == fail_write_call_.load()) {
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        return PosixFsAdapter::WriteAt(fd, iov, iovcnt, offset);
    }

   private:
    std::atomic<int> write_calls_{0};
    std::atomic<int> fail_write_call_{-1};
};

class DfsSyncClientTest : public ::testing::Test {
   protected:
    void SetUp() override {
        root_ = (std::filesystem::temp_directory_path() /
                 ("dfs_sync_client_" + std::to_string(::getpid()) + "_" +
                  std::to_string(++next_root_)))
                    .string();
        std::filesystem::create_directories(root_);

        SetEnv("MOONCAKE_ENABLE_DFS", "1");
        SetEnv("MOONCAKE_DFS_FS_ADAPTER", "posix");
        SetEnv("MOONCAKE_DFS_ROOT_DIR", root_);
        SetEnv("MOONCAKE_DFS_SHARD_COUNT", "2");
        SetEnv("MOONCAKE_DFS_SHARD_CAPACITY", "16777216");
        SetEnv("MOONCAKE_DFS_ALIGNMENT", "4096");
        SetEnv("MOONCAKE_DFS_EVICTION_ENABLED", "0");
        SetEnv("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0");
        SetEnv("MOONCAKE_DFS_SINGLE_TENANT", "true");

        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
        writer_ = CreateClient("127.0.0.1:18101");
        provider_ = CreateClient("127.0.0.1:18102");
        ASSERT_NE(writer_, nullptr);
        ASSERT_NE(provider_, nullptr);

        segment_size_ = 16 * 1024 * 1024;
        segment_ = allocate_buffer_allocator_memory(segment_size_);
        ASSERT_NE(segment_, nullptr);
        ASSERT_TRUE(provider_->MountSegment(segment_, segment_size_, "tcp")
                        .has_value());

        FileStorageConfig file_config;
        file_config.storage_backend_type = StorageBackendType::kDistributed;
        file_config.storage_filepath = root_;

        DistributedStorageConfig distributed_config;
        distributed_config.fsdir = root_;
        distributed_config.fs_adapter_type = "posix";
        distributed_config.shard_count = 2;
        distributed_config.shard_capacity = 16 * 1024 * 1024;
        distributed_config.alignment = 4096;

        auto adapter = std::make_unique<FailingPosixFsAdapter>();
        adapter_ = adapter.get();
        backend_ = std::make_shared<DistributedStorageBackend>(
            file_config, distributed_config, std::move(adapter));
        ASSERT_TRUE(backend_->Init().has_value());
        writer_->SetDfsStorageBackend(backend_);
    }

    void TearDown() override {
        if (provider_ && segment_) {
            (void)provider_->UnmountSegment(segment_, segment_size_);
        }
        writer_.reset();
        provider_.reset();
        backend_.reset();
        master_.Stop();
        if (segment_) {
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
        if (!query) {
            return tl::make_unexpected(query.error());
        }
        std::vector<Replica::Descriptor> replicas;
        for (const auto& replica : query->replicas) {
            if (replica.is_dfs_replica()) {
                replicas.push_back(replica);
            }
        }
        if (replicas.empty()) {
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        }
        return QueryResult(std::move(replicas), query->lease_timeout,
                           query->object_checksum);
    }

    void ExpectDfsValue(const std::string& key, const std::string& expected) {
        auto query = QueryDfsOnly(key);
        ASSERT_TRUE(query.has_value());
        std::vector<char> output(expected.size());
        const auto& descriptor = query->replicas[0].get_dfs_descriptor();
        auto results = backend_->BatchRead(
            {{key, descriptor, {{output.data(), output.size()}}}});
        ASSERT_EQ(results.size(), 1);
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
            if (it->second) {
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
    std::shared_ptr<Client> provider_;
    std::shared_ptr<DistributedStorageBackend> backend_;
    FailingPosixFsAdapter* adapter_ = nullptr;
    void* segment_ = nullptr;
    size_t segment_size_ = 0;
    std::string root_;
    std::vector<std::pair<std::string, std::optional<std::string>>> saved_env_;
};

TEST_F(DfsSyncClientTest, PutAndUpsertReturnAfterDfsWrite) {
    const std::string key = "sync_put_upsert";
    std::string initial(4096, 'A');
    std::vector<Slice> initial_slices{{initial.data(), initial.size()}};
    ASSERT_TRUE(writer_->Put(key, initial_slices, DfsConfig()).has_value());
    ExpectDfsValue(key, initial);

    std::string updated(4096, 'B');
    std::vector<Slice> updated_slices{{updated.data(), updated.size()}};
    ASSERT_TRUE(writer_->Upsert(key, updated_slices, DfsConfig()).has_value());
    ExpectDfsValue(key, updated);
}

TEST_F(DfsSyncClientTest, BatchPutAndBatchUpsertReturnAfterDfsWrite) {
    std::vector<std::string> keys{"sync_batch_0", "sync_batch_1"};
    std::vector<std::string> initial{std::string(4096, 'C'),
                                     std::string(4096, 'D')};
    auto initial_slices = MakeSlices(initial);
    auto put_results = writer_->BatchPut(keys, initial_slices, DfsConfig());
    ASSERT_EQ(put_results.size(), keys.size());
    ASSERT_TRUE(put_results[0].has_value());
    ASSERT_TRUE(put_results[1].has_value());
    ExpectDfsValue(keys[0], initial[0]);
    ExpectDfsValue(keys[1], initial[1]);

    std::vector<std::string> updated{std::string(4096, 'E'),
                                     std::string(4096, 'F')};
    auto updated_slices = MakeSlices(updated);
    auto upsert_results =
        writer_->BatchUpsert(keys, updated_slices, DfsConfig());
    ASSERT_EQ(upsert_results.size(), keys.size());
    ASSERT_TRUE(upsert_results[0].has_value());
    ASSERT_TRUE(upsert_results[1].has_value());
    ExpectDfsValue(keys[0], updated[0]);
    ExpectDfsValue(keys[1], updated[1]);
}

TEST_F(DfsSyncClientTest, PreferSameNodeBatchPutReturnsAfterDfsWrite) {
    std::vector<std::string> keys{"sync_same_node_0", "sync_same_node_1"};
    std::vector<std::string> values{std::string(4096, 'J'),
                                    std::string(4096, 'K')};
    auto slices = MakeSlices(values);
    auto config = DfsConfig();
    config.prefer_alloc_in_same_node = true;

    auto results = writer_->BatchPut(keys, slices, config);
    ASSERT_EQ(results.size(), keys.size());
    ASSERT_TRUE(results[0].has_value());
    ASSERT_TRUE(results[1].has_value());
    ExpectDfsValue(keys[0], values[0]);
    ExpectDfsValue(keys[1], values[1]);
}

TEST_F(DfsSyncClientTest, BatchDfsFailureOnlyRevokesFailedKey) {
    std::vector<std::string> keys{"sync_success", "sync_failure"};
    std::vector<std::string> values{std::string(4096, 'G'),
                                    std::string(4096, 'H')};
    auto slices = MakeSlices(values);
    adapter_->FailWriteCall(adapter_->WriteCalls() + 2);

    auto results = writer_->BatchPut(keys, slices, DfsConfig());
    ASSERT_EQ(results.size(), keys.size());
    EXPECT_TRUE(results[0].has_value());
    ASSERT_FALSE(results[1].has_value());
    EXPECT_EQ(results[1].error(), ErrorCode::FILE_WRITE_FAIL);
    ExpectDfsValue(keys[0], values[0]);

    auto failed_query = writer_->Query(keys[1]);
    ASSERT_FALSE(failed_query.has_value());
    EXPECT_EQ(failed_query.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(DfsSyncClientTest, MissingDfsBackendRevokesObject) {
    auto client_without_backend = CreateClient("127.0.0.1:18103");
    ASSERT_NE(client_without_backend, nullptr);
    std::string value(4096, 'I');
    std::vector<Slice> slices{{value.data(), value.size()}};

    auto result =
        client_without_backend->Put("sync_no_backend", slices, DfsConfig());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::DFS_SERVICE_UNAVAILABLE);
    auto query = client_without_backend->Query("sync_no_backend");
    ASSERT_FALSE(query.has_value());
    EXPECT_EQ(query.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(DfsSyncClientTest, GetReadsDfsIntoMultipleSlices) {
    const std::string key = "dfs_multi_slice_get";
    std::string value(4096, 'M');
    std::vector<Slice> write_slices{{value.data(), value.size()}};
    ASSERT_TRUE(writer_->Put(key, write_slices, DfsConfig()).has_value());

    auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query.has_value());
    std::vector<char> first(1024), second(3072);
    std::vector<Slice> read_slices{{first.data(), first.size()},
                                   {second.data(), second.size()}};
    ASSERT_TRUE(writer_->Get(key, *query, read_slices).has_value());
    EXPECT_EQ(std::memcmp(value.data(), first.data(), first.size()), 0);
    EXPECT_EQ(
        std::memcmp(value.data() + first.size(), second.data(), second.size()),
        0);
}

TEST_F(DfsSyncClientTest, BatchGetUsesExplicitDfsDescriptors) {
    std::vector<std::string> keys{"dfs_batch_read_0", "dfs_batch_read_1"};
    std::vector<std::string> values{std::string(4096, 'N'),
                                    std::string(4096, 'O')};
    auto write_slices = MakeSlices(values);
    auto put_results = writer_->BatchPut(keys, write_slices, DfsConfig());
    ASSERT_EQ(put_results.size(), keys.size());
    ASSERT_TRUE(put_results[0].has_value());
    ASSERT_TRUE(put_results[1].has_value());

    std::vector<QueryResult> queries;
    queries.reserve(keys.size());
    for (const auto& key : keys) {
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
    ASSERT_TRUE(get_results[0].has_value());
    ASSERT_TRUE(get_results[1].has_value());
    EXPECT_EQ(output[0], values[0]);
    EXPECT_EQ(output[1], values[1]);
}

TEST_F(DfsSyncClientTest, BatchGetVerifiesDfsChecksum) {
    const char* checksum_enabled = std::getenv("MOONCAKE_STORE_CHECKSUM");
    if (checksum_enabled == nullptr || std::string(checksum_enabled) != "1") {
        GTEST_SKIP() << "MOONCAKE_STORE_CHECKSUM is not enabled";
    }

    const std::string key = "dfs_batch_checksum";
    std::string value(4096, 'P');
    std::vector<Slice> write_slices{{value.data(), value.size()}};
    ASSERT_TRUE(writer_->Put(key, write_slices, DfsConfig()).has_value());

    auto query = QueryDfsOnly(key);
    ASSERT_TRUE(query.has_value());
    ASSERT_TRUE(query->object_checksum.has_value());
    std::vector<Replica::Descriptor> replicas = query->replicas;
    std::vector<QueryResult> queries;
    queries.emplace_back(
        std::move(replicas), query->lease_timeout,
        std::optional<uint64_t>(*query->object_checksum ^ uint64_t{1}));

    std::string output(value.size(), '\0');
    std::unordered_map<std::string, std::vector<Slice>> read_slices;
    read_slices[key] = {{output.data(), output.size()}};
    auto results = writer_->BatchGet({key}, queries, read_slices);
    ASSERT_EQ(results.size(), 1);
    ASSERT_FALSE(results[0].has_value());
    EXPECT_EQ(results[0].error(), ErrorCode::CHECKSUM_MISMATCH);
}

}  // namespace mooncake::test
