#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

#include "client_service.h"
#include "common/client_buffer_allocation.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/posix_fs_adapter.h"
#include "test_server_helpers.h"

namespace mooncake::test {
namespace {

class FailingBucketPosixAdapter : public PosixFsAdapter {
   public:
    void FailNextWrite() { fail_next_.store(true); }
    tl::expected<size_t, ErrorCode> WriteAt(int fd, const iovec* iov,
                                            int iovcnt,
                                            int64_t offset) override {
        if (fail_next_.exchange(false)) {
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        return PosixFsAdapter::WriteAt(fd, iov, iovcnt, offset);
    }

   private:
    std::atomic<bool> fail_next_{false};
};

class DfsImmutableBucketClientTest : public ::testing::Test {
   protected:
    void SetUp() override {
        root_ = (std::filesystem::temp_directory_path() /
                 ("dfs_bucket_client_" + std::to_string(::getpid()) + "_" +
                  std::to_string(next_root_.fetch_add(1))))
                    .string();
        std::filesystem::create_directories(root_);
        SetEnv("MOONCAKE_ENABLE_DFS", "1");
        SetEnv("MOONCAKE_DFS_FS_ADAPTER", "posix");
        SetEnv("MOONCAKE_DFS_ROOT_DIR", root_);
        SetEnv("MOONCAKE_DFS_ALLOCATOR", "bucket");
        SetEnv("MOONCAKE_DFS_BUCKET_CAPACITY", "4096");
        SetEnv("MOONCAKE_DFS_MAX_BUCKET_COUNT", "2");
        SetEnv("MOONCAKE_DFS_ALIGNMENT", "4096");
        SetEnv("MOONCAKE_DFS_EVICTION_ENABLED", "1");
        SetEnv("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", "0.1");
        SetEnv("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", "0.0");
        SetEnv("MOONCAKE_DFS_SINGLE_TENANT", "true");

        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
        writer_ = CreateClient("127.0.0.1:18201");
        provider_ = CreateClient("127.0.0.1:18202");
        ASSERT_NE(writer_, nullptr);
        ASSERT_NE(provider_, nullptr);
        segment_size_ = 16 * 1024 * 1024;
        segment_ = allocate_buffer_allocator_memory(segment_size_);
        ASSERT_NE(segment_, nullptr);
        ASSERT_TRUE(provider_->MountSegment(segment_, segment_size_, "tcp"));

        FileStorageConfig file_config;
        file_config.storage_backend_type = StorageBackendType::kDistributed;
        file_config.storage_filepath = root_;
        DistributedStorageConfig distributed_config;
        distributed_config.fsdir = root_;
        distributed_config.fs_adapter_type = "posix";
        distributed_config.allocator_type = "bucket";
        distributed_config.alignment = 4096;
        distributed_config.bucket_capacity = 4096;
        distributed_config.max_bucket_count = 2;
        auto adapter = std::make_unique<FailingBucketPosixAdapter>();
        adapter_ = adapter.get();
        backend_ = std::make_shared<DistributedStorageBackend>(
            file_config, distributed_config, std::move(adapter));
        ASSERT_TRUE(backend_->Init());
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
        if (segment_) std::free(segment_);
        for (auto it = saved_env_.rbegin(); it != saved_env_.rend(); ++it) {
            if (it->second) {
                ::setenv(it->first.c_str(), it->second->c_str(), 1);
            } else {
                ::unsetenv(it->first.c_str());
            }
        }
        std::error_code error;
        std::filesystem::remove_all(root_, error);
    }

    std::shared_ptr<Client> CreateClient(const std::string& hostname) {
        auto client = Client::Create(hostname, "P2PHANDSHAKE", "tcp",
                                     std::nullopt, master_.master_address());
        return client ? *client : nullptr;
    }

    void SetEnv(const std::string& name, const std::string& value) {
        const char* old = ::getenv(name.c_str());
        saved_env_.emplace_back(
            name, old ? std::optional<std::string>(old) : std::nullopt);
        ::setenv(name.c_str(), value.c_str(), 1);
    }

    ReplicateConfig DfsConfig() const {
        ReplicateConfig config;
        config.replica_num = 1;
        config.dfs_replica_num = 1;
        return config;
    }

    static std::vector<Slice> Slices(std::string& value) {
        return {{value.data(), value.size()}};
    }

    inline static std::atomic<int> next_root_{0};
    testing::InProcMaster master_;
    std::shared_ptr<Client> writer_;
    std::shared_ptr<Client> provider_;
    std::shared_ptr<DistributedStorageBackend> backend_;
    FailingBucketPosixAdapter* adapter_ = nullptr;
    void* segment_ = nullptr;
    size_t segment_size_ = 0;
    std::string root_;
    std::vector<std::pair<std::string, std::optional<std::string>>> saved_env_;
};

TEST_F(DfsImmutableBucketClientTest, PutReturnsAfterWriteAndFailureRevokes) {
    std::string value(4096, 'A');
    auto slices = Slices(value);
    ASSERT_TRUE(writer_->Put("ok", slices, DfsConfig()));
    auto query = writer_->Query("ok");
    ASSERT_TRUE(query);
    auto dfs = std::find_if(query->replicas.begin(), query->replicas.end(),
                            [](const Replica::Descriptor& descriptor) {
                                return descriptor.is_dfs_replica();
                            });
    ASSERT_NE(dfs, query->replicas.end());
    std::string output(value.size(), '\0');
    auto reads = backend_->BatchRead(
        {{"ok", dfs->get_dfs_descriptor(), {{output.data(), output.size()}}}});
    ASSERT_TRUE(reads[0]);
    EXPECT_EQ(output, value);

    adapter_->FailNextWrite();
    auto failed_value = std::string(4096, 'B');
    auto failed_slices = Slices(failed_value);
    auto failed = writer_->Put("failed", failed_slices, DfsConfig());
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error(), ErrorCode::FILE_WRITE_FAIL);
    auto missing = writer_->Query("failed");
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(DfsImmutableBucketClientTest, ExhaustionEvictsWholeBucketAndRetries) {
    std::string first(4096, '1');
    std::string second(4096, '2');
    std::string third(4096, '3');
    auto first_slices = Slices(first);
    auto second_slices = Slices(second);
    auto third_slices = Slices(third);
    ASSERT_TRUE(writer_->Put("first", first_slices, DfsConfig()));
    ASSERT_TRUE(writer_->Put("second", second_slices, DfsConfig()));
    ASSERT_TRUE(writer_->Put("third", third_slices, DfsConfig()));

    auto first_query = writer_->Query("first");
    ASSERT_TRUE(first_query);
    EXPECT_EQ(first_query->replicas.size(), 1u);
    auto third_query = writer_->Query("third");
    ASSERT_TRUE(third_query);
    EXPECT_EQ(third_query->replicas.size(), 2u);
}

}  // namespace
}  // namespace mooncake::test
