#include <fcntl.h>
#include <gtest/gtest.h>
#include <limits.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "hf3fs/hf3fs.h"
#include "storage/distributed/dfs_descriptor_cache.h"
#include "storage/distributed/dfs_global_allocator.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/hf3fs_adapter.h"
#include "storage_backend.h"

namespace mooncake::test {
namespace {

constexpr const char* kDefaultRoot = "/mnt/3fs/mooncake_test";

bool IsHf3fsPath(const std::string& path) {
    char mount_point[PATH_MAX] = {};
    int ret = hf3fs_extract_mount_point(mount_point, sizeof(mount_point),
                                        path.c_str());
    return ret > 0 && ret <= static_cast<int>(sizeof(mount_point));
}

std::string DfsRootFromEnv() {
    const char* root = std::getenv("MOONCAKE_DFS_ROOT_DIR");
    return root && root[0] != '\0' ? std::string(root)
                                   : std::string(kDefaultRoot);
}

std::optional<std::string> ExistingAncestor(std::filesystem::path path) {
    while (!path.empty()) {
        if (std::filesystem::exists(path)) return path.string();
        path = path.parent_path();
    }
    return std::nullopt;
}

class Hf3fsTestDir {
   public:
    Hf3fsTestDir() {
        static std::atomic<int64_t> counter{0};
        root_ = DfsRootFromEnv();
        path_ = std::filesystem::path(root_) /
                ("dfs_hf3fs_test_" + std::to_string(::getpid()) + "_" +
                 std::to_string(++counter));
        path_str_ = path_.string();
    }

    ~Hf3fsTestDir() {
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

    void CreateOrSkip() {
        auto ancestor = ExistingAncestor(path_);
        if (!ancestor.has_value()) {
            GTEST_SKIP() << "no existing parent for test root: " << root_;
        }
        if (!IsHf3fsPath(*ancestor)) {
            GTEST_SKIP() << "test root is not under an hf3fs mount: " << root_;
        }

        std::filesystem::create_directories(path_);
        if (!IsHf3fsPath(path_str_)) {
            GTEST_SKIP() << "test directory is not on an hf3fs mount: "
                         << path_str_;
        }
    }

    const std::string& path() const { return path_str_; }

    std::string file(const std::string& name) const {
        return (path_ / name).string();
    }

   private:
    std::string root_;
    std::filesystem::path path_;
    std::string path_str_;
};

class Hf3fsAdapterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        test_dir_ = std::make_unique<Hf3fsTestDir>();
        test_dir_->CreateOrSkip();
    }

    void TearDown() override { test_dir_.reset(); }

    std::unique_ptr<Hf3fsTestDir> test_dir_;
};

}  // namespace

TEST_F(Hf3fsAdapterTest, WriteAtReadAtThroughUsrbio) {
    Hf3fsAdapter adapter;
    ASSERT_TRUE(adapter.Init(test_dir_->path()).has_value());

    const std::string path = test_dir_->file("adapter_smoke.data");
    ASSERT_TRUE(adapter.PreallocateFile(path, 4096).has_value());

    auto fd = adapter.OpenFile(path);
    ASSERT_TRUE(fd.has_value());

    std::array<char, 128> write_buf;
    std::array<char, 128> read_buf{};
    write_buf.fill('Q');

    iovec wiov{write_buf.data(), write_buf.size()};
    auto written = adapter.WriteAt(*fd, &wiov, 1, 100);
    ASSERT_TRUE(written.has_value());
    EXPECT_EQ(*written, write_buf.size());

    iovec riov{read_buf.data(), read_buf.size()};
    auto read = adapter.ReadAt(*fd, &riov, 1, 100);
    ASSERT_TRUE(read.has_value());
    EXPECT_EQ(*read, read_buf.size());
    EXPECT_EQ(std::memcmp(write_buf.data(), read_buf.data(), write_buf.size()),
              0);

    EXPECT_TRUE(adapter.CloseFile(*fd).has_value());
    EXPECT_TRUE(adapter.Shutdown().has_value());
}

TEST_F(Hf3fsAdapterTest, DistributedBackendBatchOffloadAndLoad) {
    FileStorageConfig file_config;
    file_config.storage_backend_type = StorageBackendType::kDistributed;
    file_config.storage_filepath = test_dir_->path();

    DistributedStorageConfig distributed_config;
    distributed_config.fsdir = test_dir_->path();
    distributed_config.fs_adapter_type = "hf3fs";
    distributed_config.shard_count = 2;
    distributed_config.shard_capacity = 1024 * 1024;
    distributed_config.alignment = 4096;
    distributed_config.single_tenant = true;

    auto desc_cache = std::make_shared<DfsDescriptorCache>();
    DistributedStorageBackend backend(file_config, distributed_config,
                                      std::make_unique<Hf3fsAdapter>());
    backend.SetDescriptorCache(desc_cache);
    ASSERT_TRUE(backend.Init().has_value());

    alignas(4096) std::array<char, 4096> write_buf;
    alignas(4096) std::array<char, 4096> read_buf{};
    write_buf.fill('B');

    const std::string key = "hf3fs_backend_key";
    const std::string shard_path = test_dir_->file(
        "dfs_shard_" +
        DfsGlobalAllocator::FormatShardIdx(0, distributed_config.shard_count) +
        ".data");
    desc_cache->Put(key,
                    {shard_path, 0, write_buf.size(), write_buf.size(), 0});

    std::unordered_map<std::string, std::vector<Slice>> offload_batch;
    offload_batch[key] = {{write_buf.data(), write_buf.size()}};
    auto offload = backend.BatchOffload(
        offload_batch,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_TRUE(offload.has_value());
    EXPECT_EQ(*offload, 1);

    std::unordered_map<std::string, Slice> load_batch;
    load_batch[key] = {read_buf.data(), read_buf.size()};
    auto load = backend.BatchLoad(load_batch);
    ASSERT_TRUE(load.has_value());
    EXPECT_EQ(std::memcmp(write_buf.data(), read_buf.data(), write_buf.size()),
              0);
}

}  // namespace mooncake::test
