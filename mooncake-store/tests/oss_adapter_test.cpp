#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <string>

#include "storage/distributed/oss_adapter.h"
#include "storage_backend.h"

namespace mooncake {
namespace {

TEST(OssFileSystemAdapterTest, ObjectLifecycleAndVectorIO) {
    if (!std::getenv("MOONCAKE_OSS_ENDPOINT")) {
        GTEST_SKIP() << "MOONCAKE_OSS_ENDPOINT is not configured";
    }

    OssFileSystemAdapter adapter;
    ASSERT_TRUE(adapter.Init("/mooncake-oss-adapter-test"));

    const std::string first_path = "/mooncake-oss-adapter-test/00/file%2Fone";
    const std::string first_data = "hello oss";
    auto write = adapter.WriteFile(
        first_path,
        std::span<const char>(first_data.data(), first_data.size()));
    ASSERT_TRUE(write);
    EXPECT_EQ(*write, first_data.size());

    auto exists = adapter.FileExists(first_path);
    ASSERT_TRUE(exists);
    EXPECT_TRUE(*exists);
    auto size = adapter.GetFileSize(first_path);
    ASSERT_TRUE(size);
    EXPECT_EQ(*size, first_data.size());

    std::string read_buffer(first_data.size(), '\0');
    auto read =
        adapter.ReadFile(first_path, read_buffer.data(), read_buffer.size());
    ASSERT_TRUE(read);
    EXPECT_EQ(read_buffer, first_data);

    const std::string second_path = "/mooncake-oss-adapter-test/00/file-two";
    std::array<char, 3> left{{'a', 'b', 'c'}};
    std::array<char, 3> right{{'d', 'e', 'f'}};
    std::array<iovec, 2> write_iov{
        {{left.data(), left.size()}, {right.data(), right.size()}}};
    auto vector_write =
        adapter.VectorWriteFile(second_path, write_iov.data(), 2, 0);
    ASSERT_TRUE(vector_write);
    EXPECT_EQ(*vector_write, 6U);

    std::array<char, 2> read_left{};
    std::array<char, 2> read_right{};
    std::array<iovec, 2> read_iov{{{read_left.data(), read_left.size()},
                                   {read_right.data(), read_right.size()}}};
    auto vector_read =
        adapter.VectorReadFile(second_path, read_iov.data(), 2, 1);
    ASSERT_TRUE(vector_read);
    EXPECT_EQ(std::string(read_left.data(), read_left.size()), "bc");
    EXPECT_EQ(std::string(read_right.data(), read_right.size()), "de");

    EXPECT_EQ(*adapter.ReadFile(second_path, nullptr, 0), 0U);
    EXPECT_EQ(*adapter.VectorReadFile(second_path, nullptr, 0, 0), 0U);

    auto files = adapter.ListFilesWithInfo("/mooncake-oss-adapter-test/00");
    ASSERT_TRUE(files);
    ASSERT_EQ(files->size(), 2U);

    EXPECT_TRUE(adapter.DeleteFile(first_path));
    EXPECT_TRUE(adapter.DeleteFile(second_path));
    exists = adapter.FileExists(first_path);
    ASSERT_TRUE(exists);
    EXPECT_FALSE(*exists);
    EXPECT_TRUE(adapter.Shutdown());
}

TEST(OssFileSystemAdapterTest, StorageBackendFactoryAndHealthCheck) {
    if (!std::getenv("MOONCAKE_OSS_ENDPOINT")) {
        GTEST_SKIP() << "MOONCAKE_OSS_ENDPOINT is not configured";
    }
    setenv("MOONCAKE_DISTRIBUTED_FS_TYPE", "oss", 1);
    setenv("MOONCAKE_DISTRIBUTED_ROOT_DIR", "/mooncake-oss-backend-health-test",
           1);
    setenv("MOONCAKE_DISTRIBUTED_HEALTH_CHECK", "true", 1);
    setenv("MOONCAKE_DISTRIBUTED_HASH_BUCKET_COUNT", "4", 1);

    FileStorageConfig config;
    config.storage_backend_type = StorageBackendType::kDistributed;
    auto backend = CreateStorageBackend(config);
    ASSERT_TRUE(backend);
    ASSERT_TRUE((*backend)->Init());
    auto enabled = (*backend)->IsEnableOffloading();
    ASSERT_TRUE(enabled);
    EXPECT_TRUE(*enabled);
}

}  // namespace
}  // namespace mooncake
