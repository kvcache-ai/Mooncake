#include "storage_backend.h"

#include <gtest/gtest.h>
#include <ylt/struct_pb.hpp>

#include <algorithm>
#include <barrier>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <unistd.h>

namespace mooncake::test {
namespace {

class ScopedEnvVar {
   public:
    ScopedEnvVar(const char* name, const char* value) : name_(name) {
        if (const char* original = std::getenv(name)) original_ = original;
        if (value) {
            setenv(name, value, 1);
        } else {
            unsetenv(name);
        }
    }
    ~ScopedEnvVar() {
        if (original_) {
            setenv(name_.c_str(), original_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }
    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;

   private:
    std::string name_;
    std::optional<std::string> original_;
};

struct WriteMode {
    const char* direct;
    const char* threads;
    bool use_uring;
};

std::vector<WriteMode> WriteModes() {
    std::vector<WriteMode> modes{{"true", "8", false}};
#ifdef USE_URING
    const char* direct_settings[] = {nullptr, "false", "true", "1"};
    for (const char* threads : {"1", "4", "8"}) {
        for (const char* direct : direct_settings) {
            modes.push_back({direct, threads, true});
        }
    }
    // Out-of-range counts clamp; invalid counts use the serial default.
    for (const char* threads : {"0", "-2", "9", "invalid"}) {
        modes.push_back({"true", threads, true});
    }
    modes.push_back({"invalid", "8", true});
    modes.push_back({"true", nullptr, true});
#endif
    return modes;
}

class BucketDirectWriteTest : public ::testing::TestWithParam<WriteMode> {
   protected:
    using Values = std::unordered_map<std::string, std::string>;
    std::filesystem::path path_;
    FileStorageConfig file_config_;
    BucketBackendConfig bucket_config_;
    std::unique_ptr<ScopedEnvVar> direct_env_;
    std::unique_ptr<ScopedEnvVar> threads_env_;

    void SetUp() override {
        direct_env_ = std::make_unique<ScopedEnvVar>(
            "MOONCAKE_OFFLOAD_BUCKET_DIRECT_WRITE", GetParam().direct);
        threads_env_ = std::make_unique<ScopedEnvVar>(
            "MOONCAKE_OFFLOAD_BUCKET_COPY_THREADS", GetParam().threads);
        char directory[] = "/tmp/mooncake-bucket-direct-XXXXXX";
        ASSERT_NE(mkdtemp(directory), nullptr);
        path_ = directory;
        file_config_.storage_filepath = path_.string();
        file_config_.use_uring = GetParam().use_uring;
        bucket_config_.max_total_size = 128 * 1024 * 1024;
        bucket_config_.eviction_policy = BucketEvictionPolicy::LRU;
    }

    void TearDown() override {
        std::error_code ec;
        if (!path_.empty()) std::filesystem::remove_all(path_, ec);
    }

    bool DirectEnabled() const {
        const auto mode = GetParam();
        return mode.use_uring && mode.direct &&
               (std::string(mode.direct) == "true" ||
                std::string(mode.direct) == "1");
    }

    static Values MakeValues(int round, int count) {
        constexpr size_t sizes[] = {7, 4097, 1048579, 2097155};
        Values values;
        for (int i = 0; i < count; ++i) {
            auto key = "tenant\x1f" + std::to_string(round) + "/" +
                       std::to_string(i) + std::string(i % 13, 'k');
            std::string data(sizes[i % 4], '\0');
            for (size_t j = 0; j < data.size(); ++j) {
                data[j] =
                    static_cast<char>((round * 31 + i * 17 + j * 13) % 251);
            }
            values.emplace(std::move(key), std::move(data));
        }
        return values;
    }

    static std::unordered_map<std::string, std::vector<Slice>> MakeBatch(
        Values& values) {
        std::unordered_map<std::string, std::vector<Slice>> batch;
        for (auto& [key, data] : values) {
            const size_t split = data.size() / 2;
            batch.emplace(key, std::vector<Slice>{
                                   {data.data(), split},
                                   {data.data() + split, 0},
                                   {data.data() + split, data.size() - split}});
        }
        return batch;
    }

    static void Verify(BucketStorageBackend& backend, const Values& values) {
        for (const auto& [key, expected] : values) {
            SCOPED_TRACE(key);
            // BatchLoad's direct read rounds both the offset and the end;
            // the caller supplies aligned storage with room for both edges.
            void* base = nullptr;
            ASSERT_EQ(posix_memalign(&base, 4096, expected.size() + 8192), 0);
            std::unique_ptr<void, void (*)(void*)> buffer(base, free);
            std::unordered_map<std::string, Slice> load{
                {key, {base, expected.size()}}};
            ASSERT_TRUE(backend.BatchLoad(load));
            ASSERT_EQ(load.at(key).size, expected.size());
            EXPECT_EQ(
                memcmp(load.at(key).ptr, expected.data(), expected.size()), 0);
        }
    }

    void VerifyLayout(const std::vector<std::string>& keys,
                      const std::vector<StorageObjectMetadata>& metadata,
                      const Values& values) {
        ASSERT_EQ(keys.size(), values.size());
        ASSERT_EQ(metadata.size(), keys.size());
        ASSERT_FALSE(keys.empty());
        int64_t logical_size = 0;
        std::string expected;
        for (size_t i = 0; i < keys.size(); ++i) {
            ASSERT_EQ(metadata[i].bucket_id, metadata.front().bucket_id);
            EXPECT_EQ(metadata[i].offset, logical_size);
            EXPECT_EQ(metadata[i].key_size, keys[i].size());
            EXPECT_EQ(metadata[i].data_size, values.at(keys[i]).size());
            expected += keys[i];
            expected += values.at(keys[i]);
            logical_size += keys[i].size() + values.at(keys[i]).size();
        }
        const auto stem = std::to_string(metadata.front().bucket_id);
        const auto bucket_path = path_ / (stem + ".bucket");
        const auto physical_size = DirectEnabled()
                                       ? (logical_size + 4095) / 4096 * 4096
                                       : logical_size;
        ASSERT_EQ(std::filesystem::file_size(bucket_path), physical_size);
        expected.resize(physical_size, '\0');
        std::ifstream input(bucket_path, std::ios::binary);
        std::string actual(physical_size, '?');
        input.read(actual.data(), actual.size());
        ASSERT_EQ(input.gcount(), physical_size);
        EXPECT_TRUE(actual == expected)
            << "bucket bytes or tail padding changed";

        const auto meta_path = path_ / (stem + ".meta");
        std::ifstream meta_input(meta_path, std::ios::binary);
        std::string encoded(std::filesystem::file_size(meta_path), '\0');
        meta_input.read(encoded.data(), encoded.size());
        ASSERT_EQ(meta_input.gcount(), encoded.size());
        BucketMetadata decoded;
        ASSERT_NO_THROW(struct_pb::from_pb(decoded, encoded));
        EXPECT_EQ(decoded.keys, keys);
        EXPECT_EQ(decoded.data_size, logical_size);
        ASSERT_EQ(decoded.metadatas.size(), metadata.size());
        for (size_t i = 0; i < metadata.size(); ++i) {
            EXPECT_EQ(decoded.metadatas[i].offset, metadata[i].offset);
            EXPECT_EQ(decoded.metadatas[i].key_size, metadata[i].key_size);
            EXPECT_EQ(decoded.metadatas[i].data_size, metadata[i].data_size);
        }
        std::string reencoded;
        struct_pb::to_pb(decoded, reencoded);
        EXPECT_EQ(encoded, reencoded)
            << ".meta must have its exact protobuf length";
    }
};

TEST_P(BucketDirectWriteTest, LayoutRollbackAndRestartReadback) {
    Values all_values;
    std::unordered_map<std::string, StorageObjectMetadata> committed;
    {
        BucketStorageBackend backend(file_config_, bucket_config_);
        ASSERT_TRUE(backend.Init());
        // Grow the thread-local buffer, then reuse it for a smaller bucket.
        // The middle bucket exceeds 16 MiB and crosses worker/iovec boundaries.
        for (int round = 0; round < 3; ++round) {
            auto values = MakeValues(round, round == 1 ? 32 : 4);
            auto batch = MakeBatch(values);
            std::vector<std::string> keys;
            std::vector<StorageObjectMetadata> metadata;
            auto result =
                backend.BatchOffload(batch, [&](const auto& k, auto& m) {
                    keys = k;
                    metadata = m;
                    return ErrorCode::OK;
                });
            ASSERT_TRUE(result);
            ASSERT_FALSE(metadata.empty());
            EXPECT_EQ(result.value(), metadata.front().bucket_id);
            std::vector<std::string> input_order;
            for (const auto& [key, slices] : batch) input_order.push_back(key);
            EXPECT_EQ(keys, input_order);
            VerifyLayout(keys, metadata, values);
            for (size_t i = 0; i < keys.size(); ++i)
                committed.emplace(keys[i], metadata[i]);
            all_values.merge(values);
        }
        Verify(backend, all_values);
        const auto files_before =
            std::distance(std::filesystem::directory_iterator(path_),
                          std::filesystem::directory_iterator{});
        Values failed{{"must-rollback", std::string(8193, 'z')}};
        auto failed_batch = MakeBatch(failed);
        auto failure = backend.BatchOffload(
            failed_batch,
            [](const auto&, auto&) { return ErrorCode::INTERNAL_ERROR; });
        ASSERT_FALSE(failure);
        EXPECT_EQ(failure.error(), ErrorCode::INTERNAL_ERROR);
        EXPECT_FALSE(backend.IsExist("must-rollback").value_or(true));
        EXPECT_EQ(std::distance(std::filesystem::directory_iterator(path_),
                                std::filesystem::directory_iterator{}),
                  files_before);
        Verify(backend, all_values);

        backend.SetDatasyncFailureForTest(true);
        bool notified = false;
        auto sync_failure =
            backend.BatchOffload(failed_batch, [&](const auto&, auto&) {
                notified = true;
                return ErrorCode::OK;
            });
        ASSERT_FALSE(sync_failure);
        EXPECT_EQ(sync_failure.error(), ErrorCode::FILE_WRITE_FAIL);
        EXPECT_FALSE(notified);
        EXPECT_FALSE(backend.IsExist("must-rollback").value_or(true));
        EXPECT_EQ(std::distance(std::filesystem::directory_iterator(path_),
                                std::filesystem::directory_iterator{}),
                  files_before);
    }
    {
        ScopedEnvVar rollback_setting("MOONCAKE_OFFLOAD_BUCKET_DIRECT_WRITE",
                                      DirectEnabled() ? "false" : "true");
        BucketStorageBackend backend(file_config_, bucket_config_);
        ASSERT_TRUE(backend.Init());
        size_t scanned = 0;
        ASSERT_TRUE(backend.ScanMeta([&](const auto& keys, auto& metadata) {
            EXPECT_EQ(keys.size(), metadata.size());
            for (size_t i = 0; i < keys.size(); ++i) {
                EXPECT_TRUE(all_values.contains(keys[i]));
                const auto& original = committed.at(keys[i]);
                EXPECT_EQ(metadata[i].bucket_id, original.bucket_id);
                EXPECT_EQ(metadata[i].offset, original.offset);
                EXPECT_EQ(metadata[i].key_size, original.key_size);
                EXPECT_EQ(metadata[i].data_size, original.data_size);
            }
            scanned += keys.size();
            return ErrorCode::OK;
        }));
        EXPECT_EQ(scanned, all_values.size());
        EXPECT_FALSE(backend.IsExist("must-rollback").value_or(true));
        Verify(backend, all_values);
    }
}

TEST_P(BucketDirectWriteTest, ConcurrentWritersKeepSeparateBuffers) {
    BucketStorageBackend backend(file_config_, bucket_config_);
    ASSERT_TRUE(backend.Init());
    auto first = MakeValues(10, 32);
    auto second = MakeValues(11, 32);
    std::barrier start(2);
    auto write = [&](Values& values) {
        auto batch = MakeBatch(values);
        start.arrive_and_wait();
        return backend.BatchOffload(
            batch, [](const auto&, auto&) { return ErrorCode::OK; });
    };
    auto writer = std::async(std::launch::async, [&] { return write(first); });
    auto result = write(second);
    auto first_result = writer.get();
    ASSERT_TRUE(first_result);
    ASSERT_TRUE(result);
    EXPECT_NE(first_result.value(), result.value());
    Verify(backend, first);
    Verify(backend, second);
}

INSTANTIATE_TEST_SUITE_P(WriteModes, BucketDirectWriteTest,
                         ::testing::ValuesIn(WriteModes()));

}  // namespace
}  // namespace mooncake::test
