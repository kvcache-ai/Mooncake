// Live tests against a real S3-compatible service. Skipped unless
// MOONCAKE_RUN_LIVE_S3=1 (the 1105-object listing test also needs
// MOONCAKE_RUN_LIVE_S3_PAGINATION=1); configure the service with the
// MOONCAKE_S3_* (or AWS_*) variables. The bucket must exist. Every object is
// written under a unique prefix and removed afterwards.

#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/s3_adapter.h"
#include "storage_backend.h"

namespace mooncake {
namespace {

bool LiveEnabled() {
    const char* enabled = std::getenv("MOONCAKE_RUN_LIVE_S3");
    return enabled && std::string(enabled) == "1";
}

std::string UniquePrefix(const std::string& name) {
    return "/mooncake-s3-live/" + std::to_string(getpid()) + "-" +
           std::to_string(
               std::chrono::steady_clock::now().time_since_epoch().count()) +
           "-" + name;
}

class LiveCleanup {
   public:
    explicit LiveCleanup(S3ObjectStorageAdapter& adapter) : adapter_(adapter) {}
    ~LiveCleanup() {
        auto keys = adapter_.ListKeys();
        EXPECT_TRUE(keys.has_value());
        if (!keys) return;
        for (const auto& key : *keys) {
            EXPECT_TRUE(adapter_.Delete(key.logical_key).has_value());
        }
        auto remaining = adapter_.ListKeys();
        EXPECT_TRUE(remaining.has_value() && remaining->empty());
    }

   private:
    S3ObjectStorageAdapter& adapter_;
};

// Sets an environment variable for the lifetime of the object.
class ScopedEnvVar {
   public:
    ScopedEnvVar(const char* name, const std::string& value) : name_(name) {
        if (const char* old = std::getenv(name)) previous_ = old;
        setenv(name, value.c_str(), 1);
    }
    ~ScopedEnvVar() {
        if (previous_)
            setenv(name_, previous_->c_str(), 1);
        else
            unsetenv(name_);
    }

   private:
    const char* name_;
    std::optional<std::string> previous_;
};

std::string Pattern(size_t size, int seed) {
    std::string data(size, '\0');
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<char>((i * 31 + seed) & 255);
    return data;
}

TEST(S3ObjectStorageAdapterLiveTest, ObjectLifecycle) {
    if (!LiveEnabled()) GTEST_SKIP() << "Set MOONCAKE_RUN_LIVE_S3=1";
    S3ObjectStorageAdapter adapter(UniquePrefix("lifecycle"));
    ASSERT_TRUE(adapter.Init());
    ASSERT_TRUE(adapter.CheckHealth());
    LiveCleanup cleanup(adapter);

    // Keys that exercise encoding: slash, space, percent, plus, unicode.
    const std::vector<std::string> keys = {"plain", "a/b c", "100%+x",
                                           "\xE4\xBD\xA0\xE5\xA5\xBD"};
    for (size_t i = 0; i < keys.size(); ++i) {
        const std::string data = Pattern(1000 + i * 4097, static_cast<int>(i));
        ASSERT_TRUE(adapter.Put(keys[i], {data.data(), data.size()}))
            << keys[i];
        auto exists = adapter.Exists(keys[i]);
        ASSERT_TRUE(exists && *exists) << keys[i];
        auto size = adapter.GetSize(keys[i]);
        ASSERT_TRUE(size) << keys[i];
        EXPECT_EQ(*size, data.size());
        std::string read(data.size(), '\0');
        auto got = adapter.Get(keys[i], read.data(), read.size());
        ASSERT_TRUE(got) << keys[i];
        EXPECT_EQ(*got, data.size());
        EXPECT_EQ(read, data) << keys[i];
        std::string middle(100, '\0');
        auto range = adapter.GetRange(keys[i], middle.data(), middle.size(), 7);
        ASSERT_TRUE(range) << keys[i];
        EXPECT_EQ(middle, data.substr(7, 100));
    }

    auto listed = adapter.ListKeys();
    ASSERT_TRUE(listed);
    std::set<std::string> names;
    for (const auto& key : *listed) names.insert(key.logical_key);
    EXPECT_EQ(names, std::set<std::string>(keys.begin(), keys.end()));

    std::string missing(16, '\0');
    auto absent = adapter.Get("absent", missing.data(), missing.size());
    ASSERT_FALSE(absent);
    EXPECT_EQ(absent.error(), ErrorCode::FILE_NOT_FOUND);
    auto exists = adapter.Exists("absent");
    ASSERT_TRUE(exists);
    EXPECT_FALSE(*exists);

    ASSERT_TRUE(adapter.Delete("plain"));
    exists = adapter.Exists("plain");
    ASSERT_TRUE(exists);
    EXPECT_FALSE(*exists);
}

// Writes, reads and lists 1105 objects (two ListObjectsV2 pages). Against a
// real service this is slow and exposed to request-rate limits, so it has its
// own opt-in on top of MOONCAKE_RUN_LIVE_S3.
TEST(S3ObjectStorageAdapterLiveTest, BatchesAndPaginatedListing) {
    if (!LiveEnabled()) GTEST_SKIP() << "Set MOONCAKE_RUN_LIVE_S3=1";
    const char* pagination = std::getenv("MOONCAKE_RUN_LIVE_S3_PAGINATION");
    if (!pagination || std::string(pagination) != "1") {
        GTEST_SKIP() << "Set MOONCAKE_RUN_LIVE_S3_PAGINATION=1 for the "
                        "1105-object batch and paginated listing test";
    }
    S3ObjectStorageAdapter adapter(UniquePrefix("batch"));
    ASSERT_TRUE(adapter.Init());
    LiveCleanup cleanup(adapter);

    // More than one ListObjectsV2 page (1000 keys), with names containing
    // characters that are URL-encoded twice on the way to the server.
    constexpr size_t kCount = 1105;
    std::vector<std::string> keys;
    std::vector<std::string> payloads;
    std::vector<iovec> iovs;
    keys.reserve(kCount);
    payloads.reserve(kCount);
    iovs.reserve(kCount);
    for (size_t i = 0; i < kCount; ++i) {
        keys.push_back("layer/" + std::to_string(i) + "%chunk");
        payloads.push_back(Pattern(512 + i % 7, static_cast<int>(i)));
        iovs.push_back({payloads.back().data(), payloads.back().size()});
    }
    std::vector<ObjectPutRequest> puts;
    for (size_t i = 0; i < kCount; ++i) puts.push_back({keys[i], &iovs[i], 1});
    auto put_results = adapter.PutBatch(puts);
    ASSERT_EQ(put_results.size(), kCount);
    for (size_t i = 0; i < kCount; ++i) ASSERT_TRUE(put_results[i]) << i;

    std::vector<std::string> buffers(kCount);
    std::vector<ObjectGetRequest> gets;
    for (size_t i = 0; i < kCount; ++i) {
        buffers[i].assign(payloads[i].size(), '\0');
        gets.push_back({keys[i], buffers[i].data(), buffers[i].size()});
    }
    auto get_results = adapter.GetBatch(gets);
    ASSERT_EQ(get_results.size(), kCount);
    for (size_t i = 0; i < kCount; ++i) {
        ASSERT_TRUE(get_results[i]) << i;
        ASSERT_EQ(buffers[i], payloads[i]) << i;
    }

    auto listed = adapter.ListKeys();
    ASSERT_TRUE(listed);
    std::set<std::string> names;
    for (const auto& key : *listed) names.insert(key.logical_key);
    EXPECT_EQ(names, std::set<std::string>(keys.begin(), keys.end()));
}

// Negative control: proves the service actually verifies our signatures, so
// the passing tests above are not an artifact of an unauthenticated endpoint.
TEST(S3ObjectStorageAdapterLiveTest, WrongSecretIsRejected) {
    if (!LiveEnabled()) GTEST_SKIP() << "Set MOONCAKE_RUN_LIVE_S3=1";
    const char* secret = std::getenv("MOONCAKE_S3_SECRET_ACCESS_KEY");
    const std::string original = secret ? secret : "";
    setenv("MOONCAKE_S3_SECRET_ACCESS_KEY", "definitely-not-the-secret", 1);
    S3ObjectStorageAdapter adapter(UniquePrefix("wrong-secret"));
    const bool initialized = adapter.Init().has_value();
    if (secret) {
        setenv("MOONCAKE_S3_SECRET_ACCESS_KEY", original.c_str(), 1);
    } else {
        unsetenv("MOONCAKE_S3_SECRET_ACCESS_KEY");
    }
    ASSERT_TRUE(initialized);
    const std::string data = "should not be stored";
    auto put = adapter.Put("probe", {data.data(), data.size()});
    ASSERT_FALSE(put);
    EXPECT_EQ(put.error(), ErrorCode::FILE_WRITE_FAIL);
}

// Store-level round trip: the "s3" fs_adapter_type through the storage
// backend factory, the object health check, multi-slice BatchOffload,
// BatchLoad and IsExist, including an opaque logical key.
TEST(S3ObjectStorageAdapterLiveTest, StorageBackendRoundTripsThroughFactory) {
    if (!LiveEnabled()) GTEST_SKIP() << "Set MOONCAKE_RUN_LIVE_S3=1";
    const std::string root = UniquePrefix("store");
    ScopedEnvVar fs_type("MOONCAKE_DISTRIBUTED_FS_TYPE", "s3");
    ScopedEnvVar root_dir("MOONCAKE_DISTRIBUTED_ROOT_DIR", root);
    ScopedEnvVar health("MOONCAKE_DISTRIBUTED_HEALTH_CHECK", "true");
    ScopedEnvVar buckets("MOONCAKE_DISTRIBUTED_HASH_BUCKET_COUNT", "4");

    S3ObjectStorageAdapter cleanup_adapter(root);
    ASSERT_TRUE(cleanup_adapter.Init());
    LiveCleanup cleanup(cleanup_adapter);

    FileStorageConfig config;
    config.storage_backend_type = StorageBackendType::kDistributed;
    auto created = CreateStorageBackend(config);
    ASSERT_TRUE(created);
    auto backend =
        std::dynamic_pointer_cast<DistributedStorageBackend>(*created);
    ASSERT_NE(backend, nullptr);
    EXPECT_TRUE(backend->UsesObjectStorage());
    ASSERT_TRUE(backend->Init());

    std::string opaque_key = "tenant";
    opaque_key.push_back('\0');
    opaque_key += "a/b%\\";
    std::vector<std::string> keys;
    for (int i = 0; i < 32; ++i) keys.push_back("kv/" + std::to_string(i));
    keys.push_back(opaque_key);

    std::map<std::string, std::pair<std::string, std::string>> parts;
    std::unordered_map<std::string, std::vector<Slice>> batch;
    for (size_t i = 0; i < keys.size(); ++i) {
        auto& [head, tail_part] = parts[keys[i]];
        head = Pattern(8192 + i * 97, static_cast<int>(i));
        tail_part = Pattern(1024 + i, static_cast<int>(i) + 7);
    }
    for (auto& [key, value] : parts) {
        batch[key] = {{value.first.data(), value.first.size()},
                      {value.second.data(), value.second.size()}};
    }

    std::map<std::string, int64_t> completed;
    auto offloaded = backend->BatchOffload(
        batch, [&](const std::vector<std::string>& done,
                   std::vector<StorageObjectMetadata>& metadata) {
            for (size_t i = 0; i < done.size(); ++i)
                completed[done[i]] = metadata[i].data_size;
            return ErrorCode::OK;
        });
    ASSERT_TRUE(offloaded);
    EXPECT_EQ(*offloaded, static_cast<int64_t>(keys.size()));
    ASSERT_EQ(completed.size(), keys.size());
    for (const auto& [key, value] : parts) {
        EXPECT_EQ(completed.at(key), static_cast<int64_t>(value.first.size() +
                                                          value.second.size()));
    }
    // The objects are in the bucket, seen through an independent adapter.
    auto stored = cleanup_adapter.ListKeys();
    ASSERT_TRUE(stored);
    EXPECT_EQ(stored->size(), keys.size());

    std::map<std::string, std::string> buffers;
    std::unordered_map<std::string, Slice> load;
    for (const auto& [key, value] : parts) {
        auto& buffer = buffers[key];
        buffer.assign(value.first.size() + value.second.size(), '\0');
        load[key] = {buffer.data(), buffer.size()};
    }
    ASSERT_TRUE(backend->BatchLoad(load));
    for (const auto& [key, value] : parts) {
        EXPECT_EQ(buffers.at(key), value.first + value.second);
    }

    auto present = backend->IsExist(opaque_key);
    ASSERT_TRUE(present);
    EXPECT_TRUE(*present);
    auto absent = backend->IsExist("kv/missing");
    ASSERT_TRUE(absent);
    EXPECT_FALSE(*absent);
}

}  // namespace
}  // namespace mooncake
