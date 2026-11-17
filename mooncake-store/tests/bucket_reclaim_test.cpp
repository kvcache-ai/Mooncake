#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "storage_backend.h"

namespace mooncake {
namespace fs = std::filesystem;

class BucketReclaimTest : public ::testing::Test {
   protected:
    void SetUp() override {
        data_path_ = (fs::current_path() / "bucket_reclaim_test_data").string();
        fs::remove_all(data_path_);
        fs::create_directories(data_path_);
        config_.storage_filepath = data_path_;
        Restart();
    }

    void TearDown() override {
        backend_.reset();
        fs::remove_all(data_path_);
    }

    void Restart() {
        backend_.reset();
        backend_ = std::make_unique<BucketStorageBackend>(
            config_, BucketBackendConfig{});
        ASSERT_TRUE(backend_->Init());
    }

    int64_t Offload(const std::unordered_map<std::string, std::string>& kvs) {
        std::unordered_map<std::string, std::vector<Slice>> batch;
        for (const auto& [key, value] : kvs) {
            batch.emplace(
                key, std::vector<Slice>{
                         Slice{const_cast<char*>(value.data()), value.size()}});
        }
        auto result = backend_->BatchOffload(
            batch,
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
        EXPECT_TRUE(result.has_value());
        return result.value_or(-1);
    }

    std::optional<std::string> Load(const std::string& key, size_t size) {
        std::string buf(size, '\0');
        std::unordered_map<std::string, Slice> slices;
        slices.emplace(key, Slice{buf.data(), size});
        if (!backend_->BatchLoad(slices)) {
            return std::nullopt;
        }
        return buf;
    }

    // Keys BucketScan reports, with the bucket each is reported in.
    std::vector<std::pair<std::string, int64_t>> Scan() {
        std::vector<std::string> keys;
        std::vector<StorageObjectMetadata> metadatas;
        std::vector<int64_t> buckets;
        EXPECT_TRUE(backend_->BucketScan(0, keys, metadatas, buckets, 1000));
        std::vector<std::pair<std::string, int64_t>> scanned;
        for (size_t i = 0; i < keys.size(); ++i) {
            scanned.emplace_back(keys[i], metadatas[i].bucket_id);
        }
        std::sort(scanned.begin(), scanned.end());
        return scanned;
    }

    // Rewrites a bucket's files, so that a directory listing in creation
    // order meets them after any bucket written later.
    void RecreateFiles(int64_t bucket_id) const {
        for (const char* suffix : {".bucket", ".meta"}) {
            const std::string path =
                data_path_ + "/" + std::to_string(bucket_id) + suffix;
            fs::copy_file(path, path + ".tmp");
            fs::remove(path);
            fs::rename(path + ".tmp", path);
        }
    }

    std::string DataFile(int64_t bucket_id) const {
        return data_path_ + "/" + std::to_string(bucket_id) + ".bucket";
    }

    std::string data_path_;
    FileStorageConfig config_;
    std::unique_ptr<BucketStorageBackend> backend_;
};

TEST_F(BucketReclaimTest, PartiallyDeadBucketStays) {
    const std::string v1(4096, 'a'), v2(4096, 'b');
    const int64_t bucket_id = Offload({{"k1", v1}, {"k2", v2}});

    EXPECT_EQ(backend_->RemoveDeadBuckets({{bucket_id, {"k1"}}}), 0u);
    EXPECT_TRUE(fs::exists(DataFile(bucket_id)));
    EXPECT_EQ(Load("k1", v1.size()), v1);
    EXPECT_EQ(Load("k2", v2.size()), v2);
}

TEST_F(BucketReclaimTest, UnmappedKeyMapsToNewestCopy) {
    // Several keys, each with a stale copy in a bucket kept alive by another
    // key and a newer copy in its own bucket: after a restart every key must
    // map to its newer copy whatever order Init lists the buckets in.
    constexpr int kKeys = 8;
    const std::string old_value(4096, 'o'), new_value(4096, 'n');
    std::vector<std::pair<std::string, int64_t>> expected;
    std::vector<int64_t> old_buckets;
    for (int i = 0; i < kKeys; ++i) {
        const std::string key = "k" + std::to_string(i);
        const std::string live = "live" + std::to_string(i);
        const int64_t old_bucket =
            Offload({{key, old_value}, {live, old_value}});
        backend_->UnmapKeys({{old_bucket, {key}}});
        const int64_t new_bucket = Offload({{key, new_value}});
        ASSERT_GT(new_bucket, old_bucket);
        expected.emplace_back(key, new_bucket);
        expected.emplace_back(live, old_bucket);
        old_buckets.push_back(old_bucket);
    }
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(Scan(), expected);

    // After a restart both bucket files still list each key. The stale
    // copies' files are rewritten so that neither a creation-order nor a
    // reverse-creation-order listing meets the newest copy first by luck.
    backend_.reset();
    for (int i = 0; i < kKeys; i += 2) {
        RecreateFiles(old_buckets[i]);
    }
    Restart();
    EXPECT_EQ(Scan(), expected);
    for (int i = 0; i < kKeys; ++i) {
        const std::string key = "k" + std::to_string(i);
        EXPECT_EQ(Load(key, new_value.size()), new_value) << key;
        // The stale copy does not keep its bucket alive.
        EXPECT_EQ(backend_->RemoveDeadBuckets(
                      {{old_buckets[i], {"live" + std::to_string(i)}}}),
                  1u);
        EXPECT_EQ(Load(key, new_value.size()), new_value) << key;
    }
}

TEST_F(BucketReclaimTest, ConcurrentReadsDuringDeleteCompleteOrMiss) {
    const std::string value(1 << 20, 'r');
    const int64_t bucket_id = Offload({{"k1", value}});
    std::atomic<bool> stop{false};
    std::atomic<int> hits{0}, misses{0}, corrupt{0};
    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&] {
            while (!stop.load()) {
                auto loaded = Load("k1", value.size());
                if (!loaded) {
                    misses.fetch_add(1);
                } else if (*loaded == value) {
                    hits.fetch_add(1);
                } else {
                    corrupt.fetch_add(1);
                }
            }
        });
    }
    while (hits.load() < 8) {
        std::this_thread::yield();
    }
    EXPECT_EQ(backend_->RemoveDeadBuckets({{bucket_id, {"k1"}}}), 1u);
    while (misses.load() < 8) {
        std::this_thread::yield();
    }
    stop.store(true);
    for (auto& reader : readers) {
        reader.join();
    }
    EXPECT_EQ(corrupt.load(), 0);
    EXPECT_FALSE(fs::exists(DataFile(bucket_id)));
}

}  // namespace mooncake
