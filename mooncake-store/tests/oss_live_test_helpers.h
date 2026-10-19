#pragma once
#include <gtest/gtest.h>
#include <unistd.h>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <optional>
#include <string>
#include <vector>
#include "storage/distributed/oss_adapter.h"

namespace mooncake::test {
class LiveOssParameter {
   public:
    LiveOssParameter(const char* key, const char* value) : key_(key) {
        if (const char* old = getenv(key)) old_ = old;
        setenv(key, value, 1);
    }
    ~LiveOssParameter() {
        if (old_)
            setenv(key_.c_str(), old_->c_str(), 1);
        else
            unsetenv(key_.c_str());
    }

   private:
    std::string key_;
    std::optional<std::string> old_;
};
class LiveOssCleanup {
   public:
    explicit LiveOssCleanup(OssObjectStorageAdapter& adapter)
        : adapter_(adapter) {}
    ~LiveOssCleanup() {
        for (const auto& key : keys)
            EXPECT_TRUE(adapter_.Delete(key).has_value());
        auto remaining = adapter_.ListKeys();
        EXPECT_TRUE(remaining.has_value());
        if (remaining) {
            EXPECT_TRUE(remaining->empty());
        }
    }
    std::vector<std::string> keys;

   private:
    OssObjectStorageAdapter& adapter_;
};

// Cloud acceptance timing: a finite continuous queue, NOT a maximum-throughput
// guarantee. Every request owns a separate buffer; only final queue drain
// occurs.
template <class Execute>
void RunLiveOssBatchGet(const char* mode, Execute execute) {
    const char* enabled = getenv("MOONCAKE_RUN_LIVE_OSS");
    if (!enabled || std::string(enabled) != "1") {
        GTEST_SKIP() << "Set MOONCAKE_RUN_LIVE_OSS=1 for real OSS batch I/O";
    }
    constexpr size_t size = size_t{4} << 20, depth = 64, count = 1024,
                     rounds = 3;
    LiveOssParameter connections("MOONCAKE_OSS_MAX_CONNECTIONS", "64");
    const auto nonce =
        std::chrono::steady_clock::now().time_since_epoch().count();
    OssObjectStorageAdapter adapter("/mooncake-review-live/" +
                                    std::to_string(getpid()) + "-" +
                                    std::to_string(nonce) + "-" + mode);
    ASSERT_TRUE(adapter.Init());
    LiveOssCleanup cleanup(adapter);
    std::vector<char> source(size);
    for (size_t i = 0; i < size; ++i)
        source[i] = static_cast<char>((i * 17 + 29) & 255);
    const iovec source_iov{source.data(), size};
    std::vector<ObjectPutRequest> puts;
    for (size_t i = 0; i < depth; ++i) {
        cleanup.keys.push_back("object-" + std::to_string(i));
        puts.push_back({cleanup.keys.back(), &source_iov, 1});
    }
    auto uploaded = adapter.PutBatch(puts);
    ASSERT_EQ(uploaded.size(), puts.size());
    for (const auto& result : uploaded) ASSERT_TRUE(result);
    // Allocate and touch all 4 GiB before timed GET; reuse a 256 MiB OSS
    // working set.
    std::vector<char> destination(size * count);
    std::vector<ObjectGetRequest> requests;
    for (size_t i = 0; i < count; ++i)
        requests.push_back(
            {cleanup.keys[i % depth], destination.data() + i * size, size});
    std::vector<ObjectGetRequest> warm_requests(requests.begin(),
                                                requests.begin() + depth);
    auto warm = execute(adapter, warm_requests);
    ASSERT_EQ(warm.size(), depth);
    for (size_t i = 0; i < depth; ++i) {
        ASSERT_TRUE(warm[i]);
        ASSERT_EQ(*warm[i], size);
        ASSERT_EQ(
            std::memcmp(destination.data() + i * size, source.data(), size), 0);
    }
    std::cout
        << "LIVE_OSS_FUNCTIONAL mode=" << mode
        << " objects=64 object_mib=4 batch_put=ok batch_get=ok memcmp=ok\n";
    double total = 0;
    for (size_t round = 0; round < rounds; ++round) {
        auto begin = std::chrono::steady_clock::now();
        auto results = execute(adapter, requests);
        const double seconds = std::chrono::duration<double>(
                                   std::chrono::steady_clock::now() - begin)
                                   .count();
        ASSERT_EQ(results.size(), count);
        for (const auto& result : results) {
            ASSERT_TRUE(result);
            ASSERT_EQ(*result, size);
        }
        total += seconds;
        std::cout << "LIVE_OSS_GET mode=" << mode << " round=" << round
                  << " object_mib=4 depth=64 requests=1024 working_set_mib=256"
                  << " window=continuous-queue-final-drain GiB=4 seconds="
                  << seconds << " GiBps=" << 4.0 / seconds
                  << " memcmp=0 length_check=1 errors=0\n";
    }
    std::cout << "LIVE_OSS_GET_TOTAL mode=" << mode
              << " rounds=3 GiB=12 seconds=" << total
              << " GiBps=" << 12.0 / total << " errors=0\n";
}
}  // namespace mooncake::test
