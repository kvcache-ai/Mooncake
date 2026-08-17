// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Tests RedisMetaStore without a live server: a fake runCommand() feeds the
// real get()/set()/remove() paths fabricated replies. Covers the two fixes:
// (1) client_mutex_ serializes concurrent ops; (2) get() keeps embedded NULs.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdarg>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "tent/metastore/redis.h"

namespace mooncake {
namespace tent {
namespace {

// Heap redisReply that freeReplyObject() can release: it frees str and the
// struct with malloc/free, so allocate the same way; calloc zeros the rest.
redisReply *makeStringReply(const std::string &data) {
    auto *reply = static_cast<redisReply *>(std::calloc(1, sizeof(redisReply)));
    reply->type = REDIS_REPLY_STRING;
    reply->len = data.size();
    reply->str = static_cast<char *>(std::malloc(data.size() + 1));
    std::memcpy(reply->str, data.data(), data.size());
    reply->str[data.size()] = '\0';
    return reply;
}

redisReply *makeStatusReply(const char *status) {
    auto *reply = static_cast<redisReply *>(std::calloc(1, sizeof(redisReply)));
    reply->type = REDIS_REPLY_STATUS;
    const size_t len = std::strlen(status);
    reply->len = len;
    reply->str = static_cast<char *>(std::malloc(len + 1));
    std::memcpy(reply->str, status, len + 1);
    return reply;
}

// Fake store: runCommand() fabricates replies and flags if two commands are
// ever in flight at once. Without client_mutex_ the widened window overlaps.
class FakeRedisMetaStore : public RedisMetaStore {
   public:
    // connected_ is protected; client_ stays nullptr (redisFree() tolerates it).
    FakeRedisMetaStore() { connected_ = true; }

    std::atomic<int> in_flight_{0};
    std::atomic<bool> saw_concurrent_{false};
    std::atomic<int> command_count_{0};

    // Embedded NUL proves get() does not truncate at the first '\0'.
    const std::string stored_value_ = std::string("seg\0desc", 8);

   protected:
    redisReply *runCommand(const char *format, va_list /*ap*/) override {
        command_count_.fetch_add(1, std::memory_order_relaxed);
        if (in_flight_.fetch_add(1, std::memory_order_acq_rel) + 1 > 1) {
            saw_concurrent_.store(true, std::memory_order_relaxed);
        }
        // Widen the critical section so a missing lock reliably overlaps.
        std::this_thread::sleep_for(std::chrono::microseconds(200));
        in_flight_.fetch_sub(1, std::memory_order_acq_rel);

        if (std::strncmp(format, "GET", 3) == 0) {
            return makeStringReply(stored_value_);
        }
        return makeStatusReply("OK");
    }
};

// get() must preserve the full binary value, including embedded NULs.
TEST(RedisMetaStore, GetPreservesEmbeddedNul) {
    FakeRedisMetaStore store;
    std::string value;
    Status status = store.get("segment/key", value);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(value.size(), store.stored_value_.size());
    EXPECT_EQ(value, store.stored_value_);
}

// client_mutex_ must serialize concurrent get()/set()/remove() so that the
// shared redisContext is never used from two threads at once.
TEST(RedisMetaStore, ConcurrentAccessIsSerialized) {
    FakeRedisMetaStore store;
    constexpr int kThreads = 8;
    constexpr int kItersPerThread = 40;

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&store] {
            for (int i = 0; i < kItersPerThread; ++i) {
                std::string value;
                store.get("segment/key", value);
                store.set("segment/key", "payload");
                store.remove("segment/key");
            }
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }

    EXPECT_FALSE(store.saw_concurrent_.load())
        << "client_mutex_ failed to serialize access to the shared client";
    EXPECT_EQ(store.command_count_.load(), kThreads * kItersPerThread * 3);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
