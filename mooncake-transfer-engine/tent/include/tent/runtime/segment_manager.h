// Copyright 2024 KVCache.AI
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

#ifndef SEGMENT_MANAGER_H
#define SEGMENT_MANAGER_H

#include <glog/logging.h>
#include <netdb.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include "tent/runtime/segment.h"

namespace mooncake {
namespace tent {
class SegmentRegistry;

class SegmentManager {
   public:
    SegmentManager(std::unique_ptr<SegmentRegistry> registry);

    ~SegmentManager();

    SegmentManager(const SegmentManager &) = delete;
    SegmentManager &operator=(const SegmentManager &) = delete;

   public:
    Status openRemote(SegmentID &handle, const std::string &segment_name);

    Status closeRemote(SegmentID handle);

    // Use `withCachedSegment()` when you want automatic
    // cache invalidation and retry on stale segment cache.
    Status getRemoteCached(SegmentDesc *&desc, SegmentID handle);

    Status getRemote(SegmentDescRef &desc, const std::string &segment_name);

    // Invalidates the thread-local cache.
    // This only affects the calling thread's cache.
    Status invalidateRemote(SegmentID handle);

    // Invalidates all threads' caches.
    Status invalidateAllCacheForRemote(const std::string &segment_name);

    // Execute an operation with automatic cache invalidation and retry.
    //
    // This helper wraps segment cache lookups with automatic retry logic:
    // 1. For local segment, the operation is executed directly without
    //    caching or retrying.
    // 2. For remote segments, it first tries the cached segment.
    //    If the operation returns NeedsRefreshCache, the cache is
    //    invalidated, the segment is refetched and the operation is retried.
    template <typename Func>
    Status withCachedSegment(SegmentID segment_id, Func operation) {
        static_assert(
            std::is_same_v<std::invoke_result_t<Func &, SegmentDesc *>, Status>,
            "operation must return Status");

        // Local segment: no cache lookup or retry required.
        if (segment_id == LOCAL_SEGMENT_ID) {
            return operation(getLocal().get());
        }

        // First get a cached version.
        SegmentDesc *desc = nullptr;
        CHECK_STATUS(getRemoteCached(desc, segment_id));

        // Do operation under the cached segment.
        Status res = operation(desc);
        if (!res.IsNeedsRefreshCache()) {
            return res;
        }

        // If result status is IsNeedsRefreshCache, invalidate cache and retry
        invalidateRemote(segment_id);
        CHECK_STATUS(getRemoteCached(desc, segment_id));

        // Do operation again
        res = operation(desc);

        // If the operation still fails, return the error.
        // Convert the status to InvalidEntry if it is NeedsRefreshCache.
        if (res.IsNeedsRefreshCache()) {
            res = Status::InvalidEntry(
                "Segment refetched from registry but still invalid: " +
                std::string{res.message()});
        }
        return res;
    }

   public:
    SegmentDescRef getLocal() { return local_desc_; }

    Status synchronizeLocal();

    Status deleteLocal();

    // Registers a peer's RPC address to receive proactive cache invalidation
    // notifications when the local segment is updated.
    void addSubscriber(const std::string &subscriber_addr);

   private:
    Status getRemote(SegmentDescRef &desc, SegmentID handle);

    Status makeFileRemote(SegmentDescRef &desc,
                          const std::string &segment_name);

   private:
    struct RemoteSegmentCache {
        uint64_t last_refresh = 0;
        uint64_t version = 0;
        std::unordered_map<SegmentID, SegmentDescRef> id_to_desc_map;
    };

   private:
    RWSpinlock lock_;
    std::unordered_map<SegmentID, std::string> id_to_name_map_;
    std::unordered_map<std::string, SegmentID> name_to_id_map_;
    std::atomic<SegmentID> next_id_;

    std::atomic<uint64_t> version_;

    SegmentDescRef local_desc_;
    ThreadLocalStorage<RemoteSegmentCache> tl_remote_cache_;

    std::unique_ptr<SegmentRegistry> registry_;

    std::string file_desc_basepath_;
    uint64_t ttl_ms_ = 10 * 1000;  // N.B. Frequent TTL harms p999

    RWSpinlock subscribers_lock_;
    std::unordered_set<std::string> subscribers_;
};
}  // namespace tent
}  // namespace mooncake

#endif  // SEGMENT_MANAGER_H