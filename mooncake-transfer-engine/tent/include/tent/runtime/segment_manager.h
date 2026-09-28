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
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include "tent/runtime/segment.h"

// Fallback refresh interval for cached remote SegmentDesc. Invalidation
// normally comes from the best-effort SegmentUpdate push; this TTL only bounds
// staleness if a push is lost.
#define TENT_SEGMENT_DESC_TTL_MS (60 * 60 * 1000)  // 1h

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

    // Owning-reference variant: the returned SegmentDescRef keeps the desc
    // (and raw pointers into it) alive independently of the thread-local
    // cache's lifetime.
    Status getRemoteCached(SegmentDescRef &desc, SegmentID handle);

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
        SegmentDescRef pin;
        return withCachedSegment(segment_id, pin, operation);
    }

    // Same as above, but additionally hands the caller an owning reference
    // to the segment snapshot in `pin`. Use this variant whenever a raw
    // pointer obtained inside `operation` (e.g. a BufferDesc* from
    // findBuffer()) must remain valid after `operation` returns: the pointer
    // is guaranteed valid only for as long as `pin` is held.
    template <typename Func>
    Status withCachedSegment(SegmentID segment_id, SegmentDescRef &pin,
                             Func operation) {
        static_assert(
            std::is_same_v<std::invoke_result_t<Func &, SegmentDesc *>, Status>,
            "operation must return Status");

        // Local segment: no cache lookup or retry required. Pin the current
        // snapshot so pointers into it survive concurrent updateLocal().
        if (segment_id == LOCAL_SEGMENT_ID) {
            pin = getLocal();
            return operation(pin.get());
        }

        // First get a cached version.
        SegmentDesc *desc = nullptr;
        CHECK_STATUS(getRemoteCached(pin, segment_id));
        desc = pin.get();

        // Do operation under the cached segment.
        Status res = operation(desc);
        if (!res.IsNeedsRefreshCache()) {
            return res;
        }

        // If result status is IsNeedsRefreshCache, invalidate cache and retry
        invalidateRemote(segment_id);
        CHECK_STATUS(getRemoteCached(pin, segment_id));
        desc = pin.get();

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
    // Returns the current immutable snapshot of the local SegmentDesc.
    //
    // Snapshots are copy-on-write: once published they are never mutated, so
    // the returned SegmentDescRef (and raw pointers into it, e.g. from
    // findBuffer()) stays valid and self-consistent for as long as the caller
    // holds the reference. A reader may observe a snapshot that is stale with
    // respect to a concurrent register/unregister, never a torn one —
    // staleness is already a designed-in property of segment metadata (remote
    // peers cache descs with a TTL plus best-effort invalidation pushes).
    //
    // Do NOT write through the returned pointer; all mutations must go
    // through updateLocal().
    SegmentDescRef getLocal() {
        // Per-thread snapshot cache with version-based invalidation — the
        // same pattern getRemoteCached() uses for remote descs. The fast
        // path is one relaxed-acquire load; the shared_mutex is only taken
        // after a publication. Keyed by a monotonic manager id (not `this`)
        // so a recycled allocation can never satisfy a stale cache entry.
        struct Cache {
            uint64_t manager_id = UINT64_MAX;
            uint64_t version = 0;
            SegmentDescRef ref;
        };
        thread_local Cache cache;
        auto version = local_desc_version_.load(std::memory_order_acquire);
        if (cache.manager_id != manager_id_ || cache.version != version ||
            !cache.ref) {
            std::shared_lock<std::shared_mutex> guard(local_desc_lock_);
            cache.ref = local_desc_;
            cache.manager_id = manager_id_;
            // Tag with the pre-lock version: if a publication raced in
            // between, the tag mismatches on the next call and we simply
            // refresh again — the cache can serve a fresher snapshot than
            // its tag, never a staler one.
            cache.version = version;
        }
        return cache.ref;
    }

    // Applies `mutator` to a private clone of the local SegmentDesc and
    // atomically publishes the result as the new snapshot. Mutations are
    // serialized by an internal writer mutex. If `mutator` returns a non-OK
    // status, nothing is published.
    //
    // This is the only way to modify the local SegmentDesc; see getLocal()
    // for the snapshot semantics it guarantees.
    Status updateLocal(const std::function<Status(SegmentDesc &)> &mutator);

    // Returns a serialized JSON snapshot of local_desc_. The result is cached
    // and shared across concurrent GetSegmentDesc RPC handlers; the cache is
    // invalidated by updateLocal() on every publication. This avoids
    // re-dumping the full segment desc on every peer fetch — the previous
    // behavior multiplied dump cost by the number of concurrent peer RPCs and
    // dominated remote getRemote latency.
    std::shared_ptr<const std::string> getLocalDumpedJson();

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

    // Current published snapshot of the local SegmentDesc. Replaced wholesale
    // by updateLocal(); never mutated in place. local_desc_lock_ only guards
    // the pointer swap, not the pointee. std::shared_mutex (rather than the
    // in-tree RWSpinlock) keeps the synchronization visible to
    // ThreadSanitizer.
    std::shared_mutex local_desc_lock_;
    SegmentDescRef local_desc_;
    // Serializes clone-mutate-publish cycles in updateLocal().
    std::mutex local_update_mu_;
    // Serializes {snapshot, putSegmentDesc} pairs in synchronizeLocal() so a
    // stale in-flight put cannot overwrite a newer snapshot in the registry.
    std::mutex local_sync_mu_;
    // Publication counter; invalidates the per-thread snapshot caches in
    // getLocal() and tags local_json_cache_ so that a slow JSON dump of an
    // old snapshot can never overwrite the cache entry of a newer one.
    std::atomic<uint64_t> local_desc_version_{0};
    // Process-unique id for the thread-local cache key in getLocal().
    const uint64_t manager_id_;

    ThreadLocalStorage<RemoteSegmentCache> tl_remote_cache_;

    std::unique_ptr<SegmentRegistry> registry_;

    std::string file_desc_basepath_;

    // shared_ptr to prevent UAF in async callbacks
    std::shared_ptr<RWSpinlock> subscribers_lock_;
    std::shared_ptr<std::unordered_set<std::string>> subscribers_;

    // Cache for the serialized JSON of local_desc_. Invalidated by
    // updateLocal(); local_json_cache_version_ records which publication the
    // cached string was computed from.
    std::mutex local_json_cache_mu_;
    std::shared_ptr<const std::string> local_json_cache_;
    uint64_t local_json_cache_version_{0};
};
}  // namespace tent
}  // namespace mooncake

#endif  // SEGMENT_MANAGER_H