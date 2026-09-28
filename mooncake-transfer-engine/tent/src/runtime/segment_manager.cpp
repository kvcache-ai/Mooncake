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

#include "tent/runtime/segment_manager.h"

#include <cassert>
#include <filesystem>
#include <set>

#include "tent/common/status.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/segment_registry.h"
#include "tent/common/utils/os.h"

namespace mooncake {
namespace tent {
namespace {
uint64_t nextManagerId() {
    static std::atomic<uint64_t> counter{0};
    return counter.fetch_add(1, std::memory_order_relaxed);
}
}  // namespace

SegmentManager::SegmentManager(std::unique_ptr<SegmentRegistry> agent)
    : next_id_(1),
      version_(0),
      manager_id_(nextManagerId()),
      registry_(std::move(agent)) {
    local_desc_ = std::make_shared<SegmentDesc>();
    subscribers_lock_ = std::make_shared<RWSpinlock>();
    subscribers_ = std::make_shared<std::unordered_set<std::string>>();
}

SegmentManager::~SegmentManager() {}

Status SegmentManager::openRemote(SegmentID &handle,
                                  const std::string &segment_name) {
    RWSpinlock::WriteGuard guard(lock_);
    if (name_to_id_map_.count(segment_name)) {
        handle = name_to_id_map_.at(segment_name);
    } else {
        handle = next_id_.fetch_add(1, std::memory_order_relaxed);
        name_to_id_map_[segment_name] = handle;
        id_to_name_map_[handle] = segment_name;
        LOG(INFO) << "Opened segment #" << handle << ": " << segment_name;
        version_.fetch_add(1, std::memory_order_relaxed);
    }
    return Status::OK();
}

Status SegmentManager::closeRemote(SegmentID handle) {
    RWSpinlock::WriteGuard guard(lock_);
    if (!id_to_name_map_.count(handle))
        return Status::InvalidArgument("Invalid segment handle" LOC_MARK);
    auto segment_name = id_to_name_map_[handle];
    name_to_id_map_.erase(segment_name);
    id_to_name_map_.erase(handle);
    version_.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
}

Status SegmentManager::updateLocal(
    const std::function<Status(SegmentDesc &)> &mutator) {
    std::lock_guard<std::mutex> g(local_update_mu_);
    // No concurrent writer exists (serialized by local_update_mu_), so
    // reading local_desc_ without local_desc_lock_ is safe here.
    auto next = std::make_shared<SegmentDesc>(*local_desc_);
    CHECK_STATUS(mutator(*next));
    {
        std::unique_lock<std::shared_mutex> guard(local_desc_lock_);
        local_desc_ = std::move(next);
    }
    // Release pairs with the acquire load in getLocal(): a reader that
    // observes the new version must also observe the pointer swap above.
    // With a relaxed bump, on weakly-ordered architectures the version
    // store may become visible before the swap, letting a reader tag the
    // OLD snapshot with the NEW version — which its thread-local cache
    // would then serve until the next publication.
    local_desc_version_.fetch_add(1, std::memory_order_release);
    {
        std::lock_guard<std::mutex> jg(local_json_cache_mu_);
        local_json_cache_.reset();
    }
    return Status::OK();
}

Status SegmentManager::getRemoteCached(SegmentDesc *&desc, SegmentID handle) {
    SegmentDescRef ref;
    CHECK_STATUS(getRemoteCached(ref, handle));
    desc = ref.get();
    return Status::OK();
}

Status SegmentManager::getRemoteCached(SegmentDescRef &desc, SegmentID handle) {
    auto &cache = tl_remote_cache_.get();
    auto current_ts = getCurrentTimeInNano();
    auto current_version = version_.load(std::memory_order_relaxed);
    if (current_ts - cache.last_refresh >
            static_cast<uint64_t>(TENT_SEGMENT_DESC_TTL_MS) * 1000000 ||
        cache.version != current_version) {
        cache.id_to_desc_map.clear();
        cache.last_refresh = current_ts;
        cache.version = current_version;
    }
    if (!cache.id_to_desc_map.count(handle)) {
        SegmentDescRef desc_ref;
        auto status = getRemote(desc_ref, handle);
        if (!status.ok()) return status;
        cache.id_to_desc_map[handle] = desc_ref;

        std::string peer_rpc_addr = desc_ref->rpc_server_addr;
        std::string local_rpc_addr = getLocal()->rpc_server_addr;
        if (!peer_rpc_addr.empty() && !local_rpc_addr.empty()) {
            // Send a subscription request to enable proactive cache
            // invalidation. This is a best-effort mechanism to reduce stale
            // cache hits. Errors are logged and ignored; correctness should not
            // depend on this.
            ControlClient::subscribeSegmentUpdateAsync(peer_rpc_addr,
                                                       local_rpc_addr);
        } else {
            LOG(ERROR) << "Unexpected empty RPC address, peer: '"
                       << peer_rpc_addr << "', local: '" << local_rpc_addr
                       << "'.";
        }
    }
    desc = cache.id_to_desc_map[handle];
    assert(desc);
    return Status::OK();
}

Status SegmentManager::getRemote(SegmentDescRef &desc, SegmentID handle) {
    // Only the id_to_name lookup needs the lock.
    std::string segment_name;
    {
        RWSpinlock::ReadGuard guard(lock_);
        auto it = id_to_name_map_.find(handle);
        if (it == id_to_name_map_.end()) {
            return Status::InvalidArgument("Invalid segment handle" LOC_MARK);
        }
        segment_name = it->second;
    }
    if (segment_name.starts_with(kLocalFileSegmentPrefix)) {
        CHECK_STATUS(makeFileRemote(desc, segment_name));
    } else {
        CHECK_STATUS(registry_->getSegmentDesc(desc, segment_name));
    }
    return Status::OK();
}

Status SegmentManager::getRemote(SegmentDescRef &desc,
                                 const std::string &segment_name) {
    return registry_->getSegmentDesc(desc, segment_name);
}

Status SegmentManager::invalidateRemote(SegmentID handle) {
    if (handle == LOCAL_SEGMENT_ID) return Status::OK();
    auto &cache = tl_remote_cache_.get();
    cache.id_to_desc_map.erase(handle);
    return Status::OK();
}

Status SegmentManager::invalidateAllCacheForRemote(
    const std::string &segment_name) {
    // TODO: Optimize if this becomes a bottleneck.
    // Currently, this invalidates all cached segments globally instead of
    // targeting just the specified segment. This is acceptable because
    // segment updates should be infrequent.
    // A per-segment versioning scheme could be more precise but it could
    // introduce additional complexity and might slightly degrade the
    // fast-path performance of `getRemoteCached`.
    (void)segment_name;
    version_.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
}

void SegmentManager::addSubscriber(const std::string &subscriber_addr) {
    RWSpinlock::WriteGuard guard(*subscribers_lock_);
    subscribers_->insert(subscriber_addr);
}

Status SegmentManager::makeFileRemote(SegmentDescRef &desc,
                                      const std::string &segment_name) {
    std::string path = segment_name.substr(kLocalFileSegmentPrefix.length());
    if (!file_desc_basepath_.empty()) {
        if (file_desc_basepath_.ends_with("/"))
            path = file_desc_basepath_ + path;
        else
            path = file_desc_basepath_ + "/" + path;
    }

    struct stat st;
    if (stat(path.c_str(), &st) || !S_ISREG(st.st_mode))
        return Status::InvalidArgument(std::string("Invalid path: ") + path);

    desc = std::make_shared<SegmentDesc>();
    desc->name = segment_name;
    desc->type = SegmentType::File;
    desc->machine_id = getLocal()->machine_id;
    FileSegmentDesc detail;
    FileBufferDesc buffer;
    buffer.path = path;
    buffer.length = st.st_size;
    buffer.offset = 0;
    detail.buffers.push_back(buffer);
    desc->detail = detail;
    return Status::OK();
}

std::shared_ptr<const std::string> SegmentManager::getLocalDumpedJson() {
    // Capture the snapshot together with its publication version so a slow
    // dump of an older snapshot can never overwrite the cache entry computed
    // from a newer one. Acquire keeps the tag conservative: the snapshot
    // read below is then guaranteed to be at least as new as the tag.
    auto version = local_desc_version_.load(std::memory_order_acquire);
    auto snapshot = getLocal();
    {
        std::lock_guard<std::mutex> g(local_json_cache_mu_);
        if (local_json_cache_ && local_json_cache_version_ == version)
            return local_json_cache_;
    }
    json j = *snapshot;
    auto computed = std::make_shared<const std::string>(j.dump());

    std::lock_guard<std::mutex> g(local_json_cache_mu_);
    // Store only if no publication happened since we sampled `version`;
    // otherwise this dump is already outdated and must not evict a cache
    // entry computed from a newer snapshot.
    if (local_desc_version_.load(std::memory_order_relaxed) == version &&
        !local_json_cache_) {
        local_json_cache_ = computed;
        local_json_cache_version_ = version;
    }
    return computed;
}

Status SegmentManager::synchronizeLocal() {
    {
        // Serialize {snapshot, put} pairs: without this, a put carrying an
        // older snapshot could complete after (and overwrite) one carrying a
        // newer snapshot in the registry, hiding a completed registration
        // from peers until the next synchronizeLocal call.
        std::lock_guard<std::mutex> g(local_sync_mu_);
        auto snapshot = getLocal();
        CHECK_STATUS(registry_->putSegmentDesc(snapshot));
    }

    std::vector<std::string> subscribers_snapshot;
    {
        RWSpinlock::ReadGuard guard(*subscribers_lock_);
        subscribers_snapshot.reserve(subscribers_->size());
        for (const auto &subscriber : *subscribers_) {
            subscribers_snapshot.emplace_back(subscriber);
        }
    }

    // Avoid holding the lock while issuing RPCs. Since the async RPC is
    // based on coroutine, the callback might be executed on the current thread
    // (e.g., if an error occurs before the first suspension point).
    // Holding the lock here could lead to a deadlock.
    for (const auto &subscriber : subscribers_snapshot) {
        // Remove subscribers that have failed (e.g., peer might shutdown)
        // to avoid repeated RPC failures.
        ControlClient::notifySegmentUpdatedAsync(
            subscriber, getLocal()->name,
            /* on_failure */
            [subscribers = subscribers_, lock = subscribers_lock_, subscriber] {
                RWSpinlock::WriteGuard guard(*lock);
                subscribers->erase(subscriber);
            });
    }
    return Status::OK();
}

Status SegmentManager::deleteLocal() {
    return registry_->deleteSegmentDesc(getLocal()->name);
}

}  // namespace tent
}  // namespace mooncake
