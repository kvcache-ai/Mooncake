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
SegmentManager::SegmentManager(std::unique_ptr<SegmentRegistry> agent)
    : next_id_(1), version_(0), registry_(std::move(agent)) {
    local_desc_ = std::make_shared<SegmentDesc>();
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

Status SegmentManager::getRemoteCached(SegmentDesc *&desc, SegmentID handle) {
    auto &cache = tl_remote_cache_.get();
    auto current_ts = getCurrentTimeInNano();
    auto current_version = version_.load(std::memory_order_relaxed);
    if (current_ts - cache.last_refresh > ttl_ms_ * 1000000 ||
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
        std::string local_rpc_addr = local_desc_->rpc_server_addr;
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
    desc = cache.id_to_desc_map[handle].get();
    assert(desc);
    return Status::OK();
}

Status SegmentManager::getRemote(SegmentDescRef &desc, SegmentID handle) {
    RWSpinlock::WriteGuard guard(lock_);
    if (!id_to_name_map_.count(handle)) {
        return Status::InvalidArgument("Invalid segment handle" LOC_MARK);
    }
    auto segment_name = id_to_name_map_[handle];
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
    RWSpinlock::WriteGuard guard(subscribers_lock_);
    subscribers_.insert(subscriber_addr);
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
    desc->machine_id = local_desc_->machine_id;
    FileSegmentDesc detail;
    FileBufferDesc buffer;
    buffer.path = path;
    buffer.length = st.st_size;
    buffer.offset = 0;
    detail.buffers.push_back(buffer);
    desc->detail = detail;
    return Status::OK();
}

Status SegmentManager::synchronizeLocal() {
    CHECK_STATUS(registry_->putSegmentDesc(local_desc_));

    RWSpinlock::ReadGuard guard(subscribers_lock_);
    if (subscribers_.empty()) return Status::OK();

    for (const auto &subscriber : subscribers_) {
        // Remove subscribers that have failed (e.g., peer might shutdown)
        // to avoid repeated RPC failures.
        ControlClient::notifySegmentUpdatedAsync(
            subscriber, local_desc_->name, [this, subscriber] {
                RWSpinlock::WriteGuard guard(subscribers_lock_);
                subscribers_.erase(subscriber);
            });
    }
    return Status::OK();
}

Status SegmentManager::deleteLocal() {
    return registry_->deleteSegmentDesc(local_desc_->name);
}

}  // namespace tent
}  // namespace mooncake
