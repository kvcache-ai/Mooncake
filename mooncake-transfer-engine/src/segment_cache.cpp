// Copyright 2025 KVCache.AI
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

#include "segment_cache.h"

namespace mooncake {
SegmentCache::SegmentCache(Callbacks callbacks,
                           const std::string &local_segment_name)
    : next_segment_id_(1), callbacks_(std::move(callbacks)) {
    auto &entry = segment_entry_map_[LocalSegmentID];
    segment_name_to_id_map_[local_segment_name] = LocalSegmentID;
    entry.ref = std::make_shared<SegmentDesc>();
    entry.name = entry.ref->name = local_segment_name;
    entry.ref->type = MemoryKind;
    entry.ref_cnt = 1;
    entry.dirty = true;
    flushLocal();
}

SegmentCache::~SegmentCache() { destroyLocal(); }

SegmentID SegmentCache::open(const std::string &segment_name) {
    RWSpinlock::WriteGuard guard(segment_lock_);
    if (segment_name_to_id_map_.count(segment_name)) {
        auto id = segment_name_to_id_map_[segment_name];
        auto &entry = segment_entry_map_[id];
        entry.ref_cnt++;
        return id;
    }
    auto segment = callbacks_.on_get_segment_desc(segment_name);
    if (!segment || segment->name != segment_name) {
        return InvalidSegmentID;
    }
    auto id = next_segment_id_.fetch_add(1);
    segment_name_to_id_map_[segment_name] = id;
    segment_entry_map_[id].ref = segment;
    segment_entry_map_[id].ref_cnt = 1;
    segment_entry_map_[id].dirty = false;
    return id;
}

Status SegmentCache::close(SegmentID segment_id) {
    RWSpinlock::WriteGuard guard(segment_lock_);
    if (!segment_entry_map_.count(segment_id)) {
        return Status::InvalidArgument("invalid segment id");
    }
    auto &entry = segment_entry_map_[segment_id];
    if (entry.ref_cnt > 1)
        entry.ref_cnt--;
    else {
        segment_entry_map_.erase(segment_id);
        segment_name_to_id_map_.erase(entry.name);
    }
    return Status::OK();
}

SegmentCache::SegmentDescRef SegmentCache::getLocal() {
    RWSpinlock::ReadGuard guard(segment_lock_);
    auto &entry = segment_entry_map_[LocalSegmentID];
    entry.dirty = true;
    return entry.ref;
}

Status SegmentCache::setAndFlushLocal(SegmentDescRef &&segment) {
    {
        RWSpinlock::ReadGuard guard(segment_lock_);
        auto &entry = segment_entry_map_[LocalSegmentID];
        segment_name_to_id_map_[segment->name] = LocalSegmentID;
        entry.dirty = true;
        entry.ref = std::move(segment);
    }
    return flushLocal();
}

const SegmentCache::SegmentDescRef SegmentCache::get(SegmentID segment_id,
                                                     bool direct) {
    RWSpinlock::ReadGuard guard(segment_lock_);
    if (!segment_entry_map_.count(segment_id)) {
        return nullptr;
    }
    auto &entry = segment_entry_map_.at(segment_id);
    if (!direct) return entry.ref;
    auto segment = callbacks_.on_get_segment_desc(entry.name);
    if (!segment || segment->name != entry.name) {
        return nullptr;
    }
    entry.ref = std::move(segment);
    return entry.ref;
}

Status SegmentCache::flushLocal() {
    RWSpinlock::ReadGuard guard(segment_lock_);
    auto &entry = segment_entry_map_[LocalSegmentID];
    if (entry.dirty) {
        auto status = callbacks_.on_put_segment_desc(entry.ref);
        if (status != Status::OK()) return status;
        entry.dirty = false;
    }
    return Status::OK();
}

Status SegmentCache::destroyLocal() {
    RWSpinlock::WriteGuard guard(segment_lock_);
    if (!segment_entry_map_.count(LocalSegmentID)) {
        return Status::OK();
    }
    auto &entry = segment_entry_map_[LocalSegmentID];
    segment_entry_map_.erase(LocalSegmentID);
    segment_name_to_id_map_.erase(entry.name);
    return callbacks_.on_delete_segment_desc(entry.name);
}

Status SegmentCache::invalidateAll() {
    RWSpinlock::WriteGuard guard(segment_lock_);
    for (auto &name_id_pair : segment_name_to_id_map_) {
        if (name_id_pair.second == LocalSegmentID) continue;
        auto &entry = segment_entry_map_[name_id_pair.second];
        auto segment = callbacks_.on_get_segment_desc(name_id_pair.first);
        entry.ref = std::move(segment);
    }
    return Status::OK();
}
}  // namespace mooncake