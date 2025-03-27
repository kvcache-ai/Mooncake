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

#ifndef SEGMENT_CACHE_H
#define SEGMENT_CACHE_H

#include "common/base/status.h"
#include "segment.h"

namespace mooncake {
class SegmentCache {
   public:
    using SegmentDescRef = std::shared_ptr<SegmentDesc>;

    struct Callbacks {
        std::function<SegmentDescRef(const std::string &)> on_get_segment_desc;
        std::function<Status(SegmentDescRef)> on_put_segment_desc;
        std::function<Status(const std::string &)> on_delete_segment_desc;
    };

    SegmentCache(Callbacks callbacks, const std::string &local_segment_name);

    ~SegmentCache();

    SegmentCache(const SegmentCache &) = delete;

    SegmentCache &operator=(const SegmentCache &) = delete;

    SegmentID open(const std::string &segment_name);

    Status close(SegmentID segment_id);

    const SegmentDescRef get(SegmentID segment_id, bool direct = false);

    SegmentDescRef getLocal();

    Status setAndFlushLocal(SegmentDescRef &&segment);

    Status flushLocal();

    Status destroyLocal();

    Status invalidateAll();

   private:
    struct SegmentEntry {
        std::string name;
        SegmentDescRef ref;
        int ref_cnt;
        bool dirty;
    };

    RWSpinlock segment_lock_;
    std::unordered_map<SegmentID, SegmentEntry> segment_entry_map_;
    std::unordered_map<std::string, SegmentID> segment_name_to_id_map_;
    std::atomic<SegmentID> next_segment_id_;
    Callbacks callbacks_;
};
}  // namespace mooncake

#endif  // SEGMENT_CACHE_H