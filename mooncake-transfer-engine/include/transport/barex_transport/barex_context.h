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

#ifndef BAREX_CONTEXT_H_
#define BAREX_CONTEXT_H_

#include <infiniband/verbs.h>

#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common.h"
#include "transport/transport.h"

#ifdef USE_BAREX
#include <accl/barex/barex.h>
#include <accl/barex/xcontext.h>
#include <accl/barex/xlistener.h>
#include <accl/barex/xconnector.h>
#include <accl/barex/xsimple_mempool.h>
#include <accl/barex/xthreadpool.h>
#include <accl/barex/xtimer.h>
#include <accl/barex/xconfig_util.h>
#endif

namespace mooncake {

#ifdef USE_BAREX

using namespace accl::barex;
using XChannel = accl::barex::XChannel;
using SegmentID = Transport::SegmentID;
using XContext = accl::barex::XContext;
using BarexResult = accl::barex::BarexResult;

class ChannelCache {
   public:
    // put channel
    void put(SegmentID key, int nic_id, XChannel* channel) {
        RWSpinlock::WriteGuard guard(lock_);
        auto& channels = cache_[key];
        auto& vec = channels[nic_id];
        status_map_[key] = true;
        vec.push_back(channel);
    }

    // get channel
    XChannel* find(SegmentID key, int nic_id, int idx) {
        RWSpinlock::ReadGuard guard(lock_);
        auto it = cache_.find(key);
        if (it == cache_.end()) return nullptr;
        auto& channels = it->second;
        auto ch_it = channels.find(nic_id);
        if (ch_it == channels.end()) return nullptr;
        auto& vec = ch_it->second;
        if (idx >= 0 && idx < static_cast<int>(vec.size())) {
            return vec[idx];
        }
        return nullptr;
    }

    // delete channel
    bool erase(SegmentID key, int nic_id, int idx) {
        RWSpinlock::WriteGuard guard(lock_);
        auto it = cache_.find(key);
        if (it == cache_.end()) return false;

        auto& channels = it->second;
        auto ch_it = channels.find(nic_id);
        if (ch_it == channels.end()) return false;

        auto& vec = ch_it->second;
        if (idx < 0 || idx >= static_cast<int>(vec.size())) return false;

        vec.erase(vec.begin() + idx);
        status_map_[key] = false;
        if (vec.empty()) {
            channels.erase(ch_it);
            if (channels.empty()) {
                cache_.erase(it);
            }
        }
        return true;
    }

    // get channel state
    bool CheckAllChannels(SegmentID segment_id) {
        RWSpinlock::ReadGuard guard(lock_);
        auto it = cache_.find(segment_id);
        if (it == cache_.end()) {
            return false;
        }
        auto& inner_map = it->second;
        for (auto& pair : inner_map) {
            auto& channels = pair.second;
            for (XChannel* channel : channels) {
                if (!channel->IsActive()) {
                    return false;
                }
            }
        }
        return true;
    }

    // check and delete invalid channels
    int RemoveInvalidChannels(SegmentID segment_id) {
        RWSpinlock::WriteGuard guard(lock_);
        auto it = cache_.find(segment_id);
        if (it == cache_.end()) {
            return 0;
        }

        int invalid_count = 0;
        auto& inner_map = it->second;

        for (auto& pair : inner_map) {
            auto& channels = pair.second;
            auto new_end = std::remove_if(
                channels.begin(), channels.end(),
                [](XChannel* channel) { return !channel->IsActive(); });
            invalid_count += std::distance(new_end, channels.end());
            channels.erase(new_end, channels.end());
        }
        return invalid_count;
    }

    // get all channels
    std::vector<XChannel*> copyAll() {
        RWSpinlock::WriteGuard guard(lock_);
        std::vector<XChannel*> result;
        for (const auto& [key, channels] : cache_) {
            for (const auto& [nic_id, vec] : channels) {
                result.insert(result.end(), vec.begin(), vec.end());
            }
        }
        return result;
    }

   private:
    std::unordered_map<SegmentID,
                       std::unordered_map<int, std::vector<XChannel*>>>
        cache_;
    std::unordered_map<SegmentID, bool> status_map_;
    RWSpinlock lock_;
};
class BarexContext {
   public:
    int submitPostSend(const std::vector<Transport::Slice*>& slice_list);
    int addChannel(SegmentID sid, int device_id, XChannel* ch);
    XChannel* getChannel(SegmentID sid, int device_id, int idx);
    int checkStatus(SegmentID sid);
    XContext* getCtx();
    // int ClearAllChannel();
    std::vector<XChannel*> getAllChannel();
    bool active() const { return active_; }
    void setQpNum(int qp_num) { qp_num_per_ctx_ = qp_num; }
    int getQpNum() const { return qp_num_per_ctx_; }

   public:
    BarexContext(XContext* xcontext, bool use_cpu, int device_id);

    ~BarexContext();

    XContext* xcontext_;
    bool barex_use_cpu_;
    int barex_local_device_;

   private:
    ChannelCache channel_cache_;
    bool active_ = true;
    int qp_num_per_ctx_ = 2;
};
#endif
}  // namespace mooncake

#endif  // BAREX_CONTEXT_H_