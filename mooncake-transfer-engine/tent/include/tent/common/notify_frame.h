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

#ifndef TENT_NOTIFY_FRAME_H
#define TENT_NOTIFY_FRAME_H

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>

#include "tent/common/types.h"

namespace mooncake {
namespace tent {

// Typed notification frame: the classic engine's ctrl_frame v1 byte for byte
// (rdma_twosided/ctrl_frame.h), so both engines speak one format; that is why
// epoch/ack_seq/flags are kept. Its payload is the raw encoding, whose leading
// name length can never equal the magic, so the magic tells the two apart.
constexpr uint32_t kNotifyFrameMagic = 0x434B434Du;  // 'MCKC'
constexpr uint8_t kNotifyFrameVersion = 1;
constexpr uint8_t kNotifyFrameTypeNotifyCompat = 10;
constexpr size_t kNotifyFrameHeaderSize = 44;
constexpr uint16_t kNotifyFrameKnownFlagsMask = 0x0001;  // NeedsAck

struct NotifyFrameHeader {
    uint8_t version = kNotifyFrameVersion;
    uint8_t type = kNotifyFrameTypeNotifyCompat;
    uint16_t flags = 0;
    uint64_t session = 0;
    uint64_t epoch = 0;
    uint64_t seq = 0;
    uint64_t ack_seq = 0;
};

bool isNotifyFrameMagic(const char* data, size_t len);

// Raw payload; decoding tolerates trailing bytes, as receivers always have.
bool encodeNotifyPayload(const std::string& name, const std::string& msg,
                         char* out, size_t capacity, uint32_t* out_len);
bool decodeNotifyPayload(const char* data, size_t len, std::string* name,
                         std::string* msg);

// Whole frame; decoding rejects anything it does not fully understand.
bool encodeNotifyFrame(const NotifyFrameHeader& header, const std::string& name,
                       const std::string& msg, char* out, size_t capacity,
                       uint32_t* out_len);
bool decodeNotifyFrame(const char* data, size_t len, NotifyFrameHeader* header,
                       std::string* name, std::string* msg);

class Config;

// How many notifications a receive queue holds before an arrival drops the
// oldest unread one: notification/recv_backlog_max, 1 to the limit, else this.
constexpr size_t kNotifyRecvBacklogDefault = 65536;
constexpr size_t kNotifyRecvBacklogLimit = size_t{1} << 20;
size_t loadNotifyRecvBacklogMax(const Config& conf);

// Stamps a sequence number per target segment name, under a random session
// per name, so reopening a segment continues its count. A notification already
// stamped for this target (a resend) keeps its stamp; any other is restamped.
class NotifySequencer {
   public:
    using Clock = std::chrono::steady_clock;
    static constexpr size_t kPruneEvery = 4096;

    Notification stamp(const std::string& target, const Notification& notifi,
                       Clock::time_point now = Clock::now());

   private:
    struct Target {
        uint64_t session;
        uint64_t last_seq = 0;
        Clock::time_point last_used{};
    };

    std::mutex mutex_;
    std::unordered_map<std::string, Target> targets_;
    size_t stamps_since_prune_ = 0;
};

// Receiver-side filter per sender session: each (session, seq) is admitted
// once, however late, while the hole [lo, hi) it fell in is open. A hole is
// given up after ttl (expired) or as the oldest past max_holes (evicted).
class NotifyDedupFilter {
   public:
    using Clock = std::chrono::steady_clock;
    static constexpr size_t kPruneEvery = 4096;

    // What a filter refuses or gives up, one at a time: the metrics are
    // process-wide, while expired() and evicted() count this filter alone.
    enum class Drop { Duplicate, Expired, HoleExpired, HoleEvicted };

    struct Config {
        std::chrono::milliseconds ttl{600000};
        size_t max_holes = 65536;
        std::function<void(Drop)> on_drop;  // called under the filter's lock
    };

    void configure(const Config& config);

    // seq 0 (unstamped) is always admitted.
    bool admit(uint64_t session, uint64_t seq,
               Clock::time_point now = Clock::now());

    size_t sessions();
    size_t holes();
    size_t expired() const;
    size_t evicted() const;

   private:
    struct Hole {
        uint64_t hi;
        Clock::time_point created;
        // Below a session's first seq, as a restarted receiver sees one: not
        // known to be missing, so given up without counting.
        bool before_first = false;
    };
    using Holes = std::map<uint64_t, Hole>;

    struct SessionState {
        uint64_t max_seq = 0;
        Holes holes;
        Clock::time_point last_seen;
    };

    void pruneLocked(Clock::time_point now);
    void giveUpLocked(uint64_t session, Holes& holes, Holes::iterator hole,
                      Drop drop);

    std::mutex mutex_;
    Config config_;
    std::unordered_map<uint64_t, SessionState> sessions_;
    size_t admits_since_prune_ = 0;
    std::atomic<size_t> expired_{0};
    std::atomic<size_t> evicted_{0};
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_NOTIFY_FRAME_H
