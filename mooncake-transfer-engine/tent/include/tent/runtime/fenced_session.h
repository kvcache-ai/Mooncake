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

// Receiver session table and writer lease cache for RFC #5182 PR 1.
// The receiver mints a lease; the application carries it to the writer.
// submit checks that cache only. Abort, heartbeat, GC, and destination
// checks are not implemented here.

#ifndef TENT_FENCED_SESSION_H
#define TENT_FENCED_SESSION_H

#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "tent/common/status.h"
#include "tent/common/types.h"

namespace mooncake {
namespace tent {

struct SessionId {
    uint64_t hi = 0;
    uint64_t lo = 0;

    bool valid() const { return hi != 0 || lo != 0; }
    friend bool operator==(SessionId a, SessionId b) {
        return a.hi == b.hi && a.lo == b.lo;
    }
    friend bool operator<(SessionId a, SessionId b) {
        return a.hi < b.hi || (a.hi == b.hi && a.lo < b.lo);
    }
};

using SessionHandle = uint64_t;

struct BufferRange {
    uint64_t addr = 0;
    uint64_t length = 0;
};

struct SessionOptions {
    uint32_t max_writers = 16;
    std::chrono::milliseconds lease_ttl{120000};
    bool hard_fence = false;
    std::string tag;
};

struct WriterLease {
    SessionId session_id;
    uint64_t epoch = 0;
    uint32_t writer_id = 0;
    uint64_t expires_at_ns = 0;

    static constexpr uint8_t kSchemaVersion = 1;
    static constexpr size_t kWireBytes = 44;

    // Network byte order. Version byte is kSchemaVersion.
    void serialize(uint8_t out[kWireBytes]) const;
    static Status parse(const uint8_t* bytes, size_t len, WriterLease& out);
};

// One engine holds both sides. The table is the receiver. The cache is the
// writer, filled on the first admitted submit of a lease rather than at mint
// time, so a writer process that only received the bytes can still admit it.
class FencedSessions {
   public:
    FencedSessions();
    explicit FencedSessions(bool enabled);

    bool enabled() const { return enabled_; }
    static bool EnabledFromEnv();
    static uint64_t NowNs();

    Status openSession(SessionHandle& out, SegmentID dst_segment,
                       const std::vector<BufferRange>& dst_ranges,
                       const SessionOptions& opts);

    Status getLease(SessionHandle session, WriterLease& out,
                    uint64_t now_ns = 0);

    // Admits the lease and records batch_id. On StaleLease the batch is not
    // recorded. now_ns == 0 uses NowNs().
    Status admitSubmission(const WriterLease& lease, uint64_t now_ns,
                           BatchID batch_id);

    void forgetSubmission(const WriterLease& lease, BatchID batch_id);

    // Test and later-PR hook. Same epoch stays rejected; a higher epoch
    // replaces it on the next admit.
    Status revokeWriter(SessionId session_id, uint32_t writer_id,
                        uint64_t epoch);

    std::vector<BatchID> submittedBatches(SessionId session_id,
                                          uint32_t writer_id) const;

   private:
    struct SessionRecord {
        SessionId id;
        SegmentID dst_segment = 0;
        std::vector<BufferRange> dst_ranges;
        uint32_t max_writers = 0;
        std::chrono::milliseconds lease_ttl{0};
        uint32_t writers_minted = 0;
        uint64_t epoch = 1;
    };

    struct CacheEntry {
        uint64_t epoch = 0;
        uint64_t expires_at_ns = 0;
        bool revoked = false;
        std::vector<BatchID> batches;
    };

    struct WriterKey {
        SessionId session_id;
        uint32_t writer_id = 0;
        friend bool operator<(WriterKey a, WriterKey b) {
            if (a.session_id < b.session_id) return true;
            if (b.session_id < a.session_id) return false;
            return a.writer_id < b.writer_id;
        }
    };

    Status admitLocked(const WriterLease& lease, uint64_t now_ns,
                       CacheEntry** entry);

    bool enabled_ = false;
    uint64_t nonce_hi_ = 1;
    uint64_t next_lo_ = 1;
    uint64_t next_handle_ = 1;
    bool logged_hard_fence_ = false;
    mutable std::mutex mu_;
    std::map<SessionHandle, SessionRecord> sessions_;
    std::map<WriterKey, CacheEntry> cache_;
};

}  // namespace tent
}  // namespace mooncake

#endif
