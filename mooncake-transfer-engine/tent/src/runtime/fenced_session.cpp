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

#include "tent/runtime/fenced_session.h"

#include <cstdlib>
#include <cstring>
#include <random>

#include "glog/logging.h"

namespace mooncake {
namespace tent {
namespace {

constexpr uint64_t kNoExpiry = ~uint64_t{0};

void StoreBe32(uint8_t* p, uint32_t v) {
    p[0] = static_cast<uint8_t>(v >> 24);
    p[1] = static_cast<uint8_t>(v >> 16);
    p[2] = static_cast<uint8_t>(v >> 8);
    p[3] = static_cast<uint8_t>(v);
}

void StoreBe64(uint8_t* p, uint64_t v) {
    for (int i = 7; i >= 0; --i) {
        p[i] = static_cast<uint8_t>(v);
        v >>= 8;
    }
}

uint32_t LoadBe32(const uint8_t* p) {
    return (uint32_t{p[0]} << 24) | (uint32_t{p[1]} << 16) |
           (uint32_t{p[2]} << 8) | uint32_t{p[3]};
}

uint64_t LoadBe64(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | p[i];
    return v;
}

uint64_t RandomU64() {
    std::random_device rd;
    uint64_t value = 0;
    for (int i = 0; i < 2; ++i) {
        value = (value << 32) | static_cast<uint32_t>(rd());
    }
    return value == 0 ? 1 : value;
}

}  // namespace

void WriterLease::serialize(uint8_t out[kWireBytes]) const {
    std::memset(out, 0, kWireBytes);
    out[0] = kSchemaVersion;
    StoreBe64(out + 4, session_id.hi);
    StoreBe64(out + 12, session_id.lo);
    StoreBe64(out + 20, epoch);
    StoreBe32(out + 28, writer_id);
    StoreBe64(out + 36, expires_at_ns);
}

Status WriterLease::parse(const uint8_t* bytes, size_t len, WriterLease& out) {
    if (bytes == nullptr || len != kWireBytes) {
        return Status::InvalidArgument("writer lease wire length");
    }
    if (bytes[1] != 0 || bytes[2] != 0 || bytes[3] != 0 ||
        LoadBe32(bytes + 32) != 0) {
        return Status::InvalidArgument("writer lease reserved bytes");
    }
    if (bytes[0] != kSchemaVersion) {
        return Status::InvalidArgument("writer lease schema version");
    }
    out.session_id.hi = LoadBe64(bytes + 4);
    out.session_id.lo = LoadBe64(bytes + 12);
    out.epoch = LoadBe64(bytes + 20);
    out.writer_id = LoadBe32(bytes + 28);
    out.expires_at_ns = LoadBe64(bytes + 36);
    return Status::OK();
}

FencedSessions::FencedSessions() : FencedSessions(EnabledFromEnv()) {}

FencedSessions::FencedSessions(bool enabled)
    : enabled_(enabled), nonce_hi_(RandomU64()) {}

bool FencedSessions::EnabledFromEnv() {
    const char* value = std::getenv("MC_TE_FENCED_SESSIONS");
    if (value == nullptr) return false;
    return std::strcmp(value, "1") == 0 || std::strcmp(value, "true") == 0 ||
           std::strcmp(value, "TRUE") == 0 || std::strcmp(value, "on") == 0 ||
           std::strcmp(value, "ON") == 0;
}

uint64_t FencedSessions::NowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

Status FencedSessions::openSession(SessionHandle& out, SegmentID dst_segment,
                                   const std::vector<BufferRange>& dst_ranges,
                                   const SessionOptions& opts) {
    if (!enabled_) {
        return Status::NotImplemented("fenced sessions are disabled");
    }
    if (dst_ranges.empty()) {
        return Status::InvalidArgument("session requires destination ranges");
    }
    for (const BufferRange& range : dst_ranges) {
        if (range.length == 0) {
            return Status::InvalidArgument("destination range length is 0");
        }
    }
    if (opts.max_writers == 0) {
        return Status::InvalidArgument("max_writers is 0");
    }
    if (opts.lease_ttl.count() < 0) {
        return Status::InvalidArgument("lease_ttl is negative");
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (opts.hard_fence && !logged_hard_fence_) {
        logged_hard_fence_ = true;
        LOG(WARNING) << "hard_fence=true is a no-op in fenced-sessions v1";
    }
    SessionRecord record;
    record.id.hi = nonce_hi_;
    record.id.lo = next_lo_++;
    record.dst_segment = dst_segment;
    record.dst_ranges = dst_ranges;
    record.max_writers = opts.max_writers;
    record.lease_ttl = opts.lease_ttl;
    SessionHandle handle = next_handle_++;
    sessions_.emplace(handle, std::move(record));
    out = handle;
    return Status::OK();
}

Status FencedSessions::getLease(SessionHandle session, WriterLease& out,
                                uint64_t now_ns) {
    if (!enabled_) {
        return Status::NotImplemented("fenced sessions are disabled");
    }
    if (now_ns == 0) now_ns = NowNs();
    std::lock_guard<std::mutex> lock(mu_);
    auto it = sessions_.find(session);
    if (it == sessions_.end()) {
        return Status::InvalidArgument("unknown session handle");
    }
    SessionRecord& record = it->second;
    if (record.writers_minted >= record.max_writers) {
        return Status::TooManyRequests("session writer limit");
    }
    uint64_t expires = kNoExpiry;
    if (record.lease_ttl.count() > 0) {
        uint64_t ttl_ns =
            static_cast<uint64_t>(record.lease_ttl.count()) * 1000000ull;
        expires = now_ns > kNoExpiry - ttl_ns ? kNoExpiry : now_ns + ttl_ns;
    }
    ++record.writers_minted;
    out.session_id = record.id;
    out.epoch = record.epoch;
    out.writer_id = record.writers_minted;
    out.expires_at_ns = expires;
    return Status::OK();
}

Status FencedSessions::admitLocked(const WriterLease& lease, uint64_t now_ns,
                                   CacheEntry** entry) {
    if (!lease.session_id.valid() || lease.writer_id == 0 || lease.epoch == 0 ||
        lease.expires_at_ns == 0) {
        return Status::StaleLease("malformed writer lease");
    }
    if (lease.expires_at_ns <= now_ns) {
        return Status::StaleLease("writer lease expired");
    }
    WriterKey key{lease.session_id, lease.writer_id};
    auto it = cache_.find(key);
    if (it == cache_.end()) {
        CacheEntry created;
        created.epoch = lease.epoch;
        created.expires_at_ns = lease.expires_at_ns;
        it = cache_.emplace(key, std::move(created)).first;
        *entry = &it->second;
        return Status::OK();
    }
    CacheEntry& cached = it->second;
    if (lease.epoch < cached.epoch) {
        return Status::StaleLease("writer lease epoch is older");
    }
    if (lease.epoch > cached.epoch) {
        cached.epoch = lease.epoch;
        cached.expires_at_ns = lease.expires_at_ns;
        cached.revoked = false;
        cached.batches.clear();
        *entry = &cached;
        return Status::OK();
    }
    if (cached.revoked) {
        return Status::StaleLease("writer lease revoked");
    }
    cached.expires_at_ns = lease.expires_at_ns;
    *entry = &cached;
    return Status::OK();
}

Status FencedSessions::admitSubmission(const WriterLease& lease,
                                       uint64_t now_ns, BatchID batch_id) {
    if (!enabled_) {
        return Status::NotImplemented("fenced sessions are disabled");
    }
    if (now_ns == 0) now_ns = NowNs();
    std::lock_guard<std::mutex> lock(mu_);
    CacheEntry* entry = nullptr;
    Status admitted = admitLocked(lease, now_ns, &entry);
    if (!admitted.ok()) return admitted;
    entry->batches.push_back(batch_id);
    return Status::OK();
}

void FencedSessions::forgetSubmission(const WriterLease& lease,
                                      BatchID batch_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = cache_.find(WriterKey{lease.session_id, lease.writer_id});
    if (it == cache_.end()) return;
    auto& batches = it->second.batches;
    for (auto batch = batches.begin(); batch != batches.end(); ++batch) {
        if (*batch == batch_id) {
            batches.erase(batch);
            return;
        }
    }
}

Status FencedSessions::revokeWriter(SessionId session_id, uint32_t writer_id,
                                    uint64_t epoch) {
    if (!enabled_) {
        return Status::NotImplemented("fenced sessions are disabled");
    }
    if (!session_id.valid() || writer_id == 0 || epoch == 0) {
        return Status::InvalidArgument("revoke writer arguments");
    }
    std::lock_guard<std::mutex> lock(mu_);
    WriterKey key{session_id, writer_id};
    CacheEntry& entry = cache_[key];
    if (entry.epoch != 0 && epoch < entry.epoch) return Status::OK();
    entry.epoch = epoch;
    entry.revoked = true;
    return Status::OK();
}

std::vector<BatchID> FencedSessions::submittedBatches(
    SessionId session_id, uint32_t writer_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = cache_.find(WriterKey{session_id, writer_id});
    if (it == cache_.end()) return {};
    return it->second.batches;
}

}  // namespace tent
}  // namespace mooncake
