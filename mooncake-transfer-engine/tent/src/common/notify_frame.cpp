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

#include "tent/common/notify_frame.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <iterator>
#include <limits>
#include <random>

namespace mooncake {
namespace tent {

namespace {

// Little-endian on the wire; so is every host tent builds on.
template <typename T>
void putPod(char*& out, T value) {
    std::memcpy(out, &value, sizeof(value));
    out += sizeof(value);
}

template <typename T>
T getPod(const char*& in) {
    T value;
    std::memcpy(&value, in, sizeof(value));
    in += sizeof(value);
    return value;
}

uint64_t randomSession() {
    std::random_device device;
    uint64_t session = 0;
    while (session == 0) session = (uint64_t{device()} << 32) | device();
    return session;
}

constexpr auto kIdleTarget = std::chrono::minutes(10);  // both tables

// In ms: a ttl meant as "never" would overflow once converted to ns.
bool outlived(NotifyDedupFilter::Clock::time_point created,
              NotifyDedupFilter::Clock::time_point now,
              std::chrono::milliseconds ttl) {
    using std::chrono::milliseconds;
    return std::chrono::duration_cast<milliseconds>(now - created) > ttl;
}

}  // namespace

bool isNotifyFrameMagic(const char* data, size_t len) {
    if (!data || len < sizeof(uint32_t)) return false;
    uint32_t magic = 0;
    std::memcpy(&magic, data, sizeof(magic));
    return magic == kNotifyFrameMagic;
}

bool encodeNotifyPayload(const std::string& name, const std::string& msg,
                         char* out, size_t capacity, uint32_t* out_len) {
    if (!out || !out_len) return false;
    constexpr size_t kMax = std::numeric_limits<uint32_t>::max();
    if (name.size() > kMax || msg.size() > kMax) return false;
    const uint32_t name_len = static_cast<uint32_t>(name.size());
    const uint32_t msg_len = static_cast<uint32_t>(msg.size());
    const size_t total =
        sizeof(name_len) + name_len + sizeof(msg_len) + msg_len;
    if (total > capacity || total > kMax) return false;
    char* ptr = out;
    putPod(ptr, name_len);
    std::memcpy(ptr, name.data(), name_len);
    ptr += name_len;
    putPod(ptr, msg_len);
    std::memcpy(ptr, msg.data(), msg_len);
    *out_len = static_cast<uint32_t>(total);
    return true;
}

bool decodeNotifyPayload(const char* data, size_t len, std::string* name,
                         std::string* msg) {
    if (!data || !name || !msg || len < 8) return false;
    const char* ptr = data;
    const uint32_t name_len = getPod<uint32_t>(ptr);
    if (name_len > len - 8) return false;
    const char* name_ptr = ptr;
    ptr += name_len;
    const uint32_t msg_len = getPod<uint32_t>(ptr);
    if (msg_len > len - 8 - name_len) return false;
    name->assign(name_ptr, name_len);
    msg->assign(ptr, msg_len);
    return true;
}

bool encodeNotifyFrame(const NotifyFrameHeader& header, const std::string& name,
                       const std::string& msg, char* out, size_t capacity,
                       uint32_t* out_len) {
    if (!out || !out_len) return false;
    if (header.version != kNotifyFrameVersion) return false;
    if (header.type != kNotifyFrameTypeNotifyCompat) return false;
    if ((header.flags & ~kNotifyFrameKnownFlagsMask) != 0) return false;
    if (capacity < kNotifyFrameHeaderSize) return false;
    uint32_t payload_len = 0;
    if (!encodeNotifyPayload(name, msg, out + kNotifyFrameHeaderSize,
                             capacity - kNotifyFrameHeaderSize, &payload_len))
        return false;
    char* ptr = out;
    putPod(ptr, kNotifyFrameMagic);
    putPod(ptr, header.version);
    putPod(ptr, header.type);
    putPod(ptr, header.flags);
    putPod(ptr, header.session);
    putPod(ptr, header.epoch);
    putPod(ptr, header.seq);
    putPod(ptr, header.ack_seq);
    putPod(ptr, payload_len);
    *out_len = static_cast<uint32_t>(kNotifyFrameHeaderSize + payload_len);
    return true;
}

bool decodeNotifyFrame(const char* data, size_t len, NotifyFrameHeader* header,
                       std::string* name, std::string* msg) {
    if (!data || !header || !name || !msg) return false;
    if (len < kNotifyFrameHeaderSize) return false;
    const char* ptr = data;
    if (getPod<uint32_t>(ptr) != kNotifyFrameMagic) return false;
    NotifyFrameHeader decoded;
    decoded.version = getPod<uint8_t>(ptr);
    decoded.type = getPod<uint8_t>(ptr);
    decoded.flags = getPod<uint16_t>(ptr);
    decoded.session = getPod<uint64_t>(ptr);
    decoded.epoch = getPod<uint64_t>(ptr);
    decoded.seq = getPod<uint64_t>(ptr);
    decoded.ack_seq = getPod<uint64_t>(ptr);
    const uint32_t payload_len = getPod<uint32_t>(ptr);
    if (decoded.version != kNotifyFrameVersion) return false;
    if (decoded.type != kNotifyFrameTypeNotifyCompat) return false;
    if ((decoded.flags & ~kNotifyFrameKnownFlagsMask) != 0) return false;
    if (len - kNotifyFrameHeaderSize != payload_len) return false;
    if (!decodeNotifyPayload(ptr, payload_len, name, msg)) return false;
    if (8 + name->size() + msg->size() != payload_len) return false;
    *header = decoded;
    return true;
}

Notification NotifySequencer::stamp(const std::string& target,
                                    const Notification& notifi,
                                    Clock::time_point now) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (++stamps_since_prune_ >= kPruneEvery) {
        stamps_since_prune_ = 0;
        for (auto it = targets_.begin(); it != targets_.end();) {
            if (now - it->second.last_used > kIdleTarget) {
                it = targets_.erase(it);
            } else {
                ++it;
            }
        }
    }
    auto it = targets_.find(target);
    if (it == targets_.end())
        it = targets_.emplace(target, Target{randomSession()}).first;
    it->second.last_used = now;
    if (notifi.seq != 0 && notifi.session == it->second.session) return notifi;
    Notification stamped = notifi;
    stamped.session = it->second.session;
    stamped.seq = ++it->second.last_seq;
    return stamped;
}

void NotifyDedupFilter::configure(const Config& config) {
    std::lock_guard<std::mutex> guard(mutex_);
    config_ = config;
}

bool NotifyDedupFilter::admit(uint64_t session, uint64_t seq,
                              Clock::time_point now) {
    if (session == 0 || seq == 0) return true;
    std::lock_guard<std::mutex> guard(mutex_);
    if (++admits_since_prune_ >= kPruneEvery) pruneLocked(now);
    auto [it, fresh] = sessions_.try_emplace(session);
    SessionState& state = it->second;
    // now is read before the lock; never step back, so that holes are created
    // in time order as well as in seq order.
    state.last_seen = std::max(state.last_seen, now);
    if (seq > state.max_seq) {
        if (seq - state.max_seq > 1) {
            state.holes.emplace_hint(state.holes.end(), state.max_seq + 1,
                                     Hole{seq, state.last_seen, fresh});
        }
        state.max_seq = seq;
    } else {
        auto hole = state.holes.upper_bound(seq);
        if (hole == state.holes.begin() || seq >= std::prev(hole)->second.hi) {
            if (config_.on_drop) config_.on_drop(Drop::Duplicate);
            return false;  // delivered before
        }
        --hole;
        if (outlived(hole->second.created, now, config_.ttl)) {
            giveUpLocked(session, state.holes, hole, Drop::HoleExpired);
            if (config_.on_drop) config_.on_drop(Drop::Expired);
            return false;
        }
        const Hole whole = hole->second;
        const auto next = std::next(hole);
        if (hole->first < seq) {
            hole->second.hi = seq;
        } else {
            state.holes.erase(hole);
        }
        if (seq + 1 < whole.hi) {
            state.holes.emplace_hint(
                next, seq + 1,
                Hole{whole.hi, whole.created, whole.before_first});
        }
    }
    while (state.holes.size() > config_.max_holes)
        giveUpLocked(session, state.holes, state.holes.begin(),
                     Drop::HoleEvicted);
    return true;
}

size_t NotifyDedupFilter::sessions() {
    std::lock_guard<std::mutex> guard(mutex_);
    return sessions_.size();
}

void NotifyDedupFilter::pruneLocked(Clock::time_point now) {
    admits_since_prune_ = 0;
    const auto forget_after =
        std::max<std::chrono::milliseconds>(kIdleTarget, config_.ttl);
    for (auto it = sessions_.begin(); it != sessions_.end();) {
        // Holes are only added above the others or split off one, keeping its
        // time, so the first is the oldest. Counted here because a late
        // arrival in a dropped hole can no longer be told from a duplicate.
        auto& holes = it->second.holes;
        while (!holes.empty() &&
               outlived(holes.begin()->second.created, now, config_.ttl))
            giveUpLocked(it->first, holes, holes.begin(), Drop::HoleExpired);
        if (outlived(it->second.last_seen, now, forget_after)) {
            it = sessions_.erase(it);
        } else {
            ++it;
        }
    }
}

void NotifyDedupFilter::giveUpLocked(uint64_t session, Holes& holes,
                                     Holes::iterator hole, Drop drop) {
    if (!hole->second.before_first) {
        auto& counter = drop == Drop::HoleExpired ? expired_ : evicted_;
        counter.fetch_add(1, std::memory_order_relaxed);
        if (config_.on_drop) config_.on_drop(drop);
        LOG_EVERY_N(WARNING, 100)
            << "Gave up on notifications " << hole->first << ".."
            << hole->second.hi - 1 << " of session " << session << ", "
            << expired_.load(std::memory_order_relaxed) << " expired and "
            << evicted_.load(std::memory_order_relaxed)
            << " evicted ranges so far";
    }
    holes.erase(hole);
}

size_t NotifyDedupFilter::holes() {
    std::lock_guard<std::mutex> guard(mutex_);
    size_t total = 0;
    for (const auto& entry : sessions_) total += entry.second.holes.size();
    return total;
}

size_t NotifyDedupFilter::expired() const {
    return expired_.load(std::memory_order_relaxed);
}

size_t NotifyDedupFilter::evicted() const {
    return evicted_.load(std::memory_order_relaxed);
}

}  // namespace tent
}  // namespace mooncake
