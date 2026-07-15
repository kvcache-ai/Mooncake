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

#ifndef CONNECT_PAUSE_TRACKER_H
#define CONNECT_PAUSE_TRACKER_H

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>

namespace mooncake {

// Active-connect circuit-breaker state: per peer server name (peer IP), a
// "pause active reconnection until" timestamp. After an endpoint to a peer is
// torn down (path failure / QP fatal), the peer is paused so the CQ poller is
// not blocked re-handshaking a likely-gone peer. The clock is injected so the
// TTL/expiry/prune logic is unit-testable without RDMA hardware or real sleeps.
// All methods are thread-safe.
class ConnectPauseTracker {
   public:
    using Clock = std::function<uint64_t()>;  // nanosecond timestamp source

    explicit ConnectPauseTracker(Clock clock) : clock_(std::move(clock)) {}

    // Arm/refresh the pause for `server` until the absolute timestamp
    // `until_ns`.
    void pause(const std::string &server, uint64_t until_ns) {
        std::lock_guard<std::mutex> lock(mu_);
        until_ns_[server] = until_ns;
    }

    // Whether `server` is currently paused. Expired entries are deleted on
    // access (lazy expiry).
    bool isPaused(const std::string &server) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = until_ns_.find(server);
        if (it == until_ns_.end()) return false;
        if (clock_() >= it->second) {
            until_ns_.erase(it);  // expired -> delete
            return false;
        }
        return true;
    }

    // Drop all expired entries (so the map doesn't grow for peers that are
    // never re-checked after their pause lapses). Intended for a periodic tick.
    void prune() {
        uint64_t now = clock_();
        std::lock_guard<std::mutex> lock(mu_);
        for (auto it = until_ns_.begin(); it != until_ns_.end();) {
            if (now >= it->second)
                it = until_ns_.erase(it);
            else
                ++it;
        }
    }

    // For tests / diagnostics.
    size_t size() {
        std::lock_guard<std::mutex> lock(mu_);
        return until_ns_.size();
    }

   private:
    const Clock clock_;
    std::mutex mu_;
    std::unordered_map<std::string, uint64_t> until_ns_;
};

}  // namespace mooncake

#endif  // CONNECT_PAUSE_TRACKER_H
