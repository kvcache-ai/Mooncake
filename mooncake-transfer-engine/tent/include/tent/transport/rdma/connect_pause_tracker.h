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

#ifndef TENT_CONNECT_PAUSE_TRACKER_H
#define TENT_CONNECT_PAUSE_TRACKER_H

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>

namespace mooncake {
namespace tent {

class ConnectPauseTracker {
   public:
    using Clock = std::function<uint64_t()>;

    explicit ConnectPauseTracker(Clock clock) : clock_(std::move(clock)) {}

    void pauseFor(const std::string& server, uint64_t duration_ns) {
        std::lock_guard<std::mutex> lock(mu_);
        until_ns_[server] = clock_() + duration_ns;
    }

    bool isPaused(const std::string& server) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = until_ns_.find(server);
        if (it == until_ns_.end()) return false;
        if (clock_() >= it->second) {
            until_ns_.erase(it);
            return false;
        }
        return true;
    }

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

   private:
    const Clock clock_;
    std::mutex mu_;
    std::unordered_map<std::string, uint64_t> until_ns_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_CONNECT_PAUSE_TRACKER_H
