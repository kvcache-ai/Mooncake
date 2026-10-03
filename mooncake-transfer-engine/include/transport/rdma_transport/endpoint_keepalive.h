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

#ifndef ENDPOINT_KEEPALIVE_H_
#define ENDPOINT_KEEPALIVE_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <queue>
#include <vector>

namespace mooncake {

// Per posting-thread schedule of endpoint keepalives
// (MC_ENDPOINT_IDLE_TIMEOUT).
//
// An RC QP has no keepalive of its own, and the transfer engine frees an
// endpoint only when a WR on it fails. A peer that went away (a restarted pod
// comes back under a new NIC path) is never sent to again, so its endpoint and
// QPs would otherwise live until the store fills. Each endpoint's owning
// posting thread keeps it here, keyed by when it next becomes idle for
// idle_ns. When that time passes without a post, the thread posts one
// zero-length RDMA WRITE on the established QP; a dead peer makes it fail with
// a retry-exceeded CQE, and the completion path retires the endpoint.
//
// EndPoint provides lastUsedNs() and retired(). Single-threaded: only the
// owning posting thread touches a schedule.
template <typename EndPoint>
class KeepaliveSchedule {
   public:
    void add(std::weak_ptr<EndPoint> endpoint, uint64_t due_ns) {
        heap_.push(Entry{due_ns, std::move(endpoint)});
    }

    // Handles the entries due at now_ns. An entry whose endpoint is gone or
    // retired is dropped. One used since it was queued is re-queued for
    // lastUsedNs() + idle_ns. Otherwise post(endpoint) is called and the
    // entry re-queued for now_ns + idle_ns, so an idle endpoint gets one
    // keepalive per idle_ns. Returns the number of post() calls.
    template <typename Post>
    size_t service(uint64_t now_ns, uint64_t idle_ns, Post &&post) {
        size_t posts = 0;
        while (!heap_.empty() && heap_.top().due_ns <= now_ns) {
            Entry entry = heap_.top();
            heap_.pop();
            auto endpoint = entry.endpoint.lock();
            if (!endpoint || endpoint->retired()) continue;
            const uint64_t last_used = endpoint->lastUsedNs();
            if (last_used + idle_ns > now_ns) {
                heap_.push(
                    Entry{last_used + idle_ns, std::move(entry.endpoint)});
                continue;
            }
            post(*endpoint);
            ++posts;
            heap_.push(Entry{now_ns + idle_ns, std::move(entry.endpoint)});
        }
        return posts;
    }

    size_t size() const { return heap_.size(); }
    bool empty() const { return heap_.empty(); }
    uint64_t nextDueNs() const {
        return heap_.empty() ? 0 : heap_.top().due_ns;
    }

   private:
    struct Entry {
        uint64_t due_ns;
        std::weak_ptr<EndPoint> endpoint;
        bool operator>(const Entry &other) const {
            return due_ns > other.due_ns;
        }
    };
    std::priority_queue<Entry, std::vector<Entry>, std::greater<Entry>> heap_;
};

}  // namespace mooncake

#endif  // ENDPOINT_KEEPALIVE_H_
