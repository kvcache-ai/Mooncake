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

#ifndef RDMA_SENDER_CREDIT_H_
#define RDMA_SENDER_CREDIT_H_

#include <array>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "transport/rdma_twosided/ctrl_frame.h"

namespace mooncake {

// Sender-side credit ledger keyed by peer server name + local session epoch.
// Cumulative grants; tryReserve fails with ERR_TOO_MANY_REQUESTS when short.
class SenderCreditLedger {
   public:
    explicit SenderCreditLedger(size_t max_entries = 1024)
        : max_entries_(max_entries) {}

    int activate(const std::string &peer, uint64_t session, uint64_t epoch);
    int deactivate(const std::string &peer, uint64_t session, uint64_t epoch);

    // Apply cumulative CREDIT_GRANT. disposition: 0=applied, 1=dup/old, 2=gap.
    int applyGrant(const std::string &peer, uint64_t session, uint64_t epoch,
                   uint64_t seq, const std::vector<CreditAmount> &grants,
                   int &disposition);

    int tryReserve(
        const std::string &peer, uint64_t session,
        const std::vector<std::pair<CreditResource, uint64_t>> &charge);
    int rollbackReservation(
        const std::string &peer, uint64_t session,
        const std::vector<std::pair<CreditResource, uint64_t>> &charge);

    int available(const std::string &peer, uint64_t session,
                  CreditResource resource, uint64_t &out) const;

    // Sum available units of `resource` across all sessions for peer.
    uint64_t availableForPeer(const std::string &peer,
                              CreditResource resource) const;

    bool hasPeer(const std::string &peer) const;

   private:
    static constexpr size_t kResourceCount = 4;

    struct Key {
        std::string peer;
        uint64_t session = 0;
        bool operator==(const Key &o) const {
            return session == o.session && peer == o.peer;
        }
    };
    struct KeyHash {
        size_t operator()(const Key &k) const noexcept {
            size_t h = std::hash<std::string>{}(k.peer);
            h ^= std::hash<uint64_t>{}(k.session) + 0x9e3779b97f4a7c15ULL +
                 (h << 6) + (h >> 2);
            return h;
        }
    };
    struct Entry {
        uint64_t epoch = 0;
        uint64_t last_sequence = 0;
        bool has_update = false;
        std::array<uint64_t, kResourceCount> grants{};
        std::array<uint64_t, kResourceCount> consumed{};
    };

    static int resourceIndex(CreditResource r, size_t &idx);
    static int normalize(
        const std::vector<std::pair<CreditResource, uint64_t>> &charge,
        std::array<uint64_t, kResourceCount> &out);

    mutable std::mutex mutex_;
    const size_t max_entries_;
    std::unordered_map<Key, Entry, KeyHash> entries_;
};

}  // namespace mooncake

#endif  // RDMA_SENDER_CREDIT_H_
