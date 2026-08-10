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

#include "transport/rdma_twosided/sender_credit.h"

#include "error.h"

namespace mooncake {

int SenderCreditLedger::resourceIndex(CreditResource r, size_t &idx) {
    auto raw = static_cast<uint16_t>(r);
    if (raw < 1 || raw > kResourceCount) return ERR_INVALID_ARGUMENT;
    idx = raw - 1;
    return 0;
}

int SenderCreditLedger::normalize(
    const std::vector<std::pair<CreditResource, uint64_t>> &charge,
    std::array<uint64_t, kResourceCount> &out) {
    out.fill(0);
    if (charge.empty()) return ERR_INVALID_ARGUMENT;
    for (const auto &item : charge) {
        size_t i = 0;
        if (resourceIndex(item.first, i)) return ERR_INVALID_ARGUMENT;
        if (item.second == 0 || out[i] != 0) return ERR_INVALID_ARGUMENT;
        out[i] = item.second;
    }
    return 0;
}

int SenderCreditLedger::activate(const std::string &peer, uint64_t session,
                                 uint64_t epoch) {
    if (peer.empty() || epoch == 0) return ERR_INVALID_ARGUMENT;
    std::lock_guard<std::mutex> lock(mutex_);
    Key key{peer, session};
    auto it = entries_.find(key);
    if (it != entries_.end()) {
        if (epoch < it->second.epoch) return ERR_INVALID_ARGUMENT;
        if (epoch == it->second.epoch) return 0;
        Entry replacement;
        replacement.epoch = epoch;
        it->second = replacement;
        return 0;
    }
    if (entries_.size() >= max_entries_) return ERR_TOO_MANY_REQUESTS;
    Entry e;
    e.epoch = epoch;
    entries_.emplace(std::move(key), e);
    return 0;
}

int SenderCreditLedger::deactivate(const std::string &peer, uint64_t session,
                                   uint64_t epoch) {
    if (epoch == 0) return ERR_INVALID_ARGUMENT;
    std::lock_guard<std::mutex> lock(mutex_);
    Key key{peer, session};
    auto it = entries_.find(key);
    if (it == entries_.end()) return 0;
    if (it->second.epoch != epoch) return ERR_INVALID_ARGUMENT;
    entries_.erase(it);
    return 0;
}

int SenderCreditLedger::applyGrant(const std::string &peer, uint64_t session,
                                   uint64_t epoch, uint64_t seq,
                                   const std::vector<CreditAmount> &grants,
                                   int &disposition) {
    disposition = 0;
    if (epoch == 0 || seq == 0 || grants.size() > kResourceCount)
        return ERR_INVALID_ARGUMENT;
    std::array<uint64_t, kResourceCount> proposed{};
    std::array<bool, kResourceCount> present{};
    for (const auto &g : grants) {
        size_t i = 0;
        if (resourceIndex(g.resource, i)) return ERR_INVALID_ARGUMENT;
        if (present[i]) return ERR_INVALID_ARGUMENT;
        present[i] = true;
        proposed[i] = g.grant_total;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    Key key{peer, session};
    auto it = entries_.find(key);
    if (it == entries_.end() || it->second.epoch != epoch)
        return ERR_INVALID_ARGUMENT;
    auto &e = it->second;
    if (e.has_update && seq <= e.last_sequence) {
        disposition = 1;  // duplicate/old
        return 0;
    }
    for (size_t i = 0; i < kResourceCount; ++i) {
        if (present[i] &&
            (proposed[i] < e.grants[i] || proposed[i] < e.consumed[i]))
            return ERR_INVALID_ARGUMENT;
    }
    bool gap = e.has_update && seq > e.last_sequence + 1;
    for (size_t i = 0; i < kResourceCount; ++i)
        if (present[i]) e.grants[i] = proposed[i];
    e.last_sequence = seq;
    e.has_update = true;
    disposition = gap ? 2 : 0;
    return 0;
}

int SenderCreditLedger::tryReserve(
    const std::string &peer, uint64_t session,
    const std::vector<std::pair<CreditResource, uint64_t>> &charge) {
    std::array<uint64_t, kResourceCount> n{};
    if (normalize(charge, n)) return ERR_INVALID_ARGUMENT;
    std::lock_guard<std::mutex> lock(mutex_);
    Key key{peer, session};
    auto it = entries_.find(key);
    if (it == entries_.end() || !it->second.has_update)
        return ERR_TOO_MANY_REQUESTS;
    auto &e = it->second;
    for (size_t i = 0; i < kResourceCount; ++i) {
        if (e.consumed[i] > e.grants[i] || n[i] > e.grants[i] - e.consumed[i])
            return ERR_TOO_MANY_REQUESTS;
    }
    for (size_t i = 0; i < kResourceCount; ++i) e.consumed[i] += n[i];
    return 0;
}

int SenderCreditLedger::rollbackReservation(
    const std::string &peer, uint64_t session,
    const std::vector<std::pair<CreditResource, uint64_t>> &charge) {
    std::array<uint64_t, kResourceCount> n{};
    if (normalize(charge, n)) return ERR_INVALID_ARGUMENT;
    std::lock_guard<std::mutex> lock(mutex_);
    Key key{peer, session};
    auto it = entries_.find(key);
    if (it == entries_.end()) return ERR_INVALID_ARGUMENT;
    for (size_t i = 0; i < kResourceCount; ++i) {
        if (n[i] > it->second.consumed[i]) return ERR_INVALID_ARGUMENT;
    }
    for (size_t i = 0; i < kResourceCount; ++i) it->second.consumed[i] -= n[i];
    return 0;
}

int SenderCreditLedger::available(const std::string &peer, uint64_t session,
                                  CreditResource resource,
                                  uint64_t &out) const {
    size_t i = 0;
    if (resourceIndex(resource, i)) return ERR_INVALID_ARGUMENT;
    std::lock_guard<std::mutex> lock(mutex_);
    Key key{peer, session};
    auto it = entries_.find(key);
    if (it == entries_.end()) return ERR_INVALID_ARGUMENT;
    const auto &e = it->second;
    out = (e.grants[i] > e.consumed[i]) ? (e.grants[i] - e.consumed[i]) : 0;
    return 0;
}

uint64_t SenderCreditLedger::availableForPeer(const std::string &peer,
                                              CreditResource resource) const {
    size_t i = 0;
    if (resourceIndex(resource, i)) return 0;
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t total = 0;
    for (const auto &entry : entries_) {
        if (entry.first.peer != peer) continue;
        const auto &e = entry.second;
        if (!e.has_update) continue;
        if (e.grants[i] > e.consumed[i]) total += e.grants[i] - e.consumed[i];
    }
    return total;
}

bool SenderCreditLedger::hasPeer(const std::string &peer) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto &entry : entries_) {
        if (entry.first.peer == peer) return true;
    }
    return false;
}

}  // namespace mooncake
