// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_RUNTIME_RECEIVER_CREDIT_ALLOCATOR_H
#define TENT_RUNTIME_RECEIVER_CREDIT_ALLOCATOR_H

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "tent/runtime/receiver_credit_protocol.h"

namespace mooncake::tent {

struct ReceiverCreditAllocatorConfig {
    std::array<uint64_t, kCreditResourceCount> capacity{};
    std::array<uint64_t, kCreditResourceCount> max_grant_per_pull{};
    size_t max_entries{0};
    uint32_t ttl_ms{0};
    uint32_t retry_after_us{0};
    ReceiverSessionId receiver_session_id;
    uint64_t epoch{0};
};

struct ReceiverCreditAllocatorSnapshot {
    std::array<uint64_t, kCreditResourceCount> capacity{};
    std::array<uint64_t, kCreditResourceCount> committed{};
    std::array<uint64_t, kCreditResourceCount> free{};
    size_t entries{0};
    uint64_t pull_requests{0};
};

// Receiver-wide allocator for a configured ingress budget. Credits represent
// bounded bytes/request slots that the receiver is willing to have outstanding;
// this class deliberately does not allocate memory extents or expose rkeys.
class ReceiverCreditAllocator {
   public:
    static Status create(const ReceiverCreditAllocatorConfig& config,
                         std::unique_ptr<ReceiverCreditAllocator>& allocator);

    // A syntactically valid request always produces a protocol response. Retry,
    // restart fencing and sequence rejection are represented by
    // response.status. The output is unchanged only when the request itself is
    // malformed or an internal invariant is violated.
    Status pull(const ReceiverCreditPullRequestV1& request,
                ReceiverCreditPullResponseV1& response);

    // Recomputes all accounting under the lock and returns an error instead of
    // publishing a snapshot if any invariant has been violated.
    Status snapshot(ReceiverCreditAllocatorSnapshot& snapshot) const;

   private:
    struct EntryKey {
        uint64_t sender_peer{0};
        uint32_t qos_class{0};
        bool operator==(const EntryKey& other) const {
            return sender_peer == other.sender_peer &&
                   qos_class == other.qos_class;
        }
    };

    struct EntryKeyHash {
        size_t operator()(const EntryKey& key) const noexcept;
    };

    struct Entry {
        std::array<uint64_t, kCreditResourceCount> granted{};
        std::array<uint64_t, kCreditResourceCount> consumed{};
        std::array<uint64_t, kCreditResourceCount> completed{};
        uint64_t update_sequence{0};
        bool has_request{false};
        ReceiverCreditPullRequestV1 last_request;
        ReceiverCreditPullResponseV1 last_response;
    };

    explicit ReceiverCreditAllocator(ReceiverCreditAllocatorConfig config)
        : config_(std::move(config)) {}

    static Status validateConfig(const ReceiverCreditAllocatorConfig& config);
    ReceiverCreditPullResponseV1 makeResponse(
        ReceiverCreditPullStatus status, uint32_t retry_after_us,
        uint32_t qos_class,
        const std::array<uint64_t, kCreditResourceCount>& grants,
        uint64_t update_sequence) const;
    Status validateInvariantsLocked(
        ReceiverCreditAllocatorSnapshot* snapshot) const;

    const ReceiverCreditAllocatorConfig config_;
    std::atomic<uint64_t> pull_requests_{0};
    mutable std::mutex mutex_;
    std::array<uint64_t, kCreditResourceCount> committed_{};
    std::unordered_map<EntryKey, Entry, EntryKeyHash> entries_;
};

}  // namespace mooncake::tent

#endif  // TENT_RUNTIME_RECEIVER_CREDIT_ALLOCATOR_H
