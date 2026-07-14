// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit_allocator.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace mooncake::tent {
namespace {

constexpr size_t resourceIndex(CreditResource resource) {
    return static_cast<size_t>(resource) - 1;
}

bool isZeroSession(const ReceiverSessionId& session) {
    return session.high == 0 && session.low == 0;
}

}  // namespace

size_t ReceiverCreditAllocator::EntryKeyHash::operator()(
    const EntryKey& key) const noexcept {
    size_t hash = std::hash<uint64_t>{}(key.sender_peer);
    hash ^= std::hash<uint32_t>{}(key.qos_class) + 0x9e3779b97f4a7c15ULL +
            (hash << 6) + (hash >> 2);
    return hash;
}

Status ReceiverCreditAllocator::validateConfig(
    const ReceiverCreditAllocatorConfig& config) {
    if (isZeroSession(config.receiver_session_id) || config.epoch == 0 ||
        config.max_entries == 0 ||
        config.max_entries > std::numeric_limits<uint32_t>::max() ||
        config.ttl_ms == 0 || config.retry_after_us == 0)
        return Status::InvalidArgument(
            "invalid receiver credit allocator identity or bounds" LOC_MARK);

    bool has_ingress_budget = false;
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        const bool supported = i == resourceIndex(CreditResource::DataBytes) ||
                               i == resourceIndex(CreditResource::RequestSlots);
        const uint64_t capacity = config.capacity[i];
        const uint64_t maximum = config.max_grant_per_pull[i];
        if ((!supported && (capacity != 0 || maximum != 0)) ||
            ((capacity == 0) != (maximum == 0)) || maximum > capacity)
            return Status::InvalidArgument(
                "invalid receiver credit allocator resource bounds" LOC_MARK);
        has_ingress_budget = has_ingress_budget || capacity != 0;
    }
    if (!has_ingress_budget)
        return Status::InvalidArgument(
            "receiver credit allocator has no ingress budget" LOC_MARK);
    return Status::OK();
}

Status ReceiverCreditAllocator::create(
    const ReceiverCreditAllocatorConfig& config,
    std::unique_ptr<ReceiverCreditAllocator>& allocator) {
    CHECK_STATUS(validateConfig(config));
    std::unique_ptr<ReceiverCreditAllocator> created(
        new ReceiverCreditAllocator(config));
    allocator = std::move(created);
    return Status::OK();
}

ReceiverCreditPullResponseV1 ReceiverCreditAllocator::makeResponse(
    ReceiverCreditPullStatus status, uint32_t retry_after_us,
    uint32_t qos_class,
    const std::array<uint64_t, kCreditResourceCount>& grants,
    uint64_t update_sequence) const {
    ReceiverCreditPullResponseV1 response;
    response.status = status;
    response.retry_after_us = retry_after_us;
    response.activation.receiver_session_id = config_.receiver_session_id;
    response.activation.epoch = config_.epoch;
    response.activation.freshness_ttl_ms = config_.ttl_ms;
    response.update.qos_class = qos_class;
    response.update.receiver_session_id = config_.receiver_session_id;
    response.update.epoch = config_.epoch;
    response.update.sequence = update_sequence;
    response.update.freshness_ttl_ms = config_.ttl_ms;
    response.update.grants.reserve(kCreditResourceCount);
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        response.update.grants.push_back(
            {static_cast<CreditResource>(i + 1), grants[i]});
    }
    return response;
}

Status ReceiverCreditAllocator::pull(const ReceiverCreditPullRequestV1& request,
                                     ReceiverCreditPullResponseV1& response) {
    CHECK_STATUS(ReceiverCreditPullRequestCodecV1::validate(request));
    pull_requests_.fetch_add(1, std::memory_order_relaxed);

    std::lock_guard lock(mutex_);
    CHECK_STATUS(validateInvariantsLocked(nullptr));
    const std::array<uint64_t, kCreditResourceCount> zero_grants{};

    if (request.qos_class != 0) {
        response = makeResponse(ReceiverCreditPullStatus::Unsupported, 0,
                                request.qos_class, zero_grants, 1);
        return Status::OK();
    }

    const EntryKey key{request.sender_peer, request.qos_class};
    auto existing = entries_.find(key);
    if (existing != entries_.end() && existing->second.has_request) {
        Entry& entry = existing->second;
        if (request.request_sequence == entry.last_request.request_sequence) {
            if (request == entry.last_request) {
                response = entry.last_response;
            } else {
                response = makeResponse(ReceiverCreditPullStatus::Rejected, 0,
                                        request.qos_class, entry.granted,
                                        entry.update_sequence);
            }
            return Status::OK();
        }
        if (request.request_sequence < entry.last_request.request_sequence) {
            response = makeResponse(ReceiverCreditPullStatus::Rejected, 0,
                                    request.qos_class, entry.granted,
                                    entry.update_sequence);
            return Status::OK();
        }
    }

    const bool initial_session =
        isZeroSession(request.expected_receiver_session_id);
    const bool matching_session =
        request.expected_receiver_session_id == config_.receiver_session_id &&
        request.expected_epoch == config_.epoch;
    if ((!initial_session && !matching_session) ||
        (initial_session && existing != entries_.end())) {
        const auto& grants =
            existing == entries_.end() ? zero_grants : existing->second.granted;
        const uint64_t update_sequence =
            existing == entries_.end()
                ? 1
                : std::max<uint64_t>(1, existing->second.update_sequence);
        response = makeResponse(ReceiverCreditPullStatus::SessionChanged, 0,
                                request.qos_class, grants, update_sequence);
        return Status::OK();
    }

    Entry candidate;
    if (existing != entries_.end()) {
        candidate = existing->second;
    } else {
        if (request.last_update_sequence != 0) {
            response = makeResponse(ReceiverCreditPullStatus::Rejected, 0,
                                    request.qos_class, zero_grants, 1);
            return Status::OK();
        }
        if (entries_.size() >= config_.max_entries) {
            response = makeResponse(ReceiverCreditPullStatus::Retry,
                                    config_.retry_after_us, request.qos_class,
                                    zero_grants, 1);
            return Status::OK();
        }
    }

    if (request.last_update_sequence > candidate.update_sequence ||
        candidate.update_sequence == std::numeric_limits<uint64_t>::max()) {
        response =
            makeResponse(ReceiverCreditPullStatus::Rejected, 0,
                         request.qos_class, candidate.granted,
                         std::max<uint64_t>(1, candidate.update_sequence));
        return Status::OK();
    }

    auto proposed_consumed = candidate.consumed;
    auto proposed_completed = candidate.completed;
    std::array<bool, kCreditResourceCount> present{};
    std::array<uint64_t, kCreditResourceCount> minimum{};
    std::array<uint64_t, kCreditResourceCount> desired{};
    for (const auto& usage : request.resources) {
        const size_t index = resourceIndex(usage.resource);
        present[index] = true;
        minimum[index] = usage.minimum_available;
        desired[index] = usage.desired_available;
        if (usage.consumed_total < candidate.consumed[index] ||
            usage.completed_total < candidate.completed[index] ||
            usage.completed_total > usage.consumed_total ||
            usage.consumed_total > candidate.granted[index]) {
            response =
                makeResponse(ReceiverCreditPullStatus::Rejected, 0,
                             request.qos_class, candidate.granted,
                             std::max<uint64_t>(1, candidate.update_sequence));
            return Status::OK();
        }
        proposed_consumed[index] = usage.consumed_total;
        proposed_completed[index] = usage.completed_total;
    }

    // Completion is applied before evaluating a new pull. This makes released
    // ingress budget immediately available while keeping an all-or-nothing
    // minimum across resources.
    auto proposed_committed = committed_;
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        const uint64_t completion_delta =
            proposed_completed[i] - candidate.completed[i];
        if (completion_delta > proposed_committed[i])
            return Status::InternalError(
                "receiver credit completion exceeds committed budget" LOC_MARK);
        proposed_committed[i] -= completion_delta;
    }

    std::array<uint64_t, kCreditResourceCount> additional{};
    bool all_minimums_fit = true;
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        if (!present[i]) continue;
        const uint64_t existing_available =
            candidate.granted[i] - proposed_consumed[i];
        const uint64_t minimum_additional =
            minimum[i] > existing_available ? minimum[i] - existing_available
                                            : 0;
        const uint64_t desired_additional =
            desired[i] > existing_available ? desired[i] - existing_available
                                            : 0;
        const uint64_t free = config_.capacity[i] - proposed_committed[i];
        additional[i] =
            std::min({desired_additional, free, config_.max_grant_per_pull[i]});
        if (additional[i] < minimum_additional) all_minimums_fit = false;
    }
    if (!all_minimums_fit) additional.fill(0);

    candidate.consumed = proposed_consumed;
    candidate.completed = proposed_completed;
    committed_ = proposed_committed;
    if (all_minimums_fit) {
        for (size_t i = 0; i < kCreditResourceCount; ++i) {
            candidate.granted[i] += additional[i];
            committed_[i] += additional[i];
        }
    }
    ++candidate.update_sequence;
    candidate.has_request = true;
    candidate.last_request = request;
    candidate.last_response = makeResponse(
        all_minimums_fit ? ReceiverCreditPullStatus::Granted
                         : ReceiverCreditPullStatus::Retry,
        all_minimums_fit ? 0 : config_.retry_after_us, request.qos_class,
        candidate.granted, candidate.update_sequence);

    if (existing == entries_.end()) {
        entries_.emplace(key, candidate);
    } else {
        existing->second = candidate;
    }
    CHECK_STATUS(validateInvariantsLocked(nullptr));
    response = candidate.last_response;
    return Status::OK();
}

Status ReceiverCreditAllocator::validateInvariantsLocked(
    ReceiverCreditAllocatorSnapshot* output) const {
    if (entries_.size() > config_.max_entries)
        return Status::InternalError(
            "receiver credit entry bound violated" LOC_MARK);

    std::array<uint64_t, kCreditResourceCount> recomputed{};
    for (const auto& [key, entry] : entries_) {
        if (key.sender_peer == 0 || key.qos_class != 0 || !entry.has_request ||
            entry.update_sequence == 0)
            return Status::InternalError(
                "invalid receiver credit allocator entry" LOC_MARK);
        for (size_t i = 0; i < kCreditResourceCount; ++i) {
            if (entry.completed[i] > entry.consumed[i] ||
                entry.consumed[i] > entry.granted[i])
                return Status::InternalError(
                    "receiver credit entry totals are inconsistent" LOC_MARK);
            const uint64_t outstanding = entry.granted[i] - entry.completed[i];
            if (outstanding >
                std::numeric_limits<uint64_t>::max() - recomputed[i])
                return Status::InternalError(
                    "receiver credit committed total overflow" LOC_MARK);
            recomputed[i] += outstanding;
        }
    }

    ReceiverCreditAllocatorSnapshot snapshot;
    snapshot.capacity = config_.capacity;
    snapshot.entries = entries_.size();
    snapshot.pull_requests = pull_requests_.load(std::memory_order_relaxed);
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        if (recomputed[i] != committed_[i] ||
            committed_[i] > config_.capacity[i])
            return Status::InternalError(
                "receiver credit global budget invariant violated" LOC_MARK);
        snapshot.committed[i] = committed_[i];
        snapshot.free[i] = config_.capacity[i] - committed_[i];
    }
    if (output != nullptr) *output = snapshot;
    return Status::OK();
}

Status ReceiverCreditAllocator::snapshot(
    ReceiverCreditAllocatorSnapshot& snapshot) const {
    std::lock_guard lock(mutex_);
    ReceiverCreditAllocatorSnapshot checked;
    CHECK_STATUS(validateInvariantsLocked(&checked));
    snapshot = checked;
    return Status::OK();
}

}  // namespace mooncake::tent
