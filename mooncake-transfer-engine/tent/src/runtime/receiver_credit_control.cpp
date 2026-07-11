// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit_control.h"

#include <algorithm>

namespace mooncake::tent {

CreditCapabilityState::CreditCapabilityState(CreditRolloutMode mode)
    : mode_(mode),
      state_(mode == CreditRolloutMode::Disabled
                 ? CreditPeerState::Disabled
                 : CreditPeerState::Negotiating) {}

Status CreditCapabilityState::beginNegotiation() {
    if (mode_ == CreditRolloutMode::Disabled) return Status::OK();
    state_ = CreditPeerState::Negotiating;
    version_ = 0;
    return Status::OK();
}

Status CreditCapabilityState::completeNegotiation(
    const std::vector<uint16_t>& peer_versions) {
    if (mode_ == CreditRolloutMode::Disabled) return Status::OK();
    if (state_ != CreditPeerState::Negotiating)
        return Status::InvalidEntry("credit peer is not negotiating" LOC_MARK);
    auto supported = std::find(peer_versions.begin(), peer_versions.end(), 1);
    if (supported != peer_versions.end()) {
        version_ = 1;
        state_ = CreditPeerState::Active;
        return Status::OK();
    }
    if (mode_ == CreditRolloutMode::Optional) {
        state_ = CreditPeerState::Legacy;
        return Status::OK();
    }
    state_ = CreditPeerState::Failed;
    return Status::NotImplemented(
        "receiver credit is required but unsupported" LOC_MARK);
}

Status CreditCapabilityState::markStale() {
    if (state_ != CreditPeerState::Active)
        return Status::InvalidEntry(
            "only active credit can become stale" LOC_MARK);
    state_ = CreditPeerState::Stale;
    return Status::OK();
}

Status CreditCapabilityState::refresh(uint16_t version) {
    if (state_ != CreditPeerState::Stale || version != version_)
        return Status::InvalidArgument("invalid stale credit refresh" LOC_MARK);
    state_ = CreditPeerState::Active;
    return Status::OK();
}

Status BoundedCreditUpdateInbox::tryPublish(CreditControlEnvelope envelope) {
    if (capacity_ == 0)
        return Status::TooManyRequests("credit update inbox disabled" LOC_MARK);
    std::lock_guard lock(mutex_);
    if (queue_.size() >= capacity_)
        return Status::TooManyRequests("credit update inbox full" LOC_MARK);
    queue_.push_back(std::move(envelope));
    return Status::OK();
}

size_t BoundedCreditUpdateInbox::drain(
    std::vector<CreditControlEnvelope>& output, size_t max_updates) {
    std::lock_guard lock(mutex_);
    size_t count = std::min(max_updates, queue_.size());
    output.reserve(output.size() + count);
    for (size_t i = 0; i < count; ++i) {
        output.push_back(std::move(queue_.front()));
        queue_.pop_front();
    }
    return count;
}

size_t BoundedCreditUpdateInbox::size() const {
    std::lock_guard lock(mutex_);
    return queue_.size();
}

}  // namespace mooncake::tent
