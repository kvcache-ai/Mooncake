// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/endpoint.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <iterator>
#include <limits>
#include <utility>

namespace mooncake::tent::ub {
namespace {

uint64_t generationSeed() {
    const auto wall = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    const auto steady = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
    const uint64_t seed =
        (wall ^ (steady << 13) ^ (steady >> 7)) & 0x7fffffffffffffffULL;
    return seed == 0 ? 1 : seed;
}

std::atomic<uint64_t> g_next_endpoint_generation{generationSeed()};

bool samePeer(const UbBootstrapDesc& lhs, const UbBootstrapDesc& rhs) {
    return lhs.protocol_version == rhs.protocol_version &&
           lhs.local_eid == rhs.local_eid && lhs.jetty_ids == rhs.jetty_ids &&
           lhs.jetty_uasids == rhs.jetty_uasids &&
           lhs.endpoint_generation == rhs.endpoint_generation;
}

}  // namespace

size_t UbEndpointKeyHash::operator()(const UbEndpointKey& key) const noexcept {
    size_t seed = std::hash<int>{}(key.local_topology_id);
    auto combine = [&seed](size_t value) {
        seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
    };
    combine(std::hash<SegmentID>{}(key.remote_segment_id));
    combine(std::hash<int>{}(key.remote_topology_id));
    combine(std::hash<std::string>{}(key.peer_nic_path));
    return seed;
}

uint64_t UbEndpoint::allocateGeneration() noexcept {
    const uint64_t generation =
        g_next_endpoint_generation.fetch_add(1, std::memory_order_relaxed);
    // Generation zero is reserved for "no endpoint". Exhausting the 64-bit
    // process-wide sequence is not recoverable without reusing generations.
    if (generation == 0 || generation == std::numeric_limits<uint64_t>::max()) {
        std::terminate();
    }
    return generation;
}

UbEndpoint::UbEndpoint(UbEndpointKey key, UbContextPtr context,
                       std::shared_ptr<UrmaAdapter> adapter,
                       uint32_t jetty_count, JettyOptions jetty_options)
    : key_(std::move(key)),
      context_(std::move(context)),
      adapter_(std::move(adapter)),
      jetty_count_(jetty_count),
      jetty_options_(jetty_options),
      generation_(allocateGeneration()) {}

UbEndpoint::~UbEndpoint() {
    auto status = retire();
    if (!status.ok() || outstanding_wrs_.load(std::memory_order_relaxed) != 0) {
        // A caller that drops the final endpoint reference after a failed
        // native fence must not let Jetty destructors force RESET/delete. A
        // process-lifetime quarantine is safer than DMA-after-free.
        static auto* leaked = new std::vector<JettyPtr>();
        static auto* leaked_mutex = new std::mutex();
        std::scoped_lock lock(lifecycle_mutex_, *leaked_mutex);
        leaked->insert(leaked->end(), std::make_move_iterator(jetties_.begin()),
                       std::make_move_iterator(jetties_.end()));
        jetties_.clear();
    }
}

void UbEndpoint::rememberFirstError(const Status& candidate, Status& first) {
    if (first.ok() && !candidate.ok()) first = candidate;
}

Status UbEndpoint::resetAndUnbindLocked() {
    Status first_error = Status::OK();
    if (!adapter_) return first_error;

    for (auto it = jetties_.rbegin(); it != jetties_.rend(); ++it) {
        if (!*it) continue;
        auto reset_status = adapter_->resetJetty(*it);
        rememberFirstError(reset_status, first_error);
        // Unimporting a peer before RESET succeeds can sever resources still
        // referenced by hardware. Keep this Jetty intact for a retry.
        if (!reset_status.ok()) continue;
        rememberFirstError(adapter_->unbindJetty(*it), first_error);
    }
    return first_error;
}

Status UbEndpoint::deleteJettysLocked() {
    Status first_error = Status::OK();
    if (adapter_) {
        for (auto it = jetties_.rbegin(); it != jetties_.rend(); ++it) {
            if (!*it) continue;
            rememberFirstError(adapter_->deleteJetty(*it), first_error);
        }
    }
    jetties_.clear();
    jfc_indices_.clear();
    return first_error;
}

Status UbEndpoint::failLocked(Status status) {
    if (status.ok()) {
        status = Status::InternalError(
            "UB endpoint entered failed state without an error" LOC_MARK);
    }
    lifecycle_status_ = status;
    state_.store(State::kFailed, std::memory_order_release);

    rememberFirstError(resetAndUnbindLocked(), retire_status_);
    rememberFirstError(deleteJettysLocked(), retire_status_);
    return lifecycle_status_;
}

Status UbEndpoint::prepare() {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    const State current = state_.load(std::memory_order_relaxed);
    if (current == State::kPrepared || current == State::kReady) {
        return Status::OK();
    }
    if (current == State::kFailed) return lifecycle_status_;
    if (current != State::kUninitialized) {
        return Status::InvalidArgument(
            "UB endpoint cannot be prepared in its current state" LOC_MARK);
    }

    state_.store(State::kHandshaking, std::memory_order_release);
    if (!key_.valid()) {
        return failLocked(
            Status::InvalidArgument("Invalid UB endpoint key" LOC_MARK));
    }
    if (!adapter_ || !context_ || !context_->active() || !context_->handle() ||
        !context_->handle()->valid()) {
        return failLocked(Status::InvalidArgument(
            "UB endpoint requires an active context and adapter" LOC_MARK));
    }
    if (context_->topologyId() != key_.local_topology_id) {
        return failLocked(Status::InvalidArgument(
            "UB endpoint key does not match its local context" LOC_MARK));
    }
    if (jetty_count_ == 0 || context_->jfcs().empty()) {
        return failLocked(Status::InvalidArgument(
            "UB endpoint requires at least one Jetty and JFC" LOC_MARK));
    }
    const uint32_t max_jetty = context_->deviceInfo().capabilities.max_jetty;
    if (max_jetty != 0 && jetty_count_ > max_jetty) {
        return failLocked(
            Status::InvalidArgument("Requested endpoint Jetty count exceeds "
                                    "device capability" LOC_MARK));
    }

    jetties_.reserve(jetty_count_);
    jfc_indices_.reserve(jetty_count_);
    for (uint32_t index = 0; index < jetty_count_; ++index) {
        const size_t jfc_index = index % context_->jfcs().size();
        auto jfc = context_->jfc(jfc_index);
        if (!jfc || !jfc->valid()) {
            return failLocked(Status::InvalidArgument(
                "UB endpoint selected an inactive JFC" LOC_MARK));
        }

        JettyPtr jetty;
        auto status = adapter_->createJetty(context_->handle(), jfc->handle(),
                                            jetty_options_, jetty);
        if (!status.ok()) return failLocked(std::move(status));
        if (!jetty || !jetty->valid() || jetty->id() == 0) {
            if (jetty) jetties_.push_back(std::move(jetty));
            return failLocked(Status::InternalError(
                "URMA adapter returned an invalid Jetty" LOC_MARK));
        }
        jetties_.push_back(std::move(jetty));
        jfc_indices_.push_back(jfc_index);
    }

    lifecycle_status_ = Status::OK();
    state_.store(State::kPrepared, std::memory_order_release);
    return Status::OK();
}

Status UbEndpoint::bind(const UbBootstrapDesc& peer) {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    const State current = state_.load(std::memory_order_relaxed);
    if (current == State::kReady) {
        if (samePeer(peer_, peer)) return Status::OK();
        return Status::InvalidArgument(
            "UB endpoint is already bound to another peer generation" LOC_MARK);
    }
    if (current == State::kFailed) return lifecycle_status_;
    if (current != State::kPrepared) {
        return Status::InvalidArgument(
            "UB endpoint must be prepared before bind" LOC_MARK);
    }

    state_.store(State::kBinding, std::memory_order_release);
    if (!peer.reply_msg.empty()) {
        return failLocked(Status::RpcServiceError(
            std::string("Peer rejected UB bootstrap: ") + peer.reply_msg));
    }
    if (peer.protocol_version != 1) {
        return failLocked(Status::InvalidArgument(
            "Unsupported UB bootstrap protocol version" LOC_MARK));
    }
    if (peer.endpoint_generation == 0 || peer.local_eid.empty()) {
        return failLocked(
            Status::InvalidArgument("Peer UB bootstrap is missing EID or "
                                    "endpoint generation" LOC_MARK));
    }
    if (peer.jetty_ids.size() != jetties_.size()) {
        return failLocked(
            Status::InvalidArgument("Peer UB bootstrap Jetty count does not "
                                    "match local endpoint" LOC_MARK));
    }
    if (!peer.jetty_uasids.empty() &&
        peer.jetty_uasids.size() != jetties_.size()) {
        return failLocked(
            Status::InvalidArgument("Peer UB bootstrap UASID count does not "
                                    "match local endpoint" LOC_MARK));
    }

    for (size_t index = 0; index < jetties_.size(); ++index) {
        if (peer.jetty_ids[index] == 0) {
            return failLocked(Status::InvalidArgument(
                "Peer UB bootstrap contains a zero Jetty ID" LOC_MARK));
        }
        RemoteJettyInfo remote;
        remote.eid = peer.local_eid;
        remote.id = peer.jetty_ids[index];
        if (!peer.jetty_uasids.empty()) {
            remote.uasid = peer.jetty_uasids[index];
        }
        auto status = adapter_->bindJetty(jetties_[index], remote);
        if (!status.ok()) return failLocked(std::move(status));
    }

    peer_ = peer;
    peer_generation_.store(peer.endpoint_generation, std::memory_order_release);
    lifecycle_status_ = Status::OK();
    state_.store(State::kReady, std::memory_order_release);
    return Status::OK();
}

Status UbEndpoint::makeBootstrapDesc(const std::string& segment_name,
                                     const std::string& local_nic_path,
                                     const std::string& peer_nic_path,
                                     uint64_t segment_generation,
                                     UbBootstrapDesc& output) const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    const State current = state_.load(std::memory_order_relaxed);
    if (current != State::kPrepared && current != State::kReady) {
        return Status::InvalidArgument(
            "UB endpoint must be prepared before bootstrap" LOC_MARK);
    }

    output = UbBootstrapDesc{};
    output.protocol_version = 1;
    output.segment_name = segment_name;
    output.local_nic_path = local_nic_path.empty()
                                ? context_->deviceInfo().native_device_path
                                : local_nic_path;
    output.peer_nic_path =
        peer_nic_path.empty() ? key_.peer_nic_path : peer_nic_path;
    output.local_device_name = context_->deviceInfo().native_device_name;
    output.local_device_id = context_->topologyId();
    output.local_eid_index = static_cast<int>(context_->deviceInfo().eid_index);
    output.local_eid = context_->deviceInfo().eid;
    output.jetty_ids.reserve(jetties_.size());
    output.jetty_uasids.reserve(jetties_.size());
    for (const auto& jetty : jetties_) {
        if (!jetty || !jetty->valid() || jetty->id() == 0) {
            return Status::InternalError(
                "UB endpoint contains an invalid Jetty" LOC_MARK);
        }
        output.jetty_ids.push_back(jetty->id());
        output.jetty_uasids.push_back(jetty->uasid());
    }
    output.endpoint_generation = generation_;
    output.segment_generation = segment_generation;
    output.capabilities = {"read", "write", "endpoint_generation"};
    return Status::OK();
}

bool UbEndpoint::tryAcquireOutstanding(uint64_t bytes) noexcept {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (state_.load(std::memory_order_relaxed) != State::kReady || !context_ ||
        !context_->active()) {
        return false;
    }

    outstanding_wrs_.fetch_add(1, std::memory_order_relaxed);
    outstanding_bytes_.fetch_add(bytes, std::memory_order_relaxed);
    context_->addInflight(bytes);
    return true;
}

void UbEndpoint::releaseOutstanding(uint64_t bytes) noexcept {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    const uint64_t current_wrs =
        outstanding_wrs_.load(std::memory_order_relaxed);
    if (current_wrs == 0) return;

    outstanding_wrs_.store(current_wrs - 1, std::memory_order_relaxed);
    const uint64_t current_bytes =
        outstanding_bytes_.load(std::memory_order_relaxed);
    outstanding_bytes_.store(current_bytes >= bytes ? current_bytes - bytes : 0,
                             std::memory_order_relaxed);
    context_->removeInflight(bytes);

    if (current_wrs == 1 &&
        state_.load(std::memory_order_relaxed) == State::kDestroying) {
        (void)finishRetireLocked();
    }
}

Status UbEndpoint::finishRetireLocked() {
    if (state_.load(std::memory_order_relaxed) == State::kDestroyed) {
        return retire_status_;
    }
    if (outstanding_wrs_.load(std::memory_order_relaxed) != 0) {
        return retire_status_;
    }

    // With no outstanding WR, RESET itself is a sufficient fence. When
    // quiesce() ran earlier this is an idempotent cleanup pass.
    auto reset_status = resetAndUnbindLocked();
    if (!reset_status.ok()) {
        retire_status_ = reset_status;
        return reset_status;
    }
    native_quiesced_ = true;
    auto delete_status = deleteJettysLocked();
    if (!delete_status.ok()) {
        retire_status_ = delete_status;
        return delete_status;
    }
    retire_status_ = Status::OK();
    peer_ = UbBootstrapDesc{};
    peer_generation_.store(0, std::memory_order_release);
    state_.store(State::kDestroyed, std::memory_order_release);
    return retire_status_;
}

Status UbEndpoint::retire() {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    const State current = state_.load(std::memory_order_relaxed);
    if (current == State::kDestroyed) return retire_status_;

    if (current != State::kDestroying) {
        state_.store(State::kDestroying, std::memory_order_release);
    }
    if (outstanding_wrs_.load(std::memory_order_relaxed) != 0) {
        return retire_status_;
    }
    return finishRetireLocked();
}

Status UbEndpoint::quiesce(uint32_t timeout_ms,
                           std::vector<Completion>& completions) {
    completions.clear();
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    const State current = state_.load(std::memory_order_relaxed);
    if (current == State::kDestroyed) return retire_status_;
    if (!adapter_ || timeout_ms == 0) {
        return Status::InvalidArgument(
            "UB endpoint quiesce requires an adapter and timeout" LOC_MARK);
    }

    state_.store(State::kDestroying, std::memory_order_release);
    Status fence_error = Status::OK();
    if (!native_quiesced_) {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        for (const auto& jetty : jetties_) {
            if (!jetty) continue;
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                if (fence_error.ok()) {
                    fence_error = Status::RdmaError(
                        "UB endpoint Jetty drain budget exhausted");
                }
                break;
            }
            const auto remaining =
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline -
                                                                      now);
            const uint32_t remaining_ms =
                static_cast<uint32_t>(std::max<int64_t>(1, remaining.count()));
            std::vector<Completion> drained;
            auto status = adapter_->quiesceJetty(jetty, remaining_ms, drained);
            completions.insert(completions.end(),
                               std::make_move_iterator(drained.begin()),
                               std::make_move_iterator(drained.end()));
            if (!status.ok() && fence_error.ok()) fence_error = status;
        }
        if (!fence_error.ok()) {
            // Do not RESET, unbind, or delete anything without a fence for
            // every Jetty. A later shutdown attempt can safely retry.
            return fence_error;
        }
        native_quiesced_ = true;
    }

    auto reset_status = resetAndUnbindLocked();
    if (!reset_status.ok()) return reset_status;
    if (outstanding_wrs_.load(std::memory_order_relaxed) == 0) {
        return finishRetireLocked();
    }
    return Status::OK();
}

bool UbEndpoint::reusable() const noexcept {
    switch (state()) {
        case State::kUninitialized:
        case State::kHandshaking:
        case State::kPrepared:
        case State::kBinding:
        case State::kReady:
            return true;
        case State::kFailed:
        case State::kDestroying:
        case State::kDestroyed:
            return false;
    }
    return false;
}

size_t UbEndpoint::jettyCount() const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    return jetties_.size();
}

JettyPtr UbEndpoint::jetty(size_t index) const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (jetties_.empty()) return nullptr;
    return jetties_[index % jetties_.size()];
}

size_t UbEndpoint::jfcIndex(size_t jetty_index) const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (jfc_indices_.empty()) return 0;
    return jfc_indices_[jetty_index % jfc_indices_.size()];
}

std::vector<JettyPtr> UbEndpoint::jetties() const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    return jetties_;
}

Status UbEndpoint::lifecycleStatus() const {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    return lifecycle_status_;
}

}  // namespace mooncake::tent::ub
