// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/context.h"

#include <algorithm>
#include <chrono>
#include <limits>

namespace mooncake::tent::ub {
namespace {

uint64_t steadyNowNs() noexcept {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

}  // namespace

UbContext::UbContext(Topology::NicID topology_id, DeviceInfo device,
                     std::shared_ptr<UrmaAdapter> adapter)
    : topology_id_(topology_id),
      device_(std::move(device)),
      adapter_(std::move(adapter)) {}

UbContext::~UbContext() { (void)shutdown(); }

Status UbContext::initialize(uint32_t jfc_count, const JfcOptions& options) {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (state_.load(std::memory_order_relaxed) != State::kUninitialized) {
        return Status::InvalidArgument(
            "UB context can only be initialized once" LOC_MARK);
    }
    if (!adapter_ || !device_.active || jfc_count == 0) {
        return Status::InvalidArgument(
            "Invalid UB context device or JFC count" LOC_MARK);
    }
    // Each UbJfc owns one send JFC and one receive JFC/JFR.
    if (device_.capabilities.max_jfc != 0 &&
        (static_cast<uint64_t>(jfc_count) * 2U) >
            device_.capabilities.max_jfc) {
        return Status::InvalidArgument(
            "Requested UB JFC count exceeds device capability" LOC_MARK);
    }

    auto status = adapter_->openContext(device_, handle_);
    if (!status.ok()) {
        // Providers are allowed to report an error together with a partially
        // created handle. Adopt it and make cleanup retryable just like a
        // later JFC creation failure.
        if (handle_) {
            state_.store(State::kDraining, std::memory_order_release);
            (void)shutdownLocked();
        }
        return status;
    }

    jfcs_.reserve(jfc_count);
    for (uint32_t i = 0; i < jfc_count; ++i) {
        JfcPtr native_jfc;
        status = adapter_->createJfc(handle_, options, native_jfc);
        if (!status.ok()) {
            // A provider may return an error after allocating a partial native
            // JFC. Retain that handle as part of the same retryable ownership
            // graph instead of letting a temporary shared_ptr destroy it.
            if (native_jfc) {
                jfcs_.push_back(std::make_shared<UbJfc>(jfcs_.size(), adapter_,
                                                        std::move(native_jfc)));
            }
            state_.store(State::kDraining, std::memory_order_release);
            (void)shutdownLocked();
            return status;
        }
        jfcs_.push_back(
            std::make_shared<UbJfc>(i, adapter_, std::move(native_jfc)));
    }
    jfc_success_epochs_.assign(jfcs_.size(), 0);
    failure_cleanup_complete_ = false;
    state_.store(State::kActive, std::memory_order_release);
    return Status::OK();
}

Status UbContext::shutdown() {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    return shutdownLocked();
}

Status UbContext::shutdownLocked() {
    auto current = state_.load(std::memory_order_relaxed);
    if (current == State::kClosed) return Status::OK();
    if (current == State::kUninitialized && !handle_ && jfcs_.empty()) {
        state_.store(State::kClosed, std::memory_order_release);
        return Status::OK();
    }
    state_.store(State::kDraining, std::memory_order_release);

    Status first_error = Status::OK();
    for (size_t index = jfcs_.size(); index > 0;) {
        --index;
        auto& jfc = jfcs_[index];
        if (!jfc) {
            jfcs_.erase(jfcs_.begin() + static_cast<std::ptrdiff_t>(index));
            continue;
        }
        auto status = jfc->close();
        if (!status.ok()) {
            if (first_error.ok()) first_error = status;
            continue;
        }
        if (jfc->handle()) {
            if (first_error.ok()) {
                first_error = Status::InternalError(
                    "URMA adapter retained a JFC after successful "
                    "delete" LOC_MARK);
            }
            continue;
        }
        jfcs_.erase(jfcs_.begin() + static_cast<std::ptrdiff_t>(index));
    }
    // A Context is the parent of every JFC. Never ask the provider to delete
    // it while a failed JFC remains owned; the next shutdown call resumes at
    // the first failed child.
    if (!jfcs_.empty()) return first_error;

    if (handle_) {
        auto status = adapter_->closeContext(handle_);
        if (!status.ok()) {
            if (first_error.ok()) first_error = status;
            return first_error;
        }
        if (handle_) {
            return Status::InternalError(
                "URMA adapter retained a Context after successful "
                "close" LOC_MARK);
        }
    }
    state_.store(State::kClosed, std::memory_order_release);
    return first_error;
}

std::shared_ptr<UbJfc> UbContext::jfc(size_t index) const {
    if (jfcs_.empty()) return nullptr;
    return jfcs_[index % jfcs_.size()];
}

void UbContext::addInflight(uint64_t bytes) noexcept {
    inflight_bytes_.fetch_add(bytes, std::memory_order_relaxed);
    outstanding_wrs_.fetch_add(1, std::memory_order_relaxed);
}

bool UbContext::markUnavailable() noexcept {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    const auto current = state_.load(std::memory_order_relaxed);
    if (current != State::kActive && current != State::kFailed) return false;

    const bool newly_failed = current == State::kActive;
    const uint64_t now_ns = steadyNowNs();
    if (newly_failed) {
        state_.store(State::kFailed, std::memory_order_release);
        failure_cleanup_complete_ = false;
        failure_started_ns_.store(now_ns, std::memory_order_release);
    }
    ++failure_epoch_;
    if (failure_epoch_ == 0) ++failure_epoch_;
    last_failure_ns_.store(now_ns, std::memory_order_release);
    return newly_failed;
}

void UbContext::completeFailureCleanup() noexcept {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (state_.load(std::memory_order_relaxed) == State::kFailed) {
        failure_cleanup_complete_ = true;
    }
}

bool UbContext::recordPollSuccess(size_t jfc_index,
                                  uint64_t cooldown_ns) noexcept {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (state_.load(std::memory_order_relaxed) != State::kFailed ||
        !failure_cleanup_complete_ || jfc_index >= jfc_success_epochs_.size()) {
        return false;
    }
    jfc_success_epochs_[jfc_index] = failure_epoch_;
    if (outstanding_wrs_.load(std::memory_order_acquire) != 0) return false;

    const uint64_t now_ns = steadyNowNs();
    const uint64_t failed_ns = last_failure_ns_.load(std::memory_order_acquire);
    if (failed_ns == 0 || now_ns < failed_ns ||
        now_ns - failed_ns < cooldown_ns) {
        return false;
    }
    if (!std::all_of(
            jfc_success_epochs_.begin(), jfc_success_epochs_.end(),
            [this](uint64_t epoch) { return epoch == failure_epoch_; })) {
        return false;
    }

    state_.store(State::kActive, std::memory_order_release);
    failure_cleanup_complete_ = false;
    recovery_count_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

void UbContext::removeInflight(uint64_t bytes) noexcept {
    auto current = inflight_bytes_.load(std::memory_order_relaxed);
    while (!inflight_bytes_.compare_exchange_weak(
        current, current >= bytes ? current - bytes : 0,
        std::memory_order_relaxed)) {
    }
    current = outstanding_wrs_.load(std::memory_order_relaxed);
    while (current != 0 &&
           !outstanding_wrs_.compare_exchange_weak(current, current - 1,
                                                   std::memory_order_relaxed)) {
    }
}

}  // namespace mooncake::tent::ub
