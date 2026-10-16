// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_CONTEXT_H_
#define TENT_TRANSPORT_UB_CONTEXT_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/topology.h"
#include "tent/transport/ub/jfc.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {

class UbContext final : public std::enable_shared_from_this<UbContext> {
   public:
    enum class State : uint8_t {
        kUninitialized,
        kActive,
        kFailed,
        kDraining,
        kClosed,
    };

    UbContext(Topology::NicID topology_id, DeviceInfo device,
              std::shared_ptr<UrmaAdapter> adapter);
    ~UbContext();

    UbContext(const UbContext&) = delete;
    UbContext& operator=(const UbContext&) = delete;

    Status initialize(uint32_t jfc_count, const JfcOptions& options);
    Status shutdown();
    // Returns true only for the Active -> Failed transition. Every subsequent
    // poll error advances the failure epoch so stale successes from another
    // JFC can never reactivate the device.
    [[nodiscard]] bool markUnavailable() noexcept;
    // The failure owner calls this only after every endpoint backed by this
    // device has been unpublished and has reached Destroyed. Pending native
    // cleanup remains quarantined and keeps this recovery barrier incomplete.
    void completeFailureCleanup() noexcept;
    // Pollers continue probing every JFC. A transiently failed context becomes
    // active only after all JFCs have succeeded in the current failure epoch,
    // the error-free cooldown has elapsed, old WRs have drained, and endpoint
    // unpublication has completed.
    [[nodiscard]] bool recordPollSuccess(size_t jfc_index,
                                         uint64_t cooldown_ns) noexcept;

    [[nodiscard]] Topology::NicID topologyId() const noexcept {
        return topology_id_;
    }
    [[nodiscard]] const DeviceInfo& deviceInfo() const noexcept {
        return device_;
    }
    [[nodiscard]] const ContextPtr& handle() const noexcept { return handle_; }
    [[nodiscard]] State state() const noexcept {
        return state_.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool active() const noexcept {
        return state() == State::kActive;
    }
    [[nodiscard]] const std::vector<std::shared_ptr<UbJfc>>& jfcs() const {
        return jfcs_;
    }
    [[nodiscard]] std::shared_ptr<UbJfc> jfc(size_t index) const;

    void addInflight(uint64_t bytes) noexcept;
    void removeInflight(uint64_t bytes) noexcept;
    [[nodiscard]] uint64_t inflightBytes() const noexcept {
        return inflight_bytes_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t outstandingWrs() const noexcept {
        return outstanding_wrs_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t recoveryCount() const noexcept {
        return recovery_count_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t failureStartedNs() const noexcept {
        return failure_started_ns_.load(std::memory_order_acquire);
    }

   private:
    Status shutdownLocked();

    const Topology::NicID topology_id_;
    const DeviceInfo device_;
    std::shared_ptr<UrmaAdapter> adapter_;
    ContextPtr handle_;
    std::vector<std::shared_ptr<UbJfc>> jfcs_;
    mutable std::mutex lifecycle_mutex_;
    std::atomic<State> state_{State::kUninitialized};
    std::atomic<uint64_t> inflight_bytes_{0};
    std::atomic<uint64_t> outstanding_wrs_{0};
    std::atomic<uint64_t> failure_started_ns_{0};
    std::atomic<uint64_t> last_failure_ns_{0};
    std::atomic<uint64_t> recovery_count_{0};
    uint64_t failure_epoch_{0};
    std::vector<uint64_t> jfc_success_epochs_;
    bool failure_cleanup_complete_{false};
};

using UbContextPtr = std::shared_ptr<UbContext>;

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_CONTEXT_H_
