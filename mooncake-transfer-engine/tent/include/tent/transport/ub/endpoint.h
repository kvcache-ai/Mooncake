// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_ENDPOINT_H_
#define TENT_TRANSPORT_UB_ENDPOINT_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "tent/common/status.h"
#include "tent/common/types.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/topology.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {

// One cache entry represents a local-device to remote-device path. The peer
// NIC path is part of the identity because a remote topology ID may be reused
// after the peer republishes its topology.
struct UbEndpointKey {
    Topology::NicID local_topology_id{-1};
    SegmentID remote_segment_id{LOCAL_SEGMENT_ID};
    Topology::NicID remote_topology_id{-1};
    std::string peer_nic_path;

    bool operator==(const UbEndpointKey&) const = default;

    [[nodiscard]] bool valid() const noexcept {
        return local_topology_id >= 0 && remote_topology_id >= 0 &&
               !peer_nic_path.empty();
    }
};

struct UbEndpointKeyHash {
    size_t operator()(const UbEndpointKey& key) const noexcept;
};

// A UB endpoint is one immutable incarnation of a Jetty set. Its generation
// is allocated process-wide and is never reused. Lifecycle operations are
// serialized internally; a failed endpoint can only retire, never reconnect.
class UbEndpoint final : public std::enable_shared_from_this<UbEndpoint> {
   public:
    enum class State : uint8_t {
        kUninitialized,
        kHandshaking,
        kPrepared,
        kBinding,
        kReady,
        kFailed,
        kDestroying,
        kDestroyed,
    };

    UbEndpoint(UbEndpointKey key, UbContextPtr context,
               std::shared_ptr<UrmaAdapter> adapter, uint32_t jetty_count,
               JettyOptions jetty_options = {});
    ~UbEndpoint();

    UbEndpoint(const UbEndpoint&) = delete;
    UbEndpoint& operator=(const UbEndpoint&) = delete;

    // Creates the local Jetty set. Concurrent calls share the same attempt.
    // An error permanently moves this incarnation to kFailed.
    Status prepare();

    // Binds each local Jetty to the peer EID and the corresponding peer Jetty
    // ID. The bootstrap must describe exactly one peer Jetty per local Jetty.
    Status bind(const UbBootstrapDesc& peer);

    // Builds the local half of the native UB bootstrap after prepare().
    Status makeBootstrapDesc(const std::string& segment_name,
                             const std::string& local_nic_path,
                             const std::string& peer_nic_path,
                             uint64_t segment_generation,
                             UbBootstrapDesc& output) const;

    // Admission to the posting path is synchronized with retirement: once
    // kDestroying is visible no new work can acquire the endpoint. Every
    // successful acquire must have one matching release.
    [[nodiscard]] bool tryAcquireOutstanding(uint64_t bytes = 0) noexcept;
    void releaseOutstanding(uint64_t bytes = 0) noexcept;

    // Stops new posts immediately. With outstanding work, native resources
    // remain intact until either completions drain naturally or quiesce()
    // establishes an explicit no-more-DMA fence. Idempotent.
    Status retire();

    // Establishes a native drain fence for every Jetty, then resets/unbinds
    // them. Returned completions still own the corresponding logical tokens
    // and must be dispatched by Workers. A failed fence leaves all resources
    // alive so shutdown can be retried safely.
    Status quiesce(uint32_t timeout_ms, std::vector<Completion>& completions);

    [[nodiscard]] const UbEndpointKey& key() const noexcept { return key_; }
    [[nodiscard]] const UbContextPtr& context() const noexcept {
        return context_;
    }
    [[nodiscard]] uint64_t generation() const noexcept { return generation_; }
    [[nodiscard]] State state() const noexcept {
        return state_.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool ready() const noexcept {
        return state() == State::kReady;
    }
    [[nodiscard]] bool failed() const noexcept {
        return state() == State::kFailed;
    }
    [[nodiscard]] bool reusable() const noexcept;
    [[nodiscard]] uint64_t peerGeneration() const noexcept {
        return peer_generation_.load(std::memory_order_acquire);
    }
    [[nodiscard]] bool peerSupportsCapability(
        std::string_view capability) const;
    [[nodiscard]] bool peerMatchesReceiverCredit(uint64_t session_high,
                                                 uint64_t session_low,
                                                 uint64_t epoch) const;
    [[nodiscard]] uint64_t outstandingWrs() const noexcept {
        return outstanding_wrs_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] uint64_t outstandingBytes() const noexcept {
        return outstanding_bytes_.load(std::memory_order_relaxed);
    }
    [[nodiscard]] size_t jettyCount() const;
    [[nodiscard]] JettyPtr jetty(size_t index) const;
    [[nodiscard]] size_t jfcIndex(size_t jetty_index) const;
    [[nodiscard]] std::vector<JettyPtr> jetties() const;
    [[nodiscard]] Status lifecycleStatus() const;

   private:
    static uint64_t allocateGeneration() noexcept;

    Status failLocked(Status status);
    Status resetAndUnbindLocked();
    Status deleteJettysLocked();
    Status finishRetireLocked();
    static void rememberFirstError(const Status& candidate, Status& first);

    const UbEndpointKey key_;
    const UbContextPtr context_;
    const std::shared_ptr<UrmaAdapter> adapter_;
    const uint32_t jetty_count_;
    const JettyOptions jetty_options_;
    const uint64_t generation_;

    mutable std::mutex lifecycle_mutex_;
    std::vector<JettyPtr> jetties_;
    std::vector<size_t> jfc_indices_;
    UbBootstrapDesc peer_;
    Status lifecycle_status_;
    Status retire_status_;
    bool native_quiesced_{false};
    std::atomic<State> state_{State::kUninitialized};
    std::atomic<uint64_t> peer_generation_{0};
    std::atomic<uint64_t> outstanding_wrs_{0};
    std::atomic<uint64_t> outstanding_bytes_{0};
};

using UbEndpointPtr = std::shared_ptr<UbEndpoint>;
// Keep the spelling used by the design document available to callers.
using UbEndPoint = UbEndpoint;

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_ENDPOINT_H_
