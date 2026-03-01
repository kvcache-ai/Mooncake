#pragma once

#include <boost/functional/hash.hpp>
#include "segment_manager.h"
#include <chrono>
#include <memory>
#include <ylt/util/expected.hpp>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "types.h"

namespace mooncake {

struct ClientHealthState {
    ClientStatus status = ClientStatus::UNDEFINED;
    std::chrono::steady_clock::time_point last_heartbeat;
};
/**
 * @brief ClientMeta records the meta data of a client including health status
 * and segment information.
 */
class ClientMeta {
   public:
    virtual ~ClientMeta() = default;

    ClientMeta(const UUID& client_id);

    tl::expected<void, ErrorCode> MountSegment(const Segment& segment);

    tl::expected<void, ErrorCode> UnmountSegment(const UUID& segment_id);

    tl::expected<std::vector<Segment>, ErrorCode> GetSegments();

    tl::expected<std::pair<size_t, size_t>, ErrorCode> QuerySegments(
        const std::string& segment_name);
    tl::expected<std::shared_ptr<Segment>, ErrorCode> QuerySegment(
        const UUID& segment_id);

    virtual tl::expected<std::vector<std::string>, ErrorCode> QueryIp(
        const UUID& client_id) = 0;

    using SegmentRemovalCallback = std::function<void(const UUID& segment_id)>;
    void SetSegmentRemovalCallback(SegmentRemovalCallback cb);

   public:
    static void SetTimeouts(int64_t disconnect_sec, int64_t crash_sec);

    /**
     * @brief Update heartbeat timestamp and health status.
     * Attention: if client is CRASHED, the heartbeat will not be updated.
     * @return std::pair<ClientStatus, ClientStatus> {old_status, new_status}
     */
    std::pair<ClientStatus, ClientStatus> Heartbeat();

    /**
     * @brief Based on last heartbeat timestamp, update health status
     *
     * States machine:
     * - HEALTH:
     *   -> DISCONNECTION: If (now - last_heartbeat) > disconnect_timeout_sec.
     *
     * - DISCONNECTION:
     *   -> HEALTH: If (now - last_heartbeat) <= disconnect_timeout_sec.
     *      (Implies a Heartbeat() call updated the timestamp).
     *   -> CRASHED: If (now - last_heartbeat) > crash_timeout_sec.
     *
     * - CRASHED: Final state.
     *
     * @return std::pair<ClientStatus, ClientStatus> {old_status, new_status}
     */
    std::pair<ClientStatus, ClientStatus> CheckHealth();

   public:
    // Hooks for health status changes
    void OnDisconnected();
    virtual void DoOnDisconnected() = 0;
    void OnRecovered();
    virtual void DoOnRecovered() = 0;
    void OnCrashed();

   public:
    UUID get_client_id() const { return client_id_; }

    ClientHealthState get_health_state() const;
    bool is_health() const;

   protected:
    tl::expected<void, ErrorCode> InnerStatusCheck() const;
    void InnerUpdateHeartbeat();
    std::pair<ClientStatus, ClientStatus> InnerUpdateHealthStatus();
    std::string HealthToString(ClientStatus status) const;

   protected:
    virtual std::shared_ptr<SegmentManager> GetSegmentManager() = 0;

   protected:
    static int64_t disconnect_timeout_sec_;
    static int64_t crash_timeout_sec_;

    mutable SharedMutex client_mutex_;
    UUID client_id_;
    ClientHealthState health_state_ GUARDED_BY(client_mutex_);
};

}  // namespace mooncake
