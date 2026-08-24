#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "types.h"

namespace mooncake {

class SnapshotMaintenanceLease {
   public:
    static constexpr int64_t kTtlSeconds = 30;
    static constexpr int kKeepAliveReadyTimeoutMs = 1000;

    explicit SnapshotMaintenanceLease(std::string cluster_id);
    ~SnapshotMaintenanceLease();

    SnapshotMaintenanceLease(const SnapshotMaintenanceLease&) = delete;
    SnapshotMaintenanceLease& operator=(const SnapshotMaintenanceLease&) =
        delete;

    ErrorCode Acquire();
    ErrorCode Release();

    bool IsHeld() const;
    EtcdLeaseId lease_id() const;
    std::string owner_token() const;
    const std::string& lock_key() const { return lock_key_; }

    // Keeps publisher transaction tests independent of a live etcd process.
    static std::unique_ptr<SnapshotMaintenanceLease> MakeForTesting(
        std::string cluster_id, std::string owner_token);

   private:
    SnapshotMaintenanceLease(std::string cluster_id, std::string owner_token,
                             bool testing);

    ErrorCode StopKeepAlive(bool revoke);

    std::string cluster_id_;
    std::string lock_key_;
    std::string owner_token_;
    EtcdLeaseId lease_id_{0};
    mutable std::mutex mutex_;
    std::thread keepalive_thread_;
    ErrorCode keepalive_result_{ErrorCode::OK};
    bool testing_{false};
    bool lock_created_{false};
    bool keepalive_stopped_{true};
    bool shutdown_requested_{false};
};

}  // namespace mooncake
