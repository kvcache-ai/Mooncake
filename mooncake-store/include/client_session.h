#pragma once

#include <memory>
#include <mutex>
#include <string>

#include "client_liveness.h"
#include "types.h"

namespace mooncake {

class ClientRegistry;

// One process-local incarnation of a client. Registry membership and resource
// ownership are independent: buffers may retain a terminal session after the
// registry has retired it. An OFFLINE incarnation can never be revived.
class ClientSession final : public ClientLivenessRecord {
   public:
    const UUID& client_id() const noexcept { return client_id_; }

    // Independent of the transition guard: pool commits can update the host
    // while a registration retains the client, without reentering the registry.
    std::string host_id() const {
        std::lock_guard lock(host_mutex_);
        return host_id_;
    }
    void SetHostId(const std::string& host_id) {
        if (host_id.empty()) return;
        std::lock_guard lock(host_mutex_);
        host_id_ = host_id;
    }

   private:
    friend class ClientRegistry;
    ClientSession(const UUID& client_id, TimePoint now)
        : ClientLivenessRecord(now), client_id_(client_id) {}

    const UUID client_id_;
    mutable std::mutex host_mutex_;
    std::string host_id_;
};

using ClientSessionPtr = std::shared_ptr<ClientSession>;

}  // namespace mooncake
