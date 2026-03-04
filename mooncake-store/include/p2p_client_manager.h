#pragma once

#include "client_manager.h"

namespace mooncake {
class P2PClientManager final : public ClientManager {
   public:
    P2PClientManager(const int64_t disconnect_timeout_sec,
                     const int64_t crash_timeout_sec,
                     const ViewVersionId view_version);

   protected:
    std::unique_ptr<ClientIterator> InnerBuildClientIterator(
        ObjectIterateStrategy strategy) override;
    std::shared_ptr<ClientMeta> CreateClientMeta(
        const RegisterClientRequest& req) override;

    HeartbeatTaskResult ProcessTask(const UUID& client_id,
                                    const HeartbeatTask& task) override;
};

}  // namespace mooncake
