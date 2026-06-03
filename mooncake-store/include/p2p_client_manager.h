#pragma once

#include "client_manager.h"
#include "p2p_client_meta.h"

namespace mooncake {
class P2PClientManager final : public ClientManager {
   public:
    P2PClientManager(const int64_t disconnect_timeout_sec,
                     const int64_t crash_timeout_sec,
                     const ViewVersionId view_version);

   protected:
    DeploymentMode GetDeploymentMode() const override {
        return DeploymentMode::P2P;
    }

    std::unique_ptr<ClientIterator> InnerBuildClientIterator(
        ObjectIterateStrategy strategy) override;
    std::shared_ptr<ClientMeta> CreateClientMeta(
        const RegisterClientRequest& req) override;

    void OnClientRegistered(const std::shared_ptr<ClientMeta>& meta) override {
        auto p2p_meta = std::dynamic_pointer_cast<P2PClientMeta>(meta);
        if (p2p_meta) {
            p2p_meta->SetSyncing(true);
        }
    }

    HeartbeatTaskResult ProcessTask(const UUID& client_id,
                                    const HeartbeatTask& task) override;
};

}  // namespace mooncake
