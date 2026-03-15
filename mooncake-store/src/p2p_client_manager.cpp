#include "p2p_client_manager.h"
#include "p2p_client_meta.h"
#include <glog/logging.h>
#include <algorithm>

namespace mooncake {

class CapacityPriorityIterator : public ClientIterator {
   public:
    CapacityPriorityIterator(
        const std::unordered_map<UUID, std::shared_ptr<ClientMeta>,
                                 boost::hash<UUID>>& client_metas) {
        if (client_metas.empty()) return;

        clients_.reserve(client_metas.size());
        for (auto& client : client_metas) {
            if (auto p2p_meta =
                    std::static_pointer_cast<P2PClientMeta>(client.second)) {
                clients_.emplace_back(p2p_meta);
            }
        }

        std::sort(clients_.begin(), clients_.end(),
                  [](const auto& a, const auto& b) {
                      return std::static_pointer_cast<P2PClientMeta>(a)
                                 ->GetAvailableCapacity() >
                             std::static_pointer_cast<P2PClientMeta>(b)
                                 ->GetAvailableCapacity();
                  });
    }
};

P2PClientManager::P2PClientManager(const int64_t disconnect_timeout_sec,
                                   const int64_t crash_timeout_sec,
                                   const ViewVersionId view_version)
    : ClientManager(disconnect_timeout_sec, crash_timeout_sec, view_version) {}

std::unique_ptr<ClientIterator> P2PClientManager::InnerBuildClientIterator(
    ObjectIterateStrategy strategy) {
    auto iterator = ClientManager::InnerBuildClientIterator(strategy);
    if (iterator) {
        return iterator;
    }
    switch (strategy) {
        case ObjectIterateStrategy::CAPACITY_PRIORITY:
            return std::make_unique<CapacityPriorityIterator>(client_metas_);
        default:
            return nullptr;
    }
}

std::shared_ptr<ClientMeta> P2PClientManager::CreateClientMeta(
    const RegisterClientRequest& req) {
    auto meta = std::make_shared<P2PClientMeta>(
        req.client_id, req.ip_address.value_or(""), req.rpc_port.value_or(0));
    return meta;
}

HeartbeatTaskResult P2PClientManager::ProcessTask(const UUID& client_id,
                                                  const HeartbeatTask& task) {
    HeartbeatTaskResult result;
    result.type = task.type_;

    switch (task.type_) {
        case HeartbeatTaskType::SYNC_SEGMENT_META: {
            auto client_meta =
                std::static_pointer_cast<P2PClientMeta>(GetClient(client_id));
            const auto* param = std::get_if<SyncSegmentMetaParam>(&task.param_);
            if (client_meta && param) {
                auto sync_res =
                    client_meta->UpdateSegmentUsages(param->tier_usages);
                result.detail = sync_res;
                for (const auto& sub : sync_res.sub_results) {
                    if (sub.error != ErrorCode::OK) {
                        // result.error means the task is failed.
                        // here just sub task error, don't affect task result.
                        LOG(ERROR) << "fail to update segment usages"
                                   << ", client_id=" << client_id
                                   << ", segment_id=" << sub.segment_id
                                   << ", error=" << sub.error;
                    }
                }
            } else {
                result.error = ErrorCode::INVALID_PARAMS;
            }
            break;
        }
        default:
            result.error = ErrorCode::NOT_IMPLEMENTED;
            break;
    }
    return result;
}

}  // namespace mooncake
