#pragma once

#include <string>

#include "ha/progress/standby_progress_store.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace redis {

class RedisStandbyProgressStore final : public StandbyProgressStore {
   public:
    RedisStandbyProgressStore(std::string connstring,
                              ClusterNamespace cluster_namespace);

    ErrorCode Publish(const StandbyProgressRecord& record) override;

    tl::expected<std::vector<StandbyProgressRecord>, ErrorCode> List() override;

    ErrorCode Delete(std::string_view standby_id) override;

   private:
    static ClusterNamespace ResolveClusterNamespace(
        const ClusterNamespace& cluster_namespace);
    static std::string BuildIndexKey(const ClusterNamespace& cluster_namespace);
    static std::string BuildRecordKey(const ClusterNamespace& cluster_namespace,
                                      std::string_view standby_id);

    std::string connstring_;
    ClusterNamespace cluster_namespace_;
    std::string index_key_;
};

}  // namespace redis
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
